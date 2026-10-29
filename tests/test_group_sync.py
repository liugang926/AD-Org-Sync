import copy
import uuid
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from sync_app import synchronization as sync
from sync_app.directory import ActiveDirectory, validate_group_dn
from sync_app.domain import RuleError
from sync_app.models import (
    Configuration,
    GroupBinding,
    Job,
    Operation,
    SyncAuditEvent,
)

from .fakes import Directory, account, user


ROOT = "OU=People,DC=example,DC=com"
CHILD = "OU=研发,OU=People,DC=example,DC=com"
GRANDCHILD = "OU=平台,OU=研发,OU=People,DC=example,DC=com"


class TreeSource:
    def __init__(self, users=None, departments=None):
        self.users = copy.deepcopy(users or [])
        self.departments = copy.deepcopy(departments or [])

    def collect(self, root):
        return copy.deepcopy(self.users), copy.deepcopy(self.departments)

    def user(self, uid):
        return copy.deepcopy(next(item for item in self.users if item["source_id"] == uid))

    def user_in_scope(self, employee, root_id):
        return str(root_id) in employee["departments"]

    def close(self):
        pass


def employee(uid, name, employee_id, departments, primary):
    item = user(uid, employee_id)
    item.update(
        name=name,
        departments=departments,
        primary_department=primary,
    )
    return item


@pytest.fixture
def group_setup():
    config = Configuration.current()
    config.root_ou = ROOT
    config.identity_anchor = sync.directory_identity_anchor()
    config.match_field = "employee_id"
    config.sync_groups = True
    config.auto_onboard_accounts = True
    config.save()
    departments = [
        {"id": "1", "name": "公司", "parent": "0"},
        {"id": "2", "name": "研发", "parent": "1"},
        {"id": "3", "name": "平台", "parent": "2"},
    ]
    users = [
        employee("u1", "研发员工", "T0001919", ["2"], "2"),
        employee("u2", "公司员工", "T0002000", ["1"], "1"),
    ]
    return config, TreeSource(users, departments), Directory(
        [account("T0001919", "t0001919"), account("T0002000", "t0002000")]
    )


@pytest.mark.django_db
def test_plan_and_apply_create_global_groups_with_child_group_nesting(group_setup):
    _, source, ad = group_setup
    job = Job.objects.create()
    job.plan = sync.plan(job, source, ad)

    groups = {item["source_id"]: item for item in job.plan["groups"]}
    assert set(groups) == {"1", "2", "3"}
    assert groups["2"]["dn"] == f"CN=研发,{CHILD}"
    assert all(item["action"] == "ensure_group" for item in groups.values())
    assert all(len(item["sam_account_name"]) <= 20 for item in groups.values())

    nesting = [
        item for item in job.plan["group_memberships"]
        if item["member_type"] == "group"
    ]
    assert {(item["department_id"], item["member_source_id"]) for item in nesting} == {
        ("1", "group:2"),
        ("2", "group:3"),
    }
    root_group_members = [
        item for item in job.plan["group_memberships"]
        if item["department_id"] == "1" and item["action"] == "add_group_member"
    ]
    assert "group:3" not in {item["member_source_id"] for item in root_group_members}

    assert sync.apply(job, source, ad) == "success"
    assert GroupBinding.objects.count() == 3
    assert len(ad.groups) == 3
    root_group = next(
        group for group in ad.groups.values()
        if group["dn"] == f"CN=公司,{ROOT}"
    )
    child_group = next(
        group for group in ad.groups.values()
        if group["dn"] == f"CN=研发,{CHILD}"
    )
    grandchild_group = next(
        group for group in ad.groups.values()
        if group["dn"] == f"CN=平台,{GRANDCHILD}"
    )
    assert all(group["group_type"] == 0x80000002 for group in ad.groups.values())
    assert child_group["dn"] in root_group["members"]
    assert f"CN=t0001919,{CHILD}" in child_group["members"]
    assert grandchild_group["dn"] in child_group["members"]
    assert grandchild_group["dn"] not in root_group["members"]
    assert any(item["dn"] == f"CN=t0001919,{CHILD}" for item in ad.items)
    assert Operation.objects.filter(action="ensure_group").count() == 3
    assert Operation.objects.filter(action="add_group_member").count() >= 4
    assert SyncAuditEvent.objects.filter(object_type="group", status="success").count() == 3
    assert SyncAuditEvent.objects.filter(object_type="group_membership", status="success").count() >= 4


@pytest.mark.django_db
def test_user_inside_managed_root_is_moved_and_policy_rejection_has_reason_code(group_setup):
    config, source, ad = group_setup
    job = Job.objects.create()
    job.plan = sync.plan(job, source, ad)
    employee_operation = next(
        item for item in job.plan["operations"] if item["source_id"] == "u1"
    )
    assert employee_operation["action"] == "bind"
    assert employee_operation["ou"] == CHILD
    assert employee_operation["reason_code"] == "bind"

    config.auto_onboard_accounts = False
    config.save()
    ad.items[0]["dn"] = "CN=t0001919,OU=Existing,DC=example,DC=com"
    blocked = Job.objects.create()
    blocked.plan = sync.plan(blocked, source, ad)
    blocked_operation = next(
        item for item in blocked.plan["operations"] if item["source_id"] == "u1"
    )
    assert blocked_operation["action"] == "conflict"
    assert blocked_operation["reason_code"] == "auto_onboard_disabled"
    assert "自动纳管" in blocked_operation["reason"]


@pytest.mark.django_db
def test_existing_same_ou_global_group_is_reused_with_its_actual_sam(group_setup):
    _, source, ad = group_setup
    group_dn = f"CN=研发,{CHILD}"
    existing_guid = str(uuid.uuid4())
    ad.ous[CHILD.casefold()] = str(uuid.uuid4())
    ad.groups[group_dn.casefold()] = {
        "guid": existing_guid,
        "dn": group_dn,
        "sam_account_name": "existing-group",
        "group_type": 0x80000002,
        "members": [],
    }

    job = Job.objects.create()
    job.plan = sync.plan(job, source, ad)
    planned = next(item for item in job.plan["groups"] if item["source_id"] == "2")

    assert planned["action"] == "ensure_group"
    assert planned["guid"] == existing_guid
    assert planned["sam_account_name"] == "existing-group"
    assert planned["reason_code"] == "reuse_group"
    assert len(ad.groups) == 1

    assert sync.apply(job, source, ad) == "success"
    binding = GroupBinding.objects.get(source_id="2")
    assert binding.object_guid == uuid.UUID(existing_guid)
    assert binding.sam_account_name == "existing-group"
    assert len(ad.groups) == 3
    event = SyncAuditEvent.objects.get(
        object_type="group",
        source_id="2",
        reason_code="reuse_group",
    )
    assert event.before_values == {
        "bound": False,
        "dn": "",
        "guid": None,
        "sam_account_name": "",
    }
    assert event.after_values["bound"] is True
    assert event.after_values["guid"] == existing_guid
    assert event.after_values["sam_account_name"] == "existing-group"


@pytest.mark.django_db
def test_existing_binding_adopts_changed_sam_of_same_ou_group(group_setup):
    _, source, ad = group_setup
    first = Job.objects.create()
    first.plan = sync.plan(first, source, ad)
    assert sync.apply(first, source, ad) == "success"

    group_dn = f"CN=研发,{CHILD}"
    original_binding = GroupBinding.objects.get(source_id="2")
    original_sam = original_binding.sam_account_name
    original_guid = str(original_binding.object_guid)
    ad.groups[group_dn.casefold()]["sam_account_name"] = "renamed-group"
    job = Job.objects.create()
    job.plan = sync.plan(job, source, ad)
    planned = next(item for item in job.plan["groups"] if item["source_id"] == "2")

    assert planned["action"] == "ensure_group"
    assert planned["sam_account_name"] == "renamed-group"
    assert planned["reason_code"] == "reuse_group_rebind"

    assert sync.apply(job, source, ad) == "success"
    binding = GroupBinding.objects.get(source_id="2")
    assert binding.sam_account_name == "renamed-group"
    event = SyncAuditEvent.objects.filter(object_type="group", source_id="2").order_by("-started_at").first()
    assert event.reason_code == "reuse_group_rebind"
    assert event.before_values == {
        "bound": True,
        "dn": group_dn,
        "guid": original_guid,
        "sam_account_name": original_sam,
    }
    assert event.after_values["bound"] is True
    assert event.after_values["guid"] == original_guid
    assert event.after_values["sam_account_name"] == "renamed-group"


@pytest.mark.django_db
def test_existing_group_with_replaced_guid_remains_conflict(group_setup):
    _, source, ad = group_setup
    group_dn = f"CN=研发,{CHILD}"
    original_guid = uuid.uuid4()
    GroupBinding.objects.create(
        source_id="2",
        dn=group_dn,
        object_guid=original_guid,
        sam_account_name="existing-group",
    )
    ad.groups[group_dn.casefold()] = {
        "guid": str(uuid.uuid4()),
        "dn": group_dn,
        "sam_account_name": "existing-group",
        "group_type": 0x80000002,
        "members": [],
    }

    job = Job.objects.create()
    job.plan = sync.plan(job, source, ad)
    planned = next(item for item in job.plan["groups"] if item["source_id"] == "2")

    assert planned["action"] == "conflict"
    assert planned["reason_code"] == "group_conflict"
    assert "对象已被替换" in planned["reason"]


@pytest.mark.django_db
def test_group_sync_can_be_disabled_without_changing_ou_plan(group_setup):
    config, source, ad = group_setup
    config.sync_groups = False
    config.save()
    job = Job.objects.create(scope="organization")
    job.plan = sync.plan(job, source, ad)
    assert len(job.plan["departments"]) == 3
    assert job.plan["groups"] == []
    assert sync.apply(job, source, ad) == "success"
    assert ad.groups == {}


@pytest.mark.django_db
def test_job_page_explains_organization_scope_and_user_skip_reason(group_setup, admin_client):
    config, source, ad = group_setup
    ad.items[0]["dn"] = "CN=t0001919,OU=Existing,DC=example,DC=com"
    config.auto_onboard_accounts = False
    config.save()
    job = Job.objects.create(status="preview_ready")
    job.plan = sync.plan(job, source, ad)
    job.save()

    response = admin_client.get(f"/jobs/{job.pk}")

    assert response.status_code == 200
    body = response.content.decode()
    assert "部门 AD 全局安全组计划" in body
    assert "分组成员与子组嵌套计划" in body
    assert "自动纳管未开启" in body
    assert "不会创建、移动、更新或禁用人员账号" not in body

    organization = Job.objects.create(scope="organization", status="preview_ready")
    organization.plan = sync.plan(organization, source, ad)
    organization.save()
    response = admin_client.get(f"/jobs/{organization.pk}")
    assert response.status_code == 200
    assert "不会创建、移动、更新或禁用人员账号" in response.content.decode()


@pytest.mark.django_db
def test_audit_prewrite_failure_stops_group_creation(group_setup, monkeypatch):
    _, source, ad = group_setup
    job = Job.objects.create()
    job.plan = sync.plan(job, source, ad)
    original = sync.start_sync_audit

    def fail_group_audit(job, **kwargs):
        if kwargs["object_type"] == "group":
            raise RuntimeError("audit unavailable")
        return original(job, **kwargs)

    monkeypatch.setattr(sync, "start_sync_audit", fail_group_audit)
    with pytest.raises(RuntimeError, match="audit unavailable"):
        sync.apply(job, source, ad)
    assert ad.groups == {}
    assert not GroupBinding.objects.exists()


@pytest.mark.django_db
def test_audit_prewrite_failure_stops_group_membership_write(group_setup, monkeypatch):
    _, source, ad = group_setup
    job = Job.objects.create()
    job.plan = sync.plan(job, source, ad)
    original = sync.start_sync_audit

    def fail_membership_audit(job, **kwargs):
        if kwargs["object_type"] == "group_membership":
            raise RuntimeError("membership audit unavailable")
        return original(job, **kwargs)

    monkeypatch.setattr(sync, "start_sync_audit", fail_membership_audit)
    with pytest.raises(RuntimeError, match="membership audit unavailable"):
        sync.apply(job, source, ad)
    assert all(not group["members"] for group in ad.groups.values())


@pytest.mark.django_db
def test_group_audit_survives_job_cleanup(group_setup):
    _, source, ad = group_setup
    job = Job.objects.create()
    job.plan = sync.plan(job, source, ad)
    assert sync.apply(job, source, ad) == "success"
    event = SyncAuditEvent.objects.filter(object_type="group").first()
    identifier = event.job_identifier
    job.delete()
    event.refresh_from_db()
    assert event.job is None
    assert event.job_identifier == identifier
    assert event.after_values["group_type"] == 0x80000002


@pytest.mark.django_db
def test_sync_audit_page_exposes_structured_change_details(group_setup, admin_client):
    _, source, ad = group_setup
    job = Job.objects.create(actor="sync-admin")
    job.plan = sync.plan(job, source, ad)
    assert sync.apply(job, source, ad) == "success"

    response = admin_client.get("/logs/sync?q=公司&action=ensure_group")

    assert response.status_code == 200
    body = response.content.decode()
    assert "同步操作审计" in body
    assert "建立全局安全组" in body
    assert "sync-admin" in body
    assert "CN=公司" in body


@pytest.mark.django_db
def test_departments_page_shows_bound_global_security_group(group_setup, admin_client):
    _, source, ad = group_setup
    job = Job.objects.create(scope="organization")
    job.plan = sync.plan(job, source, ad)
    assert sync.apply(job, source, ad) == "success"

    response = admin_client.get("/departments")

    assert response.status_code == 200
    body = response.content.decode()
    assert "AD 全局安全组" in body
    assert "CN=研发" in body


def test_group_dn_requires_cn_below_managed_ou():
    assert validate_group_dn(f"CN=研发,{CHILD}", ROOT)[0][0].casefold() == "cn"
    with pytest.raises(RuleError, match="分组 DN"):
        validate_group_dn(f"OU=研发,{CHILD}", ROOT)
    with pytest.raises(RuleError, match="分组 DN"):
        validate_group_dn("CN=研发,OU=Outside,DC=example,DC=com", ROOT)


def test_active_directory_rejects_non_global_security_group(monkeypatch):
    ad = object.__new__(ActiveDirectory)
    ad.search = lambda *args, **kwargs: [{
        "dn": f"CN=研发,{CHILD}",
        "attributes": {
            "objectClass": ["top", "group"],
            "objectGUID": str(uuid.uuid4()),
            "groupType": 2,
            "sAMAccountName": "g-domain-local",
        },
    }]
    with pytest.raises(RuleError, match="全局安全组"):
        ad.group_identity(f"CN=研发,{CHILD}", ROOT)


def test_active_directory_creates_global_security_group(monkeypatch):
    ad = object.__new__(ActiveDirectory)
    ad.require_write = lambda: None
    ad.verify_ou = lambda dn: str(uuid.uuid4())
    created = {
        "guid": str(uuid.uuid4()),
        "dn": f"CN=研发,{CHILD}",
        "sam_account_name": "g-test",
        "group_type": 0x80000002,
        "members": [],
    }
    monkeypatch.setattr(ad, "group_identity", Mock(side_effect=[None, created]))
    ad.conn = SimpleNamespace(add=Mock(return_value=True), result={})

    result = ad.ensure_group(f"CN=研发,{CHILD}", "g-test", ROOT)

    assert result == created
    dn, object_classes, attrs = ad.conn.add.call_args.args
    assert dn == f"CN=研发,{CHILD}"
    assert object_classes == ["top", "group"]
    assert attrs == {"sAMAccountName": "g-test", "groupType": 0x80000002}


def test_active_directory_reuses_same_ou_global_group_regardless_of_sam(monkeypatch):
    ad = object.__new__(ActiveDirectory)
    ad.require_write = lambda: None
    existing = {
        "guid": str(uuid.uuid4()),
        "dn": f"CN=研发,{CHILD}",
        "sam_account_name": "existing-group",
        "group_type": 0x80000002,
        "members": [],
    }
    monkeypatch.setattr(ad, "group_identity", Mock(return_value=existing))
    ad.conn = SimpleNamespace(add=Mock(), result={})

    result = ad.ensure_group(f"CN=研发,{CHILD}", "expected-generated-name", ROOT)

    assert result == existing
    ad.conn.add.assert_not_called()


def test_active_directory_adds_group_member_once(monkeypatch):
    ad = object.__new__(ActiveDirectory)
    ad.require_write = lambda: None
    group_guid = str(uuid.uuid4())
    group_dn = f"CN=研发,{CHILD}"
    member_dn = f"CN=t0001919,{CHILD}"
    group = {
        "guid": group_guid,
        "dn": group_dn,
        "sam_account_name": "g-test",
        "group_type": 0x80000002,
        "members": [],
    }
    ad.search = lambda *args, **kwargs: [{"dn": group_dn}]
    monkeypatch.setattr(ad, "group_identity", lambda dn, root: copy.deepcopy(group))
    ad.conn = SimpleNamespace(modify=Mock(return_value=True), result={})

    result = ad.add_group_member(group_guid, member_dn, ROOT)

    assert result["guid"] == group_guid
    changes = ad.conn.modify.call_args.args[1]
    assert changes["member"][0][1] == [member_dn]
