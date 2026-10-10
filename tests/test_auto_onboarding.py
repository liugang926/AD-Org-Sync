"""Automation runs normal personnel changes; exceptions retain per-person evidence."""
import copy
import os
import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from sync_app import synchronization as sync
from sync_app.directory import ActiveDirectory, under
from sync_app.domain import RuleError, protected
from sync_app.models import Binding, Configuration, DepartmentBinding, Job, Operation, Person, RuntimeState
from .fakes import Directory, Source, account, user


ROOT = "OU=People,DC=example,DC=com"
OLD = "CN=Users,DC=example,DC=com"


class OnboardingDirectory(Directory):
    """Exercise the real onboarding method with a tracked LDAP ModifyDN."""
    onboard = ActiveDirectory.onboard
    require_write = ActiveDirectory.require_write

    def __init__(self, accounts):
        super().__init__(accounts)
        self.writes = []
        self.conn = SimpleNamespace(read_only=False, modify_dn=self.move)

    def set_read_only(self, value):
        self.conn.read_only = value

    def move(self, dn, rdn, *, new_superior):
        self.require_write()
        item = next(a for a in self.items if a["dn"] == dn)
        item["dn"] = rdn + "," + new_superior
        item["ad_revision"] = str(int(item["ad_revision"]) + 1)
        self.writes.append(("move", item["guid"]))
        return True

    def ensure_ou(self, dn, root, **kwargs):
        self.require_write()
        if dn.casefold() not in self.ous:
            self.writes.append(("ou", dn))
        return super().ensure_ou(dn, root, **kwargs)

    def update(self, guid, attrs, ou, root, **kwargs):
        self.require_write()
        current = self.by_guid(guid)
        assert under(current["dn"], root) and under(ou, root)
        assert not protected(current) and current["enabled"]
        result = super().update(guid, attrs, ou, root, **kwargs)
        if current != result:
            self.writes.append(("update", guid))
        return result


@pytest.fixture
def setup_onboarding(monkeypatch):
    config = Configuration.current()
    config.root_ou = ROOT
    config.identity_anchor = sync.directory_identity_anchor()
    config.auto_onboard_accounts = True
    config.attributes = ["displayName", "mail", "department"]
    config.save()
    target = account()
    target["dn"] = "CN=testuser," + OLD
    ad = OnboardingDirectory([target])
    source = Source()
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    return config, source, ad


def execute_scheduled():
    job = sync.enqueue(kind="scheduled")
    assert sync.run_next()
    job.refresh_from_db()
    return job


@pytest.mark.django_db
@pytest.mark.parametrize("bound", [False, True])
def test_read_only_preview_preserves_existing_identity(setup_onboarding, bound):
    _, _, ad = setup_onboarding
    if bound:
        person = Person.objects.create(source_id="u1", name="员工")
        Binding.objects.create(person=person, object_guid=ad.items[0]["guid"], username="testuser", sync_managed=False)
    before = copy.deepcopy(ad.items)
    job = sync.enqueue()
    assert sync.run_next()
    job.refresh_from_db()
    assert job.status == "preview_ready", job.message
    op = job.plan["operations"][0]
    assert op["action"] == "onboard"
    assert op["target"]["guid"] == before[0]["guid"] and op["username"] == "testuser"
    assert {"field": "OU", "before": OLD, "after": ROOT} in op["changes"]
    assert ad.conn.read_only and ad.writes == [] and ad.items == before
    assert not Binding.objects.filter(sync_managed=True).exists()
    assert Binding.objects.count() == int(bound)
    assert not Operation.objects.exists()


@pytest.mark.django_db
def test_scheduled_automatic_onboarding_is_idempotent_without_confirmation_or_batch_limit(setup_onboarding):
    config, source, ad = setup_onboarding
    source.users = [user(f"u{i}", str(1000 + i)) for i in range(6)]
    ad.items = [account(u["employee_id"], "existing" + u["source_id"]) for u in source.users]
    for item in ad.items:
        item["dn"] = "CN=" + item["username"] + "," + OLD
    old_identities = {(a["guid"], a["username"]) for a in ad.items}
    first = execute_scheduled()
    assert first.kind == "scheduled" and first.status == "success", first.message
    assert not first.confirmed and Binding.objects.filter(sync_managed=True).count() == 6
    assert {(str(b.object_guid), b.username) for b in Binding.objects.all()} == old_identities
    assert all(under(a["dn"], ROOT) and a["enabled"] for a in ad.items)
    assert ad.created == ad.resets == 0 and not ad.disabled
    for op in first.operation_set.filter(action="onboard"):
        assert op.status == "success" and "已纳管" in op.message
        assert op.evidence["before_dn"].endswith(OLD) and op.evidence["after_dn"].endswith(ROOT)
    writes = list(ad.writes)
    second = execute_scheduled()
    assert second.status == "success" and not second.operation_set.filter(action="onboard").exists()
    assert ad.writes == writes and Binding.objects.count() == 6
    assert Configuration.current().auto_onboard_accounts == config.auto_onboard_accounts


@pytest.mark.django_db
@pytest.mark.parametrize("mode", ["off", "protected", "disabled", "managed", "excluded", "no_revision", "outside_ldap"])
def test_ineligible_accounts_are_never_migrated(setup_onboarding, mode):
    config, _, ad = setup_onboarding
    if mode == "off":
        config.auto_onboard_accounts = False
        config.save()
    elif mode in {"protected", "disabled", "no_revision", "outside_ldap"}:
        key, value = {"protected": ("protected", True), "disabled": ("enabled", False),
                      "no_revision": ("ad_revision", ""), "outside_ldap": ("dn", "CN=testuser,DC=other,DC=com")}[mode]
        ad.items[0][key] = value
    else:
        person = Person.objects.create(source_id="u1", name="员工", excluded=mode == "excluded")
        if mode == "managed":
            Binding.objects.create(person=person, object_guid=ad.items[0]["guid"], username="testuser", sync_managed=True)
    before = copy.deepcopy(ad.items)
    job = execute_scheduled()
    assert job.plan["operations"][0]["action"] == ("skip" if mode == "excluded" else "conflict")
    assert not job.operation_set.filter(action="onboard").exists()
    assert ad.writes == [] and ad.items == before and ad.created == ad.resets == 0


@pytest.mark.django_db
def test_one_identity_conflict_does_not_block_other_people_in_scheduled_run(setup_onboarding):
    _, source, ad = setup_onboarding
    source.users.append(user("missing-id", ""))
    job = execute_scheduled()
    assert job.status == "partial_failed", job.message
    assert Binding.objects.get().person.source_id == "u1"
    failure = job.operation_set.get(source_id="missing-id")
    assert failure.action == "conflict" and failure.status == "failed" and failure.message
    assert job.operation_set.get(source_id="u1").status == "success"
    assert RuntimeState.current().last_full_success is None
    assert ad.created == ad.resets == 0


@pytest.mark.django_db
def test_manual_conflicted_plan_still_requires_resolution(setup_onboarding):
    _, source, ad = setup_onboarding
    source.users.append(user("missing-id", ""))
    job = sync.enqueue()
    sync.run_next()
    job.refresh_from_db()
    assert job.status == "blocked" and ad.writes == []
    with pytest.raises(RuleError):
        sync.queue_apply(job.pk, "admin")


@pytest.mark.django_db
@pytest.mark.parametrize("change", ["dn", "guid", "ad_revision", "protected", "enabled", "attrs", "policy", "source", "binding", "ou"])
def test_stale_plan_blocks_before_any_write(setup_onboarding, change):
    config, source, ad = setup_onboarding
    job = Job.objects.create(scope="users", selected=["u1"])
    job.plan = sync.plan(job, source, ad)
    if change == "policy":
        config.auto_onboard_accounts = False
        config.save()
    elif change == "source":
        source.users[0]["name"] = "已变更"
    elif change == "binding":
        Binding.objects.create(person=Person.objects.get(source_id="u1"), object_guid=ad.items[0]["guid"], username="testuser")
    elif change == "ou":
        ad.ous[ROOT.casefold()] = str(uuid.uuid4())
    else:
        ad.items[0][change] = {"dn": "CN=testuser,OU=Other,DC=example,DC=com", "guid": str(uuid.uuid4()),
                               "ad_revision": "2", "protected": True, "enabled": False, "attrs": {"mail": "changed"}}[change]
    before = copy.deepcopy(ad.items)
    with pytest.raises(RuleError):
        sync.apply(job, source, ad)
    assert ad.writes == [] and ad.items == before
    assert not Operation.objects.exists()


@pytest.mark.django_db
def test_retry_after_move_and_attribute_failure_keeps_same_account(setup_onboarding):
    _, _, ad = setup_onboarding
    identity = ad.items[0]["guid"]
    ad.fail_update.add(identity)
    first = execute_scheduled()
    assert first.status == "partial_failed"
    assert first.operation_set.get(action="onboard").status == "failed"
    assert not Binding.objects.exists() and under(ad.items[0]["dn"], ROOT)
    ad.fail_update.clear()
    second = execute_scheduled()
    assert second.status == "success"
    assert str(Binding.objects.get().object_guid) == identity
    assert ad.created == ad.resets == 0
    assert len([w for w in ad.writes if w[0] == "move"]) == 1


@pytest.mark.django_db
def test_department_conflict_blocks_all_scheduled_writes(setup_onboarding):
    _, _, ad = setup_onboarding
    DepartmentBinding.objects.create(source_id="1", dn=ROOT, object_guid=uuid.uuid4())
    job = execute_scheduled()
    assert job.status == "blocked" and not job.operation_set.exists()
    assert not ad.writes


@pytest.mark.django_db
def test_automatic_onboarding_does_not_bypass_disable_threshold(setup_onboarding):
    config, _, ad = setup_onboarding
    config.disable_missing = True
    config.save()
    absent = account("departed", "departed")
    ad.items.append(absent)
    Binding.objects.create(person=Person.objects.create(source_id="departed", name="离职"),
                           object_guid=absent["guid"], username=absent["username"])
    job = execute_scheduled()
    assert job.status == "needs_confirmation" and job.kind == "preview"
    assert not ad.writes and not ad.disabled


def test_ldap_onboarding_rejects_read_only_before_any_mutation():
    ad = object.__new__(ActiveDirectory)
    ad.conn = Mock(read_only=True)
    ad.by_guid = Mock()
    with pytest.raises(RuleError, match="只读"):
        ad.onboard(account(), {}, ROOT, ROOT, str(uuid.uuid4()))
    ad.by_guid.assert_not_called()
    ad.conn.modify_dn.assert_not_called()
    ad.conn.modify.assert_not_called()


@pytest.mark.parametrize("change", ["dn", "revision", "protected", "disabled", "target_ou", "ou_guid"])
def test_ldap_rechecks_policy_identity_and_destination_before_moving(change):
    target = account()
    target["dn"] = "CN=Last\\, First," + OLD
    current = copy.deepcopy(target)
    if change == "dn":
        current["dn"] = "CN=last," + ROOT
    elif change == "revision":
        current["ad_revision"] = "2"
    elif change == "protected":
        target["protected"] = current["protected"] = True
    elif change == "disabled":
        target["enabled"] = current["enabled"] = False
    ad = object.__new__(ActiveDirectory)
    ad.conn = Mock(read_only=False)
    ad.by_guid = Mock(return_value=current)
    ad.verify_ou = Mock(side_effect=RuleError("OU 对象已被替换") if change == "ou_guid" else None)
    with pytest.raises(RuleError):
        ad.onboard(target, {}, "OU=Outside,DC=example,DC=com" if change == "target_ou" else ROOT, ROOT, str(uuid.uuid4()))
    ad.conn.modify_dn.assert_not_called()
    ad.conn.modify.assert_not_called()


@pytest.mark.django_db
def test_escaped_rdn_survives_migration_and_failed_modify_dn_does_not_update_attrs(setup_onboarding):
    _, _, ad = setup_onboarding
    ad.items[0]["dn"] = "CN=Last\\, First," + OLD
    expected = ad.by_guid(ad.items[0]["guid"])
    ad.conn.modify_dn = Mock(return_value=False)
    with pytest.raises(RuleError, match="迁入失败"):
        ad.onboard(expected, {"displayName": "new"}, ROOT, ROOT, ad.ou_identity(ROOT))
    assert ad.writes == [] and ad.items[0] == expected
    ad.conn.modify_dn = ad.move
    result = ad.onboard(expected, {}, ROOT, ROOT, ad.ou_identity(ROOT))
    assert result["dn"] == "CN=Last\\, First," + ROOT


@pytest.mark.django_db
def test_admin_policy_and_preview_expose_automatic_onboarding(admin_client, setup_onboarding):
    config, source, ad = setup_onboarding
    response = admin_client.get("/admin/sync_app/configuration/1/change/")
    assert "auto_onboard_accounts" in response.context["adminform"].form.fields
    assert "定时同步无需逐人确认" in response.content.decode()
    job = Job.objects.create()
    job.plan = sync.plan(job, source, ad)
    job.save()
    response = admin_client.get(f"/jobs/{job.pk}")
    html = response.content.decode()
    assert "纳管并迁入 OU" in html and "当前 OU：" in html and "目标 OU：" in html
    assert OLD in html and ROOT in html and ad.items[0]["guid"] in html
    assert "每批" not in html and "逐人审批" not in html
    assert not ad.writes and config.auto_onboard_accounts


@pytest.mark.django_db
def test_onboarding_is_opt_in_configuration():
    assert not Configuration.current().auto_onboard_accounts


@pytest.mark.django_db
def test_save_automation_once_without_additional_approval(admin_client, setup_onboarding):
    from .test_sync_match_settings import configuration_payload

    config, _, ad = setup_onboarding
    config.auto_onboard_accounts = False
    config.save()
    payload = {**configuration_payload(config), "auto_onboard_accounts": "on", "schedule_enabled": "on"}
    response = admin_client.post("/admin/sync_app/configuration/1/change/", payload)
    assert response.status_code == 302
    config.refresh_from_db()
    assert config.auto_onboard_accounts and config.schedule_enabled
    assert ad.writes == []  # Saving policy does not execute AD changes.
    job = execute_scheduled()
    assert job.status == "success" and not job.confirmed


def test_migration_preserves_config_and_supports_old_image_inserts(tmp_path):
    env = os.environ.copy()
    env.update(AD_ORG_SYNC_DATA_DIR=str(tmp_path), DJANGO_SETTINGS_MODULE="sync_app.settings")
    code = '''
import django, uuid
django.setup()
from django.core.management import call_command
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
call_command("migrate", "sync_app", "0014_default_account_associations", interactive=False, verbosity=0)
apps = MigrationExecutor(connection).loader.project_state([("sync_app", "0014_default_account_associations")]).apps
OldConfig = apps.get_model("sync_app", "Configuration")
Person = apps.get_model("sync_app", "Person")
Binding = apps.get_model("sync_app", "Binding")
OldConfig.objects.create(pk=1, root_ou="OU=Kept,DC=example,DC=com", schedule_enabled=True,
                         auto_associate_accounts=True, sspr_enabled=True, identity_anchor="kept")
person = Person.objects.create(source_id="kept", name="Kept")
Binding.objects.create(person=person, username="existing", object_guid=uuid.uuid4(), sync_managed=False, enabled=False)
before = OldConfig.objects.values().get(pk=1)
bindings = list(Binding.objects.values())
call_command("migrate", interactive=False, verbosity=0)
from sync_app.models import Configuration
after = Configuration.objects.values().get(pk=1)
assert after.pop("auto_onboard_accounts") is False
assert after == before and list(Binding.objects.values()) == bindings
OldConfig.objects.filter(pk=1).delete()
OldConfig.objects.create(pk=1)
assert Configuration.objects.get(pk=1).auto_onboard_accounts is False
call_command("db_check", verbosity=0)
'''
    result = subprocess.run([sys.executable, "-c", code], cwd=Path(__file__).resolve().parents[1],
                            env=env, capture_output=True, text=True, timeout=40)
    assert result.returncode == 0, result.stderr

