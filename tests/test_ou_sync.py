"""OU regressions use a directory that rejects missing parents and all user writes."""
import copy
import uuid
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from ldap3.utils.dn import escape_rdn

from sync_app import synchronization as sync
from sync_app.directory import ActiveDirectory
from sync_app.domain import RuleError
from sync_app.models import Binding, Configuration, DepartmentBinding, Job, Operation, RuntimeState
from .fakes import Directory, Source

BASE = "DC=example,DC=com"
ROOT = "OU=公司," + BASE


class TreeSource(Source):
    def __init__(self):
        super().__init__()
        self.departments = [
            {"id": "1", "name": "公司", "parent": "0"},
            {"id": "9", "name": "研发", "parent": "1"},
            {"id": "2", "name": "平台, +\\组", "parent": "9"},
            {"id": "3", "name": "空部门", "parent": "1"},
        ]
        self.users[0].update(employee_id="", primary_department="", departments=["2", "3"])

    def collect(self, root):
        return copy.deepcopy(self.users), copy.deepcopy(self.departments)


class StrictDirectory(Directory):
    def __init__(self, root=None):
        super().__init__([])
        self.ous = {root.casefold(): str(uuid.uuid4())} if root else {}
        self.containers = {BASE.casefold(): str(uuid.uuid4())}
        self.writes = []
        self.read_only = False

    def set_read_only(self, value):
        self.read_only = value

    def container_identity(self, dn):
        identity = self.containers.get(dn.casefold()) or self.ous.get(dn.casefold())
        if not identity:
            raise RuleError("根 OU 的父容器不存在")
        return identity

    def ensure_ou(self, dn, root, *, allow_root_creation=False):
        assert not self.read_only, "Dry Run attempted an AD write"
        if dn.casefold() not in self.ous:
            assert self.container_identity(sync.parent_dn(dn))
            assert dn.casefold() != root.casefold() or allow_root_creation
            self.ous[dn.casefold()] = str(uuid.uuid4())
            self.writes.append(dn)
        return self.ous[dn.casefold()]

    def accounts(self):
        raise AssertionError("Organization-only synchronization must not read/match AD users")

    def create(self, *args, **kwargs):
        raise AssertionError("Organization-only synchronization attempted a user write")

    update = disable = enable = create


@pytest.mark.django_db
@pytest.mark.parametrize("configured_root", ["", ROOT])
def test_root_creation_is_previewed_then_applied_parent_first(configured_root, monkeypatch):
    config = Configuration.current()
    config.root_ou = configured_root
    config.disable_missing = True
    config.save()
    source, ad = TreeSource(), StrictDirectory()
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    job = sync.enqueue(scope="organization")
    assert sync.run_next()
    job.refresh_from_db()
    assert job.status == "preview_ready", job.message
    assert job.plan["root_ou"] == ROOT
    assert job.plan["operations"] == [] and not job.plan["high_risk"]
    assert ad.read_only and ad.writes == [] and ad.ous == {}
    assert not DepartmentBinding.objects.exists() and not Operation.objects.exists()
    paths = {item["source_id"]: item["dn"] for item in job.plan["departments"]}
    assert paths["1"] == ROOT
    assert paths["9"] == "OU=研发," + ROOT
    assert paths["2"] == "OU=" + escape_rdn("平台, +\\组") + "," + paths["9"]
    sync.queue_apply(job.pk, "admin")
    assert sync.run_next()
    job.refresh_from_db()
    assert job.status == "success", job.message
    assert ad.writes[0] == ROOT
    assert ad.writes.index(paths["9"]) < ad.writes.index(paths["2"])
    assert len(ad.writes) == DepartmentBinding.objects.count() == 4
    assert not Binding.objects.exists() and RuntimeState.current().last_full_success is None
    repeated = Job.objects.create(scope="organization")
    repeated.plan = sync.plan(repeated, source, ad)
    assert sync.apply(repeated, source, ad) == "success"
    assert len(ad.writes) == 4


@pytest.mark.django_db
def test_explicit_test_root_is_preserved():
    source = TreeSource()
    config = Configuration.current()
    config.root_ou = "OU=ADOrgSync-SyncTest-8fa5fcdf," + BASE
    config.save()
    ad = StrictDirectory(config.root_ou)
    result = sync.plan(Job.objects.create(scope="organization"), source, ad)
    assert result["root_ou"] == config.root_ou
    assert result["departments"][0]["guid"] == ad.verify_ou(config.root_ou)
    assert not ad.writes


@pytest.mark.parametrize("manual_dn", ["OU=Outside," + BASE, "CN=Wrong," + ROOT])
def test_manual_ancestor_cannot_redirect_descendants_outside_valid_ou_path(manual_dn):
    source = TreeSource()
    tree = {d["id"]: d for d in source.departments}
    with pytest.raises(RuleError):
        sync.department_dn("2", tree, SimpleNamespace(root_ou=ROOT, root_department="1"),
                           {"9": SimpleNamespace(manual=True, dn=manual_dn)})


def test_mapping_cannot_hide_missing_ancestry_or_retarget_source_root():
    tree = {d["id"]: d for d in TreeSource().departments}
    config = SimpleNamespace(root_ou=ROOT, root_department="1")
    with pytest.raises(RuleError):
        sync.department_dn("1", tree, config, {"1": SimpleNamespace(manual=True, dn="OU=Other," + ROOT)})
    tree["9"]["parent"] = "missing"
    with pytest.raises(RuleError):
        sync.department_dn("2", tree, config, {"9": SimpleNamespace(manual=True, dn="OU=Mapped," + ROOT)})


@pytest.mark.django_db
def test_readonly_directory_rejects_every_write_before_sending_ldap():
    directory = object.__new__(ActiveDirectory)
    directory.conn = Mock(read_only=False)
    directory.set_read_only(True)
    calls = [
        lambda: directory.ensure_ou(ROOT, ROOT, allow_root_creation=True),
        lambda: directory.create({}, "name", ROOT, ROOT),
        lambda: directory.update("guid", {}, ROOT, ROOT),
        lambda: directory.disable("guid", ROOT),
        lambda: directory.enable("guid", ROOT),
        lambda: directory.reset_password("guid", "unused"),
    ]
    for call in calls:
        with pytest.raises(RuleError, match="只读"):
            call()
    assert not directory.conn.method_calls


@pytest.mark.django_db
@pytest.mark.parametrize("change", ["root_appeared", "parent_replaced", "source_renamed"])
def test_root_preview_drift_blocks_before_any_write(change):
    source, ad = TreeSource(), StrictDirectory()
    job = Job.objects.create(scope="organization")
    job.plan = sync.plan(job, source, ad)
    if change == "root_appeared":
        ad.ous[ROOT.casefold()] = str(uuid.uuid4())
    elif change == "parent_replaced":
        ad.containers[BASE.casefold()] = str(uuid.uuid4())
    else:
        source.departments[0]["name"] = "另一公司"
    with pytest.raises(RuleError):
        sync.apply(job, source, ad)
    assert ad.writes == [] and not DepartmentBinding.objects.exists()


@pytest.mark.django_db
@pytest.mark.parametrize("base_is_root", [False, True])
def test_automatic_root_reuses_exact_existing_ou_without_duplicate_company(base_is_root, settings):
    if base_is_root:
        settings.LDAP_BASE_DN = ROOT
    ad = StrictDirectory(ROOT)
    result = sync.plan(Job.objects.create(scope="organization"), TreeSource(), ad)
    assert result["root_ou"] == ROOT and result["root_parent"] is None
    assert result["departments"][0]["guid"] == ad.verify_ou(ROOT)


@pytest.mark.django_db
@pytest.mark.parametrize("root", [BASE, "CN=Users," + BASE, "OU=Bad+CN=Compound," + BASE, "OU=Else,DC=other,DC=com"])
def test_invalid_or_outside_configured_root_never_becomes_creation_plan(root):
    config = Configuration.current()
    config.root_ou = root
    config.save()
    ad = StrictDirectory()
    with pytest.raises(RuleError):
        sync.plan(Job.objects.create(scope="organization"), TreeSource(), ad)
    assert not ad.writes


@pytest.mark.django_db
def test_missing_parent_of_explicit_root_does_not_create_ancestors_outside_root():
    config = Configuration.current()
    config.root_ou = "OU=Company,OU=Missing," + BASE
    config.save()
    ad = StrictDirectory()
    with pytest.raises(RuleError, match="父容器"):
        sync.plan(Job.objects.create(scope="organization"), TreeSource(), ad)
    assert not ad.writes


@pytest.mark.django_db
def test_scheduled_root_creation_requires_review(monkeypatch):
    source, ad = Source(), Directory([])
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    job = sync.enqueue(kind="scheduled")
    assert sync.run_next()
    job.refresh_from_db()
    assert job.kind == "preview" and job.status == "needs_confirmation"
    assert ROOT.casefold() not in ad.ous and ad.read_only
    assert not Operation.objects.exists() and not Binding.objects.exists()


def test_manual_mapping_inherits_full_path_and_escapes_only_source_names():
    source = TreeSource()
    tree = {d["id"]: d for d in source.departments}
    mapped = "OU=部门别名,OU=业务总组," + ROOT
    dn = sync.department_dn("2", tree, SimpleNamespace(root_ou=ROOT, root_department="1"),
                            {"9": SimpleNamespace(manual=True, dn=mapped)})
    assert dn == "OU=" + escape_rdn(tree["2"]["name"]) + "," + mapped


@pytest.mark.django_db
def test_renamed_department_blocks_organization_apply_without_deleting_old_ou():
    source, ad = TreeSource(), StrictDirectory(ROOT)
    first = Job.objects.create(scope="organization")
    first.plan = sync.plan(first, source, ad)
    assert sync.apply(first, source, ad) == "success"
    previous = dict(ad.ous)
    source.departments[1]["name"] = "改名研发"
    changed = Job.objects.create(scope="organization")
    changed.plan = sync.plan(changed, source, ad)
    assert sync.has_conflicts(changed.plan)
    with pytest.raises(RuleError, match="冲突"):
        sync.apply(changed, source, ad)
    assert ad.ous == previous


@pytest.mark.django_db
def test_real_ldap_ou_creation_checks_parent_order_and_keeps_exact_escaped_dn():
    directory = object.__new__(ActiveDirectory)
    directory.conn = Mock(read_only=False)
    nodes = {BASE: str(uuid.uuid4())}
    writes = []

    def search(query, base=None, **kwargs):
        if base not in nodes:
            directory.conn.result = {"result": 32}
            raise RuleError("AD 查询未完整成功")
        directory.conn.result = {"result": 0}
        return [{"dn": base, "attributes": {"objectGUID": nodes[base]}}]

    def add(dn, classes):
        assert sync.parent_dn(dn) in nodes
        assert dn not in nodes and classes == ["top", "organizationalUnit"]
        nodes[dn] = str(uuid.uuid4())
        writes.append(dn)
        return True

    directory.search = search
    directory.conn.add.side_effect = add
    child = "OU=" + escape_rdn(" 空, +\\组 ") + ",OU=研发," + ROOT
    with pytest.raises(RuleError, match="根 OU 不存在"):
        directory.ensure_ou(child, ROOT)
    assert not writes
    directory.ensure_ou(ROOT, ROOT, allow_root_creation=True)
    result = directory.ensure_ou(child, ROOT)
    assert writes == [ROOT, "OU=研发," + ROOT, child] and result == nodes[child]
    assert directory.ensure_ou(child, ROOT) == result
    assert len(writes) == 3


@pytest.mark.parametrize("code", [50, 52, 81])
def test_ldap_ou_read_failure_is_never_interpreted_as_missing(code):
    directory = object.__new__(ActiveDirectory)
    directory.conn = Mock(read_only=False, result={"result": code})
    directory.search = Mock(side_effect=RuleError("AD 查询未完整成功"))
    with pytest.raises(RuleError):
        directory.ensure_ou(ROOT, ROOT, allow_root_creation=True)
    directory.conn.add.assert_not_called()
