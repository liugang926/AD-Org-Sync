"""Identity association persists verified GUIDs without granting AD write control."""
import copy
from collections import Counter
from datetime import timedelta
import os
from pathlib import Path
import subprocess
import sys
import uuid
from types import SimpleNamespace

from django.core.exceptions import ValidationError
from django.utils import timezone
import pytest

from sync_app import synchronization as sync
from sync_app.domain import RuleError, resolve
from sync_app.models import Audit, Binding, Configuration, Job, Operation, Person, Snapshot
from .fakes import Directory, Source, account, user


pytestmark = pytest.mark.django_db


class ReadOnlyDirectory(Directory):
    """Every AD mutator fails so a locally successful binding cannot hide a write."""

    def ensure_ou(self, *args, **kwargs):
        pytest.fail("Identity association must not create an OU")

    def create(self, *args, **kwargs):
        pytest.fail("Identity association must not create an AD account")

    def update(self, *args, **kwargs):
        pytest.fail("Identity association must not update or move an AD account")

    def enable(self, *args, **kwargs):
        pytest.fail("Identity association must not enable an AD account")

    def disable(self, *args, **kwargs):
        pytest.fail("Identity association must not disable an AD account")

    def reset_password(self, *args, **kwargs):
        pytest.fail("Identity association must not reset an AD password")


@pytest.fixture
def association_config():
    config = Configuration.current()
    config.identity_anchor = sync.directory_identity_anchor()
    config.match_field = "employee_username"
    config.root_ou = "OU=People,DC=example,DC=com"
    config.save()
    return config


def run_association(monkeypatch, source, directory):
    from sync_app import account_associations as associations

    for module in (sync, associations):
        monkeypatch.setattr(module, "DingTalk", lambda: source)
        monkeypatch.setattr(module, "ActiveDirectory", lambda: directory)
    job = sync.enqueue(kind="associate", actor="association-admin")
    before = copy.deepcopy(directory.items)
    assert sync.run_next()
    job.refresh_from_db()
    assert directory.items == before
    assert directory.created == directory.resets == 0
    assert directory.disabled == []
    return job


def save_source(config, users=None):
    return sync.collect(Source(users), config)


def test_unique_employee_to_sam_persists_real_guid_and_audit(association_config, monkeypatch):
    target = account(employee="unrelated", name="t0001919")
    job = run_association(monkeypatch, Source([user(employee=" T0001919 ")]), ReadOnlyDirectory([target]))
    assert job.status == "success"
    binding = Binding.objects.get(person__source_id="u1")
    assert str(binding.object_guid) == target["guid"]
    assert binding.username == "t0001919"
    # Legacy workers interpret enabled=False as paused, so a rollback cannot
    # accidentally manage a newly associated identity.
    assert not binding.enabled and not binding.manual and not binding.sync_managed
    assert Snapshot.objects.count() == 1
    assert Person.objects.get(source_id="u1").name == "测试员工"
    assert Audit.objects.filter(action="auto_associate", target="u1", success=True).exists()


def test_association_sets_ldap_connection_read_only_before_search(association_config, monkeypatch):
    directory = ReadOnlyDirectory([account(name="1001")])
    directory.conn = SimpleNamespace(read_only=False)
    read_accounts = directory.accounts

    def assert_read_only_search():
        assert directory.conn.read_only is True
        return read_accounts()

    monkeypatch.setattr(directory, "accounts", assert_read_only_search)
    job = run_association(monkeypatch, Source(), directory)
    assert job.status == "success" and Binding.objects.count() == 1


def test_identity_association_works_before_a_sync_root_is_chosen(association_config, monkeypatch):
    association_config.root_ou = ""
    association_config.save()
    target = account(name="1001")
    job = run_association(monkeypatch, Source(), ReadOnlyDirectory([target]))
    assert job.status == "success"
    assert str(Binding.objects.get().object_guid) == target["guid"]
    assert not Binding.objects.get().sync_managed


@pytest.mark.parametrize("state", ["outside_root", "protected", "disabled"])
def test_identity_association_does_not_depend_on_sync_eligibility(association_config, monkeypatch, state):
    target = account(name="1001")
    if state == "outside_root":
        target["dn"] = "CN=1001,OU=Existing,DC=example,DC=com"
    elif state == "protected":
        target.update(protected=True, domain_admin=True)
    else:
        target["enabled"] = False
    job = run_association(monkeypatch, Source(), ReadOnlyDirectory([target]))
    assert job.status == "success"
    binding = Binding.objects.get()
    assert str(binding.object_guid) == target["guid"]
    assert not binding.enabled
    assert job.plan["operations"][0]["target"]["enabled"] is target["enabled"]
    assert not binding.sync_managed
    association_config.refresh_from_db()
    assert association_config.root_ou == "OU=People,DC=example,DC=com"


@pytest.mark.parametrize("employee,target_name", [
    ("T-000@1919", "T-0001919"),
    ("12345678901234567890-extra", "12345678901234567890"),
    ("T0001919", "T00019190"),
])
def test_matching_never_cleans_truncates_or_prefix_matches_job_number(association_config, monkeypatch, employee, target_name):
    association_config.naming = "source_id"
    association_config.save()
    run_association(monkeypatch, Source([user(employee=employee)]), ReadOnlyDirectory([account(name=target_name)]))
    assert not Binding.objects.exists()


@pytest.mark.parametrize("problem", ["missing_job", "duplicate_source_jobs", "duplicate_ad_sam", "occupied_guid", "excluded"])
def test_ambiguous_missing_occupied_or_excluded_identity_is_not_claimed(association_config, monkeypatch, problem):
    users, targets = [user()], [account(name="1001")]
    if problem == "missing_job":
        users[0]["employee_id"] = "   "
    elif problem == "duplicate_source_jobs":
        users.append(user("u2", " 1001 "))
    elif problem == "duplicate_ad_sam":
        targets.append(account(name="1001"))
    elif problem == "occupied_guid":
        owner = Person.objects.create(source_id="other", name="Existing owner")
        Binding.objects.create(person=owner, object_guid=targets[0]["guid"], username="1001", sync_managed=False)
    else:
        Person.objects.create(source_id="u1", name="Excluded employee", excluded=True)
    run_association(monkeypatch, Source(users), ReadOnlyDirectory(targets))
    assert not Binding.objects.filter(person__source_id__in=["u1", "u2"]).exists()


@pytest.mark.parametrize("manual,enabled", [(True, True), (True, False), (False, True), (False, False)])
def test_existing_binding_is_not_reassigned_or_reenabled(association_config, monkeypatch, manual, enabled):
    person = Person.objects.create(source_id="u1", name="Previous employee")
    old_target, new_target = account(name="old-account"), account(name="1001")
    binding = Binding.objects.create(person=person, object_guid=old_target["guid"], username="old-account", manual=manual, enabled=enabled, sync_managed=False)
    before = Binding.objects.filter(pk=binding.pk).values().get()
    run_association(monkeypatch, Source(), ReadOnlyDirectory([old_target, new_target]))
    assert Binding.objects.filter(pk=binding.pk).values().get() == before


@pytest.mark.parametrize("change", ["guid", "username", "missing"])
def test_target_is_rechecked_by_guid_before_binding(association_config, monkeypatch, change):
    target = account(name="1001")
    directory = ReadOnlyDirectory([target])
    original = directory.by_guid

    def changed(guid):
        if change == "missing":
            raise RuleError("对象不存在")
        current = original(guid)
        current[change] = str(uuid.uuid4()) if change == "guid" else "replacement"
        return current

    monkeypatch.setattr(directory, "by_guid", changed)
    run_association(monkeypatch, Source(), directory)
    assert not Binding.objects.exists()


@pytest.mark.parametrize("change", ["employee_id", "source_id", "departments"])
def test_employee_identity_and_scope_are_rechecked_before_binding(association_config, monkeypatch, change):
    source = Source()

    def changed(uid):
        current = copy.deepcopy(source.users[0])
        current[change] = {"employee_id": "1002", "source_id": "impostor", "departments": ["999"]}[change]
        return current

    monkeypatch.setattr(source, "user", changed)
    run_association(monkeypatch, source, ReadOnlyDirectory([account(name="1001")]))
    assert not Binding.objects.exists()


@pytest.mark.parametrize("same_source", [True, False])
def test_unbound_creation_recovery_reserves_source_and_target_guid(association_config, monkeypatch, same_source):
    target = account(name="1001")
    previous = Job.objects.create(kind="apply", status="partial_failed")
    Operation.objects.create(job=previous, source_id="u1" if same_source else "other", action="create", status="failed", target_guid=target["guid"] if not same_source else None)
    run_association(monkeypatch, Source(), ReadOnlyDirectory([target]))
    assert not Binding.objects.exists()


def test_binding_and_its_audit_commit_atomically(association_config, monkeypatch):
    from sync_app import account_associations as associations
    real_audit = associations.audit

    def fail_association_audit(*args, **kwargs):
        if len(args) > 1 and args[1] == "auto_associate":
            raise RuntimeError("audit storage unavailable")
        return real_audit(*args, **kwargs)

    monkeypatch.setattr(associations, "audit", fail_association_audit)
    job = run_association(monkeypatch, Source(), ReadOnlyDirectory([account(name="1001")]))
    assert job.status != "success"
    assert not Binding.objects.exists()
    assert not Audit.objects.filter(action="auto_associate", success=True).exists()


def test_incomplete_source_collection_is_not_saved_or_associated(association_config, monkeypatch):
    job = run_association(monkeypatch, Source([]), ReadOnlyDirectory([account(name="1001")]))
    assert job.status == "failed"
    assert not Binding.objects.exists() and not Snapshot.objects.exists() and not Person.objects.exists()


def test_incomplete_ad_search_never_associates_partial_results(association_config, monkeypatch):
    directory = ReadOnlyDirectory([account(name="1001")])

    def interrupted_search():
        raise RuleError("AD 查询未完整成功")

    monkeypatch.setattr(directory, "accounts", interrupted_search)
    job = run_association(monkeypatch, Source(), directory)
    assert job.status == "failed"
    assert not Binding.objects.exists()


def test_identity_only_binding_does_not_lock_organization_ou_change(association_config):
    person = Person.objects.create(source_id="u1", name="Associated employee")
    Binding.objects.create(person=person, object_guid=uuid.uuid4(), username="1001", sync_managed=False)
    association_config.root_ou = "OU=Other,DC=example,DC=com"
    association_config.clean()
    association_config.save()
    Binding.objects.filter(person=person).update(sync_managed=True)
    association_config.root_ou = "OU=Third,DC=example,DC=com"
    with pytest.raises(ValidationError, match="已有同步绑定"):
        association_config.clean()


def test_identity_only_missing_employee_is_not_disabled_by_sync(association_config):
    association_config.disable_missing = True
    association_config.save()
    person = Person.objects.create(source_id="gone", name="Identity-only employee")
    target = account(name="departed")
    Binding.objects.create(person=person, object_guid=target["guid"], username=target["username"], enabled=False, sync_managed=False)
    preview = sync.plan(Job.objects.create(), Source(), ReadOnlyDirectory([target]))
    assert not any(item["action"] == "disable" for item in preview["operations"])


def test_refresh_remains_source_only_without_creating_binding(association_config, monkeypatch):
    monkeypatch.setattr(sync, "DingTalk", lambda: Source())

    def no_ad_connection():
        pytest.fail("Refreshing source personnel must not connect to AD")

    monkeypatch.setattr(sync, "ActiveDirectory", no_ad_connection)
    job = sync.enqueue(kind="refresh")
    assert sync.run_next()
    job.refresh_from_db()
    assert job.status == "success" and Snapshot.objects.count() == 1
    assert not Binding.objects.exists()


def test_preview_does_not_persist_identity_association(association_config):
    preview = sync.plan(Job.objects.create(), Source(), ReadOnlyDirectory([account(name="1001")]))
    assert preview["operations"][0]["action"] == "bind"
    assert not Binding.objects.exists()


def test_administrator_can_override_auto_association_and_future_runs_preserve_it(association_config, monkeypatch):
    auto_target, manual_target = account(name="1001"), account(name="other-login")
    source, directory = Source(), ReadOnlyDirectory([auto_target, manual_target])
    run_association(monkeypatch, source, directory)
    person = Person.objects.get(source_id="u1")
    reviewed = sync.binding_review(person.pk, "other-login")
    sync.bind_person(person.pk, reviewed["confirmation"], "admin", "Approved account correction")
    binding = Binding.objects.get(person=person)
    assert str(binding.object_guid) == manual_target["guid"] and binding.manual
    assert not binding.sync_managed and not binding.enabled
    run_association(monkeypatch, source, directory)
    binding.refresh_from_db()
    assert str(binding.object_guid) == manual_target["guid"] and binding.manual


def test_manual_identity_override_can_associate_outside_sync_root_without_promoting(association_config, monkeypatch):
    person = Person.objects.create(source_id="u1", name="Employee")
    target = account(name="existing-login")
    target["dn"] = "CN=existing-login,OU=Existing,DC=example,DC=com"
    source, directory = Source(), ReadOnlyDirectory([target])
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: directory)
    reviewed = sync.binding_review(person.pk, target["username"])
    sync.bind_person(person.pk, reviewed["confirmation"], "admin", "Reviewed existing login")
    binding = Binding.objects.get(person=person)
    assert str(binding.object_guid) == target["guid"] and binding.manual
    assert not binding.sync_managed and not binding.enabled


def test_manual_identity_review_rejects_target_outside_ldap_directory(association_config, monkeypatch):
    person = Person.objects.create(source_id="u1", name="Employee")
    target = account(name="other-directory")
    target["dn"] = "CN=other-directory,OU=People,DC=other,DC=com"
    monkeypatch.setattr(sync, "DingTalk", lambda: Source())
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ReadOnlyDirectory([target]))
    with pytest.raises(RuleError):
        sync.binding_review(person.pk, target["username"])
    assert not Binding.objects.exists()


def test_manual_identity_confirmation_rejects_guid_swap(association_config, monkeypatch):
    person = Person.objects.create(source_id="u1", name="Employee")
    target = account(name="1001")
    directory = ReadOnlyDirectory([target])
    monkeypatch.setattr(sync, "DingTalk", lambda: Source())
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: directory)
    reviewed = sync.binding_review(person.pk, target["username"])
    directory.items[0]["guid"] = str(uuid.uuid4())
    with pytest.raises(RuleError, match="目标已变化"):
        sync.bind_person(person.pk, reviewed["confirmation"], "admin", "Should not authorize replacement")
    assert not Binding.objects.exists()


def test_confirming_same_guid_preserves_existing_managed_scope(association_config, monkeypatch):
    person = Person.objects.create(source_id="u1", name="Employee")
    target = account(name="1001")
    binding = Binding.objects.create(person=person, object_guid=target["guid"], username="1001", sync_managed=True)
    monkeypatch.setattr(sync, "DingTalk", lambda: Source())
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ReadOnlyDirectory([target]))
    reviewed = sync.binding_review(person.pk, target["username"])
    sync.bind_person(person.pk, reviewed["confirmation"], "admin", "Confirm account")
    binding.refresh_from_db()
    assert binding.manual and binding.sync_managed and binding.enabled


def test_identity_only_binding_cannot_enable_account_without_sync_preview(association_config, monkeypatch):
    person = Person.objects.create(source_id="u1", name="Employee")
    target = account(name="1001")
    target["enabled"] = False
    Binding.objects.create(person=person, object_guid=target["guid"], username="1001", enabled=False, sync_managed=False)
    monkeypatch.setattr(sync, "DingTalk", lambda: Source())
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ReadOnlyDirectory([target]))
    with pytest.raises(RuleError):
        sync.reactivate_person(person.pk, "admin", "Requires managed authorization", True)
    assert not Binding.objects.get().enabled


def test_successful_confirmed_sync_promotes_identity_association_to_management(association_config, monkeypatch):
    source, directory = Source(), ReadOnlyDirectory([account(name="1001")])
    run_association(monkeypatch, source, directory)
    assert not Binding.objects.get().sync_managed and not Binding.objects.get().enabled
    writable_directory = Directory(copy.deepcopy(directory.items))
    job = Job.objects.create()
    job.plan = sync.plan(job, source, writable_directory)
    assert not Binding.objects.get().sync_managed
    assert job.plan["operations"][0]["action"] == "update"
    assert sync.apply(job, source, writable_directory) == "success"
    assert Binding.objects.get().sync_managed and Binding.objects.get().enabled


@pytest.mark.parametrize("target_state,expected", [("enabled", "conflict"), ("disabled", "skip"), ("missing", "skip")])
def test_identity_only_binding_is_paused_for_legacy_resolver(association_config, monkeypatch, target_state, expected):
    """The unchanged legacy resolver must not update identity-only rows."""
    target = account(name="1001")
    run_association(monkeypatch, Source(), ReadOnlyDirectory([target]))
    binding = Binding.objects.get()
    if target_state == "disabled":
        target["enabled"] = False
    result = resolve(
        user(), {"guid": str(binding.object_guid), "enabled": binding.enabled},
        [] if target_state == "missing" else [target], set(), "employee_id",
        Counter({"1001": 1}), Counter({"1001": 1}), "employee_username",
    )
    assert result[0] == expected
    assert result[0] not in {"create", "bind", "update", "move", "disable", "resume_create"}


def test_identity_association_does_not_bypass_actual_disabled_ad_state_in_preview(association_config, monkeypatch):
    target = account(name="1001")
    target["enabled"] = False
    source, directory = Source(), ReadOnlyDirectory([target])
    run_association(monkeypatch, source, directory)
    assert not Binding.objects.get().enabled
    job = Job.objects.create()
    job.plan = sync.plan(job, source, directory)
    assert job.plan["operations"][0]["action"] == "conflict"
    with pytest.raises(RuleError, match="冲突"):
        sync.apply(job, source, directory)
    assert not Binding.objects.get().sync_managed and not Binding.objects.get().enabled


def test_first_source_snapshot_queues_one_background_association(association_config):
    from sync_app.account_associations import enqueue_due_association

    enqueue_due_association()
    assert not Job.objects.exists()
    save_source(association_config)
    enqueue_due_association()
    job = Job.objects.get()
    assert (job.kind, job.status, job.scope) == ("associate", "queued", "full")
    enqueue_due_association()
    assert Job.objects.count() == 1
    assert not Binding.objects.exists()


@pytest.mark.parametrize("kind,status", [("preview", "queued"), ("apply", "running"), ("refresh", "queued")])
def test_background_association_waits_for_existing_worker_job(association_config, kind, status):
    from sync_app.account_associations import enqueue_due_association

    save_source(association_config)
    Job.objects.create(kind=kind, status=status)
    enqueue_due_association()
    assert Job.objects.count() == 1


@pytest.mark.parametrize("mode,enabled", [("employee_id", True), ("source_id", True), ("email", True), ("employee_username", False)])
def test_background_association_respects_setting_and_match_mode(association_config, mode, enabled):
    from sync_app.account_associations import enqueue_due_association

    association_config.match_field = mode
    association_config.auto_associate_accounts = enabled
    association_config.save()
    save_source(association_config)
    enqueue_due_association()
    assert not Job.objects.exists()


def test_background_refresh_is_throttled_until_hourly_due(association_config, monkeypatch):
    from sync_app.account_associations import enqueue_due_association

    job = run_association(monkeypatch, Source(), ReadOnlyDirectory([account(name="1001")]))
    assert job.status == "success"
    enqueue_due_association()
    assert Job.objects.count() == 1
    old = timezone.now() - timedelta(minutes=61)
    Job.objects.filter(pk=job.pk).update(created_at=old, started_at=old, finished_at=old)
    enqueue_due_association()
    assert Job.objects.filter(kind="associate", status="queued").count() == 1


def test_failed_same_basis_is_throttled_but_source_change_can_refresh(association_config, monkeypatch):
    from sync_app.account_associations import enqueue_due_association

    directory = ReadOnlyDirectory([account(name="1001")])

    def failed_read():
        raise RuleError("AD 查询未完整成功")

    monkeypatch.setattr(directory, "accounts", failed_read)
    job = run_association(monkeypatch, Source(), directory)
    assert job.status == "failed"
    enqueue_due_association()
    assert Job.objects.count() == 1
    save_source(association_config, [user(employee="1002")])
    enqueue_due_association()
    assert Job.objects.filter(kind="associate", status="queued").count() == 1


def test_admin_can_explicitly_retry_association_before_hourly_schedule(association_config, monkeypatch):
    from sync_app.account_associations import enqueue_due_association

    run_association(monkeypatch, Source(), ReadOnlyDirectory([account(name="1001")]))
    enqueue_due_association()
    assert Job.objects.count() == 1
    job = sync.enqueue(kind="associate", actor="admin")
    assert job.kind == "associate" and job.actor == "admin" and job.status == "queued"


def test_association_basis_changes_for_source_or_identity_scope_not_password_policy(association_config):
    from sync_app.account_associations import association_basis

    snapshot = save_source(association_config)
    original = association_basis(association_config, snapshot)
    association_config.minimum_password_length = 20
    association_config.sspr_match = "email"
    association_config.save()
    assert association_basis(association_config, snapshot) == original
    newer = save_source(association_config, [user(employee="1002")])
    assert association_basis(association_config, newer) != original
    association_config.root_department = "2"
    assert association_basis(association_config, snapshot) != original


def test_association_migration_preserves_existing_management_and_allows_previous_image_inserts(tmp_path):
    """A container rollback keeps the migrated DB, including its new columns."""
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env.update(AD_ORG_SYNC_DATA_DIR=str(tmp_path / "migration"), DJANGO_SETTINGS_MODULE="sync_app.settings")
    code = '''
import uuid, django
django.setup()
from django.core.management import call_command
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
call_command("migrate", "sync_app", "0013_password_reset_notification", interactive=False, verbosity=0)
old_apps = MigrationExecutor(connection).loader.project_state([("sync_app", "0013_password_reset_notification")]).apps
OldConfiguration = old_apps.get_model("sync_app", "Configuration")
OldPerson = old_apps.get_model("sync_app", "Person")
OldBinding = old_apps.get_model("sync_app", "Binding")
OldConfiguration.objects.create(pk=1)
person = OldPerson.objects.create(source_id="old", name="Existing employee")
guid = uuid.uuid4()
revision = uuid.uuid4()
old = OldBinding.objects.create(person=person, object_guid=guid, username="old-login", manual=True, enabled=False, revision=revision)
call_command("migrate", "sync_app", "0014_default_account_associations", interactive=False, verbosity=0)
from sync_app.models import Configuration, Binding
current = Binding.objects.get(pk=old.pk)
assert current.sync_managed is True
assert current.object_guid == guid and current.username == "old-login"
assert current.manual is True and current.enabled is False and current.revision == revision
assert Configuration.objects.get(pk=1).auto_associate_accounts is True
# Previous code does not supply these fields after a rollback. Database defaults
# must keep old inserts working without a destructive reverse migration.
rollback_person = OldPerson.objects.create(source_id="rollback", name="Rollback employee")
rollback_binding = OldBinding.objects.create(person=rollback_person, object_guid=uuid.uuid4(), username="rollback-login")
assert Binding.objects.get(pk=rollback_binding.pk).sync_managed is True
# Identity-only rows must present the legacy pause flag through the old ORM.
# Old workers know no sync_managed field and use enabled in their missing-user
# guard, so even an AD account under the managed OU cannot be auto-disabled.
identity_person = OldPerson.objects.create(source_id="identity-only", name="Associated employee")
identity_binding = Binding.objects.create(person_id=identity_person.pk, object_guid=uuid.uuid4(), username="identity-login", enabled=False, sync_managed=False)
legacy_identity = OldBinding.objects.get(pk=identity_binding.pk)
assert legacy_identity.enabled is False
legacy_missing_eligible = {
    row.pk for row in OldBinding.objects.select_related("person")
    if row.person.source_id not in {"old"} and row.enabled and not row.person.excluded
}
assert identity_binding.pk not in legacy_missing_eligible
assert rollback_binding.pk in legacy_missing_eligible
OldConfiguration.objects.filter(pk=1).delete()
OldConfiguration.objects.create(pk=1)
assert Configuration.objects.get(pk=1).auto_associate_accounts is True
call_command("migrate", interactive=False, verbosity=0)
call_command("db_check", verbosity=0)
'''
    result = subprocess.run([sys.executable, "-c", code], cwd=root, env=env, capture_output=True, text=True, timeout=40)
    assert result.returncode == 0, result.stderr
