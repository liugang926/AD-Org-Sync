"""One-off full synchronization/SSPR overlap on a disposable AD account."""

import copy
import os
import secrets
import ssl
import string
from contextlib import closing
from pathlib import Path
from threading import Event, Thread

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sync_app.settings")

import django

django.setup()

from django.conf import settings
from django.core.management import call_command
from django.db import connections
from ldap3 import Connection, Server, Tls

from sync_app import sspr, synchronization
from sync_app.directory import ActiveDirectory, under
from sync_app.domain import RuleError, fingerprint
from sync_app.models import Audit, Binding, Configuration, EmployeeSession, Person


run_id = os.environ["TEST_RUN_ID"]
assert run_id.isdecimal() and len(run_id) <= 20
assert settings.DATA_DIR == Path("/data")
assert Path(settings.DATABASES["default"]["NAME"]) == Path("/data/django.sqlite3")
assert settings.LDAP_VERIFY_CERT is False
root_ou = "OU=ADOrgSync-SyncTest-8fa5fcdf,DC=tianjitest,DC=com"
assert under(root_ou, settings.LDAP_BASE_DN)
suffix = run_id[-8:]
username = "cf" + suffix
employee_id = "CF" + suffix
source_id = "codex-overlap-" + suffix
root_department = "1"
code = "synthetic-code"


class SyntheticSource:
    def __init__(self):
        self.name = "Codex Full Sync Updated"

    def user(self, given_source_id):
        assert given_source_id == source_id
        return {
            "source_id": source_id,
            "name": self.name,
            "employee_id": employee_id,
            "email": "",
            "title": "",
            "phone": "",
            "departments": [root_department],
            "primary_department": root_department,
        }

    def employee(self, given_code):
        assert given_code == code
        return self.user(source_id)

    def collect(self, given_root):
        assert str(given_root) == root_department
        return [copy.deepcopy(self.user(source_id))], [
            {"id": root_department, "name": "Test Root", "parent": "0"},
        ]

    def close(self):
        pass


def preview(expected_name):
    job = synchronization.enqueue(kind="preview", scope="users", selected=[source_id], actor="acceptance")
    assert synchronization.run_next()
    job.refresh_from_db()
    assert job.status == "preview_ready", (job.status, job.message)
    operations = job.plan["operations"]
    assert len(operations) == 1
    op = operations[0]
    assert op["source_id"] == source_id and op["action"] == "update"
    assert op["target"]["guid"] == guid and op["attrs"] == {"displayName": expected_name}
    assert op["ou"].casefold() == root_ou.casefold()
    assert not job.plan["high_risk"]
    return job


def password_authenticates(dn, password):
    server = Server(settings.LDAP_HOST, port=636, use_ssl=True, tls=Tls(validate=ssl.CERT_NONE), connect_timeout=10)
    with Connection(
        server, user=dn, password=password, auto_bind=True, auto_referrals=False, receive_timeout=20,
    ) as employee_connection:
        assert employee_connection.bound


absence_confirmed = False
release = Event()
worker = None
try:
    call_command("migrate", interactive=False, verbosity=0)
    config = Configuration.current()
    config.root_department = root_department
    config.root_ou = root_ou
    config.naming = "employee_id"
    config.match_field = "employee_id"
    config.attributes = ["displayName"]
    config.disable_missing = False
    config.sspr_match = "employee_id"
    config.sspr_enabled = True
    config.schedule_enabled = False
    config.save()
    assert not Person.objects.exists() and not Binding.objects.exists()

    with closing(ActiveDirectory()) as ad:
        ad.verify_ou(root_ou)
        assert not ad.match("employee_id", employee_id)
        assert not ad.match("source_id", username)
    absence_confirmed = True

    with closing(ActiveDirectory()) as ad:
        account = ad.create(
            {"name": "Codex Full Sync Before", "employee_id": employee_id},
            username, root_ou, root_ou, enabled=True, require_change=False,
        )
        guid = account["guid"]
        assert account["enabled"] and account["employee_id"] == employee_id
    print("disposable_account_created_in_test_root=true", flush=True)

    source = SyntheticSource()
    synchronization.DingTalk = lambda: source
    sspr.DingTalk = lambda: source
    person = Person.objects.create(source_id=source_id, name=source.name, primary_department=root_department)
    Binding.objects.create(person=person, object_guid=guid, username=username, manual=True)
    first = preview(source.name)
    print("full_sync_preview_targets_only_disposable_account=true", flush=True)

    token, verified = sspr.verify(code, "198.51.100.25")
    assert verified["guid"] == guid
    held = Event()
    failures = []
    original_update = ActiveDirectory.update
    pause_next = [True]

    def update_then_hold(self, *args, **kwargs):
        updated = original_update(self, *args, **kwargs)
        if pause_next[0] and args[0] == guid:
            pause_next[0] = False
            held.set()
            assert release.wait(30), "sync account lock release timed out"
        return updated

    ActiveDirectory.update = update_then_hold
    synchronization.queue_apply(first.pk, "acceptance")

    def apply_in_thread():
        try:
            assert synchronization.run_next()
        except Exception as exc:
            failures.append(exc)
            held.set()

    worker = Thread(target=apply_in_thread)
    worker.start()
    assert held.wait(30) and not failures
    print("full_sync_ad_update_completed_while_account_lock_held=true", flush=True)

    alphabet = string.ascii_letters + string.digits
    new_password = "Aa1!" + "".join(secrets.choice(alphabet) for _ in range(28))
    try:
        sspr.reset(token, new_password, new_password, "198.51.100.25")
    except RuleError as exc:
        assert "操作正在执行" in str(exc)
    else:
        raise AssertionError("SSPR bypassed running synchronization")
    assert not EmployeeSession.objects.get(digest=fingerprint(token)).used
    assert not Audit.objects.filter(action="sspr_reset", success=True).exists()
    print("overlapping_reset_rejected_without_consuming_session=true", flush=True)

    release.set()
    worker.join(30)
    assert not worker.is_alive() and not failures
    first.refresh_from_db()
    assert first.status == "success", (first.status, first.message)
    assert str(Binding.objects.get(person=person).object_guid) == guid
    with closing(ActiveDirectory()) as ad:
        assert ad.by_guid(guid)["attrs"]["displayName"] == source.name
    print("full_sync_apply_completed_on_same_guid=true", flush=True)

    assert sspr.reset(token, new_password, new_password, "198.51.100.25") == "密码已成功重置"
    assert EmployeeSession.objects.get(digest=fingerprint(token)).used
    with closing(ActiveDirectory()) as ad:
        password_authenticates(ad.by_guid(guid)["dn"], new_password)
    print("retry_password_authenticates_over_ldaps=true", flush=True)

    source.name = "Codex Full Sync After Reset"
    second = preview(source.name)
    synchronization.queue_apply(second.pk, "acceptance")
    assert synchronization.run_next()
    second.refresh_from_db()
    assert second.status == "success", (second.status, second.message)
    with closing(ActiveDirectory()) as ad:
        account = ad.by_guid(guid)
        assert account["attrs"]["displayName"] == source.name
        assert account["guid"] == guid
        password_authenticates(account["dn"], new_password)
    assert str(Binding.objects.get(person=person).object_guid) == guid
    print("later_full_sync_preserved_new_password_and_binding=true", flush=True)

    assert Audit.objects.filter(action="sspr_reset", success=True).count() == 1
    assert all(new_password not in str(row) for row in Audit.objects.values())
    connections.close_all()
    assert new_password.encode() not in Path(settings.DATABASES["default"]["NAME"]).read_bytes()
    print("isolated_database_has_no_plaintext_password=true", flush=True)
finally:
    release.set()
    if worker is not None:
        worker.join(30)
        assert not worker.is_alive(), "sync worker did not stop"
    if absence_confirmed:
        with closing(ActiveDirectory()) as ad:
            matches = ad.match("employee_id", employee_id)
            if matches:
                assert len(matches) == 1
                account = matches[0]
                assert account["username"].casefold() == username.casefold()
                assert under(account["dn"], root_ou)
                assert ad.conn.delete(account["dn"]), "disposable account cleanup failed"
            assert not ad.match("employee_id", employee_id)
            assert not ad.match("source_id", username)
        print("disposable_account_removed_from_test_root=true", flush=True)
