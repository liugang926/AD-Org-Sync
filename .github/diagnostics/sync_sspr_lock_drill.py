"""Exercise SSPR retry around a real AD attribute write on a disposable account."""

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

from sync_app import sspr
from sync_app.directory import ActiveDirectory, under
from sync_app.domain import RuleError, fingerprint
from sync_app.locking import lock
from sync_app.models import Audit, Binding, Configuration, EmployeeSession, Person


run_id = os.environ["TEST_RUN_ID"]
assert run_id.isdecimal() and len(run_id) <= 20
assert settings.DATA_DIR == Path("/data")
assert Path(settings.DATABASES["default"]["NAME"]) == Path("/data/django.sqlite3")
assert settings.LDAP_VERIFY_CERT is False
root_ou = "OU=ADOrgSync-SyncTest-8fa5fcdf,DC=tianjitest,DC=com"
assert under(root_ou, settings.LDAP_BASE_DN)
suffix = run_id[-8:]
ou = "OU=CodexConcurrent" + suffix + "," + root_ou
username = "sc" + suffix
employee_id = "SC" + suffix
source_id = "codex-concurrent-" + suffix
code = "synthetic-code"


class SyntheticDingTalk:
    def employee(self, given_code):
        assert given_code == code
        return self.user(source_id)

    def user(self, given_source_id):
        assert given_source_id == source_id
        return {"source_id": source_id, "name": "Codex Concurrent Test", "employee_id": employee_id}

    def close(self):
        pass


def password_authenticates(dn, password):
    server = Server(settings.LDAP_HOST, port=636, use_ssl=True, tls=Tls(validate=ssl.CERT_NONE), connect_timeout=10)
    with Connection(
        server, user=dn, password=password, auto_bind=True, auto_referrals=False, receive_timeout=20,
    ) as employee_connection:
        assert employee_connection.bound


absence_confirmed = False
release = Event()
writer = None
try:
    call_command("migrate", interactive=False, verbosity=0)
    config = Configuration.current()
    config.root_ou = root_ou
    config.sspr_match = "employee_id"
    config.sspr_enabled = True
    config.schedule_enabled = False
    config.save()
    assert not Person.objects.exists() and not Binding.objects.exists()

    with closing(ActiveDirectory()) as ad:
        ad.verify_ou(root_ou)
        assert ad.ou_identity(ou) is None
        assert not ad.match("employee_id", employee_id)
        assert not ad.match("source_id", username)
    absence_confirmed = True

    with closing(ActiveDirectory()) as ad:
        ad.ensure_ou(ou, root_ou)
        account = ad.create(
            {"name": "Codex Concurrent Before", "employee_id": employee_id},
            username, ou, root_ou, enabled=True, require_change=False,
        )
        guid = account["guid"]
        assert account["enabled"] and account["employee_id"] == employee_id
    print("disposable_ad_account_created=true", flush=True)

    sspr.DingTalk = SyntheticDingTalk
    token, verified = sspr.verify(code, "198.51.100.24")
    assert verified["guid"] == guid
    assert not Person.objects.exists() and not Binding.objects.exists()
    print("isolated_no_binding_sspr_verified=true", flush=True)

    held = Event()
    failures = []

    def update_while_locked():
        try:
            with lock("account:" + guid):
                with closing(ActiveDirectory()) as ad:
                    updated = ad.update(guid, {"displayName": "Codex Concurrent Updated"}, ou, root_ou)
                    assert updated["attrs"]["displayName"] == "Codex Concurrent Updated"
                held.set()
                assert release.wait(30), "account lock release timed out"
        except Exception as exc:
            failures.append(exc)
            held.set()

    writer = Thread(target=update_while_locked)
    writer.start()
    assert held.wait(30) and not failures
    print("real_ad_attribute_write_completed_with_account_lock_held=true", flush=True)

    alphabet = string.ascii_letters + string.digits
    new_password = "Aa1!" + "".join(secrets.choice(alphabet) for _ in range(28))
    try:
        sspr.reset(token, new_password, new_password, "198.51.100.24")
    except RuleError as exc:
        assert "操作正在执行" in str(exc)
    else:
        raise AssertionError("SSPR bypassed the account lock")
    assert not EmployeeSession.objects.get(digest=fingerprint(token)).used
    assert not Audit.objects.filter(action="sspr_reset", success=True).exists()
    print("overlapping_sspr_rejected_without_consuming_session=true", flush=True)

    release.set()
    writer.join(30)
    assert not writer.is_alive() and not failures
    assert sspr.reset(token, new_password, new_password, "198.51.100.24") == "密码已成功重置"
    assert EmployeeSession.objects.get(digest=fingerprint(token)).used
    with closing(ActiveDirectory()) as ad:
        account = ad.by_guid(guid)
        assert account["attrs"]["displayName"] == "Codex Concurrent Updated"
        password_authenticates(account["dn"], new_password)
        later = ad.update(guid, {"displayName": "Codex Concurrent After"}, ou, root_ou)
        assert later["attrs"]["displayName"] == "Codex Concurrent After"
        password_authenticates(later["dn"], new_password)
    print("retry_succeeded_and_later_ad_attribute_write_preserved_password=true", flush=True)

    assert Audit.objects.filter(action="sspr_reset", success=True).count() == 1
    assert all(new_password not in str(row) for row in Audit.objects.values())
    connections.close_all()
    assert new_password.encode() not in Path(settings.DATABASES["default"]["NAME"]).read_bytes()
    assert not Person.objects.exists() and not Binding.objects.exists()
    print("isolated_database_has_no_binding_or_plaintext_password=true", flush=True)
finally:
    release.set()
    if writer is not None:
        writer.join(30)
        assert not writer.is_alive(), "account lock writer did not stop"
    if absence_confirmed:
        with closing(ActiveDirectory()) as ad:
            matches = ad.match("employee_id", employee_id)
            if matches:
                assert len(matches) == 1
                account = matches[0]
                assert account["username"].casefold() == username.casefold()
                assert under(account["dn"], ou)
                assert ad.conn.delete(account["dn"]), "disposable account cleanup failed"
            assert not ad.match("employee_id", employee_id)
            assert not ad.match("source_id", username)
            if ad.ou_identity(ou) is not None:
                contents = ad.search("(objectClass=*)", base=ou)
                assert len(contents) == 1 and contents[0]["dn"].casefold() == ou.casefold()
                assert ad.conn.delete(ou), "disposable OU cleanup failed"
            assert ad.ou_identity(ou) is None
        print("disposable_ad_account_and_ou_removed=true", flush=True)
