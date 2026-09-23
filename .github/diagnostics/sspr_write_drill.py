"""One-off SSPR drill using isolated state and a dedicated test AD account."""

import os
import secrets
import shutil
import ssl
import string
from contextlib import closing
from pathlib import Path

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
from sync_app.models import Audit, Binding, Configuration, EmployeeSession, Person


run_id = os.environ["TEST_RUN_ID"]
assert run_id.isdecimal() and len(run_id) <= 20
data_dir = Path(f"/tmp/ad-org-sync-sspr-drill-{run_id}")
assert settings.DATA_DIR == data_dir
assert Path(settings.DATABASES["default"]["NAME"]) == data_dir / "django.sqlite3"
root_ou = "OU=ADOrgSync-SyncTest-8fa5fcdf,DC=tianjitest,DC=com"
assert under(root_ou, settings.LDAP_BASE_DN) and settings.LDAP_VERIFY_CERT is False
ou = f"OU=CodexSSPR-{run_id},{root_ou}"
employee_id = "SP" + run_id[-8:]
source_id = "codex-sspr-" + run_id
code = "isolated-test-code"


class SyntheticDingTalk:
    def employee(self, given_code):
        assert given_code == code
        return self.user(source_id)

    def user(self, given_source_id):
        assert given_source_id == source_id
        return {"source_id": source_id, "name": "Codex SSPR Test", "employee_id": employee_id}

    def close(self):
        pass


initial_absence_confirmed = False
try:
    call_command("migrate", interactive=False, verbosity=0)
    config = Configuration.current()
    config.root_ou = root_ou
    config.sspr_match = "employee_id"
    config.sspr_enabled = True
    config.schedule_enabled = False
    config.save()
    assert not Person.objects.exists() and not Binding.objects.exists()
    print("isolated_sspr_enabled_without_local_binding=true", flush=True)

    with closing(ActiveDirectory()) as ad:
        ad.verify_ou(root_ou)
        assert ad.ou_identity(ou) is None and not ad.match("employee_id", employee_id)
    initial_absence_confirmed = True
    print("dedicated_sspr_identity_absent_before_drill=true", flush=True)

    with closing(ActiveDirectory()) as ad:
        ad.ensure_ou(ou, root_ou)
        created = ad.create(
            {"name": "Codex SSPR Test", "employee_id": employee_id},
            employee_id,
            ou,
            root_ou,
            enabled=True,
            require_change=False,
        )
        assert created["enabled"] and created["employee_id"] == employee_id
        guid = created["guid"]
    print("dedicated_sspr_account_created=true", flush=True)

    sspr.DingTalk = SyntheticDingTalk
    token, verified = sspr.verify(code, "198.51.100.23")
    assert verified["guid"] == guid and token
    assert not Person.objects.exists() and not Binding.objects.exists()
    print("sspr_verified_without_sync_or_binding=true", flush=True)

    alphabet = string.ascii_letters + string.digits
    new_password = "Aa1!" + "".join(secrets.choice(alphabet) for _ in range(28))
    outcome = sspr.reset(token, new_password, new_password, "198.51.100.23")
    assert outcome == "密码已成功重置"
    session = EmployeeSession.objects.get(digest=fingerprint(token))
    assert session.used and str(session.object_guid) == guid
    print("sspr_reset_succeeded_and_session_consumed=true", flush=True)

    with closing(ActiveDirectory()) as ad:
        account = ad.by_guid(guid)
        assert account["enabled"] and under(account["dn"], ou)
    server = Server(settings.LDAP_HOST, port=636, use_ssl=True, tls=Tls(validate=ssl.CERT_NONE), connect_timeout=10)
    try:
        with Connection(
            server,
            user=account["dn"],
            password=new_password,
            auto_bind=True,
            auto_referrals=False,
            receive_timeout=20,
        ) as employee_connection:
            assert employee_connection.bound
    except Exception:
        raise AssertionError("new password did not authenticate through LDAPS") from None
    print("new_password_authenticates_over_ldaps=true", flush=True)

    try:
        sspr.reset(token, new_password, new_password, "198.51.100.23")
    except RuleError:
        print("used_session_replay_rejected=true", flush=True)
    else:
        raise AssertionError("consumed SSPR session was accepted again")

    assert not Person.objects.exists() and not Binding.objects.exists()
    assert Audit.objects.filter(action="sspr_reset", success=True).count() == 1
    assert all(new_password not in str(row) for row in Audit.objects.values())
    connections.close_all()
    assert new_password.encode() not in (data_dir / "django.sqlite3").read_bytes()
    print("no_local_binding_or_plaintext_password=true", flush=True)
finally:
    try:
        if initial_absence_confirmed:
            with closing(ActiveDirectory()) as ad:
                matches = ad.match("employee_id", employee_id)
                if matches:
                    assert len(matches) == 1
                    account = matches[0]
                    assert account["username"].casefold() == employee_id.casefold()
                    assert under(account["dn"], ou)
                    assert ad.conn.delete(account["dn"]), "dedicated SSPR account cleanup failed"
                assert not ad.match("employee_id", employee_id)
                if ad.ou_identity(ou) is not None:
                    assert ad.conn.delete(ou), "dedicated SSPR OU cleanup failed"
                    assert ad.ou_identity(ou) is None
            print("dedicated_sspr_account_and_ou_removed=true", flush=True)
    finally:
        connections.close_all()
        assert data_dir.parent == Path("/tmp")
        shutil.rmtree(data_dir)
        print("isolated_sspr_database_removed=true", flush=True)
