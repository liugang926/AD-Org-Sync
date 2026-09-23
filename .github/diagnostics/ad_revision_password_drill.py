"""Verify password changes advance AD object revision on a disposable account."""

import os
import secrets
import string
import uuid
from contextlib import closing

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sync_app.settings")

import django

django.setup()

from django.conf import settings

from sync_app.directory import ActiveDirectory, under
from sync_app.models import Binding, Configuration, Person


run_id = os.environ["TEST_RUN_ID"]
assert run_id.isdecimal() and len(run_id) <= 20
root = "OU=ADOrgSync-SyncTest-8fa5fcdf,DC=tianjitest,DC=com"
assert under(root, settings.LDAP_BASE_DN)
assert settings.LDAP_VERIFY_CERT is False

config = Configuration.objects.get(pk=1)
assert config.root_ou.casefold() == root.casefold()
assert not config.schedule_enabled and not config.sspr_enabled

username = "cxrev" + run_id[-10:]
employee_id = "CXR" + run_id[-8:]
ou = f"OU=CodexRevision-{run_id},{root}"
assert len(username) <= 20


def revision(ad, guid):
    rows = ad.search(
        f"(&(objectCategory=person)(objectClass=user)(employeeID={employee_id}))",
        attrs=["objectGUID", "uSNChanged"],
    )
    assert len(rows) == 1
    attrs = rows[0]["attributes"]
    identity = attrs["objectGUID"]
    if isinstance(identity, list):
        identity = identity[0] if identity else None
    assert str(uuid.UUID(str(identity).strip("{}"))) == guid
    value = attrs.get("uSNChanged")
    if isinstance(value, list):
        value = value[0] if value else None
    assert str(value or "").isdecimal() and int(value) > 0
    return int(value)


initial_absence_confirmed = False
created_guid = None
created_ou_guid = None
with closing(ActiveDirectory()) as ad:
    ad.verify_ou(root)
    assert ad.ou_identity(ou) is None
    assert not ad.match("source_id", username)
    assert not ad.match("employee_id", employee_id)
    assert not Binding.objects.filter(username=username).exists()
    assert not Person.objects.filter(source_id=username).exists()
    initial_absence_confirmed = True

try:
    with closing(ActiveDirectory()) as ad:
        created_ou_guid = ad.ensure_ou(ou, root)
        created = ad.create(
            {"name": "Codex Revision Test", "employee_id": employee_id},
            username, ou, root, enabled=True, require_change=True,
        )
        guid = created["guid"]
        created_guid = guid
        assert created["enabled"] and under(created["dn"], ou)
        before = revision(ad, guid)

        alphabet = string.ascii_letters + string.digits
        new_password = "Aa1!" + "".join(secrets.choice(alphabet) for _ in range(28))
        outcome = ad.reset_password(guid, new_password)
        assert outcome.complete
        after = revision(ad, guid)
        assert after > before, "password reset did not advance AD object revision"
        print("password_reset_advanced_ad_revision=true", flush=True)
finally:
    if initial_absence_confirmed:
        with closing(ActiveDirectory()) as cleanup:
            matches = cleanup.match("source_id", username)
            if matches:
                assert len(matches) == 1
                target = matches[0]
                assert target["employee_id"] == employee_id and under(target["dn"], ou)
                if created_guid is not None:
                    assert target["guid"] == created_guid
                assert cleanup.conn.delete(target["dn"]), "dedicated account cleanup failed"
            assert not cleanup.match("source_id", username)
            assert not cleanup.match("employee_id", employee_id)
            if cleanup.ou_identity(ou) is not None:
                if created_ou_guid is not None:
                    assert cleanup.ou_identity(ou) == created_ou_guid
                assert cleanup.conn.delete(ou), "dedicated OU cleanup failed"
            assert cleanup.ou_identity(ou) is None
        assert not Binding.objects.filter(username=username).exists()
        assert not Person.objects.filter(source_id=username).exists()
        print("dedicated_account_and_ou_removed=true", flush=True)
        print("production_business_database_unchanged_for_test_identity=true", flush=True)
