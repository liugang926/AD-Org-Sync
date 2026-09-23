"""One-off staged account creation drill in the dedicated test AD OU."""

import os
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

username = "cxstage" + run_id[-10:]
employee_id = "CXS" + run_id[-8:]
ou = f"OU=CodexStaged-{run_id},{root}"
assert len(username) <= 20

initial_absence_confirmed = False
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
        ad.ensure_ou(ou, root)
        created = ad.create(
            {"name": "Codex Staged Test", "employee_id": employee_id},
            username, ou, root, enabled=False, require_change=True,
        )
        guid = created["guid"]
        assert not created["enabled"] and under(created["dn"], ou)
        print("account_created_disabled=true", flush=True)

        updated = ad.update(
            guid,
            {"displayName": "Codex Staged Updated", "title": "Acceptance Test"},
            ou, root, allow_disabled=True,
        )
        assert updated["guid"] == guid and not updated["enabled"]
        assert updated["attrs"]["displayName"] == "Codex Staged Updated"
        assert updated["attrs"]["title"] == "Acceptance Test"
        print("attributes_initialized_while_disabled=true", flush=True)

        enabled = ad.enable(guid, root)
        assert enabled["guid"] == guid and enabled["enabled"]
        assert enabled["attrs"]["displayName"] == "Codex Staged Updated"
        print("account_enabled_after_initialization=true", flush=True)
finally:
    if initial_absence_confirmed:
        with closing(ActiveDirectory()) as cleanup:
            matches = cleanup.match("source_id", username)
            if matches:
                assert len(matches) == 1
                target = matches[0]
                assert target["employee_id"] == employee_id and under(target["dn"], ou)
                assert cleanup.conn.delete(target["dn"]), "dedicated account cleanup failed"
            assert not cleanup.match("source_id", username)
            assert not cleanup.match("employee_id", employee_id)
            if cleanup.ou_identity(ou) is not None:
                assert cleanup.conn.delete(ou), "dedicated OU cleanup failed"
            assert cleanup.ou_identity(ou) is None
        assert not Binding.objects.filter(username=username).exists()
        assert not Person.objects.filter(source_id=username).exists()
        print("dedicated_account_and_ou_removed=true", flush=True)
        print("production_business_database_unchanged_for_test_identity=true", flush=True)
