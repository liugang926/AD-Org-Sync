"""One-off test-domain write drill. This file is not part of the application release."""

import os
from contextlib import closing

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sync_app.settings")

import django

django.setup()

from django.conf import settings

from sync_app.directory import ActiveDirectory, under
from sync_app.models import Configuration


run_id = os.environ["TEST_RUN_ID"]
assert run_id.isdecimal() and len(run_id) <= 20
root = "OU=ADOrgSync-SyncTest-8fa5fcdf,DC=tianjitest,DC=com"
assert settings.LDAP_BASE_DN.casefold() == "DC=tianjitest,DC=com".casefold()
assert settings.LDAP_VERIFY_CERT is False
config = Configuration.objects.get(pk=1)
assert config.root_ou.casefold() == root.casefold()
assert config.schedule_enabled is False and config.sspr_enabled is False

username = "cx" + run_id[-12:]
employee_id = "CODEX-ACCEPT-" + run_id
ou_a = f"OU=CodexAcceptanceA-{run_id},{root}"
ou_b = f"OU=CodexAcceptanceB-{run_id},{root}"

with closing(ActiveDirectory()) as ad:
    ad.verify_ou(root)
    assert ad.ou_identity(ou_a) is None and ad.ou_identity(ou_b) is None
    assert not ad.match("source_id", username)
    assert not ad.match("employee_id", employee_id)
    print("test_domain_and_unique_identity_verified=true", flush=True)
    try:
        ad.ensure_ou(ou_a, root)
        ad.ensure_ou(ou_b, root)
        print("dedicated_ous_created=true", flush=True)

        user = {"name": "Codex Acceptance Test", "employee_id": employee_id}
        created = ad.create(user, username, ou_a, root)
        assert created["username"] == username and created["employee_id"] == employee_id
        assert created["enabled"] and under(created["dn"], ou_a)
        guid = created["guid"]
        print("account_created_and_enabled=true", flush=True)

        changed = ad.update(
            guid,
            {"displayName": "Codex Acceptance Updated", "title": "Acceptance Test"},
            ou_a,
            root,
        )
        assert changed["guid"] == guid
        assert changed["attrs"]["displayName"] == "Codex Acceptance Updated"
        assert changed["attrs"]["title"] == "Acceptance Test"
        print("attributes_updated_with_stable_guid=true", flush=True)

        moved = ad.update(guid, {}, ou_b, root)
        assert moved["guid"] == guid and under(moved["dn"], ou_b)
        assert moved["attrs"]["displayName"] == "Codex Acceptance Updated"
        print("account_moved_with_stable_guid=true", flush=True)

        ad.disable(guid, root)
        disabled = ad.by_guid(guid)
        assert disabled["guid"] == guid and not disabled["enabled"]
        print("account_disabled=true", flush=True)
    finally:
        matches = ad.match("source_id", username)
        if matches:
            assert len(matches) == 1
            account = matches[0]
            assert account["employee_id"] == employee_id
            assert under(account["dn"], ou_a) or under(account["dn"], ou_b)
            assert ad.conn.delete(account["dn"]), "dedicated account cleanup failed"
        assert not ad.match("source_id", username)
        for ou in (ou_b, ou_a):
            current_guid = ad.ou_identity(ou)
            if current_guid is not None:
                assert ad.conn.delete(ou), "dedicated OU cleanup failed"
                assert ad.ou_identity(ou) is None
        assert not ad.match("employee_id", employee_id)
        print("dedicated_account_and_ous_removed=true", flush=True)
