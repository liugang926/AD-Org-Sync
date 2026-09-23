"""One-off live AD object replacement guard for sync and SSPR."""

import copy
import os
import secrets
import string
from contextlib import closing
from pathlib import Path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sync_app.settings")

import django

django.setup()

from django.conf import settings
from django.core.management import call_command

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
username = "cg" + suffix
employee_id = "CG" + suffix
source_id = "codex-guid-" + suffix
root_department = "1"
code = "synthetic-code"


class SyntheticSource:
    def user(self, given_source_id):
        assert given_source_id == source_id
        return {
            "source_id": source_id,
            "name": "Codex GUID Updated",
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


absence_confirmed = False
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
        original = ad.create(
            {"name": "Codex GUID Before", "employee_id": employee_id},
            username, root_ou, root_ou, enabled=True, require_change=False,
        )
        original_guid = original["guid"]
        assert original["enabled"]
    print("original_disposable_account_created=true", flush=True)

    source = SyntheticSource()
    synchronization.DingTalk = lambda: source
    sspr.DingTalk = lambda: source
    person = Person.objects.create(
        source_id=source_id, name="Codex GUID Updated", primary_department=root_department,
    )
    Binding.objects.create(person=person, object_guid=original_guid, username=username, manual=True)
    preview = synchronization.enqueue(kind="preview", scope="users", selected=[source_id], actor="acceptance")
    assert synchronization.run_next()
    preview.refresh_from_db()
    assert preview.status == "preview_ready", (preview.status, preview.message)
    operations = preview.plan["operations"]
    assert len(operations) == 1
    assert operations[0]["action"] == "update"
    assert operations[0]["target"]["guid"] == original_guid
    assert operations[0]["attrs"] == {"displayName": "Codex GUID Updated"}
    print("old_sync_preview_targets_original_guid=true", flush=True)

    token, verified = sspr.verify(code, "198.51.100.26")
    assert verified["guid"] == original_guid
    session = EmployeeSession.objects.get(digest=fingerprint(token))
    assert str(session.object_guid) == original_guid and not session.used
    print("old_sspr_session_targets_original_guid=true", flush=True)

    with closing(ActiveDirectory()) as ad:
        assert ad.conn.delete(original["dn"]), "original disposable account deletion failed"
        assert not ad.match("employee_id", employee_id)
        replacement = ad.create(
            {"name": "Codex GUID Before", "employee_id": employee_id},
            username, root_ou, root_ou, enabled=True, require_change=False,
        )
        replacement_guid = replacement["guid"]
        assert replacement_guid != original_guid
        assert replacement["username"].casefold() == username.casefold()
        assert replacement["employee_id"] == employee_id
        assert replacement["dn"].casefold() == original["dn"].casefold()
        replacement_before = fingerprint(replacement)
    print("same_name_and_employee_id_recreated_with_new_guid=true", flush=True)

    synchronization.queue_apply(preview.pk, "acceptance")
    assert synchronization.run_next()
    preview.refresh_from_db()
    assert preview.status == "failed", (preview.status, preview.message)
    assert "AD 目标对象不存在" in preview.message or "AD 状态已变化" in preview.message
    binding = Binding.objects.get(person=person)
    assert str(binding.object_guid) == original_guid and binding.manual and binding.enabled
    with closing(ActiveDirectory()) as ad:
        assert fingerprint(ad.by_guid(replacement_guid)) == replacement_before
    print("stale_sync_apply_rejected_without_rebinding_or_ad_write=true", flush=True)

    new_password = "Aa1!" + "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(28))
    try:
        sspr.reset(token, new_password, new_password, "198.51.100.26")
    except RuleError as exc:
        assert "AD 匹配对象发生变化" in str(exc)
    else:
        raise AssertionError("old SSPR session was accepted for replacement account")
    session.refresh_from_db()
    assert session.used
    assert not Audit.objects.filter(action="sspr_reset", success=True).exists()
    with closing(ActiveDirectory()) as ad:
        assert fingerprint(ad.by_guid(replacement_guid)) == replacement_before
    print("stale_sspr_session_rejected_without_password_change=true", flush=True)

    fresh = synchronization.enqueue(kind="preview", scope="users", selected=[source_id], actor="acceptance")
    assert synchronization.run_next()
    fresh.refresh_from_db()
    assert fresh.status == "blocked", (fresh.status, fresh.message)
    fresh_ops = fresh.plan["operations"]
    assert len(fresh_ops) == 1 and fresh_ops[0]["action"] == "conflict"
    assert str(Binding.objects.get(person=person).object_guid) == original_guid
    print("new_preview_does_not_auto_adopt_replacement_guid=true", flush=True)

    fresh_token, fresh_account = sspr.verify(code, "198.51.100.26")
    assert fresh_account["guid"] == replacement_guid
    assert EmployeeSession.objects.get(digest=fingerprint(fresh_token)).object_guid != session.object_guid
    print("new_sspr_authorization_requires_fresh_guid_verification=true", flush=True)
finally:
    if absence_confirmed:
        with closing(ActiveDirectory()) as ad:
            matches = ad.match("employee_id", employee_id)
            if matches:
                assert len(matches) == 1
                account = matches[0]
                assert account["username"].casefold() == username.casefold()
                assert under(account["dn"], root_ou)
                assert ad.conn.delete(account["dn"]), "replacement account cleanup failed"
            assert not ad.match("employee_id", employee_id)
            assert not ad.match("source_id", username)
        print("disposable_replacement_account_removed=true", flush=True)
