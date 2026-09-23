"""Verify the candidate blocks staged recovery after a real AD password change."""

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

from sync_app import synchronization
from sync_app.directory import ActiveDirectory, under
from sync_app.domain import RuleError, fingerprint
from sync_app.models import Binding, Configuration, Operation, Person


run_id = os.environ["TEST_RUN_ID"]
assert run_id.isdecimal() and len(run_id) <= 20
assert settings.DATA_DIR == Path("/data")
assert Path(settings.DATABASES["default"]["NAME"]) == Path("/data/django.sqlite3")
root = "OU=ADOrgSync-SyncTest-8fa5fcdf,DC=tianjitest,DC=com"
assert under(root, settings.LDAP_BASE_DN)
assert settings.LDAP_VERIFY_CERT is False

source_id = "codex-staged-source-" + run_id
employee_id = "CXS" + run_id[-8:]
name = "Codex Revision Recovery Test"


class SyntheticSource:
    def collect(self, requested_root):
        assert requested_root == "codex-staged-root-" + run_id
        return copy.deepcopy([{
            "source_id": source_id,
            "name": name,
            "employee_id": employee_id,
            "email": "",
            "title": "Acceptance Test",
            "phone": "",
            "departments": [requested_root],
            "primary_department": requested_root,
        }]), [{"id": requested_root, "name": "Codex Test Root", "parent": "0"}]

    def close(self):
        pass


absence_confirmed = False
created_guid = None
try:
    call_command("migrate", interactive=False, verbosity=0)
    config = Configuration.current()
    config.root_department = "codex-staged-root-" + run_id
    config.root_ou = root
    config.naming = "employee_id"
    config.match_field = "employee_id"
    config.attributes = ["displayName", "title"]
    config.enable_new_accounts = True
    config.disable_missing = False
    config.schedule_enabled = False
    config.sspr_enabled = False
    config.save()
    assert not Person.objects.exists() and not Binding.objects.exists()
    with closing(ActiveDirectory()) as ad:
        ad.verify_ou(root)
        assert not ad.match("employee_id", employee_id)
        assert not ad.match("source_id", employee_id)
    absence_confirmed = True
    print("isolated_database_and_test_identity_verified=true", flush=True)

    synchronization.DingTalk = SyntheticSource
    preview = synchronization.enqueue(kind="preview", scope="full", actor="revision-drill")
    assert synchronization.run_next()
    preview.refresh_from_db()
    assert preview.status == "preview_ready"
    assert [(op["source_id"], op["action"]) for op in preview.plan["operations"]] == [(source_id, "create")]
    synchronization.queue_apply(preview.pk, "revision-drill")

    original_enable = ActiveDirectory.enable
    failure_injected = False

    def fail_enable(self, guid, managed_root):
        nonlocal_marker[0] = True
        account = self.by_guid(guid)
        assert not account["enabled"] and account["ad_revision"]
        assert account["attrs"]["displayName"] == name
        raise RuleError("诊断：模拟账号启用前中断")

    nonlocal_marker = [False]
    ActiveDirectory.enable = fail_enable
    try:
        assert synchronization.run_next()
    finally:
        ActiveDirectory.enable = original_enable
    failure_injected = nonlocal_marker[0]
    preview.refresh_from_db()
    assert failure_injected and preview.status == "partial_failed"
    assert not Binding.objects.exists()
    failed_create = Operation.objects.get(job=preview, source_id=source_id, action="create")
    assert failed_create.status == "failed" and failed_create.target_guid
    assert failed_create.evidence.get("initialized_fingerprint")
    created_guid = str(failed_create.target_guid)

    with closing(ActiveDirectory()) as ad:
        before = ad.by_guid(created_guid)
        assert not before["enabled"] and before["ad_revision"]
        alphabet = string.ascii_letters + string.digits
        new_password = "Aa1!" + "".join(secrets.choice(alphabet) for _ in range(28))
        assert ad.conn.extend.microsoft.modify_password(before["dn"], new_password)
        after = ad.by_guid(created_guid)
        assert int(after["ad_revision"]) > int(before["ad_revision"])
        assert {key: value for key, value in after.items() if key != "ad_revision"} == {
            key: value for key, value in before.items() if key != "ad_revision"
        }
        assert fingerprint(after) != failed_create.evidence["initialized_fingerprint"]
    print("password_change_advanced_candidate_ad_fingerprint=true", flush=True)

    retry = synchronization.enqueue(kind="preview", scope="full", actor="revision-drill-retry")
    assert synchronization.run_next()
    retry.refresh_from_db()
    assert retry.status == "blocked"
    assert [(op["source_id"], op["action"]) for op in retry.plan["operations"]] == [(source_id, "conflict")]
    assert retry.plan["operations"][0]["target"]["guid"] == created_guid
    assert not Binding.objects.exists()
    with closing(ActiveDirectory()) as ad:
        matches = ad.match("employee_id", employee_id)
        assert len(matches) == 1 and matches[0]["guid"] == created_guid
        assert not matches[0]["enabled"]
    print("candidate_recovery_blocked_after_password_change=true", flush=True)
finally:
    if absence_confirmed:
        with closing(ActiveDirectory()) as ad:
            matches = ad.match("employee_id", employee_id)
            if matches:
                assert len(matches) == 1
                account = matches[0]
                assert account["username"].casefold() == employee_id.casefold()
                assert under(account["dn"], root)
                if created_guid is not None:
                    assert account["guid"] == created_guid
                assert ad.conn.delete(account["dn"]), "dedicated staged account cleanup failed"
            assert not ad.match("employee_id", employee_id)
            assert not ad.match("source_id", employee_id)
        print("dedicated_staged_account_removed=true", flush=True)
