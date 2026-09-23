"""Exercise candidate synchronization against one disposable test AD identity."""

import copy
import os
from contextlib import closing
from pathlib import Path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sync_app.settings")

import django

django.setup()

from django.conf import settings
from django.core.management import call_command

from sync_app import synchronization
from sync_app.directory import ActiveDirectory, under
from sync_app.models import Binding, Configuration, Person


run_id = os.environ["TEST_RUN_ID"]
assert run_id.isdecimal() and len(run_id) <= 20
assert settings.DATA_DIR == Path("/data")
assert Path(settings.DATABASES["default"]["NAME"]) == Path("/data/django.sqlite3")
root = "OU=ADOrgSync-SyncTest-8fa5fcdf,DC=tianjitest,DC=com"
assert under(root, settings.LDAP_BASE_DN)
assert settings.LDAP_VERIFY_CERT is False

source_id = "codex-staged-source-" + run_id
employee_id = "CXS" + run_id[-8:]
name = "Codex Staged Sync Test"


class SyntheticSource:
    def collect(self, requested_root):
        assert requested_root == "codex-staged-root-" + run_id
        return copy.deepcopy([
            {
                "source_id": source_id,
                "name": name,
                "employee_id": employee_id,
                "email": "",
                "title": "Acceptance Test",
                "phone": "",
                "departments": [requested_root],
                "primary_department": requested_root,
            }
        ]), [{"id": requested_root, "name": "Codex Test Root", "parent": "0"}]

    def close(self):
        pass


absence_confirmed = False
try:
    call_command("migrate", interactive=False, verbosity=0)
    config = Configuration.current()
    config.root_department = "codex-staged-root-" + run_id
    config.root_ou = root
    config.naming = "employee_id"
    config.match_field = "employee_id"
    config.attributes = ["displayName", "title"]
    config.enable_new_accounts = False
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
    preview = synchronization.enqueue(kind="preview", scope="full", actor="staged-drill")
    assert synchronization.run_next()
    preview.refresh_from_db()
    assert preview.status == "preview_ready", (preview.status, preview.message)
    assert [(op["source_id"], op["action"]) for op in preview.plan["operations"]] == [(source_id, "create")]
    assert not preview.plan["high_risk"]
    synchronization.queue_apply(preview.pk, "staged-drill")
    assert synchronization.run_next()
    preview.refresh_from_db()
    assert preview.status == "success", (preview.status, preview.message)
    binding = Binding.objects.select_related("person").get(person__source_id=source_id)
    assert binding.username == employee_id and not binding.enabled
    with closing(ActiveDirectory()) as ad:
        matches = ad.match("employee_id", employee_id)
        assert len(matches) == 1
        account = matches[0]
        assert account["guid"] == str(binding.object_guid)
        assert account["username"] == employee_id
        assert not account["enabled"] and under(account["dn"], root)
        assert account["attrs"]["displayName"] == name
        assert account["attrs"]["title"] == "Acceptance Test"
    print("candidate_sync_created_initialized_and_held_disabled=true", flush=True)

    repeat = synchronization.enqueue(kind="preview", scope="full", actor="staged-drill-repeat")
    assert synchronization.run_next()
    repeat.refresh_from_db()
    assert repeat.status == "preview_ready", (repeat.status, repeat.message)
    assert [(op["source_id"], op["action"]) for op in repeat.plan["operations"]] == [(source_id, "skip")]
    synchronization.queue_apply(repeat.pk, "staged-drill-repeat")
    assert synchronization.run_next()
    repeat.refresh_from_db()
    assert repeat.status == "success", (repeat.status, repeat.message)
    assert Binding.objects.get(person__source_id=source_id).object_guid == binding.object_guid
    print("repeat_sync_preserved_paused_binding_without_duplicate=true", flush=True)
finally:
    if absence_confirmed:
        with closing(ActiveDirectory()) as ad:
            matches = ad.match("employee_id", employee_id)
            if matches:
                assert len(matches) == 1
                account = matches[0]
                assert account["username"].casefold() == employee_id.casefold()
                assert under(account["dn"], root)
                assert ad.conn.delete(account["dn"]), "dedicated staged account cleanup failed"
            assert not ad.match("employee_id", employee_id)
            assert not ad.match("source_id", employee_id)
        print("dedicated_staged_account_removed=true", flush=True)
