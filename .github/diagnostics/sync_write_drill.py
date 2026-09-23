"""One-off business sync drill against a dedicated test AD OU and isolated DB."""

import copy
import os
import shutil
from contextlib import closing
from pathlib import Path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sync_app.settings")

import django

django.setup()

from django.conf import settings
from django.core.management import call_command
from django.db import connections

from sync_app import synchronization
from sync_app.directory import ActiveDirectory, under
from sync_app.models import Binding, Configuration, Job, Person


run_id = os.environ["TEST_RUN_ID"]
assert run_id.isdecimal() and len(run_id) <= 20
expected_data_dir = Path(f"/tmp/ad-org-sync-sync-drill-{run_id}")
assert settings.DATA_DIR == expected_data_dir
assert Path(settings.DATABASES["default"]["NAME"]) == expected_data_dir / "django.sqlite3"
assert under("OU=ADOrgSync-SyncTest-8fa5fcdf,DC=tianjitest,DC=com", settings.LDAP_BASE_DN)
assert settings.LDAP_VERIFY_CERT is False

root_ou = "OU=ADOrgSync-SyncTest-8fa5fcdf,DC=tianjitest,DC=com"
root_id = "codex-root-" + run_id
dept_a = "codex-a-" + run_id
dept_b = "codex-b-" + run_id
ou_a = f"OU=CodexSyncA-{run_id},{root_ou}"
ou_b = f"OU=CodexSyncB-{run_id},{root_ou}"
source_ids = ["codex-source-a-" + run_id, "codex-source-b-" + run_id]
employee_ids = ["CX" + run_id[-8:] + "A", "CX" + run_id[-8:] + "B"]


def user(index, department):
    return {
        "source_id": source_ids[index],
        "name": "Codex Sync Test " + str(index + 1),
        "employee_id": employee_ids[index],
        "email": "",
        "title": "Acceptance Test",
        "phone": "",
        "departments": [department],
        "primary_department": department,
    }


class SyntheticSource:
    def __init__(self):
        self.users = [user(0, dept_a), user(1, dept_a)]
        self.departments = [
            {"id": root_id, "name": "Codex Root", "parent": "0"},
            {"id": dept_a, "name": "CodexSyncA-" + run_id, "parent": root_id},
            {"id": dept_b, "name": "CodexSyncB-" + run_id, "parent": root_id},
        ]

    def collect(self, requested_root):
        assert requested_root == root_id
        return copy.deepcopy(sorted(self.users, key=lambda item: item["source_id"])), copy.deepcopy(
            sorted(self.departments, key=lambda item: item["id"])
        )

    def close(self):
        pass


def preview_and_apply(label, expected_actions):
    job = synchronization.enqueue(kind="preview", scope="full", actor="acceptance-" + label)
    assert synchronization.run_next()
    job.refresh_from_db()
    assert job.status == "preview_ready", (label, job.status, job.message)
    actual = {item["source_id"]: item["action"] for item in job.plan["operations"]}
    assert actual == expected_actions, (label, actual)
    assert not job.plan["high_risk"]
    print(label + "_preview_actions=" + ",".join(sorted(actual.values())), flush=True)
    synchronization.queue_apply(job.pk, "acceptance-" + label)
    assert synchronization.run_next()
    job.refresh_from_db()
    assert job.status == "success", (label, job.status, job.message)
    print(label + "_apply_status=success", flush=True)
    return job


initial_absence_confirmed = False
try:
    call_command("migrate", interactive=False, verbosity=0)
    config = Configuration.current()
    config.root_department = root_id
    config.root_ou = root_ou
    config.naming = "employee_id"
    config.match_field = "employee_id"
    config.attributes = ["displayName", "title"]
    config.clear_attributes = []
    config.disable_missing = True
    config.disable_limit = 5
    config.disable_percent = 100
    config.schedule_enabled = False
    config.sspr_enabled = False
    config.save()
    assert not Binding.objects.exists() and not Person.objects.exists()
    print("isolated_database_initialized=true", flush=True)

    with closing(ActiveDirectory()) as ad:
        ad.verify_ou(root_ou)
        assert ad.ou_identity(ou_a) is None and ad.ou_identity(ou_b) is None
        for employee_id in employee_ids:
            assert not ad.match("employee_id", employee_id)
    initial_absence_confirmed = True
    print("test_domain_objects_absent_before_drill=true", flush=True)

    source = SyntheticSource()
    synchronization.DingTalk = lambda: source

    preview_and_apply("create", {source_ids[0]: "create", source_ids[1]: "create"})
    bindings = {item.person.source_id: str(item.object_guid) for item in Binding.objects.select_related("person")}
    assert set(bindings) == set(source_ids)
    with closing(ActiveDirectory()) as ad:
        for index, source_id in enumerate(source_ids):
            account = ad.by_guid(bindings[source_id])
            assert account["employee_id"] == employee_ids[index]
            assert account["enabled"] and under(account["dn"], ou_a)
        assert ad.ou_identity(ou_b) is not None
    print("two_accounts_and_empty_department_created=true", flush=True)

    preview_and_apply("repeat", {source_ids[0]: "update", source_ids[1]: "update"})
    assert {item.person.source_id: str(item.object_guid) for item in Binding.objects.select_related("person")} == bindings
    with closing(ActiveDirectory()) as ad:
        assert all(len(ad.match("employee_id", employee_id)) == 1 for employee_id in employee_ids)
    print("repeat_apply_created_no_duplicate=true", flush=True)

    source.users[0] = user(0, dept_b)
    preview_and_apply("move", {source_ids[0]: "move", source_ids[1]: "update"})
    with closing(ActiveDirectory()) as ad:
        moved = ad.by_guid(bindings[source_ids[0]])
        assert under(moved["dn"], ou_b) and moved["guid"] == bindings[source_ids[0]]
    print("department_move_preserved_guid=true", flush=True)

    source.users = [source.users[1]]
    preview_and_apply("disable", {source_ids[0]: "disable", source_ids[1]: "update"})
    with closing(ActiveDirectory()) as ad:
        removed = ad.by_guid(bindings[source_ids[0]])
        remaining = ad.by_guid(bindings[source_ids[1]])
        assert not removed["enabled"] and remaining["enabled"]
    print("full_sync_disabled_only_missing_account=true", flush=True)
finally:
    try:
        # Only identities proven absent before the drill may be removed.
        if initial_absence_confirmed:
            with closing(ActiveDirectory()) as ad:
                for employee_id in employee_ids:
                    matches = ad.match("employee_id", employee_id)
                    if matches:
                        assert len(matches) == 1
                        account = matches[0]
                        assert account["username"].casefold() == employee_id.casefold()
                        assert under(account["dn"], ou_a) or under(account["dn"], ou_b)
                        assert ad.conn.delete(account["dn"]), "dedicated sync account cleanup failed"
                    assert not ad.match("employee_id", employee_id)
                for ou in (ou_b, ou_a):
                    if ad.ou_identity(ou) is not None:
                        assert ad.conn.delete(ou), "dedicated sync OU cleanup failed"
                        assert ad.ou_identity(ou) is None
            print("dedicated_sync_accounts_and_ous_removed=true", flush=True)
    finally:
        connections.close_all()
        assert expected_data_dir.parent == Path("/tmp")
        shutil.rmtree(expected_data_dir)
        print("isolated_database_removed=true", flush=True)
