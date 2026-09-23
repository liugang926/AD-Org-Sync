"""One-off real DingTalk source write to a disposable test AD account."""

import os
from contextlib import closing
from pathlib import Path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sync_app.settings")

import django

django.setup()

from django.conf import settings
from django.core.management import call_command

from sync_app import synchronization
from sync_app.directory import ActiveDirectory, DingTalk, under
from sync_app.domain import fingerprint
from sync_app.models import Binding, Configuration, DepartmentBinding, Person


run_id = os.environ["TEST_RUN_ID"]
source_id = os.environ["TEST_SOURCE_ID"]
root_department = os.environ["TEST_ROOT_DEPARTMENT"]
assert run_id.isdecimal() and len(run_id) <= 20
assert source_id and not any(char in source_id for char in "\r\n=")
assert root_department.isdecimal() and int(root_department) > 0
assert settings.DATA_DIR == Path("/data")
assert Path(settings.DATABASES["default"]["NAME"]) == Path("/data/django.sqlite3")
root_ou = "OU=ADOrgSync-SyncTest-8fa5fcdf,DC=tianjitest,DC=com"
assert under(root_ou, settings.LDAP_BASE_DN) and settings.LDAP_VERIFY_CERT is False

username = "cxsource" + run_id[-8:]
employee_id = "CXR" + run_id[-8:]
before_name = "Codex Source Before " + run_id[-8:]
move_ou = "OU=CodexSourceMove" + run_id[-8:] + "," + root_ou

absence_confirmed = False
real_before = None
real_guid = None
move_guid = None
try:
    call_command("migrate", interactive=False, verbosity=0)
    config = Configuration.current()
    config.root_department = root_department
    config.root_ou = root_ou
    config.naming = "employee_id"
    config.match_field = "employee_id"
    config.attributes = ["displayName"]
    config.disable_missing = False
    config.schedule_enabled = False
    config.sspr_enabled = False
    config.save()
    assert not Person.objects.exists() and not Binding.objects.exists()

    with closing(DingTalk()) as source:
        user = source.user(source_id)
        assert user["source_id"] == source_id and user["employee_id"] == "T0001919"
        assert user["name"] and user["name"] != before_name
        assert source.user_in_scope(user, root_department)
        primary = user["primary_department"]
        assert primary and primary in user["departments"]
        chain, current = [], primary
        while True:
            assert current.isdecimal() and int(current) > 0
            assert current not in chain and len(chain) < 100
            chain.append(current)
            if current == root_department:
                break
            detail = source.call("/topapi/v2/department/get", {"dept_id": int(current)})
            assert isinstance(detail, dict) and str(detail.get("dept_id")) == current
            current = str(detail.get("parent_id", ""))
    print("live_dingtalk_identity_and_scope_verified=true", flush=True)

    with closing(ActiveDirectory()) as ad:
        root_guid = ad.verify_ou(root_ou)
        real_matches = ad.match("employee_id", "T0001919")
        assert len(real_matches) == 1
        real_before = real_matches[0]
        real_guid = real_before["guid"]
        assert real_before["enabled"] and not real_before["protected"]
        assert not ad.match("employee_id", employee_id)
        assert not ad.match("source_id", username)
        assert ad.ou_identity(move_ou) is None
    absence_confirmed = True
    print("test_identity_absent_and_real_ad_account_read_only=true", flush=True)

    with closing(ActiveDirectory()) as ad:
        synthetic = ad.create(
            {"name": before_name, "employee_id": employee_id},
            username, root_ou, root_ou, enabled=True, require_change=False,
        )
        assert synthetic["enabled"] and synthetic["attrs"]["displayName"] == before_name
        synthetic_guid = synthetic["guid"]
        assert synthetic_guid != real_guid and under(synthetic["dn"], root_ou)
    print("disposable_ad_account_created_in_test_ou=true", flush=True)

    person = Person.objects.create(source_id=source_id, name=user["name"], primary_department=primary)
    for department_id in chain:
        DepartmentBinding.objects.create(source_id=department_id, dn=root_ou, object_guid=root_guid, manual=True)
    review = synchronization.binding_review(person.pk, username)
    assert review["target"]["guid"] == synthetic_guid
    synchronization.bind_person(person.pk, review["confirmation"], "acceptance", "隔离测试账号关联")
    binding = Binding.objects.get(person=person)
    assert binding.manual and binding.enabled and str(binding.object_guid) == synthetic_guid
    print("live_source_manual_binding_to_disposable_ad_verified=true", flush=True)

    preview = synchronization.enqueue(kind="preview", scope="users", selected=[source_id], actor="acceptance")
    assert synchronization.run_next()
    preview.refresh_from_db()
    assert preview.status == "preview_ready", (preview.status, preview.message)
    operations = preview.plan["operations"]
    assert len(operations) == 1 and operations[0]["source_id"] == source_id
    assert operations[0]["action"] == "update" and operations[0]["target"]["guid"] == synthetic_guid
    assert operations[0]["attrs"]["displayName"] == user["name"]
    assert operations[0]["ou"].casefold() == root_ou.casefold()
    assert all(department["guid"] == root_guid for department in preview.plan["departments"])
    assert not preview.plan["high_risk"]
    print("live_dingtalk_source_preview_targets_only_disposable_ad=true", flush=True)

    synchronization.queue_apply(preview.pk, "acceptance")
    assert synchronization.run_next()
    preview.refresh_from_db()
    assert preview.status == "success", (preview.status, preview.message)
    with closing(ActiveDirectory()) as ad:
        updated = ad.by_guid(synthetic_guid)
        assert updated["username"] == username and updated["employee_id"] == employee_id
        assert updated["attrs"]["displayName"] == user["name"]
        assert updated["enabled"] and under(updated["dn"], root_ou)
        assert len(ad.match("employee_id", employee_id)) == 1
        assert fingerprint(ad.by_guid(real_guid)) == fingerprint(real_before)
    binding.refresh_from_db()
    assert str(binding.object_guid) == synthetic_guid and binding.enabled
    print("real_dingtalk_source_updated_disposable_ad_same_guid=true", flush=True)

    with closing(ActiveDirectory()) as ad:
        move_guid = ad.ensure_ou(move_ou, root_ou)
    department_binding = DepartmentBinding.objects.get(source_id=primary)
    department_binding.dn = move_ou
    department_binding.object_guid = move_guid
    department_binding.save(update_fields=["dn", "object_guid"])

    move_preview = synchronization.enqueue(kind="preview", scope="users", selected=[source_id], actor="acceptance")
    assert synchronization.run_next()
    move_preview.refresh_from_db()
    assert move_preview.status == "preview_ready", (move_preview.status, move_preview.message)
    move_operations = move_preview.plan["operations"]
    assert len(move_operations) == 1
    assert move_operations[0]["action"] == "move"
    assert move_operations[0]["target"]["guid"] == synthetic_guid
    assert move_operations[0]["ou"].casefold() == move_ou.casefold()
    assert not move_preview.plan["high_risk"]
    with closing(ActiveDirectory()) as ad:
        assert synchronization.parent_dn(ad.by_guid(synthetic_guid)["dn"]).casefold() == root_ou.casefold()
        assert fingerprint(ad.by_guid(real_guid)) == fingerprint(real_before)
    print("live_source_mapping_move_preview_read_only=true", flush=True)

    synchronization.queue_apply(move_preview.pk, "acceptance")
    assert synchronization.run_next()
    move_preview.refresh_from_db()
    assert move_preview.status == "success", (move_preview.status, move_preview.message)
    with closing(ActiveDirectory()) as ad:
        moved = ad.by_guid(synthetic_guid)
        assert moved["username"] == username and moved["employee_id"] == employee_id
        assert synchronization.parent_dn(moved["dn"]).casefold() == move_ou.casefold()
        assert fingerprint(ad.by_guid(real_guid)) == fingerprint(real_before)
    binding.refresh_from_db()
    assert str(binding.object_guid) == synthetic_guid and binding.enabled
    print("real_dingtalk_source_mapping_moved_disposable_ad_same_guid=true", flush=True)
finally:
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
            if move_guid is not None:
                ad.verify_ou(move_ou, move_guid)
                contents = ad.search("(objectClass=*)", base=move_ou)
                assert len(contents) == 1 and contents[0]["dn"].casefold() == move_ou.casefold()
                assert ad.conn.delete(move_ou), "disposable move OU cleanup failed"
                assert ad.ou_identity(move_ou) is None
            if real_before is not None:
                assert fingerprint(ad.by_guid(real_guid)) == fingerprint(real_before)
        print("disposable_ad_removed_real_employee_ad_unchanged=true", flush=True)
