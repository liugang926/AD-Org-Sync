import pytest
from django.conf import settings
from sync_app.models import Configuration
from sync_app.security import client_address


def test_client_address_uses_proxy_header_and_validates_it(rf):
    request = rf.get("/", REMOTE_ADDR="172.18.0.4", HTTP_X_REAL_IP="198.51.100.17")
    assert client_address(request) == "198.51.100.17"
    request = rf.get("/", REMOTE_ADDR="172.18.0.4", HTTP_X_REAL_IP="spoofed, 198.51.100.17")
    assert client_address(request) == "172.18.0.4"


@pytest.mark.django_db
def test_administrator_pages_require_login(client):
    for path in ["/dashboard", "/people", "/departments", "/logs"]:
        assert client.get(path).status_code == 302
    assert client.get("/sspr").status_code == 200
    assert client.get("/sspr/callback/dingtalk").status_code == 200


@pytest.mark.django_db
def test_dingtalk_workbench_homepage_alias_renders_employee_verification(client, settings):
    settings.DINGTALK_CORP_ID = "ding-test-corp"
    settings.DINGTALK_APP_KEY = "test-client-id"
    config = Configuration.current()
    config.sspr_enabled = True
    config.save()
    for path in ("/sspr", "/sspr/callback/dingtalk"):
        response = client.get(path)
        assert response.status_code == 200
        html = response.content.decode()
        assert 'id="verify"' in html
        assert 'data-corp="ding-test-corp"' in html
        assert 'data-client="test-client-id"' in html
        assert f'/static/sspr.js?v={settings.SSPR_SCRIPT_VERSION}' in html


@pytest.mark.django_db
def test_pages_and_readiness(admin_client):
    Configuration.current()
    (settings.DATA_DIR / "worker-heartbeat").touch()
    for path in ["/dashboard", "/people", "/departments", "/logs", "/admin/sync_app/configuration/1/change/"]:
        assert admin_client.get(path).status_code == 200
    assert admin_client.get("/admin/")["Location"] == "/dashboard"
    assert admin_client.get("/admin/sync_app/departmentbinding/")["Location"] == "/departments"
    assert admin_client.get("/login")["Location"] == "/dashboard"
    assert admin_client.get("/healthz").status_code == 200
    assert admin_client.get("/readyz").json()["checks"] == {"database": True, "schema": True, "worker": True}


@pytest.mark.django_db
@pytest.mark.parametrize("scope,selected,expected", [
    ("full", "", []),
    ("department", "2 3", ["2", "3"]),
    ("users", "u1 u2", ["u1", "u2"]),
])
def test_dashboard_preview_requires_csrf_and_only_queues_selected_scope(admin_user, monkeypatch, scope, selected, expected):
    from django.test import Client
    from unittest.mock import Mock
    from sync_app import synchronization as sync
    from sync_app.models import Binding, Job, Operation, Snapshot
    from .fakes import Directory, Source

    source_factory = Mock(return_value=Source())
    ad_factory = Mock(return_value=Directory())
    monkeypatch.setattr(sync, "DingTalk", source_factory)
    monkeypatch.setattr(sync, "ActiveDirectory", ad_factory)
    client = Client(enforce_csrf_checks=True)
    client.force_login(admin_user)
    assert client.get("/dashboard").status_code == 200
    data = {"scope": scope, "selected": selected}
    assert client.post("/dashboard", data).status_code == 403
    assert not Job.objects.exists()
    data["csrfmiddlewaretoken"] = client.cookies["csrftoken"].value
    response = client.post("/dashboard", data)
    assert response.status_code == 302 and response["Location"] == "/dashboard"
    job = Job.objects.get()
    assert (job.kind, job.status, job.scope, job.selected, job.actor) == (
        "preview", "queued", scope, expected, admin_user.username,
    )
    assert client.post("/dashboard", data).status_code == 302
    assert Job.objects.count() == 1
    source_factory.assert_not_called()
    ad_factory.assert_not_called()
    assert not Binding.objects.exists() and not Operation.objects.exists() and not Snapshot.objects.exists()


@pytest.mark.django_db
def test_dashboard_preview_with_unset_root_ou_plans_creation_without_directory_writes(admin_user, monkeypatch):
    from django.test import Client
    from unittest.mock import Mock
    from sync_app import synchronization as sync
    from sync_app.models import Binding, Job, Operation, Snapshot
    from .fakes import Directory, Source

    config = Configuration.current()
    assert not config.root_ou
    source, ad = Source(), Directory()
    source.collect = Mock(wraps=source.collect)
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    client = Client(enforce_csrf_checks=True)
    client.force_login(admin_user)
    page = client.get("/dashboard")
    assert "自动确定" in page.content.decode()
    assert client.post("/dashboard", {
        "csrfmiddlewaretoken": client.cookies["csrftoken"].value, "scope": "organization",
    }).status_code == 302
    assert sync.run_next()
    job = Job.objects.get()
    assert job.status == "preview_ready"
    assert job.plan["root_ou"] == "OU=公司,DC=example,DC=com"
    assert job.plan["root_parent"]
    assert job.plan["operations"] == []
    assert job.message in client.get("/dashboard").content.decode()
    source.collect.assert_called_once()
    assert ad.created == ad.resets == 0 and ad.disabled == []
    assert not Binding.objects.exists() and not Operation.objects.exists() and Snapshot.objects.count() == 1
    detail = client.get(f"/jobs/{job.pk}")
    assert "根 OU 待创建" in detail.content.decode()


@pytest.mark.django_db
def test_department_console_prioritizes_names_and_filters_mapping_state(admin_client):
    import uuid
    from sync_app.models import DepartmentBinding, Snapshot

    Snapshot.objects.create(
        fingerprint="complete-source", root_department="1", users=[],
        departments=[{"id": "1", "name": "公司"}, {"id": "2", "name": "研发"}],
    )
    DepartmentBinding.objects.create(source_id="2", dn="OU=研发,OU=同步,DC=example,DC=com", object_guid=uuid.uuid4())
    response = admin_client.get("/departments")
    html = response.content.decode()
    assert response.status_code == 200
    assert "研发" in html and "公司" in html
    assert "查看完整 DN" in html
    assert "已映射" in html and "待映射" in html
    assert "部门 ID：2" in html
    assert [row["status"] for row in admin_client.get("/departments?status=unmapped").context["page"].object_list] == ["unmapped"]
    assert [row["source_id"] for row in admin_client.get("/departments?q=研发").context["page"].object_list] == ["2"]


@pytest.mark.django_db
def test_people_can_find_employee_by_dingtalk_employee_id(admin_client):
    from sync_app.models import Person, Snapshot

    Snapshot.objects.create(
        fingerprint="complete-source", root_department="1", departments=[],
        users=[
            {"source_id": "ding-a", "name": "甲", "employee_id": "T0001919", "departments": []},
            {"source_id": "ding-b", "name": "乙", "employee_id": "T0001920", "departments": []},
        ],
    )
    Person.objects.create(source_id="ding-a", name="甲")
    Person.objects.create(source_id="ding-b", name="乙")

    response = admin_client.get("/people?q=t0001919")
    assert response.status_code == 200
    assert [person.source_id for person in response.context["page"].object_list] == ["ding-a"]
    assert "按姓名、工号或钉钉 userId 搜索" in response.content.decode()


@pytest.mark.django_db
def test_configuration_editor_uses_guided_chinese_fields(admin_client):
    Configuration.current()
    html = admin_client.get("/admin/sync_app/configuration/1/change/").content.decode()
    assert "同步边界" in html and "员工自助重置" in html
    assert "同步到 AD 的属性" in html
    assert 'name="attributes"' in html


@pytest.mark.django_db
def test_configuration_editor_saves_attribute_choices_and_protected_accounts():
    from sync_app.admin import ConfigurationForm

    form = ConfigurationForm(data={
        "root_department": "1", "root_ou": "OU=Sync,DC=example,DC=com",
        "match_field": "employee_id", "naming": "employee_id",
        "attributes": ["displayName", "mail"], "clear_attributes": ["mail"],
        "protected_usernames": "svc-sync\nshared-admin\nsvc-sync",
        "disable_limit": 5, "disable_percent": 10, "sspr_match": "employee_id",
        "minimum_password_length": 8, "interval_minutes": 60,
    }, instance=Configuration.current())
    assert form.is_valid(), form.errors
    saved = form.save()
    assert saved.attributes == ["displayName", "mail"]
    assert saved.clear_attributes == ["mail"]
    assert saved.protected_usernames == ["svc-sync", "shared-admin"]
    assert saved.minimum_password_length == 8
    too_short = ConfigurationForm(data={**form.data, "minimum_password_length": 7}, instance=saved)
    assert not too_short.is_valid()
    assert "minimum_password_length" in too_short.errors


@pytest.mark.django_db
def test_settings_form_loaded_before_collection_preserves_new_identity_anchor(rf):
    from types import SimpleNamespace
    from django.contrib.admin import site
    from sync_app.admin import ConfigurationAdmin
    from sync_app.synchronization import collect, directory_identity_anchor
    from .fakes import Source

    old_settings = Configuration.current()
    assert not old_settings.identity_anchor
    collect(Source(), Configuration.current())
    old_settings.minimum_password_length = 10
    request = rf.post("/")
    request.user = SimpleNamespace(username="admin")
    ConfigurationAdmin(Configuration, site).save_model(request, old_settings, None, True)

    current = Configuration.current()
    assert current.minimum_password_length == 10
    assert current.identity_anchor == directory_identity_anchor()
    assert collect(Source(), current).users[0]["source_id"] == "u1"


@pytest.mark.django_db
def test_non_admin_cannot_mutate(client, django_user_model):
    person = django_user_model.objects.create_user("reader", password="randomlongpassword")
    client.force_login(person)
    assert client.post("/dashboard", {"scope": "full"}).status_code == 403


@pytest.mark.django_db
def test_admin_builtin_login_cannot_bypass_rate_limit(client):
    for _ in range(10):
        assert client.post("/login", {"username": "unknown", "password": "incorrect"}, HTTP_X_REAL_IP="198.51.100.17").status_code == 200
    assert client.post("/admin/login/", {"username": "unknown", "password": "incorrect"}, HTTP_X_REAL_IP="198.51.100.17").status_code == 429
    assert client.post("/login", {"username": "unknown", "password": "incorrect"}, HTTP_X_REAL_IP="198.51.100.18").status_code == 200


@pytest.mark.django_db
def test_binding_requires_review_before_mutation(admin_client, monkeypatch):
    from sync_app import synchronization as sync
    from sync_app.models import Person, Binding
    from .fakes import Source, Directory
    config = Configuration.current()
    config.root_ou = "OU=People,DC=example,DC=com"
    config.save()
    ad = Directory()
    monkeypatch.setattr(sync, "DingTalk", Source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    person = Person.objects.create(source_id="u1", name="测试员工")
    response = admin_client.post(f"/people/{person.pk}", {"action": "bind", "username": "testuser", "reason": "核对工号"})
    assert response.status_code == 200
    assert not Binding.objects.exists()
    confirmation = response.context["confirmation"]
    response = admin_client.post(f"/people/{person.pk}", {"action": "confirm_bind", "confirmation": confirmation, "reason": "核对工号"})
    assert response.status_code == 302
    assert Binding.objects.get().username == "testuser"


@pytest.mark.django_db
def test_primary_department_is_chosen_from_current_employee_departments(admin_client):
    from sync_app.models import Configuration, Person, Snapshot
    from sync_app.synchronization import directory_identity_anchor
    from .fakes import user

    Configuration.objects.update_or_create(pk=1, defaults={"identity_anchor": directory_identity_anchor()})
    employee = user()
    employee["departments"] = ["1", "2", "999"]
    employee["primary_department"] = ""
    Snapshot.objects.create(
        fingerprint="complete-source", root_department="1", users=[employee],
        departments=[{"id": "1", "name": "公司"}, {"id": "2", "name": "研发"}],
    )
    person = Person.objects.create(source_id=employee["source_id"], name=employee["name"])
    page = admin_client.get("/people")
    assert page.status_code == 200
    assert 'name="department"' in page.content.decode()
    assert 'value="2"' in page.content.decode()
    assert 'value="999"' not in page.content.decode()
    assert "研发（2）" in page.content.decode()

    invalid = admin_client.post(f"/people/{person.pk}", {"action": "policy", "department": "999", "excluded": "on"})
    assert invalid.status_code == 302 and invalid["Location"] == "/people"
    person.refresh_from_db()
    assert person.primary_department == "" and not person.excluded

    valid = admin_client.post(f"/people/{person.pk}", {"action": "policy", "department": "2"})
    assert valid.status_code == 302
    person.refresh_from_db()
    assert person.primary_department == "2"


@pytest.mark.django_db
def test_department_mapping_rejects_changed_or_missing_directory_anchor(settings, monkeypatch):
    from sync_app import admin as admin_module
    from sync_app.models import Configuration, Snapshot, DepartmentBinding
    from sync_app.synchronization import directory_identity_anchor
    from .fakes import Directory

    config = Configuration.current()
    config.root_ou = "OU=People,DC=example,DC=com"
    config.identity_anchor = directory_identity_anchor()
    config.save()
    Snapshot.objects.create(fingerprint="source", root_department="1", users=[], departments=[{"id": "1", "name": "公司"}])
    calls = []

    def directory_factory():
        calls.append("AD connection")
        return Directory()

    monkeypatch.setattr(admin_module, "ActiveDirectory", directory_factory)
    data = {"source_id": "1", "dn": config.root_ou}
    valid = admin_module.DepartmentForm(data=data)
    assert valid.is_valid(), valid.errors
    assert calls == ["AD connection"]
    calls.clear()

    settings.DINGTALK_CORP_ID = "replacement-enterprise"
    changed = admin_module.DepartmentForm(data=data)
    assert not changed.is_valid() and "企业或 AD 目录已更换" in str(changed.errors)
    config.identity_anchor = ""
    config.save()
    missing = admin_module.DepartmentForm(data=data)
    assert not missing.is_valid() and "缺少目录身份锚点" in str(missing.errors)
    assert not DepartmentBinding.objects.exists() and not calls


@pytest.mark.django_db
def test_department_mapping_rechecks_identity_at_save(rf, settings, monkeypatch):
    from django.contrib.admin import site
    from sync_app import admin as admin_module
    from sync_app.domain import RuleError
    from sync_app.models import Configuration, Snapshot, DepartmentBinding, Audit
    from sync_app.synchronization import directory_identity_anchor
    from .fakes import Directory

    config = Configuration.current()
    config.root_ou = "OU=People,DC=example,DC=com"
    config.identity_anchor = directory_identity_anchor()
    config.save()
    Snapshot.objects.create(fingerprint="source", root_department="1", users=[], departments=[{"id": "1", "name": "公司"}])
    calls = []

    def directory_factory():
        calls.append("AD connection")
        return Directory()

    monkeypatch.setattr(admin_module, "ActiveDirectory", directory_factory)
    form = admin_module.DepartmentForm(data={"source_id": "1", "dn": config.root_ou})
    assert form.is_valid(), form.errors
    obj = form.save(commit=False)
    calls.clear()
    settings.LDAP_HOST = "replacement-ad"
    with pytest.raises(RuleError, match="企业或 AD 目录已更换"):
        admin_module.DepartmentAdmin(DepartmentBinding, site).save_model(rf.post("/"), obj, form, False)
    assert not DepartmentBinding.objects.exists() and not Audit.objects.exists() and not calls


@pytest.mark.django_db
def test_job_conflicts_can_be_filtered_and_opened_in_people(admin_client):
    from sync_app.models import Job

    job = Job.objects.create(status="blocked", plan={"operations": [
        {"source_id": "u/conflict", "user": {"name": "冲突员工"}, "action": "conflict", "reason": "主部门不明确"},
        {"source_id": "u-ok", "user": {"name": "正常员工"}, "action": "bind", "reason": "唯一工号匹配"},
    ]})
    response = admin_client.get(f"/jobs/{job.pk}?only=conflicts")
    content = response.content.decode()
    assert response.status_code == 200
    assert "人员冲突 1 项" in content
    assert "冲突员工" in content and "正常员工" not in content
    assert "/people?q=u/conflict" in content
    assert admin_client.get(f"/jobs/{job.pk}").content.decode().count("正常员工") == 1
    job.status = "preview_ready"
    job.save(update_fields=["status"])
    assert "执行此计划" not in admin_client.get(f"/jobs/{job.pk}").content.decode()


@pytest.mark.django_db
def test_job_plan_pages_large_lists_and_exposes_department_and_disable_actions(admin_client):
    from sync_app.models import Job, Operation

    operations = [
        {"source_id": f"u-{index:03}", "user": {"name": f"员工 {index:03}"}, "action": "bind", "reason": "唯一工号匹配"}
        for index in range(105)
    ]
    operations.append({"source_id": "u-left", "user": {"name": "离职员工"}, "action": "disable", "reason": "全量缺失"})
    job = Job.objects.create(status="blocked", plan={
        "operations": operations,
        "departments": [{"source_id": "42", "name": "研发部", "action": "conflict", "reason": "OU 已变化"}],
        "high_risk": True,
    })

    first = admin_client.get(f"/jobs/{job.pk}")
    assert first.status_code == 200
    assert first.context["planned_page"].paginator.count == 106
    assert len(first.context["planned_rows"]) == 50
    assert "员工 000" in first.content.decode()
    assert "员工 050" not in first.content.decode()
    assert "部门冲突" in first.content.decode()
    assert "/departments?q=42" in first.content.decode()
    assert "?only=disables#person-plan" in first.content.decode()
    assert "执行此计划" not in first.content.decode()

    last = admin_client.get(f"/jobs/{job.pk}?page=3")
    assert last.context["planned_page"].number == 3
    assert "离职员工" in last.content.decode()
    assert "?only=&amp;page=2#person-plan" in last.content.decode()

    disables = admin_client.get(f"/jobs/{job.pk}?only=disables")
    assert disables.context["planned_page"].paginator.count == 1
    assert "离职员工" in disables.content.decode()
    assert "员工 000" not in disables.content.decode()

    no_conflicts = admin_client.get(f"/jobs/{job.pk}?only=conflicts")
    assert "没有人员冲突" in no_conflicts.content.decode()
    assert f'href="/jobs/{job.pk}#person-plan"' in no_conflicts.content.decode()

    Operation.objects.bulk_create([
        Operation(job=job, source_id=f"result-{index:03}", action="bind", status="success")
        for index in range(101)
    ])
    results_first = admin_client.get(f"/jobs/{job.pk}?only=disables")
    assert len(results_first.context["operation_rows"]) == 50
    assert "result-050" not in results_first.content.decode()
    assert "result_page=2#execution-results" in results_first.content.decode()
    results_last = admin_client.get(f"/jobs/{job.pk}?only=disables&result_page=3")
    assert len(results_last.context["operation_rows"]) == 1
    assert "result-100" in results_last.content.decode()
    assert "only=disables&amp;page=1&amp;result_page=2#execution-results" in results_last.content.decode()


@pytest.mark.django_db
def test_dashboard_reports_finished_preview_instead_of_waiting(admin_client, monkeypatch):
    from sync_app import synchronization as sync
    from sync_app.models import Binding, Configuration, Operation

    from .fakes import Directory, Source, user

    config = Configuration.current()
    config.root_ou = "OU=People,DC=example,DC=com"
    config.save()
    current = {"source": Source()}
    monkeypatch.setattr(sync, "DingTalk", lambda: current["source"])
    monkeypatch.setattr(sync, "ActiveDirectory", Directory)

    ready = sync.enqueue(kind="preview", actor="admin")
    assert sync.run_next()
    ready.refresh_from_db()
    assert ready.status == "preview_ready"
    assert "预览完成" in ready.message
    assert ready.message in admin_client.get("/dashboard").content.decode()

    current["source"] = Source([user("u2", "")])
    blocked = sync.enqueue(kind="preview", actor="admin")
    assert sync.run_next()
    blocked.refresh_from_db()
    assert blocked.status == "blocked"
    assert "1 项人员冲突" in blocked.message
    dashboard = admin_client.get("/dashboard").content.decode()
    assert blocked.message in dashboard
    assert "等待任务结果" not in dashboard
    assert not Binding.objects.exists()
    assert not Operation.objects.filter(job__in=[ready, blocked]).exists()


@pytest.mark.django_db
def test_dashboard_keeps_latest_full_plan_conflicts_visible_after_other_tasks(admin_client):
    from sync_app.models import Job

    blocked = Job.objects.create(
        kind="preview", status="blocked", scope="full",
        plan={
            "operations": [{"action": "conflict"}],
            "departments": [{"action": "conflict"}],
        },
    )
    Job.objects.create(kind="refresh", status="success", scope="full")
    Job.objects.create(
        kind="preview", status="preview_ready", scope="users",
        plan={"operations": [{"action": "bind"}], "departments": []},
    )

    response = admin_client.get("/dashboard")
    assert response.context["latest_full_plan"] == blocked
    assert response.context["conflict_count"] == 2
    assert f'href="/jobs/{blocked.pk}"' in response.content.decode()

    current = Job.objects.create(
        kind="preview", status="preview_ready", scope="full",
        plan={"operations": [{"action": "bind"}], "departments": []},
    )
    response = admin_client.get("/dashboard")
    assert response.context["latest_full_plan"] == current
    assert response.context["conflict_count"] == 0


@pytest.mark.django_db
def test_held_disabled_account_can_be_reviewed_for_reactivation(admin_client, monkeypatch):
    from sync_app import synchronization as sync
    from sync_app.models import Person, Binding
    from .fakes import Directory, account

    target = account()
    target["enabled"] = False
    ad = Directory([target])
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    Configuration.objects.update_or_create(pk=1, defaults={"identity_anchor": sync.directory_identity_anchor()})
    person = Person.objects.create(source_id="u1", name="测试员工")
    Binding.objects.create(person=person, object_guid=target["guid"], username=target["username"], enabled=False)
    response = admin_client.post(f"/people/{person.pk}", {"action": "verify"})
    assert response.status_code == 200
    assert "恢复 AD 账号及同步绑定" in response.content.decode()


@pytest.mark.django_db
def test_audit_filters_and_invalid_date(admin_client):
    from sync_app.models import Audit
    Audit.objects.create(actor="admin", action="sample_ok", success=True)
    Audit.objects.create(actor="admin", action="sample_failed", success=False)
    Audit.objects.create(actor="employee", action="sample_partial", success=False, state="partial")
    Audit.objects.create(actor="employee", action="sample_pending", success=False, state="pending")
    Audit.objects.create(actor="employee", action="sample_unknown", success=False, state="unknown")
    response = admin_client.get("/logs?result=failed")
    assert [entry.action for entry in response.context["page"]] == ["sample_failed"]
    assert response.context["audit_rows"][0]["status"] == "失败"
    response = admin_client.get("/logs?result=partial")
    assert [entry.action for entry in response.context["page"]] == ["sample_partial"]
    assert response.context["audit_rows"][0]["status"] == "部分完成"
    response = admin_client.get("/logs?result=attention")
    assert [entry.action for entry in response.context["page"]] == ["sample_unknown", "sample_pending"]
    assert [row["status"] for row in response.context["audit_rows"]] == ["结果不明", "处理中 / 未完成"]
    assert "待确认或未完成" in response.content.decode()
    response = admin_client.get("/logs?result=pending")
    assert [entry.action for entry in response.context["page"]] == ["sample_pending"]
    assert response.context["audit_rows"][0]["tone"] == "active"
    response = admin_client.get("/logs?result=unknown")
    assert [entry.action for entry in response.context["page"]] == ["sample_unknown"]
    response = admin_client.get("/logs?start=2026-99-99")
    assert response.status_code == 200
    assert list(response.context["page"]) == []


@pytest.mark.django_db
def test_audit_password_reset_snapshots_are_searchable_and_escaped(admin_client):
    from datetime import datetime, timezone as datetime_timezone
    from sync_app.models import Audit, Person

    request_time = datetime(2026, 9, 30, 1, 2, 3, tzinfo=datetime_timezone.utc)
    record = Audit.objects.create(
        actor="ding-user-1919", actor_name="测试员工<script>alert(1)</script>",
        employee_id="T0001919", action="sspr_reset", target_username="test.ad",
        target="d0a00000-1111-2222-3333-444444444444", state="failed", success=False,
        result="密码不符合 AD 密码策略：<script>alert(2)</script>",
        completed_at=request_time.replace(second=5), client_ip="198.51.100.17",
    )
    Audit.objects.filter(pk=record.pk).update(created_at=request_time)
    # Current directory data must not replace the operation's historical snapshot.
    Person.objects.create(source_id=record.actor, name="后来修改的姓名")
    Audit.objects.create(actor="admin", action="settings", result="配置已更新")
    for query in ("测试员工", "T0001919", "ding-user-1919", "TEST.AD", "d0a00000-1111"):
        response = admin_client.get("/logs", {"action": "sspr_reset", "q": query})
        assert [item.pk for item in response.context["page"]] == [record.pk]
    html = response.content.decode()
    assert "2026-09-30 09:02:03" in html and "完成：2026-09-30 09:02:05" in html
    assert "北京时间（Asia/Shanghai，UTC+08:00）" in html
    for value in ("工号：T0001919", "userId：ding-user-1919", "test.ad", "198.51.100.17", "密码不符合 AD 密码策略"):
        assert value in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert "<script>alert(" not in html and "后来修改的姓名" not in html
    assert list(admin_client.get("/logs?q=后来修改的姓名").context["page"]) == []


@pytest.mark.django_db
def test_audit_password_reset_incomplete_results_are_distinct(admin_client):
    from sync_app.models import Audit

    for state in ("pending", "unknown", "partial"):
        Audit.objects.create(actor="ding-user", action="sspr_reset", success=False, state=state)
    response = admin_client.get("/logs?action=sspr_reset&result=failed")
    assert list(response.context["page"]) == []
    html = admin_client.get("/logs?action=sspr_reset").content.decode()
    for value in ("处理中 / 未完成", "结果不明", "部分完成", "密码已修改，解锁未完成。", "完成：—", "工号：—", "AD GUID：—"):
        assert value in html
    Audit.objects.create(actor="anonymous", action="sspr_auth_failed", success=False, state="failed", result="授权码无效")
    assert "未验证访客" in admin_client.get("/logs?action=sspr_auth_failed").content.decode()


@pytest.mark.django_db
def test_employee_http_denials_are_audited_once_without_client_supplied_identity(client, admin_client):
    from sync_app.models import Audit

    forged = {"actor": "forged-user", "employee_id": "forged-employee", "target_username": "forged-account"}
    response = client.post("/sspr/auth/dingtalk", forged)
    assert response.status_code == 400
    denied = Audit.objects.get(action="sspr_auth_failed")
    assert denied.actor == "未验证访客" and denied.state == "failed"
    assert denied.result == "缺少有效钉钉授权码"
    assert not denied.employee_id and not denied.target_username
    assert denied.completed_at is not None

    response = client.post("/sspr/reset", {**forged, "password": "Never-audit-this-password!", "confirmation": "Never-audit-this-password!"})
    assert response.status_code == 400
    assert Audit.objects.filter(action="sspr_reset").count() == 1
    denied = Audit.objects.get(action="sspr_reset")
    assert denied.actor == "未验证访客" and denied.state == "failed"
    html = admin_client.get("/logs?action=sspr_reset").content.decode()
    assert "<strong>未验证访客</strong>" in html and "验证已失效" in html
    assert "userId：—" in html and "userId：未验证访客" not in html
    assert "forged-" not in html and "Never-audit-this-password!" not in html


@pytest.mark.django_db
def test_audit_pagination_retains_search_operation_date_and_result(admin_client):
    from datetime import datetime, timezone as datetime_timezone
    from urllib.parse import parse_qs
    from sync_app.models import Audit

    Audit.objects.bulk_create([
        Audit(actor="u1", actor_name="A&B", action="sspr_reset", state="success", result="密码已修改")
        for _ in range(51)
    ])
    Audit.objects.update(created_at=datetime(2026, 9, 30, 3, tzinfo=datetime_timezone.utc))
    filters = {"q": "A&B", "action": "sspr_reset", "result": "success", "start": "2026-09-30", "end": "2026-09-30"}
    response = admin_client.get("/logs", filters)
    assert response.context["page"].paginator.count == 51
    assert len(response.context["page"]) == 50
    assert parse_qs(response.context["filter_query"]) == {key: [value] for key, value in filters.items()}
    assert "q=A%26B" in response.content.decode() and "page=2" in response.content.decode()
    response = admin_client.get("/logs", {**filters, "page": 2})
    assert len(response.context["page"]) == 1 and response.context["page"].number == 2


@pytest.mark.django_db
def test_django_audit_admin_search_and_details_remain_read_only(admin_client):
    from django.contrib import admin
    from sync_app.models import Audit

    record = Audit.objects.create(
        actor="ding-user-1919", actor_name="测试员工", employee_id="T0001919", action="sspr_reset",
        target_username="test.ad", state="unknown", success=False, result="连接中断，无法确认写入结果",
        client_ip="198.51.100.17",
    )
    response = admin_client.get("/admin/sync_app/audit/", {"q": "T0001919"})
    assert response.status_code == 200
    assert [item.pk for item in response.context["cl"].result_list] == [record.pk]
    assert "结果不明" in response.content.decode()
    response = admin_client.get(f"/admin/sync_app/audit/{record.pk}/change/")
    assert response.status_code == 200
    html = response.content.decode()
    assert "完成时间（北京时间）" in html and "test.ad" in html and "198.51.100.17" in html
    model_admin = admin.site._registry[Audit]
    assert not model_admin.has_add_permission(response.wsgi_request)
    assert not model_admin.has_change_permission(response.wsgi_request, record)
    assert not model_admin.has_delete_permission(response.wsgi_request, record)
    assert admin_client.post(f"/admin/sync_app/audit/{record.pk}/change/", {"result": "覆盖审计"}).status_code == 403
    record.refresh_from_db()
    assert record.result == "连接中断，无法确认写入结果"
