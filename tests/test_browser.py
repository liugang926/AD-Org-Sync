from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pytest
from playwright.sync_api import sync_playwright


@pytest.mark.django_db(transaction=True)
def test_dashboard_updates_task_without_losing_input_and_retries_network_errors(live_server, django_user_model):
    from django.utils import timezone
    from playwright.sync_api import expect
    from sync_app.models import Job

    django_user_model.objects.create_superuser("live-task-admin", password="Browser-test-only-823!")
    job = Job.objects.create(kind="associate", status="queued")
    output = Path("test_artifacts/browser")
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright, ThreadPoolExecutor(max_workers=1) as executor:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.goto(live_server.url + "/login")
        page.get_by_label("用户名").fill("live-task-admin")
        page.get_by_label("密码").fill("Browser-test-only-823!")
        page.get_by_role("button", name="登录", exact=True).click()
        page.wait_for_url("**/dashboard")
        page.clock.install()
        page.reload()
        page.get_by_label("同步范围", exact=True).select_option("department")
        page.get_by_label("部门 ID 或人员 userId", exact=True).fill("keep-this-input")
        row = page.locator(f'[data-job-id="{job.pk}"]')
        expect(row.locator("[data-job-status]")).to_have_text("排队中")
        page.route("**/jobs/status?*", lambda route: route.fulfill(status=503, body="temporary failure"), times=1)
        page.clock.run_for(5000)
        expect(page.locator("[data-poll-status]")).to_contain_text("暂时无法更新")
        expect(row.locator("[data-job-status]")).to_have_text("排队中")
        executor.submit(lambda: Job.objects.filter(pk=job.pk).update(status="running", started_at=timezone.now())).result()
        page.clock.run_for(5000)
        expect(row.locator("[data-job-status]")).to_have_text("执行中")
        expect(row.locator("[data-job-message]")).to_contain_text("不创建部门 OU")
        page.locator(".jobs-card").screenshot(path=str(output / "task-live-running.png"))
        executor.submit(lambda: Job.objects.filter(pk=job.pk).update(status="success", finished_at=timezone.now(), message="关联核验完成")).result()
        page.clock.run_for(5000)
        expect(row.locator("[data-job-status]")).to_have_text("已完成")
        expect(row.locator("[data-job-message]")).to_have_text("关联核验完成")
        expect(page.get_by_label("部门 ID 或人员 userId", exact=True)).to_have_value("keep-this-input")
        expect(page.get_by_label("同步范围", exact=True)).to_have_value("department")
        requests = []
        page.on("request", lambda request: requests.append(request.url))
        page.clock.run_for(15000)
        assert not any("/jobs/status" in url for url in requests)
        assert executor.submit(Job.objects.count).result() == 1
        executor.submit(lambda: Job.objects.filter(pk=job.pk).update(status="running", finished_at=None)).result()
        page.reload()
        page.context.clear_cookies()
        page.clock.run_for(5000)
        expect(page.locator("[data-poll-status]")).to_contain_text("登录已失效")
        expect(page.locator("[data-job-status]")).to_have_text("执行中")
        requests.clear()
        page.clock.run_for(15000)
        assert not any("/jobs/status" in url for url in requests)
        browser.close()


@pytest.mark.django_db(transaction=True)
def test_task_detail_reloads_completed_preview_without_applying_it(live_server, django_user_model, monkeypatch):
    from playwright.sync_api import expect
    from sync_app import synchronization as sync
    from sync_app.models import DepartmentBinding, Job
    from .test_ou_sync import TreeSource, StrictDirectory

    source, directory = TreeSource(), StrictDirectory()
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: directory)
    django_user_model.objects.create_superuser("live-preview-admin", password="Browser-test-only-823!")
    job = Job.objects.create(kind="preview", scope="organization")
    with sync_playwright() as playwright, ThreadPoolExecutor(max_workers=1) as executor:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.goto(live_server.url + "/login")
        page.get_by_label("用户名").fill("live-preview-admin")
        page.get_by_label("密码").fill("Browser-test-only-823!")
        page.get_by_role("button", name="登录", exact=True).click()
        page.wait_for_url("**/dashboard")
        page.clock.install()
        page.goto(live_server.url + f"/jobs/{job.pk}")
        expect(page.get_by_role("button", name="执行此计划", exact=True)).to_have_count(0)
        assert executor.submit(sync.run_next).result()
        page.clock.run_for(5000)
        expect(page.get_by_role("button", name="执行此计划", exact=True)).to_be_visible()
        expect(page.locator("[data-job-status]")).to_have_text("待审阅")
        assert not directory.writes
        assert not executor.submit(DepartmentBinding.objects.exists).result()
        assert executor.submit(lambda: Job.objects.get(pk=job.pk).kind).result() == "preview"
        browser.close()


@pytest.mark.django_db(transaction=True)
def test_organization_preview_and_confirmed_root_creation_browser(live_server, django_user_model, monkeypatch):
    from sync_app import synchronization as sync
    from sync_app.models import Binding, DepartmentBinding, Job
    from .test_ou_sync import TreeSource, StrictDirectory, ROOT

    source, directory = TreeSource(), StrictDirectory()
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: directory)
    django_user_model.objects.create_superuser("ou-browser-admin", password="Browser-test-only-823!")
    output = Path("test_artifacts/browser")
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright, ThreadPoolExecutor(max_workers=1) as executor:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.goto(live_server.url + "/login")
        page.get_by_label("用户名").fill("ou-browser-admin")
        page.get_by_label("密码").fill("Browser-test-only-823!")
        page.get_by_role("button", name="登录", exact=True).click()
        page.wait_for_url("**/dashboard")
        page.goto(live_server.url + "/departments")
        page.get_by_role("button", name="预览组织架构", exact=True).click()
        page.wait_for_url("**/dashboard")
        assert executor.submit(sync.run_next).result()
        job_id = executor.submit(lambda: str(Job.objects.get().pk)).result()
        page.goto(live_server.url + "/jobs/" + job_id)
        assert page.get_by_text("仅同步组织架构（不修改人员）", exact=True).is_visible()
        assert page.get_by_text("根 OU 待创建", exact=False).is_visible()
        assert ROOT in page.locator(".task-overview").inner_text()
        assert not directory.writes and directory.read_only
        assert not executor.submit(DepartmentBinding.objects.exists).result()
        page.screenshot(path=str(output / "organization-preview.png"), full_page=True)
        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.get_by_role("button", name="执行此计划", exact=True).click()
        assert not directory.writes  # Submission only queues; the worker owns writes.
        assert executor.submit(sync.run_next).result()
        page.reload()
        assert page.get_by_text("同步执行完成：4 项成功", exact=False).is_visible()
        department_results = page.locator("#department-plan")
        assert department_results.get_by_role("heading", name="部门 OU 执行结果", exact=True).is_visible()
        assert department_results.get_by_text("已创建", exact=True).count() == 4
        assert department_results.get_by_text("待创建", exact=True).count() == 0
        assert page.get_by_text("根 OU 待创建", exact=False).count() == 0
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.set_viewport_size({"width": 1440, "height": 1000})
        department_results.screenshot(path=str(output / "organization-created.png"))
        assert executor.submit(lambda: all(d["guid"] is None for d in Job.objects.get().plan["departments"])).result()
        assert executor.submit(DepartmentBinding.objects.count).result() == 4
        assert not executor.submit(Binding.objects.exists).result()
        assert directory.writes[0] == ROOT
        browser.close()


@pytest.mark.django_db(transaction=True)
def test_real_saved_account_association_and_manual_override_browser(live_server, django_user_model, monkeypatch):
    from unittest.mock import Mock
    from sync_app import account_associations as associations, synchronization as sync
    from sync_app.models import Binding, Configuration, Job
    from .fakes import Source, account, user
    from .test_account_associations import ReadOnlyDirectory

    config = Configuration.current()
    config.match_field = "employee_username"
    config.root_ou = "OU=People,DC=example,DC=com"
    config.auto_associate_accounts = True
    config.save()
    django_user_model.objects.create_superuser("association-browser-admin", password="Association-browser-only-823!")
    target, replacement = account(name="T0002320"), account(name="manual.other")
    target.update(dn="CN=T0002320,OU=Existing,DC=example,DC=com", protected=True)
    source, directory = Source([user(employee="T0002320")]), ReadOnlyDirectory([target, replacement])
    source_factory, ad_factory = Mock(return_value=source), Mock(return_value=directory)
    for module in (associations, sync):
        monkeypatch.setattr(module, "DingTalk", source_factory)
        monkeypatch.setattr(module, "ActiveDirectory", ad_factory)

    def saved_identity():
        binding = Binding.objects.get()
        return str(binding.object_guid), binding.manual, binding.sync_managed, binding.enabled

    output = Path("test_artifacts/browser")
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright, ThreadPoolExecutor(max_workers=1) as executor:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.goto(live_server.url + "/login")
        page.get_by_label("用户名").fill("association-browser-admin")
        page.get_by_label("密码").fill("Association-browser-only-823!")
        page.get_by_role("button", name="登录", exact=True).click()
        page.wait_for_url("**/dashboard")
        page.goto(live_server.url + "/people")
        with page.expect_response(lambda response: response.request.method == "POST" and response.url.endswith("/people/associate")) as submitted:
            page.get_by_role("button", name="刷新账号关联", exact=True).click()
        assert submitted.value.status == 302
        source_factory.assert_not_called()
        ad_factory.assert_not_called()
        assert executor.submit(lambda: Job.objects.get().kind).result() == "associate"
        assert executor.submit(sync.run_next).result()
        assert executor.submit(saved_identity).result() == (target["guid"], False, False, False)
        page.goto(live_server.url + "/people?q=T0002320")
        card = page.locator(".person-card")
        assert card.get_by_text("objectGUID：" + target["guid"], exact=True).is_visible()
        assert card.get_by_text("自动关联", exact=True).is_visible()
        assert card.get_by_text("仅账号关联", exact=True).is_visible()
        assert card.get_by_text("关联已停用", exact=True).count() == 0
        assert card.get_by_label("已有 AD 账号", exact=True).input_value() == "T0002320"
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.screenshot(path=str(output / "account-association-desktop.png"), full_page=True)
        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.screenshot(path=str(output / "account-association-mobile.png"), full_page=True)
        card.locator("summary").click()
        card.get_by_label("已有 AD 账号", exact=True).fill("manual.other")
        card.get_by_label("修改原因", exact=True).fill("管理员核验本人已有账号")
        card.get_by_role("button", name="核验并查看变更", exact=True).click()
        assert page.get_by_role("heading", name="确认修改账号关联", exact=True).is_visible()
        assert page.get_by_text("objectGUID：" + replacement["guid"], exact=True).is_visible()
        page.get_by_role("button", name="确认绑定此账号", exact=True).click()
        page.wait_for_url("**/people")
        assert executor.submit(saved_identity).result() == (replacement["guid"], True, False, False)
        card = page.locator(".person-card")
        assert card.get_by_text("objectGUID：" + replacement["guid"], exact=True).is_visible()
        assert card.get_by_text("人工关联", exact=True).is_visible()
        assert card.get_by_text("objectGUID：" + target["guid"], exact=True).count() == 0
        assert card.get_by_text("唯一工号匹配，已默认关联", exact=True).count() == 0
        assert card.get_by_text("受保护账号，仅维护账号关联", exact=True).count() == 0
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.screenshot(path=str(output / "account-association-manual-mobile.png"), full_page=True)
        browser.close()


def _assert_employee_identity_withheld(page, platform_names=()):
    assert page.get_by_text("正在通过钉钉确认身份", exact=False).count() == 0
    assert page.get_by_text("正在确认身份", exact=False).count() == 0
    assert page.get_by_text("testuser", exact=True).count() == 0
    assert page.get_by_text("当前 AD 账号", exact=True).count() == 0
    assert page.get_by_role("heading", name="企业 AD 认证平台", exact=True).count() == 0
    assert page.locator('form[action="/sspr/reset"]').count() == 0
    assert page.get_by_label("新密码", exact=True).count() == 0
    for name in platform_names:
        assert page.get_by_text(name, exact=True).count() == 0


@pytest.mark.django_db(transaction=True)
def test_dashboard_preview_browser_submits_its_own_csrf_token_and_queues_once(live_server, django_user_model, monkeypatch):
    from unittest.mock import Mock
    from sync_app import synchronization as sync
    from sync_app.models import Binding, Configuration, Job, Operation, Snapshot
    from .fakes import Directory, Source

    Configuration.current()
    django_user_model.objects.create_superuser("preview-browser-admin", password="Preview-browser-only-823!")
    source_factory = Mock(return_value=Source())
    ad_factory = Mock(return_value=Directory())
    monkeypatch.setattr(sync, "DingTalk", source_factory)
    monkeypatch.setattr(sync, "ActiveDirectory", ad_factory)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.goto(live_server.url + "/login")
        page.get_by_label("用户名").fill("preview-browser-admin")
        page.get_by_label("密码").fill("Preview-browser-only-823!")
        page.get_by_role("button", name="登录", exact=True).click()
        page.wait_for_url("**/dashboard")
        button = page.get_by_role("button", name="生成预览", exact=True)
        form = page.locator("form").filter(has=button)
        assert form.count() == 1
        token = form.locator('input[name="csrfmiddlewaretoken"]')
        assert token.count() == 1 and token.input_value()
        form.get_by_label("同步范围").select_option("users")
        form.get_by_label("部门 ID 或人员 userId").fill("u1 u2")
        with page.expect_response(lambda response: response.request.method == "POST" and response.url == live_server.url + "/dashboard") as submitted:
            button.click()
        assert submitted.value.status == 302
        assert page.get_by_text("任务已排队，执行进程将生成预览", exact=True).is_visible()
        browser.close()
    job = Job.objects.get()
    assert (job.kind, job.status, job.scope, job.selected, job.actor) == (
        "preview", "queued", "users", ["u1", "u2"], "preview-browser-admin",
    )
    source_factory.assert_not_called()
    ad_factory.assert_not_called()
    assert not Binding.objects.exists() and not Operation.objects.exists() and not Snapshot.objects.exists()


@pytest.mark.django_db(transaction=True)
def test_password_reset_audit_filters_and_mobile_table(live_server, django_user_model):
    from datetime import datetime, timezone as datetime_timezone
    from sync_app.models import Audit

    django_user_model.objects.create_superuser("audit-admin", password="Audit-browser-only-823!")
    record = Audit.objects.create(
        actor="ding-user-1919", actor_name="审计测试员工", employee_id="T0001919",
        action="sspr_reset", target_username="test.ad", target="d0a00000-1111-2222-3333-444444444444",
        state="partial", success=False, result="密码已重置，AD 账号解锁失败",
        completed_at=datetime(2026, 9, 30, 1, 2, 5, tzinfo=datetime_timezone.utc),
        client_ip="198.51.100.17",
    )
    Audit.objects.filter(pk=record.pk).update(created_at=datetime(2026, 9, 30, 1, 2, 3, tzinfo=datetime_timezone.utc))
    Audit.objects.create(actor="admin", action="settings", result="不应出现在密码重置筛选中")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page.goto(live_server.url + "/login")
        page.get_by_label("用户名").fill("audit-admin")
        page.get_by_label("密码").fill("Audit-browser-only-823!")
        page.get_by_role("button", name="登录", exact=True).click()
        page.wait_for_url("**/dashboard")
        page.goto(live_server.url + "/logs?action=sspr_reset")
        page.get_by_label("人员或 AD 账号").fill("T0001919")
        page.get_by_label("结果", exact=True).select_option("partial")
        page.get_by_role("button", name="筛选记录", exact=True).click()
        assert page.get_by_role("cell", name="审计测试员工", exact=False).is_visible()
        assert page.get_by_role("cell", name="test.ad", exact=False).is_visible()
        assert page.get_by_role("cell", name="部分完成", exact=True).is_visible()
        assert page.get_by_text("密码已修改，解锁未完成。", exact=True).is_visible()
        assert page.get_by_text("完成：2026-09-30 09:02:05", exact=True).is_visible()
        assert page.get_by_text("不应出现在密码重置筛选中", exact=False).count() == 0
        output = Path("test_artifacts/browser")
        output.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(output / "password-audit-desktop.png"), full_page=True)
        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        region = page.get_by_role("region", name="审计记录列表")
        assert region.evaluate("el => el.scrollWidth > el.clientWidth")
        region.evaluate("el => { el.scrollLeft = el.scrollWidth; }")
        assert page.get_by_role("cell", name="198.51.100.17", exact=True).is_visible()
        page.screenshot(path=str(output / "password-audit-mobile.png"), full_page=True)
        browser.close()


@pytest.mark.django_db(transaction=True)
def test_admin_and_mobile_employee_journeys(live_server, django_user_model, monkeypatch):
    from sync_app.models import Configuration
    Configuration.current()
    django_user_model.objects.create_superuser("browser-admin", password="Browser-test-only-823!")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1366, "height": 900})
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(live_server.url + "/login")
        page.get_by_label("用户名").fill("browser-admin")
        page.get_by_label("密码").fill("Browser-test-only-823!")
        page.get_by_role("button", name="登录", exact=True).click()
        page.wait_for_url("**/dashboard")
        assert page.get_by_role("heading", name="组织同步概览", exact=True).is_visible()
        page.get_by_role("link", name="人员与账号关联", exact=True).click()
        assert page.get_by_text("尚无人员。", exact=False).is_visible()
        page.get_by_role("link", name="操作审计", exact=True).click()
        assert page.get_by_role("heading", name="操作日志").is_visible()
        output = Path("test_artifacts/browser")
        output.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(output / "administrator.png"), full_page=True)
        mobile = browser.new_page(viewport={"width": 390, "height": 844})
        mobile.goto(live_server.url + "/sspr")
        assert mobile.get_by_role("heading", name="重置我的 AD 密码").is_visible()
        assert mobile.get_by_text("服务尚未开启", exact=False).is_visible()
        assert mobile.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        mobile.screenshot(path=str(output / "employee.png"), full_page=True)
        # A different employee has never been synchronized or locally bound.
        from sync_app import sspr
        from tests.fakes import Source, Directory
        from sync_app.models import Binding, Job
        def enable_sspr():
            config = Configuration.current()
            config.sspr_enabled = True
            config.save()
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(enable_sspr).result()
        source, directory = Source(), Directory()
        monkeypatch.setattr(sspr, "DingTalk", lambda: source)
        monkeypatch.setattr(sspr, "ActiveDirectory", lambda: directory)
        mobile.route("https://g.alicdn.com/**", lambda route: route.abort())
        mobile.reload()
        assert mobile.get_by_text("钉钉组件加载失败，请从钉钉工作台打开并检查网络后重试。", exact=True).is_visible()
        assert mobile.get_by_role("button", name="重新验证").is_enabled()
        _assert_employee_identity_withheld(mobile)
        stalled = browser.new_page(viewport={"width": 390, "height": 844})
        stalled.clock.install()
        stalled.route("https://g.alicdn.com/**", lambda route: None)
        stalled.goto(live_server.url + "/sspr", wait_until="domcontentloaded")
        stalled.clock.fast_forward(9000)
        assert stalled.get_by_text("连接钉钉超时，请从钉钉工作台打开并检查网络后重试。", exact=True).is_visible()
        assert stalled.get_by_role("button", name="重新验证").is_enabled()
        _assert_employee_identity_withheld(stalled)
        stalled.close()
        silent = browser.new_page(viewport={"width": 390, "height": 844})
        silent.clock.install()
        silent.add_init_script("window.dd = {requestAuthCode: () => {}};")
        silent.goto(live_server.url + "/sspr")
        silent.clock.fast_forward(13000)
        assert silent.get_by_text("钉钉授权超时，请重新验证。", exact=True).is_visible()
        assert silent.get_by_role("button", name="重新验证").is_enabled()
        _assert_employee_identity_withheld(silent)
        silent.close()
        request_stalled = browser.new_page(viewport={"width": 390, "height": 844})
        request_stalled.clock.install()
        request_stalled.add_init_script("window.AbortController = undefined; window.dd = {requestAuthCode: opts => opts.success({code: 'valid'})};")
        request_stalled.route("**/sspr/auth/dingtalk", lambda route: None)
        request_stalled.goto(live_server.url + "/sspr", wait_until="domcontentloaded")
        request_stalled.clock.fast_forward(21000)
        assert request_stalled.get_by_text("身份核验超时，请检查网络后重试。", exact=True).is_visible()
        assert request_stalled.get_by_role("button", name="重新验证").is_enabled()
        _assert_employee_identity_withheld(request_stalled)
        request_stalled.close()
        automatic = browser.new_page(viewport={"width": 390, "height": 844})
        automatic.route("https://g.alicdn.com/**", lambda route: route.abort())
        automatic.add_init_script("window.dd = {requestAuthCode: opts => opts.success({code: 'valid'})};")
        automatic.goto(live_server.url + "/sspr")
        automatic.get_by_text("当前 AD 账号", exact=True).wait_for(state="visible")
        assert automatic.get_by_text("testuser", exact=True).is_visible()
        assert automatic.get_by_role("button", name="重新验证").count() == 0
        automatic.close()
        mobile.add_init_script("window.authAttempt = 0; window.dd = {requestAuthCode: opts => { window.authAttempt++; if (window.authAttempt === 1) opts.fail(); else opts.success({code: 'valid'}); }};")
        mobile.reload()
        verify_button = mobile.get_by_role("button", name="重新验证")
        assert mobile.get_by_text("钉钉验证失败，请重新验证。", exact=True).is_visible()
        assert verify_button.is_enabled()
        _assert_employee_identity_withheld(mobile)
        verify_button.click()
        mobile.get_by_text("当前 AD 账号", exact=True).wait_for(state="visible")
        assert mobile.get_by_text("当前 AD 账号", exact=True).is_visible()
        assert mobile.get_by_text("testuser", exact=True).is_visible()
        assert mobile.get_by_role("button", name="重新验证").count() == 0
        mobile.get_by_label("新密码", exact=True).fill("Browser-password-43!")
        mobile.get_by_label("确认新密码").fill("Browser-password-43!")
        mobile.get_by_role("button", name="确认重置本人密码").click()
        assert mobile.get_by_text("密码已成功重置", exact=True).is_visible()
        assert directory.resets == 1
        mobile.screenshot(path=str(output / "employee-success.png"), full_page=True)
        assert not errors
        browser.close()
    assert not Binding.objects.exists() and not Job.objects.exists()


@pytest.mark.django_db(transaction=True)
def test_admin_employee_page_text_and_authenticated_platforms_mobile(live_server, django_user_model, monkeypatch):
    from sync_app import sspr
    from sync_app.models import (
        Audit,
        AuthPlatform,
        Configuration,
        EmployeePageSettings,
        EmployeeSession,
    )
    from tests.fakes import Directory, Source

    config = Configuration.current()
    config.sspr_enabled = True
    config.save()
    settings = EmployeePageSettings.current()
    settings.save()
    settings.platforms.all().delete()
    platform_names = ("VPN", "Nextcloud", "AI知识库")
    for position, name in enumerate(platform_names):
        AuthPlatform.objects.create(page_settings=settings, name=name, position=position)
    django_user_model.objects.create_superuser("page-browser-admin", password="Page-browser-only-823!")
    source, directory = Source(), Directory()
    monkeypatch.setattr(sspr, "DingTalk", lambda: source)
    monkeypatch.setattr(sspr, "ActiveDirectory", lambda: directory)
    title = "企业账号密码服务"
    description = "在钉钉工作台确认本人身份后，可查看当前 AD 账号及企业认证平台。"
    announcement = "维护公告：请在业务允许的时间修改密码。\n" + "https://docs.example.com/" + "a" * 350 + "\n<script>window.employeeNoticeExecuted=true</script>"
    help_text = "请先确认页面显示的是本人账号，再设置新密码。\n" + "修改后请保存个人工作，并按各平台的说明重新登录。" * 12
    support_text = "如有疑问，请联系企业 IT 服务台。"
    platform_urls = ("https://vpn.example.com/login", "https://nextcloud.example.com/login", "https://knowledge.example.com/login")
    output = Path("test_artifacts/browser")
    output.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        admin_page = browser.new_page(viewport={"width": 1440, "height": 1000})
        errors = []
        admin_page.on("pageerror", lambda error: errors.append(str(error)))
        admin_page.goto(live_server.url + "/login")
        admin_page.get_by_label("用户名").fill("page-browser-admin")
        admin_page.get_by_label("密码").fill("Page-browser-only-823!")
        admin_page.get_by_role("button", name="登录", exact=True).click()
        admin_page.wait_for_url("**/dashboard")
        admin_page.goto(live_server.url + "/admin/sync_app/employeepagesettings/1/change/")
        admin_page.get_by_label("页面标题").fill(title)
        admin_page.get_by_label("页面说明").fill(description)
        admin_page.get_by_label("公告").fill(announcement)
        admin_page.get_by_label("操作帮助").fill(help_text)
        admin_page.get_by_label("联系支持").fill(support_text)
        for position, name in enumerate(platform_names):
            assert admin_page.locator(f"#id_platforms-{position}-name").input_value() == name
            admin_page.locator(f"#id_platforms-{position}-authentication_note").fill("使用当前企业 AD 账号认证")
            admin_page.locator(f"#id_platforms-{position}-login_url").fill(platform_urls[position])
            admin_page.locator(f"#id_platforms-{position}-password_note").fill(f"{name} 下次登录请使用新密码。")
            admin_page.locator(f"#id_platforms-{position}-enabled").check()
        admin_page.get_by_role("button", name="保存", exact=True).click()
        admin_page.wait_for_url("**/admin/sync_app/employeepagesettings/")
        admin_page.goto(live_server.url + "/admin/sync_app/employeepagesettings/1/change/")
        assert admin_page.get_by_label("页面标题").input_value() == title
        assert admin_page.get_by_label("公告").input_value() == announcement
        assert admin_page.get_by_label("操作帮助").input_value() == help_text
        admin_page.screenshot(path=str(output / "employee-page-settings.png"), full_page=True)

        def saved_audit():
            record = Audit.objects.get(action="employee_page_settings")
            return record.actor, record.target, record.state, record.success, record.result

        with ThreadPoolExecutor(max_workers=1) as executor:
            actor, target, state, success, result = executor.submit(saved_audit).result()
        assert (actor, target, state, success) == ("page-browser-admin", "1", "success", True)
        assert "announcement" in result and "修改 3" in result
        assert title not in result and platform_urls[0] not in result

        unverified = browser.new_page(viewport={"width": 390, "height": 844})
        unverified.route("https://g.alicdn.com/**", lambda route: route.abort())
        unverified.goto(live_server.url + "/sspr")
        assert unverified.get_by_role("heading", name=title, exact=True).is_visible()
        assert unverified.get_by_text(description, exact=True).is_visible()
        assert unverified.get_by_label("服务公告").get_by_text(announcement, exact=True).is_visible()
        assert unverified.get_by_text(support_text, exact=True).is_visible()
        unverified.get_by_text("钉钉组件加载失败，请从钉钉工作台打开并检查网络后重试。", exact=True).wait_for(state="visible")
        _assert_employee_identity_withheld(unverified, platform_names)
        assert unverified.get_by_text("钉钉核验后会显示本人当前AD账号；本服务无需先同步或绑定", exact=True).is_visible()
        assert unverified.evaluate("window.employeeNoticeExecuted === undefined")
        assert unverified.evaluate("document.documentElement.scrollWidth <= window.innerWidth")

        denial_cases = (
            (True, True, False, "AD账号受保护，不能自助重置；无需先同步或绑定，请联系AD管理员核查权限与保护状态"),
            (False, False, False, "AD账号已禁用，不能自助重置；无需先同步或绑定，请联系AD管理员核查账号启用状态"),
            (True, False, False, "AD账号已禁用，不能自助重置；无需先同步或绑定，请联系AD管理员核查账号启用状态"),
            (True, False, True, "AD账号已禁用，不能自助重置；无需先同步或绑定，请联系AD管理员核查账号启用状态"),
        )
        expected_denials = []
        for protected, enabled, domain_admin, message in denial_cases:
            directory.items[0].update(protected=protected, enabled=enabled, domain_admin=domain_admin)
            blocked = browser.new_page(viewport={"width": 390, "height": 844})
            blocked.on("pageerror", lambda error: errors.append(str(error)))
            blocked.clock.install()
            blocked.route("https://g.alicdn.com/**", lambda route: route.abort())
            blocked.add_init_script("window.dd = {requestAuthCode: opts => opts.success({code: 'valid'})};")
            with blocked.expect_response(lambda response: response.request.method == "POST" and response.url == live_server.url + "/sspr/auth/dingtalk") as verification:
                blocked.goto(live_server.url + "/sspr")
            status = blocked.locator("#status")
            status.get_by_text(message, exact=True).wait_for(state="visible")
            assert verification.value.status == 400
            assert verification.value.json() == {"error": message}
            assert "employee_verification=" not in (verification.value.header_value("set-cookie") or "")
            retry = blocked.get_by_role("button", name="重新验证", exact=True)
            assert retry.is_enabled()
            with blocked.expect_response(lambda response: response.request.method == "POST" and response.url == live_server.url + "/sspr/auth/dingtalk") as retried:
                retry.click()
            assert retried.value.status == 400
            assert retried.value.json() == {"error": message}
            status.get_by_text(message, exact=True).wait_for(state="visible")
            blocked.clock.fast_forward(21000)
            assert status.text_content() == message
            assert retry.is_enabled()
            _assert_employee_identity_withheld(blocked, platform_names)
            assert not any(cookie["name"] == "employee_verification" for cookie in blocked.context.cookies())
            assert blocked.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            expected_denials.extend([message, message])
            blocked.close()

        def rejected_authorization_records():
            return EmployeeSession.objects.exists(), list(Audit.objects.filter(action="sspr_auth_failed").order_by("pk").values_list("result", "success"))

        with ThreadPoolExecutor(max_workers=1) as executor:
            session_exists, denials = executor.submit(rejected_authorization_records).result()
        assert not session_exists
        assert denials == [(message, False) for message in expected_denials]
        assert directory.resets == 0

        # Domain Admins remains protected for synchronization, but an enabled
        # member may reset only the account confirmed by its own DingTalk identity.
        directory.items[0].update(protected=True, enabled=True, domain_admin=True)
        expected_guid = directory.items[0]["guid"]
        domain_admin = browser.new_page(viewport={"width": 390, "height": 844})
        domain_admin.on("pageerror", lambda error: errors.append(str(error)))
        domain_admin.route("https://g.alicdn.com/**", lambda route: route.abort())
        domain_admin.add_init_script("window.dd = {requestAuthCode: opts => opts.success({code: 'valid'})};")
        with domain_admin.expect_response(lambda response: response.request.method == "POST" and response.url == live_server.url + "/sspr/auth/dingtalk") as verification:
            domain_admin.goto(live_server.url + "/sspr")
        assert verification.value.status == 200
        domain_admin.get_by_text("当前 AD 账号", exact=True).wait_for(state="visible")
        assert domain_admin.get_by_text("testuser", exact=True).is_visible()
        assert domain_admin.get_by_role("heading", name="企业 AD 认证平台", exact=True).is_visible()
        assert domain_admin.get_by_role("button", name="重新验证").count() == 0
        domain_admin.get_by_label("新密码", exact=True).fill("Browser-domain-admin-43!")
        domain_admin.get_by_label("确认新密码").fill("Browser-domain-admin-43!")
        with domain_admin.expect_response(lambda response: response.request.method == "POST" and response.url == live_server.url + "/sspr/reset") as reset:
            domain_admin.get_by_role("button", name="确认重置本人密码").click()
        assert reset.value.status == 200
        assert domain_admin.get_by_text("密码已成功重置", exact=True).is_visible()
        assert domain_admin.get_by_label("新密码", exact=True).count() == 0
        assert directory.resets == 1
        domain_admin.close()

        def domain_admin_reset_audit():
            record = Audit.objects.get(action="sspr_reset")
            return record.actor, record.employee_id, record.target_username, record.target, record.state, record.success, record.result

        with ThreadPoolExecutor(max_workers=1) as executor:
            recorded_reset = executor.submit(domain_admin_reset_audit).result()
        assert recorded_reset == ("u1", "1001", "testuser", expected_guid, "success", True, "密码已成功重置")

        # A verified page must re-read eligibility and the target GUID before it
        # renders private identity, platforms, or another password reset form.
        stale = browser.new_page(viewport={"width": 390, "height": 844})
        stale.on("pageerror", lambda error: errors.append(str(error)))
        stale.route("https://g.alicdn.com/**", lambda route: route.abort())
        stale.add_init_script("window.dd = {requestAuthCode: opts => opts.success({code: 'valid'})};")
        stale.goto(live_server.url + "/sspr")
        stale.get_by_text("当前 AD 账号", exact=True).wait_for(state="visible")
        directory.items[0]["domain_admin"] = False
        stale.reload()
        stale.get_by_text(denial_cases[0][3], exact=True).first.wait_for(state="visible")
        _assert_employee_identity_withheld(stale, platform_names)
        assert stale.get_by_role("button", name="重新验证").is_enabled()
        assert directory.resets == 1
        stale.close()

        directory.items[0]["domain_admin"] = True
        changed = browser.new_page(viewport={"width": 390, "height": 844})
        changed.on("pageerror", lambda error: errors.append(str(error)))
        changed.route("https://g.alicdn.com/**", lambda route: route.abort())
        changed.add_init_script("window.dd = {requestAuthCode: opts => opts.success({code: 'valid'})};")
        changed.goto(live_server.url + "/sspr")
        changed.get_by_text("当前 AD 账号", exact=True).wait_for(state="visible")
        from uuid import uuid4
        directory.items[0]["guid"] = str(uuid4())
        # Prevent fresh authorization so the existing cookie alone is assessed.
        changed.route("**/sspr/auth/dingtalk", lambda route: route.abort())
        changed.reload()
        assert changed.get_by_text("AD 匹配对象发生变化，请重新验证", exact=True).is_visible()
        changed.get_by_text("身份核验请求失败，请检查网络后重试。", exact=True).wait_for(state="visible")
        _assert_employee_identity_withheld(changed, platform_names)
        assert changed.get_by_role("button", name="重新验证").is_enabled()
        assert directory.resets == 1
        changed.close()
        directory.items[0].update(protected=False, enabled=True, domain_admin=False, guid=expected_guid)

        employee = browser.new_page(viewport={"width": 390, "height": 844})
        employee.on("pageerror", lambda error: errors.append(str(error)))
        employee.route("https://g.alicdn.com/**", lambda route: route.abort())
        employee.add_init_script("window.dd = {requestAuthCode: opts => opts.success({code: 'valid'})};")
        employee.goto(live_server.url + "/sspr")
        employee.get_by_text("当前 AD 账号", exact=True).wait_for(state="visible")
        assert employee.get_by_text("testuser", exact=True).is_visible()
        assert employee.get_by_role("heading", name="企业 AD 认证平台", exact=True).is_visible()
        for position, name in enumerate(platform_names):
            assert employee.get_by_text(name, exact=True).is_visible()
            assert employee.get_by_text(f"{name} 下次登录请使用新密码。", exact=True).is_visible()
            link = employee.get_by_role("link", name=f"打开{name}", exact=True)
            assert link.get_attribute("href") == platform_urls[position]
            assert link.get_attribute("target") == "_blank"
            assert {"noopener", "noreferrer"}.issubset(set(link.get_attribute("rel").split()))
        assert employee.get_by_label("新密码", exact=True).is_visible()
        assert employee.get_by_text(support_text, exact=True).is_visible()
        assert employee.evaluate("window.employeeNoticeExecuted === undefined")
        assert employee.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        employee.screenshot(path=str(output / "employee-platforms-mobile.png"), full_page=True)
        assert not errors
        browser.close()
