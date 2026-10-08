from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pytest
from playwright.sync_api import sync_playwright


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
            (True, True, "AD账号受保护，不能自助重置；无需先同步或绑定，请联系AD管理员核查权限与保护状态"),
            (False, False, "AD账号已禁用，不能自助重置；无需先同步或绑定，请联系AD管理员核查账号启用状态"),
            (True, False, "AD账号受保护且已禁用，不能自助重置；无需先同步或绑定，请联系AD管理员核查权限、保护与启用状态"),
        )
        expected_denials = []
        for protected, enabled, message in denial_cases:
            directory.items[0].update(protected=protected, enabled=enabled)
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
        directory.items[0].update(protected=False, enabled=True)

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
