from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pytest
from playwright.sync_api import sync_playwright


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
        monkeypatch.setattr("sync_app.views.ActiveDirectory", lambda: directory)
        mobile.route("https://g.alicdn.com/**", lambda route: route.abort())
        mobile.reload()
        assert mobile.get_by_text("请从钉钉工作台打开此应用。", exact=True).is_visible()
        assert mobile.get_by_text("testuser", exact=True).count() == 0
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
