from pathlib import Path
import pytest
from playwright.sync_api import sync_playwright


@pytest.mark.django_db(transaction=True)
def test_admin_and_mobile_employee_journeys(live_server, django_user_model):
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
        assert page.get_by_role("heading", name="同步", exact=True).is_visible()
        page.get_by_role("link", name="人员与绑定", exact=True).click()
        assert page.get_by_text("尚无人员。", exact=False).is_visible()
        page.get_by_role("link", name="日志", exact=True).click()
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
        assert not errors
        browser.close()
