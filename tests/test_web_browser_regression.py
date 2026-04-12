import json
import socket
import threading
import time
import unittest
import urllib.request
from pathlib import Path

import uvicorn

from sync_app.storage.local_db import DatabaseManager, OrganizationRepository, WebAdminUserRepository
from sync_app.web.app import create_app
from sync_app.web.security import hash_password

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover - exercised when browser tooling is absent
    PlaywrightError = Exception
    sync_playwright = None


ARTIFACT_DIR = Path.cwd() / "test_artifacts" / "browser"


def _reserve_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return int(port)


def _wait_for_http(url: str, *, timeout_seconds: float = 20.0) -> None:
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status < 500:
                    return
        except Exception as exc:  # pragma: no cover - timing-sensitive
            last_error = exc
            time.sleep(0.25)
    if last_error is not None:
        raise last_error
    raise TimeoutError(f"timed out waiting for {url}")


class WebBrowserRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sync_playwright is None:
            raise unittest.SkipTest("playwright is not installed")

        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        cls.db_path = ARTIFACT_DIR / "browser_regression.db"
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(cls.db_path) + suffix)
            if candidate.exists():
                candidate.unlink()

        manager = DatabaseManager(db_path=str(cls.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        OrganizationRepository(manager).ensure_default(config_path="config.ini")
        WebAdminUserRepository(manager).create_user("admin", hash_password("simple888"))

        cls.port = _reserve_port()
        cls.base_url = f"http://127.0.0.1:{cls.port}"
        cls.server = uvicorn.Server(
            uvicorn.Config(
                create_app(db_path=str(cls.db_path), config_path="config.ini", bind_host="127.0.0.1", bind_port=cls.port),
                host="127.0.0.1",
                port=cls.port,
                log_level="warning",
            )
        )
        cls.server.install_signal_handlers = lambda: None
        cls.server_thread = threading.Thread(target=cls.server.run, name="browser-regression-server", daemon=True)
        cls.server_thread.start()
        _wait_for_http(f"{cls.base_url}/login")

        try:
            cls.playwright = sync_playwright().start()
            cls.browser = cls.playwright.chromium.launch()
        except PlaywrightError as exc:  # pragma: no cover - depends on browser install state
            cls.server.should_exit = True
            cls.server_thread.join(timeout=10)
            raise unittest.SkipTest(f"playwright browser is not installed: {exc}")

    @classmethod
    def tearDownClass(cls):
        browser = getattr(cls, "browser", None)
        if browser is not None:
            browser.close()
        playwright = getattr(cls, "playwright", None)
        if playwright is not None:
            playwright.stop()
        server = getattr(cls, "server", None)
        if server is not None:
            server.should_exit = True
        thread = getattr(cls, "server_thread", None)
        if thread is not None:
            thread.join(timeout=10)

    def setUp(self):
        self.context = self.browser.new_context(viewport={"width": 1440, "height": 1100})
        self.page = self.context.new_page()

    def tearDown(self):
        self.context.close()

    def _capture(self, name: str) -> Path:
        target = ARTIFACT_DIR / name
        self.page.screenshot(path=str(target), full_page=True)
        self.assertTrue(target.exists())
        self.assertGreater(target.stat().st_size, 0)
        return target

    def _login(self) -> None:
        self.page.goto(f"{self.base_url}/login", wait_until="networkidle")
        self.page.fill("#username", "admin")
        self.page.fill("#password", "simple888")
        self.page.click("button[type='submit']")
        self.page.wait_for_url(f"{self.base_url}/dashboard")

    def _height(self, selector: str) -> float:
        return float(
            self.page.eval_on_selector(
                selector,
                "element => parseFloat(getComputedStyle(element).height || '0')",
            )
        )

    def _style(self, selector: str, prop: str) -> str:
        return str(
            self.page.eval_on_selector(
                selector,
                f"element => getComputedStyle(element).getPropertyValue('{prop}')",
            )
        ).strip()

    def test_login_page_loads_styles_and_primary_action(self):
        self.page.goto(f"{self.base_url}/login", wait_until="networkidle")
        stylesheet_loaded = self.page.evaluate(
            "() => Array.from(document.styleSheets).some(sheet => (sheet.href || '').includes('/static/app.css'))"
        )
        self.assertTrue(stylesheet_loaded)
        submit_height = self._height("button[type='submit']")
        language_height = self._height(".login-language-switcher a.active")
        self.assertGreaterEqual(submit_height, 42.0)
        self.assertGreaterEqual(language_height, 40.0)
        self.assertLessEqual(abs(submit_height - language_height), 10.0)
        self.assertIn("AD Org Sync", self.page.title())
        self._capture("login-page.png")

    def test_dashboard_header_controls_share_consistent_height(self):
        self._login()
        self.page.goto(f"{self.base_url}/dashboard", wait_until="networkidle")
        mode_height = self._height(".mode-switcher button.active")
        language_height = self._height(".language-switcher a.active")
        signout_height = self._height(".header-signout")
        self.assertLessEqual(abs(mode_height - language_height), 6.0)
        self.assertLessEqual(abs(signout_height - language_height), 6.0)
        self.assertNotEqual(self._style(".mode-switcher button.active", "color"), "rgb(255, 255, 255)")
        self.assertNotEqual(self._style(".language-switcher a.active", "color"), "rgb(255, 255, 255)")
        self.assertNotEqual(self._style(".header-signout", "border-top-color"), "rgba(0, 0, 0, 0)")
        self._capture("dashboard-page.png")

    def test_config_page_renders_multi_provider_schema_controls(self):
        self._login()
        self.page.goto(f"{self.base_url}/config", wait_until="networkidle")
        self.assertIn("WeCom Connector Configuration", self.page.locator("body").inner_text())
        self.assertIn("Shared Page, Provider-Specific Fields", self.page.locator("body").inner_text())
        option_text = self.page.locator("#source_provider option").all_inner_texts()
        self.assertTrue(any("WeCom" in item for item in option_text))
        self.assertTrue(any("DingTalk" in item for item in option_text))
        self.assertTrue(any("Feishu" in item for item in option_text))
        self.assertTrue(self.page.locator("#group-corpid").is_visible())
        self.assertTrue(self.page.locator("#group-corpsecret").is_visible())
        self.assertTrue(self.page.locator("#group-webhook_url").is_visible())
        self.page.select_option("#source_provider", "dingtalk")
        self.page.wait_for_function(
            "() => document.querySelector('[data-config-provider-card-title]')?.textContent.includes('DingTalk Source Connector')"
        )
        self.assertIn("DingTalk Source Connector", self.page.locator("body").inner_text())
        self.assertIn("AppKey / Client ID", self.page.locator("#group-corpid label").inner_text())
        self.assertEqual(self.page.locator("#corpid").get_attribute("placeholder"), "Enter AppKey")
        self.assertIn(
            "The DingTalk application key or client ID.",
            self.page.locator("#group-corpid").inner_text(),
        )
        self.assertIn("DingTalk Bot Webhook", self.page.locator("#group-webhook_url label").inner_text())
        self.assertTrue(self.page.get_by_text("OU Filter And Root Mapping").first.is_visible())
        browse_source_button = self.page.get_by_role("button", name="Browse Source Unit Tree")
        select_target_button = self.page.get_by_role("button", name="Select Target Root OU")
        self.assertTrue(browse_source_button.is_visible())
        self.assertTrue(select_target_button.is_visible())
        browse_source_button.click()
        self.assertTrue(
            self.page.locator("#group-source_root_unit_ids [data-config-source-browser]").is_visible()
        )
        select_target_button.click()
        self.assertTrue(
            self.page.locator("#group-directory_root_ou_path [data-config-target-browser]").is_visible()
        )
        self.assertFalse(
            self.page.locator("#group-disabled_users_ou_path [data-config-target-browser]").is_visible()
        )
        self.assertTrue(self.page.get_by_role("button", name="Select Disabled Users OU").is_visible())
        self.assertTrue(self.page.get_by_role("button", name="Select Custom Group OU").is_visible())
        self._capture("config-page.png")

    def test_config_source_picker_loads_and_selects_inside_same_field_frame(self):
        self._login()
        self.page.route(
            "**/config/source-units/catalog",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "ok": True,
                        "provider": "WeCom",
                        "items": [
                            {
                                "department_id": "1",
                                "name": "HQ",
                                "path_display": "HQ",
                                "level": 0,
                                "selected": False,
                            },
                            {
                                "department_id": "8",
                                "name": "China",
                                "path_display": "HQ / China",
                                "level": 1,
                                "selected": False,
                            },
                        ],
                    }
                ),
            ),
        )
        self.page.goto(f"{self.base_url}/config", wait_until="networkidle")
        source_group = self.page.locator("#group-source_root_unit_ids")
        self.assertTrue(source_group.locator(".picker-field__surface").is_visible())
        source_group.get_by_role("button", name="Browse Source Unit Tree").click()
        source_group.locator("[data-config-source-browser]").wait_for(state="visible")
        source_group.locator("[data-config-source-list] .config-tree-row").nth(1).wait_for()
        self.assertTrue(source_group.locator(".picker-field__surface .picker-inline-panel").is_visible())
        source_group.locator('[data-source-unit-checkbox][value="8"]').check()
        self.assertEqual(source_group.locator('input[name="source_root_unit_ids"]').input_value(), "8")
        self.assertIn(
            "China [8]",
            source_group.locator('[data-picker-summary-for="source_root_unit_ids"]').inner_text(),
        )
        self.assertRegex(
            source_group.locator('[data-picker-meta-for="source_root_unit_ids"]').inner_text(),
            r"1",
        )
        source_group.get_by_role("button", name="Close Picker").click()
        source_group.get_by_role("button", name="Browse Source Unit Tree").click()
        self.assertTrue(source_group.locator("[data-config-source-list] .config-tree-row").nth(1).is_visible())

    def test_jobs_empty_state_actions_remain_visually_consistent(self):
        self._login()
        self.page.goto(f"{self.base_url}/jobs", wait_until="networkidle")
        dry_run_button = self.page.locator("button:has-text('Run Dry Run')").first
        apply_button = self.page.locator("button:has-text('Run Apply')").first
        dry_run_box = dry_run_button.bounding_box()
        apply_box = apply_button.bounding_box()
        self.assertIsNotNone(dry_run_box)
        self.assertIsNotNone(apply_box)
        self.assertLessEqual(abs(float(dry_run_box["y"]) - float(apply_box["y"])), 8.0)
        self.assertGreater(float(apply_box["x"]) - float(dry_run_box["x"]), 20.0)
        self.page.wait_for_selector(".empty-state .button")
        button_count = self.page.locator(".empty-state .button").count()
        self.assertGreaterEqual(button_count, 2)
        heights = self.page.locator(".empty-state .button").evaluate_all(
            "elements => elements.map(element => parseFloat(getComputedStyle(element).height || '0'))"
        )
        first_height = float(heights[0])
        for height in heights[1:]:
            self.assertLessEqual(abs(first_height - float(height)), 6.0)
        self._capture("jobs-page.png")


if __name__ == "__main__":
    unittest.main()
