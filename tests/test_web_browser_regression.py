import json
import socket
import threading
import time
import unittest
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import uvicorn

from sync_app.storage.local_db import (
    DatabaseManager,
    OffboardingQueueRepository,
    OrganizationConfigRepository,
    OrganizationRepository,
    SyncConflictRepository,
    SyncJobRepository,
    SyncOperationLogRepository,
    PlannedOperationRepository,
    SyncReplayRequestRepository,
    SourceDirectoryRepository,
    SettingsRepository,
    UserIdentityBindingRepository,
    UserLifecycleQueueRepository,
    WebAdminUserRepository,
)
from sync_app.web.app import create_app
from sync_app.web.security import hash_password
from sync_app.modules.sspr import SSPRVerifiedIdentity
from sync_app.modules.sspr.repositories import hash_capability
from sync_app.services.typed_settings import SSPRSettings
from tests.helpers.execution_plans import create_eligible_execution_plan

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover - exercised when browser tooling is absent
    PlaywrightError = Exception
    sync_playwright = None


ARTIFACT_DIR = Path.cwd() / "test_artifacts" / "browser"


class _MissingAccountPreviewProvider:
    def get_users_batch(self, _usernames):
        return {}

    def close(self):
        return None


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


def _launch_chromium(playwright):
    try:
        return playwright.chromium.launch()
    except PlaywrightError as bundled_error:
        candidates = (
            Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
            Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
            Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
            Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
        )
        for candidate in candidates:
            if candidate.is_file():
                return playwright.chromium.launch(executable_path=str(candidate))
        raise bundled_error


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
                create_app(
                    db_path=str(cls.db_path),
                    config_path="config.ini",
                    bind_host="127.0.0.1",
                    bind_port=cls.port,
                ),
                host="127.0.0.1",
                port=cls.port,
                log_level="warning",
            )
        )
        cls.server.install_signal_handlers = lambda: None
        cls.server_thread = threading.Thread(
            target=cls.server.run, name="browser-regression-server", daemon=True
        )
        cls.server_thread.start()
        _wait_for_http(f"{cls.base_url}/login")

        try:
            cls.playwright = sync_playwright().start()
            cls.browser = _launch_chromium(cls.playwright)
        except (
            PlaywrightError
        ) as exc:  # pragma: no cover - depends on browser install state
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
        self.context = self.browser.new_context(
            viewport={"width": 1440, "height": 1100}, locale="en-US"
        )
        self.page = self.context.new_page()

    def tearDown(self):
        self.context.close()

    def _capture(self, name: str) -> Path:
        target = ARTIFACT_DIR / name
        self.page.screenshot(path=str(target), full_page=True)
        self.assertTrue(target.exists())
        self.assertGreater(target.stat().st_size, 0)
        return target

    def _capture_viewport(self, name: str) -> Path:
        target = ARTIFACT_DIR / name
        self.page.screenshot(path=str(target), full_page=False)
        self.assertTrue(target.exists())
        self.assertGreater(target.stat().st_size, 0)
        return target

    def _login(self, *, ui_mode: str = "basic") -> None:
        self.page.goto(f"{self.base_url}/login", wait_until="networkidle")
        self.page.fill("#username", "admin")
        self.page.fill("#password", "simple888")
        self.page.click("button[type='submit']")
        self.page.wait_for_url(f"{self.base_url}/dashboard")
        if ui_mode != "advanced":
            return
        with self.page.expect_navigation(wait_until="networkidle"):
            self.page.locator(
                ".mode-switcher button[name='ui_mode'][value='advanced']"
            ).evaluate("button => button.click()")

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

    @classmethod
    def _configure_sspr_browser_org(cls):
        manager = DatabaseManager(db_path=str(cls.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        org_repo = OrganizationRepository(manager)
        org_repo.upsert_organization(
            org_id="sspr-browser",
            name="SSPR Browser Organization",
            config_path=str(ARTIFACT_DIR / "sspr-browser.ini"),
            is_enabled=True,
        )
        OrganizationConfigRepository(manager).save_config(
            "sspr-browser",
            {
                "source_provider": "dingtalk",
                "corpid": "browser-ding-app-key",
                "agentid": "10001",
                "corpsecret": "browser-ding-secret",
                "webhook_url": "",
                "ldap_server": "dc.browser.example",
                "ldap_domain": "browser.example",
                "ldap_username": "svc-browser",
                "ldap_password": "browser-directory-secret",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": True,
                "ldap_ca_cert_path": "",
                "default_password": "",
                "force_change_password": False,
                "password_complexity": "strong",
            },
            config_path=str(ARTIFACT_DIR / "sspr-browser.ini"),
        )
        SSPRSettings(
            enabled=True,
            dingtalk_corp_id="browser-ding-corp",
            min_password_length=12,
            unlock_account_default=True,
            verification_session_ttl_seconds=600,
        ).persist(SettingsRepository(manager), org_id="sspr-browser")
        UserIdentityBindingRepository(manager).upsert_binding(
            "browser-alice",
            "browser.alice",
            org_id="sspr-browser",
            source_provider="dingtalk",
            connector_id="default",
            source_display_name="Browser Alice",
            preserve_manual=False,
        )

    def _inject_sspr_employee_session(self):
        user_agent = self.page.evaluate("navigator.userAgent")
        session = self.server.config.app.state.sspr_session_store.create_session(
            SSPRVerifiedIdentity(
                org_id="sspr-browser",
                provider_id="dingtalk",
                connector_id="default",
                source_user_id="browser-alice",
                display_name="Browser Alice",
            ),
            user_agent=user_agent,
            ttl_seconds=600,
        )
        self.context.add_cookies(
            [
                {
                    "name": "ad_org_sync_sspr",
                    "value": session.session_id,
                    "url": f"{self.base_url}/sspr",
                    "httpOnly": True,
                    "sameSite": "Lax",
                },
                {
                    "name": "ad_org_sync_sspr_csrf",
                    "value": session.csrf_token,
                    "url": f"{self.base_url}/sspr",
                    "httpOnly": True,
                    "sameSite": "Lax",
                },
            ]
        )
        return session

    def test_sspr_mobile_dingtalk_stub_password_feedback_and_success_states(self):
        self._configure_sspr_browser_org()
        self.context.close()
        self.context = self.browser.new_context(
            viewport={"width": 390, "height": 844}, locale="en-US"
        )
        self.page = self.context.new_page()

        captured_auth = {}
        self.page.route(
            "https://g.alicdn.com/**",
            lambda route: route.fulfill(
                status=200,
                content_type="application/javascript",
                body=(
                    "window.dd={ready:function(fn){fn();},error:function(){},"
                    "requestAuthCode:function(options){options.onSuccess({code:'stub-code'});}};"
                ),
            ),
        )

        def fulfill_auth(route):
            captured_auth.update(json.loads(route.request.post_data or "{}"))
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"status": "verified", "nextUrl": "/sspr/browser-auth-complete"}),
            )

        self.page.route(f"{self.base_url}/sspr/auth/dingtalk", fulfill_auth)
        self.page.route(
            f"{self.base_url}/sspr/browser-auth-complete",
            lambda route: route.fulfill(status=200, content_type="text/html", body="<h1>stub verified</h1>"),
        )
        self.page.goto(
            f"{self.base_url}/sspr?corpid=browser-ding-corp",
            wait_until="networkidle",
        )
        self.assertEqual(captured_auth.get("authCode"), "stub-code")
        self.assertTrue(captured_auth.get("state"))
        self.assertTrue(self.page.get_by_role("heading", name="stub verified").is_visible())

        class BrowserTarget:
            def get_user_account_state(self, _username):
                return {
                    "available": True,
                    "exists": True,
                    "enabled": True,
                    "locked": True,
                    "domain": "browser.example",
                }

            def close(self):
                return None

        self._inject_sspr_employee_session()
        with patch("sync_app.web.routes_sspr.build_target_provider", return_value=BrowserTarget()):
            self.page.goto(f"{self.base_url}/sspr/account", wait_until="networkidle")
        self.assertTrue(self.page.get_by_role("heading", name="Browser Alice").is_visible())
        self.assertIn("browser.alice", self.page.locator("body").inner_text())
        self.assertEqual(self.page.locator("[data-app-sidebar]").count(), 0)
        self._assert_page_has_no_horizontal_overflow()
        self.assertGreaterEqual(self._height("#new_password"), 44)
        self.assertGreaterEqual(self._height("[data-sspr-submit]"), 44)

        self.page.locator("#new_password").focus()
        self.page.keyboard.press("Tab")
        self.assertEqual(
            self.page.evaluate("document.activeElement?.dataset?.passwordToggle || ''"),
            "new_password",
        )

        self.page.fill("#new_password", "N3w!BrowserPass")
        self.page.fill("#confirm_password", "N3w!BrowserPass")
        self.assertGreaterEqual(self.page.locator(".sspr-rules .is-valid").count(), 3)
        self.page.locator('[data-password-toggle="new_password"]').click()
        self.assertEqual(self.page.locator("#new_password").get_attribute("type"), "text")
        self.page.locator('[data-password-toggle="new_password"]').click()
        self.assertEqual(self.page.locator("#new_password").get_attribute("type"), "password")
        self.page.locator("[data-sspr-reset-form]").evaluate(
            "form => { form.addEventListener('submit', event => event.preventDefault(), {once: true}); form.requestSubmit(); }"
        )
        self.assertTrue(self.page.locator("[data-sspr-submit]").is_disabled())
        self.assertIn("Securely resetting", self.page.locator("[data-sspr-submit]").inner_text())
        self._capture("sspr-account-mobile.png")

        receipt = self.server.config.app.state.sspr_receipt_store.create_receipt(
            org_id="sspr-browser",
            ad_username="browser.alice",
            unlock_requested=True,
            unlock_succeeded=True,
        )
        self.context.add_cookies(
            [
                {
                    "name": "ad_org_sync_sspr_result",
                    "value": receipt.token,
                    "url": f"{self.base_url}/sspr/result",
                    "httpOnly": True,
                    "sameSite": "Lax",
                }
            ]
        )
        self.page.goto(f"{self.base_url}/sspr/result?lang=zh-CN", wait_until="networkidle")
        self.assertIn("密码重置已完成", self.page.locator("body").inner_text())
        self.assertIn("browser.alice", self.page.locator("body").inner_text())
        self.assertNotIn("N3w!BrowserPass", self.page.content())
        self._assert_page_has_no_horizontal_overflow()
        self._capture("sspr-success-mobile-zh.png")

    def test_sspr_dingtalk_stub_error_recovery_is_accessible(self):
        self._configure_sspr_browser_org()
        self.context.close()
        self.context = self.browser.new_context(
            viewport={"width": 390, "height": 844}, locale="zh-CN"
        )
        self.page = self.context.new_page()
        self.page.route(
            "https://g.alicdn.com/**",
            lambda route: route.fulfill(
                status=200,
                content_type="application/javascript",
                body=(
                    "window.dd={ready:function(fn){fn();},error:function(){},"
                    "requestAuthCode:function(options){options.onFail({errorCode:'stub'});}};"
                ),
            ),
        )

        expired = self._inject_sspr_employee_session()
        with self.server.config.app.state.db_manager.transaction() as connection:
            connection.execute(
                "UPDATE sspr_verification_sessions SET expires_at = ? WHERE token_hash = ?",
                (
                    "2000-01-01T00:00:00+00:00",
                    hash_capability(expired.session_id),
                ),
            )
        self.page.goto(f"{self.base_url}/sspr/account", wait_until="networkidle")

        self.assertIn("/sspr/oauth/start", self.page.url)
        self.assertIn("验证会话已过期", self.page.locator("body").inner_text())
        self.assertTrue(self.page.get_by_role("heading", name="钉钉验证失败").is_visible())
        self.assertTrue(self.page.get_by_role("link", name="重试").is_visible())
        self.assertGreaterEqual(self.page.locator("[aria-live='polite']").count(), 1)
        self._assert_page_has_no_horizontal_overflow()
        self._capture("sspr-auth-error-mobile-zh.png")

    def test_source_directory_page_renders_snapshot_views_without_secrets(self):
        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        source_repo = SourceDirectoryRepository(manager)
        jobs = SyncJobRepository(manager)
        operations = SyncOperationLogRepository(manager)
        plans = PlannedOperationRepository(manager)
        bindings = UserIdentityBindingRepository(manager)
        config_repo = OrganizationConfigRepository(manager)
        previous_config = config_repo.get_raw_config(
            "default", config_path="config.ini"
        )
        config_repo.save_config(
            "default",
            {**previous_config, "source_provider": "dingtalk"},
            config_path="config.ini",
        )
        self.addCleanup(
            config_repo.save_config,
            "default",
            previous_config,
            config_path="config.ini",
        )
        snapshot_id = source_repo.start_refresh(
            org_id="default", provider_id="dingtalk", created_by="browser"
        )
        source_repo.replace_snapshot(
            snapshot_id,
            departments=[
                {
                    "source_department_id": "1",
                    "name": "Headquarters",
                    "parent_department_id": "0",
                    "path_ids": ["1"],
                    "path_names": ["Headquarters"],
                }
            ],
            users=[
                {
                    "source_user_id": "browser-alice",
                    "display_name": "Alice Zhang",
                    "employee_id": "TJ001",
                    "email": "alice@example.com",
                    "department_ids": ["1"],
                    "department_names": ["Headquarters"],
                    "is_active": True,
                    "raw_payload": {"userid": "browser-alice", "employee_id": "TJ001"},
                    "search_text": "Alice Zhang TJ001",
                },
                {
                    "source_user_id": "browser-bob",
                    "display_name": "Bob Li",
                    "employee_id": "TJ002",
                    "email": "bob@example.com",
                    "department_ids": ["1"],
                    "department_names": ["Headquarters"],
                    "is_active": True,
                    "raw_payload": {"userid": "browser-bob", "employee_id": "TJ002"},
                    "search_text": "Bob Li TJ002",
                },
                {
                    "source_user_id": "browser-carol",
                    "display_name": "Carol Wu",
                    "employee_id": "",
                    "department_ids": ["1"],
                    "department_names": ["Headquarters"],
                    "is_active": True,
                    "raw_payload": {"userid": "browser-carol"},
                    "search_text": "Carol Wu",
                },
            ],
            fields=[
                {
                    "name": "employee_id",
                    "label": "Employee ID",
                    "coverage": 2,
                    "samples": ["TJ001", "TJ002"],
                }
            ],
            fingerprint="browser-identity-snapshot-v1",
        )
        selection = source_repo.save_scope_selection(
            org_id="default",
            provider_id="dingtalk",
            scope_type="full",
            username_strategy="employee_id",
            source_field="employee_id",
            snapshot_id=snapshot_id,
            requested_by="browser",
        )
        compatibility_snapshot_id = source_repo.start_refresh(
            org_id="default", provider_id="wecom", created_by="browser"
        )
        source_repo.replace_snapshot(
            compatibility_snapshot_id,
            departments=source_repo.list_departments(
                snapshot_id, org_id="default"
            ),
            users=source_repo.list_users(
                snapshot_id,
                org_id="default",
                provider_id="dingtalk",
                limit=100,
            )["items"],
            fields=[
                {
                    "name": item["field_name"],
                    "label": item["field_label"],
                    "coverage": item["coverage_count"],
                    "samples": item["samples"],
                }
                for item in source_repo.list_field_catalog(
                    snapshot_id, org_id="default"
                )
            ],
            fingerprint="browser-identity-snapshot-wecom-compat-v1",
        )
        source_repo.save_scope_selection(
            org_id="default",
            provider_id="wecom",
            scope_type="full",
            username_strategy="employee_id",
            source_field="employee_id",
            snapshot_id=compatibility_snapshot_id,
            requested_by="browser",
        )
        bindings.upsert_binding(
            "browser-alice",
            "alice.manual",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            source="manual",
        )
        bindings.upsert_binding(
            "browser-carol",
            "carol.disabled",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            source="managed_generated",
            is_enabled=False,
        )
        resolution = {
            "source_provider": "dingtalk",
            "connector_id": "default",
            "source_display_name": "Alice Zhang",
            "source": "manual_binding",
            "ad_username": "alice.manual",
            "mapping_input": {
                "field_name": "employee_id",
                "field_label": "Employee ID",
                "value": "TJ001",
                "method": "employee_id",
                "template": "{employee_id}",
            },
            "candidate_mapping": {
                "ad_username": "TJ001",
                "source": "managed_username_primary",
                "risks": [],
            },
            "before_state": {
                "bound_ad_username": "alice.manual",
                "binding_source": "manual",
                "binding_enabled": True,
                "connector_id": "default",
                "ad_account_state": {
                    "status": "unavailable",
                    "exists": None,
                    "enabled": None,
                    "locked": None,
                    "protected": False,
                },
                "verified_at": "2026-07-14T01:00:00+00:00",
            },
            "rule_hits": ["manual_binding"],
            "explanation": "Manual binding overrides the field-generated candidate",
        }
        jobs.create_job(
            "browser-identity-dry",
            "browser_regression",
            "dry_run",
            "COMPLETED",
            org_id="default",
        )
        jobs.update_job(
            "browser-identity-dry",
            summary={"plan_fingerprint": "browser-plan-1"},
            ended=True,
        )
        source_repo.bind_job_scope(
            job_id="browser-identity-dry",
            execution_mode="dry_run",
            config_fingerprint="browser-config-1",
            selection=selection,
        )
        operations.add_record(
            job_id="browser-identity-dry",
            stage_name="plan",
            object_type="user_binding",
            operation_type="resolve_identity_binding",
            status="selected",
            message="structured resolution",
            source_id="browser-alice",
            target_id="alice.manual",
            details=resolution,
        )
        plans.add_operation(
            "browser-identity-dry",
            "user",
            "update_user",
            source_id="browser-alice",
            desired_state={"connector_id": "default", "ad_username": "alice.manual"},
        )
        jobs.create_job(
            "browser-identity-apply",
            "browser_regression",
            "apply",
            "COMPLETED",
            org_id="default",
        )
        jobs.update_job(
            "browser-identity-apply",
            summary={"plan_fingerprint": "browser-plan-1"},
            ended=True,
        )
        source_repo.bind_job_scope(
            job_id="browser-identity-apply",
            execution_mode="apply",
            config_fingerprint="browser-config-1",
            selection=selection,
        )
        operations.add_record(
            job_id="browser-identity-apply",
            stage_name="plan",
            object_type="user_binding",
            operation_type="resolve_identity_binding",
            status="selected",
            message="structured resolution",
            source_id="browser-alice",
            target_id="alice.manual",
            details=resolution,
        )
        operations.add_record(
            job_id="browser-identity-apply",
            stage_name="apply",
            object_type="user",
            operation_type="update_user",
            status="succeeded",
            message="updated",
            source_id="browser-alice",
            target_id="alice.manual",
            details={
                "connector_id": "default",
                "binding_resolution": resolution,
                "post_apply_ad_account_state": {
                    "status": "exists",
                    "exists": True,
                    "enabled": True,
                },
            },
        )
        failed_resolution = {
            **resolution,
            "source_display_name": "Bob Li",
            "source": "generated_username",
            "ad_username": "TJ002",
            "mapping_input": {
                **resolution["mapping_input"],
                "value": "TJ002",
            },
            "candidate_mapping": {
                **resolution["candidate_mapping"],
                "ad_username": "TJ002",
            },
            "before_state": {
                **resolution["before_state"],
                "bound_ad_username": "",
                "binding_source": "",
            },
            "rule_hits": ["managed_username_primary"],
            "explanation": "Field mapping candidate selected for Apply",
        }
        jobs.create_job(
            "browser-identity-failed",
            "browser_regression",
            "apply",
            "FAILED",
            org_id="default",
        )
        jobs.update_job(
            "browser-identity-failed",
            summary={"plan_fingerprint": "browser-plan-1"},
            ended=True,
        )
        source_repo.bind_job_scope(
            job_id="browser-identity-failed",
            execution_mode="apply",
            config_fingerprint="browser-config-1",
            selection=selection,
        )
        operations.add_record(
            job_id="browser-identity-failed",
            stage_name="plan",
            object_type="user_binding",
            operation_type="resolve_identity_binding",
            status="selected",
            message="structured resolution",
            source_id="browser-bob",
            target_id="TJ002",
            details=failed_resolution,
        )
        operations.add_record(
            job_id="browser-identity-failed",
            stage_name="apply",
            object_type="user",
            operation_type="create_user",
            status="failed",
            message="simulated browser evidence only",
            source_id="browser-bob",
            target_id="TJ002",
            details={
                "connector_id": "default",
                "binding_resolution": failed_resolution,
            },
        )

        self._login(ui_mode="advanced")
        self.page.goto(f"{self.base_url}/source-directory", wait_until="networkidle")
        self.assertTrue(self.page.get_by_role("heading", name="Source Directory").is_visible())
        self.assertTrue(self.page.get_by_role("button", name="Refresh Directory").is_visible())
        self.assertEqual(self.page.get_by_role("button", name="Test Connection").count(), 0)
        self.assertEqual(self.page.locator('select[name="scope_type"]').count(), 0)
        self.assertEqual(self.page.locator(".source-directory-tabs a").count(), 4)
        self.assertEqual(self.page.locator(".source-metric-card").count(), 9)
        self.assertEqual(
            self.page.locator(".page-header__actions .button:not(.secondary):not(.ghost)").count(),
            1,
        )
        self._capture("source-directory-overview-desktop-en.png")

        self.page.locator(".source-directory-tabs").get_by_role(
            "link", name="Users", exact=True
        ).click()
        self.page.wait_for_load_state("networkidle")
        source_table = self.page.locator(".source-directory-business-table")
        self.assertTrue(source_table.is_visible())
        self.assertEqual(source_table.locator("thead th").count(), 7)
        source_text = source_table.inner_text()
        for expected in ("Alice Zhang", "browser-alice", "TJ001", "Headquarters"):
            self.assertIn(expected.lower(), source_text.lower())
        for excluded in ("alice.manual", "Latest Dry Run", "Latest Apply"):
            self.assertNotIn(excluded.lower(), source_text.lower())
        self.assertNotIn("corpsecret", self.page.content().lower())
        self.assertNotIn("distinguishedname", self.page.content().lower())
        self._capture("source-directory-users-desktop-en.png")

        self.page.locator(".source-directory-tabs").get_by_role(
            "link", name="Departments", exact=True
        ).click()
        self.page.wait_for_load_state("networkidle")
        department_table = self.page.locator(".source-department-tree")
        self.assertTrue(department_table.is_visible())
        self.assertEqual(department_table.locator("thead th").count(), 4)
        self.assertIn("Headquarters", department_table.inner_text())
        self._capture("source-directory-departments-desktop-en.png")

        self.page.locator(".source-directory-tabs").get_by_role(
            "link", name="Snapshot History", exact=True
        ).click()
        self.page.wait_for_load_state("networkidle")
        history_table = self.page.locator(".source-snapshot-history")
        self.assertTrue(history_table.is_visible())
        self.assertEqual(history_table.locator("thead th").count(), 6)
        self._capture("source-directory-snapshot-history-desktop-en.png")

        self.page.set_viewport_size({"width": 390, "height": 844})
        self.page.goto(
            f"{self.base_url}/source-directory?view=overview&lang=zh-CN",
            wait_until="networkidle",
        )
        self._assert_page_has_no_horizontal_overflow()
        self.assertEqual(self.page.locator(".source-directory-tabs a").count(), 4)
        self._capture("source-directory-overview-mobile-zh.png")

        self.page.set_viewport_size({"width": 1440, "height": 1100})
        self.page.goto(
            f"{self.base_url}/identity-governance/binding-reconciliation?lang=en",
            wait_until="networkidle",
        )
        self.assertTrue(
            self.page.get_by_role(
                "heading", name="Start a Binding Scan", exact=True
            ).is_visible()
        )
        self.assertTrue(
            self.page.get_by_role("button", name="Scan Bindings").is_visible()
        )
        self.assertIn("Scanning is read-only", self.page.locator("body").inner_text())
        self.assertEqual(self.page.locator(".binding-reconciliation-table").count(), 0)
        self._capture("binding-reconciliation-start-desktop-en.png")

        self.page.goto(
            f"{self.base_url}/identity-governance/identity-matching?lang=en",
            wait_until="networkidle",
        )
        matching_table = self.page.locator("[data-identity-matching-table]")
        self.assertTrue(matching_table.is_visible())
        self.assertEqual(matching_table.locator("thead th").count(), 8)
        self.assertEqual(self.page.locator("[data-identity-queue]").count(), 6)
        self.assertEqual(
            self._style("[data-identity-matching-table] th:nth-child(1)", "position"),
            "sticky",
        )
        self.assertEqual(
            self._style("[data-identity-matching-table] th:nth-child(8)", "position"),
            "sticky",
        )
        opener = self.page.locator("[data-identity-drawer-open]").first
        opener.focus()
        opener.evaluate("element => element.click()")
        drawer = self.page.locator("[data-identity-drawer]:visible")
        self.assertEqual(drawer.count(), 1)
        self.assertEqual(
            self._style(
                "[data-identity-drawer]:visible .identity-drawer__backdrop",
                "background-color",
            ),
            "rgba(15, 23, 42, 0.56)",
        )
        self.assertEqual(drawer.locator("[data-identity-timeline-step]").count(), 7)
        for stage in (
            "Source fields and Candidate calculation",
            "Before: Saved binding",
            "Latest verified AD status",
            "Planned: Latest Dry Run",
            "Applied: Latest Apply",
            "Current AD actual state",
            "Risks, conflicts, and audit records",
        ):
            self.assertTrue(drawer.get_by_text(stage, exact=True).is_visible())
        self.assertEqual(
            drawer.locator(
                'a[href="/execution-center/jobs/browser-identity-dry"]'
            ).count(),
            2,
        )
        self.assertEqual(
            drawer.locator(
                'a[href="/execution-center/jobs/browser-identity-apply"]'
            ).count(),
            2,
        )
        self.assertTrue(
            drawer.evaluate("element => element.contains(document.activeElement)")
        )
        self.page.keyboard.press("Shift+Tab")
        self.assertTrue(
            drawer.evaluate("element => element.contains(document.activeElement)")
        )
        self._capture_viewport("identity-matching-drawer-desktop-en.png")
        self.page.keyboard.press("Escape")
        self.assertEqual(self.page.locator("[data-identity-drawer]:visible").count(), 0)
        self.assertTrue(opener.evaluate("element => document.activeElement === element"))

        self.page.goto(
            (
                f"{self.base_url}/identity-governance/identity-matching"
                "?lang=en&queue=all&search=Bob&employee_status=active"
                "&identity_status=ad_status_unknown&ad_status=unknown"
            ),
            wait_until="networkidle",
        )
        self.assertEqual(self.page.locator("[data-active-filter]").count(), 4)
        self.assertEqual(
            self.page.locator("[data-identity-matching-table] tbody tr").count(),
            1,
        )
        self.assertIn(
            "Bob Li",
            self.page.locator("[data-identity-matching-table]").inner_text(),
        )

        self.page.set_viewport_size({"width": 480, "height": 900})
        self.page.goto(
            f"{self.base_url}/identity-governance/identity-matching?lang=en",
            wait_until="networkidle",
        )
        self.page.locator("[data-identity-drawer-open]").first.click()
        narrow_panel = self.page.locator(
            "[data-identity-drawer]:visible .identity-drawer__panel"
        )
        self.assertGreaterEqual(narrow_panel.bounding_box()["width"], 475)
        self.assertTrue(
            self.page.locator(
                "[data-identity-drawer]:visible .identity-drawer__footer"
            ).is_visible()
        )
        self.assertTrue(
            self.page.evaluate(
                "() => document.documentElement.scrollWidth <= window.innerWidth"
            )
        )
        self._capture_viewport("identity-matching-drawer-narrow-en.png")
        self.page.keyboard.press("Escape")
        self.page.set_viewport_size({"width": 1440, "height": 1100})

        with patch(
            "sync_app.web.app.build_target_provider",
            return_value=_MissingAccountPreviewProvider(),
        ):
            self.page.goto(
                f"{self.base_url}/identity-governance/identity-matching?lang=en&verify_ad=true",
                wait_until="networkidle",
            )
        self.assertTrue(
            self.page.get_by_role(
                "heading", name="Identity Matching Workbench", level=1
            ).is_visible()
        )
        matching_table = self.page.locator("[data-identity-matching-table]")
        self.assertEqual(matching_table.locator("thead th").count(), 8)
        alice_row = self.page.locator("tbody tr").filter(has_text="browser-alice")
        self.assertTrue(
            alice_row.get_by_text("Saved binding has expired", exact=True).is_visible()
        )
        self.assertEqual(
            alice_row.locator('input[name="selected_source_user_ids"]:disabled').count(),
            1,
        )
        bob_row = self.page.locator("tbody tr").filter(has_text="browser-bob")
        bob_row.locator('input[name="selected_source_user_ids"]').check()
        self.assertTrue(
            bob_row.locator('input[name="selected_source_user_ids"]').is_checked()
        )
        self.assertTrue(
            self.page.get_by_role(
                "button", name="Prepare account creation"
            ).is_enabled()
        )
        batch_bar = self.page.locator("[data-identity-batch-bar]")
        self.assertTrue(batch_bar.is_visible())
        self.assertEqual(
            batch_bar.evaluate("element => getComputedStyle(element).position"),
            "fixed",
        )
        self.assertIn("1 users selected", batch_bar.inner_text())
        self.assertIn("for preparing account creation", batch_bar.inner_text())
        self.page.select_option("[data-identity-batch-mode]", "defer")
        self.assertFalse(
            bob_row.locator('input[name="selected_source_user_ids"]').is_checked()
        )
        self.assertNotIn("is-active", batch_bar.get_attribute("class"))
        self.assertEqual(batch_bar.locator("[data-selection-count]").inner_text(), "0")
        self.assertTrue(
            self.page.get_by_role(
                "button", name="Temporarily defer selected"
            ).is_disabled()
        )
        self._capture("identity-relationship-create-selection-desktop-en.png")

        self.page.goto(
            f"{self.base_url}/sync-policies/account-naming?lang=en",
            wait_until="networkidle",
        )
        self.assertEqual(self.page.locator('select[name="source_field"]').count(), 1)
        self.page.select_option('select[name="source_field"]', "source_user_id")
        self.assertEqual(
            self.page.locator('select[name="source_field"]').input_value(),
            "source_user_id",
        )
        self.page.select_option('select[name="source_field"]', "employee_id")

        self.page.goto(
            f"{self.base_url}/identity-governance/binding-reconciliation?lang=en",
            wait_until="networkidle",
        )
        cleanup_button = self.page.get_by_role("button", name="Scan Bindings")
        self.assertTrue(cleanup_button.is_visible())
        with patch(
            "sync_app.web.app.build_target_provider",
            return_value=_MissingAccountPreviewProvider(),
        ):
            cleanup_button.click()
            self.page.wait_for_url(
                "**/identity-governance/binding-reconciliation?**cleanup_preview=true"
            )

        self.assertTrue(
            self.page.get_by_role("heading", name="Scan Results").is_visible()
        )
        self.assertEqual(self.page.locator("[data-high-risk-step]").count(), 5)
        self.assertEqual(
            self.page.locator(
                "[data-binding-reconciliation-categories] .compact-stat"
            ).count(),
            8,
        )
        preview_text = self.page.locator("[data-high-risk-context]").inner_text().lower()
        self.assertIn("organization", preview_text)
        self.assertIn("environment", preview_text)
        self.assertIn("snapshot version", preview_text)
        self.assertIn("impact count", preview_text)
        scan_context = self.page.locator(
            "[data-binding-scan-context]"
        ).inner_text().lower()
        self.assertIn("scan time", scan_context)
        self.assertIn("cleanable count", scan_context)
        self.assertIn("skip count", scan_context)
        relationship_table = self.page.locator(".binding-reconciliation-table")
        self.assertTrue(relationship_table.is_visible())
        self.assertEqual(relationship_table.locator("thead th").count(), 8)
        self.assertIn(
            "alice.manual",
            relationship_table.locator("tbody tr")
            .filter(has_text="browser-alice")
            .inner_text(),
        )

        self.page.get_by_role(
            "button", name="Confirm Cleanup of 2 Binding(s)"
        ).click()
        cleanup_dialog = self.page.locator("[data-confirm-dialog]")
        self.assertTrue(cleanup_dialog.is_visible())
        self.assertIn(
            "Every item must again return missing with exists=false",
            cleanup_dialog.inner_text(),
        )
        for expected in (
            "Current Organization",
            "Current Environment",
            "Snapshot Version",
            "Scan Time",
            "Cleanable Count",
            "Skip Count",
            "Exact Accounts To Delete",
        ):
            self.assertIn(expected, cleanup_dialog.inner_text())
        with patch(
            "sync_app.web.app.build_target_provider",
            return_value=_MissingAccountPreviewProvider(),
        ):
            cleanup_dialog.locator("[data-confirm-approve]").click()
            self.page.wait_for_url(
                "**/identity-governance/binding-reconciliation?**cleanup_preview=true"
            )
        self.assertIn(
            "Binding cleanup completed: 2 removed",
            self.page.locator("body").inner_text(),
        )
        cleaned_alice_row = relationship_table.locator("tbody tr").filter(
            has_text="browser-alice"
        )
        current_binding_cell = cleaned_alice_row.locator("td").nth(2).inner_text()
        self.assertIn("alice.manual", current_binding_cell)
        self.assertIn("Now", current_binding_cell)
        self.assertIn("No binding", current_binding_cell)
        self.assertTrue(
            cleaned_alice_row.get_by_text("Can create", exact=True).is_visible()
        )
        self.assertTrue(
            self.page.get_by_role(
                "link", name="Go to Identity Matching Workbench"
            ).first.is_visible()
        )
        self._capture("binding-reconciliation-cleanup-desktop-en.png")

        bindings.upsert_binding(
            "browser-alice",
            "alice.manual",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            source="manual",
        )
        bindings.upsert_binding(
            "browser-carol",
            "carol.disabled",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            source="managed_generated",
            is_enabled=False,
        )

        self.page.set_viewport_size({"width": 390, "height": 844})
        self.page.goto(
            f"{self.base_url}/identity-governance/binding-reconciliation?lang=zh-CN",
            wait_until="networkidle",
        )
        self._assert_page_has_no_horizontal_overflow()
        self.assertEqual(
            relationship_table.get_attribute("aria-label"),
            "绑定对账明细",
        )
        self.assertTrue(self.page.get_by_role("heading", name="绑定对账").is_visible())
        self.assertEqual(relationship_table.locator("thead th").count(), 8)
        self.page.goto(
            f"{self.base_url}/sync-policies/scope?lang=zh-CN",
            wait_until="networkidle",
        )
        self._assert_page_has_no_horizontal_overflow()
        self.assertEqual(
            self.page.locator("[data-source-selection-status]").inner_text(),
            "当前页已选择 0 个用户。",
        )
        self._capture("identity-relationship-mobile-zh.png")

        self.page.set_viewport_size({"width": 1440, "height": 1100})
        self.page.goto(
            f"{self.base_url}/jobs/browser-identity-apply?lang=zh-CN",
            wait_until="networkidle",
        )
        self.assertIn("身份解析结果", self.page.locator("body").inner_text())
        self.assertIn("Alice Zhang", self.page.locator("body").inner_text())
        self._capture("identity-resolution-job-detail-zh.png")

        self.page.goto(
            f"{self.base_url}/source-directory?view=users&lang=en&search=no-such-browser-user",
            wait_until="networkidle",
        )
        self.assertIn("No cached users match", self.page.locator("body").inner_text())
        self._capture("source-directory-empty-state.png")

        polling_snapshot_id = source_repo.start_refresh(
            org_id="default", provider_id="dingtalk", created_by="browser-poll"
        )
        self.page.goto(
            f"{self.base_url}/source-directory?lang=en",
            wait_until="networkidle",
        )
        self.assertTrue(self.page.locator("[data-source-refresh-poll]").is_visible())
        self.assertTrue(
            self.page.get_by_text(
                "previous successful snapshot remains available", exact=False
            ).is_visible()
        )
        self.assertTrue(
            self.page.get_by_role("link", name="View refresh task").is_visible()
        )
        self._capture("source-directory-refreshing-auto-update-en.png")
        source_repo.replace_snapshot(
            polling_snapshot_id,
            departments=[
                {
                    "source_department_id": "1",
                    "name": "Headquarters",
                    "parent_department_id": "0",
                    "path_ids": ["1"],
                    "path_names": ["Headquarters"],
                }
            ],
            users=[
                {
                    "source_user_id": "browser-refresh-result",
                    "display_name": "Refresh Result",
                    "employee_id": "TJ900",
                    "department_ids": ["1"],
                    "department_names": ["Headquarters"],
                    "is_active": True,
                    "raw_payload": {
                        "userid": "browser-refresh-result",
                        "employee_id": "TJ900",
                    },
                    "search_text": "Refresh Result TJ900",
                }
            ],
            fields=[
                {
                    "name": "employee_id",
                    "label": "Employee ID",
                    "coverage": 1,
                    "samples": ["TJ900"],
                }
            ],
            fingerprint="browser-refresh-poll-v1",
        )
        self.page.locator("[data-source-refresh-poll]").wait_for(
            state="detached", timeout=8000
        )
        self.page.locator(".source-directory-tabs").get_by_role(
            "link", name="Users", exact=True
        ).click()
        self.page.wait_for_load_state("networkidle")
        self.assertIn("Refresh Result", self.page.locator("body").inner_text())
        self.page.goto(
            f"{self.base_url}/sync-policies/account-naming?lang=en",
            wait_until="networkidle",
        )
        self.page.select_option('select[name="source_field"]', "employee_id")
        self.assertEqual(
            self.page.locator('select[name="source_field"]').input_value(),
            "employee_id",
        )
        self._capture("sync-account-naming-refresh-complete-mapping-en.png")

    def _assert_page_has_no_horizontal_overflow(self) -> None:
        dimensions = self.page.evaluate(
            """() => ({
                viewportWidth: document.documentElement.clientWidth,
                documentWidth: document.documentElement.scrollWidth,
                bodyWidth: document.body.scrollWidth,
                overflowing: Array.from(document.querySelectorAll('body *'))
                    .filter(element => {
                        const rect = element.getBoundingClientRect();
                        const style = getComputedStyle(element);
                        return style.position !== 'fixed'
                            && rect.width > 0
                            && (rect.right > innerWidth + 1 || rect.left < -1);
                    })
                    .slice(0, 12)
                    .map(element => ({
                        tag: element.tagName,
                        className: String(element.className || ''),
                        id: element.id || '',
                        rect: element.getBoundingClientRect().toJSON(),
                    })),
            })"""
        )
        self.assertLessEqual(
            dimensions["documentWidth"], dimensions["viewportWidth"] + 1, dimensions
        )
        self.assertLessEqual(
            dimensions["bodyWidth"], dimensions["viewportWidth"] + 1, dimensions
        )

    def _assert_mobile_page_header_is_compact(self) -> None:
        header = self.page.locator(".page-header")
        if header.count() == 0:
            return
        header_box = header.bounding_box()
        meta_box = self.page.locator(".page-header__meta").bounding_box()
        self.assertIsNotNone(header_box)
        self.assertIsNotNone(meta_box)
        self.assertLess(float(header_box["height"]), 420.0, header_box)
        self.assertLess(float(meta_box["height"]), 220.0, meta_box)

    def test_responsive_pages_keep_tables_local_and_body_within_viewport(self):
        self._login()
        for width in (390, 768, 1024, 1366, 1440):
            self.page.set_viewport_size({"width": width, "height": 900})
            for path in ("/dashboard", "/data-sources/connectors", "/audit"):
                with self.subTest(width=width, path=path):
                    self.page.goto(f"{self.base_url}{path}", wait_until="networkidle")
                    self._assert_page_has_no_horizontal_overflow()
                    if width <= 1024:
                        self._assert_mobile_page_header_is_compact()

                    table_shells = self.page.locator(".table-shell")
                    for index in range(table_shells.count()):
                        shell = table_shells.nth(index)
                        if not shell.is_visible():
                            continue
                        shell_box = shell.bounding_box()
                        self.assertIsNotNone(shell_box)
                        self.assertLessEqual(
                            float(shell_box["width"]), float(width) + 1
                        )
                        self.assertIn(
                            shell.evaluate(
                                "element => getComputedStyle(element).overflowX"
                            ),
                            {"auto", "scroll"},
                        )
                        self.assertGreaterEqual(int(shell.get_attribute("tabindex") or -1), 0)

    def test_connectors_mobile_summary_and_connection_form_remain_usable(self):
        self.context.close()
        self.context = self.browser.new_context(
            viewport={"width": 390, "height": 900}, locale="en-US"
        )
        self.page = self.context.new_page()
        self._login()
        self.page.goto(
            f"{self.base_url}/data-sources/connectors?lang=zh-CN",
            wait_until="networkidle",
        )

        metrics = self.page.evaluate(
            """() => {
                const root = document.documentElement;
                const header = document.querySelector('.page-header')?.getBoundingClientRect();
                const stats = document.querySelector('.stat-grid')?.getBoundingClientRect();
                const firstField = document.querySelector(
                    'form[data-connector-base-form] input:not([type="hidden"]), form[data-connector-base-form] select, form[data-connector-base-form] textarea'
                )?.getBoundingClientRect();
                const form = document.querySelector('form[data-connector-base-form]');
                const zeroTextContainers = Array.from(document.querySelectorAll('main *'))
                    .filter((element) => {
                        const style = getComputedStyle(element);
                        const rect = element.getBoundingClientRect();
                        return !['SVG', 'PATH'].includes(element.tagName)
                            && style.display !== 'none'
                            && style.visibility !== 'hidden'
                            && rect.height > 0
                            && rect.width === 0
                            && (element.innerText || '').trim();
                    })
                    .slice(0, 10)
                    .map((element) => `${element.tagName}.${element.className}`);
                return {
                    clientWidth: root.clientWidth,
                    scrollWidth: root.scrollWidth,
                    headerWidth: header?.width || 0,
                    statsWidth: stats?.width || 0,
                    firstFieldY: firstField?.y ?? Number.MAX_SAFE_INTEGER,
                    formWidth: form?.getBoundingClientRect().width || 0,
                    zeroTextContainers,
                };
            }"""
        )

        self.assertLessEqual(metrics["scrollWidth"], metrics["clientWidth"] + 1, metrics)
        self.assertGreater(metrics["headerWidth"], 200, metrics)
        self.assertGreater(metrics["statsWidth"], 200, metrics)
        self.assertGreater(metrics["formWidth"], 200, metrics)
        self.assertLess(metrics["firstFieldY"], 2200, metrics)
        self.assertEqual(metrics["zeroTextContainers"], [], metrics)
        self._capture("connectors-page-390.png")

        self.page.set_viewport_size({"width": 1024, "height": 900})
        self.page.reload(wait_until="networkidle")
        self._assert_page_has_no_horizontal_overflow()
        self.assertTrue(self.page.locator("form[data-connector-base-form]").is_visible())

    def test_high_risk_flow_uses_five_three_two_and_one_column_layouts(self):
        self._login()
        expected_columns = {390: 1, 768: 2, 1024: 3, 1366: 5, 1440: 5}
        for width, expected in expected_columns.items():
            with self.subTest(width=width):
                self.page.set_viewport_size({"width": width, "height": 900})
                self.page.goto(f"{self.base_url}/jobs", wait_until="networkidle")
                self._assert_page_has_no_horizontal_overflow()
                layout = self.page.locator(".execution-flow").evaluate(
                    """element => {
                        const rects = Array.from(element.children).map(child => child.getBoundingClientRect());
                        const firstY = rects[0]?.y || 0;
                        return {
                            columns: rects.filter(rect => Math.abs(rect.y - firstY) < 1).length,
                            clientWidth: element.clientWidth,
                            scrollWidth: element.scrollWidth,
                        };
                    }"""
                )
                self.assertEqual(layout["columns"], expected, layout)
                self.assertLessEqual(layout["scrollWidth"], layout["clientWidth"] + 1, layout)

    def test_sync_policy_pages_keep_one_primary_action_and_keyboard_flow(self):
        self._login(ui_mode="advanced")
        policy_pages = (
            ("/sync-policies/scope?lang=en", "Sync Scope"),
            ("/sync-policies/account-naming?lang=en", "Account Naming"),
            ("/sync-policies/attribute-mappings?lang=en", "Attribute Mappings"),
            ("/sync-policies/department-ou-routing?lang=en", "Department & OU Routing"),
            ("/sync-policies/group-rules?lang=en", "Group Rules"),
            ("/sync-policies/lifecycle?lang=en", "Lifecycle & Security"),
            ("/sync-policies/security?lang=en", "Security Policy"),
        )
        for path, heading in policy_pages:
            with self.subTest(path=path):
                self.page.goto(f"{self.base_url}{path}", wait_until="networkidle")
                self.assertTrue(
                    self.page.get_by_role("heading", name=heading, level=1).is_visible()
                )
                self.assertEqual(
                    self.page.locator(
                        "main .button:not(.secondary):not(.ghost):not(.danger)"
                    ).count(),
                    1,
                )
                self.assertIn("Current status", self.page.locator("main").inner_text())
                self.assertIn("Blocking reason", self.page.locator("main").inner_text())
                self.assertIn("Next step", self.page.locator("main").inner_text())
                for index in range(self.page.locator("main table").count()):
                    self.assertLessEqual(
                        self.page.locator("main table").nth(index).locator("thead th").count(),
                        8,
                    )

        self.page.goto(
            f"{self.base_url}/sync-policies/scope?lang=en",
            wait_until="networkidle",
        )
        current_tab = self.page.locator(".policy-tabs__link[aria-current='page']")
        current_tab.focus()
        self.assertEqual(current_tab.evaluate("element => document.activeElement === element"), True)
        self.page.keyboard.press("Tab")
        self.assertIn(
            "/sync-policies/account-naming",
            self.page.evaluate("document.activeElement?.getAttribute('href') || ''"),
        )

        self.page.goto(
            f"{self.base_url}/sync-policies/account-naming?lang=en",
            wait_until="networkidle",
        )
        self.page.get_by_role("button", name="Examples & Preview").click()
        self.page.locator("#account-naming-help:not([hidden])").wait_for(state="visible")
        self.page.fill('input[name="sample_userid"]', "browser-policy-user")
        self.page.fill('input[name="sample_name"]', "Browser Policy User")
        self.page.get_by_role("button", name="Preview Username").click()
        self.page.wait_for_function(
            "() => (document.querySelector('[data-username-preview-results]')?.textContent || '').trim().length > 0"
        )
        self.assertNotIn("browser-directory-secret", self.page.content())
        self._capture_viewport("sync-policy-account-naming-desktop-en.png")

        self.page.set_viewport_size({"width": 390, "height": 900})
        self.page.reload(wait_until="networkidle")
        self._assert_page_has_no_horizontal_overflow()
        tab_metrics = self.page.locator(".policy-tabs").evaluate(
            "element => ({clientWidth: element.clientWidth, scrollWidth: element.scrollWidth, overflowX: getComputedStyle(element).overflowX})"
        )
        self.assertIn(tab_metrics["overflowX"], {"auto", "scroll"}, tab_metrics)
        self.assertGreaterEqual(tab_metrics["scrollWidth"], tab_metrics["clientWidth"], tab_metrics)
        self._capture_viewport("sync-policy-account-naming-narrow-en.png")

    def test_execution_center_pages_and_review_apply_flow_are_accessible(self):
        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        with manager.transaction() as connection:
            connection.execute(
                "UPDATE sync_conflicts SET status = 'resolved' WHERE status = 'open'"
            )
            connection.execute(
                "UPDATE sync_jobs SET status = 'FAILED', ended_at = ? "
                "WHERE org_id = 'default' AND status IN "
                "('QUEUED', 'LEASED', 'CREATED', 'PLANNING', 'READY', 'RUNNING', 'CANCELING')",
                (datetime.now(timezone.utc).isoformat(timespec="seconds"),),
            )
        OrganizationConfigRepository(manager).save_config(
            "default",
            {
                "source_provider": "wecom",
                "corpid": "browser-execution-corp",
                "agentid": "10001",
                "corpsecret": "browser-execution-secret",
                "webhook_url": "",
                "ldap_server": "dc.browser.example",
                "ldap_domain": "browser.example",
                "ldap_username": "svc-browser-execution",
                "ldap_password": "browser-directory-secret",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": False,
                "ldap_ca_cert_path": "",
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
            },
            config_path="config.ini",
        )
        created = create_eligible_execution_plan(
            manager,
            job_id="browser-execution-plan",
            environment_label="Local environment",
            planned_operation_count=3,
        )

        self._login()
        pages = (
            "/jobs?plan_id=browser-execution-plan&lang=en",
            "/execution-center/dry-run?lang=en",
            (
                "/execution-center/plan-review"
                "?plan_id=browser-execution-plan&lang=en"
            ),
            (
                "/execution-center/apply"
                "?plan_id=browser-execution-plan&lang=en"
            ),
            "/execution-center/jobs?lang=en",
        )
        for path in pages:
            with self.subTest(path=path):
                self.page.goto(f"{self.base_url}{path}", wait_until="networkidle")
                self.assertTrue(
                    self.page.get_by_role(
                        "heading", name="Jobs", level=1, exact=True
                    ).is_visible()
                )
                self.assertEqual(
                    self.page.locator(".page-header__actions .button").count(),
                    1,
                )
                self.assertEqual(
                    self.page.locator(".execution-tabs a[aria-current='page']").count(),
                    1,
                )
                for section_id in (
                    "current-status",
                    "dry-run",
                    "plan-review",
                    "apply",
                    "history",
                ):
                    self.assertEqual(
                        self.page.locator(f"section#{section_id}").count(),
                        1,
                    )
                main_text = self.page.locator("main").inner_text().lower()
                self.assertIn("organization", main_text)
                self.assertIn("environment", main_text)
                self.assertIn("current source snapshot", main_text)
                self.assertIn("current sync policy version", main_text)
                self.assertIn("selected dry run id", main_text)
                self.assertIn("task history", main_text)
                for index in range(self.page.locator("main table").count()):
                    self.assertLessEqual(
                        self.page.locator("main table")
                        .nth(index)
                        .locator("thead th")
                        .count(),
                        8,
                    )

        self.page.goto(
            (
                f"{self.base_url}/jobs"
                "?plan_id=browser-execution-plan&lang=en#plan-review"
            ),
            wait_until="networkidle",
        )
        current_tab = self.page.locator(
            ".execution-tabs a[aria-current='page']"
        )
        current_tab.focus()
        self.assertTrue(
            current_tab.evaluate("element => document.activeElement === element")
        )
        self.page.keyboard.press("Tab")
        self.assertEqual(
            self.page.evaluate("document.activeElement?.getAttribute('href') || ''"),
            "#dry-run",
        )

        self.page.get_by_role("button", name="Approve selected plan").click()
        approval_dialog = self.page.locator("[data-confirm-dialog]")
        self.assertTrue(approval_dialog.is_visible())
        approval_dialog.locator("[data-confirm-approve]").click()
        self.page.wait_for_load_state("networkidle")
        self.assertIn("approved", self.page.locator("main").inner_text().lower())

        self.page.goto(
            (
                f"{self.base_url}/jobs"
                "?plan_id=browser-execution-plan&lang=en#apply"
            ),
            wait_until="networkidle",
        )
        self.assertIn(f"#{created['snapshot_id']}", self.page.locator("main").inner_text())
        apply_button = self.page.get_by_role("button", name="Apply 3 Changes")
        with patch.object(
            self.server.config.app.state.sync_runner,
            "launch",
            return_value=(True, "Apply queued for browser regression"),
        ) as launch:
            apply_button.click()
            dialog = self.page.locator("[data-confirm-dialog]")
            self.assertTrue(dialog.is_visible())
            details = dialog.locator("[data-confirm-details]").inner_text()
            self.assertIn("Default Organization", details)
            self.assertIn("Local environment", details)
            self.assertIn(f"#{created['snapshot_id']}", details)
            self.assertIn("3", details)
            dialog.locator("[data-confirm-approve]").click()
            self.page.wait_for_load_state("networkidle")
        self.assertEqual(
            launch.call_args.kwargs["plan_source_job_id"],
            "browser-execution-plan",
        )

        local_time = self.page.locator("time[data-local-time]").first
        self.assertTrue(local_time.is_visible())
        self.assertTrue(local_time.get_attribute("datetime"))
        self.assertTrue(local_time.get_attribute("title"))
        self._capture_viewport("execution-center-desktop-en.png")

        self.page.goto(
            (
                f"{self.base_url}/jobs"
                "?plan_id=browser-execution-plan&lang=zh-CN#plan-review"
            ),
            wait_until="networkidle",
        )
        self.assertTrue(
            self.page.get_by_role(
                "heading", name="\u4efb\u52a1", level=1, exact=True
            ).is_visible()
        )

        self.page.set_viewport_size({"width": 390, "height": 900})
        self.page.goto(
            (
                f"{self.base_url}/jobs"
                "?plan_id=browser-execution-plan&lang=zh-CN#history"
            ),
            wait_until="networkidle",
        )
        self._assert_page_has_no_horizontal_overflow()
        context_columns = self.page.locator(".execution-context").evaluate(
            "element => getComputedStyle(element).gridTemplateColumns.split(' ').length"
        )
        self.assertEqual(context_columns, 1)
        tabs = self.page.locator(".execution-tabs").evaluate(
            "element => ({clientWidth: element.clientWidth, scrollWidth: element.scrollWidth, overflowX: getComputedStyle(element).overflowX})"
        )
        self.assertIn(tabs["overflowX"], {"auto", "scroll"}, tabs)
        self.assertGreaterEqual(tabs["scrollWidth"], tabs["clientWidth"], tabs)
        history_section = self.page.locator("section#history")
        self.assertTrue(history_section.is_visible())
        history_section.scroll_into_view_if_needed()
        history_box = history_section.bounding_box()
        self.assertIsNotNone(history_box)
        self.assertLess(float(history_box["y"]), 900)
        self._capture_viewport("execution-center-narrow-zh.png")

    def test_jobs_is_the_only_browser_apply_surface(self):
        self._login()
        self.page.goto(f"{self.base_url}/jobs?lang=en", wait_until="networkidle")
        self.assertEqual(
            self.page.locator("aside a[href='/jobs']").count(),
            1,
        )
        self.assertEqual(
            self.page.locator(
                "aside a[href^='/execution-center/']"
            ).count(),
            0,
        )

        non_execution_pages = (
            "/dashboard?lang=en",
            "/data-sources/connectors?lang=en",
            "/data-sources/source-directory?lang=en",
            "/sync-policies?lang=en",
            "/operations/automation?lang=en",
        )
        for path in non_execution_pages:
            with self.subTest(path=path):
                self.page.goto(
                    f"{self.base_url}{path}",
                    wait_until="networkidle",
                )
                self.assertEqual(
                    self.page.locator(
                        "form[action='/execution-center/apply/run'], "
                        "form:has(input[name='mode'][value='apply'])"
                    ).count(),
                    0,
                )

        self.page.goto(
            f"{self.base_url}/dashboard?lang=en",
            wait_until="networkidle",
        )
        self.assertEqual(
            self.page.locator("form[action='/jobs/run']").count(),
            0,
        )

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
        self.assertNotEqual(
            self._style(".mode-switcher button.active", "color"), "rgb(255, 255, 255)"
        )
        self.assertNotEqual(
            self._style(".language-switcher a.active", "color"), "rgb(255, 255, 255)"
        )
        self.assertNotEqual(
            self._style(".header-signout", "border-top-color"), "rgba(0, 0, 0, 0)"
        )
        self.assertTrue(self.page.locator(".control-tower").is_visible())
        self.assertTrue(self.page.locator(".control-gate-card").is_visible())
        gate_box = self.page.locator(".control-gate-card").bounding_box()
        self.assertIsNotNone(gate_box)
        self.assertGreater(float(gate_box["x"]), 300.0)
        self._capture("dashboard-page.png")

    def test_mobile_header_controls_meet_touch_target_minimum(self):
        self.context.close()
        self.context = self.browser.new_context(
            viewport={"width": 390, "height": 900}, locale="en-US"
        )
        self.page = self.context.new_page()
        self._login()
        self.page.goto(f"{self.base_url}/dashboard", wait_until="networkidle")

        self.page.locator("[data-mobile-nav-toggle]").click()

        for selector in (
            ".mobile-nav-toggle",
            ".sidebar-mode-switcher .topbar-segment",
            ".sidebar-language-switcher .topbar-segment",
            ".sidebar-mobile-signout .button",
        ):
            with self.subTest(selector=selector):
                self.assertGreaterEqual(self._height(selector), 40.0)

    def test_recent_navigation_never_duplicates_the_current_page_highlight(self):
        self._login()
        self.page.goto(f"{self.base_url}/dashboard", wait_until="networkidle")
        self.page.evaluate(
            """window.localStorage.setItem(
                "ad-org-sync.recent-navigation",
                JSON.stringify(["/dashboard", "/jobs"])
            )"""
        )

        self.page.reload(wait_until="networkidle")

        current_links = self.page.locator(
            "[data-sidebar-nav] a[aria-current='page']"
        )
        recent_links = self.page.locator("[data-sidebar-recent-link]")
        self.assertEqual(current_links.count(), 1)
        self.assertEqual(
            current_links.first.get_attribute("href"), "/overview/control-tower"
        )
        self.assertEqual(recent_links.count(), 1)
        self.assertEqual(
            recent_links.first.get_attribute("href"), "/jobs"
        )
        self.assertEqual(
            self.page.locator("[data-sidebar-recent-link].active").count(), 0
        )

    def test_legacy_job_detail_keeps_job_history_navigation_active(self):
        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        job_repo = SyncJobRepository(manager)
        if job_repo.get_job_record("browser-legacy-job") is None:
            job_repo.create_job(
                "browser-legacy-job",
                trigger_type="unit_test",
                execution_mode="dry_run",
                status="COMPLETED",
                org_id="default",
            )

        self._login()
        self.page.goto(
            f"{self.base_url}/jobs/browser-legacy-job",
            wait_until="networkidle",
        )

        current_links = self.page.locator(
            "[data-sidebar-nav] a[aria-current='page']"
        )
        self.assertEqual(current_links.count(), 1)
        self.assertEqual(
            current_links.first.get_attribute("href"),
            "/jobs",
        )

    def test_mobile_navigation_traps_focus_and_restores_the_menu_button(self):
        self.context.close()
        self.context = self.browser.new_context(
            viewport={"width": 390, "height": 900}, locale="en-US"
        )
        self.page = self.context.new_page()
        self._login()
        self.page.goto(f"{self.base_url}/dashboard", wait_until="networkidle")

        toggle = self.page.locator("[data-mobile-nav-toggle]")
        self.assertTrue(toggle.is_visible())
        self.assertEqual(
            self.page.locator("[data-sidebar-nav] a[aria-current='page']").count(), 1
        )
        self.assertFalse(self.page.locator("header .mode-switcher").is_visible())
        self.assertFalse(self.page.locator("header .language-switcher").is_visible())

        toggle.click()
        close_button = self.page.locator("[data-app-sidebar] [data-mobile-nav-close]")
        self.assertTrue(close_button.is_visible())
        self.assertTrue(
            close_button.evaluate("element => document.activeElement === element")
        )
        self.assertTrue(self.page.locator(".sidebar-mobile-preferences").is_visible())
        self.assertIsNotNone(self.page.locator("main").get_attribute("inert"))

        self.page.keyboard.press("Escape")
        self.assertFalse(self.page.locator("body").evaluate("body => body.classList.contains('mobile-nav-open')"))
        self.assertTrue(toggle.evaluate("element => document.activeElement === element"))
        self.assertIsNone(self.page.locator("main").get_attribute("inert"))

    def test_text_scaling_and_reduced_motion_keep_critical_pages_operable(self):
        self.context.close()
        self.context = self.browser.new_context(
            viewport={"width": 390, "height": 900}, locale="en-US"
        )
        self.page = self.context.new_page()
        self.page.emulate_media(reduced_motion="reduce")
        self.page.goto(f"{self.base_url}/login", wait_until="networkidle")
        self.page.evaluate(
            "document.documentElement.style.webkitTextSizeAdjust = '200%'"
        )
        self.assertEqual(
            self.page.evaluate(
                "getComputedStyle(document.documentElement).webkitTextSizeAdjust"
            ),
            "200%",
        )
        self._assert_page_has_no_horizontal_overflow()
        for selector in (".login-card", "#username", "#password", "button[type='submit']"):
            with self.subTest(selector=selector):
                box = self.page.locator(selector).bounding_box()
                self.assertIsNotNone(box)
                self.assertGreater(float(box["width"]), 0)
                self.assertGreaterEqual(float(box["x"]), 0)
                self.assertLessEqual(float(box["x"] + box["width"]), 390)
        self._capture("login-text-scale-200.png")

        self.page.fill("#username", "admin")
        self.page.fill("#password", "simple888")
        self.page.click("button[type='submit']")
        self.page.wait_for_url(f"{self.base_url}/dashboard")
        reduced_transition = self._style("aside", "transition-duration")
        self.assertTrue(reduced_transition.endswith("s"))
        self.assertLessEqual(float(reduced_transition.removesuffix("s")), 0.001)
        self.page.goto(
            f"{self.base_url}/data-sources/connectors",
            wait_until="networkidle",
        )
        self.page.evaluate(
            "document.documentElement.style.webkitTextSizeAdjust = '200%'"
        )
        self._assert_page_has_no_horizontal_overflow()
        first_field = self.page.locator(
            "form[data-connector-base-form] input:not([type='hidden']), form[data-connector-base-form] select, form[data-connector-base-form] textarea"
        ).first
        self.assertGreater(float(first_field.bounding_box()["width"]), 0)
        self._capture("connectors-text-scale-200.png")

    def test_chinese_operating_pages_do_not_leak_known_dynamic_english_messages(self):
        self._login()
        for path in ("/dashboard?lang=zh-CN", "/jobs?lang=zh-CN"):
            with self.subTest(path=path):
                self.page.goto(f"{self.base_url}{path}", wait_until="networkidle")
                page_text = self.page.locator("main").inner_text()
                self.assertNotIn("Fix Configuration", page_text)
                self.assertNotIn("Apply gate blocker", page_text)
                self.assertNotIn("Failed to load configuration", page_text)
                self.assertNotIn("jobs.blocker.", page_text)

    def test_connectors_page_renders_multi_provider_connection_controls(self):
        self._login()
        self.page.goto(
            f"{self.base_url}/data-sources/connectors",
            wait_until="networkidle",
        )
        self.assertTrue(
            self.page.get_by_role("heading", name="Connectors", level=1).is_visible()
        )
        self.assertIn(
            "ORGANIZATION SCOPE",
            self.page.locator("main").inner_text().upper(),
        )
        self.assertTrue(self.page.locator("form[data-connector-base-form]").is_visible())

        option_text = self.page.locator("#source_provider option").all_inner_texts()
        self.assertTrue(any("WeCom" in item for item in option_text))
        self.assertTrue(any("DingTalk" in item for item in option_text))
        self.assertTrue(any("Feishu" in item for item in option_text))
        self.assertTrue(self.page.locator("#group-corpid").is_visible())
        self.assertTrue(self.page.locator("#group-corpsecret").is_visible())
        self.assertEqual(self.page.locator("#group-webhook_url").count(), 0)

        self.page.select_option("#source_provider", "dingtalk")
        self.page.wait_for_function(
            "() => document.querySelector('[data-config-provider-current]')?.textContent.includes('DingTalk')"
        )
        self.assertIn(
            "AppKey / Client ID", self.page.locator("#group-corpid label").inner_text()
        )
        self.assertEqual(
            self.page.locator("#corpid").get_attribute("placeholder"), "Enter AppKey"
        )
        self.assertIn(
            "The DingTalk application key or client ID.",
            self.page.locator("#group-corpid").inner_text(),
        )
        self.assertTrue(
            self.page.get_by_role(
                "button", name="Save Connection Settings"
            ).is_visible()
        )
        self.assertTrue(
            self.page.get_by_role(
                "button", name="Test Saved Connections"
            ).is_visible()
        )
        self.assertTrue(self.page.locator("#ldap_server").is_visible())
        self.assertTrue(self.page.locator("#ldap_password").is_visible())
        self.assertTrue(
            self.page.get_by_role("link", name="Open Sync Policies").is_visible()
        )
        self._capture("connectors-page.png")

    def test_advanced_sync_redirects_to_sync_policy_authority(self):
        self._login()
        self.page.goto(f"{self.base_url}/advanced-sync", wait_until="networkidle")
        self.assertEqual(self.page.url, f"{self.base_url}/sync-policies/scope")
        self.assertTrue(
            self.page.get_by_role("heading", name="Sync Scope", level=1).is_visible()
        )
        self.assertEqual(self.page.locator("#account-creation-rules").count(), 0)
        self.assertEqual(
            self.page.locator("main form[action^='/advanced-sync']").count(), 0
        )
        self.assertNotIn("Pending Lifecycle Queue", self.page.locator("main").inner_text())

    def test_config_source_picker_moves_to_searchable_sync_scope_tree(self):
        self._login(ui_mode="advanced")
        self.page.goto(f"{self.base_url}/config", wait_until="networkidle")
        self.assertEqual(self.page.url, f"{self.base_url}/data-sources/connectors")
        self.assertEqual(
            self.page.locator("[name='source_root_unit_ids']").count(),
            0,
        )
        self.page.goto(f"{self.base_url}/sync-policies/scope", wait_until="networkidle")
        self.assertTrue(
            self.page.get_by_role("heading", name="Sync Scope", level=1).is_visible()
        )
        self.assertTrue(
            self.page.locator("[data-department-tree]").is_visible()
        )
        self.assertTrue(
            self.page.locator("[data-department-tree-search]").is_visible()
        )
        self.assertEqual(self.page.locator("#sync-scope-policy-form").count(), 1)
        self.assertEqual(self.page.locator("#sync-scope-selection-form").count(), 1)

    def test_jobs_execution_actions_remain_visually_consistent_when_blocked(self):
        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        job_repo = SyncJobRepository(manager)
        job_repo.create_job(
            "browser-jobs-active-blocker",
            trigger_type="browser_regression",
            execution_mode="dry_run",
            status="RUNNING",
            org_id="default",
        )
        try:
            self._login()
            self.page.goto(f"{self.base_url}/jobs", wait_until="networkidle")
            self.assertTrue(self.page.locator(".run-review").is_visible())
            self.assertEqual(self.page.locator("[data-high-risk-step]").count(), 5)
            self.assertTrue(self.page.locator("[data-high-risk-context]").is_visible())
            self.assertIn(
                "current execution status",
                self.page.locator(".run-review").inner_text().lower(),
            )
            dry_run_button = self.page.get_by_role(
                "button", name="Run New Dry Run", exact=True
            )
            apply_button = self.page.get_by_role(
                "button", name="Apply Blocked", exact=True
            )
            self.assertTrue(dry_run_button.is_disabled())
            self.assertTrue(apply_button.is_disabled())
            self.assertTrue(
                self.page.locator("section#dry-run").is_visible()
            )
            self.assertTrue(
                self.page.locator("section#apply").is_visible()
            )
            self._capture("jobs-page.png")
        finally:
            job_repo.update_job(
                "browser-jobs-active-blocker",
                status="COMPLETED",
                ended=True,
            )

    def test_z_job_detail_prioritizes_run_review_summary(self):
        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        job_repo = SyncJobRepository(manager)
        job_repo.create_job(
            "browser-job-detail-001",
            trigger_type="browser_regression",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="default",
        )
        job_repo.update_job(
            "browser-job-detail-001",
            planned_operation_count=7,
            executed_operation_count=0,
            error_count=0,
            summary={
                "planned_operation_count": 7,
                "high_risk_operation_count": 2,
                "conflict_count": 1,
            },
        )

        self._login(ui_mode="advanced")
        self.page.goto(
            f"{self.base_url}/jobs/browser-job-detail-001", wait_until="networkidle"
        )
        self.assertTrue(self.page.locator(".job-review-hero").is_visible())
        hero_text = self.page.locator(".job-review-hero").inner_text()
        self.assertIn("high risk", hero_text.lower())
        self.assertIn("conflicts", hero_text.lower())
        self.assertIn("browser-job-detail-001", hero_text)
        self._capture("job-detail-page.png")

    def test_z_irreversible_dialog_shows_context_requires_name_and_restores_focus(self):
        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        OrganizationRepository(manager).upsert_organization(
            org_id="browser-delete",
            name="Browser Delete Org",
            config_path=str(ARTIFACT_DIR / "browser-delete.ini"),
            description="Browser confirmation fixture",
            is_enabled=True,
        )

        self.context.close()
        self.context = self.browser.new_context(
            viewport={"width": 390, "height": 900}, locale="en-US"
        )
        self.page = self.context.new_page()
        self._login(ui_mode="advanced")
        self.page.goto(f"{self.base_url}/system-management/organizations", wait_until="networkidle")

        delete_form = self.page.locator(
            "form[action='/system-management/organizations/browser-delete/delete']"
        )
        delete_button = delete_form.get_by_role("button", name="Delete Organization")
        self.assertEqual(delete_button.count(), 1)
        delete_button.click()

        dialog = self.page.locator("[data-confirm-dialog]")
        self.assertTrue(dialog.is_visible())
        details = dialog.locator("[data-confirm-details]").inner_text()
        self.assertIn("Browser Delete Org", details)
        self.assertIn("Environment", details)
        self.assertIn("Snapshot Version", details)
        self.assertIn("Impact Count", details)
        self.assertIn("Reversible", details)
        approve = dialog.locator("[data-confirm-approve]")
        self.assertTrue(approve.is_disabled())
        self.assertIn("confirm-dialog-input-help", approve.get_attribute("aria-describedby"))
        self.assertEqual(approve.inner_text().strip(), "Delete Organization")

        panel_box = dialog.locator(".confirm-dialog__panel").bounding_box()
        self.assertIsNotNone(panel_box)
        self.assertGreaterEqual(float(panel_box["x"]), 0)
        self.assertGreaterEqual(float(panel_box["y"]), 0)
        self.assertLessEqual(float(panel_box["x"] + panel_box["width"]), 390)
        self.assertLessEqual(float(panel_box["y"] + panel_box["height"]), 900)

        confirmation_input = dialog.locator("[data-confirm-input]")
        confirmation_input.fill("wrong")
        self.assertTrue(approve.is_disabled())
        confirmation_input.fill("Browser Delete Org")
        self.assertTrue(approve.is_enabled())
        self._capture("high-risk-confirm-390.png")

        self.page.keyboard.press("Escape")
        self.assertFalse(dialog.is_visible())
        self.assertTrue(
            delete_button.evaluate("element => document.activeElement === element")
        )

    def test_z_conflict_queue_and_decision_wizard_use_decision_surfaces(self):
        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        job_repo = SyncJobRepository(manager)
        conflict_repo = SyncConflictRepository(manager)
        job_repo.create_job(
            "browser-conflict-001",
            trigger_type="browser_regression",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="default",
        )
        conflict_id = conflict_repo.add_conflict(
            job_id="browser-conflict-001",
            conflict_type="multiple_ad_candidates",
            source_id="browser-alice",
            target_key="identity_binding",
            message="browser-alice matched multiple AD candidates",
            resolution_hint="create manual binding",
            details={
                "userid": "browser-alice",
                "candidates": [
                    {"rule": "existing_ad_userid", "username": "browser-alice"},
                    {
                        "rule": "existing_ad_email_localpart",
                        "username": "browser.alice",
                    },
                ],
            },
        )

        self._login(ui_mode="advanced")
        self.page.goto(
            f"{self.base_url}/conflicts?job_id=browser-conflict-001",
            wait_until="networkidle",
        )
        self.assertTrue(self.page.locator(".conflict-command-center").is_visible())
        self.assertTrue(self.page.locator(".bulk-action-bar").is_visible())
        self.assertGreaterEqual(self.page.locator(".conflict-card").count(), 1)
        self.assertIn(
            "Resolve identity ambiguity before Apply.",
            self.page.locator("body").inner_text(),
        )

        bulk_button = self.page.locator("[data-selection-requires-items]")
        self.assertFalse(bulk_button.is_enabled())
        self.page.locator("input[name='conflict_ids']").check()
        self.assertTrue(bulk_button.is_enabled())
        self.page.locator("select[name='action']").select_option("dismiss")
        bulk_button.click()
        self.assertTrue(self.page.locator("[data-confirm-dialog]").is_visible())
        confirmation_details = self.page.locator("[data-confirm-details]").inner_text()
        self.assertIn("Selected Conflicts\n1", confirmation_details)
        self.assertIn("Requested Action\nDismiss", confirmation_details)
        self.page.keyboard.press("Escape")
        self.assertFalse(self.page.locator("[data-confirm-dialog]").is_visible())
        self.assertTrue(
            bulk_button.evaluate("element => document.activeElement === element")
        )
        self._capture("conflict-queue-page.png")

        self.page.goto(
            f"{self.base_url}/conflicts?job_id=browser-conflict-001&lang=zh-CN",
            wait_until="networkidle",
        )
        localized_conflict = self.page.locator(".conflict-card").inner_text()
        self.assertNotIn(
            "browser-alice matched multiple AD candidates", localized_conflict
        )
        self.assertNotIn("create manual binding", localized_conflict.lower())
        self.assertIn("多个 AD 候选账号", localized_conflict)
        self.assertIn("创建手工绑定", localized_conflict)

        self.page.goto(
            f"{self.base_url}/conflicts/{conflict_id}/decision-guide?lang=zh-CN",
            wait_until="networkidle",
        )
        localized_guide = self.page.locator(".decision-wizard").inner_text()
        self.assertNotIn("Prefer browser-alice because", localized_guide)
        self.assertNotIn("failed to get access token", localized_guide.lower())
        self.assertIn("创建手工绑定", localized_guide)

        self.page.goto(
            f"{self.base_url}/conflicts/{conflict_id}/decision-guide?lang=en",
            wait_until="networkidle",
        )
        self.assertTrue(self.page.locator(".decision-wizard").is_visible())
        self.assertEqual(self.page.locator(".decision-step").count(), 5)
        self.assertTrue(self.page.locator(".outcome-card").first.is_visible())
        outcome_badge_layouts = self.page.locator(
            ".outcome-card .section-header > .badge"
        ).evaluate_all(
            """elements => elements.map(element => ({
                width: element.getBoundingClientRect().width,
                height: element.getBoundingClientRect().height,
                whiteSpace: getComputedStyle(element).whiteSpace
            }))"""
        )
        self.assertEqual(len(outcome_badge_layouts), 2)
        for layout in outcome_badge_layouts:
            self.assertEqual(layout["whiteSpace"], "nowrap")
            self.assertGreater(layout["width"], layout["height"])
        self.assertIn(
            "If You Bind This Account", self.page.locator("body").inner_text()
        )
        self._capture("conflict-decision-page.png")

    def test_z_apply_confirmation_shows_real_impact_and_explicit_action(self):
        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        config_repo = OrganizationConfigRepository(manager)
        configured_values = {
            "source_provider": "wecom",
            "corpid": "browser-corp",
            "agentid": "10001",
            "corpsecret": "browser-secret",
            "webhook_url": "",
            "ldap_server": "dc01.example.local",
            "ldap_domain": "example.local",
            "ldap_username": "EXAMPLE\\administrator",
            "ldap_password": "Password123!",
            "ldap_use_ssl": True,
            "ldap_port": 636,
            "ldap_validate_cert": False,
            "ldap_ca_cert_path": "",
            "default_password": "ChangeMe123!",
            "force_change_password": True,
            "password_complexity": "strong",
        }
        config_repo.save_config("default", configured_values, config_path="config.ini")
        created = create_eligible_execution_plan(
            manager,
            job_id="browser-apply-ready-001",
            environment_label="Local environment",
            planned_operation_count=126,
            high_risk_operation_count=3,
            approved=True,
        )
        SyncJobRepository(manager).update_job(
            "browser-apply-ready-001",
            started_at=datetime.now(timezone.utc).isoformat(
                timespec="microseconds"
            ),
        )

        try:
            self.context.close()
            self.context = self.browser.new_context(
                viewport={"width": 390, "height": 900}, locale="en-US"
            )
            self.page = self.context.new_page()
            self._login(ui_mode="advanced")
            self.page.goto(f"{self.base_url}/jobs", wait_until="networkidle")

            apply_button = self.page.get_by_role(
                "button", name="Apply 126 Changes", exact=True
            )
            self.assertEqual(apply_button.count(), 1)
            self.assertTrue(apply_button.is_enabled())
            apply_button.click()

            dialog = self.page.locator("[data-confirm-dialog]")
            self.assertTrue(dialog.is_visible())
            details = dialog.locator("[data-confirm-details]").inner_text()
            for expected in (
                "Default Organization",
                "Local environment",
                f"Snapshot Version\n#{created['snapshot_id']}",
                "Impact Count\n126",
                "browser-apply-ready-001",
                "Planned Changes\n126",
                "High-Risk Changes\n3",
                "Reversible\nNo",
            ):
                self.assertIn(expected, details)
            self.assertEqual(
                dialog.locator("[data-confirm-approve]").inner_text().strip(),
                "Apply 126 Changes",
            )
            panel_box = dialog.locator(".confirm-dialog__panel").bounding_box()
            self.assertLessEqual(float(panel_box["x"] + panel_box["width"]), 390)
            self.assertLessEqual(float(panel_box["y"] + panel_box["height"]), 900)
            self._capture("apply-confirm-390.png")
            self.page.keyboard.press("Escape")
            self.assertFalse(dialog.is_visible())
            self.assertTrue(
                apply_button.evaluate("element => document.activeElement === element")
            )
        finally:
            config_repo.save_config(
                "default",
                {
                    **configured_values,
                    "corpid": "",
                    "corpsecret": "",
                    "ldap_server": "",
                    "ldap_domain": "",
                    "ldap_username": "",
                    "ldap_password": "",
                    "default_password": "",
                },
                config_path="config.ini",
            )

    def test_z_lifecycle_workbench_uses_four_lane_board(self):
        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        now = datetime.now(timezone.utc)
        offboarding_repo = OffboardingQueueRepository(manager)
        lifecycle_repo = UserLifecycleQueueRepository(manager)
        replay_repo = SyncReplayRequestRepository(manager)
        lifecycle_repo.upsert_pending(
            lifecycle_type="future_onboarding",
            connector_id="default",
            source_user_id="browser-newhire",
            ad_username="browser.newhire",
            effective_at=(now + timedelta(days=3)).isoformat(timespec="seconds"),
            org_id="default",
            reason="future start date",
            sponsor_userid="sponsor.browser",
            manager_userids=["manager.browser"],
        )
        lifecycle_repo.upsert_pending(
            lifecycle_type="contractor_expiry",
            connector_id="default",
            source_user_id="browser-contractor",
            ad_username="browser.contractor",
            effective_at=(now - timedelta(hours=3)).isoformat(timespec="seconds"),
            org_id="default",
            reason="contract expired",
            employment_type="contractor",
            sponsor_userid="sponsor.browser",
        )
        offboarding_repo.upsert_pending(
            connector_id="default",
            source_user_id="browser-offboard",
            ad_username="browser.offboard",
            due_at=(now - timedelta(hours=2)).isoformat(timespec="seconds"),
            org_id="default",
            reason="source account missing",
            manager_userids=["manager.browser"],
        )
        replay_repo.enqueue_request(
            request_type="browser_replay",
            execution_mode="apply",
            requested_by="browser",
            target_scope="source_user",
            target_id="browser-offboard",
            trigger_reason="browser_regression",
            org_id="default",
        )

        self._login(ui_mode="advanced")
        self.page.goto(f"{self.base_url}/lifecycle", wait_until="networkidle")
        self.assertTrue(self.page.locator(".lifecycle-command-center").is_visible())
        self.assertEqual(self.page.locator(".lifecycle-lane").count(), 4)
        self.assertIn(
            "daily operations board", self.page.locator("body").inner_text().lower()
        )
        self.assertIn("browser-contractor", self.page.locator("body").inner_text())
        self._capture("lifecycle-workbench-page.png")

    def test_z_long_dry_run_id_truncates_copies_and_announces(self):
        long_job_id = "dry-run-" + ("identity-scope-" * 8) + "001"
        long_apply_job_id = "apply-" + ("identity-scope-" * 8) + "001"
        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        job_repo = SyncJobRepository(manager)
        job_repo.create_job(
            long_job_id,
            trigger_type="browser_regression",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="default",
        )
        job_repo.create_job(
            long_apply_job_id,
            trigger_type="browser_regression",
            execution_mode="apply",
            status="COMPLETED",
            org_id="default",
        )

        self.context.close()
        self.context = self.browser.new_context(
            viewport={"width": 390, "height": 900}, locale="en-US"
        )
        self.page = self.context.new_page()
        self._login(ui_mode="advanced")
        self.page.goto(f"{self.base_url}/dashboard", wait_until="networkidle")

        identifier = self.page.locator(".control-tower .identifier").first
        self.assertTrue(identifier.is_visible())
        value = identifier.locator(".identifier__value")
        self.assertEqual(value.get_attribute("title"), long_job_id)
        self.assertLessEqual(
            float(identifier.bounding_box()["width"]),
            float(self.page.locator(".control-metric").first.bounding_box()["width"]),
        )
        identifier_link = identifier.locator(".identifier__link")
        self.assertGreaterEqual(
            float(identifier_link.bounding_box()["width"]),
            44,
        )
        identifier.locator("[data-copy-value]").click()
        self.page.wait_for_function(
            "() => document.querySelector('[data-copy-status]')?.textContent.includes('Dry Run ID copied')"
        )
        self.assertIn(
            "Dry Run ID copied",
            self.page.locator("[data-copy-status]").text_content(),
        )

        self.page.goto(f"{self.base_url}/jobs", wait_until="networkidle")
        job_links = self.page.locator(".identifier__link:visible")
        self.assertGreater(job_links.count(), 0)
        for link_index in range(job_links.count()):
            with self.subTest(surface="jobs", link_index=link_index):
                self.assertGreaterEqual(
                    float(job_links.nth(link_index).bounding_box()["width"]),
                    44,
                )

    def test_z_responsive_evidence_covers_critical_operating_pages(self):
        self._login(ui_mode="advanced")
        viewports = (390, 768, 1024, 1366, 1440)
        pages = {
            "dashboard": ("/dashboard?lang=zh-CN", ".control-tower"),
            "jobs": ("/jobs?lang=zh-CN", ".execution-flow"),
            "audit": ("/audit?lang=zh-CN", ".table-shell"),
            "organizations": ("/organizations?lang=zh-CN", ".table-shell"),
            "connectors": ("/data-sources/connectors?lang=zh-CN", ".page-header"),
            "source-directory": (
                "/data-sources/source-directory?lang=zh-CN",
                ".source-metric-grid",
            ),
            "snapshot-history": (
                "/data-sources/snapshots?lang=zh-CN",
                ".table-scroll",
            ),
            "binding-reconciliation": (
                "/identity-governance/binding-reconciliation?lang=zh-CN",
                ".page-header",
            ),
            "identity-matching": (
                "/identity-governance/identity-matching?lang=zh-CN",
                ".page-header",
            ),
            "conflict-queue": (
                "/identity-governance/conflicts?lang=zh-CN",
                ".conflict-command-center",
            ),
            "manual-overrides": (
                "/identity-governance/manual-overrides?lang=zh-CN",
                "[data-manual-bindings-table]",
            ),
            "exception-rules": (
                "/identity-governance/exception-rules?lang=zh-CN",
                ".table-shell",
            ),
            "sync-scope": (
                "/sync-policies/scope?lang=zh-CN",
                'select[name="scope_type"]',
            ),
            "sync-account-naming": (
                "/sync-policies/account-naming?lang=zh-CN",
                ".policy-status-grid",
            ),
            "sync-attribute-mappings": (
                "/sync-policies/attribute-mappings?lang=zh-CN",
                ".policy-status-grid",
            ),
            "sync-department-ou-routing": (
                "/sync-policies/department-ou-routing?lang=zh-CN",
                ".policy-status-grid",
            ),
            "sync-group-rules": (
                "/sync-policies/group-rules?lang=zh-CN",
                ".policy-status-grid",
            ),
            "sync-lifecycle-policy": (
                "/sync-policies/lifecycle?lang=zh-CN",
                ".policy-status-grid",
            ),
            "sync-security-policy": (
                "/sync-policies/security?lang=zh-CN",
                ".policy-status-grid",
            ),
            "mappings": ("/mappings?lang=zh-CN", ".table-shell"),
            "identity-job": (
                "/jobs/browser-identity-apply?lang=zh-CN",
                ".identity-resolution-results",
            ),
        }
        records: list[dict[str, object]] = []
        console_errors: list[dict[str, str]] = []
        failed_requests: list[dict[str, str]] = []
        self.page.on(
            "console",
            lambda message: console_errors.append(
                {"url": self.page.url, "message": message.text}
            )
            if message.type == "error"
            else None,
        )
        self.page.on(
            "requestfailed",
            lambda request: failed_requests.append(
                {"url": request.url, "failure": str(request.failure or "")}
            ),
        )

        for width in viewports:
            self.page.set_viewport_size({"width": width, "height": 900})
            for page_name, (path, first_selector) in pages.items():
                with self.subTest(width=width, page=page_name):
                    self.page.goto(f"{self.base_url}{path}", wait_until="networkidle")
                    metrics = self.page.evaluate(
                        """firstSelector => {
                            const root = document.documentElement;
                            const first = document.querySelector(firstSelector)?.getBoundingClientRect();
                            const sticky = document.querySelector('.sticky-submit-bar');
                            const stickyRect = sticky?.getBoundingClientRect();
                            const tables = Array.from(document.querySelectorAll('.table-shell')).map(table => ({
                                clientWidth: table.clientWidth,
                                scrollWidth: table.scrollWidth,
                                overflowX: getComputedStyle(table).overflowX,
                                tabindex: table.getAttribute('tabindex'),
                            }));
                            const zeroTextContainers = Array.from(document.querySelectorAll('main *'))
                                .filter(element => {
                                    const style = getComputedStyle(element);
                                    const rect = element.getBoundingClientRect();
                                    return !['SVG', 'PATH'].includes(element.tagName)
                                        && style.display !== 'none'
                                        && style.visibility !== 'hidden'
                                        && rect.height > 0
                                        && rect.width === 0
                                        && (element.innerText || '').trim();
                                })
                                .slice(0, 10)
                                .map(element => `${element.tagName}.${element.className}`);
                            return {
                                viewport: {width: innerWidth, height: innerHeight},
                                clientWidth: root.clientWidth,
                                scrollWidth: root.scrollWidth,
                                pageHeight: root.scrollHeight,
                                firstVisible: first ? {x: first.x, y: first.y, width: first.width, height: first.height} : null,
                                tables,
                                sticky: stickyRect ? {
                                    x: stickyRect.x,
                                    y: stickyRect.y,
                                    width: stickyRect.width,
                                    height: stickyRect.height,
                                    position: getComputedStyle(sticky).position,
                                } : null,
                                zeroTextContainers,
                            };
                        }""",
                        first_selector,
                    )
                    self.assertLessEqual(
                        metrics["scrollWidth"], metrics["clientWidth"] + 1, metrics
                    )
                    self.assertEqual(metrics["zeroTextContainers"], [], metrics)
                    self.assertIsNotNone(metrics["firstVisible"], metrics)
                    self.assertGreater(metrics["firstVisible"]["width"], 0, metrics)
                    for table in metrics["tables"]:
                        self.assertIn(table["overflowX"], {"auto", "scroll"})
                        self.assertGreaterEqual(int(table["tabindex"] or -1), 0)
                    records.append(
                        {"page": page_name, "path": path, "width": width, **metrics}
                    )
                    screenshot_path = ARTIFACT_DIR / f"evidence-{page_name}-{width}.png"
                    self.page.screenshot(path=str(screenshot_path), full_page=True)

        login_context = self.browser.new_context(locale="en-US")
        try:
            login_page = login_context.new_page()
            for width in viewports:
                login_page.set_viewport_size({"width": width, "height": 900})
                login_page.goto(f"{self.base_url}/login?lang=zh-CN", wait_until="networkidle")
                dimensions = login_page.evaluate(
                    "() => ({clientWidth: document.documentElement.clientWidth, scrollWidth: document.documentElement.scrollWidth, pageHeight: document.documentElement.scrollHeight})"
                )
                self.assertLessEqual(dimensions["scrollWidth"], dimensions["clientWidth"] + 1)
                records.append(
                    {
                        "page": "login",
                        "path": "/login?lang=zh-CN",
                        "width": width,
                        "viewport": {"width": width, "height": 900},
                        **dimensions,
                    }
                )
                login_page.screenshot(
                    path=str(ARTIFACT_DIR / f"evidence-login-{width}.png"),
                    full_page=True,
                )
        finally:
            login_context.close()

        evidence_path = ARTIFACT_DIR / "responsive-evidence.json"
        evidence_path.write_text(
            json.dumps(
                {
                    "records": records,
                    "console_errors": console_errors,
                    "failed_requests": failed_requests,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.assertEqual(console_errors, [])
        self.assertEqual(failed_requests, [])

    def test_z_phase3_operating_pages_render_shells(self):
        self._login(ui_mode="advanced")

        self.page.goto(
            f"{self.base_url}/data-sources/data-quality", wait_until="networkidle"
        )
        self.assertTrue(self.page.locator(".quality-ops-hero").is_visible())
        self.assertEqual(
            self.page.locator('form[action="/data-sources/data-quality/run"]').count(),
            1,
        )
        self.assertIn(
            "quality operations", self.page.locator("body").inner_text().lower()
        )
        self._capture("data-quality-page.png")

        self.page.goto(f"{self.base_url}/config/releases", wait_until="networkidle")
        self.assertTrue(self.page.locator(".release-pipeline").is_visible())
        self.assertIn(
            "release pipeline", self.page.locator("body").inner_text().lower()
        )
        self._capture("config-release-page.png")

        self.page.goto(f"{self.base_url}/operations-center/notifications", wait_until="networkidle")
        self.page.locator("details.detail-toggle > summary").click()
        self.assertTrue(self.page.locator(".integration-portal-hero").is_visible())
        self.assertIn(
            "integration portal", self.page.locator("body").inner_text().lower()
        )
        self._capture("integration-center-page.png")

    def test_z_phase7_operations_and_system_matrix(self):
        self._login(ui_mode="advanced")
        pages = {
            "lifecycle": ("/operations-center/lifecycle-queue?lang=zh-CN", ".lifecycle-command-center"),
            "automation": ("/operations-center/automation?lang=zh-CN", "form[action='/operations-center/automation/policy']"),
            "notifications": ("/operations-center/notifications?lang=zh-CN", "form[action='/operations-center/notifications/policy']"),
            "audit": ("/operations-center/audit-log?lang=zh-CN", ".table-shell"),
            "organizations": ("/system-management/organizations?lang=zh-CN", ".table-shell"),
            "administrators": ("/system-management/administrators?lang=zh-CN", "form[action='/system-management/administrators']"),
            "employee-self-service": ("/system-management/employee-self-service?lang=zh-CN", "form[action='/system-management/employee-self-service']"),
            "database": ("/system-management/database?lang=zh-CN", "time[data-local-time]"),
            "branding": ("/system-management/branding?lang=zh-CN", "form[action='/system-management/branding']"),
            "deployment": ("/system-management/deployment?lang=zh-CN", "form[action='/system-management/deployment']"),
        }
        for width in (390, 1440):
            self.page.set_viewport_size({"width": width, "height": 900})
            for name, (path, selector) in pages.items():
                with self.subTest(width=width, page=name):
                    self.page.goto(f"{self.base_url}{path}", wait_until="networkidle")
                    self.assertTrue(self.page.locator(selector).first.is_visible())
                    dimensions = self.page.evaluate(
                        "() => ({clientWidth: document.documentElement.clientWidth, scrollWidth: document.documentElement.scrollWidth})"
                    )
                    self.assertLessEqual(dimensions["scrollWidth"], dimensions["clientWidth"] + 1)
                    primary = self.page.locator(
                        "main .button:not(.secondary):not(.ghost):not(.danger)"
                    )
                    self.assertEqual(primary.count(), 1, f"{name} must expose one primary CTA")
                    self._capture(f"phase7-{name}-{width}.png")

        self.page.goto(f"{self.base_url}/database?lang=en", wait_until="networkidle")
        self.assertEqual(
            self.page.url,
            f"{self.base_url}/system-management/database?lang=en",
        )
        localized_time = self.page.locator("time[data-local-time]").first
        self.assertRegex(localized_time.inner_text(), r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}, ")
        raw_timestamp = localized_time.get_attribute("title")
        self.assertTrue(raw_timestamp)
        self.assertIn("T", raw_timestamp)

        account_link = self.page.locator(".topbar-account__meta")
        account_link.focus()
        self.assertTrue(account_link.evaluate("element => document.activeElement === element"))

        self.page.route(
            "**/static/oidc-callback.js*",
            lambda route: route.fulfill(
                status=200,
                content_type="application/javascript",
                body="",
            ),
        )
        self.page.set_viewport_size({"width": 390, "height": 900})
        self.page.goto(
            f"{self.base_url}/auth/oidc/callback"
            "?code=browser-code&state=browser-state&lang=zh-CN",
            wait_until="networkidle",
        )
        self.assertTrue(
            self.page.get_by_role("heading", name="正在完成单点登录").is_visible()
        )
        callback_form = self.page.locator(
            "form[method='post'][action='/auth/oidc/callback']"
        )
        self.assertTrue(callback_form.is_visible())
        self.assertEqual(
            self.page.locator(
                "main .button:not(.secondary):not(.ghost):not(.danger)"
            ).count(),
            1,
        )
        self._assert_page_has_no_horizontal_overflow()
        complete_button = self.page.get_by_role("button", name="完成登录")
        complete_button.focus()
        self.assertTrue(
            complete_button.evaluate("element => document.activeElement === element")
        )
        self.assertEqual(
            self.page.evaluate("document.querySelector('[data-app-sidebar]') === null"),
            True,
        )
        self._capture("phase7-oidc-callback-390.png")
        self.page.unroute("**/static/oidc-callback.js*")

    def test_z_unified_admin_visual_matrix_uses_exact_target_viewports(self):
        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        source_repo = SourceDirectoryRepository(manager)
        snapshot_id = source_repo.start_refresh(
            org_id="default",
            provider_id="wecom",
            created_by="visual-matrix",
        )
        source_repo.replace_snapshot(
            snapshot_id,
            departments=[
                {
                    "source_department_id": "visual-dept",
                    "name": "Visual QA",
                    "parent_department_id": "0",
                    "path_ids": ["visual-dept"],
                    "path_names": ["Visual QA"],
                }
            ],
            users=[
                {
                    "source_user_id": "visual-user-001",
                    "display_name": "Visual Queue User",
                    "employee_id": "VQA001",
                    "department_ids": ["visual-dept"],
                    "department_names": ["Visual QA"],
                    "email": "visual.user@example.test",
                    "is_active": True,
                    "account_status": "active",
                    "raw_payload": {
                        "userid": "visual-user-001",
                        "employee_id": "VQA001",
                    },
                    "search_text": "Visual Queue User VQA001",
                }
            ],
            fields=[
                {
                    "name": "employee_id",
                    "label": "Employee ID",
                    "coverage": 1,
                    "samples": ["VQA001"],
                }
            ],
            fingerprint=f"visual-matrix-{snapshot_id}",
        )
        source_repo.save_scope_selection(
            org_id="default",
            provider_id="wecom",
            scope_type="full",
            username_strategy="employee_id",
            source_field="employee_id",
            snapshot_id=snapshot_id,
            requested_by="visual-matrix",
        )

        self._login()
        target_viewports = (
            (1920, 1080),
            (1440, 900),
            (1280, 800),
            (768, 900),
            (390, 844),
        )
        for width, height in target_viewports:
            with self.subTest(width=width, height=height):
                self.page.set_viewport_size({"width": width, "height": height})
                self.page.goto(
                    f"{self.base_url}/identity-governance/identity-matching?lang=zh-CN",
                    wait_until="networkidle",
                )
                self.assertTrue(
                    self.page.locator("[data-component='page-header']").is_visible()
                )
                self.assertTrue(
                    self.page.locator("[data-component='filter-bar']").is_visible()
                )
                self.assertTrue(
                    self.page.locator("[data-component='work-queue-table']").is_visible()
                )
                self.assertEqual(
                    self.page.locator(
                        "main [data-action-priority='primary']"
                    ).count(),
                    1,
                )
                self.assertGreaterEqual(
                    self.page.locator(
                        "[data-business-status] .badge__icon"
                    ).count(),
                    1,
                )
                self.assertGreaterEqual(
                    self.page.locator(
                        "[data-business-status] .badge__label"
                    ).count(),
                    1,
                )
                self._assert_page_has_no_horizontal_overflow()

                queue = self.page.locator("[data-table-region]").first
                queue.focus()
                self.page.keyboard.press("ArrowDown")
                self.assertEqual(
                    self.page.evaluate("document.activeElement?.tagName"),
                    "TR",
                )
                self.page.keyboard.press("Enter")
                self.assertIn(
                    self.page.evaluate("document.activeElement?.tagName"),
                    {"INPUT", "A", "BUTTON", "SUMMARY", "SELECT"},
                )
                self.page.evaluate(
                    """() => {
                        document.activeElement?.blur();
                        document.querySelectorAll('[data-keyboard-row]').forEach(
                            element => element.removeAttribute('data-keyboard-row')
                        );
                        window.scrollTo(0, 0);
                    }"""
                )

                target = (
                    ARTIFACT_DIR
                    / f"visual-identity-workbench-{width}x{height}.png"
                )
                self.page.screenshot(path=str(target), full_page=False)
                self.assertTrue(target.exists())
                self.assertGreater(target.stat().st_size, 0)

    def test_mappings_page_uses_search_selectors_instead_of_manual_ids(self):
        self._login(ui_mode="advanced")

        self.page.goto(f"{self.base_url}/mappings", wait_until="networkidle")
        self.assertTrue(
            self.page.locator("#group-binding_source_user_id .ts-wrapper").is_visible()
        )
        self.assertTrue(
            self.page.locator("#group-binding_ad_username .ts-wrapper").is_visible()
        )
        self.assertTrue(
            self.page.locator("#group-override_source_user_id .ts-wrapper").is_visible()
        )
        self.assertTrue(
            self.page.locator(
                "#group-override_primary_department_id .ts-wrapper"
            ).is_visible()
        )
        self.assertEqual(self.page.locator('input[name="source_user_id"]').count(), 0)
        self.assertIn(
            "Search and choose a source user",
            self.page.locator("#group-binding_source_user_id").inner_text(),
        )
        self.assertIn(
            "Search and choose an AD user",
            self.page.locator("#group-binding_ad_username").inner_text(),
        )
        self.assertIn(
            "Select a source user first",
            self.page.locator("#group-override_primary_department_id").inner_text(),
        )
        self._capture("mappings-page-selectors.png")


if __name__ == "__main__":
    unittest.main()
