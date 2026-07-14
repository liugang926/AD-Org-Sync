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

    def test_source_directory_page_renders_paginated_scope_workflow_without_secrets(self):
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

        self._login()
        self.page.goto(f"{self.base_url}/source-directory", wait_until="networkidle")
        self.assertTrue(self.page.get_by_role("heading", name="DingTalk Users").is_visible())
        self.assertTrue(self.page.get_by_role("button", name="Test Connection").is_visible())
        self.assertTrue(self.page.get_by_role("button", name="Refresh Directory").is_visible())
        self.assertTrue(self.page.get_by_text("Partial sync safety", exact=True).is_visible())
        self.assertTrue(self.page.get_by_role("button", name="Select Current Page").is_visible())
        self.assertTrue(self.page.get_by_role("button", name="Select All Filtered Results").is_visible())
        self.assertEqual(self.page.locator('select[name="scope_type"]').count(), 1)
        self.assertEqual(self.page.locator('select[name="source_field"]').count(), 1)
        self.page.select_option('select[name="source_field"]', "source_user_id")
        self.assertEqual(
            self.page.locator('select[name="source_field"]').input_value(),
            "source_user_id",
        )
        self.page.select_option('select[name="source_field"]', "employee_id")
        self.assertEqual(
            self.page.locator('select[name="source_field"]').input_value(),
            "employee_id",
        )
        self.assertEqual(self.page.locator("thead tr").count(), 2)
        relationship_table = self.page.locator(".identity-relationship-table")
        self.assertTrue(relationship_table.is_visible())
        self.assertEqual(relationship_table.get_attribute("tabindex"), "0")
        self.assertGreater(
            relationship_table.evaluate("element => element.scrollWidth"),
            relationship_table.evaluate("element => element.clientWidth"),
        )
        page_text = relationship_table.inner_text()
        for expected in (
            "Alice Zhang",
            "browser-alice",
            "TJ001",
            "alice.manual",
            "Manual binding overrides the field-generated candidate",
            "Temporarily unavailable",
            "browser-identity-dry",
            "browser-identity-apply",
            "Apply failed",
            "carol.disabled",
            "Binding disabled",
            "Post-Apply AD state",
        ):
            self.assertIn(expected.lower(), page_text.lower())
        self.assertNotIn("corpsecret", self.page.content().lower())
        self.assertNotIn("distinguishedname", self.page.content().lower())
        self._capture("identity-relationship-desktop-en.png")
        relationship_table.evaluate(
            "element => { element.scrollLeft = element.scrollWidth; }"
        )
        self.assertGreater(
            relationship_table.evaluate("element => element.scrollLeft"),
            0,
        )
        self._capture("identity-relationship-evidence-desktop-en.png")

        self.page.select_option('select[name="relationship_status"]', "manual")
        self.page.get_by_role("button", name="Apply Filters").click()
        self.page.wait_for_load_state("networkidle")
        self.assertIn("1 matching users", self.page.locator(".pagination-bar").inner_text())
        self.assertIn("Alice Zhang", relationship_table.inner_text())

        self.page.set_viewport_size({"width": 390, "height": 844})
        self.page.goto(
            f"{self.base_url}/source-directory?lang=zh-CN",
            wait_until="networkidle",
        )
        self._assert_page_has_no_horizontal_overflow()
        self.assertEqual(
            relationship_table.get_attribute("aria-label"),
            "身份关联及同步前后预览",
        )
        self.assertIn("暂时无法验证", self.page.locator("body").inner_text())
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
            f"{self.base_url}/source-directory?lang=en&search=no-such-browser-user",
            wait_until="networkidle",
        )
        self.assertIn("No cached users match", self.page.locator("body").inner_text())
        self._capture("identity-relationship-empty-state.png")

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
                "This page will update automatically.", exact=False
            ).is_visible()
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
        self.assertIn("Refresh Result", self.page.locator("body").inner_text())
        self.page.select_option('select[name="source_field"]', "employee_id")
        self.assertEqual(
            self.page.locator('select[name="source_field"]').input_value(),
            "employee_id",
        )
        self._capture("source-directory-refresh-complete-mapping-en.png")

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
            for path in ("/dashboard", "/config", "/audit"):
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

    def test_config_mobile_summary_fields_and_action_bar_remain_usable(self):
        self.context.close()
        self.context = self.browser.new_context(
            viewport={"width": 390, "height": 900}, locale="en-US"
        )
        self.page = self.context.new_page()
        self._login()
        self.page.goto(f"{self.base_url}/config?lang=zh-CN", wait_until="networkidle")

        metrics = self.page.evaluate(
            """() => {
                const root = document.documentElement;
                const brief = document.querySelector('.config-brief__main')?.getBoundingClientRect();
                const actionCopy = document.querySelector('.config-action-row__copy')?.getBoundingClientRect();
                const firstField = document.querySelector(
                    '#config-section-source input:not([type="hidden"]), #config-section-source select, #config-section-source textarea'
                )?.getBoundingClientRect();
                const bar = document.querySelector('.sticky-submit-bar');
                const barRect = bar?.getBoundingClientRect();
                const form = document.querySelector('form[data-config-form]');
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
                    briefWidth: brief?.width || 0,
                    actionCopyWidth: actionCopy?.width || 0,
                    firstFieldY: firstField?.y ?? Number.MAX_SAFE_INTEGER,
                    barPosition: bar ? getComputedStyle(bar).position : '',
                    barHeight: barRect?.height || 0,
                    formPaddingBottom: form ? parseFloat(getComputedStyle(form).paddingBottom || '0') : 0,
                    zeroTextContainers,
                };
            }"""
        )

        self.assertLessEqual(metrics["scrollWidth"], metrics["clientWidth"] + 1, metrics)
        self.assertGreater(metrics["briefWidth"], 200, metrics)
        self.assertGreater(metrics["actionCopyWidth"], 200, metrics)
        self.assertLess(metrics["firstFieldY"], 1350, metrics)
        self.assertEqual(metrics["barPosition"], "fixed", metrics)
        self.assertGreaterEqual(metrics["formPaddingBottom"], metrics["barHeight"], metrics)
        self.assertEqual(metrics["zeroTextContainers"], [], metrics)
        self._capture("config-page-390.png")

        self.page.set_viewport_size({"width": 1024, "height": 900})
        self.page.reload(wait_until="networkidle")
        self.assertEqual(self._style(".sticky-submit-bar", "position"), "sticky")

    def test_execution_flow_uses_six_three_two_and_one_column_layouts(self):
        self._login()
        expected_columns = {390: 1, 768: 2, 1024: 3, 1366: 6, 1440: 6}
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
        self.page.goto(f"{self.base_url}/config", wait_until="networkidle")
        self.page.evaluate(
            "document.documentElement.style.webkitTextSizeAdjust = '200%'"
        )
        self._assert_page_has_no_horizontal_overflow()
        first_field = self.page.locator(
            "#config-section-source input:not([type='hidden']), #config-section-source select, #config-section-source textarea"
        ).first
        self.assertGreater(float(first_field.bounding_box()["width"]), 0)
        self._capture("config-text-scale-200.png")

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

    def test_config_page_renders_multi_provider_schema_controls(self):
        self._login()
        self.page.goto(f"{self.base_url}/config", wait_until="networkidle")
        self.assertIn(
            "WeCom Connector Configuration", self.page.locator("body").inner_text()
        )
        self.assertIn(
            "Shared Page, Provider-Specific Fields",
            self.page.locator("body").inner_text(),
        )
        self.assertTrue(
            self.page.get_by_role(
                "button", name="Source System Provider and credentials"
            ).is_visible()
        )
        self.assertTrue(
            self.page.get_by_role(
                "button", name="Target AD LDAP and OU roots"
            ).is_visible()
        )
        self.assertTrue(self.page.locator("#config-section-source").is_visible())
        self.assertFalse(self.page.locator("#config-section-target").is_visible())
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
        self.assertIn(
            "DingTalk Source Connector", self.page.locator("body").inner_text()
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
        self.assertIn(
            "DingTalk Bot Webhook",
            self.page.locator("#group-webhook_url label").inner_text(),
        )
        self.assertTrue(self.page.get_by_text("Source Scope").first.is_visible())
        browse_source_button = self.page.get_by_role(
            "button", name="Browse Source Unit Tree"
        )
        self.assertTrue(browse_source_button.is_visible())
        self.assertTrue(self.page.get_by_role("button", name="Save Draft").is_visible())
        self.assertTrue(
            self.page.get_by_role("button", name="Preview Changes").is_visible()
        )
        self.assertTrue(self.page.get_by_role("link", name="Publish").is_visible())
        supporting_help = self.page.locator(".config-supporting-help")
        self.assertTrue(supporting_help.is_visible())
        self.assertFalse(
            self.page.get_by_role(
                "link", name="Open Account Creation Rules"
            ).is_visible()
        )
        supporting_help.locator("summary").click()
        self.assertTrue(
            self.page.get_by_role(
                "link", name="Open Account Creation Rules"
            ).is_visible()
        )
        self.assertTrue(
            self.page.get_by_role("link", name="Open Department Routing").is_visible()
        )
        browse_source_button.click()
        self.page.locator(
            "#group-source_root_unit_ids [data-config-source-browser]"
        ).wait_for(state="visible")
        self.assertTrue(
            self.page.locator(
                "#group-source_root_unit_ids [data-config-source-browser]"
            ).is_visible()
        )
        self.page.get_by_role("button", name="Target AD LDAP and OU roots").click()
        self.page.locator("#config-section-target").wait_for(state="visible")
        self.assertTrue(
            self.page.get_by_text("OU Filter And Root Mapping").first.is_visible()
        )
        select_target_button = self.page.get_by_role(
            "button", name="Select Target Root OU"
        )
        self.assertTrue(select_target_button.is_visible())
        select_target_button.click()
        self.page.locator(
            "#group-directory_root_ou_path [data-config-target-browser]"
        ).wait_for(state="visible")
        self.assertTrue(
            self.page.locator(
                "#group-directory_root_ou_path [data-config-target-browser]"
            ).is_visible()
        )
        self.assertFalse(
            self.page.locator(
                "#group-disabled_users_ou_path [data-config-target-browser]"
            ).is_visible()
        )
        self.assertTrue(
            self.page.get_by_role(
                "button", name="Select Disabled Users OU"
            ).is_visible()
        )
        self.assertTrue(
            self.page.get_by_role("button", name="Select Custom Group OU").is_visible()
        )
        self._capture("config-page.png")

    def test_advanced_sync_page_surfaces_account_creation_rules_as_first_class_section(
        self,
    ):
        self._login()
        self.page.goto(f"{self.base_url}/advanced-sync", wait_until="networkidle")
        self.assertEqual(self.page.locator("#account-creation-rules").count(), 1)
        self.assertIn(
            "Account Creation Rules And Connector Routing",
            self.page.locator("body").inner_text(),
        )
        toggle = (
            self.page.locator("summary")
            .filter(has_text="Configure Account Creation Rule")
            .first
        )
        self.assertTrue(toggle.is_visible())
        if not self.page.locator("#username_collision_policy").is_visible():
            toggle.click()
        self.page.locator("#username_collision_policy").wait_for(state="visible")
        self.assertTrue(self.page.locator("#username_collision_policy").is_visible())
        self.assertTrue(self.page.locator("#root_department_ids").is_visible())
        self.assertTrue(
            self.page.get_by_role(
                "button", name="Save Account Creation Rule"
            ).is_visible()
        )

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
        source_group.locator("[data-config-source-list] .config-tree-row").nth(
            1
        ).wait_for()
        self.assertTrue(
            source_group.locator(
                ".picker-field__surface .picker-inline-panel"
            ).is_visible()
        )
        source_group.locator('[data-source-unit-checkbox][value="8"]').check()
        self.assertEqual(
            source_group.locator('input[name="source_root_unit_ids"]').input_value(),
            "8",
        )
        self.assertIn(
            "China [8]",
            source_group.locator(
                '[data-picker-summary-for="source_root_unit_ids"]'
            ).inner_text(),
        )
        self.assertRegex(
            source_group.locator(
                '[data-picker-meta-for="source_root_unit_ids"]'
            ).inner_text(),
            r"1",
        )
        source_group.get_by_role("button", name="Close Picker").click()
        source_group.get_by_role("button", name="Browse Source Unit Tree").click()
        self.assertTrue(
            source_group.locator("[data-config-source-list] .config-tree-row")
            .nth(1)
            .is_visible()
        )

    def test_jobs_empty_state_actions_remain_visually_consistent(self):
        self._login()
        self.page.goto(f"{self.base_url}/jobs", wait_until="networkidle")
        self.assertTrue(self.page.locator(".run-review").is_visible())
        self.assertIn(
            "execution readiness and impact preview",
            self.page.locator(".run-review").inner_text().lower(),
        )
        dry_run_button = self.page.locator("button:has-text('Run Dry Run')").first
        apply_button = self.page.locator(
            "form:has(input[name='mode'][value='apply']) button[type='submit']"
        )
        self.assertTrue(apply_button.is_disabled())
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

        self._login()
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
        self._login()
        self.page.goto(f"{self.base_url}/organizations", wait_until="networkidle")

        delete_form = self.page.locator(
            "form[action='/organizations/browser-delete/delete']"
        )
        delete_button = delete_form.get_by_role("button", name="Delete Organization")
        self.assertEqual(delete_button.count(), 1)
        delete_button.click()

        dialog = self.page.locator("[data-confirm-dialog]")
        self.assertTrue(dialog.is_visible())
        details = dialog.locator("[data-confirm-details]").inner_text()
        self.assertIn("Browser Delete Org", details)
        self.assertIn("Environment", details)
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

        self._login()
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
        job_repo = SyncJobRepository(manager)
        job_repo.create_job(
            "browser-apply-ready-001",
            trigger_type="browser_regression",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="default",
        )
        job_repo.update_job(
            "browser-apply-ready-001",
            planned_operation_count=126,
            error_count=0,
            summary={
                "planned_operation_count": 126,
                "high_risk_operation_count": 3,
                "conflict_count": 0,
                "review_required": False,
            },
        )

        try:
            self.context.close()
            self.context = self.browser.new_context(
                viewport={"width": 390, "height": 900}, locale="en-US"
            )
            self.page = self.context.new_page()
            self._login()
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

        self._login()
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

        self.context.close()
        self.context = self.browser.new_context(
            viewport={"width": 390, "height": 900}, locale="en-US"
        )
        self.page = self.context.new_page()
        self._login()
        self.page.goto(f"{self.base_url}/config", wait_until="networkidle")

        identifier = self.page.locator(".config-brief .identifier")
        self.assertTrue(identifier.is_visible())
        value = identifier.locator(".identifier__value")
        self.assertEqual(value.get_attribute("title"), long_job_id)
        self.assertLessEqual(
            float(identifier.bounding_box()["width"]),
            float(self.page.locator(".config-brief__status").bounding_box()["width"]),
        )
        identifier.locator("[data-copy-value]").click()
        self.page.wait_for_function(
            "() => document.querySelector('[data-copy-status]')?.textContent.includes('Dry Run ID copied')"
        )
        self.assertIn(
            "Dry Run ID copied",
            self.page.locator("[data-copy-status]").text_content(),
        )

    def test_z_responsive_evidence_covers_critical_operating_pages(self):
        self._login()
        viewports = (390, 768, 1024, 1366, 1440)
        pages = {
            "dashboard": ("/dashboard?lang=zh-CN", ".control-tower"),
            "config": (
                "/config?lang=zh-CN",
                "#config-section-source input:not([type='hidden']), #config-section-source select, #config-section-source textarea",
            ),
            "jobs": ("/jobs?lang=zh-CN", ".execution-flow"),
            "audit": ("/audit?lang=zh-CN", ".table-shell"),
            "organizations": ("/organizations?lang=zh-CN", ".table-shell"),
            "connectors": ("/advanced-sync?lang=zh-CN", ".page-header"),
            "source-directory": (
                "/source-directory?lang=zh-CN",
                ".identity-relationship-table",
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
        self._login()

        self.page.goto(f"{self.base_url}/data-quality", wait_until="networkidle")
        self.assertTrue(self.page.locator(".quality-ops-hero").is_visible())
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

        self.page.goto(f"{self.base_url}/integrations", wait_until="networkidle")
        self.assertTrue(self.page.locator(".integration-portal-hero").is_visible())
        self.assertIn(
            "integration portal", self.page.locator("body").inner_text().lower()
        )
        self._capture("integration-center-page.png")

    def test_mappings_page_uses_search_selectors_instead_of_manual_ids(self):
        self._login()

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
