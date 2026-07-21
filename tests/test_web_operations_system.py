import re

from fastapi.testclient import TestClient

from sync_app.web.navigation import CANONICAL_ROUTE_PATHS, PHASE7_LEGACY_GET_REDIRECTS
from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class WebOperationsSystemTests(WebAuthzBaseTestCase):
    @staticmethod
    def _csrf(body: str) -> str:
        match = re.search(r'name="csrf_token" value="([^"]+)"', body)
        if not match:
            raise AssertionError("CSRF token was not rendered")
        return match.group(1)

    def test_system_management_pages_have_one_primary_task_and_scoped_saves(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"

        employee_path = CANONICAL_ROUTE_PATHS["employee-self-service"]
        employee_page = self._route(employee_path, "GET")(self._request(employee_path))
        employee_body = self._text(employee_page)
        self.assertIn("Employee Self-Service", employee_body)
        self.assertIn("Organization Scope", employee_body)
        self.assertEqual(employee_body.count('class="button"'), 1)
        employee_save = self._route(employee_path, "POST")(
            self._request(employee_path, "POST"),
            csrf_token=self._csrf(employee_body),
            sspr_enabled="1",
            sspr_dingtalk_corp_id="ding-test-org",
            sspr_min_password_length=14,
            sspr_unlock_account_default="1",
            sspr_verification_session_ttl_seconds=900,
        )
        self.assertEqual(employee_save.status_code, 303)
        self.assertEqual(employee_save.headers["location"], employee_path)
        self.assertTrue(self.app.state.settings_repo.get_bool("sspr_enabled", False, org_id="default"))

        branding_path = CANONICAL_ROUTE_PATHS["branding"]
        branding_page = self._route(branding_path, "GET")(self._request(branding_path))
        branding_body = self._text(branding_page)
        self.assertIn("Global Scope", branding_body)
        branding_save = self._route(branding_path, "POST")(
            self._request(branding_path, "POST"),
            csrf_token=self._csrf(branding_body),
            brand_display_name="Directory Control",
            brand_mark_text="DC",
            brand_attribution="IT Operations",
        )
        self.assertEqual(branding_save.status_code, 303)
        self.assertEqual(self.app.state.settings_repo.get_value("brand_display_name", ""), "Directory Control")

        deployment_path = CANONICAL_ROUTE_PATHS["deployment"]
        deployment_page = self._route(deployment_path, "GET")(self._request(deployment_path))
        deployment_body = self._text(deployment_page)
        self.assertIn("Active Process", deployment_body)
        self.assertIn("Global Scope", deployment_body)
        self.assertIn("Environment Classification", deployment_body)
        deployment_save = self._route(deployment_path, "POST")(
            self._request(deployment_path, "POST"),
            csrf_token=self._csrf(deployment_body),
            environment_label="staging",
            web_bind_host="127.0.0.1",
            web_bind_port=8123,
            web_public_base_url="https://sync.example.test",
            web_session_cookie_secure_mode="always",
            web_trust_proxy_headers="1",
            web_forwarded_allow_ips="127.0.0.1",
        )
        self.assertEqual(deployment_save.status_code, 303)
        self.assertEqual(self.app.state.settings_repo.get_int("web_bind_port", 0), 8123)
        self.assertEqual(self.app.state.settings_repo.get_value("environment_label", ""), "staging")
        self.assertEqual(self.app.state.environment_label, "staging")
        actions = [item.action_type for item in self.app.state.audit_repo.list_recent_logs(limit=20)]
        self.assertIn("system_management.employee_self_service.update", actions)
        self.assertIn("system_management.branding.update", actions)
        self.assertIn("system_management.deployment.update", actions)

    def test_operations_pages_preserve_settings_owned_by_the_other_page(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"

        notification_path = CANONICAL_ROUTE_PATHS["integrations"]
        notification_page = self._route(notification_path, "GET")(self._request(notification_path))
        notification_body = self._text(notification_page)
        self.assertIn("Notification Policy", notification_body)
        self.assertEqual(notification_body.count('class="button"'), 1)
        notification_save = self._route(notification_path + "/policy", "POST")(
            self._request(notification_path + "/policy", "POST"),
            csrf_token=self._csrf(notification_body),
            ops_notify_dry_run_failure_enabled="1",
            ops_notify_conflict_backlog_enabled="1",
            ops_notify_conflict_backlog_threshold=7,
            ops_notify_review_pending_enabled="1",
            ops_notify_rule_governance_enabled=None,
        )
        self.assertEqual(notification_save.status_code, 303)

        automation_path = CANONICAL_ROUTE_PATHS["automation-center"]
        automation_page = self._route(automation_path, "GET")(self._request(automation_path))
        automation_body = self._text(automation_page)
        self.assertNotIn("ops_notify_dry_run_failure_enabled", automation_body)
        automation_save = self._route(automation_path + "/policy", "POST")(
            self._request(automation_path + "/policy", "POST"),
            csrf_token=self._csrf(automation_body),
            schedule_time="04:15",
            retry_interval=45,
            max_retries=4,
            schedule_execution_mode="dry_run",
            ops_scheduled_apply_gate_enabled="1",
            ops_scheduled_apply_max_dry_run_age_hours=18,
            ops_scheduled_apply_requires_zero_conflicts="1",
            ops_scheduled_apply_requires_review_approval="1",
        )
        self.assertEqual(automation_save.status_code, 303)
        self.assertTrue(
            self.app.state.settings_repo.get_bool(
                "ops_notify_dry_run_failure_enabled", False, org_id="default"
            )
        )
        editable = self.app.state.org_config_repo.get_editable_config("default", config_path=str(self.config_path))
        self.assertEqual(editable["schedule_time"], "04:15")
        self.assertEqual(editable["retry_interval"], 45)
        self.assertEqual(editable["max_retries"], 4)
        actions = [item.action_type for item in self.app.state.audit_repo.list_recent_logs(limit=20)]
        self.assertIn("operations_center.notifications.update", actions)
        self.assertIn("operations_center.automation.update", actions)

    def test_canonical_setting_writes_require_capability_and_valid_csrf(self):
        employee_path = CANONICAL_ROUTE_PATHS["employee-self-service"]
        branding_path = CANONICAL_ROUTE_PATHS["branding"]
        deployment_path = CANONICAL_ROUTE_PATHS["deployment"]
        automation_path = CANONICAL_ROUTE_PATHS["automation-center"] + "/policy"
        notification_path = CANONICAL_ROUTE_PATHS["integrations"] + "/policy"
        cases = (
            (
                employee_path,
                {
                    "sspr_enabled": "1",
                    "sspr_dingtalk_corp_id": "blocked-corp",
                    "sspr_min_password_length": 14,
                    "sspr_unlock_account_default": "1",
                    "sspr_verification_session_ttl_seconds": 900,
                },
            ),
            (
                branding_path,
                {
                    "brand_display_name": "Blocked Brand",
                    "brand_mark_text": "BB",
                    "brand_attribution": "Blocked",
                },
            ),
            (
                deployment_path,
                {
                    "web_bind_host": "127.0.0.1",
                    "web_bind_port": 8999,
                    "web_public_base_url": "https://blocked.example.test",
                    "web_session_cookie_secure_mode": "always",
                    "web_trust_proxy_headers": "1",
                    "web_forwarded_allow_ips": "127.0.0.1",
                },
            ),
            (
                automation_path,
                {
                    "schedule_time": "05:30",
                    "retry_interval": 15,
                    "max_retries": 2,
                    "schedule_execution_mode": "dry_run",
                    "ops_scheduled_apply_gate_enabled": "1",
                    "ops_scheduled_apply_max_dry_run_age_hours": 12,
                    "ops_scheduled_apply_requires_zero_conflicts": "1",
                    "ops_scheduled_apply_requires_review_approval": "1",
                },
            ),
            (
                notification_path,
                {
                    "ops_notify_dry_run_failure_enabled": "1",
                    "ops_notify_conflict_backlog_enabled": "1",
                    "ops_notify_conflict_backlog_threshold": 9,
                    "ops_notify_review_pending_enabled": "1",
                    "ops_notify_rule_governance_enabled": "1",
                    "notification_webhook_url": "",
                    "clear_notification_webhook_url": None,
                },
            ),
        )

        self._login("operator1")
        for path, payload in cases:
            with self.subTest(role="operator", path=path):
                response = self._route(path, "POST")(
                    self._request(path, "POST"),
                    csrf_token="not-authorized",
                    **payload,
                )
                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers["location"], "/dashboard")

        self._login("superadmin")
        before_logs = len(self.app.state.audit_repo.list_recent_logs(limit=100))
        for path, payload in cases:
            with self.subTest(role="super_admin", path=path):
                response = self._route(path, "POST")(
                    self._request(path, "POST"),
                    csrf_token="invalid-csrf",
                    **payload,
                )
                self.assertEqual(response.status_code, 303)

        self.assertEqual(
            len(self.app.state.audit_repo.list_recent_logs(limit=100)),
            before_logs,
        )
        self.assertFalse(
            self.app.state.settings_repo.get_bool("sspr_enabled", False, org_id="default")
        )
        self.assertNotEqual(
            self.app.state.settings_repo.get_value("brand_display_name", ""),
            "Blocked Brand",
        )
        self.assertNotEqual(
            self.app.state.settings_repo.get_int("web_bind_port", 0),
            8999,
        )
        editable = self.app.state.org_config_repo.get_editable_config(
            "default",
            config_path=str(self.config_path),
        )
        self.assertNotEqual(editable["schedule_time"], "05:30")
        self.assertFalse(
            self.app.state.settings_repo.get_bool(
                "ops_notify_dry_run_failure_enabled",
                False,
                org_id="default",
            )
        )

    def test_legacy_get_urls_redirect_permanently_and_preserve_query(self):
        with TestClient(self.app) as client:
            login_page = client.get("/login")
            csrf_token = self._csrf(login_page.text)
            login = client.post(
                "/login",
                data={"csrf_token": csrf_token, "username": "superadmin", "password": "Admin123!"},
                follow_redirects=False,
            )
            self.assertEqual(login.status_code, 303)
            for legacy_path, canonical_path in PHASE7_LEGACY_GET_REDIRECTS.items():
                with self.subTest(legacy_path=legacy_path):
                    response = client.get(f"{legacy_path}?lang=zh-CN", follow_redirects=False)
                    self.assertEqual(response.status_code, 308)
                    self.assertEqual(response.headers["location"], f"{canonical_path}?lang=zh-CN")

    def test_organization_export_get_does_not_write_an_audit_record(self):
        self._login("superadmin")
        export_path = CANONICAL_ROUTE_PATHS["organizations"] + "/{org_id}/export"
        before = len(self.app.state.audit_repo.list_recent_logs(limit=100))
        response = self._route(export_path, "GET")(
            self._request(CANONICAL_ROUTE_PATHS["organizations"] + "/default/export"),
            org_id="default",
        )
        self.assertEqual(response.status_code, 200)
        after = len(self.app.state.audit_repo.list_recent_logs(limit=100))
        self.assertEqual(after, before)
