import json
import os
import re
import unittest
from pathlib import Path
from urllib.parse import urlencode
from unittest.mock import patch

from starlette.requests import Request

from sync_app.core.models import DepartmentNode
from sync_app.services.config_store import save_editable_config
from sync_app.web.app import resolve_web_runtime_settings
from sync_app.web import create_app
from sync_app.web.security import hash_password


class WebAuthorizationTests(unittest.TestCase):
    def setUp(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        self.db_path = test_root / "web_authz.db"
        self.config_path = test_root / "web_authz.ini"

        for path in (self.db_path, self.config_path):
            if path.exists():
                path.unlink()

        save_editable_config(
            {
                "corpid": "corp-001",
                "agentid": "10001",
                "corpsecret": "secret-001",
                "webhook_url": "https://example.invalid/cgi-bin/webhook/send?key=test",
                "ldap_server": "dc01.example.local",
                "ldap_domain": "example.local",
                "ldap_username": "administrator",
                "ldap_password": "Password123!",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": False,
                "ldap_ca_cert_path": "",
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
                "schedule_time": "03:00",
                "retry_interval": 60,
                "max_retries": 3,
            },
            config_path=str(self.config_path),
        )

        self.app = create_app(db_path=str(self.db_path), config_path=str(self.config_path))
        self.app.state.user_repo.create_user("superadmin", hash_password("Admin123!"), role="super_admin")
        self.app.state.user_repo.create_user("operator1", hash_password("Admin123!"), role="operator")
        self.app.state.user_repo.create_user("auditor1", hash_password("Admin123!"), role="auditor")
        self.session: dict[str, str] = {}

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_path) + suffix)
            if path.exists():
                try:
                    path.unlink()
                except PermissionError:
                    pass
        if self.config_path.exists():
            self.config_path.unlink()
        for config_file in self.db_path.parent.glob("web_authz*.ini"):
            if config_file == self.config_path:
                continue
            try:
                config_file.unlink()
            except PermissionError:
                pass
        backup_dir = self.db_path.parent / "backups"
        if backup_dir.exists():
            for item in backup_dir.glob("*"):
                try:
                    item.unlink()
                except PermissionError:
                    pass

    def _route(self, path: str, method: str):
        method = method.upper()
        for route in self.app.router.routes:
            if getattr(route, "path", None) == path and method in getattr(route, "methods", set()):
                return route.endpoint
        raise AssertionError(f"route not found: {method} {path}")

    def _request(
        self,
        path: str,
        method: str = "GET",
        *,
        query: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Request:
        query_string = urlencode(query or {}).encode("utf-8")
        scope = {
            "type": "http",
            "method": method.upper(),
            "path": path,
            "headers": [
                (str(key).lower().encode("utf-8"), str(value).encode("utf-8"))
                for key, value in (headers or {}).items()
            ],
            "query_string": query_string,
            "app": self.app,
            "session": self.session,
        }
        return Request(scope)

    @staticmethod
    def _response_body(response) -> bytes:
        render_for_test = getattr(response, "render_for_test", None)
        if callable(render_for_test):
            return render_for_test()
        body = getattr(response, "body", None)
        if body is not None:
            return body
        raise AssertionError(f"response does not expose body bytes: {response!r}")

    @classmethod
    def _text(cls, response) -> str:
        return cls._response_body(response).decode("utf-8")

    def _login(self, username: str):
        self.session = {}
        login_page = self._route("/login", "GET")(self._request("/login"))
        self.assertEqual(login_page.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(login_page))
        self.assertIsNotNone(match)
        response = self._route("/login", "POST")(
            self._request("/login", "POST"),
            csrf_token=match.group(1),
            username=username,
            password="Admin123!",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(self.session.get("username"), username)

    def _build_config_form_payload(self, **overrides):
        current_org = self.app.state.organization_repo.get_default_organization_record()
        self.assertIsNotNone(current_org)
        editable = self.app.state.org_config_repo.get_editable_config(
            current_org.org_id,
            config_path=str(self.config_path),
        )
        payload = {
            "corpid": editable["corpid"],
            "agentid": editable["agentid"],
            "corpsecret": "secret-001",
            "webhook_url": "https://example.invalid/cgi-bin/webhook/send?key=test",
            "ldap_server": editable["ldap_server"],
            "ldap_domain": editable["ldap_domain"],
            "ldap_username": editable["ldap_username"],
            "ldap_password": "Password123!",
            "ldap_port": editable["ldap_port"],
            "ldap_use_ssl": "true" if editable["ldap_use_ssl"] else "false",
            "ldap_validate_cert": "true" if editable["ldap_validate_cert"] else "false",
            "ldap_ca_cert_path": editable["ldap_ca_cert_path"],
            "default_password": "ChangeMe123!",
            "force_change_password": "true" if editable["force_change_password"] else "false",
            "password_complexity": editable["password_complexity"],
            "schedule_time": editable["schedule_time"],
            "retry_interval": editable["retry_interval"],
            "max_retries": editable["max_retries"],
            "group_display_separator": self.app.state.settings_repo.get_value(
                "group_display_separator",
                "-",
                org_id=current_org.org_id,
            ),
            "group_recursive_enabled": "true"
            if self.app.state.settings_repo.get_bool("group_recursive_enabled", True, org_id=current_org.org_id)
            else "false",
            "managed_relation_cleanup_enabled": "true"
            if self.app.state.settings_repo.get_bool("managed_relation_cleanup_enabled", False, org_id=current_org.org_id)
            else "false",
            "schedule_execution_mode": self.app.state.settings_repo.get_value(
                "schedule_execution_mode",
                "apply",
                org_id=current_org.org_id,
            ),
            "web_bind_host": self.app.state.settings_repo.get_value("web_bind_host", "127.0.0.1"),
            "web_bind_port": self.app.state.settings_repo.get_int("web_bind_port", 8000),
            "web_public_base_url": self.app.state.settings_repo.get_value("web_public_base_url", ""),
            "web_session_cookie_secure_mode": self.app.state.settings_repo.get_value(
                "web_session_cookie_secure_mode",
                "auto",
            ),
            "web_trust_proxy_headers": "true"
            if self.app.state.settings_repo.get_bool("web_trust_proxy_headers", False)
            else "false",
            "web_forwarded_allow_ips": self.app.state.settings_repo.get_value(
                "web_forwarded_allow_ips",
                "127.0.0.1",
            ),
            "brand_display_name": self.app.state.settings_repo.get_value("brand_display_name", "AD Org Sync"),
            "brand_mark_text": self.app.state.settings_repo.get_value("brand_mark_text", "AD"),
            "brand_attribution": self.app.state.settings_repo.get_value(
                "brand_attribution",
                "微信公众号：大刘讲IT",
            ),
            "user_ou_placement_strategy": self.app.state.settings_repo.get_value(
                "user_ou_placement_strategy",
                "source_primary_department",
                org_id=current_org.org_id,
            ),
            "source_root_unit_ids": self.app.state.settings_repo.get_value(
                "source_root_unit_ids",
                "",
                org_id=current_org.org_id,
            ),
            "directory_root_ou_path": self.app.state.settings_repo.get_value(
                "directory_root_ou_path",
                "",
                org_id=current_org.org_id,
            ),
            "disabled_users_ou_path": self.app.state.settings_repo.get_value(
                "disabled_users_ou_path",
                "Disabled Users",
                org_id=current_org.org_id,
            ),
            "custom_group_ou_path": self.app.state.settings_repo.get_value(
                "custom_group_ou_path",
                "Managed Groups",
                org_id=current_org.org_id,
            ),
            "soft_excluded_groups": "\n".join(
                self.app.state.exclusion_repo.list_soft_excluded_group_names(
                    enabled_only=False,
                    org_id=current_org.org_id,
                )
            ),
        }
        payload.update(overrides)
        return payload

    def test_operator_cannot_access_config_or_database_actions(self):
        self._login("operator1")

        dashboard = self._route("/dashboard", "GET")(self._request("/dashboard"))
        self.assertEqual(dashboard.status_code, 200)
        self.assertNotIn('href="/config"', self._text(dashboard))

        response = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")

        response = self._route("/database/backup", "POST")(
            self._request("/database/backup", "POST"),
            csrf_token="",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")

        response = self._route("/jobs", "GET")(self._request("/jobs"))
        self.assertEqual(response.status_code, 200)

    def test_auditor_sees_readonly_mappings_and_cannot_run_jobs(self):
        self._login("auditor1")

        with patch("sync_app.providers.source.wecom.WeComAPI") as mock_wecom:
            mock_wecom.return_value.get_department_list.return_value = []
            response = self._route("/mappings", "GET")(self._request("/mappings"))
        self.assertEqual(response.status_code, 200)
        self.assertIn("Current role can view mappings only.", self._text(response))

        response = self._route("/jobs/run", "POST")(
            self._request("/jobs/run", "POST"),
            csrf_token="",
            mode="dry_run",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")

    def test_operator_can_view_exceptions_but_cannot_modify_them(self):
        self._login("operator1")

        with patch("sync_app.providers.source.wecom.WeComAPI") as mock_wecom:
            mock_wecom.return_value.get_department_list.return_value = []
            response = self._route("/exceptions", "GET")(self._request("/exceptions"))
        self.assertEqual(response.status_code, 200)
        self.assertIn("read-only for exception rules", self._text(response))

        response = self._route("/exceptions", "POST")(
            self._request("/exceptions", "POST"),
            csrf_token="",
            rule_type="skip_user_disable",
            match_value="alice",
            notes="keep enabled",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")

    def test_login_is_rate_limited_after_repeated_failures(self):
        self.app.state.login_rate_limiter.max_attempts = 2
        self.app.state.login_rate_limiter.window_seconds = 300
        self.app.state.login_rate_limiter.lockout_seconds = 60
        self.session = {}

        login_page = self._route("/login", "GET")(self._request("/login"))
        self.assertEqual(login_page.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(login_page))
        self.assertIsNotNone(match)
        csrf_token = match.group(1)

        for _ in range(2):
            response = self._route("/login", "POST")(
                self._request("/login", "POST"),
                csrf_token=csrf_token,
                username="superadmin",
                password="wrong-password",
            )
            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], "/login")

        response = self._route("/login", "POST")(
            self._request("/login", "POST"),
            csrf_token=csrf_token,
            username="superadmin",
            password="Admin123!",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/login")
        self.assertNotEqual(self.session.get("username"), "superadmin")

    def test_super_admin_cannot_create_user_with_weak_password(self):
        self._login("superadmin")

        users_page = self._route("/users", "GET")(self._request("/users"))
        self.assertEqual(users_page.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(users_page))
        self.assertIsNotNone(match)

        response = self._route("/users", "POST")(
            self._request("/users", "POST"),
            csrf_token=match.group(1),
            username="weakuser",
            password="short7!",
            role="operator",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/users")
        self.assertIsNone(self.app.state.user_repo.get_user_record_by_username("weakuser"))

    def test_super_admin_can_create_user_with_simple_eight_character_password(self):
        self._login("superadmin")

        users_page = self._route("/users", "GET")(self._request("/users"))
        self.assertEqual(users_page.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(users_page))
        self.assertIsNotNone(match)

        response = self._route("/users", "POST")(
            self._request("/users", "POST"),
            csrf_token=match.group(1),
            username="simple8",
            password="simple88",
            role="operator",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/users")
        self.assertIsNotNone(self.app.state.user_repo.get_user_record_by_username("simple8"))

    def test_super_admin_can_manage_exception_rules(self):
        self._login("superadmin")

        with patch("sync_app.providers.source.wecom.WeComAPI") as mock_wecom:
            mock_wecom.return_value.get_department_list.return_value = [{"id": 1, "name": "HQ"}]
            response = self._route("/exceptions", "GET")(self._request("/exceptions"))
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(response))
        self.assertIsNotNone(match)

        response = self._route("/exceptions", "POST")(
            self._request("/exceptions", "POST"),
            csrf_token=match.group(1),
            rule_type="skip_user_disable",
            match_value="alice",
            notes="keep enabled",
            expires_at="2030-01-01T10:00",
            is_once="1",
        )
        self.assertEqual(response.status_code, 303)
        self.assertIsNotNone(self.app.state.exception_rule_repo.list_rule_records())
        self.assertTrue(
            any(
                item.rule_type == "skip_user_disable"
                and item.match_value == "alice"
                and item.is_once
                and item.expires_at
                for item in self.app.state.exception_rule_repo.list_rule_records()
            )
        )

        response = self._route("/exceptions/import", "POST")(
            self._request("/exceptions/import", "POST"),
            csrf_token=match.group(1),
            bulk_rules=(
                "rule_type,match_value,notes,is_enabled,expires_at,is_once\n"
                "skip_user_sync,bob,bulk import,true,2031-01-01T09:00,true\n"
                "skip_group_relation_cleanup,WECOM_D1001,bulk import,false,,false"
            ),
        )
        self.assertEqual(response.status_code, 303)
        self.assertTrue(
            any(
                item.rule_type == "skip_user_sync"
                and item.match_value == "bob"
                and item.is_once
                and item.expires_at
                for item in self.app.state.exception_rule_repo.list_rule_records()
            )
        )

        export_response = self._route("/exceptions/export", "GET")(self._request("/exceptions/export"))
        self.assertEqual(export_response.status_code, 200)
        export_text = self._response_body(export_response).decode("utf-8-sig")
        self.assertIn("rule_type,match_value,notes,is_enabled,expires_at,is_once", export_text)
        self.assertIn("skip_user_disable,alice,keep enabled,true,", export_text)
        self.assertIn(",true", export_text)
        self.assertIn("skip_user_sync,bob,bulk import,true,", export_text)

    def test_super_admin_can_manage_advanced_sync_settings_connectors_and_mappings(self):
        self._login("superadmin")

        response = self._route("/advanced-sync", "GET")(self._request("/advanced-sync"))
        self.assertEqual(response.status_code, 200)
        body = self._text(response)
        self.assertIn("Advanced Sync", body)
        self.assertIn("All advanced capabilities are opt-in.", body)
        self.assertIn("Pending Lifecycle Queue", body)
        self.assertIn("Pending Replay Requests", body)
        self.assertNotIn('name="advanced_connector_routing_enabled" value="1" checked', body)
        match = re.search(r'name="csrf_token" value="([^"]+)"', body)
        self.assertIsNotNone(match)
        csrf_token = match.group(1)

        response = self._route("/advanced-sync/policies", "POST")(
            self._request("/advanced-sync/policies", "POST"),
            csrf_token=csrf_token,
            advanced_connector_routing_enabled="1",
            attribute_mapping_enabled="1",
            write_back_enabled="1",
            custom_group_sync_enabled="1",
            offboarding_lifecycle_enabled="1",
            rehire_restore_enabled="1",
            automatic_replay_enabled="1",
            future_onboarding_enabled="1",
            future_onboarding_start_field="hire_date",
            contractor_lifecycle_enabled="1",
            lifecycle_employment_type_field="employment_type",
            contractor_end_field="contract_end_date",
            lifecycle_sponsor_field="sponsor_userid",
            contractor_type_values="contractor,intern",
            offboarding_grace_days=7,
            offboarding_notify_managers="1",
            disable_circuit_breaker_enabled="1",
            disable_circuit_breaker_percent=3.5,
            disable_circuit_breaker_min_count=2,
            disable_circuit_breaker_requires_approval="1",
            managed_group_type="distribution",
            managed_group_mail_domain="groups.example.com",
            custom_group_ou_path="Managed Groups/Regional",
        )
        self.assertEqual(response.status_code, 303)
        self.assertTrue(self.app.state.settings_repo.get_bool("advanced_connector_routing_enabled", False, org_id="default"))
        self.assertTrue(self.app.state.settings_repo.get_bool("attribute_mapping_enabled", False, org_id="default"))
        self.assertTrue(self.app.state.settings_repo.get_bool("write_back_enabled", False, org_id="default"))
        self.assertTrue(self.app.state.settings_repo.get_bool("custom_group_sync_enabled", False, org_id="default"))
        self.assertTrue(self.app.state.settings_repo.get_bool("offboarding_lifecycle_enabled", False, org_id="default"))
        self.assertTrue(self.app.state.settings_repo.get_bool("rehire_restore_enabled", False, org_id="default"))
        self.assertTrue(self.app.state.settings_repo.get_bool("automatic_replay_enabled", False, org_id="default"))
        self.assertTrue(self.app.state.settings_repo.get_bool("future_onboarding_enabled", False, org_id="default"))
        self.assertTrue(self.app.state.settings_repo.get_bool("contractor_lifecycle_enabled", False, org_id="default"))
        self.assertEqual(self.app.state.settings_repo.get_int("offboarding_grace_days", 0, org_id="default"), 7)
        self.assertEqual(
            self.app.state.settings_repo.get_value("future_onboarding_start_field", "", org_id="default"),
            "hire_date",
        )
        self.assertEqual(
            self.app.state.settings_repo.get_value("contractor_end_field", "", org_id="default"),
            "contract_end_date",
        )
        self.assertEqual(
            self.app.state.settings_repo.get_value("contractor_type_values", "", org_id="default"),
            "contractor,intern",
        )
        self.assertEqual(
            self.app.state.settings_repo.get_float("disable_circuit_breaker_percent", 0.0, org_id="default"),
            3.5,
        )
        self.assertEqual(
            self.app.state.settings_repo.get_value("managed_group_type", "", org_id="default"),
            "distribution",
        )

        response = self._route("/advanced-sync/connectors", "POST")(
            self._request("/advanced-sync/connectors", "POST"),
            csrf_token=csrf_token,
            connector_id="asia",
            name="Asia Domain",
            config_path="config.asia.ini",
            ldap_server="dc01.asia.example.local",
            ldap_domain="asia.example.local",
            ldap_username="ASIA\\administrator",
            ldap_password="Password123!",
            ldap_use_ssl="true",
            ldap_port="636",
            ldap_validate_cert="true",
            ldap_ca_cert_path="C:\\certs\\asia-ca.pem",
            default_password="ConnectorPass123!",
            force_change_password="true",
            password_complexity="medium",
            root_department_ids="2,3",
            username_template="{pinyin_initials}{employee_id}",
            disabled_users_ou="Disabled Users",
            group_type="mail_enabled_security",
            group_mail_domain="groups.asia.example.com",
            custom_group_ou_path="Managed Groups/Asia",
            managed_tag_ids="1001,1002",
            managed_external_chat_ids="chat_01",
            is_enabled="1",
        )
        self.assertEqual(response.status_code, 303)
        connector = self.app.state.connector_repo.get_connector_record("asia")
        self.assertIsNotNone(connector)
        self.assertEqual(connector.root_department_ids, [2, 3])
        self.assertEqual(connector.group_type, "mail_enabled_security")
        self.assertEqual(connector.managed_tag_ids, ["1001", "1002"])
        self.assertEqual(connector.ldap_server, "dc01.asia.example.local")
        self.assertEqual(connector.ldap_domain, "asia.example.local")
        self.assertTrue(connector.ldap_use_ssl)
        self.assertTrue(connector.ldap_validate_cert)
        self.assertEqual(connector.password_complexity, "medium")

        response = self._route("/advanced-sync/mappings", "POST")(
            self._request("/advanced-sync/mappings", "POST"),
            csrf_token=csrf_token,
            connector_id="asia",
            direction="source_to_ad",
            source_field="position",
            target_field="title",
            transform_template="",
            sync_mode="replace",
            notes="map position",
            is_enabled="1",
        )
        self.assertEqual(response.status_code, 303)
        rules = self.app.state.attribute_mapping_repo.list_rule_records(connector_id="asia", org_id="default")
        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0].direction, "source_to_ad")
        self.assertEqual(rules[0].source_field, "position")
        self.assertEqual(rules[0].target_field, "title")

    def test_super_admin_cannot_bind_system_protected_ad_account(self):
        self._login("superadmin")

        response = self._route("/mappings", "GET")(self._request("/mappings"))
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(response))
        self.assertIsNotNone(match)

        response = self._route("/mappings/bind", "POST")(
            self._request("/mappings/bind", "POST"),
            csrf_token=match.group(1),
            source_user_id="alice",
            ad_username="administrator",
            notes="should be blocked",
        )
        self.assertEqual(response.status_code, 303)
        self.assertIsNone(self.app.state.user_binding_repo.get_binding_record_by_source_user_id("alice"))
        self.assertIn("system-protected", self.session["_flash"]["message"])

    def test_mappings_and_advanced_sync_pages_use_generic_source_wording(self):
        self._login("superadmin")

        mappings_response = self._route("/mappings", "GET")(self._request("/mappings"))
        self.assertEqual(mappings_response.status_code, 200)
        mappings_text = self._text(mappings_response)
        self.assertIn("Source User ID", mappings_text)
        self.assertIn("Search source user, AD user, or notes", mappings_text)
        self.assertNotIn("WeCom User ID", mappings_text)
        self.assertNotIn("Search WeCom user, AD user, or notes", mappings_text)

        advanced_response = self._route("/advanced-sync", "GET")(self._request("/advanced-sync"))
        self.assertEqual(advanced_response.status_code, 200)
        advanced_text = self._text(advanced_response)
        self.assertIn("Enable source -&gt; AD attribute mapping", advanced_text)
        self.assertIn("Enable AD -&gt; source write-back", advanced_text)
        self.assertIn("Source Root Unit IDs", advanced_text)
        self.assertNotIn("Enable WeCom -&gt; AD attribute mapping", advanced_text)
        self.assertNotIn("Enable AD -&gt; WeCom write-back", advanced_text)
        self.assertNotIn("WeCom Root Department IDs", advanced_text)

    def test_advanced_sync_policies_and_mappings_are_scoped_to_selected_organization(self):
        self._login("superadmin")
        asia_config_path = self.db_path.parent / "web_authz_asia.ini"
        save_editable_config(
            {
                "corpid": "corp-asia",
                "agentid": "20001",
                "corpsecret": "secret-asia",
                "webhook_url": "",
                "ldap_server": "dc01.asia.example.local",
                "ldap_domain": "asia.example.local",
                "ldap_username": "administrator",
                "ldap_password": "Password123!",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": True,
                "ldap_ca_cert_path": "",
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
                "schedule_time": "03:00",
                "retry_interval": 60,
                "max_retries": 3,
            },
            config_path=str(asia_config_path),
        )
        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=str(asia_config_path),
            description="Asia tenant",
            is_enabled=True,
        )
        self.app.state.connector_repo.upsert_connector(
            connector_id="asia-connector",
            org_id="asia",
            name="Asia Connector",
            config_path=str(asia_config_path),
            root_department_ids=[2],
        )
        self.session["selected_org_id"] = "asia"

        page = self._route("/advanced-sync", "GET")(self._request("/advanced-sync"))
        self.assertEqual(page.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(page))
        self.assertIsNotNone(match)
        csrf_token = match.group(1)

        response = self._route("/advanced-sync/policies", "POST")(
            self._request("/advanced-sync/policies", "POST"),
            csrf_token=csrf_token,
            offboarding_grace_days=9,
            attribute_mapping_enabled="1",
            write_back_enabled="1",
            offboarding_notify_managers=None,
            advanced_connector_routing_enabled=None,
            custom_group_sync_enabled=None,
            offboarding_lifecycle_enabled=None,
            rehire_restore_enabled=None,
            automatic_replay_enabled=None,
            future_onboarding_enabled=None,
            future_onboarding_start_field="hire_date",
            contractor_lifecycle_enabled=None,
            lifecycle_employment_type_field="employment_type",
            contractor_end_field="contract_end_date",
            lifecycle_sponsor_field="sponsor_userid",
            contractor_type_values="contractor,intern,vendor,temp",
            disable_circuit_breaker_enabled=None,
            disable_circuit_breaker_percent=5.0,
            disable_circuit_breaker_min_count=10,
            disable_circuit_breaker_requires_approval="1",
            managed_group_type="distribution",
            managed_group_mail_domain="groups.asia.example.com",
            custom_group_ou_path="Managed Groups/Asia",
        )
        self.assertEqual(response.status_code, 303)
        self.assertTrue(self.app.state.settings_repo.get_bool("attribute_mapping_enabled", False, org_id="asia"))
        self.assertFalse(self.app.state.settings_repo.get_bool("attribute_mapping_enabled", False, org_id="default"))
        self.assertEqual(self.app.state.settings_repo.get_int("offboarding_grace_days", 0, org_id="asia"), 9)
        self.assertEqual(self.app.state.settings_repo.get_int("offboarding_grace_days", 0, org_id="default"), 0)

        response = self._route("/advanced-sync/mappings", "POST")(
            self._request("/advanced-sync/mappings", "POST"),
            csrf_token=csrf_token,
            connector_id="asia-connector",
            direction="source_to_ad",
            source_field="position",
            target_field="title",
            transform_template="",
            sync_mode="replace",
            notes="asia only",
            is_enabled="1",
        )
        self.assertEqual(response.status_code, 303)
        asia_rules = self.app.state.attribute_mapping_repo.list_rule_records(
            connector_id="asia-connector",
            org_id="asia",
        )
        default_rules = self.app.state.attribute_mapping_repo.list_rule_records(
            connector_id="asia-connector",
            org_id="default",
        )
        self.assertEqual(len(asia_rules), 1)
        self.assertEqual(asia_rules[0].direction, "source_to_ad")
        self.assertEqual(len(default_rules), 0)

    def test_super_admin_can_resolve_conflict_with_manual_binding(self):
        self._login("superadmin")
        self.app.state.job_repo.create_job(
            "job-conflict-001",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
        )
        conflict_id = self.app.state.conflict_repo.add_conflict(
            job_id="job-conflict-001",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="alice matched multiple AD candidates",
            resolution_hint="create manual binding",
            details={
                "userid": "alice",
                "candidates": [
                    {"rule": "existing_ad_userid", "username": "alice"},
                    {"rule": "existing_ad_email_localpart", "username": "alice.alt"},
                ],
            },
        )

        response = self._route("/conflicts", "GET")(self._request("/conflicts"))
        self.assertEqual(response.status_code, 200)
        self.assertIn("alice matched multiple AD candidates", self._text(response))
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(response))
        self.assertIsNotNone(match)

        response = self._route("/conflicts/{conflict_id}/resolve-binding", "POST")(
            self._request("/conflicts/1/resolve-binding", "POST"),
            conflict_id=conflict_id,
            csrf_token=match.group(1),
            ad_username="alice.alt",
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn("/conflicts?job_id=job-conflict-001", response.headers["location"])

        binding = self.app.state.user_binding_repo.get_binding_record_by_source_user_id("alice")
        self.assertIsNotNone(binding)
        self.assertEqual(binding.ad_username, "alice.alt")

        conflict = self.app.state.conflict_repo.get_conflict_record(conflict_id)
        self.assertIsNotNone(conflict)
        self.assertEqual(conflict.status, "resolved")
        self.assertEqual((conflict.resolution_payload or {}).get("action"), "manual_binding")

    def test_manual_binding_resolves_all_matching_open_conflicts_without_limit(self):
        self._login("superadmin")
        self.app.state.job_repo.create_job(
            "job-conflict-many",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
        )
        first_conflict_id = None
        for index in range(520):
            conflict_id = self.app.state.conflict_repo.add_conflict(
                job_id="job-conflict-many",
                conflict_type="multiple_ad_candidates",
                source_id="alice",
                target_key="identity_binding",
                message=f"alice conflict {index}",
                resolution_hint="create manual binding",
                details={"userid": "alice"},
            )
            if first_conflict_id is None:
                first_conflict_id = conflict_id

        response = self._route("/conflicts", "GET")(
            self._request("/conflicts", query={"job_id": "job-conflict-many"})
        )
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(response))
        self.assertIsNotNone(match)

        response = self._route("/conflicts/{conflict_id}/resolve-binding", "POST")(
            self._request("/conflicts/1/resolve-binding", "POST"),
            conflict_id=first_conflict_id,
            csrf_token=match.group(1),
            ad_username="alice.ad",
        )
        self.assertEqual(response.status_code, 303)

        with self.app.state.db_manager.connection() as conn:
            open_count = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM sync_conflicts
                WHERE job_id = ?
                  AND source_id = ?
                  AND status = 'open'
                """,
                ("job-conflict-many", "alice"),
            ).fetchone()["total"]
            resolved_count = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM sync_conflicts
                WHERE job_id = ?
                  AND source_id = ?
                  AND status = 'resolved'
                """,
                ("job-conflict-many", "alice"),
            ).fetchone()["total"]
        self.assertEqual(int(open_count), 0)
        self.assertEqual(int(resolved_count), 520)

    def test_super_admin_can_bulk_skip_and_reopen_conflicts(self):
        self._login("superadmin")
        self.app.state.job_repo.create_job(
            "job-conflict-002",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
        )
        conflict_id_1 = self.app.state.conflict_repo.add_conflict(
            job_id="job-conflict-002",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="alice matched multiple AD candidates",
            resolution_hint="create manual binding",
            details={"userid": "alice"},
        )
        conflict_id_2 = self.app.state.conflict_repo.add_conflict(
            job_id="job-conflict-002",
            conflict_type="shared_ad_account",
            source_id="bob",
            target_key="shared.account",
            message="bob shares AD account",
            resolution_hint="resolve manually",
            details={"userid": "bob"},
        )

        response = self._route("/conflicts", "GET")(self._request("/conflicts"))
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(response))
        self.assertIsNotNone(match)

        response = self._route("/conflicts/bulk", "POST")(
            self._request("/conflicts/bulk", "POST"),
            csrf_token=match.group(1),
            action="skip_user_sync",
            conflict_ids=[str(conflict_id_1), str(conflict_id_2)],
            notes="bulk skip",
            return_query="",
            return_status="open",
            return_job_id="job-conflict-002",
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn("/conflicts?status=open&job_id=job-conflict-002", response.headers["location"])

        self.assertTrue(
            any(
                item.rule_type == "skip_user_sync" and item.match_value == "alice"
                for item in self.app.state.exception_rule_repo.list_rule_records()
            )
        )
        self.assertTrue(
            any(
                item.rule_type == "skip_user_sync" and item.match_value == "bob"
                for item in self.app.state.exception_rule_repo.list_rule_records()
            )
        )
        self.assertEqual(self.app.state.conflict_repo.get_conflict_record(conflict_id_1).status, "resolved")
        self.assertEqual(self.app.state.conflict_repo.get_conflict_record(conflict_id_2).status, "resolved")

        response = self._route("/conflicts/{conflict_id}/reopen", "POST")(
            self._request("/conflicts/1/reopen", "POST"),
            conflict_id=conflict_id_1,
            csrf_token=match.group(1),
            return_query="",
            return_status="resolved",
            return_job_id="job-conflict-002",
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn("/conflicts?status=resolved&job_id=job-conflict-002", response.headers["location"])

        reopened_conflict = self.app.state.conflict_repo.get_conflict_record(conflict_id_1)
        self.assertIsNotNone(reopened_conflict)
        self.assertEqual(reopened_conflict.status, "open")
        self.assertIsNone(reopened_conflict.resolution_payload)
        self.assertEqual(reopened_conflict.resolved_at, "")

    def test_super_admin_can_apply_conflict_recommendation(self):
        self._login("superadmin")
        self.app.state.job_repo.create_job(
            "job-conflict-003",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
        )
        conflict_id = self.app.state.conflict_repo.add_conflict(
            job_id="job-conflict-003",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="alice matched multiple AD candidates",
            resolution_hint="create manual binding",
            details={
                "userid": "alice",
                "candidates": [
                    {"rule": "existing_ad_email_localpart", "username": "alice.alt"},
                    {"rule": "existing_ad_userid", "username": "alice"},
                ],
            },
        )

        response = self._route("/conflicts", "GET")(self._request("/conflicts"))
        self.assertEqual(response.status_code, 200)
        response_text = self._text(response)
        self.assertIn("Recommended Action", response_text)
        self.assertIn("Apply Recommendation", response_text)
        self.assertIn("alice", response_text)
        match = re.search(r'name="csrf_token" value="([^"]+)"', response_text)
        self.assertIsNotNone(match)

        response = self._route("/conflicts/{conflict_id}/apply-recommendation", "POST")(
            self._request("/conflicts/1/apply-recommendation", "POST"),
            conflict_id=conflict_id,
            csrf_token=match.group(1),
            return_query="",
            return_status="open",
            return_job_id="job-conflict-003",
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn("/conflicts?status=open&job_id=job-conflict-003", response.headers["location"])

        binding = self.app.state.user_binding_repo.get_binding_record_by_source_user_id("alice")
        self.assertIsNotNone(binding)
        self.assertEqual(binding.ad_username, "alice")

        conflict = self.app.state.conflict_repo.get_conflict_record(conflict_id)
        self.assertIsNotNone(conflict)
        self.assertEqual(conflict.status, "resolved")
        self.assertEqual((conflict.resolution_payload or {}).get("action"), "manual_binding")

    def test_low_confidence_recommendation_requires_confirmation_reason(self):
        self._login("superadmin")
        self.app.state.job_repo.create_job(
            "job-conflict-004",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
        )
        conflict_id = self.app.state.conflict_repo.add_conflict(
            job_id="job-conflict-004",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="alice matched multiple AD candidates",
            resolution_hint="create manual binding",
            details={
                "userid": "alice",
                "candidates": [
                    {"rule": "existing_ad_email_localpart", "username": "alice.alt"},
                ],
            },
        )

        response = self._route("/conflicts", "GET")(self._request("/conflicts"))
        self.assertEqual(response.status_code, 200)
        response_text = self._text(response)
        self.assertIn("confirmation required", response_text)
        match = re.search(r'name="csrf_token" value="([^"]+)"', response_text)
        self.assertIsNotNone(match)

        response = self._route("/conflicts/{conflict_id}/apply-recommendation", "POST")(
            self._request("/conflicts/1/apply-recommendation", "POST"),
            conflict_id=conflict_id,
            csrf_token=match.group(1),
            confirmation_reason="",
            return_query="",
            return_status="open",
            return_job_id="job-conflict-004",
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn("/conflicts?status=open&job_id=job-conflict-004", response.headers["location"])
        self.assertEqual(self.app.state.conflict_repo.get_conflict_record(conflict_id).status, "open")

        response = self._route("/conflicts/{conflict_id}/apply-recommendation", "POST")(
            self._request("/conflicts/1/apply-recommendation", "POST"),
            conflict_id=conflict_id,
            csrf_token=match.group(1),
            confirmation_reason="email local part checked manually",
            return_query="",
            return_status="open",
            return_job_id="job-conflict-004",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(self.app.state.conflict_repo.get_conflict_record(conflict_id).status, "resolved")

    def test_conflicts_page_uses_database_pagination(self):
        self._login("superadmin")
        self.app.state.job_repo.create_job(
            "job-conflict-paged",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
        )
        for index in range(35):
            self.app.state.conflict_repo.add_conflict(
                job_id="job-conflict-paged",
                conflict_type="multiple_ad_candidates",
                source_id=f"user-{index:02d}",
                target_key="identity_binding",
                message=f"paged-conflict-{index:02d}",
                resolution_hint="manual review",
                details={"userid": f"user-{index:02d}"},
            )

        response = self._route("/conflicts", "GET")(
            self._request("/conflicts", query={"page_number": "2"})
        )
        self.assertEqual(response.status_code, 200)
        response_text = self._text(response)
        self.assertIn("paged-conflict-00", response_text)
        self.assertNotIn("paged-conflict-34", response_text)
        self.assertIn("page 2 / 2", response_text)

    def test_conflicts_page_remembers_filters_for_current_session(self):
        self._login("superadmin")
        self.app.state.job_repo.create_job(
            "job-conflict-remembered",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
        )
        self.app.state.conflict_repo.add_conflict(
            job_id="job-conflict-remembered",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="remembered-conflict",
            resolution_hint="manual review",
        )

        first_response = self._route("/conflicts", "GET")(
            self._request("/conflicts", query={"q": "alice", "status": "open", "job_id": "job-conflict-remembered"})
        )
        remembered_response = self._route("/conflicts", "GET")(self._request("/conflicts"))
        reset_response = self._route("/conflicts", "GET")(
            self._request("/conflicts", query={"clear_filters": "1"})
        )
        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(remembered_response.status_code, 200)
        self.assertEqual(reset_response.status_code, 200)
        remembered_text = self._text(remembered_response)
        self.assertIn('value="alice"', remembered_text)
        self.assertIn('value="job-conflict-remembered"', remembered_text)
        self.assertIn("Filters are remembered for this browser session.", remembered_text)
        self.assertIn('name="job_id" value=""', self._text(reset_response))

    def test_job_detail_supports_independent_pagination(self):
        self._login("superadmin")
        job_id = "job-detail-paged"
        self.app.state.job_repo.create_job(
            job_id,
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
        )
        for index in range(30):
            self.app.state.event_repo.add_event(
                job_id,
                "INFO",
                "test_event",
                f"event-message-{index:02d}",
                stage_name="plan",
            )
            self.app.state.planned_operation_repo.add_operation(
                job_id,
                "user",
                "create_user",
                source_id=f"planned-source-{index:02d}",
                target_dn=f"CN=User{index:02d},OU=Managed,DC=example,DC=local",
            )
            self.app.state.operation_log_repo.add_record(
                job_id=job_id,
                stage_name="apply",
                object_type="user",
                operation_type="create_user",
                status="success",
                message=f"operation-message-{index:02d}",
                source_id=f"operation-source-{index:02d}",
            )
            self.app.state.conflict_repo.add_conflict(
                job_id=job_id,
                conflict_type="multiple_ad_candidates",
                source_id=f"conflict-source-{index:02d}",
                target_key="identity_binding",
                message=f"job-detail-conflict-{index:02d}",
                resolution_hint="manual review",
            )

        response = self._route("/jobs/{job_id}", "GET")(
            self._request(
                f"/jobs/{job_id}",
                query={
                    "events_page": "2",
                    "planned_page": "2",
                    "operations_page": "2",
                    "conflicts_page": "2",
                },
            ),
            job_id=job_id,
        )
        self.assertEqual(response.status_code, 200)
        response_text = self._text(response)
        self.assertIn("event-message-00", response_text)
        self.assertNotIn("event-message-29", response_text)
        self.assertIn("planned-source-29", response_text)
        self.assertNotIn("planned-source-00", response_text)
        self.assertIn("operation-message-29", response_text)
        self.assertNotIn("operation-message-00", response_text)
        self.assertIn("job-detail-conflict-29", response_text)
        self.assertNotIn("job-detail-conflict-00", response_text)

    def test_audit_page_supports_search_and_pagination(self):
        self._login("superadmin")
        for index in range(55):
            self.app.state.audit_repo.add_log(
                actor_username="superadmin",
                action_type="job.run",
                target_type="sync_job",
                target_id=f"job-{index:02d}",
                result="success",
                message=f"audit-message-{index:02d}",
            )
        self.app.state.audit_repo.add_log(
            actor_username="superadmin",
            action_type="job.run",
            target_type="sync_job",
            target_id="job-keyword",
            result="success",
            message="keyword-only-entry",
        )

        search_response = self._route("/audit", "GET")(
            self._request("/audit", query={"q": "keyword"})
        )
        self.assertEqual(search_response.status_code, 200)
        search_text = self._text(search_response)
        self.assertIn("keyword-only-entry", search_text)
        self.assertNotIn("audit-message-54", search_text)

        paged_response = self._route("/audit", "GET")(
            self._request("/audit", query={"page_number": "2", "clear_filters": "1"})
        )
        self.assertEqual(paged_response.status_code, 200)
        paged_text = self._text(paged_response)
        self.assertIn("audit-message-04", paged_text)
        self.assertNotIn("audit-message-54", paged_text)
        self.assertIn("page 2 / 2", paged_text)

    def test_audit_page_remembers_filters_for_current_session(self):
        self._login("superadmin")
        self.app.state.audit_repo.add_log(
            actor_username="superadmin",
            action_type="job.run",
            target_type="sync_job",
            target_id="job-memory",
            result="success",
            message="remembered-audit-entry",
        )

        first_response = self._route("/audit", "GET")(
            self._request("/audit", query={"q": "remembered"})
        )
        remembered_response = self._route("/audit", "GET")(self._request("/audit"))
        reset_response = self._route("/audit", "GET")(
            self._request("/audit", query={"clear_filters": "1"})
        )
        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(remembered_response.status_code, 200)
        self.assertEqual(reset_response.status_code, 200)
        remembered_text = self._text(remembered_response)
        self.assertIn('value="remembered"', remembered_text)
        self.assertIn("Filters are remembered for this browser session.", remembered_text)
        self.assertNotIn('value="remembered"', self._text(reset_response))

    def test_audit_page_scopes_logs_to_selected_organization_with_global_entries(self):
        self._login("superadmin")
        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=str((self.db_path.parent / "web_authz_audit_asia.ini").resolve()),
            description="",
            is_enabled=True,
        )
        self.app.state.organization_repo.upsert_organization(
            org_id="europe",
            name="Europe Region",
            config_path=str((self.db_path.parent / "web_authz_audit_europe.ini").resolve()),
            description="",
            is_enabled=True,
        )
        self.session["selected_org_id"] = "asia"

        self.app.state.audit_repo.add_log(
            actor_username="superadmin",
            action_type="auth.login",
            target_type="web_admin_user",
            target_id="superadmin",
            result="success",
            message="global-audit-entry",
        )
        self.app.state.audit_repo.add_log(
            org_id="asia",
            actor_username="superadmin",
            action_type="config.update",
            target_type="config_file",
            target_id="asia.ini",
            result="success",
            message="asia-audit-entry",
        )
        self.app.state.audit_repo.add_log(
            org_id="europe",
            actor_username="superadmin",
            action_type="config.update",
            target_type="config_file",
            target_id="europe.ini",
            result="success",
            message="europe-audit-entry",
        )

        response = self._route("/audit", "GET")(self._request("/audit"))
        self.assertEqual(response.status_code, 200)
        response_text = self._text(response)
        self.assertIn("global-audit-entry", response_text)
        self.assertIn("asia-audit-entry", response_text)
        self.assertNotIn("europe-audit-entry", response_text)

    def test_mappings_page_supports_database_pagination(self):
        self._login("superadmin")
        for index in range(25):
            userid = f"user-{index:02d}"
            self.app.state.user_binding_repo.upsert_binding(
                userid,
                f"{userid}.ad",
                source="manual",
                notes=f"binding-note-{index:02d}",
                preserve_manual=False,
            )
            self.app.state.department_override_repo.upsert_override(
                userid,
                f"{2000 + index}",
                notes=f"override-note-{index:02d}",
            )

        with patch("sync_app.providers.source.wecom.WeComAPI") as mock_wecom:
            mock_wecom.return_value.get_department_list.return_value = [
                {"id": 2000 + index, "name": f"Dept {index:02d}"}
                for index in range(25)
            ]
            response = self._route("/mappings", "GET")(
                self._request("/mappings", query={"binding_page": "2", "override_page": "2"})
            )
        self.assertEqual(response.status_code, 200)
        response_text = self._text(response)
        self.assertIn("user-20.ad", response_text)
        self.assertNotIn("user-19.ad", response_text)
        self.assertIn("override-note-20", response_text)
        self.assertNotIn("override-note-19", response_text)

    def test_mappings_page_remembers_filters_for_current_session(self):
        self._login("superadmin")
        self.app.state.user_binding_repo.upsert_binding(
            "alice",
            "alice.ad",
            source="manual",
            notes="remembered-binding",
            preserve_manual=False,
        )

        with patch("sync_app.providers.source.wecom.WeComAPI") as mock_wecom:
            mock_wecom.return_value.get_department_list.return_value = []
            first_response = self._route("/mappings", "GET")(
                self._request("/mappings", query={"q": "alice", "status": "enabled"})
            )
            remembered_response = self._route("/mappings", "GET")(self._request("/mappings"))
            reset_response = self._route("/mappings", "GET")(
                self._request("/mappings", query={"clear_filters": "1"})
            )
        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(remembered_response.status_code, 200)
        self.assertEqual(reset_response.status_code, 200)
        self.assertIn('value="alice"', self._text(remembered_response))
        self.assertIn("Filters are remembered for this browser session.", self._text(remembered_response))
        self.assertIn('value="all" selected', self._text(reset_response))
        self.assertNotIn('value="alice"', self._text(reset_response))

    def test_exceptions_page_supports_database_pagination(self):
        self._login("superadmin")
        for index in range(30):
            self.app.state.exception_rule_repo.upsert_rule(
                rule_type="skip_user_disable",
                match_value=f"user-{index:02d}",
                notes=f"exception-note-{index:02d}",
            )

        with patch("sync_app.providers.source.wecom.WeComAPI") as mock_wecom:
            mock_wecom.return_value.get_department_list.return_value = []
            response = self._route("/exceptions", "GET")(
                self._request("/exceptions", query={"page_number": "2"})
            )
        self.assertEqual(response.status_code, 200)
        response_text = self._text(response)
        self.assertIn("exception-note-25", response_text)
        self.assertNotIn("exception-note-24", response_text)
        self.assertIn("page 2 / 2", response_text)

    def test_exceptions_page_remembers_filters_for_current_session(self):
        self._login("superadmin")
        self.app.state.exception_rule_repo.upsert_rule(
            rule_type="skip_user_disable",
            match_value="alice",
            notes="remembered-exception",
        )

        with patch("sync_app.providers.source.wecom.WeComAPI") as mock_wecom:
            mock_wecom.return_value.get_department_list.return_value = []
            first_response = self._route("/exceptions", "GET")(
                self._request("/exceptions", query={"q": "alice", "rule_type": "skip_user_disable", "status": "enabled"})
            )
            remembered_response = self._route("/exceptions", "GET")(self._request("/exceptions"))
            reset_response = self._route("/exceptions", "GET")(
                self._request("/exceptions", query={"clear_filters": "1"})
            )
        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(remembered_response.status_code, 200)
        self.assertEqual(reset_response.status_code, 200)
        remembered_text = self._text(remembered_response)
        self.assertIn('value="alice"', remembered_text)
        self.assertIn('value="skip_user_disable" selected', remembered_text)
        self.assertIn("Filters are remembered for this browser session.", remembered_text)
        self.assertNotIn('value="alice"', self._text(reset_response))

    def test_super_admin_config_page_masks_secrets_and_dashboard_shows_security_warning(self):
        self._login("superadmin")

        response = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("Source Provider", text)
        self.assertIn('name="source_provider"', text)
        self.assertNotIn("secret-001", text)
        self.assertNotIn("Password123!", text)
        self.assertNotIn("ChangeMe123!", text)
        self.assertIn("Leave blank to keep current", text)
        self.assertIn("Secure Cookie Policy", text)

        dashboard = self._route("/dashboard", "GET")(self._request("/dashboard"))
        self.assertEqual(dashboard.status_code, 200)
        dashboard_text = self._text(dashboard)
        self.assertIn("LDAPS certificate validation is disabled.", dashboard_text)
        self.assertIn("Default password is still a sample or weak password. Replace it immediately.", dashboard_text)

    def test_config_page_separates_optional_notification_settings(self):
        self._login("superadmin")

        response = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("Optional Notifications", text)
        self.assertIn("does not block preflight, dry run, or apply", text)

    def test_config_page_includes_sync_scope_and_ou_mapping_controls(self):
        self._login("superadmin")

        response = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("OU Filter And Root Mapping", text)
        self.assertIn('name="source_root_unit_ids"', text)
        self.assertIn('name="directory_root_ou_path"', text)
        self.assertIn('name="disabled_users_ou_path"', text)
        self.assertIn('name="custom_group_ou_path"', text)

    def test_config_source_unit_catalog_returns_department_tree(self):
        self._login("superadmin")
        config_page = self._route("/config", "GET")(self._request("/config"))
        csrf_match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(config_page))
        self.assertIsNotNone(csrf_match)

        class FakeSourceProvider:
            def list_departments(self):
                return [
                    DepartmentNode(department_id=1, name="HQ", parent_id=0),
                    DepartmentNode(department_id=8, name="China", parent_id=1),
                ]

            def close(self):
                return None

        with patch("sync_app.web.app.build_source_provider", return_value=FakeSourceProvider()):
            response = self._route("/config/source-units/catalog", "POST")(
                self._request("/config/source-units/catalog", "POST"),
                csrf_token=csrf_match.group(1),
                source_provider="wecom",
                corpid="corp-001",
                agentid="10001",
                corpsecret="secret-001",
            )

        self.assertEqual(response.status_code, 200)
        payload = json.loads(self._text(response))
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["provider"], "WeCom")
        self.assertEqual(payload["items"][1]["department_id"], "8")
        self.assertEqual(payload["items"][1]["path_display"], "HQ / China")

    def test_config_source_unit_catalog_requires_new_secret_when_source_provider_changes(self):
        self._login("superadmin")
        config_page = self._route("/config", "GET")(self._request("/config"))
        csrf_match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(config_page))
        self.assertIsNotNone(csrf_match)

        response = self._route("/config/source-units/catalog", "POST")(
            self._request("/config/source-units/catalog", "POST"),
            csrf_token=csrf_match.group(1),
            source_provider="dingtalk",
            corpid="ding-app-key",
            agentid="50001",
            corpsecret="",
        )

        self.assertEqual(response.status_code, 200)
        payload = json.loads(self._text(response))
        self.assertFalse(payload["ok"])
        self.assertIn("AppSecret / Client Secret", payload["error"])

    def test_config_target_ou_catalog_returns_ou_tree(self):
        self._login("superadmin")
        config_page = self._route("/config", "GET")(self._request("/config"))
        csrf_match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(config_page))
        self.assertIsNotNone(csrf_match)

        class FakeTargetProvider:
            def list_organizational_units(self):
                return [
                    {"name": "example.local", "dn": "DC=example,DC=local", "path": [], "guid": ""},
                    {
                        "name": "Managed Users",
                        "dn": "OU=Managed Users,DC=example,DC=local",
                        "path": ["Managed Users"],
                        "guid": "12345678-1234-5678-1234-567812345678",
                    },
                ]

        with patch("sync_app.web.app.build_target_provider", return_value=FakeTargetProvider()):
            response = self._route("/config/target-ou/catalog", "POST")(
                self._request("/config/target-ou/catalog", "POST"),
                csrf_token=csrf_match.group(1),
                ldap_server="dc01.example.local",
                ldap_domain="example.local",
                ldap_username="administrator",
                ldap_password="Password123!",
                ldap_port=636,
                ldap_use_ssl="true",
                ldap_validate_cert="false",
                ldap_ca_cert_path="",
            )

        self.assertEqual(response.status_code, 200)
        payload = json.loads(self._text(response))
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["items"][1]["path_value"], "Managed Users")
        self.assertEqual(payload["items"][1]["guid"], "12345678-1234-5678-1234-567812345678")

    def test_dashboard_does_not_block_when_webhook_is_not_configured(self):
        current_org = self.app.state.organization_repo.get_default_organization_record()
        self.assertIsNotNone(current_org)
        values = self.app.state.org_config_repo.get_raw_config(
            current_org.org_id,
            config_path=str(self.config_path),
        )
        values["webhook_url"] = ""
        self.app.state.org_config_repo.save_config(
            current_org.org_id,
            values,
            config_path=str(self.config_path),
        )

        self._login("superadmin")
        response = self._route("/dashboard", "GET")(self._request("/dashboard"))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertNotIn("Webhook is not configured", text)
        self.assertIn("Required WeCom and LDAP settings are complete.", text)

    def test_config_page_surfaces_selected_provider_context_for_dingtalk(self):
        current_org = self.app.state.organization_repo.get_default_organization_record()
        self.assertIsNotNone(current_org)
        values = self.app.state.org_config_repo.get_raw_config(
            current_org.org_id,
            config_path=str(self.config_path),
        )
        values.update(
            {
                "source_provider": "dingtalk",
                "corpid": "ding-app-key",
                "corpsecret": "ding-app-secret",
                "agentid": "50001",
                "webhook_url": "https://oapi.dingtalk.com/robot/send?access_token=test",
            }
        )
        self.app.state.org_config_repo.save_config(
            current_org.org_id,
            values,
            config_path=str(self.config_path),
        )

        self._login("superadmin")
        response = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("DingTalk Connector Configuration", text)
        self.assertIn("Shared Page, Provider-Specific Fields", text)
        self.assertIn("Current provider", text)
        self.assertIn("DingTalk Source Connector", text)
        self.assertIn("AppKey / Client ID", text)
        webhook_group = re.search(r'(?s)<div class="form-group field-span-full" id="group-webhook_url">.*?</div>\s*</div>', text)
        self.assertIsNotNone(webhook_group)
        webhook_html = webhook_group.group(0)
        self.assertIn("DingTalk Bot Webhook", webhook_html)
        self.assertNotIn("WeCom Webhook", webhook_html)

    def test_config_page_persists_web_deployment_settings_and_reloaded_app_uses_them(self):
        self._login("superadmin")

        response = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(response))
        self.assertIsNotNone(match)

        submit_response = self._route("/config", "POST")(
            self._request("/config", "POST"),
            csrf_token=match.group(1),
            corpid="corp-001",
            agentid="10001",
            corpsecret="secret-001",
            webhook_url="https://example.invalid/cgi-bin/webhook/send?key=test",
            ldap_server="dc01.example.local",
            ldap_domain="example.local",
            ldap_username="administrator",
            ldap_password="Password123!",
            ldap_port=636,
            ldap_use_ssl="true",
            ldap_validate_cert="false",
            ldap_ca_cert_path="",
            default_password="ChangeMe123!",
            force_change_password="true",
            password_complexity="strong",
            schedule_time="03:00",
            retry_interval=60,
            max_retries=3,
            group_display_separator="-",
            group_recursive_enabled="true",
            managed_relation_cleanup_enabled="false",
            schedule_execution_mode="apply",
            web_bind_host="0.0.0.0",
            web_bind_port=8443,
            web_public_base_url="https://sync.example.com/",
            web_session_cookie_secure_mode="always",
            web_trust_proxy_headers="true",
            web_forwarded_allow_ips="10.0.0.1,10.0.0.2",
            brand_display_name="Directory Hub",
            brand_mark_text="DH",
            brand_attribution="微信公众号：大刘讲IT",
            user_ou_placement_strategy="wecom_primary_department",
            soft_excluded_groups="",
        )
        self.assertEqual(submit_response.status_code, 303)
        self.assertEqual(
            self.app.state.org_config_repo.get_raw_config("default", config_path=str(self.config_path))["source_provider"],
            "wecom",
        )
        self.assertEqual(self.app.state.settings_repo.get_value("web_bind_host", ""), "0.0.0.0")
        self.assertEqual(self.app.state.settings_repo.get_int("web_bind_port", 0), 8443)
        self.assertEqual(self.app.state.settings_repo.get_value("web_public_base_url", ""), "https://sync.example.com")
        self.assertEqual(self.app.state.settings_repo.get_value("web_session_cookie_secure_mode", ""), "always")
        self.assertTrue(self.app.state.settings_repo.get_bool("web_trust_proxy_headers", False))
        self.assertEqual(
            self.app.state.settings_repo.get_value("web_forwarded_allow_ips", ""),
            "10.0.0.1,10.0.0.2",
        )
        self.assertEqual(self.app.state.settings_repo.get_value("brand_display_name", ""), "Directory Hub")
        self.assertEqual(self.app.state.settings_repo.get_value("brand_mark_text", ""), "DH")
        self.assertEqual(self.app.state.settings_repo.get_value("brand_attribution", ""), "微信公众号：大刘讲IT")
        self.assertIn("Restart the web process", self.session["_flash"]["message"])

        reloaded_settings = resolve_web_runtime_settings(self.app.state.settings_repo)
        self.assertTrue(reloaded_settings["session_cookie_secure"])
        self.assertEqual(reloaded_settings["bind_host"], "0.0.0.0")
        self.assertEqual(reloaded_settings["bind_port"], 8443)
        self.assertEqual(reloaded_settings["public_base_url"], "https://sync.example.com")
        self.assertEqual(reloaded_settings["session_cookie_secure_mode"], "always")
        self.assertTrue(reloaded_settings["trust_proxy_headers"])
        self.assertEqual(reloaded_settings["forwarded_allow_ips"], "10.0.0.1,10.0.0.2")
        self.session = {}
        login = self._route("/login", "GET")(self._request("/login"))
        self.assertIn("Directory Hub", self._text(login))

    def test_config_preview_shows_pending_changes_and_confirm_save_persists_them(self):
        self._login("superadmin")

        response = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(response))
        self.assertIsNotNone(match)
        csrf_token = match.group(1)

        preview_response = self._route("/config/preview", "POST")(
            self._request("/config/preview", "POST"),
            csrf_token=csrf_token,
            **self._build_config_form_payload(
                ldap_server="dc02.example.local",
                schedule_execution_mode="dry_run",
                web_bind_port=8443,
                soft_excluded_groups="Domain Users\nHelpdesk",
            ),
        )
        self.assertEqual(preview_response.status_code, 200)
        preview_text = self._text(preview_response)
        self.assertIn("Pending Changes", preview_text)
        self.assertIn("dc01.example.local", preview_text)
        self.assertIn("dc02.example.local", preview_text)
        self.assertIn("preview_token", preview_text)

        preview_state = self.session.get("_config_preview")
        self.assertIsInstance(preview_state, dict)
        self.assertEqual(
            ((preview_state or {}).get("submission") or {}).get("settings_values", {}).get("schedule_execution_mode"),
            "dry_run",
        )

        confirm_match = re.search(r'name="preview_token" value="([^"]+)"', preview_text)
        self.assertIsNotNone(confirm_match)
        confirm_response = self._route("/config/confirm", "POST")(
            self._request("/config/confirm", "POST"),
            csrf_token=csrf_token,
            preview_token=confirm_match.group(1),
        )
        self.assertEqual(confirm_response.status_code, 303)
        self.assertEqual(confirm_response.headers["location"], "/config")
        self.assertIsNone(self.session.get("_config_preview"))
        self.assertEqual(self.app.state.settings_repo.get_int("web_bind_port", 0), 8443)
        self.assertEqual(
            self.app.state.settings_repo.get_value("schedule_execution_mode", "", org_id="default"),
            "dry_run",
        )
        self.assertEqual(
            self.app.state.org_config_repo.get_app_config("default", config_path=str(self.config_path)).ldap.server,
            "dc02.example.local",
        )

    def test_config_preview_formats_special_field_types_consistently(self):
        self._login("superadmin")

        response = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(response))
        self.assertIsNotNone(match)

        preview_response = self._route("/config/preview", "POST")(
            self._request("/config/preview", "POST"),
            csrf_token=match.group(1),
            **self._build_config_form_payload(
                password_complexity="medium",
                group_display_separator=" ",
                schedule_execution_mode="dry_run",
                source_root_unit_ids="2, 8",
                directory_root_ou_path="Managed Users/China",
                soft_excluded_groups="",
            ),
        )
        self.assertEqual(preview_response.status_code, 200)
        preview_text = self._text(preview_response)

        self.assertRegex(
            preview_text,
            re.compile(r"Password Complexity.*?Strong.*?Medium", re.DOTALL),
        )
        self.assertRegex(
            preview_text,
            re.compile(r"Group Separator.*?-.*?Space", re.DOTALL),
        )
        self.assertRegex(
            preview_text,
            re.compile(r"Scheduled Mode.*?Apply.*?Dry Run", re.DOTALL),
        )
        self.assertRegex(
            preview_text,
            re.compile(r"Source Root Unit IDs Filter.*?All departments.*?2,\s*8", re.DOTALL),
        )
        self.assertRegex(
            preview_text,
            re.compile(r"Target AD Root OU Path / DN.*?Domain root.*?Managed Users/China", re.DOTALL),
        )
        self.assertRegex(
            preview_text,
            re.compile(r"Soft Excluded Groups.*?Domain Users.*?None", re.DOTALL),
        )

    def test_config_page_persists_sync_scope_and_ou_mapping_settings(self):
        self._login("superadmin")

        response = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(response))
        self.assertIsNotNone(match)

        submit_response = self._route("/config", "POST")(
            self._request("/config", "POST"),
            csrf_token=match.group(1),
            **self._build_config_form_payload(
                source_root_unit_ids="2, 8",
                directory_root_ou_path="Managed Users/China",
                disabled_users_ou_path="Managed Users/Disabled Users",
                custom_group_ou_path="Managed Groups/Regional",
            ),
        )
        self.assertEqual(submit_response.status_code, 303)
        self.assertEqual(
            self.app.state.settings_repo.get_value("source_root_unit_ids", "", org_id="default"),
            "2, 8",
        )
        self.assertEqual(
            self.app.state.settings_repo.get_value("directory_root_ou_path", "", org_id="default"),
            "Managed Users/China",
        )
        self.assertEqual(
            self.app.state.settings_repo.get_value("disabled_users_ou_path", "", org_id="default"),
            "Managed Users/Disabled Users",
        )
        self.assertEqual(
            self.app.state.settings_repo.get_value("custom_group_ou_path", "", org_id="default"),
            "Managed Groups/Regional",
        )

    def test_config_preview_redirects_when_nothing_changed(self):
        self._login("superadmin")

        response = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(response))
        self.assertIsNotNone(match)

        preview_response = self._route("/config/preview", "POST")(
            self._request("/config/preview", "POST"),
            csrf_token=match.group(1),
            **self._build_config_form_payload(),
        )
        self.assertEqual(preview_response.status_code, 303)
        self.assertEqual(preview_response.headers["location"], "/config")
        self.assertEqual(self.session.get("_flash", {}).get("message"), "No configuration changes were detected")

    def test_super_admin_can_create_and_select_organization_with_separate_config_file(self):
        self._login("superadmin")

        response = self._route("/organizations", "GET")(self._request("/organizations"))
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(response))
        self.assertIsNotNone(match)
        csrf_token = match.group(1)
        asia_config_path = str((self.db_path.parent / "web_authz_asia.ini").resolve())

        response = self._route("/organizations", "POST")(
            self._request("/organizations", "POST"),
            csrf_token=csrf_token,
            org_id="asia",
            name="Asia Region",
            config_path_value=asia_config_path,
            description="regional tenant",
            is_enabled="1",
        )
        self.assertEqual(response.status_code, 303)
        organization = self.app.state.organization_repo.get_organization_record("asia")
        self.assertIsNotNone(organization)
        self.assertEqual(organization.config_path, asia_config_path)

        response = self._route("/organizations/{org_id}/select", "POST")(
            self._request("/organizations/asia/select", "POST"),
            org_id="asia",
            csrf_token=csrf_token,
            return_url="/dashboard",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(self.session.get("selected_org_id"), "asia")

        config_page = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(config_page.status_code, 200)
        self.assertIn("Asia Region", self._text(config_page))
        config_match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(config_page))
        self.assertIsNotNone(config_match)

        response = self._route("/config", "POST")(
            self._request("/config", "POST"),
            csrf_token=config_match.group(1),
            corpid="corp-asia",
            agentid="20002",
            corpsecret="secret-asia",
            webhook_url="https://example.invalid/webhook/asia",
            ldap_server="dc01.asia.example.local",
            ldap_domain="asia.example.local",
            ldap_username="asia-admin",
            ldap_password="Password123!",
            ldap_port=636,
            ldap_use_ssl="true",
            ldap_validate_cert="true",
            ldap_ca_cert_path="",
            default_password="ChangeMe123!",
            force_change_password="true",
            password_complexity="strong",
            schedule_time="03:00",
            retry_interval=60,
            max_retries=3,
            group_display_separator="-",
            group_recursive_enabled="true",
            managed_relation_cleanup_enabled="false",
            schedule_execution_mode="apply",
            web_bind_host="127.0.0.1",
            web_bind_port=8000,
            web_public_base_url="",
            web_session_cookie_secure_mode="auto",
            web_trust_proxy_headers="false",
            web_forwarded_allow_ips="127.0.0.1",
            user_ou_placement_strategy="wecom_primary_department",
            soft_excluded_groups="",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            self.app.state.org_config_repo.get_editable_config("asia", config_path=asia_config_path)["corpid"],
            "corp-asia",
        )

    def test_super_admin_can_export_organization_bundle(self):
        self._login("superadmin")
        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path="",
            description="regional tenant",
            is_enabled=True,
        )
        self.app.state.org_config_repo.save_config(
            "asia",
            {
                "corpid": "corp-asia",
                "agentid": "20002",
                "corpsecret": "secret-asia",
                "webhook_url": "https://example.invalid/webhook/asia",
                "ldap_server": "dc01.asia.local",
                "ldap_domain": "asia.local",
                "ldap_username": "asia-admin",
                "ldap_password": "Password123!",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": True,
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
            },
            config_path="",
        )
        self.app.state.connector_repo.upsert_connector(
            connector_id="asia-main",
            org_id="asia",
            name="Asia Main",
            config_path="",
            ldap_server="dc01.asia.local",
            ldap_domain="asia.local",
            ldap_username="asia-admin",
            ldap_password="Password123!",
            ldap_use_ssl=True,
            ldap_port=636,
            ldap_validate_cert=True,
            default_password="ChangeMe123!",
            force_change_password=True,
            password_complexity="strong",
            root_department_ids=[2],
            username_template="{userid}",
        )
        self.app.state.attribute_mapping_repo.upsert_rule(
            org_id="asia",
            connector_id="asia-main",
            direction="wecom_to_ad",
            source_field="position",
            target_field="title",
            transform_template="{value}",
            sync_mode="replace",
            is_enabled=True,
            notes="title mapping",
        )
        self.app.state.exclusion_repo.upsert_rule(
            org_id="asia",
            rule_type="protect",
            protection_level="hard",
            match_type="samaccountname",
            match_value="Domain Admins",
            display_name="Domain Admins",
            is_enabled=True,
            source="unit_test",
        )

        response = self._route("/organizations/{org_id}/export", "GET")(
            self._request("/organizations/asia/export"),
            org_id="asia",
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn('attachment; filename="asia-config-bundle.json"', response.headers.get("content-disposition", ""))
        payload = json.loads(self._text(response))
        self.assertEqual(payload["organization"]["org_id"], "asia")
        self.assertEqual(payload["organization_config"]["corpid"], "corp-asia")
        self.assertEqual(payload["connectors"][0]["connector_id"], "asia-main")
        self.assertEqual(payload["attribute_mappings"][0]["target_field"], "title")
        self.assertTrue(
            any(rule["match_value"] == "Domain Admins" for rule in payload["group_exclusion_rules"])
        )

    def test_super_admin_can_import_organization_bundle(self):
        self._login("superadmin")

        response = self._route("/organizations", "GET")(self._request("/organizations"))
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(response))
        self.assertIsNotNone(match)

        bundle = {
            "bundle_type": "organization_config_bundle",
            "bundle_version": 1,
            "organization": {
                "org_id": "source-org",
                "name": "Source Organization",
                "description": "import source",
                "is_enabled": True,
                "config_path": "",
            },
            "organization_config": {
                "corpid": "corp-imported",
                "agentid": "30003",
                "corpsecret": "secret-imported",
                "webhook_url": "https://example.invalid/webhook/imported",
                "ldap_server": "dc01.imported.local",
                "ldap_domain": "imported.local",
                "ldap_username": "imported-admin",
                "ldap_password": "Password123!",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": True,
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
            },
            "org_settings": {
                "group_display_separator": "/",
                "attribute_mapping_enabled": True,
            },
            "connectors": [
                {
                    "connector_id": "import-main",
                    "name": "Imported Main",
                    "config_path": "",
                    "ldap_server": "dc01.imported.local",
                    "ldap_domain": "imported.local",
                    "ldap_username": "imported-admin",
                    "ldap_password": "Password123!",
                    "ldap_use_ssl": True,
                    "ldap_port": 636,
                    "ldap_validate_cert": True,
                    "ldap_ca_cert_path": "",
                    "default_password": "ChangeMe123!",
                    "force_change_password": True,
                    "password_complexity": "strong",
                    "root_department_ids": [3],
                    "username_template": "{userid}",
                    "disabled_users_ou": "",
                    "group_type": "security",
                    "group_mail_domain": "",
                    "custom_group_ou_path": "",
                    "managed_tag_ids": ["101"],
                    "managed_external_chat_ids": [],
                    "is_enabled": True,
                }
            ],
            "attribute_mappings": [
                {
                    "connector_id": "import-main",
                    "direction": "wecom_to_ad",
                    "source_field": "mobile",
                    "target_field": "mobile",
                    "transform_template": "{value}",
                    "sync_mode": "replace",
                    "is_enabled": True,
                    "notes": "mobile mapping",
                }
            ],
            "group_exclusion_rules": [
                {
                    "rule_type": "exclude",
                    "protection_level": "soft",
                    "match_type": "samaccountname",
                    "match_value": "LegacyGroup",
                    "display_name": "LegacyGroup",
                    "is_enabled": True,
                    "source": "import",
                }
            ],
        }

        response = self._route("/organizations/import", "POST")(
            self._request("/organizations/import", "POST"),
            csrf_token=match.group(1),
            bundle_json=json.dumps(bundle),
            target_org_id="europe",
            replace_existing="1",
        )
        self.assertEqual(response.status_code, 303)

        organization = self.app.state.organization_repo.get_organization_record("europe")
        self.assertIsNotNone(organization)
        self.assertEqual(organization.name, "Source Organization")
        self.assertEqual(
            self.app.state.org_config_repo.get_editable_config("europe", config_path="")["corpid"],
            "corp-imported",
        )
        self.assertEqual(
            self.app.state.settings_repo.get_value("group_display_separator", "-", org_id="europe"),
            "/",
        )
        self.assertTrue(
            self.app.state.settings_repo.get_bool("attribute_mapping_enabled", False, org_id="europe")
        )
        connector = self.app.state.connector_repo.get_connector_record("import-main", org_id="europe")
        self.assertIsNotNone(connector)
        self.assertEqual(connector.ldap_server, "dc01.imported.local")
        mapping = self.app.state.attribute_mapping_repo.list_rule_records(org_id="europe")
        self.assertEqual(len(mapping), 1)
        self.assertEqual(mapping[0].direction, "source_to_ad")
        self.assertEqual(mapping[0].target_field, "mobile")
        group_rules = self.app.state.exclusion_repo.list_rule_records(org_id="europe")
        self.assertTrue(any(rule.match_value == "LegacyGroup" for rule in group_rules))
        self.assertEqual(
            self.app.state.org_config_repo.get_editable_config("default", config_path=str(self.config_path))["corpid"],
            "corp-001",
        )

    def test_config_page_scopes_group_exclusion_rules_to_selected_organization(self):
        self._login("superadmin")
        asia_config_path = str((self.db_path.parent / "web_authz_config_asia.ini").resolve())
        save_editable_config(
            {
                "corpid": "corp-asia",
                "agentid": "20001",
                "corpsecret": "secret-asia",
                "webhook_url": "",
                "ldap_server": "dc01.asia.example.local",
                "ldap_domain": "asia.example.local",
                "ldap_username": "administrator",
                "ldap_password": "Password123!",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": True,
                "ldap_ca_cert_path": "",
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
                "schedule_time": "03:00",
                "retry_interval": 60,
                "max_retries": 3,
            },
            config_path=asia_config_path,
        )
        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=asia_config_path,
            description="Asia tenant",
            is_enabled=True,
        )
        self.session["selected_org_id"] = "asia"

        config_page = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(config_page.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(config_page))
        self.assertIsNotNone(match)

        response = self._route("/config", "POST")(
            self._request("/config", "POST"),
            csrf_token=match.group(1),
            corpid="corp-asia",
            agentid="20001",
            corpsecret="",
            webhook_url="",
            ldap_server="dc01.asia.example.local",
            ldap_domain="asia.example.local",
            ldap_username="administrator",
            ldap_password="",
            ldap_port=636,
            ldap_use_ssl="true",
            ldap_validate_cert="true",
            ldap_ca_cert_path="",
            default_password="",
            force_change_password="true",
            password_complexity="strong",
            schedule_time="03:00",
            retry_interval=60,
            max_retries=3,
            group_display_separator="-",
            group_recursive_enabled="true",
            managed_relation_cleanup_enabled="false",
            schedule_execution_mode="apply",
            web_bind_host="127.0.0.1",
            web_bind_port=8000,
            web_public_base_url="",
            web_session_cookie_secure_mode="auto",
            web_trust_proxy_headers="false",
            web_forwarded_allow_ips="127.0.0.1",
            user_ou_placement_strategy="wecom_primary_department",
            soft_excluded_groups="Asia Shared\nAsia Vendors",
        )
        self.assertEqual(response.status_code, 303)

        asia_excluded = self.app.state.exclusion_repo.list_soft_excluded_group_names(
            enabled_only=False,
            org_id="asia",
        )
        default_excluded = self.app.state.exclusion_repo.list_soft_excluded_group_names(
            enabled_only=False,
            org_id="default",
        )
        self.assertIn("Asia Shared", asia_excluded)
        self.assertIn("Asia Vendors", asia_excluded)
        self.assertNotIn("Asia Shared", default_excluded)
        asia_protected = self.app.state.exclusion_repo.list_rules(
            rule_type="protect",
            protection_level="hard",
            org_id="asia",
        )
        self.assertGreater(len(asia_protected), 0)

    def test_advanced_sync_page_scopes_connectors_to_selected_organization(self):
        self._login("superadmin")
        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=str((self.db_path.parent / "web_authz_asia_scope.ini").resolve()),
            description="",
            is_enabled=True,
        )
        self.app.state.connector_repo.upsert_connector(
            connector_id="hq",
            org_id="default",
            name="HQ Connector",
            config_path="config.hq.ini",
            root_department_ids=[1],
        )
        self.app.state.connector_repo.upsert_connector(
            connector_id="asia",
            org_id="asia",
            name="Asia Connector",
            config_path="config.asia.ini",
            root_department_ids=[2],
        )
        self.session["selected_org_id"] = "asia"

        response = self._route("/advanced-sync", "GET")(self._request("/advanced-sync"))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("Asia Connector", text)
        self.assertNotIn("HQ Connector", text)

    def test_mappings_page_scopes_bindings_and_overrides_to_selected_organization(self):
        self._login("superadmin")
        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=str((self.db_path.parent / "web_authz_mappings_asia.ini").resolve()),
            description="",
            is_enabled=True,
        )
        self.app.state.user_binding_repo.upsert_binding(
            "alice",
            "alice.default",
            org_id="default",
            source="manual",
            notes="default-binding",
            preserve_manual=False,
        )
        self.app.state.user_binding_repo.upsert_binding(
            "alice",
            "alice.asia",
            org_id="asia",
            source="manual",
            notes="asia-binding",
            preserve_manual=False,
        )
        self.app.state.department_override_repo.upsert_override(
            "alice",
            "1001",
            org_id="default",
            notes="default-override",
        )
        self.app.state.department_override_repo.upsert_override(
            "alice",
            "2002",
            org_id="asia",
            notes="asia-override",
        )
        self.session["selected_org_id"] = "asia"

        with patch("sync_app.providers.source.wecom.WeComAPI") as mock_wecom:
            mock_wecom.return_value.get_department_list.return_value = [{"id": 2002, "name": "Asia Dept"}]
            response = self._route("/mappings", "GET")(self._request("/mappings"))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("alice.asia", text)
        self.assertIn("asia-override", text)
        self.assertNotIn("alice.default", text)
        self.assertNotIn("default-override", text)

    def test_exceptions_page_scopes_rules_to_selected_organization(self):
        self._login("superadmin")
        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=str((self.db_path.parent / "web_authz_exceptions_asia.ini").resolve()),
            description="",
            is_enabled=True,
        )
        self.app.state.exception_rule_repo.upsert_rule(
            rule_type="skip_user_disable",
            match_value="alice",
            org_id="default",
            notes="default-exception",
        )
        self.app.state.exception_rule_repo.upsert_rule(
            rule_type="skip_user_disable",
            match_value="alice",
            org_id="asia",
            notes="asia-exception",
        )
        self.session["selected_org_id"] = "asia"

        with patch("sync_app.providers.source.wecom.WeComAPI") as mock_wecom:
            mock_wecom.return_value.get_department_list.return_value = []
            response = self._route("/exceptions", "GET")(self._request("/exceptions"))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("asia-exception", text)
        self.assertNotIn("default-exception", text)

    def test_conflicts_page_scopes_conflicts_to_selected_organization(self):
        self._login("superadmin")
        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=str((self.db_path.parent / "web_authz_conflicts_asia.ini").resolve()),
            description="",
            is_enabled=True,
        )
        self.app.state.job_repo.create_job(
            "job-default-conflict",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="default",
        )
        self.app.state.job_repo.create_job(
            "job-asia-conflict",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="asia",
        )
        self.app.state.conflict_repo.add_conflict(
            job_id="job-default-conflict",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="default-conflict",
            resolution_hint="resolve manually",
        )
        self.app.state.conflict_repo.add_conflict(
            job_id="job-asia-conflict",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="asia-conflict",
            resolution_hint="resolve manually",
        )
        self.session["selected_org_id"] = "asia"

        response = self._route("/conflicts", "GET")(self._request("/conflicts"))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("asia-conflict", text)
        self.assertNotIn("default-conflict", text)

    def test_run_job_uses_selected_organization_context(self):
        self._login("superadmin")
        asia_config_path = str((self.db_path.parent / "web_authz_run_asia.ini").resolve())
        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=asia_config_path,
            description="",
            is_enabled=True,
        )
        self.session["selected_org_id"] = "asia"

        jobs_page = self._route("/jobs", "GET")(self._request("/jobs"))
        self.assertEqual(jobs_page.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(jobs_page))
        self.assertIsNotNone(match)

        with patch.object(self.app.state.sync_runner, "launch", return_value=(True, "Synchronization job started")) as mock_launch:
            response = self._route("/jobs/run", "POST")(
                self._request("/jobs/run", "POST"),
                csrf_token=match.group(1),
                mode="dry_run",
            )
        self.assertEqual(response.status_code, 303)
        mock_launch.assert_called_once_with(
            mode="dry_run",
            actor_username="superadmin",
            org_id="asia",
            config_path=asia_config_path,
        )

    def test_dashboard_renders_global_organization_switcher_when_multiple_orgs_exist(self):
        self._login("superadmin")
        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=str((self.db_path.parent / "web_authz_dashboard_asia.ini").resolve()),
            description="",
            is_enabled=True,
        )

        response = self._route("/dashboard", "GET")(self._request("/dashboard"))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn('action="/organization-switch"', text)
        self.assertIn("Asia Region", text)
        self.assertIn('action="/ui-mode"', text)

    def test_scope_guidance_is_visible_on_dashboard_config_and_organizations_pages(self):
        self._login("superadmin")
        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=str((self.db_path.parent / "web_authz_scope_asia.ini").resolve()),
            description="",
            is_enabled=True,
        )
        self.session["selected_org_id"] = "asia"

        dashboard_response = self._route("/dashboard", "GET")(self._request("/dashboard"))
        self.assertEqual(dashboard_response.status_code, 200)
        dashboard_text = self._text(dashboard_response)
        self.assertIn("Scope Guide", dashboard_text)
        self.assertIn("Global Scope", dashboard_text)
        self.assertIn("Organization Scope", dashboard_text)

        config_response = self._route("/config", "GET")(self._request("/config"))
        self.assertEqual(config_response.status_code, 200)
        config_text = self._text(config_response)
        self.assertIn("Organization Settings", config_text)
        self.assertIn("Global Settings", config_text)
        self.assertIn("Web Deployment", config_text)

        organizations_response = self._route("/organizations", "GET")(self._request("/organizations"))
        self.assertEqual(organizations_response.status_code, 200)
        organizations_text = self._text(organizations_response)
        self.assertIn("Global Scope", organizations_text)
        self.assertIn("Organization List", organizations_text)
        self.assertIn("independent source-directory tenants", organizations_text)

    def test_favicon_route_serves_icon_file(self):
        response = self._route("/favicon.ico", "GET")(self._request("/favicon.ico"))
        self.assertEqual(response.status_code, 200)
        self.assertTrue(str(getattr(response, "path", "")).endswith("icon.ico"))

    def test_login_page_can_switch_to_simplified_chinese(self):
        response = self._route("/login", "GET")(self._request("/login", query={"lang": "zh-CN"}))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("AD 组织同步", text)
        self.assertIn("登录以管理你的目录同步运维。", text)
        self.assertIn("登录", text)
        self.assertEqual(self.session.get("ui_language"), "zh-CN")

    def test_dashboard_uses_session_language_for_translated_navigation(self):
        self._login("superadmin")
        self.session["ui_language"] = "zh-CN"

        dashboard = self._route("/dashboard", "GET")(self._request("/dashboard"))
        self.assertEqual(dashboard.status_code, 200)
        text = self._text(dashboard)
        self.assertIn(">仪表盘<", text)
        self.assertIn(">任务<", text)
        self.assertIn("配置校验", text)
        self.assertIn("AD 组织同步", text)


    def test_dashboard_defaults_to_basic_mode_and_can_switch_to_advanced_mode(self):
        self._login("superadmin")
        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=str((self.db_path.parent / "web_authz_mode_asia.ini").resolve()),
            description="",
            is_enabled=True,
        )

        dashboard = self._route("/dashboard", "GET")(self._request("/dashboard"))
        self.assertEqual(dashboard.status_code, 200)
        dashboard_text = self._text(dashboard)
        self.assertIn('action="/ui-mode"', dashboard_text)
        self.assertNotIn('href="/advanced-sync"', dashboard_text)
        self.assertNotIn('href="/organizations"', dashboard_text)
        match = re.search(r'name="csrf_token" value="([^"]+)"', dashboard_text)
        self.assertIsNotNone(match)

        response = self._route("/ui-mode", "POST")(
            self._request("/ui-mode", "POST"),
            csrf_token=match.group(1),
            ui_mode="advanced",
            return_url="/dashboard",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(self.session.get("ui_mode"), "advanced")

        advanced_dashboard = self._route("/dashboard", "GET")(self._request("/dashboard"))
        advanced_text = self._text(advanced_dashboard)
        self.assertIn('href="/advanced-sync"', advanced_text)
        self.assertIn('href="/organizations"', advanced_text)

    def test_preflight_run_persists_live_results_on_dashboard(self):
        self._login("superadmin")
        dashboard = self._route("/dashboard", "GET")(self._request("/dashboard"))
        self.assertEqual(dashboard.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(dashboard))
        self.assertIsNotNone(match)

        with patch("sync_app.web.app.test_source_connection", return_value=(True, "WeCom connection succeeded (self-built app), departments: 1")), patch(
            "sync_app.web.app.test_ldap_connection",
            return_value=(True, "LDAP connection succeeded (auth: NTLM, protocol: LDAPS)"),
        ):
            response = self._route("/preflight/run", "POST")(
                self._request("/preflight/run", "POST"),
                csrf_token=match.group(1),
                return_url="/dashboard",
            )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")
        self.assertTrue(self.session.get("_preflight_snapshot"))

        refreshed_dashboard = self._route("/dashboard", "GET")(self._request("/dashboard"))
        dashboard_text = self._text(refreshed_dashboard)
        self.assertIn("Deployment Preflight", dashboard_text)
        self.assertIn("Live WeCom connection", dashboard_text)
        self.assertIn("Last live check", dashboard_text)

    def test_preflight_and_getting_started_use_selected_provider_context_for_dingtalk(self):
        current_org = self.app.state.organization_repo.get_default_organization_record()
        self.assertIsNotNone(current_org)
        values = self.app.state.org_config_repo.get_raw_config(
            current_org.org_id,
            config_path=str(self.config_path),
        )
        values.update(
            {
                "source_provider": "dingtalk",
                "corpid": "ding-app-key",
                "corpsecret": "ding-app-secret",
                "agentid": "50001",
            }
        )
        self.app.state.org_config_repo.save_config(
            current_org.org_id,
            values,
            config_path=str(self.config_path),
        )

        self._login("superadmin")
        dashboard = self._route("/dashboard", "GET")(self._request("/dashboard"))
        self.assertEqual(dashboard.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(dashboard))
        self.assertIsNotNone(match)

        with patch("sync_app.web.app.test_source_connection", return_value=(True, "DingTalk connection succeeded (generic), departments: 1")), patch(
            "sync_app.web.app.test_ldap_connection",
            return_value=(True, "LDAP connection succeeded (auth: NTLM, protocol: LDAPS)"),
        ):
            response = self._route("/preflight/run", "POST")(
                self._request("/preflight/run", "POST"),
                csrf_token=match.group(1),
                return_url="/dashboard",
            )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")

        refreshed_dashboard = self._route("/dashboard", "GET")(self._request("/dashboard"))
        dashboard_text = self._text(refreshed_dashboard)
        self.assertIn("Live DingTalk connection", dashboard_text)

        getting_started = self._route("/getting-started", "GET")(self._request("/getting-started"))
        self.assertEqual(getting_started.status_code, 200)
        getting_started_text = self._text(getting_started)
        self.assertIn("Complete the DingTalk and LDAP values for the current organization.", getting_started_text)
        self.assertIn("Live DingTalk connection", getting_started_text)

    def test_getting_started_page_renders_rollout_steps(self):
        self._login("superadmin")

        response = self._route("/getting-started", "GET")(self._request("/getting-started"))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("Recommended Rollout Steps", text)
        self.assertIn("Run live connectivity preflight", text)
        self.assertIn("Run the first dry run", text)
        self.assertIn("Latest Preflight Snapshot", text)

    def test_jobs_empty_state_guides_first_sync_run(self):
        self._login("superadmin")

        response = self._route("/jobs", "GET")(self._request("/jobs"))
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("Run Your First Dry Run", text)
        self.assertIn("Open Guided Setup", text)
        self.assertIn("Review Config", text)

    def test_login_page_defaults_to_browser_language_when_chinese_is_preferred(self):
        response = self._route("/login", "GET")(
            self._request("/login", headers={"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"})
        )
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("娆㈣繋鍥炴潵", text)
        self.assertIn("鐧诲綍", text)
        self.assertNotIn("AD Org Sync", text)
        self.assertIsNone(self.session.get("ui_language"))

    def test_login_page_defaults_to_english_for_non_chinese_browser_language(self):
        response = self._route("/login", "GET")(
            self._request("/login", headers={"Accept-Language": "fr-FR,fr;q=0.9,zh;q=0.8"})
        )
        self.assertEqual(response.status_code, 200)
        text = self._text(response)
        self.assertIn("AD Org Sync", text)
        self.assertIn("Sign In", text)
        self.assertNotIn("娆㈣繋鍥炴潵", text)
        self.assertIsNone(self.session.get("ui_language"))

def _patched_test_login_page_defaults_to_browser_language_when_chinese_is_preferred(self):
    response = self._route("/login", "GET")(
        self._request("/login", headers={"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"})
    )
    self.assertEqual(response.status_code, 200)
    text = self._text(response)
    self.assertIn('<html lang="zh-CN">', text)
    self.assertIn('class="topbar-segment is-active active">简体中文</a>', text)
    self.assertNotIn("AD Org Sync", text)
    self.assertIsNone(self.session.get("ui_language"))


def _patched_test_login_page_defaults_to_english_for_non_chinese_browser_language(self):
    response = self._route("/login", "GET")(
        self._request("/login", headers={"Accept-Language": "fr-FR,fr;q=0.9,zh;q=0.8"})
    )
    self.assertEqual(response.status_code, 200)
    text = self._text(response)
    self.assertIn('<html lang="en">', text)
    self.assertIn("AD Org Sync", text)
    self.assertIn("Sign In", text)
    self.assertNotIn('class="topbar-segment is-active active">简体中文</a>', text)
    self.assertIsNone(self.session.get("ui_language"))


WebAuthorizationTests.test_login_page_defaults_to_browser_language_when_chinese_is_preferred = (
    _patched_test_login_page_defaults_to_browser_language_when_chinese_is_preferred
)
WebAuthorizationTests.test_login_page_defaults_to_english_for_non_chinese_browser_language = (
    _patched_test_login_page_defaults_to_english_for_non_chinese_browser_language
)


if __name__ == "__main__":
    unittest.main()
