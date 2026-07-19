import re

from sync_app.web.authz import has_capability
from sync_app.web.configuration_ownership import (
    GLOBAL_SCOPE,
    LEGACY_CONFIG_FIELD_AUTHORITIES,
)
from sync_app.web.routes_config import CONFIG_SUBMISSION_FIELD_NAMES
from sync_app.web.navigation import PHASE7_LEGACY_GET_REDIRECTS
from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class ConfigurationInformationArchitectureTests(WebAuthzBaseTestCase):
    def test_every_legacy_config_field_has_one_canonical_authority(self):
        self.assertEqual(
            set(LEGACY_CONFIG_FIELD_AUTHORITIES),
            set(CONFIG_SUBMISSION_FIELD_NAMES),
        )
        self.assertTrue(
            all(
                authority.path.startswith("/")
                for authority in LEGACY_CONFIG_FIELD_AUTHORITIES.values()
            )
        )
        self.assertEqual(
            LEGACY_CONFIG_FIELD_AUTHORITIES["web_bind_host"].scope,
            GLOBAL_SCOPE,
        )
        self.assertEqual(
            LEGACY_CONFIG_FIELD_AUTHORITIES["default_password"].path,
            "/sync-policies/account-naming",
        )
        self.assertEqual(
            LEGACY_CONFIG_FIELD_AUTHORITIES["webhook_url"].path,
            "/operations-center/notifications",
        )

    def test_legacy_config_and_advanced_sync_redirect_with_query(self):
        self.assertEqual(
            PHASE7_LEGACY_GET_REDIRECTS["/config"],
            "/data-sources/connectors",
        )
        self.assertEqual(
            PHASE7_LEGACY_GET_REDIRECTS["/advanced-sync"],
            "/sync-policies/scope",
        )

    def test_account_creation_fields_have_one_visible_editor(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"

        account_naming = self._text(
            self._route("/sync-policies/account-naming", "GET")(
                self._request("/sync-policies/account-naming")
            )
        )
        connectors = self._text(
            self._route("/data-sources/connectors", "GET")(
                self._request("/data-sources/connectors")
            )
        )
        lifecycle = self._text(
            self._route("/sync-policies/lifecycle", "GET")(
                self._request("/sync-policies/lifecycle")
            )
        )

        for field in (
            "default_password",
            "force_change_password",
            "password_complexity",
        ):
            with self.subTest(field=field):
                self.assertEqual(
                    len(re.findall(rf'name="{field}"', account_naming)),
                    1,
                )
                self.assertNotIn(f'name="{field}"', connectors)
                self.assertNotIn(f'name="{field}"', lifecycle)

        self.assertIn("Account Naming Examples &amp; Preview", account_naming)
        drawer_start = account_naming.index('id="account-naming-help"')
        sample_start = account_naming.index('name="sample_userid"')
        self.assertGreater(sample_start, drawer_start)
        self.assertIn("data-identity-drawer", account_naming)

    def test_account_creation_save_preserves_blank_secret_and_unrelated_fields(self):
        self._login("superadmin")
        before = self.app.state.org_config_repo.get_raw_config(
            "default",
            config_path=str(self.config_path),
        )
        path = "/sync-policies/account-naming/account-creation"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            default_password="",
            force_change_password="false",
            password_complexity="medium",
        )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/sync-policies/account-naming")
        after = self.app.state.org_config_repo.get_raw_config(
            "default",
            config_path=str(self.config_path),
        )
        self.assertEqual(after["default_password"], before["default_password"])
        self.assertEqual(after["ldap_server"], before["ldap_server"])
        self.assertEqual(after["schedule_time"], before["schedule_time"])
        self.assertFalse(after["force_change_password"])
        self.assertEqual(after["password_complexity"], "medium")
        log = next(
            item
            for item in self.app.state.audit_repo.list_recent_logs(limit=20)
            if item.action_type == "sync_policy.account_creation.update"
        )
        self.assertFalse(log.payload["default_password_updated"])
        self.assertNotIn(before["default_password"], str(log.payload))

    def test_connector_account_creation_save_preserves_connection_and_scope(self):
        self._login("superadmin")
        self.app.state.connector_repo.upsert_connector(
            connector_id="asia",
            org_id="default",
            name="Asia Domain",
            config_path="",
            ldap_server="dc01.asia.example.local",
            ldap_password="connector-secret",
            default_password="bootstrap-secret",
            force_change_password=True,
            password_complexity="strong",
            root_department_ids=[2, 8],
            username_strategy="userid",
            username_template="{userid}",
            disabled_users_ou="Disabled/Asia",
            managed_tag_ids=["1001"],
        )
        path = "/sync-policies/account-naming/account-creation"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            connector_id="asia",
            default_password="",
            force_change_password="false",
            password_complexity="medium",
        )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            response.headers["location"],
            "/sync-policies/account-naming?connector_id=asia",
        )
        saved = self.app.state.connector_repo.get_connector_record(
            "asia",
            org_id="default",
        )
        self.assertEqual(saved.ldap_server, "dc01.asia.example.local")
        self.assertEqual(saved.ldap_password, "connector-secret")
        self.assertEqual(saved.default_password, "bootstrap-secret")
        self.assertEqual(saved.root_department_ids, [2, 8])
        self.assertEqual(saved.username_strategy, "userid")
        self.assertEqual(saved.disabled_users_ou, "Disabled/Asia")
        self.assertEqual(saved.managed_tag_ids, ["1001"])
        self.assertFalse(saved.force_change_password)
        self.assertEqual(saved.password_complexity, "medium")

    def test_employee_service_shows_cross_scope_summaries_without_duplicate_editors(self):
        self._login("superadmin")
        self.app.state.settings_repo.set_value(
            "web_public_base_url",
            "https://sync.example.test",
            "string",
        )

        body = self._text(
            self._route("/system-management/employee-self-service", "GET")(
                self._request("/system-management/employee-self-service")
            )
        )

        self.assertIn("Organization Scope", body)
        self.assertIn("Global Scope", body)
        self.assertIn("https://sync.example.test/sspr/callback/dingtalk", body)
        self.assertIn('href="/data-sources/connectors"', body)
        self.assertIn('href="/system-management/deployment"', body)
        self.assertNotIn('name="web_public_base_url"', body)
        self.assertNotIn('name="corpsecret"', body)

    def test_global_configuration_uses_dedicated_permission_and_confirmation(self):
        self.assertTrue(has_capability("super_admin", "system.manage"))
        self.assertFalse(has_capability("operator", "system.manage"))
        self.assertFalse(has_capability("auditor", "system.manage"))

        self._login("superadmin")
        for path in (
            "/system-management/branding",
            "/system-management/deployment",
        ):
            with self.subTest(path=path):
                body = self._text(
                    self._route(path, "GET")(self._request(path))
                )
                self.assertIn("Global Scope", body)
                self.assertIn('data-confirm-require="GLOBAL"', body)

        database = self._text(
            self._route("/system-management/database", "GET")(
                self._request("/system-management/database")
            )
        )
        platform_accounts = self._text(
            self._route("/system-management/administrators", "GET")(
                self._request("/system-management/administrators")
            )
        )
        self.assertIn(
            'action="/system-management/database/backup"',
            database,
        )
        self.assertIn('data-confirm-require="GLOBAL"', database)
        self.assertIn(
            'action="/system-management/administrators"',
            platform_accounts,
        )
        self.assertIn('data-confirm-require="GLOBAL"', platform_accounts)

        self._login("operator1")
        for path in (
            "/system-management/branding",
            "/system-management/deployment",
        ):
            with self.subTest(role="operator", path=path):
                response = self._route(path, "GET")(self._request(path))
                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers["location"], "/dashboard")

    def test_lifecycle_workbench_is_the_only_queue_surface_in_refactored_areas(self):
        self._login("superadmin")
        lifecycle = self._text(
            self._route("/operations-center/lifecycle-queue", "GET")(
                self._request("/operations-center/lifecycle-queue")
            )
        )
        legacy = self._route("/advanced-sync", "GET")(
            self._request("/advanced-sync")
        )
        policy = self._text(
            self._route("/sync-policies/lifecycle", "GET")(
                self._request("/sync-policies/lifecycle")
            )
        )

        self.assertIn("Future Onboarding Queue", lifecycle)
        self.assertIn("Offboarding Grace Queue", lifecycle)
        self.assertIn("Replay Queue", lifecycle)
        self.assertEqual(legacy.status_code, 200)
        legacy_body = self._text(legacy)
        self.assertNotIn("Future Onboarding Queue", legacy_body)
        self.assertNotIn("Offboarding Grace Queue", legacy_body)
        self.assertNotIn("Replay Queue", legacy_body)
        self.assertNotIn("Future Onboarding Queue", policy)
        self.assertNotIn("Offboarding Grace Queue", policy)
        self.assertNotIn("Replay Queue", policy)
