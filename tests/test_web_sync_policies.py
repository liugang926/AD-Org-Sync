import re
from unittest.mock import patch

from sync_app.services.typed_settings import AdvancedSyncPolicySettings
from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class WebSyncPolicyTests(WebAuthzBaseTestCase):
    def _seed_policy_fixture(self):
        snapshot_id = self.app.state.source_directory_repo.start_refresh(
            org_id="default",
            provider_id="wecom",
            created_by="superadmin",
        )
        self.app.state.source_directory_repo.replace_snapshot(
            snapshot_id,
            departments=[
                {
                    "source_department_id": "2",
                    "name": "Asia",
                    "parent_department_id": "0",
                    "path_ids": ["2"],
                    "path_names": ["Asia"],
                }
            ],
            users=[
                {
                    "source_user_id": "alice",
                    "display_name": "Alice",
                    "employee_id": "E001",
                    "department_ids": ["2"],
                    "department_names": ["Asia"],
                    "is_active": True,
                    "raw_payload": {"userid": "alice", "employee_id": "E001"},
                    "search_text": "Alice E001",
                }
            ],
            fields=[],
            fingerprint="phase5-policy-fixture",
        )
        self.app.state.source_directory_repo.save_scope_selection(
            org_id="default",
            provider_id="wecom",
            scope_type="full",
            username_strategy="employee_id",
            username_template="{employee_id}",
            source_field="employee_id",
            snapshot_id=snapshot_id,
            requested_by="superadmin",
        )
        self.app.state.connector_repo.upsert_connector(
            connector_id="asia",
            org_id="default",
            name="Asia Domain",
            config_path="",
            ldap_server="dc01.asia.example.local",
            ldap_domain="asia.example.local",
            ldap_username="svc-sync",
            ldap_password="connector-secret",
            default_password="bootstrap-secret",
            root_department_ids=[2],
            username_strategy="userid",
            username_collision_policy="append_employee_id",
            username_template="{userid}",
            disabled_users_ou="Disabled/Asia",
            group_mail_domain="groups.example.com",
            managed_tag_ids=["1001"],
            is_enabled=True,
        )
        return snapshot_id

    def test_canonical_policy_pages_are_task_focused_and_legacy_page_remains(self):
        self._login("superadmin")
        self._seed_policy_fixture()
        paths = (
            "/sync-policies/scope",
            "/sync-policies/account-naming",
            "/sync-policies/attribute-mappings",
            "/sync-policies/department-ou-routing",
            "/sync-policies/group-rules",
            "/sync-policies/lifecycle",
            "/sync-policies/security",
        )

        for path in paths:
            with self.subTest(path=path):
                response = self._route(path, "GET")(self._request(path))
                self.assertEqual(response.status_code, 200)
                body = self._text(response)
                self.assertEqual(len(re.findall(r'class="button"(?:\s|>)', body)), 1)
                self.assertIn("Current status", body)
                self.assertIn("Blocking reason", body)
                self.assertIn("Next step", body)
                for table in re.findall(r"<table[^>]*>(.*?)</table>", body, re.S):
                    self.assertLessEqual(len(re.findall(r"<th(?:\s|>)", table)), 8)

        landing = self._route("/sync-policies", "GET")(
            self._request("/sync-policies")
        )
        self.assertEqual(landing.status_code, 307)
        self.assertEqual(landing.headers["location"], "/sync-policies/scope")
        legacy = self._route("/advanced-sync", "GET")(
            self._request("/advanced-sync")
        )
        self.assertEqual(legacy.status_code, 200)
        self.assertIn("Advanced Sync", self._text(legacy))
        self.assertIn("Pending Lifecycle Queue", self._text(legacy))

    def test_sync_scope_never_invokes_ad_verification_from_legacy_query(self):
        self._login("superadmin")
        self._seed_policy_fixture()
        path = "/sync-policies/scope"

        with patch(
            "sync_app.web.app.build_target_provider",
            side_effect=AssertionError("sync scope must not build an AD provider"),
        ) as build_target_provider:
            response = self._route(path, "GET")(
                self._request(path, query={"verify_ad": "true"})
            )

        self.assertEqual(response.status_code, 200)
        build_target_provider.assert_not_called()

    def test_attribute_section_update_preserves_lifecycle_and_is_audited(self):
        self._login("superadmin")
        AdvancedSyncPolicySettings(
            offboarding_grace_days=21,
            future_onboarding_enabled=True,
            disable_circuit_breaker_percent=8.5,
        ).persist(self.app.state.settings_repo, org_id="default")
        path = "/sync-policies/attribute-mappings"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            attribute_mapping_enabled="1",
            write_back_enabled="1",
            connector_id="",
            direction="source_to_ad",
            source_field="",
            target_field="",
            transform_template="",
            sync_mode="replace",
            notes="",
            is_enabled=None,
        )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], path)
        saved = AdvancedSyncPolicySettings.load(
            self.app.state.settings_repo,
            org_id="default",
        )
        self.assertTrue(saved.attribute_mapping_enabled)
        self.assertTrue(saved.write_back_enabled)
        self.assertEqual(saved.offboarding_grace_days, 21)
        self.assertTrue(saved.future_onboarding_enabled)
        self.assertEqual(saved.disable_circuit_breaker_percent, 8.5)
        self.assertTrue(
            any(
                row.action_type == "sync_policy.attribute_mappings.update"
                for row in self.app.state.audit_repo.list_recent_logs(20)
            )
        )

    def test_invalid_csrf_does_not_update_policy(self):
        self._login("superadmin")
        path = "/sync-policies/security"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token="wrong",
            advanced_connector_routing_enabled="1",
            disable_circuit_breaker_enabled="1",
            disable_circuit_breaker_percent=1.0,
            disable_circuit_breaker_min_count=1,
            disable_circuit_breaker_requires_approval="1",
            first_sync_identity_claim_mode="review",
            connector_id="",
            force_change_password="",
            password_complexity="",
        )

        self.assertEqual(response.status_code, 303)
        saved = AdvancedSyncPolicySettings.load(
            self.app.state.settings_repo,
            org_id="default",
        )
        self.assertFalse(saved.disable_circuit_breaker_enabled)
        self.assertFalse(saved.advanced_connector_routing_enabled)

    def test_policy_pages_and_writes_require_config_capabilities(self):
        self._login("operator1")
        for path in (
            "/sync-policies/scope",
            "/sync-policies/account-naming",
            "/sync-policies/attribute-mappings",
            "/sync-policies/department-ou-routing",
            "/sync-policies/group-rules",
            "/sync-policies/lifecycle",
            "/sync-policies/security",
            "/sync-policies/releases",
        ):
            with self.subTest(path=path):
                response = self._route(path, "GET")(self._request(path))
                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers["location"], "/dashboard")

        security = self._route("/sync-policies/security", "POST")(
            self._request("/sync-policies/security", "POST"),
            csrf_token=self.session["_csrf_token"],
            advanced_connector_routing_enabled="1",
            disable_circuit_breaker_enabled="1",
            disable_circuit_breaker_percent=1.0,
            disable_circuit_breaker_min_count=1,
            disable_circuit_breaker_requires_approval="1",
            first_sync_identity_claim_mode="review",
            connector_id="",
            force_change_password="",
            password_complexity="",
        )
        self.assertEqual(security.status_code, 303)
        self.assertEqual(security.headers["location"], "/dashboard")
        saved = AdvancedSyncPolicySettings.load(
            self.app.state.settings_repo,
            org_id="default",
        )
        self.assertFalse(saved.disable_circuit_breaker_enabled)

    def test_connector_naming_update_preserves_connection_scope_group_and_lifecycle(self):
        self._login("superadmin")
        self._seed_policy_fixture()
        path = "/sync-policies/account-naming"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            policy_target="connector",
            connector_id="asia",
            username_strategy="employee_id",
            username_template="{employee_id}",
            username_collision_policy="append_userid",
            username_collision_template="",
            employee_id_attribute="",
        )

        self.assertEqual(response.status_code, 303)
        record = self.app.state.connector_repo.get_connector_record(
            "asia",
            org_id="default",
        )
        self.assertEqual(record.username_strategy, "employee_id")
        self.assertEqual(record.username_collision_policy, "append_userid")
        self.assertEqual(record.ldap_server, "dc01.asia.example.local")
        self.assertEqual(record.ldap_password, "connector-secret")
        self.assertEqual(record.default_password, "bootstrap-secret")
        self.assertEqual(record.root_department_ids, [2])
        self.assertEqual(record.managed_tag_ids, ["1001"])
        self.assertEqual(record.disabled_users_ou, "Disabled/Asia")

    def test_source_dynamic_field_naming_preserves_scope_and_is_audited(self):
        self._login("superadmin")
        self._seed_policy_fixture()
        path = "/sync-policies/account-naming"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            policy_target="source",
            connector_id="",
            username_strategy="userid",
            username_template="ignored for dynamic field",
            username_collision_policy="append_employee_id",
            username_collision_template="",
            employee_id_attribute="employee_number",
            source_field="tenant_staff_code",
        )

        self.assertEqual(response.status_code, 303)
        scope = self.app.state.source_directory_repo.get_scope_selection(
            org_id="default",
            provider_id="wecom",
        )
        self.assertEqual(scope["scope_type"], "full")
        self.assertEqual(scope["source_field"], "tenant_staff_code")
        self.assertEqual(scope["username_strategy"], "custom_template")
        self.assertEqual(scope["username_template"], "{tenant_staff_code}")
        self.assertEqual(
            self.app.state.settings_repo.get_value(
                "source_employee_id_attribute", "", org_id="default"
            ),
            "employee_number",
        )
        self.assertTrue(
            any(
                row.action_type == "sync_policy.account_naming.update"
                for row in self.app.state.audit_repo.list_recent_logs(20)
            )
        )

    def test_connector_policy_sections_preserve_secrets_and_emit_audit(self):
        self._login("superadmin")
        self._seed_policy_fixture()

        group_path = "/sync-policies/group-rules"
        group = self._route(group_path, "POST")(
            self._request(group_path, "POST"),
            csrf_token=self.session["_csrf_token"],
            custom_group_sync_enabled="1",
            managed_group_type="security",
            managed_group_mail_domain="org.example.com",
            custom_group_ou_path="Managed Groups",
            connector_id="asia",
            connector_group_type="distribution",
            connector_group_mail_domain="asia.example.com",
            connector_custom_group_ou_path="Groups/Asia",
            managed_tag_ids="1001,1002",
            managed_external_chat_ids="chat-1",
        )
        self.assertEqual(group.status_code, 303)

        lifecycle_path = "/sync-policies/lifecycle"
        lifecycle = self._route(lifecycle_path, "POST")(
            self._request(lifecycle_path, "POST"),
            csrf_token=self.session["_csrf_token"],
            offboarding_grace_days=14,
            offboarding_notify_managers="1",
            offboarding_lifecycle_enabled="1",
            rehire_restore_enabled="1",
            automatic_replay_enabled="1",
            future_onboarding_enabled="1",
            future_onboarding_start_field="hire_date",
            contractor_lifecycle_enabled="1",
            lifecycle_employment_type_field="employment_type",
            contractor_end_field="contract_end_date",
            lifecycle_sponsor_field="sponsor_userid",
            contractor_type_values="contractor,vendor",
            connector_id="asia",
            disabled_users_ou="Disabled/Reviewed",
        )
        self.assertEqual(lifecycle.status_code, 303)

        security_path = "/sync-policies/security"
        security = self._route(security_path, "POST")(
            self._request(security_path, "POST"),
            csrf_token=self.session["_csrf_token"],
            advanced_connector_routing_enabled="1",
            disable_circuit_breaker_enabled="1",
            disable_circuit_breaker_percent=7.5,
            disable_circuit_breaker_min_count=12,
            disable_circuit_breaker_requires_approval="1",
            first_sync_identity_claim_mode="review",
            connector_id="asia",
            force_change_password="false",
            password_complexity="reviewed-strong",
        )
        self.assertEqual(security.status_code, 303)

        record = self.app.state.connector_repo.get_connector_record(
            "asia", org_id="default"
        )
        self.assertEqual(record.ldap_password, "connector-secret")
        self.assertEqual(record.default_password, "bootstrap-secret")
        self.assertEqual(record.root_department_ids, [2])
        self.assertEqual(record.username_strategy, "userid")
        self.assertEqual(record.group_type, "distribution")
        self.assertEqual(record.managed_tag_ids, ["1001", "1002"])
        self.assertEqual(record.disabled_users_ou, "Disabled/Reviewed")
        self.assertFalse(record.force_change_password)
        self.assertEqual(record.password_complexity, "reviewed-strong")
        audit_actions = {
            row.action_type for row in self.app.state.audit_repo.list_recent_logs(30)
        }
        self.assertTrue(
            {
                "sync_policy.group_rules.update",
                "sync_policy.lifecycle.update",
                "sync_policy.security.update",
            }.issubset(audit_actions)
        )

    def test_foreign_connector_cannot_be_updated_from_selected_organization(self):
        self._login("superadmin")
        self.app.state.organization_repo.upsert_organization(
            org_id="other",
            name="Other Organization",
            config_path="other.ini",
            is_enabled=True,
        )
        self.app.state.connector_repo.upsert_connector(
            connector_id="foreign",
            org_id="other",
            name="Foreign Connector",
            config_path="",
            ldap_server="foreign.example.local",
            ldap_password="foreign-secret",
            is_enabled=True,
        )
        path = "/sync-policies/account-naming"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            policy_target="connector",
            connector_id="foreign",
            username_strategy="employee_id",
            username_template="{employee_id}",
            username_collision_policy="append_userid",
            username_collision_template="",
            employee_id_attribute="",
            source_field="",
        )

        self.assertEqual(response.status_code, 303)
        foreign = self.app.state.connector_repo.get_connector_record(
            "foreign", org_id="other"
        )
        self.assertEqual(foreign.username_strategy, "custom_template")
        self.assertEqual(foreign.ldap_password, "foreign-secret")

    def test_scope_update_preserves_naming_and_can_update_connector_roots(self):
        self._login("superadmin")
        self._seed_policy_fixture()
        path = "/sync-policies/scope"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            scope_type="full",
            selected_department_ids=[],
            selected_source_user_ids=[],
            source_field="",
            username_template=None,
            employee_id_attribute=None,
            connector_id="asia",
            root_department_ids="2, 8",
            selection_mode="explicit",
            selection_search="",
            selection_department_id="",
            selection_status="",
            selection_employee_id_state="",
        )

        self.assertEqual(response.status_code, 303)
        scope = self.app.state.source_directory_repo.get_scope_selection(
            org_id="default",
            provider_id="wecom",
        )
        self.assertEqual(scope["username_strategy"], "employee_id")
        self.assertEqual(scope["username_template"], "{employee_id}")
        connector = self.app.state.connector_repo.get_connector_record(
            "asia",
            org_id="default",
        )
        self.assertEqual(connector.root_department_ids, [2, 8])
        self.assertEqual(connector.username_strategy, "userid")
        self.assertEqual(connector.ldap_password, "connector-secret")

    def test_canonical_routing_write_and_delete_keep_csrf_org_scope_and_audit(self):
        self._login("superadmin")
        self._seed_policy_fixture()
        path = "/sync-policies/department-ou-routing"

        saved = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            connector_id="asia",
            source_department_id="2",
            source_department_name="Asia",
            target_ou_path="Managed Users/Asia",
            apply_mode="subtree",
            notes="regional route",
            is_enabled="1",
        )
        self.assertEqual(saved.status_code, 303)
        self.assertEqual(saved.headers["location"], path)
        records = self.app.state.department_ou_mapping_repo.list_mapping_records(
            org_id="default"
        )
        self.assertEqual(len(records), 1)
        self.assertTrue(
            any(
                row.action_type == "sync_policy.department_ou_routing.update"
                for row in self.app.state.audit_repo.list_recent_logs(20)
            )
        )

        delete_path = path + "/{mapping_id}/delete"
        deleted = self._route(delete_path, "POST")(
            self._request(path + f"/{records[0].id}/delete", "POST"),
            mapping_id=records[0].id,
            csrf_token=self.session["_csrf_token"],
        )
        self.assertEqual(deleted.status_code, 303)
        self.assertEqual(deleted.headers["location"], path)
        self.assertEqual(
            self.app.state.department_ou_mapping_repo.list_mapping_records(
                org_id="default"
            ),
            [],
        )
        self.assertTrue(
            any(
                row.action_type == "sync_policy.department_ou_routing.delete"
                for row in self.app.state.audit_repo.list_recent_logs(20)
            )
        )

    def test_release_alias_uses_canonical_actions_and_existing_release_service(self):
        self._login("superadmin")
        path = "/sync-policies/releases"
        page = self._route(path, "GET")(self._request(path))
        body = self._text(page)
        self.assertEqual(page.status_code, 200)
        self.assertIn('action="/sync-policies/releases/publish"', body)
        self.assertNotIn('action="/config/releases/publish"', body)

        published = self._route(path + "/publish", "POST")(
            self._request(path + "/publish", "POST"),
            csrf_token=self.session["_csrf_token"],
            snapshot_name="Phase 5 baseline",
        )
        self.assertEqual(published.status_code, 303)
        self.assertEqual(published.headers["location"], path)
        snapshots = self.app.state.config_release_snapshot_repo.list_snapshot_records(
            org_id="default"
        )
        self.assertEqual(len(snapshots), 1)
        self.assertTrue(
            any(
                row.action_type == "config.release_publish"
                for row in self.app.state.audit_repo.list_recent_logs(20)
            )
        )


if __name__ == "__main__":
    import unittest

    unittest.main()
