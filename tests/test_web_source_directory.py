import json

from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class WebSourceDirectoryTests(WebAuthzBaseTestCase):
    def test_super_admin_can_open_source_directory_without_exposing_secrets(self):
        self._login("superadmin")
        response = self._route("/source-directory", "GET")(
            self._request("/source-directory")
        )
        self.assertEqual(response.status_code, 200)
        body = self._text(response)
        self.assertIn("Source Directory", body)
        self.assertIn("Refresh Directory", body)
        self.assertNotIn("secret-001", body)
        self.assertIn("Partial synchronization will not process offboarding", body)

    def test_operator_cannot_open_or_modify_source_directory(self):
        self._login("operator1")
        response = self._route("/source-directory", "GET")(
            self._request("/source-directory")
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")
        response = self._route("/source-directory/scope", "POST")(
            self._request("/source-directory/scope", "POST"),
            csrf_token="",
            scope_type="selected_users",
            selected_department_ids=[],
            selected_source_user_ids=["alice"],
            source_field="employee_id",
            username_template="",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")

    def test_source_directory_api_is_organization_scoped(self):
        self._login("superadmin")
        response = self._route("/api/source-directory/users", "GET")(
            self._request("/api/source-directory/users")
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body.decode("utf-8"), '{"ok":true,"items":[],"total":0}')

    def test_account_source_options_show_dynamic_business_and_actual_field_names(self):
        self._login("superadmin")
        self.session["ui_language"] = "zh-CN"
        snapshot_id = self.app.state.source_directory_repo.start_refresh(
            org_id="default", provider_id="wecom", created_by="superadmin"
        )
        self.app.state.source_directory_repo.replace_snapshot(
            snapshot_id,
            departments=[{"source_department_id": "1", "name": "HQ", "parent_department_id": "0", "path_ids": ["1"], "path_names": ["HQ"]}],
            users=[{"source_user_id": "alice", "display_name": "Alice", "employee_id": "E1", "department_ids": ["1"], "department_names": ["HQ"], "is_active": True, "search_text": "alice E1"}],
            fields=[{"name": "tenant_staff_code", "label": "Employee ID", "coverage": 1, "samples": ["E1"]}],
            fingerprint="sha256:v2:dynamic-field-label",
        )

        response = self._route("/source-directory", "GET")(
            self._request("/source-directory")
        )
        self.assertEqual(response.status_code, 200)
        body = self._text(response)
        self.assertIn("工号 / 员工 ID (employee_id)", body)
        self.assertIn("工号 / 员工 ID (tenant_staff_code) · 覆盖 1/1", body)

    def test_all_filtered_selection_is_resolved_server_side(self):
        self._login("superadmin")
        snapshot_id = self.app.state.source_directory_repo.start_refresh(
            org_id="default", provider_id="wecom", created_by="superadmin"
        )
        self.app.state.source_directory_repo.replace_snapshot(
            snapshot_id,
            departments=[{"source_department_id": "1", "name": "HQ", "parent_department_id": "0", "path_ids": ["1"], "path_names": ["HQ"]}],
            users=[
                {"source_user_id": "alice", "display_name": "Alice", "employee_id": "E1", "department_ids": ["1"], "department_names": ["HQ"], "is_active": True, "search_text": "alice E1"},
                {"source_user_id": "bob", "display_name": "Bob", "employee_id": "E2", "department_ids": ["1"], "department_names": ["HQ"], "is_active": True, "search_text": "bob E2"},
            ],
            fields=[],
            fingerprint="sha256:v2:test-source",
        )
        response = self._route("/source-directory/scope", "POST")(
            self._request("/source-directory/scope", "POST"),
            csrf_token=self.session["_csrf_token"],
            scope_type="selected_users",
            selected_department_ids=[],
            selected_source_user_ids=[],
            source_field="employee_id",
            username_template="",
            employee_id_attribute="",
            selection_mode="all_filtered",
            selection_search="alice",
            selection_department_id="",
            selection_status="active",
            selection_employee_id_state="",
        )
        self.assertEqual(response.status_code, 303)
        selection = self.app.state.source_directory_repo.get_scope_selection(
            org_id="default", provider_id="wecom"
        )
        self.assertEqual(selection["selected_source_user_ids"], ["alice"])

    def test_relationship_page_and_api_separate_candidate_from_actual_binding(self):
        self._login("superadmin")
        snapshot_id = self.app.state.source_directory_repo.start_refresh(
            org_id="default", provider_id="wecom", created_by="superadmin"
        )
        self.app.state.source_directory_repo.replace_snapshot(
            snapshot_id,
            departments=[
                {
                    "source_department_id": "1",
                    "name": "HQ",
                    "parent_department_id": "0",
                    "path_ids": ["1"],
                    "path_names": ["HQ"],
                }
            ],
            users=[
                {
                    "source_user_id": "alice",
                    "display_name": "Alice Ding",
                    "employee_id": "TJ001",
                    "department_ids": ["1"],
                    "department_names": ["HQ"],
                    "is_active": True,
                    "raw_payload": {"userid": "alice", "employee_id": "TJ001"},
                    "search_text": "Alice Ding TJ001",
                },
                {
                    "source_user_id": "bob",
                    "display_name": "Bob Ding",
                    "employee_id": "TJ002",
                    "department_ids": ["1"],
                    "department_names": ["HQ"],
                    "is_active": True,
                    "raw_payload": {"userid": "bob", "employee_id": "TJ002"},
                    "search_text": "Bob Ding TJ002",
                },
            ],
            fields=[
                {
                    "name": "employee_id",
                    "label": "Employee ID",
                    "coverage": 2,
                    "samples": ["TJ001"],
                }
            ],
            fingerprint="relationship-web-v1",
        )
        self.app.state.source_directory_repo.save_scope_selection(
            org_id="default",
            provider_id="wecom",
            scope_type="full",
            username_strategy="employee_id",
            source_field="employee_id",
            snapshot_id=snapshot_id,
            requested_by="superadmin",
        )
        self.app.state.user_binding_repo.upsert_binding(
            "alice",
            "alice.manual",
            org_id="default",
            source_provider="wecom",
            connector_id="default",
            source="manual",
            source_display_name="Alice Ding",
        )

        page = self._route("/source-directory", "GET")(
            self._request("/source-directory")
        )
        body = self._text(page)
        self.assertIn("Source and mapping method", body)
        self.assertIn("Before synchronization", body)
        self.assertIn("Planned after Dry Run", body)
        self.assertIn("Applied result", body)
        self.assertIn("Post-Apply AD state", body)
        self.assertIn("TJ001", body)
        self.assertIn("alice.manual", body)
        self.assertIn("Manual binding overrides the field-generated candidate", body)
        self.assertNotIn("DistinguishedName", body)
        self.assertNotIn("Password123!", body)

        response = self._route("/api/source-directory/relationships", "GET")(
            self._request("/api/source-directory/relationships"),
            relationship_status="bound",
        )
        payload = json.loads(response.body)
        self.assertEqual(payload["total"], 1)
        relationship = payload["items"][0]
        self.assertEqual(relationship["source_user"]["display_name"], "Alice Ding")
        self.assertEqual(relationship["mapping_input"]["field_name"], "employee_id")
        self.assertEqual(relationship["candidate_mapping"]["ad_username"], "TJ001")
        self.assertEqual(relationship["before_state"]["bound_ad_username"], "alice.manual")
        self.assertEqual(relationship["applied_after_state"]["result"], "not_applied")

        unbound = self._route("/api/source-directory/relationships", "GET")(
            self._request("/api/source-directory/relationships"),
            relationship_status="unbound",
        )
        unbound_payload = json.loads(unbound.body)
        self.assertEqual(unbound_payload["total"], 1)
        self.assertEqual(unbound_payload["items"][0]["source_user_id"], "bob")

    def test_relationship_api_requires_read_capability(self):
        self._login("operator1")
        response = self._route("/api/source-directory/relationships", "GET")(
            self._request("/api/source-directory/relationships")
        )
        self.assertEqual(response.status_code, 403)
        self.assertNotIn("session", response.body.decode("utf-8").lower())


if __name__ == "__main__":
    import unittest
    unittest.main()
