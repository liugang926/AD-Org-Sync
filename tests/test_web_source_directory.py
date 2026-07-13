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


if __name__ == "__main__":
    import unittest
    unittest.main()
