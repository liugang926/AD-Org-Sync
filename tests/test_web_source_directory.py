import json
from unittest.mock import patch

from sync_app.core.models import DirectoryUserRecord
from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class _CreationPreviewTargetProvider:
    def __init__(self, existing_usernames=()):
        self.existing_usernames = {
            str(username).strip().lower() for username in existing_usernames
        }

    def get_users_batch(self, usernames):
        return {
            username: DirectoryUserRecord(username=username, dn="redacted")
            for username in usernames
            if str(username).strip().lower() in self.existing_usernames
        }

    def close(self):
        return None


class _UnavailableCreationPreviewTargetProvider(_CreationPreviewTargetProvider):
    last_batch_lookup_failed = True


class WebSourceDirectoryTests(WebAuthzBaseTestCase):
    def _scan_stale_bindings(self):
        return self._route("/source-directory/reconcile-stale-bindings", "POST")(
            self._request("/source-directory/reconcile-stale-bindings", "POST"),
            csrf_token=self.session["_csrf_token"],
            page_number=1,
            search="",
            department_id="",
            status="",
            employee_id_state="",
            relationship_status="all",
        )

    def _confirm_stale_binding_cleanup(self, *, csrf_token=None, **overrides):
        preview = self.session.get("_binding_cleanup_preview") or {}
        context = dict(preview.get("context") or {})
        submission = {
            "csrf_token": csrf_token
            if csrf_token is not None
            else self.session["_csrf_token"],
            "operation_code": context.get("operation_code", ""),
            "organization_id": context.get("organization_id", ""),
            "environment_label": context.get("environment_label", ""),
            "snapshot_version": context.get("snapshot_version", ""),
            "impact_count": str(context.get("impact_count", "")),
            "preview_id": context.get("preview_id", ""),
        }
        submission.update(overrides)
        return self._route(
            "/source-directory/reconcile-stale-bindings/confirm", "POST"
        )(
            self._request(
                "/source-directory/reconcile-stale-bindings/confirm", "POST"
            ),
            **submission,
        )

    def _seed_creation_candidates(self):
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
                    "samples": ["TJ001", "TJ002"],
                }
            ],
            fingerprint="creation-selection-v1",
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
            "legacy.alice",
            org_id="default",
            source_provider="wecom",
            connector_id="default",
            source="manual",
            source_display_name="Alice Ding",
        )
        return snapshot_id

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
        self.assertIn("This daily list contains eight business columns", body)
        self.assertEqual(body.count("<th>"), 8)
        self.assertNotIn("Test Connection", body)
        self.assertNotIn("Save Scope and Mapping", body)
        self.assertNotIn("Before synchronization", body)

    def test_empty_directory_explains_refresh_prerequisite_and_keeps_daily_columns_visible(self):
        self._login("superadmin")

        response = self._route("/source-directory", "GET")(
            self._request("/source-directory")
        )

        self.assertEqual(response.status_code, 200)
        body = self._text(response)
        self.assertIn("No successful source directory snapshot is available yet", body)
        self.assertIn("Employee ID", body)
        self.assertIn("Open Connectors", body)
        self.assertEqual(body.count("<th>"), 8)
        self.assertNotIn("verify_ad=true", body)

    def test_refreshing_directory_renders_automatic_status_poll(self):
        self._login("superadmin")
        self.app.state.source_directory_repo.start_refresh(
            org_id="default", provider_id="wecom", created_by="superadmin"
        )

        response = self._route("/source-directory", "GET")(
            self._request("/source-directory")
        )

        self.assertEqual(response.status_code, 200)
        body = self._text(response)
        self.assertIn("data-source-refresh-poll", body)
        self.assertIn('data-status-url="/api/source-directory/status"', body)
        self.assertIn("This page will update automatically", body)
        self.assertIn("disabled aria-disabled=\"true\"", body)

    def test_operator_cannot_open_or_modify_source_directory(self):
        self._login("operator1")
        response = self._route("/source-directory", "GET")(
            self._request("/source-directory")
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")
        response = self._route(
            "/source-directory/reconcile-stale-bindings/confirm", "POST"
        )(
            self._request(
                "/source-directory/reconcile-stale-bindings/confirm", "POST"
            ),
            csrf_token="",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")
        response = self._route("/source-directory/create-selection", "POST")(
            self._request("/source-directory/create-selection", "POST"),
            csrf_token="",
            selected_source_user_ids=["alice"],
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")
        response = self._route(
            "/source-directory/reconcile-stale-bindings", "POST"
        )(
            self._request(
                "/source-directory/reconcile-stale-bindings", "POST"
            ),
            csrf_token="",
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

        response = self._route("/sync-policies/account-naming", "GET")(
            self._request("/sync-policies/account-naming")
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

        page = self._route("/identity-governance/binding-reconciliation", "GET")(
            self._request("/identity-governance/binding-reconciliation")
        )
        body = self._text(page)
        self.assertIn("Identity Evidence", body)
        self.assertIn("Candidate", body)
        self.assertIn("Current Binding", body)
        self.assertIn("Latest Dry Run", body)
        self.assertIn("Latest Apply", body)
        self.assertEqual(body.count("<th>"), 8)
        self.assertEqual(body.count("data-identity-timeline>"), 2)
        self.assertEqual(body.count("data-identity-timeline-step"), 10)
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

    def test_verified_missing_candidate_can_be_selected_but_binding_mismatch_is_blocked(self):
        self._login("superadmin")
        self._seed_creation_candidates()

        with patch(
            "sync_app.web.app.build_target_provider",
            return_value=_CreationPreviewTargetProvider(),
        ):
            page = self._route("/identity-governance/identity-matching", "GET")(
                self._request("/identity-governance/identity-matching"),
                verify_ad=True,
            )
            response = self._route("/api/source-directory/relationships", "GET")(
                self._request("/api/source-directory/relationships"),
                verify_ad=True,
            )

        body = self._text(page)
        self.assertIn("missing", body)
        self.assertIn("Select for creation", body)
        self.assertIn("Review Manual Override", body)
        self.assertIn("Current Binding", body)
        scope = self._route("/sync-policies/scope", "GET")(
            self._request("/sync-policies/scope")
        )
        self.assertNotIn("Select for creation", self._text(scope))
        self.assertNotIn("Current Binding", self._text(scope))
        payload = json.loads(response.body)
        self.assertEqual(payload["candidate_missing_count"], 2)
        self.assertEqual(payload["creation_eligible_count"], 1)
        relationships = {
            item["source_user_id"]: item for item in payload["items"]
        }
        self.assertTrue(relationships["bob"]["creation_eligibility"]["eligible"])
        self.assertEqual(
            relationships["alice"]["creation_eligibility"]["status"],
            "binding_review_required",
        )

    def test_live_verification_cleans_only_confirmed_missing_saved_binding(self):
        self._login("superadmin")
        self._seed_creation_candidates()

        page = self._route("/identity-governance/binding-reconciliation", "GET")(
            self._request("/identity-governance/binding-reconciliation")
        )
        self.assertIn("Scan Binding Differences", self._text(page))

        with patch(
            "sync_app.web.app.build_target_provider",
            return_value=_CreationPreviewTargetProvider(),
        ):
            scan_response = self._scan_stale_bindings()

            self.assertEqual(scan_response.status_code, 303)
            self.assertIn("verify_ad=true", scan_response.headers["location"])
            self.assertIsNotNone(
                self.app.state.user_binding_repo.get_binding_record_by_source_user_id(
                    "alice",
                    org_id="default",
                    source_provider="wecom",
                    connector_id="default",
                )
            )
            preview = self.session["_binding_cleanup_preview"]
            self.assertEqual(preview["status"], "preview")
            self.assertEqual(preview["context"]["impact_count"], 1)
            self.assertEqual(preview["context"]["organization_id"], "default")
            self.assertEqual(preview["context"]["environment_label"], "Local environment")

            preview_page = self._route(
                "/identity-governance/binding-reconciliation", "GET"
            )(
                self._request("/identity-governance/binding-reconciliation")
            )
            preview_body = self._text(preview_page)
            self.assertIn("Binding Cleanup Preview", preview_body)
            self.assertIn("Snapshot Version", preview_body)
            self.assertIn("Impact Count", preview_body)

            response = self._confirm_stale_binding_cleanup()

        self.assertEqual(response.status_code, 303)
        self.assertIn("verify_ad=true", response.headers["location"])
        self.assertIsNone(
            self.app.state.user_binding_repo.get_binding_record_by_source_user_id(
                "alice",
                org_id="default",
                source_provider="wecom",
                connector_id="default",
            )
        )
        self.assertEqual(
            self.session["_flash"]["message"],
            {
                "key": "Removed {removed_count} verified stale binding(s). Recheck the candidates before selecting account creation.",
                "params": {"removed_count": 1},
            },
        )
        cleanup_logs = [
            item
            for item in self.app.state.audit_repo.list_recent_logs(20)
            if item.action_type == "mapping.binding_stale_cleanup"
        ]
        self.assertEqual(len(cleanup_logs), 1)
        self.assertEqual(cleanup_logs[0].target_id, "alice")
        execute_logs = [
            item
            for item in self.app.state.audit_repo.list_recent_logs(20)
            if item.action_type == "high_risk.binding_cleanup.execute"
        ]
        self.assertEqual(len(execute_logs), 1)
        self.assertEqual(execute_logs[0].result, "success")
        self.assertEqual(execute_logs[0].payload["organization_id"], "default")
        self.assertEqual(execute_logs[0].payload["environment_label"], "Local environment")
        self.assertEqual(execute_logs[0].payload["impact_count"], 1)
        self.assertTrue(execute_logs[0].payload["snapshot_version"].startswith("#"))

    def test_unavailable_ad_verification_never_cleans_saved_binding(self):
        self._login("superadmin")
        self._seed_creation_candidates()

        with patch(
            "sync_app.web.app.build_target_provider",
            return_value=_UnavailableCreationPreviewTargetProvider(),
        ):
            response = self._scan_stale_bindings()

        self.assertEqual(response.status_code, 303)
        self.assertIsNotNone(
            self.app.state.user_binding_repo.get_binding_record_by_source_user_id(
                "alice",
                org_id="default",
                source_provider="wecom",
                connector_id="default",
            )
        )
        self.assertEqual(
            self.session["_flash"]["message"],
            {
                "key": "Live AD verification is unavailable. No saved bindings were removed.",
                "params": {},
            },
        )

    def test_cleanup_confirmation_fails_closed_for_csrf_org_change_and_unlabeled_environment(self):
        self._login("superadmin")
        self._seed_creation_candidates()
        with patch(
            "sync_app.web.app.build_target_provider",
            return_value=_CreationPreviewTargetProvider(),
        ):
            self._scan_stale_bindings()
            csrf_response = self._confirm_stale_binding_cleanup(csrf_token="invalid")

        self.assertEqual(csrf_response.status_code, 303)
        self.assertIsNotNone(
            self.app.state.user_binding_repo.get_binding_record_by_source_user_id(
                "alice",
                org_id="default",
                source_provider="wecom",
                connector_id="default",
            )
        )

        self.app.state.organization_repo.upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=str(self.config_path),
            description="",
            is_enabled=True,
        )
        self.session["selected_org_id"] = "asia"
        org_response = self._confirm_stale_binding_cleanup()
        self.assertEqual(org_response.status_code, 303)
        self.assertIsNotNone(
            self.app.state.user_binding_repo.get_binding_record_by_source_user_id(
                "alice",
                org_id="default",
                source_provider="wecom",
                connector_id="default",
            )
        )

        self.session["selected_org_id"] = "default"
        self.app.state.environment_label = "Unlabeled environment"
        with patch(
            "sync_app.web.app.build_target_provider",
            return_value=_CreationPreviewTargetProvider(),
        ):
            self._scan_stale_bindings()
            unlabeled_response = self._confirm_stale_binding_cleanup()
        self.assertEqual(unlabeled_response.status_code, 303)
        self.assertIsNotNone(
            self.app.state.user_binding_repo.get_binding_record_by_source_user_id(
                "alice",
                org_id="default",
                source_provider="wecom",
                connector_id="default",
            )
        )
        blocked_logs = [
            item
            for item in self.app.state.audit_repo.list_recent_logs(30)
            if item.action_type == "high_risk.binding_cleanup.execute"
            and item.result == "blocked"
        ]
        self.assertTrue(blocked_logs)
        self.assertEqual(
            blocked_logs[0].payload["reason_code"],
            "high_risk.blocker.environment_unlabeled",
        )

    def test_prepare_creation_reverifies_and_saves_exact_dry_run_scope(self):
        self._login("superadmin")
        self._seed_creation_candidates()

        with patch(
            "sync_app.web.app.build_target_provider",
            return_value=_CreationPreviewTargetProvider(),
        ):
            response = self._route(
                "/source-directory/create-selection", "POST"
            )(
                self._request("/source-directory/create-selection", "POST"),
                csrf_token=self.session["_csrf_token"],
                selected_source_user_ids=["bob"],
            )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            response.headers["location"],
            "/execution-center/dry-run",
        )
        selection = self.app.state.source_directory_repo.get_scope_selection(
            org_id="default", provider_id="wecom"
        )
        self.assertEqual(selection["scope_type"], "selected_users")
        self.assertEqual(selection["selected_source_user_ids"], ["bob"])
        self.assertEqual(selection["source_field"], "employee_id")
        self.assertIn("no AD changes", self.session["_flash"]["message"])

    def test_prepare_creation_rejects_candidate_that_now_exists(self):
        self._login("superadmin")
        self._seed_creation_candidates()

        with patch(
            "sync_app.web.app.build_target_provider",
            return_value=_CreationPreviewTargetProvider(["TJ002"]),
        ):
            response = self._route(
                "/source-directory/create-selection", "POST"
            )(
                self._request("/source-directory/create-selection", "POST"),
                csrf_token=self.session["_csrf_token"],
                selected_source_user_ids=["bob"],
            )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            response.headers["location"],
            "/identity-governance/identity-matching?verify_ad=true",
        )
        selection = self.app.state.source_directory_repo.get_scope_selection(
            org_id="default", provider_id="wecom"
        )
        self.assertEqual(selection["scope_type"], "full")
        self.assertEqual(
            self.session["_flash"]["message"],
            "The candidate AD account already exists.",
        )


if __name__ == "__main__":
    import unittest
    unittest.main()
