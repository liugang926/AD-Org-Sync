import json

from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class WebIdentityRelationshipTests(WebAuthzBaseTestCase):
    def _seed_source(self):
        repo = self.app.state.source_directory_repo
        snapshot_id = repo.start_refresh(
            org_id="default",
            provider_id="wecom",
            created_by="test",
        )
        repo.replace_snapshot(
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
                    "source_user_id": "alice.wecom",
                    "display_name": "Alice Zhang",
                    "employee_id": "E100",
                    "email": "alice@example.com",
                    "department_ids": ["1"],
                    "department_names": ["Headquarters"],
                    "is_active": True,
                    "raw_payload": {
                        "userid": "alice.wecom",
                        "employee_id": "E100",
                    },
                    "search_text": "Alice Zhang E100",
                }
            ],
            fields=[
                {
                    "name": "employee_id",
                    "label": "Employee ID",
                    "coverage": 1,
                    "samples": ["E100"],
                }
            ],
            fingerprint="web-identity-snapshot-v1",
        )
        selection = repo.save_scope_selection(
            org_id="default",
            provider_id="wecom",
            scope_type="full",
            username_strategy="employee_id",
            source_field="employee_id",
            snapshot_id=snapshot_id,
            requested_by="test",
        )
        return snapshot_id, selection

    def _seed_job(self, selection, *, job_id="identity-job-1", mode="apply"):
        jobs = self.app.state.job_repo
        jobs.create_job(
            job_id,
            "test",
            mode,
            "COMPLETED",
            org_id="default",
        )
        jobs.update_job(
            job_id,
            summary={"plan_fingerprint": "plan-web-1"},
            ended=True,
        )
        self.app.state.source_directory_repo.bind_job_scope(
            job_id=job_id,
            execution_mode=mode,
            config_fingerprint="config-web-1",
            selection=selection,
        )
        resolution = {
            "source_provider": "wecom",
            "connector_id": "default",
            "source_user_id": "alice.wecom",
            "source_display_name": "Alice Zhang",
            "source": "manual_binding",
            "ad_username": "alice.manual",
            "mapping_input": {
                "field_name": "employee_id",
                "field_label": "Employee ID",
                "value": "E100",
                "method": "employee_id",
            },
            "candidate_mapping": {
                "ad_username": "E100",
                "source": "managed_username_primary",
                "risks": [],
            },
            "before_state": {
                "bound_ad_username": "alice.manual",
                "binding_source": "manual",
                "binding_enabled": True,
                "connector_id": "default",
                "ad_account_state": {"status": "exists", "exists": True},
                "verified_at": "2026-07-14T01:00:00+00:00",
            },
            "rule_hits": ["manual_binding"],
            "explanation": "Manual binding overrides the field-generated candidate",
        }
        self.app.state.operation_log_repo.add_record(
            job_id=job_id,
            stage_name="plan",
            object_type="user_binding",
            operation_type="resolve_identity_binding",
            status="selected",
            message="structured identity resolution",
            source_id="alice.wecom",
            target_id="alice.manual",
            rule_source="manual_binding",
            reason_code="manual_binding",
            details=resolution,
        )
        self.app.state.planned_operation_repo.add_operation(
            job_id,
            "user",
            "update_user",
            source_id="alice.wecom",
            desired_state={
                "connector_id": "default",
                "ad_username": "alice.manual",
            },
        )
        if mode == "apply":
            self.app.state.operation_log_repo.add_record(
                job_id=job_id,
                stage_name="apply",
                object_type="user",
                operation_type="update_user",
                status="succeeded",
                message="updated",
                source_id="alice.wecom",
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
        return job_id

    def test_job_detail_and_api_render_structured_identity_resolution(self):
        _snapshot_id, selection = self._seed_source()
        job_id = self._seed_job(selection)
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"

        page = self._route("/jobs/{job_id}", "GET")(
            self._request(f"/jobs/{job_id}"),
            job_id=job_id,
        )
        self.assertEqual(page.status_code, 200)
        text = self._text(page)
        self.assertIn("Identity Resolution Results", text)
        self.assertIn("Alice Zhang", text)
        self.assertIn("alice.wecom", text)
        self.assertIn("alice.manual", text)
        self.assertIn("E100", text)
        self.assertIn("update_user", text)
        self.assertNotIn("password", text.lower())

        api = self._route("/api/jobs/{job_id}/identity-resolutions", "GET")(
            self._request(f"/api/jobs/{job_id}/identity-resolutions"),
            job_id=job_id,
        )
        payload = json.loads(self._response_body(api))
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["items"][0]["source_user_id"], "alice.wecom")
        self.assertEqual(payload["items"][0]["apply_result"], "succeeded")

    def test_mappings_page_shows_relationship_context_and_snapshot_orphan(self):
        _snapshot_id, selection = self._seed_source()
        job_id = self._seed_job(selection)
        bindings = self.app.state.user_binding_repo
        bindings.upsert_binding(
            "alice.wecom",
            "alice.manual",
            org_id="default",
            source_provider="wecom",
            connector_id="default",
            source="manual",
        )
        bindings.upsert_binding(
            "departed.wecom",
            "departed.ad",
            org_id="default",
            source_provider="wecom",
            connector_id="default",
            source="managed_generated",
        )
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"

        page = self._route("/mappings", "GET")(
            self._request("/mappings", query={"q": "Alice Zhang"})
        )
        self.assertEqual(page.status_code, 200)
        text = self._text(page)
        self.assertIn("Alice Zhang", text)
        self.assertIn("wecom", text)
        self.assertIn("default", text)
        self.assertIn("Employee ID", text)
        self.assertIn("E100", text)
        self.assertIn("alice.manual", text)
        self.assertIn("manual", text)
        self.assertIn(job_id, text)
        self.assertIn("Total 1 items", text)

        orphan_page = self._route("/mappings", "GET")(
            self._request("/mappings", query={"q": "departed.ad"})
        )
        orphan_text = self._text(orphan_page)
        self.assertIn("departed.ad", orphan_text)
        self.assertIn("Not in current snapshot", orphan_text)

    def test_job_identity_api_is_current_organization_scoped(self):
        self.app.state.job_repo.create_job(
            "other-org-job",
            "test",
            "dry_run",
            "COMPLETED",
            org_id="other",
        )
        self._login("superadmin")

        response = self._route(
            "/api/jobs/{job_id}/identity-resolutions", "GET"
        )(
            self._request("/api/jobs/other-org-job/identity-resolutions"),
            job_id="other-org-job",
        )
        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    import unittest

    unittest.main()
