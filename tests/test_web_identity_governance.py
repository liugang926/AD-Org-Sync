import re
from unittest.mock import patch

from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class WebIdentityGovernanceTests(WebAuthzBaseTestCase):
    def _seed_identity_snapshot(self):
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
                    "display_name": "Alice",
                    "employee_id": "E001",
                    "department_ids": ["1"],
                    "department_names": ["HQ"],
                    "is_active": True,
                    "raw_payload": {"userid": "alice", "employee_id": "E001"},
                    "search_text": "Alice E001",
                },
                {
                    "source_user_id": "bob",
                    "display_name": "Bob",
                    "employee_id": "E002",
                    "department_ids": ["1"],
                    "department_names": ["HQ"],
                    "is_active": True,
                    "raw_payload": {"userid": "bob", "employee_id": "E002"},
                    "search_text": "Bob E002",
                },
            ],
            fields=[
                {
                    "name": "employee_id",
                    "label": "Employee ID",
                    "coverage": 2,
                    "samples": ["E001", "E002"],
                }
            ],
            fingerprint="identity-governance-v1",
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
            "alice.reviewed",
            org_id="default",
            source_provider="wecom",
            source="manual",
            source_display_name="Alice",
        )
        return snapshot_id

    def test_identity_matching_is_read_only_eight_column_evidence_with_shared_timeline(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        self._seed_identity_snapshot()
        self.app.state.user_binding_repo.upsert_binding(
            "alice",
            "other-org.secret",
            org_id="other-org",
            source_provider="wecom",
            source="manual",
        )

        response = self._route("/identity-governance/identity-matching", "GET")(
            self._request("/identity-governance/identity-matching")
        )

        self.assertEqual(response.status_code, 200)
        body = self._text(response)
        table = re.search(
            r'<table data-identity-matching-table>(.*?)</table>', body, re.S
        ).group(1)
        self.assertEqual(table.count("<th>"), 8)
        self.assertEqual(len(re.findall(r'class="button"(?:\s|>)', body)), 1)
        self.assertIn("Review Next Candidate", body)
        self.assertIn("data-identity-drawer", body)
        self.assertEqual(body.count("data-identity-timeline-step"), 14)
        self.assertIn("alice.reviewed", body)
        self.assertIn("Manual binding overrides the field-generated candidate", body)
        self.assertIn("Candidate AD account differs from saved binding", body)
        self.assertIn("Complete evidence timeline", body)
        self.assertNotIn("other-org.secret", body)
        self.assertNotIn("Password123!", body)

    def test_identity_matching_preserves_mapping_read_permission_boundary(self):
        self._login("operator1")

        response = self._route("/identity-governance/identity-matching", "GET")(
            self._request("/identity-governance/identity-matching")
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("Identity Matching", self._text(response))

    def test_canonical_manual_overrides_show_only_manual_records_and_keep_legacy_page(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        self._seed_identity_snapshot()
        self.app.state.user_binding_repo.upsert_binding(
            "bob",
            "bob.generated",
            org_id="default",
            source_provider="wecom",
            source="managed_generated",
            source_display_name="Bob",
        )

        canonical = self._route("/identity-governance/manual-overrides", "GET")(
            self._request("/identity-governance/manual-overrides")
        )
        legacy = self._route("/mappings", "GET")(self._request("/mappings"))
        exported = self._route(
            "/identity-governance/manual-overrides/export", "GET"
        )(self._request("/identity-governance/manual-overrides/export"))

        canonical_body = self._text(canonical)
        self.assertIn("Manual Override Health", canonical_body)
        self.assertIn("alice.reviewed", canonical_body)
        self.assertNotIn("bob.generated", canonical_body)
        self.assertIn(
            'action="/identity-governance/manual-overrides/bind"', canonical_body
        )
        self.assertEqual(
            len(re.findall(r'class="button"(?:\s|>)', canonical_body)), 1
        )
        self.assertIn("bob.generated", self._text(legacy))
        self.assertIn("Identity Overrides", self._text(legacy))
        export_body = self._response_body(exported).decode("utf-8-sig")
        self.assertIn("alice.reviewed", export_body)
        self.assertNotIn("bob.generated", export_body)
        self.assertIn(
            "manual-overrides-export.csv",
            exported.headers["content-disposition"],
        )

    def test_canonical_exception_write_keeps_csrf_rbac_audit_and_legacy_visibility(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        canonical_path = "/identity-governance/exception-rules"

        blocked = self._route(canonical_path, "POST")(
            self._request(canonical_path, "POST"),
            csrf_token="wrong",
            rule_type="skip_user_disable",
            match_value="alice",
        )
        self.assertEqual(blocked.status_code, 303)
        self.assertEqual(blocked.headers["location"], canonical_path)
        self.assertEqual(self.app.state.exception_rule_repo.list_rule_records(), [])

        saved = self._route(canonical_path, "POST")(
            self._request(canonical_path, "POST"),
            csrf_token=self.session["_csrf_token"],
            rule_type="skip_user_disable",
            match_value="alice",
            rule_owner="iam@example.com",
            effective_reason="Reviewed leave window",
            next_review_at="",
            notes="",
            expires_at="",
            is_once=None,
        )
        self.assertEqual(saved.status_code, 303)
        self.assertEqual(saved.headers["location"], canonical_path)
        self.assertTrue(
            any(
                item.action_type == "exception_rule.upsert"
                for item in self.app.state.audit_repo.list_recent_logs(20)
            )
        )

        with patch("sync_app.providers.source.wecom.WeComAPI") as source_api:
            source_api.return_value.get_department_list.return_value = []
            canonical = self._route(canonical_path, "GET")(
                self._request(canonical_path)
            )
            legacy = self._route("/exceptions", "GET")(
                self._request("/exceptions")
            )
        self.assertIn("Reviewed leave window", self._text(canonical))
        self.assertIn("Reviewed leave window", self._text(legacy))
        self.assertEqual(
            len(re.findall(r'class="button"(?:\s|>)', self._text(canonical))), 1
        )
        table = re.search(
            r'<table>(.*?)</table>', self._text(canonical), re.S
        ).group(1)
        self.assertEqual(table.count("<th>"), 7)

    def test_canonical_conflict_and_write_aliases_are_registered(self):
        aliases = (
            ("/identity-governance/conflicts/{conflict_id}/decision-guide", "GET"),
            ("/identity-governance/conflicts/{conflict_id}/resolve-binding", "POST"),
            ("/identity-governance/conflicts/bulk", "POST"),
            ("/identity-governance/manual-overrides/import", "POST"),
            ("/identity-governance/manual-overrides/export", "GET"),
            ("/identity-governance/exception-rules/import", "POST"),
            ("/identity-governance/exception-rules/export", "GET"),
        )
        for path, method in aliases:
            with self.subTest(path=path, method=method):
                self.assertTrue(callable(self._route(path, method)))

    def test_canonical_conflict_queue_keeps_filters_and_decision_links_canonical(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        self.app.state.job_repo.create_job(
            "phase4-conflict-job",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="default",
        )
        conflict_id = self.app.state.conflict_repo.add_conflict(
            job_id="phase4-conflict-job",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="phase4 canonical conflict",
            resolution_hint="review candidates",
            details={
                "userid": "alice",
                "candidates": [
                    {"rule": "existing_ad_userid", "username": "alice.ad"},
                    {"rule": "existing_ad_email_localpart", "username": "alice.alt"},
                ],
            },
        )
        path = "/identity-governance/conflicts"

        response = self._route(path, "GET")(
            self._request(
                path,
                query={
                    "q": "alice",
                    "status": "open",
                    "job_id": "phase4-conflict-job",
                },
            )
        )

        body = self._text(response)
        self.assertIn("phase4 canonical conflict", body)
        self.assertIn(
            f'href="{path}/{conflict_id}/decision-guide?', body
        )
        self.assertIn(f'action="{path}/bulk"', body)
        self.assertIn(f'action="{path}/{conflict_id}/resolve-binding"', body)
        self.assertIn("Process Next Conflict", body)
        self.assertEqual(len(re.findall(r'class="button"(?:\s|>)', body)), 1)
