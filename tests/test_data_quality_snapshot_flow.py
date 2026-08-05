from __future__ import annotations

import re
from unittest.mock import patch

from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class DataQualitySnapshotFlowTests(WebAuthzBaseTestCase):
    def _csrf(self, body: str) -> str:
        match = re.search(r'name="csrf_token" value="([^"]+)"', body)
        self.assertIsNotNone(match)
        return str(match.group(1))

    def _seed_source_snapshot(
        self,
        *,
        fingerprint: str,
        qualified: bool = True,
    ) -> int:
        snapshot_id = self.app.state.source_directory_repo.start_refresh(
            org_id="default",
            provider_id="wecom",
            created_by="quality-test",
        )
        self.app.state.source_directory_repo.replace_snapshot(
            snapshot_id,
            departments=[
                {
                    "source_department_id": "1",
                    "name": "Headquarters",
                    "parent_department_id": "0",
                }
            ],
            users=[
                {
                    "source_user_id": "alice",
                    "display_name": "Alice",
                    "employee_id": "1001",
                    "email": "alice@example.com",
                    "department_ids": ["1"] if qualified else [],
                    "department_names": ["Headquarters"] if qualified else [],
                    "primary_department_id": "1" if qualified else "",
                    "is_active": True,
                    "raw_payload": {},
                }
            ],
            fields=[
                {
                    "name": "employee_id",
                    "label": "Employee ID",
                    "data_type": "string",
                    "coverage": 1,
                }
            ],
            fingerprint=fingerprint,
        )
        return snapshot_id

    def _run_scan(self, snapshot_id: int, fingerprint: str):
        page = self._route("/data-sources/data-quality", "GET")(
            self._request("/data-sources/data-quality")
        )
        return self._route("/data-sources/data-quality/run", "POST")(
            self._request("/data-sources/data-quality/run", "POST"),
            csrf_token=self._csrf(self._text(page)),
            source_snapshot_id=snapshot_id,
            fingerprint=fingerprint,
        )

    def test_offline_source_with_saved_snapshot_can_complete_quality_scan(self):
        self._login("superadmin")
        fingerprint = "sha256:v2:offline-source"
        snapshot_id = self._seed_source_snapshot(fingerprint=fingerprint)
        page = self._text(
            self._route("/data-sources/data-quality", "GET")(
                self._request("/data-sources/data-quality")
            )
        )
        self.assertIn(f'name="source_snapshot_id" value="{snapshot_id}"', page)
        self.assertIn(f'name="fingerprint" value="{fingerprint}"', page)
        self.assertIn('action="/data-sources/source-directory/refresh"', page)
        self.assertNotIn('name="refresh_source"', page)

        with patch(
            "sync_app.web.app.build_source_provider",
            side_effect=ConnectionError("source platform is offline"),
        ) as source_provider:
            response = self._run_scan(snapshot_id, fingerprint)

        self.assertEqual(response.status_code, 303)
        self.assertIn("snapshot_id=", response.headers["location"])
        source_provider.assert_not_called()
        scan = self.app.state.data_quality_snapshot_repo.get_latest_snapshot_record(
            org_id="default"
        )
        self.assertEqual(scan.source_snapshot_id, snapshot_id)
        self.assertEqual(scan.source_snapshot_fingerprint, fingerprint)
        self.assertEqual(scan.scan_status, "qualified")

    def test_scan_rejects_missing_or_mismatched_snapshot_evidence(self):
        self._login("superadmin")
        snapshot_id = self._seed_source_snapshot(
            fingerprint="sha256:v2:explicit-evidence"
        )
        page = self._route("/data-sources/data-quality", "GET")(
            self._request("/data-sources/data-quality")
        )
        csrf_token = self._csrf(self._text(page))

        missing = self._route("/data-sources/data-quality/run", "POST")(
            self._request("/data-sources/data-quality/run", "POST"),
            csrf_token=csrf_token,
        )
        mismatch = self._route("/data-sources/data-quality/run", "POST")(
            self._request("/data-sources/data-quality/run", "POST"),
            csrf_token=csrf_token,
            source_snapshot_id=snapshot_id,
            fingerprint="sha256:v2:not-the-snapshot",
        )

        self.assertEqual(missing.status_code, 303)
        self.assertEqual(mismatch.status_code, 303)
        self.assertEqual(
            self.app.state.data_quality_snapshot_repo.list_snapshot_records(
                org_id="default"
            ),
            [],
        )

    def test_quality_states_and_same_fingerprint_review_reuse_are_distinct(self):
        self._login("superadmin")
        empty_page = self._text(
            self._route("/data-sources/data-quality", "GET")(
                self._request("/data-sources/data-quality")
            )
        )
        self.assertIn('data-quality-state="no_snapshot"', empty_page)
        self.assertIn("No available snapshot", empty_page)

        fingerprint = "sha256:v2:review-reuse"
        first_snapshot_id = self._seed_source_snapshot(fingerprint=fingerprint)
        scan_response = self._run_scan(first_snapshot_id, fingerprint)
        self.assertEqual(scan_response.status_code, 303)
        awaiting_page = self._text(
            self._route("/data-sources/data-quality", "GET")(
                self._request("/data-sources/data-quality")
            )
        )
        self.assertIn('data-quality-state="awaiting_review"', awaiting_page)
        self.assertIn("Snapshot awaiting manual review", awaiting_page)
        self.assertIn("Based on Source Snapshot", awaiting_page)
        self.assertIn("Source Snapshot Time", awaiting_page)
        self.assertIn("Source Users", awaiting_page)

        review_response = self._route("/data-sources/data-quality/review", "POST")(
            self._request("/data-sources/data-quality/review", "POST"),
            csrf_token=self._csrf(awaiting_page),
            source_snapshot_id=first_snapshot_id,
            source_snapshot_fingerprint=fingerprint,
            confirmed="1",
            review_notes="Reviewed exact immutable evidence.",
        )
        self.assertEqual(review_response.status_code, 303)
        approved_page = self._text(
            self._route("/data-sources/data-quality", "GET")(
                self._request("/data-sources/data-quality")
            )
        )
        self.assertIn('data-quality-state="approved"', approved_page)
        self.assertIn("Snapshot quality review passed", approved_page)

        second_snapshot_id = self._seed_source_snapshot(fingerprint=fingerprint)
        reused_page = self._text(
            self._route("/data-sources/data-quality", "GET")(
                self._request("/data-sources/data-quality")
            )
        )
        self.assertIn('data-quality-state="approved"', reused_page)
        self.assertIn(
            f"Reused the quality conclusion from source snapshot #{first_snapshot_id}",
            reused_page,
        )
        self.assertNotEqual(first_snapshot_id, second_snapshot_id)

    def test_unqualified_snapshot_cannot_be_reviewed(self):
        self._login("superadmin")
        fingerprint = "sha256:v2:unqualified"
        snapshot_id = self._seed_source_snapshot(
            fingerprint=fingerprint,
            qualified=False,
        )
        self.assertEqual(self._run_scan(snapshot_id, fingerprint).status_code, 303)
        page = self._text(
            self._route("/data-sources/data-quality", "GET")(
                self._request("/data-sources/data-quality")
            )
        )
        self.assertIn('data-quality-state="unqualified"', page)
        self.assertIn("Snapshot data quality failed", page)
        self.assertNotIn('action="/data-sources/data-quality/review"', page)

        response = self._route("/data-sources/data-quality/review", "POST")(
            self._request("/data-sources/data-quality/review", "POST"),
            csrf_token=self._csrf(page),
            source_snapshot_id=snapshot_id,
            source_snapshot_fingerprint=fingerprint,
            confirmed="1",
        )
        self.assertEqual(response.status_code, 303)
        self.assertIsNone(
            self.app.state.data_quality_review_repo.get_review_for_snapshot(
                org_id="default",
                source_snapshot_id=snapshot_id,
            )
        )

    def test_refresh_failure_keeps_previous_snapshot_scan_state_available(self):
        self._login("superadmin")
        fingerprint = "sha256:v2:previous-success"
        snapshot_id = self._seed_source_snapshot(fingerprint=fingerprint)
        self.assertEqual(self._run_scan(snapshot_id, fingerprint).status_code, 303)
        failed_refresh_id = self.app.state.source_directory_repo.start_refresh(
            org_id="default",
            provider_id="wecom",
            created_by="quality-test",
        )
        self.app.state.source_directory_repo.fail_refresh(
            failed_refresh_id,
            "source network unavailable",
        )

        page = self._text(
            self._route("/data-sources/data-quality", "GET")(
                self._request("/data-sources/data-quality")
            )
        )
        self.assertIn('data-quality-source-state="refresh_failed"', page)
        self.assertIn(
            "The previous successful snapshot remains available for quality scanning.",
            page,
        )
        self.assertIn('data-quality-state="awaiting_review"', page)
        self.assertNotIn("Data quality snapshot failed: source network", page)
