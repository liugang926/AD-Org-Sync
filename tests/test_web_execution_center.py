from __future__ import annotations

import re
from unittest.mock import patch

from sync_app.storage.local_db import (
    SettingsRepository,
    SyncJobRepository,
    WebAuditLogRepository,
)
from tests.helpers.execution_plans import create_eligible_execution_plan
from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class WebExecutionCenterTests(WebAuthzBaseTestCase):
    CANONICAL_PAGES = (
        "/execution-center/dry-run",
        "/execution-center/plan-review",
        "/execution-center/apply",
        "/execution-center/jobs",
    )

    def _csrf_token(self, body: str) -> str:
        match = re.search(r'name="csrf_token" value="([^"]+)"', body)
        self.assertIsNotNone(match)
        return match.group(1)

    def test_canonical_pages_are_read_only_task_pages_with_one_primary_cta(self) -> None:
        self._login("superadmin")
        before_count = SyncJobRepository(self.app.state.db_manager).count_jobs()

        for path in self.CANONICAL_PAGES:
            with self.subTest(path=path):
                response = self._route(path, "GET")(self._request(path))
                self.assertEqual(response.status_code, 200)
                body = self._text(response)
                self.assertIn('aria-label="Execution Center"', body)
                header_actions = re.search(
                    r'<div class="page-header__actions">(.*?)</div>\s*</div>\s*</div>',
                    body,
                    re.S,
                )
                self.assertIsNotNone(header_actions)
                self.assertEqual(
                    len(re.findall(r'class="button(?:\s|\")', header_actions.group(1))),
                    1,
                )
                for table_head in re.findall(r"<thead>(.*?)</thead>", body, re.S):
                    self.assertLessEqual(table_head.count("<th"), 8)
                self.assertIn("Organization", body)
                self.assertIn("Environment", body)
                self.assertIn("Snapshot Version", body)
                self.assertIn("Impact Count", body)

        self.assertEqual(
            SyncJobRepository(self.app.state.db_manager).count_jobs(),
            before_count,
        )

    def test_review_then_apply_binds_the_exact_plan_and_writes_audit(self) -> None:
        self._login("superadmin")
        created = create_eligible_execution_plan(
            self.app.state.db_manager,
            job_id="web-plan-ready",
            environment_label=self.app.state.environment_label,
        )
        review_path = "/execution-center/plan-review"
        review_page = self._route(review_path, "GET")(
            self._request(review_path, query={"plan_id": "web-plan-ready"}),
            plan_id="web-plan-ready",
        )
        review_body = self._text(review_page)
        self.assertIn("Approve selected plan", review_body)

        approval_path = "/execution-center/plan-review/{job_id}/approve"
        approval_response = self._route(approval_path, "POST")(
            self._request(
                "/execution-center/plan-review/web-plan-ready/approve",
                "POST",
            ),
            job_id="web-plan-ready",
            csrf_token=self._csrf_token(review_body),
            review_notes="approved in browser regression",
        )
        self.assertEqual(approval_response.status_code, 303)
        self.assertIn("plan_id=web-plan-ready", approval_response.headers["location"])

        apply_path = "/execution-center/apply"
        apply_page = self._route(apply_path, "GET")(
            self._request(apply_path, query={"plan_id": "web-plan-ready"}),
            plan_id="web-plan-ready",
        )
        apply_body = self._text(apply_page)
        self.assertIn("Apply 1 Changes", apply_body)
        self.assertIn(f"#{created['snapshot_id']}", apply_body)

        run_path = "/execution-center/apply/run"
        with patch.object(
            self.app.state.sync_runner,
            "launch",
            return_value=(True, "Apply queued"),
        ) as launch:
            run_response = self._route(run_path, "POST")(
                self._request(run_path, "POST"),
                csrf_token=self._csrf_token(apply_body),
                operation_code="sync.apply",
                organization_id="default",
                environment_label=self.app.state.environment_label,
                snapshot_version=f"#{created['snapshot_id']}",
                impact_count="1",
                preview_id="web-plan-ready",
            )

        self.assertEqual(run_response.status_code, 303)
        launch.assert_called_once()
        self.assertEqual(
            launch.call_args.kwargs["plan_source_job_id"],
            "web-plan-ready",
        )
        audits = WebAuditLogRepository(
            self.app.state.db_manager
        ).list_recent_logs(20)
        requested = next(
            item for item in audits if item.action_type == "high_risk.apply.requested"
        )
        self.assertEqual(requested.org_id, "default")
        self.assertEqual(requested.payload["preview_id"], "web-plan-ready")

    def test_apply_rejects_bad_csrf_and_mismatched_confirmation(self) -> None:
        self._login("superadmin")
        created = create_eligible_execution_plan(
            self.app.state.db_manager,
            job_id="web-plan-blocked",
            environment_label=self.app.state.environment_label,
            approved=True,
        )
        run_path = "/execution-center/apply/run"
        endpoint = self._route(run_path, "POST")

        bad_csrf = endpoint(
            self._request(run_path, "POST"),
            csrf_token="invalid",
            operation_code="sync.apply",
            organization_id="default",
            environment_label=self.app.state.environment_label,
            snapshot_version=f"#{created['snapshot_id']}",
            impact_count="1",
            preview_id="web-plan-blocked",
        )
        self.assertEqual(bad_csrf.status_code, 303)

        with patch.object(self.app.state.sync_runner, "launch") as launch:
            mismatch = endpoint(
                self._request(run_path, "POST"),
                csrf_token=str(self.session.get("_csrf_token") or ""),
                operation_code="sync.apply",
                organization_id="another-org",
                environment_label=self.app.state.environment_label,
                snapshot_version=f"#{created['snapshot_id']}",
                impact_count="1",
                preview_id="web-plan-blocked",
            )
        self.assertEqual(mismatch.status_code, 303)
        launch.assert_not_called()
        blocked = next(
            item
            for item in WebAuditLogRepository(
                self.app.state.db_manager
            ).list_recent_logs(20)
            if item.action_type == "high_risk.apply.blocked"
        )
        self.assertEqual(blocked.result, "blocked")

    def test_apply_workflow_ignores_completed_apply_for_another_plan(self) -> None:
        self._login("superadmin")
        create_eligible_execution_plan(
            self.app.state.db_manager,
            job_id="web-plan-current",
            environment_label=self.app.state.environment_label,
            approved=True,
        )
        SyncJobRepository(self.app.state.db_manager).create_job(
            "web-apply-unrelated",
            trigger_type="unit_test",
            execution_mode="apply",
            status="COMPLETED",
            org_id="default",
            plan_source_job_id="another-plan",
        )

        apply_path = "/execution-center/apply"
        apply_page = self._route(apply_path, "GET")(
            self._request(
                apply_path,
                query={"plan_id": "web-plan-current"},
            ),
            plan_id="web-plan-current",
        )
        apply_body = self._text(apply_page)

        self.assertIn("Apply 1 Changes", apply_body)
        self.assertRegex(
            apply_body,
            r'data-state="current" data-high-risk-step="execute"',
        )
        self.assertNotRegex(
            apply_body,
            r'data-state="complete" data-high-risk-step="execute"',
        )

    def test_apply_page_blocks_when_current_config_changed_after_dry_run(self) -> None:
        self._login("superadmin")
        create_eligible_execution_plan(
            self.app.state.db_manager,
            job_id="web-plan-config-changed",
            environment_label=self.app.state.environment_label,
            approved=True,
        )
        settings_repo = SettingsRepository(self.app.state.db_manager)
        current_separator = settings_repo.get_value(
            "group_display_separator",
            "-",
            org_id="default",
        )
        settings_repo.set_value(
            "group_display_separator",
            "/" if current_separator != "/" else "-",
            "string",
            org_id="default",
        )

        apply_path = "/execution-center/apply"
        apply_page = self._route(apply_path, "GET")(
            self._request(
                apply_path,
                query={"plan_id": "web-plan-config-changed"},
            ),
            plan_id="web-plan-config-changed",
        )
        apply_body = self._text(apply_page)

        self.assertIn(
            "The synchronization configuration changed after the selected Dry Run.",
            apply_body,
        )
        self.assertNotIn("Apply 1 Changes", apply_body)
        self.assertIn("Run a new Dry Run", apply_body)

    def test_rbac_keeps_auditor_read_only_and_operator_out_of_review(self) -> None:
        self._login("auditor1")
        response = self._route("/execution-center/jobs", "GET")(
            self._request("/execution-center/jobs")
        )
        self.assertEqual(response.status_code, 200)
        post = self._route("/execution-center/dry-run/run", "POST")(
            self._request("/execution-center/dry-run/run", "POST"),
            csrf_token=str(self.session.get("_csrf_token") or ""),
        )
        self.assertEqual(post.status_code, 303)

        self._login("operator1")
        review = self._route(
            "/execution-center/plan-review/{job_id}/approve",
            "POST",
        )(
            self._request(
                "/execution-center/plan-review/missing/approve",
                "POST",
            ),
            job_id="missing",
            csrf_token=str(self.session.get("_csrf_token") or ""),
        )
        self.assertEqual(review.status_code, 303)


if __name__ == "__main__":
    import unittest

    unittest.main()
