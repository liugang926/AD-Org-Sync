from __future__ import annotations

import argparse
import contextlib
import io
import tempfile
import unittest

from sync_app.cli.handlers.sync import _handle_approve_plan
from sync_app.storage.local_db import (
    DatabaseManager,
    SettingsRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
    SyncReplayRequestRepository,
    WebAuditLogRepository,
)


class CliApprovePlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = f"{self.temp_dir.name}/app.db"
        self.db_manager = DatabaseManager(db_path=self.db_path)
        self.db_manager.initialize(create_startup_snapshot=False)
        SyncJobRepository(self.db_manager).create_job(
            "job-cli-approve",
            trigger_type="cli",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="tenant-a",
        )
        SyncPlanReviewRepository(self.db_manager).upsert_review_request(
            job_id="job-cli-approve",
            plan_fingerprint="sha256:v2:cli-plan",
            config_snapshot_hash="sha256:v2:cli-config",
            high_risk_operation_count=1,
        )

    def _args(self, *, org_id: str, ttl_minutes: int | None = 30) -> argparse.Namespace:
        return argparse.Namespace(
            db_path=self.db_path,
            org_id=org_id,
            job_id="job-cli-approve",
            reviewer="cli-admin",
            notes="approved from CLI",
            ttl_minutes=ttl_minutes,
        )

    def test_cli_approval_uses_tenant_context_and_shared_workflow(self) -> None:
        SettingsRepository(self.db_manager).set_value(
            "automatic_replay_enabled",
            "true",
            "bool",
            org_id="tenant-a",
        )
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = _handle_approve_plan(self._args(org_id="tenant-a"))

        self.assertEqual(exit_code, 0)
        self.assertIn("organization: tenant-a", stdout.getvalue())
        self.assertIn("replay_request_id:", stdout.getvalue())
        replay_records = SyncReplayRequestRepository(self.db_manager).list_request_records(
            org_id="tenant-a",
            limit=10,
        )
        self.assertEqual(len(replay_records), 1)
        audit_records = WebAuditLogRepository(self.db_manager).list_recent_logs(limit=10)
        approval_audit = next(item for item in audit_records if item.action_type == "plan_review.approve")
        self.assertEqual(approval_audit.org_id, "tenant-a")
        self.assertEqual(approval_audit.payload["channel"], "cli")

    def test_cli_approval_rejects_cross_tenant_access(self) -> None:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            exit_code = _handle_approve_plan(self._args(org_id="tenant-b"))

        self.assertEqual(exit_code, 1)
        self.assertIn("does not belong", stderr.getvalue())
        review = SyncPlanReviewRepository(self.db_manager).get_review_record_by_job_id("job-cli-approve")
        self.assertEqual(review.status, "pending")


if __name__ == "__main__":
    unittest.main()
