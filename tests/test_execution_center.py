from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from sync_app.services.execution_center import ExecutionCenterService
from sync_app.storage.local_db import (
    DatabaseManager,
    SettingsRepository,
    SourceDirectoryRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
)
from tests.helpers.execution_plans import create_eligible_execution_plan


class ExecutionCenterServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_manager = DatabaseManager(
            db_path=f"{self.temp_dir.name}/execution-center.db"
        )
        self.db_manager.initialize(create_startup_snapshot=False)
        self.job_repo = SyncJobRepository(self.db_manager)
        self.review_repo = SyncPlanReviewRepository(self.db_manager)
        self.source_repo = SourceDirectoryRepository(self.db_manager)
        self.service = ExecutionCenterService(
            job_repo=self.job_repo,
            review_repo=self.review_repo,
            source_directory_repo=self.source_repo,
            settings_repo=SettingsRepository(self.db_manager),
        )

    def _evaluate(self, job_id: str, **overrides):
        kwargs = {
            "org_id": "default",
            "organization_name": "Default Organization",
            "environment_label": "Local environment",
            "plan_job_id": job_id,
            "require_approval": True,
        }
        kwargs.update(overrides)
        return self.service.evaluate_plan(**kwargs)

    def test_approved_plan_is_bound_to_current_evidence(self) -> None:
        created = create_eligible_execution_plan(
            self.db_manager,
            job_id="plan-ready",
            approved=True,
        )

        evaluation = self._evaluate("plan-ready")

        self.assertTrue(evaluation.allowed)
        self.assertEqual(evaluation.context.organization_id, "default")
        self.assertEqual(
            evaluation.context.snapshot_version,
            f"#{created['snapshot_id']}",
        )
        self.assertEqual(evaluation.context.impact_count, 1)
        self.assertEqual(evaluation.review.status, "approved")

    def test_pending_review_and_changed_environment_block_apply(self) -> None:
        create_eligible_execution_plan(
            self.db_manager,
            job_id="plan-pending",
        )

        pending = self._evaluate("plan-pending")
        changed_environment = self._evaluate(
            "plan-pending",
            environment_label="Production",
        )

        self.assertFalse(pending.allowed)
        self.assertEqual(pending.reason_code, "execution.blocker.review_pending")
        self.assertEqual(
            pending.next_action_code,
            "execution.action.review_plan",
        )
        self.assertFalse(changed_environment.allowed)
        self.assertEqual(
            changed_environment.reason_code,
            "execution.blocker.environment_changed",
        )

    def test_current_config_change_invalidates_the_plan(self) -> None:
        created = create_eligible_execution_plan(
            self.db_manager,
            job_id="plan-config",
            approved=True,
        )

        evaluation = self._evaluate(
            "plan-config",
            current_config_fingerprint=(
                str(created["job"].config_snapshot_hash) + "-changed"
            ),
        )
        unavailable = self._evaluate(
            "plan-config",
            current_config_fingerprint=None,
        )

        self.assertFalse(evaluation.allowed)
        self.assertEqual(
            evaluation.reason_code,
            "execution.blocker.config_changed",
        )
        self.assertEqual(
            evaluation.next_action_code,
            "execution.action.run_dry_run",
        )
        self.assertFalse(unavailable.allowed)
        self.assertEqual(
            unavailable.reason_code,
            "execution.blocker.config_unavailable",
        )

    def test_scope_or_snapshot_change_invalidates_approval(self) -> None:
        created = create_eligible_execution_plan(
            self.db_manager,
            job_id="plan-scope",
            approved=True,
        )
        selection = created["selection"]
        self.source_repo.save_scope_selection(
            org_id="default",
            provider_id=selection["provider_id"],
            connector_id=selection["connector_id"],
            scope_type="full",
            username_strategy="employee_id",
            snapshot_id=created["snapshot_id"],
            requested_by="test",
        )

        changed_scope = self._evaluate("plan-scope")

        self.assertFalse(changed_scope.allowed)
        self.assertEqual(
            changed_scope.reason_code,
            "execution.blocker.scope_changed",
        )

        self.source_repo.save_scope_selection(
            org_id="default",
            provider_id=selection["provider_id"],
            connector_id=selection["connector_id"],
            scope_type="full",
            snapshot_id=created["snapshot_id"],
            requested_by="test",
        )
        with self.db_manager.transaction() as connection:
            connection.execute(
                "UPDATE source_directory_snapshots SET expires_at = ? WHERE id = ?",
                (
                    (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
                    created["snapshot_id"],
                ),
            )
        expired_snapshot = self._evaluate("plan-scope")
        self.assertEqual(
            expired_snapshot.reason_code,
            "execution.blocker.snapshot_expired",
        )

    def test_expired_or_consumed_plan_requires_a_new_dry_run(self) -> None:
        create_eligible_execution_plan(
            self.db_manager,
            job_id="plan-once",
            approved=True,
        )
        old_summary = dict(self.job_repo.get_job_record("plan-once").summary or {})
        old_summary["plan_generated_at"] = (
            datetime.now(timezone.utc) - timedelta(hours=25)
        ).isoformat()
        self.job_repo.update_job("plan-once", summary=old_summary)
        expired = self._evaluate("plan-once")
        self.assertEqual(expired.reason_code, "execution.blocker.plan_expired")

        old_summary["plan_generated_at"] = datetime.now(timezone.utc).isoformat()
        self.job_repo.update_job("plan-once", summary=old_summary)
        self.job_repo.create_job(
            "apply-once",
            trigger_type="web",
            execution_mode="apply",
            status="COMPLETED",
            org_id="default",
            plan_source_job_id="plan-once",
        )
        consumed = self._evaluate("plan-once")
        runtime_recheck = self._evaluate("plan-once", require_unused=False)
        self.assertEqual(
            consumed.reason_code,
            "execution.blocker.plan_already_applied",
        )
        self.assertTrue(runtime_recheck.allowed)

    def test_plan_from_another_organization_is_not_selectable(self) -> None:
        create_eligible_execution_plan(
            self.db_manager,
            job_id="tenant-a-plan",
            org_id="tenant-a",
            approved=True,
        )

        evaluation = self._evaluate("tenant-a-plan")

        self.assertFalse(evaluation.allowed)
        self.assertEqual(evaluation.reason_code, "execution.blocker.no_dry_run")
        self.assertEqual(evaluation.context.organization_id, "default")


if __name__ == "__main__":
    unittest.main()
