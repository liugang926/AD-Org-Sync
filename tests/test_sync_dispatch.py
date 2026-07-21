import unittest
import os
import time
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from sync_app.services.sync_dispatch import (
    _DispatchCancelFlag,
    _LeaseHeartbeat,
    enqueue_sync_job,
    run_sync_request,
)
from sync_app.storage.local_db import (
    DatabaseManager,
    SettingsRepository,
    SyncJobRepository,
    WebAuditLogRepository,
)
from tests.helpers.execution_plans import create_eligible_execution_plan


class SyncDispatchTests(unittest.TestCase):
    def test_unlabeled_runtime_blocks_apply_but_not_dry_run_and_audits_context(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "dispatch-unlabeled.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()
            SettingsRepository(db_manager).set_value(
                "web_bind_host",
                "0.0.0.0",
                "string",
            )

            with patch.dict(
                os.environ,
                {"AD_ORG_SYNC_ENVIRONMENT_LABEL": ""},
            ):
                apply_result = enqueue_sync_job(
                    db_path=str(db_path),
                    execution_mode="apply",
                    trigger_type="web",
                    org_id="default",
                    config_path="config.ini",
                    requested_by="alice",
                )
                dry_run_result = enqueue_sync_job(
                    db_path=str(db_path),
                    execution_mode="dry_run",
                    trigger_type="web",
                    org_id="default",
                    config_path="config.ini",
                    requested_by="alice",
                )

            self.assertFalse(apply_result.accepted)
            self.assertIsNone(apply_result.job)
            self.assertIn("unlabeled", apply_result.message)
            self.assertTrue(dry_run_result.accepted)
            logs = WebAuditLogRepository(db_manager).list_recent_logs(10)
            blocked = next(
                item
                for item in logs
                if item.action_type == "high_risk.apply.blocked"
            )
            self.assertEqual(blocked.result, "blocked")
            self.assertEqual(
                blocked.payload["reason_code"],
                "high_risk.blocker.environment_unlabeled",
            )
            self.assertEqual(blocked.payload["organization_id"], "default")
            self.assertEqual(
                blocked.payload["environment_label"],
                "Unlabeled environment",
            )

    def test_enqueue_sync_job_prevents_duplicate_active_job_for_same_org(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "dispatch.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()

            first = enqueue_sync_job(
                db_path=str(db_path),
                execution_mode="dry_run",
                trigger_type="web",
                org_id="default",
                config_path="config.ini",
                requested_by="alice",
            )
            second = enqueue_sync_job(
                db_path=str(db_path),
                execution_mode="apply",
                trigger_type="web",
                org_id="default",
                config_path="config.ini",
                requested_by="bob",
            )

            self.assertTrue(first.accepted)
            self.assertIsNotNone(first.job)
            self.assertFalse(second.accepted)
            self.assertIsNotNone(second.job)
            self.assertEqual(second.job.job_id, first.job.job_id)
            self.assertIn(first.job.job_id, second.message)

            job_record = SyncJobRepository(db_manager).get_job_record(first.job.job_id)
            self.assertEqual(job_record.status, "QUEUED")
            self.assertEqual(job_record.requested_by, "alice")
            self.assertEqual(job_record.requested_config_path, "config.ini")

    def test_fail_expired_execution_jobs_marks_stale_leases_failed(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "dispatch-expired.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()
            job_repo = SyncJobRepository(db_manager)
            job_repo.create_job(
                job_id="job-expired-001",
                trigger_type="web",
                execution_mode="apply",
                status="QUEUED",
                org_id="default",
                requested_by="admin",
                requested_config_path="config.ini",
            )

            claimed = job_repo.claim_job("job-expired-001", worker_id="worker-1", lease_seconds=60)
            self.assertIsNotNone(claimed)
            job_repo.update_job(
                "job-expired-001",
                lease_expires_at="2000-01-01T00:00:00+00:00",
            )
            job_repo.mark_phase_started("job-expired-001", "apply")

            expired_job_ids = job_repo.fail_expired_execution_jobs()
            refreshed = job_repo.get_job_record("job-expired-001")

            self.assertIn("job-expired-001", expired_job_ids)
            self.assertEqual(refreshed.status, "FAILED")
            self.assertTrue(refreshed.ended_at)
            self.assertEqual(refreshed.lease_owner, "")
            self.assertEqual(refreshed.lease_expires_at, "")
            self.assertEqual(refreshed.current_phase, "apply")
            self.assertIn("inspect operation logs", refreshed.recovery_hint)
            self.assertTrue(refreshed.summary["recovery_required"])

    def test_lease_heartbeat_marks_dispatch_cancelled_when_renewal_fails(self):
        heartbeat = _LeaseHeartbeat(
            db_path="ignored.db",
            job_id="job-lease-lost",
            worker_id="worker-1",
            lease_seconds=60,
            heartbeat_seconds=0.01,
        )
        fake_repo = SimpleNamespace(renew_lease=lambda *args, **kwargs: False)
        user_cancel_flag = SimpleNamespace(is_cancelled=False)
        dispatch_cancel_flag = _DispatchCancelFlag(user_cancel_flag, heartbeat)

        with patch("sync_app.services.sync_dispatch._open_job_repo", return_value=(None, fake_repo)):
            heartbeat.start()
            deadline = time.time() + 1
            while not heartbeat.lease_lost and time.time() < deadline:
                time.sleep(0.01)
            heartbeat.stop()

        self.assertTrue(heartbeat.lease_lost)
        self.assertTrue(dispatch_cancel_flag.is_cancelled)

    def test_run_sync_request_reuses_the_queued_job_id(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "dispatch-inline.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()

            def fake_run_sync_job(**kwargs):
                inline_db = DatabaseManager(db_path=str(db_path))
                inline_db.initialize()
                SyncJobRepository(inline_db).update_job(
                    kwargs["job_id"],
                    status="COMPLETED",
                    ended=True,
                    clear_lease=True,
                    summary={"ok": True},
                )
                return {"job_id": kwargs["job_id"], "error_count": 0}

            with patch("sync_app.services.runtime.run_sync_job", side_effect=fake_run_sync_job) as mock_run:
                result = run_sync_request(
                    execution_mode="dry_run",
                    trigger_type="cli",
                    db_path=str(db_path),
                    config_path="inline.ini",
                    org_id="default",
                    requested_by="cli-user",
                )

            self.assertEqual(mock_run.call_count, 1)
            called_kwargs = mock_run.call_args.kwargs
            self.assertEqual(result["job_id"], called_kwargs["job_id"])
            self.assertEqual(called_kwargs["active_job_guard_id"], called_kwargs["job_id"])
            self.assertEqual(called_kwargs["requested_by"], "cli-user")

            job_record = SyncJobRepository(db_manager).get_job_record(result["job_id"])
            self.assertEqual(job_record.status, "COMPLETED")
            self.assertEqual(job_record.requested_by, "cli-user")
            self.assertEqual(job_record.requested_config_path, "inline.ini")
            self.assertEqual(SyncJobRepository(db_manager).count_jobs(), 1)

    def test_enqueue_sync_job_blocks_scheduled_apply_without_recent_green_dry_run(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "dispatch-schedule-blocked.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()

            result = enqueue_sync_job(
                db_path=str(db_path),
                execution_mode="apply",
                trigger_type="schedule",
                org_id="default",
                config_path="config.ini",
                requested_by="scheduler",
            )

            self.assertFalse(result.accepted)
            self.assertIsNone(result.job)
            self.assertIn("no completed Dry Run plan", result.message)
            self.assertEqual(SyncJobRepository(db_manager).count_jobs(), 0)

    def test_enqueue_sync_job_blocks_scheduled_apply_after_dry_run_with_errors(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "dispatch-schedule-errors.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()
            job_repo = SyncJobRepository(db_manager)
            job_repo.create_job(
                job_id="job-dry-run-with-errors",
                trigger_type="unit_test",
                execution_mode="dry_run",
                status="COMPLETED_WITH_ERRORS",
                org_id="default",
                started_at=(datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            )

            result = enqueue_sync_job(
                db_path=str(db_path),
                execution_mode="apply",
                trigger_type="schedule",
                org_id="default",
                config_path="config.ini",
                requested_by="scheduler",
            )

            self.assertFalse(result.accepted)
            self.assertIsNone(result.job)
            self.assertIn("no completed Dry Run plan", result.message)
            self.assertEqual(SyncJobRepository(db_manager).count_jobs(), 1)

    def test_enqueue_sync_job_allows_scheduled_apply_after_manual_apply_and_green_dry_run(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "dispatch-schedule-ready.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()
            create_eligible_execution_plan(
                db_manager,
                job_id="job-dry-run-green",
                org_id="default",
                high_risk_operation_count=0,
                approved=True,
            )
            job_repo = SyncJobRepository(db_manager)
            job_repo.create_job(
                job_id="job-manual-apply-green",
                trigger_type="web",
                execution_mode="apply",
                status="COMPLETED",
                org_id="default",
                started_at=(datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            )
            SettingsRepository(db_manager).set_value(
                "schedule_execution_mode",
                "apply",
                "string",
                org_id="default",
            )

            result = enqueue_sync_job(
                db_path=str(db_path),
                execution_mode="apply",
                trigger_type="schedule",
                org_id="default",
                config_path="config.ini",
                requested_by="scheduler",
            )

            self.assertTrue(result.accepted)
            self.assertIsNotNone(result.job)
            self.assertEqual(SyncJobRepository(db_manager).count_jobs(), 3)
            self.assertEqual(result.job.plan_source_job_id, "job-dry-run-green")

    def test_enqueue_sync_job_does_not_reuse_a_consumed_plan(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "dispatch-plan-once.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()
            create_eligible_execution_plan(
                db_manager,
                job_id="job-plan-once",
                approved=True,
            )

            first = enqueue_sync_job(
                db_path=str(db_path),
                execution_mode="apply",
                trigger_type="web",
                org_id="default",
                config_path="config.ini",
                requested_by="alice",
                plan_source_job_id="job-plan-once",
            )
            self.assertTrue(first.accepted)
            SyncJobRepository(db_manager).update_job(
                first.job.job_id,
                status="COMPLETED",
                ended=True,
            )

            second = enqueue_sync_job(
                db_path=str(db_path),
                execution_mode="apply",
                trigger_type="web",
                org_id="default",
                config_path="config.ini",
                requested_by="alice",
                plan_source_job_id="job-plan-once",
            )

            self.assertFalse(second.accepted)
            self.assertIsNone(second.job)
            self.assertIn("already has an Apply job", second.message)
            self.assertEqual(SyncJobRepository(db_manager).count_jobs(), 2)


if __name__ == "__main__":
    unittest.main()
