import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from sync_app.services.sync_dispatch import enqueue_sync_job, run_sync_request
from sync_app.storage.local_db import DatabaseManager, SyncJobRepository


class SyncDispatchTests(unittest.TestCase):
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

            expired_job_ids = job_repo.fail_expired_execution_jobs()
            refreshed = job_repo.get_job_record("job-expired-001")

            self.assertIn("job-expired-001", expired_job_ids)
            self.assertEqual(refreshed.status, "FAILED")
            self.assertTrue(refreshed.ended_at)
            self.assertEqual(refreshed.lease_owner, "")
            self.assertEqual(refreshed.lease_expires_at, "")

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


if __name__ == "__main__":
    unittest.main()
