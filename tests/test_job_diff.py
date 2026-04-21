import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from sync_app.services.job_diff import build_job_comparison_summary
from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories.conflicts import SyncConflictRepository
from sync_app.storage.repositories.jobs import PlannedOperationRepository, SyncJobRepository


class JobDiffTests(unittest.TestCase):
    def test_build_job_comparison_summary_detects_added_removed_and_changed_items(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "job_diff.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()

            job_repo = SyncJobRepository(db_manager)
            planned_repo = PlannedOperationRepository(db_manager)
            conflict_repo = SyncConflictRepository(db_manager)

            job_repo.create_job(
                "job-baseline",
                trigger_type="unit_test",
                execution_mode="dry_run",
                status="COMPLETED",
                started_at="2026-04-16T00:00:00+00:00",
            )
            job_repo.create_job(
                "job-current",
                trigger_type="unit_test",
                execution_mode="dry_run",
                status="COMPLETED",
                started_at="2026-04-17T00:00:00+00:00",
            )

            planned_repo.add_operation(
                "job-baseline",
                "user",
                "create_user",
                source_id="alice",
                target_dn="CN=alice,OU=Managed,DC=example,DC=local",
                desired_state={"mail": "alice@example.com"},
                risk_level="normal",
            )
            planned_repo.add_operation(
                "job-baseline",
                "group",
                "create_group",
                source_id="dept-01",
                target_dn="CN=Dept01,OU=Managed,DC=example,DC=local",
                desired_state={"scope": "security"},
                risk_level="normal",
            )
            conflict_repo.add_conflict(
                job_id="job-baseline",
                conflict_type="multiple_ad_candidates",
                source_id="alice",
                target_key="identity_binding",
                message="baseline conflict",
            )

            planned_repo.add_operation(
                "job-current",
                "user",
                "create_user",
                source_id="alice",
                target_dn="CN=alice,OU=Managed,DC=example,DC=local",
                desired_state={"mail": "alice@corp.example"},
                risk_level="high",
            )
            planned_repo.add_operation(
                "job-current",
                "user",
                "update_user",
                source_id="bob",
                target_dn="CN=bob,OU=Managed,DC=example,DC=local",
                desired_state={"title": "Engineer"},
                risk_level="normal",
            )
            conflict_repo.add_conflict(
                job_id="job-current",
                conflict_type="multiple_ad_candidates",
                source_id="alice",
                target_key="identity_binding",
                message="baseline conflict",
            )
            conflict_repo.add_conflict(
                job_id="job-current",
                conflict_type="shared_ad_account",
                source_id="bob",
                target_key="bob",
                message="new shared account conflict",
            )

            job_repo.update_job(
                "job-baseline",
                planned_operation_count=2,
                summary={"planned_operation_count": 2, "conflict_count": 1, "high_risk_operation_count": 0},
            )
            job_repo.update_job(
                "job-current",
                planned_operation_count=2,
                summary={"planned_operation_count": 2, "conflict_count": 2, "high_risk_operation_count": 1},
            )

            summary = build_job_comparison_summary(
                current_job=job_repo.get_job_record("job-current"),
                baseline_job=job_repo.get_job_record("job-baseline"),
                planned_operation_repo=planned_repo,
                conflict_repo=conflict_repo,
            )

        self.assertTrue(summary["changed"])
        self.assertEqual(summary["operation_diff"]["added_count"], 1)
        self.assertEqual(summary["operation_diff"]["removed_count"], 1)
        self.assertEqual(summary["operation_diff"]["changed_count"], 1)
        self.assertEqual(summary["operation_diff"]["high_risk_changed_count"], 1)
        self.assertEqual(summary["conflict_diff"]["added_count"], 1)
        self.assertEqual(summary["conflict_diff"]["removed_count"], 0)
        self.assertEqual(summary["summary_delta"]["conflict_delta"], 1)
        self.assertEqual(summary["summary_delta"]["high_risk_operation_delta"], 1)
        self.assertTrue(any(item["name"] == "user" for item in summary["operation_diff"]["object_type_breakdown"]))
        self.assertTrue(any(item["name"] == "shared_ad_account" for item in summary["conflict_diff"]["conflict_type_breakdown"]))


if __name__ == "__main__":
    unittest.main()
