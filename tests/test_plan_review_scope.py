import unittest
from pathlib import Path

from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories.conflicts import SyncPlanReviewRepository
from sync_app.storage.repositories.jobs import SyncJobRepository


class PlanReviewScopeTests(unittest.TestCase):
    def setUp(self):
        test_root = Path.cwd() / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        self.db_path = test_root / "plan_review_scope.db"
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.db_path) + suffix)
            if candidate.exists():
                candidate.unlink()
        self.manager = DatabaseManager(db_path=str(self.db_path))
        self.manager.initialize(create_startup_snapshot=False, verify_integrity=True)

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.db_path) + suffix)
            if candidate.exists():
                try:
                    candidate.unlink()
                except PermissionError:
                    pass

    def test_approved_plan_cannot_be_reused_by_another_organization(self):
        job_repo = SyncJobRepository(self.manager)
        review_repo = SyncPlanReviewRepository(self.manager)
        job_repo.create_job("default-plan", "unit_test", "dry_run", "COMPLETED", org_id="default")
        job_repo.create_job("asia-plan", "unit_test", "apply", "PLANNING", org_id="asia")
        review_repo.upsert_review_request(
            job_id="default-plan",
            plan_fingerprint="sha256:v2:same-plan",
            config_snapshot_hash="sha256:v2:same-config",
            high_risk_operation_count=1,
        )
        review_repo.approve_review(
            "default-plan",
            reviewer_username="reviewer",
            expires_at="2999-01-01T00:00:00+00:00",
        )

        default_match = review_repo.find_matching_approved_review(
            org_id="default",
            plan_fingerprint="sha256:v2:same-plan",
            config_snapshot_hash="sha256:v2:same-config",
            now_iso="2026-01-01T00:00:00+00:00",
        )
        asia_match = review_repo.find_matching_approved_review(
            org_id="asia",
            plan_fingerprint="sha256:v2:same-plan",
            config_snapshot_hash="sha256:v2:same-config",
            now_iso="2026-01-01T00:00:00+00:00",
        )

        self.assertEqual(default_match.job_id, "default-plan")
        self.assertIsNone(asia_match)


if __name__ == "__main__":
    unittest.main()
