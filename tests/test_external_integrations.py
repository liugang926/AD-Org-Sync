import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from sync_app.services.external_integrations import (
    approve_job_review,
    emit_job_lifecycle_events,
)
from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories.conflicts import SyncPlanReviewRepository
from sync_app.storage.repositories.jobs import SyncJobRepository
from sync_app.storage.repositories.organizations import OrganizationRepository
from sync_app.storage.repositories.system import (
    IntegrationWebhookSubscriptionRepository,
    SettingsRepository,
    SyncReplayRequestRepository,
)


class ExternalIntegrationServiceTests(unittest.TestCase):
    def _create_db_manager(self) -> tuple[tempfile.TemporaryDirectory[str], DatabaseManager]:
        temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(temp_dir.name) / "external_integrations.db"
        db_manager = DatabaseManager(str(db_path))
        db_manager.initialize()
        OrganizationRepository(db_manager).ensure_default(
            config_path=str((Path(temp_dir.name) / "config.ini").resolve())
        )
        return temp_dir, db_manager

    def test_emit_job_lifecycle_events_delivers_matching_webhooks(self):
        temp_dir, db_manager = self._create_db_manager()
        self.addCleanup(temp_dir.cleanup)

        job_repo = SyncJobRepository(db_manager)
        review_repo = SyncPlanReviewRepository(db_manager)
        subscription_repo = IntegrationWebhookSubscriptionRepository(db_manager)

        job_repo.create_job(
            "job-ext-001",
            trigger_type="manual",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="default",
        )
        job_repo.update_job(
            "job-ext-001",
            summary={
                "planned_operation_count": 6,
                "conflict_count": 2,
                "high_risk_operation_count": 3,
                "review_required": True,
                "plan_fingerprint": "plan-ext-001",
            },
            ended=True,
        )
        review_repo.upsert_review_request(
            job_id="job-ext-001",
            plan_fingerprint="plan-ext-001",
            config_snapshot_hash="cfg-ext-001",
            high_risk_operation_count=3,
        )
        subscription_repo.upsert_subscription(
            org_id="default",
            event_type="job.completed",
            target_url="https://example.invalid/hooks/completed",
            secret="shared-secret-1",
            description="Completed delivery",
            is_enabled=True,
        )
        subscription_repo.upsert_subscription(
            org_id="default",
            event_type="job.review_required",
            target_url="https://example.invalid/hooks/review",
            secret="shared-secret-2",
            description="Review delivery",
            is_enabled=True,
        )

        response = Mock()
        response.ok = True
        response.status_code = 200
        response.reason = "OK"
        response.text = ""
        with patch("sync_app.services.external_integrations.requests.post", return_value=response) as mock_post:
            result = emit_job_lifecycle_events(db_manager, job_id="job-ext-001")

        self.assertEqual(result["emitted_events"], ["job.completed", "job.review_required"])
        self.assertEqual(mock_post.call_count, 2)
        first_headers = mock_post.call_args_list[0].kwargs["headers"]
        second_headers = mock_post.call_args_list[1].kwargs["headers"]
        self.assertEqual(first_headers["X-AD-Org-Sync-Event"], "job.completed")
        self.assertTrue(first_headers["X-AD-Org-Sync-Signature"].startswith("sha256="))
        self.assertEqual(second_headers["X-AD-Org-Sync-Event"], "job.review_required")
        self.assertTrue(second_headers["X-AD-Org-Sync-Signature"].startswith("sha256="))

        records = subscription_repo.list_subscription_records(org_id="default", limit=10)
        status_by_event = {record.event_type: record.last_status for record in records}
        self.assertEqual(status_by_event["job.completed"], "200 OK")
        self.assertEqual(status_by_event["job.review_required"], "200 OK")

    def test_approve_job_review_is_idempotent_and_can_enqueue_replay(self):
        temp_dir, db_manager = self._create_db_manager()
        self.addCleanup(temp_dir.cleanup)

        job_repo = SyncJobRepository(db_manager)
        review_repo = SyncPlanReviewRepository(db_manager)
        replay_repo = SyncReplayRequestRepository(db_manager)
        settings_repo = SettingsRepository(db_manager)
        subscription_repo = IntegrationWebhookSubscriptionRepository(db_manager)

        settings_repo.set_value("automatic_replay_enabled", "true", "bool", org_id="default")
        job_repo.create_job(
            "job-ext-approve",
            trigger_type="manual",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="default",
        )
        job_repo.update_job(
            "job-ext-approve",
            summary={
                "planned_operation_count": 4,
                "conflict_count": 0,
                "high_risk_operation_count": 1,
                "review_required": True,
                "plan_fingerprint": "plan-approve-001",
            },
            ended=True,
        )
        review_repo.upsert_review_request(
            job_id="job-ext-approve",
            plan_fingerprint="plan-approve-001",
            config_snapshot_hash="cfg-approve-001",
            high_risk_operation_count=1,
        )
        subscription_repo.upsert_subscription(
            org_id="default",
            event_type="review.approved",
            target_url="https://example.invalid/hooks/approved",
            secret="shared-secret-approve",
            description="Approval delivery",
            is_enabled=True,
        )

        response = Mock()
        response.ok = True
        response.status_code = 200
        response.reason = "OK"
        response.text = ""
        with patch("sync_app.services.external_integrations.requests.post", return_value=response) as mock_post:
            first_result = approve_job_review(
                db_manager,
                org_id="default",
                job_id="job-ext-approve",
                reviewer_username="workflow-bot",
                review_notes="Approved by external workflow",
            )
            second_result = approve_job_review(
                db_manager,
                org_id="default",
                job_id="job-ext-approve",
                reviewer_username="workflow-bot",
                review_notes="Approved by external workflow",
            )

        self.assertTrue(first_result["fresh_approval"])
        self.assertIsNotNone(first_result["replay_request_id"])
        self.assertFalse(second_result["fresh_approval"])
        self.assertIsNone(second_result["replay_request_id"])
        self.assertEqual(mock_post.call_count, 1)

        updated_review = review_repo.get_review_record_by_job_id("job-ext-approve")
        self.assertIsNotNone(updated_review)
        self.assertEqual(updated_review.status, "approved")
        self.assertEqual(updated_review.reviewer_username, "workflow-bot")
        self.assertEqual(len(replay_repo.list_request_records(org_id="default", limit=10)), 1)


if __name__ == "__main__":
    unittest.main()
