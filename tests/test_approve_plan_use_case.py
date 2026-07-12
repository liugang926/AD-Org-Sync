from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

from sync_app.application import ApproveSyncPlanUseCase, TenantContext
from sync_app.storage.local_db import (
    DatabaseManager,
    SettingsRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
    SyncReplayRequestRepository,
    WebAuditLogRepository,
)


class _RecordingPublisher:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def publish_review_approved(self, *, tenant, job, review, replay_request_id) -> None:
        self.events.append(
            {
                "org_id": tenant.org_id,
                "actor": tenant.actor_username,
                "channel": tenant.channel,
                "job_id": job.job_id,
                "review_status": review.status,
                "replay_request_id": replay_request_id,
            }
        )


class ApproveSyncPlanUseCaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_manager = DatabaseManager(db_path=f"{self.temp_dir.name}/app.db")
        self.db_manager.initialize(create_startup_snapshot=False)
        self.job_repo = SyncJobRepository(self.db_manager)
        self.review_repo = SyncPlanReviewRepository(self.db_manager)

    def _create_review(self, *, job_id: str, org_id: str) -> None:
        self.job_repo.create_job(
            job_id,
            trigger_type="manual",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id=org_id,
        )
        self.review_repo.upsert_review_request(
            job_id=job_id,
            plan_fingerprint=f"sha256:v2:{job_id}",
            config_snapshot_hash=f"sha256:v2:config:{job_id}",
            high_risk_operation_count=1,
        )

    def test_tenant_context_requires_organization_and_actor(self) -> None:
        with self.assertRaisesRegex(ValueError, "Organization ID"):
            TenantContext.create(org_id="", actor_username="alice")
        with self.assertRaisesRegex(ValueError, "Actor username"):
            TenantContext.create(org_id="default", actor_username="")

    def test_approval_is_tenant_scoped_idempotent_and_emits_once(self) -> None:
        self._create_review(job_id="job-approve", org_id="tenant-a")
        SettingsRepository(self.db_manager).set_value(
            "automatic_replay_enabled",
            "true",
            "bool",
            org_id="tenant-a",
        )
        publisher = _RecordingPublisher()
        use_case = ApproveSyncPlanUseCase(self.db_manager, event_publisher=publisher)
        tenant = TenantContext.create(
            org_id="TENANT-A",
            actor_username=" alice ",
            channel="CLI",
        )

        first = use_case.execute(
            tenant,
            job_id="job-approve",
            review_notes="approved",
            ttl_minutes=15,
        )
        second = use_case.execute(tenant, job_id="job-approve", ttl_minutes=30)

        self.assertTrue(first.fresh_approval)
        self.assertFalse(second.fresh_approval)
        self.assertEqual(first.expires_at_iso, second.expires_at_iso)
        self.assertIsNotNone(first.replay_request_id)
        self.assertIsNone(second.replay_request_id)
        self.assertEqual(len(publisher.events), 1)
        self.assertEqual(publisher.events[0]["channel"], "cli")
        expiry = datetime.fromisoformat(first.expires_at_iso)
        self.assertIsNotNone(expiry.tzinfo)

        replay_records = SyncReplayRequestRepository(self.db_manager).list_request_records(
            org_id="tenant-a",
            limit=10,
        )
        self.assertEqual(len(replay_records), 1)
        audit_records = WebAuditLogRepository(self.db_manager).list_recent_logs(limit=10)
        approval_audits = [item for item in audit_records if item.action_type == "plan_review.approve"]
        self.assertEqual(len(approval_audits), 2)
        self.assertEqual(approval_audits[0].org_id, "tenant-a")

    def test_cross_tenant_and_invalid_ttl_are_rejected_without_mutation(self) -> None:
        self._create_review(job_id="job-scope", org_id="tenant-a")
        publisher = _RecordingPublisher()
        use_case = ApproveSyncPlanUseCase(self.db_manager, event_publisher=publisher)

        with self.assertRaisesRegex(ValueError, "does not belong"):
            use_case.execute(
                TenantContext.create(org_id="tenant-b", actor_username="bob"),
                job_id="job-scope",
            )
        with self.assertRaisesRegex(ValueError, "greater than zero"):
            use_case.execute(
                TenantContext.create(org_id="tenant-a", actor_username="alice"),
                job_id="job-scope",
                ttl_minutes=0,
            )

        review = self.review_repo.get_review_record_by_job_id("job-scope")
        self.assertEqual(review.status, "pending")
        self.assertEqual(publisher.events, [])

    def test_approval_replay_and_audit_roll_back_as_one_unit_of_work(self) -> None:
        self._create_review(job_id="job-rollback", org_id="tenant-a")
        SettingsRepository(self.db_manager).set_value(
            "automatic_replay_enabled",
            "true",
            "bool",
            org_id="tenant-a",
        )
        publisher = _RecordingPublisher()
        use_case = ApproveSyncPlanUseCase(self.db_manager, event_publisher=publisher)

        with patch.object(use_case.audit_repo, "add_log", side_effect=RuntimeError("audit unavailable")):
            with self.assertRaisesRegex(RuntimeError, "audit unavailable"):
                use_case.execute(
                    TenantContext.create(org_id="tenant-a", actor_username="alice"),
                    job_id="job-rollback",
                )

        review = self.review_repo.get_review_record_by_job_id("job-rollback")
        self.assertEqual(review.status, "pending")
        self.assertEqual(
            SyncReplayRequestRepository(self.db_manager).list_request_records(org_id="tenant-a", limit=10),
            [],
        )
        self.assertEqual(publisher.events, [])


if __name__ == "__main__":
    unittest.main()
