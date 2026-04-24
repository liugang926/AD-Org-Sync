import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from sync_app.services.external_integrations import (
    OutboxWebhookNotificationClient,
    approve_job_review,
    emit_integration_event,
    emit_job_lifecycle_events,
    emit_notification_webhook,
    flush_integration_outbox,
    retry_failed_outbox_deliveries,
    retry_outbox_delivery,
)
from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories.conflicts import SyncPlanReviewRepository
from sync_app.storage.repositories.jobs import SyncJobRepository
from sync_app.storage.repositories.organizations import OrganizationRepository
from sync_app.storage.repositories.system import (
    IntegrationWebhookOutboxRepository,
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

    def test_emit_integration_event_can_queue_without_inline_delivery(self):
        temp_dir, db_manager = self._create_db_manager()
        self.addCleanup(temp_dir.cleanup)

        subscription_repo = IntegrationWebhookSubscriptionRepository(db_manager)
        outbox_repo = IntegrationWebhookOutboxRepository(db_manager)
        subscription_repo.upsert_subscription(
            org_id="default",
            event_type="job.failed",
            target_url="https://example.invalid/hooks/failed",
            secret="shared-secret-queue",
            description="Failure delivery",
            is_enabled=True,
        )

        with patch("sync_app.services.external_integrations.requests.post") as mock_post:
            result = emit_integration_event(
                db_manager,
                org_id="default",
                event_type="job.failed",
                payload={"job": {"job_id": "job-queued-001"}},
                dispatch_inline=False,
                dispatch_async=False,
            )

        self.assertEqual(result["queued_count"], 1)
        self.assertFalse(result["async_dispatch_started"])
        self.assertEqual(mock_post.call_count, 0)

        delivery_records = outbox_repo.list_delivery_records(org_id="default", limit=10)
        self.assertEqual(len(delivery_records), 1)
        self.assertEqual(delivery_records[0].status, "pending")
        self.assertEqual(delivery_records[0].event_type, "job.failed")

        response = Mock()
        response.ok = True
        response.status_code = 200
        response.reason = "OK"
        response.text = ""
        with patch("sync_app.services.external_integrations.requests.post", return_value=response) as mock_post:
            flush_result = flush_integration_outbox(db_manager, org_id="default")

        self.assertEqual(flush_result["delivered_count"], 1)
        self.assertEqual(mock_post.call_count, 1)
        refreshed_delivery = outbox_repo.get_delivery_record(int(delivery_records[0].id or 0))
        self.assertIsNotNone(refreshed_delivery)
        self.assertEqual(refreshed_delivery.status, "delivered")

    def test_flush_integration_outbox_retries_failed_deliveries(self):
        temp_dir, db_manager = self._create_db_manager()
        self.addCleanup(temp_dir.cleanup)

        subscription_repo = IntegrationWebhookSubscriptionRepository(db_manager)
        outbox_repo = IntegrationWebhookOutboxRepository(db_manager)
        subscription_repo.upsert_subscription(
            org_id="default",
            event_type="job.completed",
            target_url="https://example.invalid/hooks/retry",
            secret="shared-secret-retry",
            description="Retry delivery",
            is_enabled=True,
        )
        emit_integration_event(
            db_manager,
            org_id="default",
            event_type="job.completed",
            payload={"job": {"job_id": "job-retry-001"}},
            dispatch_inline=False,
            dispatch_async=False,
        )

        failed_response = Mock()
        failed_response.ok = False
        failed_response.status_code = 503
        failed_response.reason = "Service Unavailable"
        failed_response.text = "temporary outage"
        with patch("sync_app.services.external_integrations.requests.post", return_value=failed_response):
            first_flush = flush_integration_outbox(db_manager, org_id="default")

        self.assertEqual(first_flush["retrying_count"], 1)
        first_record = outbox_repo.list_delivery_records(org_id="default", limit=1)[0]
        self.assertEqual(first_record.status, "retrying")
        self.assertEqual(first_record.attempt_count, 1)
        self.assertTrue(first_record.next_attempt_at)

        with db_manager.transaction() as conn:
            conn.execute(
                """
                UPDATE integration_webhook_outbox
                SET next_attempt_at = '2000-01-01T00:00:00+00:00'
                WHERE id = ?
                """,
                (int(first_record.id or 0),),
            )

        success_response = Mock()
        success_response.ok = True
        success_response.status_code = 200
        success_response.reason = "OK"
        success_response.text = ""
        with patch("sync_app.services.external_integrations.requests.post", return_value=success_response):
            second_flush = flush_integration_outbox(db_manager, org_id="default")

        self.assertEqual(second_flush["delivered_count"], 1)
        second_record = outbox_repo.get_delivery_record(int(first_record.id or 0))
        self.assertIsNotNone(second_record)
        self.assertEqual(second_record.status, "delivered")
        self.assertEqual(second_record.attempt_count, 2)

    def test_emit_notification_webhook_queues_markdown_delivery(self):
        temp_dir, db_manager = self._create_db_manager()
        self.addCleanup(temp_dir.cleanup)

        outbox_repo = IntegrationWebhookOutboxRepository(db_manager)
        client = OutboxWebhookNotificationClient(
            db_manager=db_manager,
            org_id="default",
            webhook_url="https://example.invalid/webhook/ops",
            source="dry_run.digest",
            dispatch_inline=False,
            dispatch_async=False,
        )

        with patch("sync_app.services.external_integrations.requests.post") as mock_post:
            queued = client.send_message("## Test\n\n> queued via outbox")

        self.assertTrue(queued)
        self.assertEqual(mock_post.call_count, 0)
        records = outbox_repo.list_delivery_records(org_id="default", limit=10)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].event_type, "ops.notification")
        self.assertEqual(records[0].status, "pending")

        response = Mock()
        response.ok = True
        response.status_code = 200
        response.reason = "OK"
        response.text = ""
        response.json.return_value = {"errcode": 0, "errmsg": "ok"}
        with patch("sync_app.services.external_integrations.requests.post", return_value=response) as mock_post:
            flush_result = flush_integration_outbox(db_manager, org_id="default")

        self.assertEqual(flush_result["delivered_count"], 1)
        self.assertEqual(mock_post.call_count, 1)
        request_payload = mock_post.call_args.kwargs["data"].decode("utf-8")
        self.assertIn("\"msgtype\": \"markdown\"", request_payload)
        self.assertIn("queued via outbox", request_payload)
        self.assertEqual(mock_post.call_args.kwargs["headers"], {"Content-Type": "application/json"})
        refreshed = outbox_repo.get_delivery_record(int(records[0].id or 0))
        self.assertIsNotNone(refreshed)
        self.assertEqual(refreshed.status, "delivered")

    def test_retry_outbox_delivery_requeues_failed_delivery(self):
        temp_dir, db_manager = self._create_db_manager()
        self.addCleanup(temp_dir.cleanup)

        subscription_repo = IntegrationWebhookSubscriptionRepository(db_manager)
        outbox_repo = IntegrationWebhookOutboxRepository(db_manager)
        subscription_repo.upsert_subscription(
            org_id="default",
            event_type="job.failed",
            target_url="https://example.invalid/hooks/requeue",
            secret="shared-secret-requeue",
            description="Replay delivery",
            is_enabled=True,
        )
        emit_integration_event(
            db_manager,
            org_id="default",
            event_type="job.failed",
            payload={"job": {"job_id": "job-requeue-001"}},
            dispatch_inline=False,
            dispatch_async=False,
        )
        delivery = outbox_repo.list_delivery_records(org_id="default", limit=1)[0]
        with db_manager.transaction() as conn:
            conn.execute(
                """
                UPDATE integration_webhook_outbox
                SET max_attempts = 1
                WHERE id = ?
                """,
                (int(delivery.id or 0),),
            )

        failed_response = Mock()
        failed_response.ok = False
        failed_response.status_code = 503
        failed_response.reason = "Service Unavailable"
        failed_response.text = "temporary outage"
        with patch("sync_app.services.external_integrations.requests.post", return_value=failed_response):
            flush_result = flush_integration_outbox(db_manager, org_id="default")

        self.assertEqual(flush_result["failed_count"], 1)
        failed_record = outbox_repo.get_delivery_record(int(delivery.id or 0))
        self.assertIsNotNone(failed_record)
        self.assertEqual(failed_record.status, "failed")
        self.assertEqual(failed_record.attempt_count, 1)
        self.assertEqual(failed_record.max_attempts, 1)

        retry_result = retry_outbox_delivery(
            db_manager,
            org_id="default",
            delivery_id=int(delivery.id or 0),
        )
        retried_record = retry_result["delivery"]
        self.assertEqual(retried_record.status, "pending")
        self.assertEqual(retried_record.max_attempts, 2)
        self.assertTrue(retried_record.next_attempt_at)

        success_response = Mock()
        success_response.ok = True
        success_response.status_code = 200
        success_response.reason = "OK"
        success_response.text = ""
        with patch("sync_app.services.external_integrations.requests.post", return_value=success_response):
            second_flush = flush_integration_outbox(db_manager, org_id="default")

        self.assertEqual(second_flush["delivered_count"], 1)
        final_record = outbox_repo.get_delivery_record(int(delivery.id or 0))
        self.assertIsNotNone(final_record)
        self.assertEqual(final_record.status, "delivered")
        self.assertEqual(final_record.attempt_count, 2)

    def test_retry_failed_outbox_deliveries_requeues_multiple_failed_records(self):
        temp_dir, db_manager = self._create_db_manager()
        self.addCleanup(temp_dir.cleanup)

        outbox_repo = IntegrationWebhookOutboxRepository(db_manager)
        for index in range(2):
            delivery = outbox_repo.enqueue_delivery(
                org_id="default",
                event_type="ops.notification",
                delivery_id=f"delivery-bulk-{index}",
                target_url=f"https://example.invalid/hooks/bulk/{index}",
                payload={
                    "_delivery_kind": "notification.markdown",
                    "body": {"msgtype": "markdown", "markdown": {"content": f"bulk {index}"}},
                },
                max_attempts=1,
                next_attempt_at="2026-04-22T00:00:00+00:00",
            )
            outbox_repo.mark_delivery_retry(
                int(delivery.id or 0),
                last_status="503 Service Unavailable",
                last_error=f"temporary outage {index}",
                attempted_at="2026-04-22T01:00:00+00:00",
                retry_delay_seconds=60,
            )

        result = retry_failed_outbox_deliveries(
            db_manager,
            org_id="default",
            limit=10,
        )

        self.assertEqual(result["retried_count"], 2)
        records = outbox_repo.list_delivery_records(org_id="default", limit=10)
        self.assertTrue(records)
        self.assertTrue(all(record.status == "pending" for record in records[:2]))

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
