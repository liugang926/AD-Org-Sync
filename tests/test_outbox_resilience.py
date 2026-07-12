from __future__ import annotations

import tempfile
import unittest

from sync_app.services.external_integrations import _build_retry_delay_seconds
from sync_app.storage.local_db import DatabaseManager, IntegrationWebhookOutboxRepository


class IntegrationOutboxResilienceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_manager = DatabaseManager(db_path=f"{self.temp_dir.name}/app.db")
        self.db_manager.initialize(create_startup_snapshot=False)
        self.repo = IntegrationWebhookOutboxRepository(self.db_manager)

    def _enqueue(self, *, delivery_id: str = "delivery-1", max_attempts: int = 5):
        return self.repo.enqueue_delivery(
            org_id="tenant-a",
            event_type="job.completed",
            delivery_id=delivery_id,
            target_url="https://example.invalid/webhook",
            payload={"delivery_id": delivery_id},
            max_attempts=max_attempts,
            next_attempt_at="2026-01-01T00:00:00+00:00",
        )

    def test_enqueue_is_idempotent_for_the_same_delivery_identity(self) -> None:
        first = self._enqueue()
        second = self._enqueue()

        self.assertEqual(first.id, second.id)
        self.assertTrue(first.idempotency_key)
        self.assertEqual(self.repo.count_delivery_records(org_id="tenant-a"), 1)

    def test_expired_lease_is_reclaimed_with_a_new_fencing_token(self) -> None:
        delivery = self._enqueue()
        first_claim = self.repo.claim_delivery_records(
            org_id="tenant-a",
            now_iso="2026-01-02T00:00:00+00:00",
            lease_seconds=1,
        )[0]
        second_claim = self.repo.claim_delivery_records(
            org_id="tenant-a",
            now_iso="2026-01-02T00:00:02+00:00",
            lease_seconds=60,
        )[0]

        self.assertNotEqual(first_claim.lease_token, second_claim.lease_token)
        stale_update = self.repo.mark_delivery_success(
            int(delivery.id or 0),
            last_status="200 stale",
            lease_token=first_claim.lease_token,
        )
        current_update = self.repo.mark_delivery_success(
            int(delivery.id or 0),
            last_status="200 OK",
            lease_token=second_claim.lease_token,
        )

        self.assertFalse(stale_update)
        self.assertTrue(current_update)
        refreshed = self.repo.get_delivery_record(int(delivery.id or 0))
        self.assertEqual(refreshed.status, "delivered")
        self.assertEqual(refreshed.attempt_count, 1)

    def test_terminal_failure_records_dead_letter_and_replay_metadata(self) -> None:
        delivery = self._enqueue(delivery_id="delivery-dead", max_attempts=1)
        claimed = self.repo.claim_delivery_records(
            org_id="tenant-a",
            now_iso="2026-01-02T00:00:00+00:00",
        )[0]
        updated = self.repo.mark_delivery_retry(
            int(delivery.id or 0),
            last_status="503",
            last_error="unavailable",
            attempted_at="2026-01-02T00:00:01+00:00",
            lease_token=claimed.lease_token,
        )
        dead_letter = self.repo.get_delivery_record(int(delivery.id or 0))

        self.assertTrue(updated)
        self.assertEqual(dead_letter.status, "failed")
        self.assertEqual(dead_letter.dead_lettered_at, "2026-01-02T00:00:01+00:00")

        replayed = self.repo.requeue_delivery(
            int(delivery.id or 0),
            org_id="tenant-a",
            failed_only=True,
            next_attempt_at="2026-01-02T00:01:00+00:00",
            requested_by="ops-admin",
        )
        self.assertEqual(replayed.status, "pending")
        self.assertEqual(replayed.dead_lettered_at, "")
        self.assertEqual(replayed.replay_count, 1)
        self.assertEqual(replayed.last_replayed_by, "ops-admin")

    def test_retry_jitter_is_bounded_deterministic_and_delivery_specific(self) -> None:
        first = _build_retry_delay_seconds(3, jitter_key="delivery-a")
        repeated = _build_retry_delay_seconds(3, jitter_key="delivery-a")
        second = _build_retry_delay_seconds(3, jitter_key="delivery-b")

        self.assertEqual(first, repeated)
        self.assertGreaterEqual(first, 96)
        self.assertLessEqual(first, 144)
        self.assertNotEqual(first, second)


if __name__ == "__main__":
    unittest.main()
