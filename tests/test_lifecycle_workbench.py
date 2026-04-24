import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from sync_app.services.lifecycle_workbench import (
    apply_lifecycle_bulk_action,
    apply_offboarding_bulk_action,
    apply_replay_bulk_action,
    build_lifecycle_workbench_data,
)
from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories.conflicts import SyncExceptionRuleRepository
from sync_app.storage.repositories.lifecycle import (
    OffboardingQueueRepository,
    UserLifecycleQueueRepository,
)
from sync_app.storage.repositories.organizations import OrganizationRepository
from sync_app.storage.repositories.system import SyncReplayRequestRepository


class LifecycleWorkbenchTests(unittest.TestCase):
    def _build_db(self, temp_dir: str) -> DatabaseManager:
        db_path = Path(temp_dir) / "lifecycle_workbench.db"
        config_path = str((Path(temp_dir) / "lifecycle_workbench.ini").resolve())
        db_manager = DatabaseManager(db_path=str(db_path))
        db_manager.initialize()
        OrganizationRepository(db_manager).ensure_default(config_path=config_path)
        return db_manager

    def test_build_lifecycle_workbench_data_groups_due_and_manual_hold_records(self):
        with TemporaryDirectory() as temp_dir:
            db_manager = self._build_db(temp_dir)
            now = datetime.now(timezone.utc)
            offboarding_repo = OffboardingQueueRepository(db_manager)
            lifecycle_repo = UserLifecycleQueueRepository(db_manager)
            replay_repo = SyncReplayRequestRepository(db_manager)
            exception_repo = SyncExceptionRuleRepository(db_manager)

            offboarding_repo.upsert_pending(
                connector_id="default",
                source_user_id="off-001",
                ad_username="off.001",
                due_at=(now - timedelta(days=1)).isoformat(timespec="seconds"),
                org_id="default",
                reason="Source account missing",
                manager_userids=["manager.off"],
            )
            lifecycle_repo.upsert_pending(
                lifecycle_type="future_onboarding",
                connector_id="default",
                source_user_id="hire-001",
                ad_username="hire.001",
                effective_at=(now + timedelta(days=5)).isoformat(timespec="seconds"),
                org_id="default",
                reason="Future hire",
                sponsor_userid="sponsor.future",
                manager_userids=["manager.future"],
            )
            lifecycle_repo.upsert_pending(
                lifecycle_type="contractor_expiry",
                connector_id="default",
                source_user_id="contract-001",
                ad_username="contract.001",
                effective_at=(now - timedelta(hours=6)).isoformat(timespec="seconds"),
                org_id="default",
                reason="Contract reached end date",
                employment_type="contractor",
                sponsor_userid="sponsor.contract",
                manager_userids=["manager.contract"],
            )
            replay_repo.enqueue_request(
                request_type="seed_replay",
                execution_mode="apply",
                requested_by="tester",
                target_scope="source_user",
                target_id="contract-001",
                trigger_reason="seeded",
                org_id="default",
            )
            exception_repo.upsert_rule(
                rule_type="skip_user_sync",
                match_value="hire-001",
                notes="Waiting for HR confirmation",
                expires_at=(now + timedelta(days=3)).isoformat(timespec="seconds"),
                org_id="default",
            )
            exception_repo.update_governance_metadata(
                rule_type="skip_user_sync",
                match_value="hire-001",
                org_id="default",
                rule_owner="ops@example.com",
                effective_reason="Hold until HR confirms start date",
            )

            payload = build_lifecycle_workbench_data(db_manager, "default")

        self.assertEqual(payload["summary"]["future_onboarding_count"], 1)
        self.assertEqual(payload["summary"]["contractor_expiry_count"], 1)
        self.assertEqual(payload["summary"]["offboarding_count"], 1)
        self.assertEqual(payload["summary"]["replay_request_count"], 1)
        self.assertEqual(payload["summary"]["manual_hold_count"], 1)
        self.assertEqual(payload["summary"]["actionable_now_count"], 3)
        self.assertEqual(payload["future_onboarding_rows"][0]["state"]["label"], "Deferred")
        self.assertEqual(
            payload["future_onboarding_rows"][0]["manual_hold"]["reason"],
            "Hold until HR confirms start date",
        )
        self.assertEqual(payload["contractor_expiry_rows"][0]["state"]["label"], "Ready")
        self.assertEqual(payload["offboarding_rows"][0]["state"]["label"], "Grace Elapsed")

    def test_offboarding_approve_reschedules_to_now_and_enqueues_replay(self):
        with TemporaryDirectory() as temp_dir:
            db_manager = self._build_db(temp_dir)
            now = datetime.now(timezone.utc)
            offboarding_repo = OffboardingQueueRepository(db_manager)
            replay_repo = SyncReplayRequestRepository(db_manager)
            exception_repo = SyncExceptionRuleRepository(db_manager)

            offboarding_repo.upsert_pending(
                connector_id="default",
                source_user_id="off-approve",
                ad_username="off.approve",
                due_at=(now + timedelta(days=5)).isoformat(timespec="seconds"),
                org_id="default",
                reason="Pending manager approval",
            )
            exception_repo.upsert_rule(
                rule_type="skip_user_disable",
                match_value="off-approve",
                notes="Temporary hold",
                org_id="default",
            )
            record = offboarding_repo.get_record(
                connector_id="default",
                ad_username="off.approve",
                org_id="default",
            )
            self.assertIsNotNone(record)

            result = apply_offboarding_bulk_action(
                db_manager,
                "default",
                actor_username="tester",
                action="approve",
                record_ids=[record.id],
            )
            updated_record = offboarding_repo.get_record(
                connector_id="default",
                ad_username="off.approve",
                org_id="default",
            )
            pending_replay_requests = replay_repo.list_request_records(
                status="pending",
                org_id="default",
                limit=10,
            )
            enabled_exception_rules = exception_repo.list_enabled_rule_records(org_id="default")

        self.assertEqual(result["processed_count"], 1)
        self.assertEqual(result["replay_request_count"], 1)
        self.assertEqual(result["hold_cleared_count"], 1)
        self.assertIsNotNone(updated_record)
        self.assertLessEqual(
            datetime.fromisoformat(updated_record.due_at),
            datetime.now(timezone.utc) + timedelta(seconds=5),
        )
        self.assertEqual(len(pending_replay_requests), 1)
        self.assertFalse(any(rule.match_value == "off-approve" for rule in enabled_exception_rules))

    def test_contractor_defer_creates_skip_disable_exception(self):
        with TemporaryDirectory() as temp_dir:
            db_manager = self._build_db(temp_dir)
            now = datetime.now(timezone.utc)
            lifecycle_repo = UserLifecycleQueueRepository(db_manager)
            exception_repo = SyncExceptionRuleRepository(db_manager)

            lifecycle_repo.upsert_pending(
                lifecycle_type="contractor_expiry",
                connector_id="default",
                source_user_id="contract-defer",
                ad_username="contract.defer",
                effective_at=(now + timedelta(days=1)).isoformat(timespec="seconds"),
                org_id="default",
                reason="Contractor extension requested",
                employment_type="contractor",
                sponsor_userid="sponsor.contract",
            )
            record = lifecycle_repo.get_record_for_source_user(
                lifecycle_type="contractor_expiry",
                connector_id="default",
                source_user_id="contract-defer",
                org_id="default",
            )
            self.assertIsNotNone(record)

            result = apply_lifecycle_bulk_action(
                db_manager,
                "default",
                actor_username="tester",
                lifecycle_type="contractor_expiry",
                action="defer",
                record_ids=[record.id],
                delay_days=3,
            )
            enabled_rules = exception_repo.list_enabled_rule_records(org_id="default")
            matching_rule = next(
                rule
                for rule in enabled_rules
                if rule.rule_type == "skip_user_disable" and rule.match_value == "contract-defer"
            )

        self.assertEqual(result["processed_count"], 1)
        self.assertEqual(result["exception_rule_count"], 1)
        self.assertTrue(matching_rule.expires_at)
        self.assertEqual(
            matching_rule.effective_reason,
            "Manual defer from lifecycle workbench contractor_expiry",
        )
        self.assertEqual(matching_rule.next_review_at, matching_rule.expires_at)
        self.assertGreater(
            datetime.fromisoformat(matching_rule.expires_at),
            datetime.fromisoformat(record.effective_at),
        )

    def test_contractor_approve_reschedules_future_expiry_to_now_and_enqueues_replay(self):
        with TemporaryDirectory() as temp_dir:
            db_manager = self._build_db(temp_dir)
            now = datetime.now(timezone.utc)
            lifecycle_repo = UserLifecycleQueueRepository(db_manager)
            replay_repo = SyncReplayRequestRepository(db_manager)
            lifecycle_repo.upsert_pending(
                lifecycle_type="contractor_expiry",
                connector_id="default",
                source_user_id="contract-approve",
                ad_username="contract.approve",
                effective_at=(now + timedelta(days=7)).isoformat(timespec="seconds"),
                org_id="default",
                reason="Admin approved early contract expiry",
                employment_type="contractor",
                sponsor_userid="sponsor.contract",
            )
            record = lifecycle_repo.get_record_for_source_user(
                lifecycle_type="contractor_expiry",
                connector_id="default",
                source_user_id="contract-approve",
                org_id="default",
            )
            self.assertIsNotNone(record)

            result = apply_lifecycle_bulk_action(
                db_manager,
                "default",
                actor_username="tester",
                lifecycle_type="contractor_expiry",
                action="approve",
                record_ids=[record.id],
            )
            updated_record = lifecycle_repo.get_record_for_source_user(
                lifecycle_type="contractor_expiry",
                connector_id="default",
                source_user_id="contract-approve",
                org_id="default",
            )
            pending_replay_requests = replay_repo.list_request_records(
                status="pending",
                org_id="default",
                limit=10,
            )

        self.assertEqual(result["processed_count"], 1)
        self.assertEqual(result["replay_request_count"], 1)
        self.assertIsNotNone(updated_record)
        self.assertLessEqual(
            datetime.fromisoformat(updated_record.effective_at),
            datetime.now(timezone.utc) + timedelta(seconds=5),
        )
        self.assertEqual(updated_record.payload["previous_effective_at"], record.effective_at)
        self.assertEqual(len(pending_replay_requests), 1)

    def test_replay_retry_supersedes_existing_request(self):
        with TemporaryDirectory() as temp_dir:
            db_manager = self._build_db(temp_dir)
            replay_repo = SyncReplayRequestRepository(db_manager)

            request_id = replay_repo.enqueue_request(
                request_type="seed_replay",
                execution_mode="apply",
                requested_by="tester",
                target_scope="source_user",
                target_id="replay-user",
                trigger_reason="seeded",
                org_id="default",
                payload={"seed": True},
            )

            result = apply_replay_bulk_action(
                db_manager,
                "default",
                actor_username="tester",
                action="retry",
                request_ids=[request_id],
            )
            original_request = replay_repo.get_request_record(request_id)
            pending_requests = replay_repo.list_request_records(
                status="pending",
                org_id="default",
                limit=10,
            )

        self.assertEqual(result["processed_count"], 1)
        self.assertEqual(result["replay_request_count"], 1)
        self.assertIsNotNone(original_request)
        self.assertEqual(original_request.status, "superseded")
        self.assertEqual(len(pending_requests), 1)
        self.assertEqual(pending_requests[0].payload["retried_from_request_id"], request_id)
        self.assertEqual(
            original_request.result_summary["replacement_request_id"],
            pending_requests[0].id,
        )


if __name__ == "__main__":
    unittest.main()
