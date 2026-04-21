import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories.conflicts import SyncExceptionRuleRepository
from sync_app.storage.repositories.mappings import (
    UserDepartmentOverrideRepository,
    UserIdentityBindingRepository,
)


class RuleGovernanceMetadataTests(unittest.TestCase):
    def test_binding_metadata_and_hits_are_persisted(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "rule_governance_binding.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()

            repo = UserIdentityBindingRepository(db_manager)
            repo.upsert_binding("alice", "alice.ad", source="manual", notes="legacy", preserve_manual=False)
            repo.update_governance_metadata_for_source_user(
                "alice",
                rule_owner="iam@corp.example",
                effective_reason="Known shared mailbox binding",
                next_review_at="2026-05-01T09:00:00+00:00",
                last_reviewed_at="2026-04-18T09:00:00+00:00",
            )
            repo.record_rule_hit_for_source_user("alice", hit_at="2026-04-18T10:00:00+00:00")

            record = repo.get_binding_record_by_source_user_id("alice")

        self.assertIsNotNone(record)
        self.assertEqual(record.rule_owner, "iam@corp.example")
        self.assertEqual(record.effective_reason, "Known shared mailbox binding")
        self.assertEqual(record.next_review_at, "2026-05-01T09:00:00+00:00")
        self.assertEqual(record.last_reviewed_at, "2026-04-18T09:00:00+00:00")
        self.assertEqual(record.hit_count, 1)
        self.assertEqual(record.last_hit_at, "2026-04-18T10:00:00+00:00")

    def test_override_metadata_and_hits_are_persisted(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "rule_governance_override.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()

            repo = UserDepartmentOverrideRepository(db_manager)
            repo.upsert_override("bob", "1001", notes="ops note")
            repo.update_governance_metadata_for_source_user(
                "bob",
                rule_owner="ops@corp.example",
                effective_reason="Primary placement must remain in HQ",
                next_review_at="2026-05-02T09:00:00+00:00",
                last_reviewed_at="2026-04-18T09:05:00+00:00",
            )
            repo.record_rule_hit_for_source_user("bob", hit_at="2026-04-18T10:05:00+00:00")

            record = repo.get_override_record_by_source_user_id("bob")

        self.assertIsNotNone(record)
        self.assertEqual(record.rule_owner, "ops@corp.example")
        self.assertEqual(record.effective_reason, "Primary placement must remain in HQ")
        self.assertEqual(record.next_review_at, "2026-05-02T09:00:00+00:00")
        self.assertEqual(record.last_reviewed_at, "2026-04-18T09:05:00+00:00")
        self.assertEqual(record.hit_count, 1)
        self.assertEqual(record.last_hit_at, "2026-04-18T10:05:00+00:00")

    def test_exception_rule_metadata_and_consumption_are_persisted(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "rule_governance_exception.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()

            repo = SyncExceptionRuleRepository(db_manager)
            repo.upsert_rule(
                rule_type="skip_user_disable",
                match_value="carol",
                notes="grace period",
                is_once=True,
            )
            repo.update_governance_metadata(
                rule_type="skip_user_disable",
                match_value="carol",
                rule_owner="security@corp.example",
                effective_reason="Waiting for manager approval",
                next_review_at="2026-05-03T09:00:00+00:00",
                last_reviewed_at="2026-04-18T09:10:00+00:00",
            )
            repo.consume_rule(rule_type="skip_user_disable", match_value="carol")

            record = repo.get_rule_record(1)

        self.assertIsNotNone(record)
        self.assertEqual(record.rule_owner, "security@corp.example")
        self.assertEqual(record.effective_reason, "Waiting for manager approval")
        self.assertEqual(record.next_review_at, "2026-05-03T09:00:00+00:00")
        self.assertEqual(record.last_reviewed_at, "2026-04-18T09:10:00+00:00")
        self.assertEqual(record.hit_count, 1)
        self.assertTrue(record.last_hit_at)
        self.assertTrue(record.last_matched_at)
        self.assertFalse(record.is_enabled)


if __name__ == "__main__":
    unittest.main()
