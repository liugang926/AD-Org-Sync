from __future__ import annotations

import tempfile
import unittest
from unittest.mock import patch

from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.schema import MIGRATIONS


class MigrationIntegrityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = f"{self.temp_dir.name}/app.db"

    def test_initialization_backfills_and_verifies_every_migration_checksum(self) -> None:
        manager = DatabaseManager(db_path=self.db_path)
        result = manager.initialize(create_startup_snapshot=False)

        migration_integrity = result["migration_integrity"]
        self.assertTrue(migration_integrity["ok"])
        self.assertEqual(migration_integrity["verified_count"], len(MIGRATIONS))
        self.assertEqual(migration_integrity["expected_count"], len(MIGRATIONS))
        with manager.connection() as connection:
            blank_count = connection.execute(
                "SELECT COUNT(1) FROM schema_migrations WHERE checksum = ''"
            ).fetchone()[0]
        self.assertEqual(blank_count, 0)

    def test_modified_migration_checksum_is_rejected_on_startup(self) -> None:
        manager = DatabaseManager(db_path=self.db_path)
        manager.initialize(create_startup_snapshot=False)
        with manager.transaction() as connection:
            connection.execute(
                "UPDATE schema_migrations SET checksum = 'tampered' WHERE version = 1"
            )

        with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
            manager.initialize(create_startup_snapshot=False, verify_integrity=False)

    def test_unknown_migration_version_is_rejected_to_prevent_unsafe_downgrade(self) -> None:
        manager = DatabaseManager(db_path=self.db_path)
        manager.initialize(create_startup_snapshot=False)
        with manager.transaction() as connection:
            connection.execute(
                """
                INSERT INTO schema_migrations (version, description, applied_at, checksum)
                VALUES (999, 'future migration', '2026-01-01T00:00:00+00:00', 'future')
                """
            )

        with self.assertRaisesRegex(RuntimeError, "unexpected versions"):
            manager.initialize(create_startup_snapshot=False, verify_integrity=False)

    def test_version_30_database_upgrades_to_sspr_schema_without_data_loss(self) -> None:
        with patch("sync_app.storage.local_db.MIGRATIONS", MIGRATIONS[:30]):
            legacy_manager = DatabaseManager(db_path=self.db_path)
            legacy_manager.initialize(create_startup_snapshot=False)
            with legacy_manager.transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO user_identity_bindings (
                      org_id, source_user_id, connector_id, ad_username, source, is_enabled, updated_at
                    ) VALUES ('default', 'alice', 'default', 'alice', 'manual', 1, '2026-01-01T00:00:00+00:00')
                    """
                )
                connection.execute(
                    """
                    INSERT INTO web_audit_logs (
                      org_id, actor_username, action_type, target_type, target_id,
                      result, message, payload_json, created_at
                    ) VALUES (
                      'default', 'admin', 'test.before_upgrade', 'binding', 'alice',
                      'success', 'preserved', '{}', '2026-01-01T00:00:00+00:00'
                    )
                    """
                )

        manager = DatabaseManager(db_path=self.db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        with manager.connection() as connection:
            applied = connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0]
            binding = connection.execute(
                "SELECT source_provider, source_user_id, ad_username FROM user_identity_bindings"
            ).fetchone()
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            index_columns = [
                row[2]
                for row in connection.execute(
                    "PRAGMA index_info('idx_user_identity_bindings_source_identity')"
                ).fetchall()
            ]
            audit_count = connection.execute(
                "SELECT COUNT(*) FROM web_audit_logs WHERE action_type = 'test.before_upgrade'"
            ).fetchone()[0]
        self.assertEqual(applied, 35)
        self.assertEqual((binding["source_provider"], binding["source_user_id"], binding["ad_username"]), ("wecom", "alice", "alice"))
        self.assertIn("source_directory_snapshots", tables)
        self.assertIn("sync_scope_selections", tables)
        self.assertIn("sspr_verification_sessions", tables)
        self.assertIn("sspr_oauth_transactions", tables)
        self.assertIn("sspr_reset_receipts", tables)
        self.assertIn("sspr_rate_limit_buckets", tables)
        self.assertIn("enterprise_identities", tables)
        self.assertIn("source_connectors", tables)
        self.assertIn("platform_accounts", tables)
        self.assertIn("ad_accounts", tables)
        self.assertIn("identity_account_links", tables)
        self.assertIn("identity_match_rules", tables)
        self.assertIn("identity_match_runs", tables)
        self.assertIn("identity_match_candidates", tables)
        self.assertIn("field_authority_rules", tables)
        self.assertIn("account_takeover_batches", tables)
        self.assertIn("rollout_data_quality_reviews", tables)
        self.assertEqual(
            index_columns,
            ["org_id", "source_provider", "connector_id", "source_user_id"],
        )
        self.assertEqual(audit_count, 1)

        # Applying initialization again must keep data and migration checksums intact.
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        with manager.connection() as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM user_identity_bindings").fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM web_audit_logs WHERE action_type = 'test.before_upgrade'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM enterprise_identities").fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM platform_accounts").fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM ad_accounts").fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM identity_account_links").fetchone()[0],
                2,
            )


if __name__ == "__main__":
    unittest.main()
