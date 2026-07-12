from __future__ import annotations

import tempfile
import unittest

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


if __name__ == "__main__":
    unittest.main()
