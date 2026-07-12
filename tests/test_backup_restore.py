from __future__ import annotations

import contextlib
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from sync_app import cli
from sync_app.storage.local_db import DatabaseManager, SettingsRepository


class BackupRestoreVerificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "app.db"
        self.manager = DatabaseManager(db_path=str(self.db_path))
        self.manager.initialize(create_startup_snapshot=False)
        SettingsRepository(self.manager).set_value("brand_display_name", "Recovery Test", "string")

    def test_backup_is_restored_and_logically_verified_in_isolation(self) -> None:
        backup_path = self.manager.backup_database(label="test")
        report = self.manager.verify_backup_restore(backup_path=backup_path)

        self.assertTrue(report["ok"])
        self.assertTrue(report["logical_counts_match"])
        self.assertTrue(report["integrity_check"]["ok"])
        self.assertTrue(report["migration_integrity"]["ok"])
        self.assertEqual(report["table_counts"]["schema_migrations"], report["migration_integrity"]["verified_count"])
        self.assertEqual(len(report["backup_sha256"]), 64)
        self.assertFalse(any(Path(self.manager.backup_dir).glob("restore-drill-*")))

    def test_cli_can_create_and_verify_a_fresh_restore_drill_backup(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = cli.main(
                [
                    "db-restore-check",
                    "--db-path",
                    str(self.db_path),
                    "--json",
                ]
            )

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertTrue(Path(payload["backup_path"]).is_file())

    def test_corrupted_backup_is_rejected(self) -> None:
        corrupted_path = Path(self.manager.backup_dir) / "corrupted.db"
        corrupted_path.write_bytes(b"not a sqlite database")

        with self.assertRaises(sqlite3.DatabaseError):
            self.manager.verify_backup_restore(backup_path=str(corrupted_path))


if __name__ == "__main__":
    unittest.main()
