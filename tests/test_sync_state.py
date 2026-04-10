import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path

from sync_app.core.common import hash_user_state
from sync_app.services.state import SyncStateManager
from sync_app.storage.local_db import DatabaseManager


class SyncStateManagerTests(unittest.TestCase):
    def test_sync_state_manager_reads_legacy_wecom_rows_and_writes_canonical_source_rows(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        db_path = test_root / "sync_state_legacy_rows.db"
        state_file = test_root / "sync_state_legacy_rows.json"
        payload = {
            "userid": "alice",
            "name": "Alice",
            "email": "alice@example.com",
            "department": [1],
        }
        try:
            if db_path.exists():
                db_path.unlink()
            if state_file.exists():
                state_file.unlink()
            manager = DatabaseManager(db_path=str(db_path))
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)

            with manager.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO object_sync_state (
                      org_id, source_type, object_type, source_id, source_hash, display_name,
                      target_dn, last_seen_at, last_job_id, last_action, last_status, extra_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "default",
                        "wecom",
                        "user",
                        "alice",
                        hash_user_state(payload),
                        "Alice",
                        "",
                        datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        "",
                        "sync_user",
                        "success",
                        json.dumps({"manager_userids": ["lead1"]}, ensure_ascii=False),
                    ),
                )

            state_manager = SyncStateManager(state_file=str(state_file), db_manager=manager)

            self.assertFalse(state_manager.is_user_changed("alice", payload))
            self.assertEqual(state_manager.get_synced_user_count(), 1)
            self.assertEqual(state_manager.get_user_state_record("alice")["source_type"], "wecom")

            state_manager.update_user_state("alice", payload, job_id="job-1")

            current_row = state_manager.get_user_state_record("alice")
            self.assertEqual(current_row["source_type"], "source")
            with manager.connection() as conn:
                source_count = conn.execute(
                    """
                    SELECT COUNT(*) AS total
                    FROM object_sync_state
                    WHERE org_id = 'default' AND object_type = 'user' AND source_id = 'alice' AND source_type = 'source'
                    """
                ).fetchone()["total"]
            self.assertEqual(source_count, 1)
        finally:
            for path in (db_path, state_file):
                if path.exists():
                    try:
                        path.unlink()
                    except PermissionError:
                        pass

    def test_sync_state_manager_migrates_legacy_json_into_canonical_source_rows(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        db_path = test_root / "sync_state_legacy_file.db"
        state_file = test_root / "sync_state_legacy_file.json"
        payload = {
            "synced_users": {
                "alice": {
                    "hash": "hash-1",
                    "timestamp": "2026-04-09T10:00:00+08:00",
                }
            },
            "last_sync_time": "2026-04-09T10:00:00+08:00",
            "last_sync_success": True,
        }
        try:
            if db_path.exists():
                db_path.unlink()
            manager = DatabaseManager(db_path=str(db_path))
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)
            state_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

            state_manager = SyncStateManager(state_file=str(state_file), db_manager=manager)

            self.assertEqual(state_manager.get_synced_user_count(), 1)
            current_row = state_manager.get_user_state_record("alice")
            self.assertIsNotNone(current_row)
            self.assertEqual(current_row["source_type"], "source")
            self.assertEqual(state_manager.get_last_sync_time(), "2026-04-09T10:00:00+08:00")
        finally:
            for path in (db_path, state_file):
                if path.exists():
                    try:
                        path.unlink()
                    except PermissionError:
                        pass


if __name__ == "__main__":
    unittest.main()
