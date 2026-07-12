import sqlite3
import threading
import time
import unittest
from pathlib import Path

from sync_app.storage.local_db import DatabaseManager


class SQLiteResilienceTests(unittest.TestCase):
    def setUp(self):
        test_root = Path.cwd() / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        self.db_path = test_root / "sqlite_resilience.db"
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.db_path) + suffix)
            if candidate.exists():
                candidate.unlink()

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.db_path) + suffix)
            if candidate.exists():
                try:
                    candidate.unlink()
                except PermissionError:
                    pass

    def _manager(self, *, attempts: int = 20) -> DatabaseManager:
        manager = DatabaseManager(
            db_path=str(self.db_path),
            connection_timeout_seconds=0.01,
            busy_timeout_ms=10,
            lock_retry_attempts=attempts,
            lock_retry_base_delay_seconds=0.005,
            lock_retry_max_delay_seconds=0.02,
        )
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        with manager.transaction() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS retry_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        return manager

    def test_locked_write_retries_until_competing_transaction_commits(self):
        manager = self._manager()
        blocker = sqlite3.connect(str(self.db_path), timeout=0.01)
        blocker.execute("PRAGMA journal_mode = WAL")
        blocker.execute("BEGIN IMMEDIATE")
        blocker.execute("INSERT INTO retry_probe (id, value) VALUES (1, 'blocker')")
        worker_errors: list[BaseException] = []

        def worker() -> None:
            try:
                with manager.transaction() as conn:
                    conn.execute("INSERT INTO retry_probe (id, value) VALUES (2, 'worker')")
            except BaseException as exc:  # pragma: no cover - asserted through worker_errors
                worker_errors.append(exc)

        worker_thread = threading.Thread(target=worker, daemon=True)
        worker_thread.start()
        time.sleep(0.08)
        blocker.commit()
        blocker.close()
        worker_thread.join(timeout=2)

        self.assertFalse(worker_thread.is_alive())
        self.assertEqual(worker_errors, [])
        self.assertGreater(manager.sqlite_retry_count, 0)
        with manager.connection() as conn:
            rows = conn.execute("SELECT id, value FROM retry_probe ORDER BY id").fetchall()
        self.assertEqual([(row["id"], row["value"]) for row in rows], [(1, "blocker"), (2, "worker")])

    def test_locked_write_raises_after_retry_budget_is_exhausted(self):
        manager = self._manager(attempts=2)
        blocker = sqlite3.connect(str(self.db_path), timeout=0.01)
        blocker.execute("PRAGMA journal_mode = WAL")
        blocker.execute("BEGIN IMMEDIATE")
        blocker.execute("INSERT INTO retry_probe (id, value) VALUES (1, 'blocker')")
        try:
            with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                with manager.transaction() as conn:
                    conn.execute("INSERT INTO retry_probe (id, value) VALUES (2, 'worker')")
        finally:
            blocker.rollback()
            blocker.close()

        self.assertEqual(manager.sqlite_retry_count, 1)


if __name__ == "__main__":
    unittest.main()
