import hashlib
import json
import logging
import os
import random
import sqlite3
import tempfile
import time
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, Optional

from sync_app.core.models import (
    AppConfig,
    OrganizationRecord,
    SyncConnectorRecord,
)
from sync_app.core.observability import METRICS
from sync_app.storage.config_codec import (
    CONNECTOR_CONFIG_FIELDS,
    ORGANIZATION_CONFIG_VALUE_TYPES,
    build_app_config_from_connector_record as _build_app_config_from_connector_record,
    build_app_config_from_org_values as _build_app_config_from_org_values,
    build_editable_org_config as _build_editable_org_config,
    load_connector_config_values_from_file as _load_connector_config_values_from_file,
    load_org_config_values_from_file as _load_org_config_values_from_file,
    normalize_connector_config_values as _normalize_connector_config_values,
    normalize_org_config_values as _normalize_org_config_values,
    record_has_connector_overrides as _record_has_connector_overrides,
)
from sync_app.storage.secret_store import (
    CONNECTOR_SECRET_FIELDS,
    ORGANIZATION_SECRET_FIELDS,
    protect_secret,
    unprotect_secret,
)
from sync_app.storage.schema import (
    DEFAULT_APP_SETTINGS,
    MIGRATIONS,
    ORG_SCOPED_APP_SETTINGS,
)

APP_NAME = "ADOrgSync"
LEGACY_APP_NAMES = ("NottingADSync",)
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SQLiteRetryPolicy:
    max_attempts: int = 4
    base_delay_seconds: float = 0.05
    max_delay_seconds: float = 0.5
    jitter_ratio: float = 0.2

    def delay_seconds(self, retry_number: int) -> float:
        exponential_delay = min(
            max(float(self.base_delay_seconds), 0.0) * (2 ** max(int(retry_number) - 1, 0)),
            max(float(self.max_delay_seconds), 0.0),
        )
        jitter = exponential_delay * max(float(self.jitter_ratio), 0.0)
        return max(exponential_delay + random.uniform(-jitter, jitter), 0.0)


def is_retryable_sqlite_lock_error(error: BaseException) -> bool:
    if not isinstance(error, sqlite3.OperationalError):
        return False
    normalized_message = str(error or "").strip().lower()
    return any(
        marker in normalized_message
        for marker in (
            "database is locked",
            "database table is locked",
            "database schema is locked",
            "database is busy",
        )
    )


class ResilientSQLiteConnection(sqlite3.Connection):
    retry_policy = SQLiteRetryPolicy()
    retry_callback: Optional[Callable[[int, sqlite3.OperationalError, float], None]] = None

    def configure_retry(self, policy: SQLiteRetryPolicy, callback=None) -> None:
        self.retry_policy = policy
        self.retry_callback = callback

    def _run_with_retry(self, operation):
        max_attempts = max(int(self.retry_policy.max_attempts or 0), 1)
        for attempt in range(1, max_attempts + 1):
            try:
                return operation()
            except sqlite3.OperationalError as exc:
                if not is_retryable_sqlite_lock_error(exc) or attempt >= max_attempts:
                    raise
                delay_seconds = self.retry_policy.delay_seconds(attempt)
                if self.retry_callback:
                    self.retry_callback(attempt, exc, delay_seconds)
                time.sleep(delay_seconds)
        raise RuntimeError("unreachable SQLite retry state")

    def execute(self, sql, parameters=(), /):
        return self._run_with_retry(lambda: sqlite3.Connection.execute(self, sql, parameters))

    def executemany(self, sql, seq_of_parameters, /):
        return self._run_with_retry(
            lambda: sqlite3.Connection.executemany(self, sql, seq_of_parameters)
        )

    def commit(self):
        return self._run_with_retry(lambda: sqlite3.Connection.commit(self))

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def resolve_app_data_dir(app_name: str = APP_NAME) -> str:
    base_dir = os.getenv("APPDATA")
    if not base_dir:
        base_dir = os.getenv("XDG_DATA_HOME")
    if not base_dir:
        base_dir = os.path.join(os.path.expanduser("~"), ".local", "share")
    path = os.path.join(base_dir, app_name)
    os.makedirs(path, exist_ok=True)
    return path


def default_db_path() -> str:
    return os.path.join(resolve_app_data_dir(), "app.db")


def workspace_fallback_db_path(app_name: str = APP_NAME) -> str:
    base_dir = os.path.join(os.getcwd(), ".appdata", app_name)
    return os.path.join(base_dir, "app.db")


def discover_legacy_db_candidates(app_name: str = APP_NAME) -> list[str]:
    cwd = os.getcwd()
    module_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    app_names = [str(app_name or "").strip() or APP_NAME, *LEGACY_APP_NAMES]
    candidates = [
        os.path.join(cwd, "app.db"),
        os.path.join(module_root, "app.db"),
    ]
    for candidate_app_name in app_names:
        candidates.extend(
            [
                os.path.join(cwd, ".appdata", candidate_app_name, "app.db"),
                os.path.join(module_root, ".appdata", candidate_app_name, "app.db"),
            ]
        )
    unique_candidates: list[str] = []
    seen = set()
    for candidate in candidates:
        normalized = os.path.normcase(os.path.abspath(candidate))
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_candidates.append(os.path.abspath(candidate))
    return unique_candidates


def sanitize_backup_label(label: Optional[str]) -> str:
    raw_label = (label or "manual").strip().lower()
    cleaned = "".join(char if char.isalnum() else "_" for char in raw_label)
    cleaned = cleaned.strip("_")
    return cleaned or "manual"


def dumps_json(value: Optional[Dict[str, Any]]) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _decode_secret_field(field_name: str, value: Any, *, secret_fields: set[str]) -> Any:
    if field_name not in secret_fields:
        return value
    return unprotect_secret(str(value or ""))


def _encode_secret_field(field_name: str, value: Any, *, secret_fields: set[str]) -> Any:
    if field_name not in secret_fields:
        return value
    return protect_secret(str(value or ""))


def normalize_org_id(org_id: Optional[str], *, fallback: Optional[str] = None) -> Optional[str]:
    normalized = str(org_id or "").strip().lower()
    if normalized:
        return normalized
    if fallback is None:
        return None
    fallback_value = str(fallback or "").strip().lower()
    return fallback_value or None


class DatabaseManager:
    _startup_snapshot_paths: set[str] = set()

    def __init__(
        self,
        db_path: Optional[str] = None,
        *,
        connection_timeout_seconds: float = 2.0,
        busy_timeout_ms: int = 2000,
        lock_retry_attempts: int = 4,
        lock_retry_base_delay_seconds: float = 0.05,
        lock_retry_max_delay_seconds: float = 0.5,
    ):
        self._auto_db_path = db_path is None
        self.connection_timeout_seconds = max(float(connection_timeout_seconds or 0.0), 0.0)
        self.busy_timeout_ms = max(int(busy_timeout_ms or 0), 0)
        self.retry_policy = SQLiteRetryPolicy(
            max_attempts=max(int(lock_retry_attempts or 0), 1),
            base_delay_seconds=max(float(lock_retry_base_delay_seconds or 0.0), 0.0),
            max_delay_seconds=max(float(lock_retry_max_delay_seconds or 0.0), 0.0),
        )
        self.sqlite_retry_count = 0
        self.db_path = os.path.abspath(db_path or default_db_path())
        self._fallback_db_path = os.path.abspath(workspace_fallback_db_path())
        if self._auto_db_path:
            self._ensure_usable_db_path()
        else:
            self._ensure_directory_layout()
        self.last_integrity_check: Optional[Dict[str, Any]] = None
        self.last_backup_path: Optional[str] = None
        self.last_startup_snapshot_path: Optional[str] = None
        self.last_restore_drill: Optional[Dict[str, Any]] = None
        self.last_initialize_result: Optional[Dict[str, Any]] = None
        self.last_migration_source_path: Optional[str] = None

    def _ensure_directory_layout(self) -> None:
        self.db_dir = os.path.dirname(self.db_path)
        self.backup_dir = os.path.join(self.db_dir, "backups")
        if self.db_dir:
            os.makedirs(self.db_dir, exist_ok=True)
            os.makedirs(self.backup_dir, exist_ok=True)

    def _ensure_usable_db_path(self) -> None:
        try:
            self._ensure_directory_layout()
        except OSError:
            self.db_path = self._fallback_db_path
            self._ensure_directory_layout()

    def _apply_connection_pragmas(self, conn: sqlite3.Connection):
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")

    def _record_sqlite_retry(
        self,
        attempt: int,
        error: sqlite3.OperationalError,
        delay_seconds: float,
    ) -> None:
        self.sqlite_retry_count += 1
        METRICS.increment("ad_org_sync_sqlite_lock_retries_total")
        METRICS.observe("ad_org_sync_sqlite_lock_retry_delay_seconds", delay_seconds)
        LOGGER.warning(
            "SQLite write contention for %s; retry=%s delay=%.3fs error=%s",
            self.db_path,
            attempt,
            delay_seconds,
            error,
        )

    def _open_connection(self, db_path: str) -> ResilientSQLiteConnection:
        conn = sqlite3.connect(
            db_path,
            timeout=self.connection_timeout_seconds,
            factory=ResilientSQLiteConnection,
        )
        conn.configure_retry(self.retry_policy, self._record_sqlite_retry)
        conn.row_factory = sqlite3.Row
        try:
            self._apply_connection_pragmas(conn)
        except Exception:
            conn.close()
            raise
        return conn

    def _connect(self) -> sqlite3.Connection:
        try:
            return self._open_connection(self.db_path)
        except sqlite3.OperationalError as exc:
            if is_retryable_sqlite_lock_error(exc):
                raise
            if not self._auto_db_path or os.path.normcase(self.db_path) == os.path.normcase(self._fallback_db_path):
                raise
            self.db_path = self._fallback_db_path
            self._ensure_directory_layout()
            return self._open_connection(self.db_path)

    def database_exists(self) -> bool:
        return os.path.exists(self.db_path) and os.path.getsize(self.db_path) > 0

    def find_legacy_database(self) -> Optional[str]:
        target_normalized = os.path.normcase(self.db_path)
        for candidate in discover_legacy_db_candidates():
            candidate_path = os.path.abspath(candidate)
            if os.path.normcase(candidate_path) == target_normalized:
                continue
            if os.path.exists(candidate_path) and os.path.getsize(candidate_path) > 0:
                return candidate_path
        return None

    def migrate_legacy_database_if_needed(self) -> Optional[str]:
        # An explicit database path is an isolation boundary.  Copying a
        # workspace-level legacy database into that path is surprising for
        # callers (and can leak production data into tests or temporary
        # environments).  Automatic legacy discovery is only appropriate
        # when the application selected its own default database location.
        if not self._auto_db_path or self.database_exists():
            return None

        legacy_source_path = self.find_legacy_database()
        if not legacy_source_path:
            return None

        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        with closing(sqlite3.connect(legacy_source_path, timeout=5.0)) as source_conn:
            self._apply_connection_pragmas(source_conn)
            with closing(sqlite3.connect(self.db_path, timeout=5.0)) as target_conn:
                self._apply_connection_pragmas(target_conn)
                source_conn.backup(target_conn)
        self.last_migration_source_path = legacy_source_path
        return legacy_source_path

    def run_integrity_check(self) -> Dict[str, Any]:
        checked_at = utcnow_iso()
        with self.connection() as conn:
            row = conn.execute("PRAGMA integrity_check").fetchone()
        result = row[0] if row else "unknown"
        summary = {
            "checked_at": checked_at,
            "result": result,
            "ok": str(result).strip().lower() == "ok",
        }
        self.last_integrity_check = summary
        return summary

    @staticmethod
    def _migration_checksum(description: str, sql_script: str) -> str:
        normalized_sql = str(sql_script or "").replace("\r\n", "\n").strip()
        payload = f"{str(description or '').strip()}\n{normalized_sql}".encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def verify_migration_checksums(self, *, backfill_missing: bool = False) -> Dict[str, Any]:
        expected = {
            int(version): {
                "description": str(description or "").strip(),
                "checksum": self._migration_checksum(description, sql_script),
            }
            for version, description, sql_script in MIGRATIONS
        }
        with self.transaction() as conn:
            columns = {
                str(row["name"] or "")
                for row in conn.execute("PRAGMA table_info(schema_migrations)").fetchall()
            }
            if "checksum" not in columns:
                return {
                    "ok": False,
                    "supported": False,
                    "verified_count": 0,
                    "backfilled_count": 0,
                    "mismatched_versions": [],
                    "unexpected_versions": [],
                }
            rows = conn.execute(
                "SELECT version, description, checksum FROM schema_migrations ORDER BY version"
            ).fetchall()
            mismatched_versions: list[int] = []
            unexpected_versions: list[int] = []
            backfilled_count = 0
            for row in rows:
                version = int(row["version"])
                expected_entry = expected.get(version)
                if expected_entry is None:
                    unexpected_versions.append(version)
                    continue
                stored_checksum = str(row["checksum"] or "").strip()
                if not stored_checksum and backfill_missing:
                    conn.execute(
                        "UPDATE schema_migrations SET checksum = ? WHERE version = ?",
                        (expected_entry["checksum"], version),
                    )
                    backfilled_count += 1
                    stored_checksum = expected_entry["checksum"]
                stored_description = str(row["description"] or "").strip()
                if (
                    stored_checksum != expected_entry["checksum"]
                    or stored_description != expected_entry["description"]
                ):
                    mismatched_versions.append(version)

        ok = not mismatched_versions and not unexpected_versions and len(rows) == len(expected)
        summary = {
            "ok": ok,
            "supported": True,
            "verified_count": len(rows),
            "expected_count": len(expected),
            "backfilled_count": backfilled_count,
            "mismatched_versions": mismatched_versions,
            "unexpected_versions": unexpected_versions,
        }
        if not ok:
            details = []
            if mismatched_versions:
                details.append(f"checksum mismatch={mismatched_versions}")
            if unexpected_versions:
                details.append(f"unexpected versions={unexpected_versions}")
            if len(rows) != len(expected):
                details.append(f"applied={len(rows)} expected={len(expected)}")
            raise RuntimeError("Migration integrity check failed: " + "; ".join(details))
        return summary

    def backup_database(self, *, label: Optional[str] = None) -> str:
        if not self.database_exists():
            raise FileNotFoundError(f"database file not found: {self.db_path}")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_label = sanitize_backup_label(label)
        backup_path = os.path.join(self.backup_dir, f"app_{safe_label}_{timestamp}.db")

        with self.connection() as source_conn:
            with closing(sqlite3.connect(backup_path)) as backup_conn:
                source_conn.backup(backup_conn)

        self.last_backup_path = backup_path
        return backup_path

    @staticmethod
    def _database_table_counts(connection: sqlite3.Connection) -> Dict[str, int]:
        rows = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()
        counts: Dict[str, int] = {}
        for row in rows:
            table_name = str(row[0] or "")
            if not table_name:
                continue
            quoted_name = table_name.replace('"', '""')
            counts[table_name] = int(
                connection.execute(f'SELECT COUNT(1) FROM "{quoted_name}"').fetchone()[0]
            )
        return counts

    @staticmethod
    def _file_sha256(path: str) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def verify_backup_restore(self, *, backup_path: Optional[str] = None) -> Dict[str, Any]:
        """Restore a backup into an isolated database and verify logical equivalence."""
        selected_backup = os.path.abspath(
            str(backup_path or "").strip() or self.backup_database(label="restore_drill")
        )
        if not os.path.isfile(selected_backup):
            raise FileNotFoundError(f"backup file not found: {selected_backup}")
        os.makedirs(self.backup_dir, exist_ok=True)
        checked_at = utcnow_iso()
        with tempfile.TemporaryDirectory(prefix="restore-drill-", dir=self.backup_dir) as drill_dir:
            restored_path = os.path.join(drill_dir, "restored.db")
            source_uri = f"file:{selected_backup.replace(os.sep, '/')}?mode=ro"
            with closing(sqlite3.connect(source_uri, uri=True, timeout=5.0)) as source_conn:
                source_counts = self._database_table_counts(source_conn)
                with closing(sqlite3.connect(restored_path, timeout=5.0)) as restored_conn:
                    source_conn.backup(restored_conn)
                    restored_counts = self._database_table_counts(restored_conn)

            restored_manager = DatabaseManager(
                db_path=restored_path,
                connection_timeout_seconds=self.connection_timeout_seconds,
                busy_timeout_ms=self.busy_timeout_ms,
                lock_retry_attempts=self.retry_policy.max_attempts,
                lock_retry_base_delay_seconds=self.retry_policy.base_delay_seconds,
                lock_retry_max_delay_seconds=self.retry_policy.max_delay_seconds,
            )
            integrity = restored_manager.run_integrity_check()
            migration_integrity = restored_manager.verify_migration_checksums(backfill_missing=False)
            report = {
                "checked_at": checked_at,
                "backup_path": selected_backup,
                "backup_sha256": self._file_sha256(selected_backup),
                "backup_size_bytes": os.path.getsize(selected_backup),
                "integrity_check": integrity,
                "migration_integrity": migration_integrity,
                "table_counts": restored_counts,
                "logical_counts_match": source_counts == restored_counts,
            }
            report["ok"] = bool(
                integrity.get("ok")
                and migration_integrity.get("ok")
                and report["logical_counts_match"]
            )
        self.last_restore_drill = report
        return report

    def ensure_startup_snapshot(self) -> Optional[str]:
        if not self.database_exists():
            return None

        normalized_path = os.path.normcase(self.db_path)
        if normalized_path in self._startup_snapshot_paths:
            return self.last_startup_snapshot_path

        startup_snapshot_path = self.backup_database(label="startup")
        self.last_startup_snapshot_path = startup_snapshot_path
        self._startup_snapshot_paths.add(normalized_path)
        return startup_snapshot_path

    def runtime_info(self) -> Dict[str, Any]:
        return {
            "db_path": self.db_path,
            "db_dir": self.db_dir,
            "backup_dir": self.backup_dir,
            "last_backup_path": self.last_backup_path,
            "last_startup_snapshot_path": self.last_startup_snapshot_path,
            "last_migration_source_path": self.last_migration_source_path,
            "last_integrity_check": self.last_integrity_check,
            "last_restore_drill": self.last_restore_drill,
            "sqlite_retry_count": self.sqlite_retry_count,
            "sqlite_retry_policy": {
                "max_attempts": self.retry_policy.max_attempts,
                "base_delay_seconds": self.retry_policy.base_delay_seconds,
                "max_delay_seconds": self.retry_policy.max_delay_seconds,
                "busy_timeout_ms": self.busy_timeout_ms,
            },
        }

    def cleanup_history(
        self,
        *,
        job_retention_days: int = 30,
        event_retention_days: int = 30,
        audit_log_retention_days: int = 90,
    ) -> Dict[str, Any]:
        normalized_job_retention_days = max(int(job_retention_days or 0), 0)
        normalized_event_retention_days = max(int(event_retention_days or 0), 0)
        normalized_audit_log_retention_days = max(int(audit_log_retention_days or 0), 0)
        now = datetime.now(timezone.utc)
        job_cutoff = None
        event_cutoff = None
        audit_log_cutoff = None
        if normalized_job_retention_days > 0:
            job_cutoff = (now - timedelta(days=normalized_job_retention_days)).isoformat(timespec="seconds")
        if normalized_event_retention_days > 0:
            event_cutoff = (now - timedelta(days=normalized_event_retention_days)).isoformat(timespec="seconds")
        if normalized_audit_log_retention_days > 0:
            audit_log_cutoff = (now - timedelta(days=normalized_audit_log_retention_days)).isoformat(timespec="seconds")

        result = {
            "checked_at": now.isoformat(timespec="seconds"),
            "job_retention_days": normalized_job_retention_days,
            "event_retention_days": normalized_event_retention_days,
            "audit_log_retention_days": normalized_audit_log_retention_days,
            "job_cutoff": job_cutoff or "",
            "event_cutoff": event_cutoff or "",
            "audit_log_cutoff": audit_log_cutoff or "",
            "deleted_jobs": 0,
            "deleted_events": 0,
            "deleted_planned_operations": 0,
            "deleted_operation_logs": 0,
            "deleted_conflicts": 0,
            "deleted_review_requests": 0,
            "deleted_replay_requests": 0,
            "deleted_audit_logs": 0,
        }

        with self.transaction() as conn:
            if job_cutoff:
                old_job_selector = """
                    SELECT job_id
                    FROM sync_jobs
                    WHERE ended_at IS NOT NULL
                      AND ended_at < ?
                """
                result["deleted_review_requests"] = conn.execute(
                    f"DELETE FROM sync_plan_reviews WHERE job_id IN ({old_job_selector})",
                    (job_cutoff,),
                ).rowcount
                result["deleted_replay_requests"] = conn.execute(
                    """
                    DELETE FROM sync_replay_requests
                    WHERE finished_at IS NOT NULL
                      AND finished_at < ?
                    """,
                    (job_cutoff,),
                ).rowcount
                result["deleted_conflicts"] = conn.execute(
                    f"DELETE FROM sync_conflicts WHERE job_id IN ({old_job_selector})",
                    (job_cutoff,),
                ).rowcount
                result["deleted_operation_logs"] = conn.execute(
                    f"DELETE FROM sync_operation_logs WHERE job_id IN ({old_job_selector})",
                    (job_cutoff,),
                ).rowcount
                result["deleted_planned_operations"] = conn.execute(
                    f"DELETE FROM planned_operations WHERE job_id IN ({old_job_selector})",
                    (job_cutoff,),
                ).rowcount
                result["deleted_events"] += conn.execute(
                    f"DELETE FROM sync_events WHERE job_id IN ({old_job_selector})",
                    (job_cutoff,),
                ).rowcount
                result["deleted_jobs"] = conn.execute(
                    """
                    DELETE FROM sync_jobs
                    WHERE ended_at IS NOT NULL
                      AND ended_at < ?
                    """,
                    (job_cutoff,),
                ).rowcount

            if event_cutoff:
                result["deleted_events"] += conn.execute(
                    """
                    DELETE FROM sync_events
                    WHERE created_at < ?
                    """,
                    (event_cutoff,),
                ).rowcount

            if audit_log_cutoff:
                result["deleted_audit_logs"] = conn.execute(
                    """
                    DELETE FROM web_audit_logs
                    WHERE created_at < ?
                    """,
                    (audit_log_cutoff,),
                ).rowcount

        return result

    def cleanup_backups(
        self,
        *,
        retention_days: int = 30,
        max_files: int = 30,
    ) -> Dict[str, Any]:
        normalized_retention_days = max(int(retention_days or 0), 0)
        normalized_max_files = max(int(max_files or 0), 0)
        now = datetime.now(timezone.utc)
        cutoff_ts = None
        if normalized_retention_days > 0:
            cutoff_ts = (now - timedelta(days=normalized_retention_days)).timestamp()

        result = {
            "checked_at": now.isoformat(timespec="seconds"),
            "retention_days": normalized_retention_days,
            "max_files": normalized_max_files,
            "deleted_backups": 0,
            "kept_backups": 0,
        }

        if not os.path.isdir(self.backup_dir):
            return result

        entries = [
            entry
            for entry in os.scandir(self.backup_dir)
            if entry.is_file() and entry.name.lower().endswith(".db")
        ]
        entries.sort(key=lambda entry: entry.stat().st_mtime, reverse=True)

        for index, entry in enumerate(entries):
            entry_stat = entry.stat()
            should_delete = False
            if normalized_max_files > 0 and index >= normalized_max_files:
                should_delete = True
            if cutoff_ts is not None and entry_stat.st_mtime < cutoff_ts:
                should_delete = True
            if should_delete:
                try:
                    os.remove(entry.path)
                    result["deleted_backups"] += 1
                except FileNotFoundError:
                    continue
            else:
                result["kept_backups"] += 1

        return result

    @contextmanager
    def connection(self):
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self):
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize(
        self,
        *,
        create_startup_snapshot: bool = True,
        verify_integrity: bool = True,
    ):
        existed_before_prepare = self.database_exists()
        existed_before_init = existed_before_prepare
        migration_source_path = None
        if not existed_before_init:
            migration_source_path = self.migrate_legacy_database_if_needed()
            existed_before_init = self.database_exists()
        startup_snapshot_path = None
        preflight_integrity = None

        if existed_before_init and verify_integrity:
            preflight_integrity = self.run_integrity_check()
            if not preflight_integrity["ok"]:
                raise RuntimeError(
                    f"SQLite integrity check failed before initialization: {preflight_integrity['result']}"
                )
        if existed_before_init and create_startup_snapshot:
            startup_snapshot_path = self.ensure_startup_snapshot()

        with self.transaction() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                  version INTEGER PRIMARY KEY,
                  description TEXT NOT NULL,
                  applied_at TEXT NOT NULL
                )
                """
            )

        applied_versions = self._get_applied_versions()
        for version, description, sql_script in MIGRATIONS:
            if version in applied_versions:
                continue
            with self.transaction() as conn:
                self._apply_migration_script(conn, sql_script)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations (version, description, applied_at) VALUES (?, ?, ?)",
                    (version, description, utcnow_iso()),
                )

        migration_integrity = self.verify_migration_checksums(backfill_missing=True)

        SettingsRepository(self).seed_defaults()
        GroupExclusionRuleRepository(self).seed_defaults()
        post_init_integrity = None
        if verify_integrity:
            post_init_integrity = self.run_integrity_check()
            if not post_init_integrity["ok"]:
                raise RuntimeError(
                    f"SQLite integrity check failed after initialization: {post_init_integrity['result']}"
                )

        self.last_initialize_result = {
            "db_path": self.db_path,
            "backup_dir": self.backup_dir,
            "created_new_database": (not existed_before_prepare) and not migration_source_path,
            "migration_source_path": migration_source_path,
            "startup_snapshot_path": startup_snapshot_path,
            "integrity_check": post_init_integrity,
            "preflight_integrity": preflight_integrity,
            "migration_integrity": migration_integrity,
        }
        return self.last_initialize_result

    def _get_applied_versions(self) -> set[int]:
        with self.connection() as conn:
            rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
        return {int(row["version"]) for row in rows}

    @staticmethod
    def _apply_migration_script(conn: sqlite3.Connection, sql_script: str) -> None:
        try:
            conn.executescript(sql_script)
            return
        except sqlite3.OperationalError as error:
            if "duplicate column name" not in str(error).lower():
                raise

        for raw_statement in str(sql_script or "").split(";"):
            statement = raw_statement.strip()
            if not statement:
                continue
            try:
                conn.execute(statement)
            except sqlite3.OperationalError as statement_error:
                if "duplicate column name" in str(statement_error).lower():
                    continue
                raise


class BaseRepository:
    def __init__(self, db: DatabaseManager, *, default_org_id: Optional[str] = None):
        self.db = db
        self.default_org_id = normalize_org_id(default_org_id)

    def _fetchone(self, query: str, params: Iterable[Any] = ()):
        with self.db.connection() as conn:
            return conn.execute(query, tuple(params)).fetchone()

    def _fetchall(self, query: str, params: Iterable[Any] = ()):
        with self.db.connection() as conn:
            return conn.execute(query, tuple(params)).fetchall()

    def _fetchcount(self, query: str, params: Iterable[Any] = ()) -> int:
        row = self._fetchone(query, params)
        if not row:
            return 0
        return int(row[0])

    @contextmanager
    def _write_connection(self, connection: sqlite3.Connection | None = None):
        """Join an existing unit of work or create a repository-local transaction."""
        if connection is not None:
            yield connection
            return
        with self.db.transaction() as managed_connection:
            yield managed_connection

    def _resolve_org_id(self, org_id: Optional[str] = None, *, default: Optional[str] = None) -> Optional[str]:
        fallback = self.default_org_id if default is None else default
        return normalize_org_id(org_id, fallback=fallback)


from sync_app.storage.repositories.organizations import OrganizationConfigRepository, OrganizationRepository
from sync_app.storage.repositories.jobs import (
    PlannedOperationRepository,
    SyncEventRepository,
    SyncJobRepository,
    SyncOperationLogRepository,
)
from sync_app.storage.repositories.admin import WebAdminUserRepository
from sync_app.storage.repositories.conflicts import (
    SyncConflictRepository,
    SyncExceptionRuleRepository,
    SyncPlanReviewRepository,
)
from sync_app.storage.repositories.connectors import SyncConnectorRepository
from sync_app.storage.repositories.exclusions import GroupExclusionRuleRepository
from sync_app.storage.repositories.system import (
    ConfigReleaseSnapshotRepository,
    DataQualitySnapshotRepository,
    IntegrationWebhookOutboxRepository,
    IntegrationWebhookSubscriptionRepository,
    SettingsRepository,
    SyncReplayRequestRepository,
    WebAuditLogRepository,
)
from sync_app.storage.repositories.mappings import (
    AttributeMappingRuleRepository,
    DepartmentOuMappingRepository,
    UserDepartmentOverrideRepository,
    UserIdentityBindingRepository,
)
from sync_app.storage.repositories.groups import (
    CustomManagedGroupBindingRepository,
    ManagedGroupBindingRepository,
)
from sync_app.storage.repositories.lifecycle import OffboardingQueueRepository, UserLifecycleQueueRepository
from sync_app.storage.repositories.state import ObjectStateRepository
from sync_app.storage.repositories.source_directory import SourceDirectoryRepository
