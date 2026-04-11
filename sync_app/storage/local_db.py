import json
import os
import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

from sync_app.core.models import (
    AppConfig,
    OrganizationRecord,
    SyncConnectorRecord,
)
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
    os.makedirs(base_dir, exist_ok=True)
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

    def __init__(self, db_path: Optional[str] = None):
        self._auto_db_path = db_path is None
        self.db_path = os.path.abspath(db_path or default_db_path())
        self._fallback_db_path = os.path.abspath(workspace_fallback_db_path())
        if self._auto_db_path:
            self._ensure_usable_db_path()
        else:
            self._ensure_directory_layout()
        self.last_integrity_check: Optional[Dict[str, Any]] = None
        self.last_backup_path: Optional[str] = None
        self.last_startup_snapshot_path: Optional[str] = None
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
        conn.execute("PRAGMA busy_timeout = 5000")

    def _connect(self) -> sqlite3.Connection:
        try:
            conn = sqlite3.connect(self.db_path, timeout=5.0)
            conn.row_factory = sqlite3.Row
            try:
                self._apply_connection_pragmas(conn)
            except Exception:
                conn.close()
                raise
            return conn
        except sqlite3.OperationalError:
            if not self._auto_db_path or os.path.normcase(self.db_path) == os.path.normcase(self._fallback_db_path):
                raise
            self.db_path = self._fallback_db_path
            self._ensure_directory_layout()
            conn = sqlite3.connect(self.db_path, timeout=5.0)
            conn.row_factory = sqlite3.Row
            try:
                self._apply_connection_pragmas(conn)
            except Exception:
                conn.close()
                raise
            return conn

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
        if self.database_exists():
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
from sync_app.storage.repositories.system import SettingsRepository, SyncReplayRequestRepository, WebAuditLogRepository
from sync_app.storage.repositories.mappings import (
    AttributeMappingRuleRepository,
    UserDepartmentOverrideRepository,
    UserIdentityBindingRepository,
)
from sync_app.storage.repositories.groups import (
    CustomManagedGroupBindingRepository,
    ManagedGroupBindingRepository,
)
from sync_app.storage.repositories.lifecycle import OffboardingQueueRepository, UserLifecycleQueueRepository
from sync_app.storage.repositories.state import ObjectStateRepository




