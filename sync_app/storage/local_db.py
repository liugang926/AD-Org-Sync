import json
import os
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

from sync_app.core.models import (
    AppConfig,
    AttributeMappingRuleRecord,
    CustomManagedGroupBindingRecord,
    ExclusionRuleRecord,
    ManagedGroupBindingRecord,
    OffboardingRecord,
    OrganizationRecord,
    UserLifecycleRecord,
    SyncConnectorRecord,
    SyncConflictRecord,
    SyncExceptionRuleRecord,
    SyncJobRecord,
    SyncOperationRecord,
    SyncPlanReviewRecord,
    SyncReplayRequestRecord,
    UserDepartmentOverrideRecord,
    UserIdentityBindingRecord,
    WebAdminUserRecord,
    WebAuditLogRecord,
)
from sync_app.core.exception_rules import (
    get_exception_rule_match_type,
    normalize_exception_match_value,
    normalize_exception_rule_type,
)
from sync_app.core.sync_policies import normalize_mapping_direction
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
    DEFAULT_HARD_PROTECTED_GROUPS,
    DEFAULT_SOFT_EXCLUDED_GROUPS,
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
            self._apply_connection_pragmas(conn)
            return conn
        except sqlite3.OperationalError:
            if not self._auto_db_path or os.path.normcase(self.db_path) == os.path.normcase(self._fallback_db_path):
                raise
            self.db_path = self._fallback_db_path
            self._ensure_directory_layout()
            conn = sqlite3.connect(self.db_path, timeout=5.0)
            conn.row_factory = sqlite3.Row
            self._apply_connection_pragmas(conn)
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
        with sqlite3.connect(legacy_source_path, timeout=5.0) as source_conn:
            self._apply_connection_pragmas(source_conn)
            with sqlite3.connect(self.db_path, timeout=5.0) as target_conn:
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
            with sqlite3.connect(backup_path) as backup_conn:
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
                conn.executescript(sql_script)
                conn.execute(
                    "INSERT INTO schema_migrations (version, description, applied_at) VALUES (?, ?, ?)",
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


class SettingsRepository(BaseRepository):
    def _resolve_settings_key(self, key: str, *, org_id: Optional[str] = None) -> tuple[Optional[str], str]:
        normalized_key = str(key or "").strip()
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_key in ORG_SCOPED_APP_SETTINGS and normalized_org_id:
            return f"org:{normalized_org_id}:{normalized_key}", normalized_key
        return None, normalized_key

    def seed_defaults(self):
        now = utcnow_iso()
        web_admin_password_default, web_admin_password_type = DEFAULT_APP_SETTINGS["web_admin_password_min_length"]
        with self.db.transaction() as conn:
            for key, (value, value_type) in DEFAULT_APP_SETTINGS.items():
                conn.execute(
                    """
                    INSERT OR IGNORE INTO app_settings (key, value, value_type, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (key, value, value_type, now),
                )
            conn.execute(
                """
                INSERT OR IGNORE INTO app_settings (key, value, value_type, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                ("web_session_secret", secrets.token_urlsafe(48), "string", now),
            )
            conn.execute(
                """
                UPDATE app_settings
                SET value = ?,
                    value_type = ?,
                    updated_at = ?
                WHERE key = 'web_admin_password_min_length'
                  AND value = '12'
                """,
                (web_admin_password_default, web_admin_password_type, now),
            )

    def get_value(
        self,
        key: str,
        default: Optional[str] = None,
        *,
        org_id: Optional[str] = None,
        fallback_to_global: bool = True,
    ) -> Optional[str]:
        scoped_key, base_key = self._resolve_settings_key(key, org_id=org_id)
        if scoped_key:
            row = self._fetchone("SELECT value FROM app_settings WHERE key = ?", (scoped_key,))
            if row:
                return row["value"]
            if not fallback_to_global:
                return default
        row = self._fetchone("SELECT value FROM app_settings WHERE key = ?", (base_key,))
        if not row:
            return default
        return row["value"]

    def get_bool(
        self,
        key: str,
        default: bool = False,
        *,
        org_id: Optional[str] = None,
        fallback_to_global: bool = True,
    ) -> bool:
        value = self.get_value(key, org_id=org_id, fallback_to_global=fallback_to_global)
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def get_int(
        self,
        key: str,
        default: int = 0,
        *,
        org_id: Optional[str] = None,
        fallback_to_global: bool = True,
    ) -> int:
        value = self.get_value(key, org_id=org_id, fallback_to_global=fallback_to_global)
        if value is None:
            return default
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return default

    def get_float(
        self,
        key: str,
        default: float = 0.0,
        *,
        org_id: Optional[str] = None,
        fallback_to_global: bool = True,
    ) -> float:
        value = self.get_value(key, org_id=org_id, fallback_to_global=fallback_to_global)
        if value is None:
            return default
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return default

    def set_value(self, key: str, value: Any, value_type: str = "string", *, org_id: Optional[str] = None):
        scoped_key, base_key = self._resolve_settings_key(key, org_id=org_id)
        persisted_key = scoped_key or base_key
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO app_settings (key, value, value_type, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  value = excluded.value,
                  value_type = excluded.value_type,
                  updated_at = excluded.updated_at
                """,
                (persisted_key, str(value), value_type, now),
            )

    def delete_org_scoped_values(self, org_id: str) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        if not normalized_org_id:
            return
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM app_settings WHERE key LIKE ?",
                (f"org:{normalized_org_id}:%",),
            )

    def all_values(self) -> Dict[str, str]:
        rows = self._fetchall("SELECT key, value FROM app_settings ORDER BY key")
        return {row["key"]: row["value"] for row in rows}


class OrganizationRepository(BaseRepository):
    def get_organization_record(self, org_id: str) -> Optional[OrganizationRecord]:
        row = self._fetchone(
            """
            SELECT *
            FROM organizations
            WHERE org_id = ?
            LIMIT 1
            """,
            (str(org_id or "").strip() or "default",),
        )
        if not row:
            return None
        return OrganizationRecord.from_row(row)

    def get_default_organization_record(self) -> Optional[OrganizationRecord]:
        row = self._fetchone(
            """
            SELECT *
            FROM organizations
            WHERE is_default = 1
            LIMIT 1
            """
        )
        if row:
            return OrganizationRecord.from_row(row)
        return self.get_organization_record("default")

    def list_organization_records(self, *, enabled_only: bool = False) -> list[OrganizationRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if enabled_only:
            clauses.append("is_enabled = 1")
        sql = "SELECT * FROM organizations"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY is_default DESC, is_enabled DESC, name COLLATE NOCASE ASC, org_id ASC"
        return [OrganizationRecord.from_row(row) for row in self._fetchall(sql, tuple(params))]

    def ensure_default(
        self,
        *,
        config_path: str,
        name: str = "Default Organization",
        description: str = "",
    ) -> OrganizationRecord:
        resolved_config_path = os.path.abspath(str(config_path or "").strip() or "config.ini")
        existing = self.get_organization_record("default")
        if existing:
            needs_update = (
                not existing.is_default
                or not existing.is_enabled
                or not existing.config_path
                or os.path.normcase(os.path.abspath(existing.config_path)) != os.path.normcase(resolved_config_path)
            )
            if needs_update:
                with self.db.transaction() as conn:
                    conn.execute(
                        """
                        UPDATE organizations
                        SET name = ?,
                            config_path = ?,
                            description = ?,
                            is_enabled = 1,
                            is_default = 1,
                            updated_at = ?
                        WHERE org_id = 'default'
                        """,
                        (
                            existing.name or name,
                            resolved_config_path,
                            existing.description or description,
                            utcnow_iso(),
                        ),
                    )
                existing = self.get_organization_record("default")
            if existing:
                return existing
        self.upsert_organization(
            org_id="default",
            name=name,
            config_path=resolved_config_path,
            description=description,
            is_enabled=True,
        )
        return self.get_organization_record("default") or OrganizationRecord(
            org_id="default",
            name=name,
            config_path=resolved_config_path,
            description=description,
            is_enabled=True,
            is_default=True,
        )

    def upsert_organization(
        self,
        *,
        org_id: str,
        name: str,
        config_path: str,
        description: str = "",
        is_enabled: bool = True,
    ) -> None:
        normalized_org_id = str(org_id or "").strip().lower() or "default"
        normalized_name = str(name or "").strip() or normalized_org_id
        normalized_config_path = str(config_path or "").strip()
        if not normalized_config_path:
            normalized_config_path = "config.ini" if normalized_org_id == "default" else f"config.{normalized_org_id}.ini"
        normalized_config_path = os.path.abspath(normalized_config_path)
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO organizations (
                  org_id, name, config_path, description, is_enabled, is_default, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id) DO UPDATE SET
                  name = excluded.name,
                  config_path = excluded.config_path,
                  description = excluded.description,
                  is_enabled = excluded.is_enabled,
                  is_default = excluded.is_default,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_name,
                    normalized_config_path,
                    str(description or "").strip(),
                    1 if is_enabled or normalized_org_id == "default" else 0,
                    1 if normalized_org_id == "default" else 0,
                    now,
                    now,
                ),
            )
        GroupExclusionRuleRepository(self.db, default_org_id=normalized_org_id).ensure_defaults_for_org()

    def set_enabled(self, org_id: str, enabled: bool) -> None:
        normalized_org_id = str(org_id or "").strip().lower() or "default"
        if normalized_org_id == "default" and not enabled:
            raise ValueError("default organization cannot be disabled")
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE organizations
                SET is_enabled = ?,
                    updated_at = ?
                WHERE org_id = ?
                """,
                (1 if enabled else 0, utcnow_iso(), normalized_org_id),
            )

    def delete_organization(self, org_id: str) -> None:
        normalized_org_id = str(org_id or "").strip().lower() or "default"
        if normalized_org_id == "default":
            raise ValueError("default organization cannot be deleted")
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM organizations WHERE org_id = ?", (normalized_org_id,))


class OrganizationConfigRepository(BaseRepository):
    KEY_PREFIX = "orgcfg"

    def _config_key(self, org_id: str, field_name: str) -> str:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        return f"{self.KEY_PREFIX}:{normalized_org_id}:{field_name}"

    def has_config(self, org_id: Optional[str] = None) -> bool:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        return bool(
            self._fetchcount(
                "SELECT COUNT(*) FROM app_settings WHERE key LIKE ?",
                (f"{self.KEY_PREFIX}:{normalized_org_id}:%",),
            )
        )

    def _decode_value(self, field_name: str, value: str) -> Any:
        value = _decode_secret_field(field_name, value, secret_fields=ORGANIZATION_SECRET_FIELDS)
        value_type = ORGANIZATION_CONFIG_VALUE_TYPES.get(field_name, "string")
        if value_type == "bool":
            return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
        if value_type == "int":
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0
        if value_type == "json":
            try:
                parsed = json.loads(value or "[]")
            except json.JSONDecodeError:
                parsed = []
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
            return []
        return str(value or "")

    def _encode_value(self, field_name: str, value: Any) -> str:
        value_type = ORGANIZATION_CONFIG_VALUE_TYPES.get(field_name, "string")
        if value_type == "bool":
            return "true" if bool(value) else "false"
        if value_type == "int":
            try:
                return str(int(value))
            except (TypeError, ValueError):
                return "0"
        if value_type == "json":
            if isinstance(value, (list, tuple, set)):
                normalized_list = [str(item).strip() for item in value if str(item).strip()]
            else:
                normalized_list = []
            return json.dumps(normalized_list, ensure_ascii=False)
        return str(_encode_secret_field(field_name, value, secret_fields=ORGANIZATION_SECRET_FIELDS) or "")

    def import_legacy_config(self, org_id: Optional[str] = None, *, config_path: str = "") -> bool:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        normalized_path = str(config_path or "").strip()
        if not normalized_path or not os.path.exists(normalized_path):
            return False
        values = _load_org_config_values_from_file(normalized_path)
        self.save_config(normalized_org_id, values, config_path=normalized_path)
        return True

    def ensure_loaded(self, org_id: Optional[str] = None, *, config_path: str = "") -> None:
        if self.has_config(org_id):
            return
        self.import_legacy_config(org_id, config_path=config_path)

    def _load_stored_values(self, org_id: str) -> Dict[str, Any]:
        rows = self._fetchall(
            "SELECT key, value FROM app_settings WHERE key LIKE ? ORDER BY key ASC",
            (f"{self.KEY_PREFIX}:{org_id}:%",),
        )
        stored_values: Dict[str, Any] = {}
        for row in rows:
            key = str(row["key"] or "")
            parts = key.split(":", 2)
            if len(parts) != 3:
                continue
            field_name = parts[2]
            stored_values[field_name] = self._decode_value(field_name, str(row["value"] or ""))
        return stored_values

    def get_raw_config(self, org_id: Optional[str] = None, *, config_path: str = "") -> Dict[str, Any]:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        normalized_path = str(config_path or "").strip()
        self.ensure_loaded(normalized_org_id, config_path=normalized_path)
        stored_values = self._load_stored_values(normalized_org_id)
        return _normalize_org_config_values(
            stored_values,
            existing=stored_values,
            config_path=normalized_path or "config.ini",
        )

    def get_editable_config(self, org_id: Optional[str] = None, *, config_path: str = "") -> Dict[str, Any]:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        return _build_editable_org_config(
            self.get_raw_config(normalized_org_id, config_path=config_path),
            config_source=f"database:{normalized_org_id}",
        )

    def get_app_config(self, org_id: Optional[str] = None, *, config_path: str = "") -> AppConfig:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        return _build_app_config_from_org_values(
            self.get_raw_config(normalized_org_id, config_path=config_path),
            config_source=f"db:org:{normalized_org_id}",
        )

    def save_config(self, org_id: Optional[str], values: Dict[str, Any], *, config_path: str = "") -> Dict[str, Any]:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        stored_values = self._load_stored_values(normalized_org_id)
        existing = _normalize_org_config_values(stored_values, existing=stored_values, config_path=config_path or "config.ini")
        normalized_values = _normalize_org_config_values(values, existing=existing, config_path=config_path or "config.ini")
        now = utcnow_iso()
        with self.db.transaction() as conn:
            for field_name in ORGANIZATION_CONFIG_VALUE_TYPES:
                conn.execute(
                    """
                    INSERT INTO app_settings (key, value, value_type, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                      value = excluded.value,
                      value_type = excluded.value_type,
                      updated_at = excluded.updated_at
                    """,
                    (
                        self._config_key(normalized_org_id, field_name),
                        self._encode_value(field_name, normalized_values.get(field_name)),
                        ORGANIZATION_CONFIG_VALUE_TYPES[field_name],
                        now,
                    ),
                )
        return normalized_values

    def delete_config(self, org_id: str) -> None:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM app_settings WHERE key LIKE ?",
                (f"{self.KEY_PREFIX}:{normalized_org_id}:%",),
            )


class GroupExclusionRuleRepository(BaseRepository):
    def seed_defaults(self):
        org_rows = self._fetchall("SELECT org_id FROM organizations ORDER BY org_id ASC")
        org_ids = {str(row["org_id"] or "").strip().lower() or "default" for row in org_rows}
        org_ids.add("default")
        for org_id in sorted(org_ids):
            self.ensure_defaults_for_org(org_id)

    def ensure_defaults_for_org(self, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            for group_name in DEFAULT_HARD_PROTECTED_GROUPS:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO group_exclusion_rules (
                      org_id, rule_type, protection_level, match_type, match_value, display_name,
                      is_enabled, source, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_org_id,
                        "protect",
                        "hard",
                        "samaccountname",
                        group_name,
                        group_name,
                        1,
                        "system_seed",
                        now,
                        now,
                    ),
                )

            for group_name in DEFAULT_SOFT_EXCLUDED_GROUPS:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO group_exclusion_rules (
                      org_id, rule_type, protection_level, match_type, match_value, display_name,
                      is_enabled, source, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_org_id,
                        "exclude",
                        "soft",
                        "samaccountname",
                        group_name,
                        group_name,
                        1,
                        "system_seed",
                        now,
                        now,
                    ),
                )

    def list_enabled_rules(self, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchall(
                """
                SELECT * FROM group_exclusion_rules
                WHERE org_id = ?
                  AND is_enabled = 1
                ORDER BY protection_level DESC, source, display_name
                """,
                (normalized_org_id,),
            )
        return self._fetchall(
            """
            SELECT * FROM group_exclusion_rules
            WHERE is_enabled = 1
            ORDER BY org_id ASC, protection_level DESC, source, display_name
            """
        )

    def list_enabled_rule_records(self, *, org_id: Optional[str] = None) -> list[ExclusionRuleRecord]:
        return [ExclusionRuleRecord.from_row(row) for row in self.list_enabled_rules(org_id=org_id)]

    def list_rules(
        self,
        *,
        rule_type: Optional[str] = None,
        protection_level: Optional[str] = None,
        org_id: Optional[str] = None,
    ):
        clauses = []
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if rule_type:
            clauses.append("rule_type = ?")
            params.append(rule_type)
        if protection_level:
            clauses.append("protection_level = ?")
            params.append(protection_level)

        sql = "SELECT * FROM group_exclusion_rules"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY is_enabled DESC, protection_level DESC, source, display_name"
        return self._fetchall(sql, tuple(params))

    def list_rule_records(
        self,
        *,
        rule_type: Optional[str] = None,
        protection_level: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> list[ExclusionRuleRecord]:
        return [
            ExclusionRuleRecord.from_row(row)
            for row in self.list_rules(
                rule_type=rule_type,
                protection_level=protection_level,
                org_id=org_id,
            )
        ]

    def list_soft_excluded_group_names(self, *, enabled_only: bool = True, org_id: Optional[str] = None) -> list[str]:
        normalized_org_id = self._resolve_org_id(org_id)
        sql = """
            SELECT match_value
            FROM group_exclusion_rules
            WHERE rule_type = 'exclude'
              AND protection_level = 'soft'
              AND match_type = 'samaccountname'
        """
        params: list[Any] = []
        if normalized_org_id:
            sql += " AND org_id = ?"
            params.append(normalized_org_id)
        if enabled_only:
            sql += " AND is_enabled = 1"
        sql += " ORDER BY LOWER(match_value)"
        rows = self._fetchall(sql, tuple(params))
        return [row["match_value"] for row in rows]

    def list_soft_excluded_rules(self, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        sql = """
            SELECT *
            FROM group_exclusion_rules
            WHERE rule_type = 'exclude'
              AND protection_level = 'soft'
              AND match_type = 'samaccountname'
        """
        params: list[Any] = []
        if normalized_org_id:
            sql += " AND org_id = ?"
            params.append(normalized_org_id)
        sql += " ORDER BY is_enabled DESC, LOWER(match_value), source"
        rows = self._fetchall(sql, tuple(params))
        return [dict(row) for row in rows]

    def replace_soft_excluded_rules(self, rules: Iterable[Dict[str, Any]], *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        self.ensure_defaults_for_org(normalized_org_id)
        normalized_rules = []
        seen = set()
        for rule in rules:
            match_value = str(rule.get("match_value", "")).strip()
            if not match_value:
                continue
            lowered = match_value.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            normalized_rules.append(
                {
                    "match_value": match_value,
                    "display_name": str(rule.get("display_name") or match_value).strip() or match_value,
                    "is_enabled": 1 if rule.get("is_enabled", True) else 0,
                    "source": str(rule.get("source") or "user_ui").strip() or "user_ui",
                }
            )

        existing_rules = self._fetchall(
            """
            SELECT *
            FROM group_exclusion_rules
            WHERE org_id = ?
              AND rule_type = 'exclude'
              AND protection_level = 'soft'
              AND match_type = 'samaccountname'
            """,
            (normalized_org_id,),
        )
        existing_by_lower = {row["match_value"].lower(): row for row in existing_rules}
        now = utcnow_iso()

        with self.db.transaction() as conn:
            for rule in normalized_rules:
                lowered = rule["match_value"].lower()
                existing = existing_by_lower.get(lowered)
                if existing:
                    conn.execute(
                        """
                        UPDATE group_exclusion_rules
                        SET display_name = ?,
                            is_enabled = ?,
                            source = ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            rule["display_name"],
                            rule["is_enabled"],
                            rule["source"],
                            now,
                            existing["id"],
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO group_exclusion_rules (
                          org_id, rule_type, protection_level, match_type, match_value, display_name,
                          is_enabled, source, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            normalized_org_id,
                            "exclude",
                            "soft",
                            "samaccountname",
                            rule["match_value"],
                            rule["display_name"],
                            rule["is_enabled"],
                            rule["source"],
                            now,
                            now,
                        ),
                    )

            desired_set = {rule["match_value"].lower() for rule in normalized_rules}
            for lowered, existing in existing_by_lower.items():
                if lowered in desired_set:
                    continue
                conn.execute(
                    """
                    UPDATE group_exclusion_rules
                    SET is_enabled = 0,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, existing["id"]),
                )

    def sync_soft_excluded_groups(self, group_names: Iterable[str], *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        self.ensure_defaults_for_org(normalized_org_id)
        normalized_names = []
        seen = set()
        for group_name in group_names:
            if group_name is None:
                continue
            normalized = str(group_name).strip()
            if not normalized:
                continue
            lowered = normalized.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            normalized_names.append(normalized)

        existing_rules = self._fetchall(
            """
            SELECT *
            FROM group_exclusion_rules
            WHERE org_id = ?
              AND rule_type = 'exclude'
              AND protection_level = 'soft'
              AND match_type = 'samaccountname'
            """,
            (normalized_org_id,),
        )
        existing_by_lower = {row["match_value"].lower(): row for row in existing_rules}
        now = utcnow_iso()

        with self.db.transaction() as conn:
            for group_name in normalized_names:
                lowered = group_name.lower()
                existing = existing_by_lower.get(lowered)
                if existing:
                    conn.execute(
                        """
                        UPDATE group_exclusion_rules
                        SET display_name = ?,
                            is_enabled = 1,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (group_name, now, existing["id"]),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO group_exclusion_rules (
                          org_id, rule_type, protection_level, match_type, match_value, display_name,
                          is_enabled, source, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            normalized_org_id,
                            "exclude",
                            "soft",
                            "samaccountname",
                            group_name,
                            group_name,
                            1,
                            "user_ui",
                            now,
                            now,
                        ),
                    )

            desired_set = {name.lower() for name in normalized_names}
            for lowered, existing in existing_by_lower.items():
                if lowered in desired_set:
                    continue
                conn.execute(
                    """
                    UPDATE group_exclusion_rules
                    SET is_enabled = 0,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, existing["id"]),
                )

    def upsert_rule(
        self,
        *,
        rule_type: str,
        protection_level: str,
        match_type: str,
        match_value: str,
        display_name: str = "",
        is_enabled: bool = True,
        source: str = "import",
        org_id: Optional[str] = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        normalized_rule_type = str(rule_type or "").strip().lower()
        normalized_level = str(protection_level or "").strip().lower()
        normalized_match_type = str(match_type or "").strip().lower()
        normalized_match_value = str(match_value or "").strip()
        normalized_display_name = str(display_name or normalized_match_value).strip() or normalized_match_value
        normalized_source = str(source or "import").strip() or "import"
        if not normalized_rule_type or not normalized_level or not normalized_match_type or not normalized_match_value:
            raise ValueError("group exclusion rule fields are required")

        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO group_exclusion_rules (
                  org_id, rule_type, protection_level, match_type, match_value, display_name,
                  is_enabled, source, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, rule_type, protection_level, match_type, match_value) DO UPDATE SET
                  display_name = excluded.display_name,
                  is_enabled = excluded.is_enabled,
                  source = excluded.source,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_rule_type,
                    normalized_level,
                    normalized_match_type,
                    normalized_match_value,
                    normalized_display_name,
                    1 if is_enabled else 0,
                    normalized_source,
                    now,
                    now,
                ),
            )

    def evaluate_group(
        self,
        *,
        group_sam: Optional[str] = None,
        group_dn: Optional[str] = None,
        display_name: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        matched_rules = []
        for row in self.list_enabled_rules(org_id=org_id):
            match_type = row["match_type"]
            match_value = row["match_value"]
            is_match = False

            if match_type == "samaccountname" and group_sam:
                is_match = group_sam.lower() == match_value.lower()
            elif match_type == "dn" and group_dn:
                is_match = group_dn.lower() == match_value.lower()
            elif match_type == "display_name" and display_name:
                is_match = display_name.lower() == match_value.lower()

            if is_match:
                matched_rules.append(dict(row))

        is_hard_protected = any(
            rule["rule_type"] == "protect" and rule["protection_level"] == "hard"
            for rule in matched_rules
        )
        is_excluded = any(rule["rule_type"] == "exclude" for rule in matched_rules) or is_hard_protected
        return {
            "is_hard_protected": is_hard_protected,
            "is_excluded": is_excluded,
            "matched_rules": matched_rules,
        }

    def delete_rules_for_org(self, org_id: str) -> None:
        normalized_org_id = self._resolve_org_id(org_id, default="default")
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM group_exclusion_rules WHERE org_id = ?", (normalized_org_id,))


class SyncJobRepository(BaseRepository):
    ACTIVE_STATUSES = {"CREATED", "PLANNING", "READY", "RUNNING", "CANCELING"}

    def get_active_job(self, *, org_id: Optional[str] = None):
        placeholders = ",".join(["?"] * len(self.ACTIVE_STATUSES))
        params: list[Any] = list(self.ACTIVE_STATUSES)
        where_clauses = [f"status IN ({placeholders})"]
        normalized_org_id = str(org_id or "").strip()
        if normalized_org_id:
            where_clauses.append("org_id = ?")
            params.append(normalized_org_id)
        row = self._fetchone(
            f"""
            SELECT * FROM sync_jobs
            WHERE {' AND '.join(where_clauses)}
            ORDER BY started_at DESC
            LIMIT 1
            """,
            tuple(params),
        )
        return row

    def get_active_job_record(self, *, org_id: Optional[str] = None) -> Optional[SyncJobRecord]:
        row = self.get_active_job(org_id=org_id)
        if not row:
            return None
        return SyncJobRecord.from_row(row)

    def get_job(self, job_id: str):
        return self._fetchone(
            """
            SELECT * FROM sync_jobs
            WHERE job_id = ?
            LIMIT 1
            """,
            (job_id,),
        )

    def get_job_record(self, job_id: str) -> Optional[SyncJobRecord]:
        row = self.get_job(job_id)
        if not row:
            return None
        return SyncJobRecord.from_row(row)

    def list_recent_jobs(self, limit: int = 20, *, org_id: Optional[str] = None):
        normalized_org_id = str(org_id or "").strip()
        if normalized_org_id:
            return self._fetchall(
                """
                SELECT * FROM sync_jobs
                WHERE org_id = ?
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (normalized_org_id, int(limit)),
            )
        return self._fetchall(
            """
            SELECT * FROM sync_jobs
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (int(limit),),
        )

    def list_recent_job_records(self, limit: int = 20, *, org_id: Optional[str] = None) -> list[SyncJobRecord]:
        return [SyncJobRecord.from_row(row) for row in self.list_recent_jobs(limit=limit, org_id=org_id)]

    def count_jobs(self, *, org_id: Optional[str] = None) -> int:
        normalized_org_id = str(org_id or "").strip()
        if normalized_org_id:
            return self._fetchcount("SELECT COUNT(*) FROM sync_jobs WHERE org_id = ?", (normalized_org_id,))
        return self._fetchcount("SELECT COUNT(*) FROM sync_jobs")

    def create_job(
        self,
        job_id: str,
        trigger_type: str,
        execution_mode: str,
        status: str,
        org_id: str = "default",
        app_version: Optional[str] = None,
        plan_source_job_id: Optional[str] = None,
        config_snapshot_hash: Optional[str] = None,
    ):
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_jobs (
                  job_id, org_id, trigger_type, execution_mode, status, plan_source_job_id,
                  app_version, config_snapshot_hash, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    str(org_id or "").strip() or "default",
                    trigger_type,
                    execution_mode,
                    status,
                    plan_source_job_id,
                    app_version,
                    config_snapshot_hash,
                    utcnow_iso(),
                ),
            )

    def update_job(
        self,
        job_id: str,
        *,
        status: Optional[str] = None,
        planned_operation_count: Optional[int] = None,
        executed_operation_count: Optional[int] = None,
        error_count: Optional[int] = None,
        summary: Optional[Dict[str, Any]] = None,
        ended: bool = False,
    ):
        updates = []
        params: list[Any] = []
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if planned_operation_count is not None:
            updates.append("planned_operation_count = ?")
            params.append(planned_operation_count)
        if executed_operation_count is not None:
            updates.append("executed_operation_count = ?")
            params.append(executed_operation_count)
        if error_count is not None:
            updates.append("error_count = ?")
            params.append(error_count)
        if summary is not None:
            updates.append("summary_json = ?")
            params.append(dumps_json(summary))
        if ended:
            updates.append("ended_at = ?")
            params.append(utcnow_iso())

        if not updates:
            return

        params.append(job_id)
        with self.db.transaction() as conn:
            conn.execute(
                f"UPDATE sync_jobs SET {', '.join(updates)} WHERE job_id = ?",
                tuple(params),
            )


class SyncEventRepository(BaseRepository):
    def add_event(
        self,
        job_id: str,
        level: str,
        event_type: str,
        message: str,
        *,
        stage_name: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ):
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_events (
                  job_id, stage_name, level, event_type, message, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    stage_name,
                    level,
                    event_type,
                    message,
                    dumps_json(payload),
                    utcnow_iso(),
                ),
            )

    def list_events_for_job(self, job_id: str, limit: int = 100):
        return self._fetchall(
            """
            SELECT * FROM sync_events
            WHERE job_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (job_id, int(limit)),
        )

    def list_events_for_job_page(
        self,
        job_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        total = self._fetchcount(
            """
            SELECT COUNT(*)
            FROM sync_events
            WHERE job_id = ?
            """,
            (job_id,),
        )
        rows = self._fetchall(
            """
            SELECT *
            FROM sync_events
            WHERE job_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (job_id, int(limit), max(int(offset), 0)),
        )
        return [dict(row) for row in rows], total


class PlannedOperationRepository(BaseRepository):
    def add_operation(
        self,
        job_id: str,
        object_type: str,
        operation_type: str,
        *,
        source_id: Optional[str] = None,
        department_id: Optional[str] = None,
        target_dn: Optional[str] = None,
        desired_state: Optional[Dict[str, Any]] = None,
        risk_level: str = "normal",
        status: str = "planned",
    ) -> int:
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO planned_operations (
                  job_id, object_type, source_id, department_id, target_dn,
                  operation_type, desired_state_json, risk_level, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    object_type,
                    source_id,
                    department_id,
                    target_dn,
                    operation_type,
                    dumps_json(desired_state),
                    risk_level,
                    status,
                    utcnow_iso(),
                ),
            )
            return int(cursor.lastrowid)

    def list_operations_for_job(self, job_id: str, limit: int = 500) -> list[dict[str, Any]]:
        rows, _total = self.list_operations_for_job_page(job_id, limit=limit, offset=0)
        return rows

    def list_operations_for_job_page(
        self,
        job_id: str,
        *,
        limit: int = 500,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        total = self._fetchcount(
            """
            SELECT COUNT(*)
            FROM planned_operations
            WHERE job_id = ?
            """,
            (job_id,),
        )
        rows = self._fetchall(
            """
            SELECT *
            FROM planned_operations
            WHERE job_id = ?
            ORDER BY created_at ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (job_id, int(limit), max(int(offset), 0)),
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            desired_state = row["desired_state_json"] if "desired_state_json" in row.keys() else None
            if isinstance(desired_state, str) and desired_state:
                try:
                    desired_state = json.loads(desired_state)
                except json.JSONDecodeError:
                    desired_state = {"raw": desired_state}
            result.append(
                {
                    "id": int(row["id"]),
                    "job_id": str(row["job_id"] or ""),
                    "object_type": str(row["object_type"] or ""),
                    "source_id": str(row["source_id"] or ""),
                    "department_id": str(row["department_id"] or ""),
                    "target_dn": str(row["target_dn"] or ""),
                    "operation_type": str(row["operation_type"] or ""),
                    "desired_state": desired_state if isinstance(desired_state, dict) or desired_state is None else {"raw": desired_state},
                    "risk_level": str(row["risk_level"] or "normal"),
                    "status": str(row["status"] or "planned"),
                    "created_at": str(row["created_at"] or ""),
                }
            )
        return result, total


class ObjectStateRepository(BaseRepository):
    def upsert_state(
        self,
        source_type: str,
        object_type: str,
        source_id: str,
        source_hash: str,
        *,
        org_id: Optional[str] = None,
        display_name: Optional[str] = None,
        target_dn: Optional[str] = None,
        last_job_id: Optional[str] = None,
        last_action: Optional[str] = None,
        last_status: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ):
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO object_sync_state (
                  org_id, source_type, object_type, source_id, source_hash, display_name,
                  target_dn, last_seen_at, last_job_id, last_action, last_status, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, source_type, object_type, source_id) DO UPDATE SET
                  source_hash = excluded.source_hash,
                  display_name = excluded.display_name,
                  target_dn = excluded.target_dn,
                  last_seen_at = excluded.last_seen_at,
                  last_job_id = excluded.last_job_id,
                  last_action = excluded.last_action,
                  last_status = excluded.last_status,
                  extra_json = excluded.extra_json
                """,
                (
                    normalized_org_id,
                    source_type,
                    object_type,
                    source_id,
                    source_hash,
                    display_name,
                    target_dn,
                    utcnow_iso(),
                    last_job_id,
                    last_action,
                    last_status,
                    dumps_json(extra),
                ),
            )

    def get_state(
        self,
        source_type: str,
        object_type: str,
        source_id: str,
        *,
        org_id: Optional[str] = None,
    ):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchone(
                """
                SELECT * FROM object_sync_state
                WHERE org_id = ? AND source_type = ? AND object_type = ? AND source_id = ?
                """,
                (normalized_org_id, source_type, object_type, source_id),
            )
        return self._fetchone(
            """
            SELECT * FROM object_sync_state
            WHERE source_type = ? AND object_type = ? AND source_id = ?
            ORDER BY org_id ASC, id ASC
            LIMIT 1
            """,
            (source_type, object_type, source_id),
        )

    def count_by_type(self, source_type: str, object_type: str, *, org_id: Optional[str] = None) -> int:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT COUNT(*) AS total FROM object_sync_state
                WHERE org_id = ? AND source_type = ? AND object_type = ?
                """,
                (normalized_org_id, source_type, object_type),
            )
        else:
            row = self._fetchone(
                """
                SELECT COUNT(*) AS total FROM object_sync_state
                WHERE source_type = ? AND object_type = ?
                """,
                (source_type, object_type),
            )
        return int(row["total"]) if row else 0

    def delete_missing(
        self,
        source_type: str,
        object_type: str,
        current_ids: Iterable[str],
        *,
        org_id: Optional[str] = None,
    ) -> int:
        current_ids = list(current_ids)
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if current_ids:
                placeholders = ",".join(["?"] * len(current_ids))
                if normalized_org_id:
                    cursor = conn.execute(
                        f"""
                        DELETE FROM object_sync_state
                        WHERE org_id = ? AND source_type = ? AND object_type = ? AND source_id NOT IN ({placeholders})
                        """,
                        (normalized_org_id, source_type, object_type, *current_ids),
                    )
                else:
                    cursor = conn.execute(
                        f"""
                        DELETE FROM object_sync_state
                        WHERE source_type = ? AND object_type = ? AND source_id NOT IN ({placeholders})
                        """,
                        (source_type, object_type, *current_ids),
                    )
            else:
                if normalized_org_id:
                    cursor = conn.execute(
                        """
                        DELETE FROM object_sync_state
                        WHERE org_id = ? AND source_type = ? AND object_type = ?
                        """,
                        (normalized_org_id, source_type, object_type),
                    )
                else:
                    cursor = conn.execute(
                        """
                        DELETE FROM object_sync_state
                        WHERE source_type = ? AND object_type = ?
                        """,
                        (source_type, object_type),
                )
            return cursor.rowcount

    def delete_states_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM object_sync_state WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )


class ManagedGroupBindingRepository(BaseRepository):
    def upsert_binding(
        self,
        department_id: str,
        group_sam: str,
        *,
        org_id: Optional[str] = None,
        parent_department_id: Optional[str] = None,
        group_dn: Optional[str] = None,
        group_cn: Optional[str] = None,
        display_name: Optional[str] = None,
        path_text: Optional[str] = None,
        status: str = "active",
    ):
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO managed_group_bindings (
                  org_id, department_id, parent_department_id, group_sam, group_dn, group_cn,
                  display_name, path_text, status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, department_id) DO UPDATE SET
                  parent_department_id = excluded.parent_department_id,
                  group_sam = excluded.group_sam,
                  group_dn = excluded.group_dn,
                  group_cn = excluded.group_cn,
                  display_name = excluded.display_name,
                  path_text = excluded.path_text,
                  status = excluded.status,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    department_id,
                    parent_department_id,
                    group_sam,
                    group_dn,
                    group_cn,
                    display_name,
                    path_text,
                    status,
                    utcnow_iso(),
                ),
            )

    def get_by_department_id(self, department_id: str, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchone(
                "SELECT * FROM managed_group_bindings WHERE org_id = ? AND department_id = ?",
                (normalized_org_id, department_id),
            )
        return self._fetchone(
            "SELECT * FROM managed_group_bindings WHERE department_id = ? ORDER BY org_id ASC LIMIT 1",
            (department_id,),
        )

    def get_binding_record_by_department_id(
        self,
        department_id: str,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[ManagedGroupBindingRecord]:
        row = self.get_by_department_id(department_id, org_id=org_id)
        if not row:
            return None
        return ManagedGroupBindingRecord.from_row(row)

    def get_by_group_sam(self, group_sam: str, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchone(
                "SELECT * FROM managed_group_bindings WHERE org_id = ? AND group_sam = ?",
                (normalized_org_id, group_sam),
            )
        return self._fetchone(
            "SELECT * FROM managed_group_bindings WHERE group_sam = ? ORDER BY org_id ASC LIMIT 1",
            (group_sam,),
        )

    def get_binding_record_by_group_sam(
        self,
        group_sam: str,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[ManagedGroupBindingRecord]:
        row = self.get_by_group_sam(group_sam, org_id=org_id)
        if not row:
            return None
        return ManagedGroupBindingRecord.from_row(row)

    def list_active_bindings(self, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchall(
                """
                SELECT * FROM managed_group_bindings
                WHERE org_id = ?
                  AND status = 'active'
                ORDER BY department_id
                """,
                (normalized_org_id,),
            )
        return self._fetchall(
            """
            SELECT * FROM managed_group_bindings
            WHERE status = 'active'
            ORDER BY org_id ASC, department_id
            """
        )

    def list_active_binding_records(self, *, org_id: Optional[str] = None) -> list[ManagedGroupBindingRecord]:
        return [ManagedGroupBindingRecord.from_row(row) for row in self.list_active_bindings(org_id=org_id)]

    def delete_bindings_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM managed_group_bindings WHERE org_id = ?", (self._resolve_org_id(org_id, default="default"),))


class UserIdentityBindingRepository(BaseRepository):
    def get_by_wecom_userid(self, wecom_userid: str, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchone(
                """
                SELECT * FROM user_identity_bindings
                WHERE org_id = ?
                  AND source_user_id = ?
                LIMIT 1
                """,
                (normalized_org_id, wecom_userid),
            )
        return self._fetchone(
            """
            SELECT * FROM user_identity_bindings
            WHERE source_user_id = ?
            ORDER BY org_id ASC, id ASC
            LIMIT 1
            """,
            (wecom_userid,),
        )

    def get_binding_record_by_wecom_userid(
        self,
        wecom_userid: str,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[UserIdentityBindingRecord]:
        row = self.get_by_wecom_userid(wecom_userid, org_id=org_id)
        if not row:
            return None
        return UserIdentityBindingRecord.from_row(row)

    def get_by_source_user_id(self, source_user_id: str, *, org_id: Optional[str] = None):
        return self.get_by_wecom_userid(source_user_id, org_id=org_id)

    def get_binding_record_by_source_user_id(
        self,
        source_user_id: str,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[UserIdentityBindingRecord]:
        return self.get_binding_record_by_wecom_userid(source_user_id, org_id=org_id)

    def get_by_ad_username(
        self,
        ad_username: str,
        *,
        connector_id: str | None = None,
        org_id: Optional[str] = None,
    ):
        normalized_connector_id = str(connector_id or "").strip()
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_connector_id:
            if normalized_org_id:
                return self._fetchone(
                    """
                    SELECT * FROM user_identity_bindings
                    WHERE org_id = ?
                      AND connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    LIMIT 1
                    """,
                    (normalized_org_id, normalized_connector_id, ad_username),
                )
            return self._fetchone(
                """
                SELECT * FROM user_identity_bindings
                WHERE connector_id = ?
                  AND LOWER(ad_username) = LOWER(?)
                ORDER BY org_id ASC, id ASC
                LIMIT 1
                """,
                (normalized_connector_id, ad_username),
            )
        if normalized_org_id:
            return self._fetchone(
                """
                SELECT * FROM user_identity_bindings
                WHERE org_id = ?
                  AND LOWER(ad_username) = LOWER(?)
                ORDER BY connector_id ASC, id ASC
                LIMIT 1
                """,
                (normalized_org_id, ad_username),
            )
        return self._fetchone(
            """
            SELECT * FROM user_identity_bindings
            WHERE LOWER(ad_username) = LOWER(?)
            ORDER BY org_id ASC, connector_id ASC, id ASC
            LIMIT 1
            """,
            (ad_username,),
        )

    def get_binding_record_by_ad_username(
        self,
        ad_username: str,
        *,
        connector_id: str | None = None,
        org_id: Optional[str] = None,
    ) -> Optional[UserIdentityBindingRecord]:
        row = self.get_by_ad_username(ad_username, connector_id=connector_id, org_id=org_id)
        if not row:
            return None
        return UserIdentityBindingRecord.from_row(row)

    def list_enabled_binding_records(self, *, org_id: Optional[str] = None) -> list[UserIdentityBindingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                """
                SELECT * FROM user_identity_bindings
                WHERE org_id = ?
                  AND is_enabled = 1
                ORDER BY source_user_id ASC
                """,
                (normalized_org_id,),
            )
        else:
            rows = self._fetchall(
                """
                SELECT * FROM user_identity_bindings
                WHERE is_enabled = 1
                ORDER BY org_id ASC, source_user_id ASC
                """
            )
        return [UserIdentityBindingRecord.from_row(row) for row in rows]

    def list_binding_records(self, *, org_id: Optional[str] = None) -> list[UserIdentityBindingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                """
                SELECT * FROM user_identity_bindings
                WHERE org_id = ?
                ORDER BY is_enabled DESC, source_user_id ASC
                """,
                (normalized_org_id,),
            )
        else:
            rows = self._fetchall(
                """
                SELECT * FROM user_identity_bindings
                ORDER BY org_id ASC, is_enabled DESC, source_user_id ASC
                """
            )
        return [UserIdentityBindingRecord.from_row(row) for row in rows]

    def list_binding_records_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        query: str = "",
        status: str = "all",
        org_id: Optional[str] = None,
    ) -> tuple[list[UserIdentityBindingRecord], int]:
        normalized_query = str(query or "").strip().lower()
        normalized_status = str(status or "all").strip().lower()
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if normalized_status == "enabled":
            clauses.append("is_enabled = 1")
        elif normalized_status == "disabled":
            clauses.append("is_enabled = 0")
        if normalized_query:
            clauses.append(
                "("
                "LOWER(source_user_id) LIKE ? OR "
                "LOWER(connector_id) LIKE ? OR "
                "LOWER(ad_username) LIKE ? OR "
                "LOWER(COALESCE(notes, '')) LIKE ?"
                ")"
            )
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 4)
        where_clause = " WHERE " + " AND ".join(clauses)
        total = self._fetchcount(
            f"""
            SELECT COUNT(*)
            FROM user_identity_bindings
            {where_clause}
            """,
            tuple(params),
        )
        rows = self._fetchall(
            f"""
            SELECT *
            FROM user_identity_bindings
            {where_clause}
            ORDER BY is_enabled DESC, connector_id ASC, source_user_id ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), max(int(offset), 0)),
        )
        return [UserIdentityBindingRecord.from_row(row) for row in rows], total

    def upsert_binding(
        self,
        wecom_userid: str,
        ad_username: str,
        *,
        org_id: Optional[str] = None,
        connector_id: str = "default",
        source: str = "derived_default",
        notes: str = "",
        is_enabled: bool = True,
        preserve_manual: bool = True,
    ) -> None:
        wecom_userid = str(wecom_userid).strip()
        ad_username = str(ad_username).strip()
        connector_id = str(connector_id or "default").strip() or "default"
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        if not wecom_userid or not ad_username:
            raise ValueError("wecom_userid and ad_username are required")

        now = utcnow_iso()
        existing = self.get_binding_record_by_wecom_userid(wecom_userid, org_id=normalized_org_id)
        if existing and preserve_manual and existing.source == "manual":
            return

        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO user_identity_bindings (
                  org_id, source_user_id, connector_id, ad_username, source, notes, is_enabled, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, source_user_id) DO UPDATE SET
                  connector_id = excluded.connector_id,
                  ad_username = excluded.ad_username,
                  source = excluded.source,
                  notes = excluded.notes,
                  is_enabled = excluded.is_enabled,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    wecom_userid,
                    connector_id,
                    ad_username,
                    source,
                    notes,
                    1 if is_enabled else 0,
                    now,
                ),
            )

    def upsert_binding_for_source_user(
        self,
        source_user_id: str,
        ad_username: str,
        *,
        connector_id: str = "default",
        source: str = "derived_default",
        notes: str = "",
        is_enabled: bool = True,
        preserve_manual: bool = True,
        org_id: Optional[str] = None,
    ) -> None:
        self.upsert_binding(
            source_user_id,
            ad_username,
            connector_id=connector_id,
            source=source,
            notes=notes,
            is_enabled=is_enabled,
            preserve_manual=preserve_manual,
            org_id=org_id,
        )

    def set_enabled(self, wecom_userid: str, enabled: bool, *, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE user_identity_bindings
                    SET is_enabled = ?,
                        updated_at = ?
                    WHERE org_id = ?
                      AND source_user_id = ?
                    """,
                    (1 if enabled else 0, utcnow_iso(), normalized_org_id, wecom_userid),
                )
            else:
                conn.execute(
                    """
                    UPDATE user_identity_bindings
                    SET is_enabled = ?,
                        updated_at = ?
                    WHERE source_user_id = ?
                    """,
                    (1 if enabled else 0, utcnow_iso(), wecom_userid),
                )

    def set_enabled_for_source_user(
        self,
        source_user_id: str,
        enabled: bool,
        *,
        org_id: Optional[str] = None,
    ) -> None:
        self.set_enabled(source_user_id, enabled, org_id=org_id)

    def delete_bindings_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM user_identity_bindings WHERE org_id = ?", (self._resolve_org_id(org_id, default="default"),))


class UserDepartmentOverrideRepository(BaseRepository):
    def get_by_wecom_userid(self, wecom_userid: str, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchone(
                """
                SELECT * FROM user_department_overrides
                WHERE org_id = ?
                  AND source_user_id = ?
                LIMIT 1
                """,
                (normalized_org_id, wecom_userid),
            )
        return self._fetchone(
            """
            SELECT * FROM user_department_overrides
            WHERE source_user_id = ?
            ORDER BY org_id ASC, id ASC
            LIMIT 1
            """,
            (wecom_userid,),
        )

    def get_by_source_user_id(self, source_user_id: str, *, org_id: Optional[str] = None):
        return self.get_by_wecom_userid(source_user_id, org_id=org_id)

    def get_override_record_by_wecom_userid(
        self,
        wecom_userid: str,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[UserDepartmentOverrideRecord]:
        row = self.get_by_wecom_userid(wecom_userid, org_id=org_id)
        if not row:
            return None
        return UserDepartmentOverrideRecord.from_row(row)

    def get_override_record_by_source_user_id(
        self,
        source_user_id: str,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[UserDepartmentOverrideRecord]:
        return self.get_override_record_by_wecom_userid(source_user_id, org_id=org_id)

    def list_override_records(self, *, org_id: Optional[str] = None) -> list[UserDepartmentOverrideRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                """
                SELECT * FROM user_department_overrides
                WHERE org_id = ?
                ORDER BY source_user_id ASC
                """,
                (normalized_org_id,),
            )
        else:
            rows = self._fetchall(
                """
                SELECT * FROM user_department_overrides
                ORDER BY org_id ASC, source_user_id ASC
                """
            )
        return [UserDepartmentOverrideRecord.from_row(row) for row in rows]

    def list_override_records_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        query: str = "",
        org_id: Optional[str] = None,
    ) -> tuple[list[UserDepartmentOverrideRecord], int]:
        normalized_query = str(query or "").strip().lower()
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if normalized_query:
            clauses.append(
                "("
                "LOWER(source_user_id) LIKE ? OR "
                "LOWER(primary_department_id) LIKE ? OR "
                "LOWER(COALESCE(notes, '')) LIKE ?"
                ")"
            )
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 3)
        where_clause = " WHERE " + " AND ".join(clauses)
        total = self._fetchcount(
            f"""
            SELECT COUNT(*)
            FROM user_department_overrides
            {where_clause}
            """,
            tuple(params),
        )
        rows = self._fetchall(
            f"""
            SELECT *
            FROM user_department_overrides
            {where_clause}
            ORDER BY source_user_id ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), max(int(offset), 0)),
        )
        return [UserDepartmentOverrideRecord.from_row(row) for row in rows], total

    def upsert_override(
        self,
        wecom_userid: str,
        primary_department_id: str,
        *,
        org_id: Optional[str] = None,
        notes: str = "",
    ) -> None:
        wecom_userid = str(wecom_userid).strip()
        primary_department_id = str(primary_department_id).strip()
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        if not wecom_userid or not primary_department_id:
            raise ValueError("wecom_userid and primary_department_id are required")

        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO user_department_overrides (
                  org_id, source_user_id, primary_department_id, notes, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(org_id, source_user_id) DO UPDATE SET
                  primary_department_id = excluded.primary_department_id,
                  notes = excluded.notes,
                  updated_at = excluded.updated_at
                """,
                (normalized_org_id, wecom_userid, primary_department_id, notes, utcnow_iso()),
            )

    def upsert_override_for_source_user(
        self,
        source_user_id: str,
        primary_department_id: str,
        *,
        org_id: Optional[str] = None,
        notes: str = "",
    ) -> None:
        self.upsert_override(
            source_user_id,
            primary_department_id,
            org_id=org_id,
            notes=notes,
        )

    def delete_override(self, wecom_userid: str, *, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    DELETE FROM user_department_overrides
                    WHERE org_id = ?
                      AND source_user_id = ?
                    """,
                    (normalized_org_id, wecom_userid),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM user_department_overrides
                    WHERE source_user_id = ?
                    """,
                    (wecom_userid,),
                )

    def delete_override_for_source_user(self, source_user_id: str, *, org_id: Optional[str] = None) -> None:
        self.delete_override(source_user_id, org_id=org_id)

    def delete_overrides_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM user_department_overrides WHERE org_id = ?", (self._resolve_org_id(org_id, default="default"),))


class AttributeMappingRuleRepository(BaseRepository):
    def get_rule_record(self, rule_id: int, *, org_id: Optional[str] = None) -> Optional[AttributeMappingRuleRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM attribute_mapping_rules
                WHERE id = ?
                  AND org_id = ?
                LIMIT 1
                """,
                (int(rule_id), normalized_org_id),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM attribute_mapping_rules
                WHERE id = ?
                LIMIT 1
                """,
                (int(rule_id),),
            )
        if not row:
            return None
        return AttributeMappingRuleRecord.from_row(row)

    def list_rule_records(
        self,
        *,
        direction: str | None = None,
        connector_id: str | None = None,
        enabled_only: bool = False,
        org_id: Optional[str] = None,
    ) -> list[AttributeMappingRuleRecord]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        normalized_direction = (
            normalize_mapping_direction(direction)
            if str(direction or "").strip()
            else ""
        )
        normalized_connector = str(connector_id or "").strip()
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if normalized_direction:
            clauses.append("direction = ?")
            params.append(normalized_direction)
        if normalized_connector:
            clauses.append("(connector_id = '' OR connector_id = ?)")
            params.append(normalized_connector)
        if enabled_only:
            clauses.append("is_enabled = 1")
        rows = self._fetchall(
            f"""
            SELECT *
            FROM attribute_mapping_rules
            WHERE {' AND '.join(clauses)}
            ORDER BY CASE WHEN connector_id = '' THEN 0 ELSE 1 END ASC, target_field ASC, id ASC
            """,
            tuple(params),
        )
        return [AttributeMappingRuleRecord.from_row(row) for row in rows]

    def list_rule_records_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        direction: str = "",
        connector_id: str = "",
        query: str = "",
        org_id: Optional[str] = None,
    ) -> tuple[list[AttributeMappingRuleRecord], int]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        normalized_direction = (
            normalize_mapping_direction(direction)
            if str(direction or "").strip()
            else ""
        )
        normalized_connector = str(connector_id or "").strip()
        normalized_query = str(query or "").strip().lower()
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if normalized_direction:
            clauses.append("direction = ?")
            params.append(normalized_direction)
        if normalized_connector:
            clauses.append("connector_id = ?")
            params.append(normalized_connector)
        if normalized_query:
            clauses.append(
                "("
                "LOWER(connector_id) LIKE ? OR "
                "LOWER(source_field) LIKE ? OR "
                "LOWER(target_field) LIKE ? OR "
                "LOWER(COALESCE(transform_template, '')) LIKE ? OR "
                "LOWER(COALESCE(notes, '')) LIKE ?"
                ")"
            )
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 5)
        where_clause = " WHERE " + " AND ".join(clauses)
        total = self._fetchcount(
            f"""
            SELECT COUNT(*)
            FROM attribute_mapping_rules
            {where_clause}
            """,
            tuple(params),
        )
        rows = self._fetchall(
            f"""
            SELECT *
            FROM attribute_mapping_rules
            {where_clause}
            ORDER BY direction ASC, connector_id ASC, target_field ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), max(int(offset), 0)),
        )
        return [AttributeMappingRuleRecord.from_row(row) for row in rows], total

    def upsert_rule(
        self,
        *,
        direction: str,
        source_field: str,
        target_field: str,
        connector_id: str = "",
        transform_template: str = "",
        sync_mode: str = "replace",
        is_enabled: bool = True,
        notes: str = "",
        org_id: Optional[str] = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_direction = normalize_mapping_direction(direction)
        normalized_source = str(source_field or "").strip()
        normalized_target = str(target_field or "").strip()
        normalized_connector = str(connector_id or "").strip()
        normalized_mode = str(sync_mode or "replace").strip().lower()
        if normalized_direction not in {"source_to_ad", "ad_to_source"}:
            raise ValueError("unsupported mapping direction")
        if normalized_mode not in {"replace", "fill_if_empty", "preserve"}:
            raise ValueError("unsupported mapping sync_mode")
        if not normalized_source or not normalized_target:
            raise ValueError("source_field and target_field are required")

        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO attribute_mapping_rules (
                  org_id, connector_id, direction, source_field, target_field, transform_template,
                  sync_mode, is_enabled, notes, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, connector_id, direction, source_field, target_field) DO UPDATE SET
                  transform_template = excluded.transform_template,
                  sync_mode = excluded.sync_mode,
                  is_enabled = excluded.is_enabled,
                  notes = excluded.notes,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_connector,
                    normalized_direction,
                    normalized_source,
                    normalized_target,
                    str(transform_template or "").strip(),
                    normalized_mode,
                    1 if is_enabled else 0,
                    str(notes or "").strip(),
                    now,
                    now,
                ),
            )

    def upsert_override_for_source_user(
        self,
        source_user_id: str,
        primary_department_id: str,
        *,
        notes: str = "",
        org_id: Optional[str] = None,
    ) -> None:
        self.upsert_override(
            source_user_id,
            primary_department_id,
            notes=notes,
            org_id=org_id,
        )

    def delete_rule(self, rule_id: int, *, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    "DELETE FROM attribute_mapping_rules WHERE id = ? AND org_id = ?",
                    (int(rule_id), normalized_org_id),
                )
            else:
                conn.execute("DELETE FROM attribute_mapping_rules WHERE id = ?", (int(rule_id),))

    def delete_rules_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM attribute_mapping_rules WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )


class SyncConnectorRepository(BaseRepository):
    @staticmethod
    def _row_with_decrypted_secrets(row: Any) -> dict[str, Any]:
        data = dict(row)
        for field_name in CONNECTOR_SECRET_FIELDS:
            if field_name in data:
                data[field_name] = _decode_secret_field(field_name, data.get(field_name), secret_fields=CONNECTOR_SECRET_FIELDS)
        return data

    def _import_legacy_connector_config(self, record: SyncConnectorRecord) -> Optional[SyncConnectorRecord]:
        normalized_path = str(record.config_path or "").strip()
        if not normalized_path or not os.path.exists(normalized_path):
            return None
        values = _load_connector_config_values_from_file(normalized_path)
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE sync_connectors
                SET config_path = ?,
                    ldap_server = ?,
                    ldap_domain = ?,
                    ldap_username = ?,
                    ldap_password = ?,
                    ldap_use_ssl = ?,
                    ldap_port = ?,
                    ldap_validate_cert = ?,
                    ldap_ca_cert_path = ?,
                    default_password = ?,
                    force_change_password = ?,
                    password_complexity = ?,
                    updated_at = ?
                WHERE connector_id = ?
                  AND org_id = ?
                """,
                (
                    values["config_path"],
                    values["ldap_server"],
                    values["ldap_domain"],
                    values["ldap_username"],
                    _encode_secret_field("ldap_password", values["ldap_password"], secret_fields=CONNECTOR_SECRET_FIELDS),
                    1 if values["ldap_use_ssl"] else 0 if values["ldap_use_ssl"] is not None else None,
                    values["ldap_port"],
                    1 if values["ldap_validate_cert"] else 0 if values["ldap_validate_cert"] is not None else None,
                    values["ldap_ca_cert_path"],
                    _encode_secret_field("default_password", values["default_password"], secret_fields=CONNECTOR_SECRET_FIELDS),
                    1 if values["force_change_password"] else 0 if values["force_change_password"] is not None else None,
                    values["password_complexity"],
                    now,
                    record.connector_id,
                    record.org_id,
                ),
            )
        return self.get_connector_record(record.connector_id, org_id=record.org_id)

    def get_connector_app_config(
        self,
        connector_id: str,
        *,
        base_config: AppConfig,
        org_id: Optional[str] = None,
    ) -> Optional[AppConfig]:
        record = self.get_connector_record(connector_id, org_id=org_id)
        if not record:
            return None
        if not _record_has_connector_overrides(record) and record.config_path and os.path.exists(record.config_path):
            record = self._import_legacy_connector_config(record) or record
        if record.config_path and not _record_has_connector_overrides(record) and not os.path.exists(record.config_path):
            return None
        return _build_app_config_from_connector_record(
            record,
            base_config=base_config,
            config_source=f"db:connector:{record.org_id}:{record.connector_id}",
        )

    def get_connector_record(self, connector_id: str, *, org_id: Optional[str] = None) -> Optional[SyncConnectorRecord]:
        normalized_connector_id = str(connector_id or "").strip()
        normalized_org_id = str(org_id or "").strip()
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM sync_connectors
                WHERE connector_id = ?
                  AND org_id = ?
                LIMIT 1
                """,
                (normalized_connector_id, normalized_org_id),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM sync_connectors
                WHERE connector_id = ?
                LIMIT 1
                """,
                (normalized_connector_id,),
            )
        if not row:
            return None
        return SyncConnectorRecord.from_row(self._row_with_decrypted_secrets(row))

    def list_connector_records(
        self,
        *,
        enabled_only: bool = False,
        org_id: Optional[str] = None,
    ) -> list[SyncConnectorRecord]:
        clauses = []
        params: list[Any] = []
        normalized_org_id = str(org_id or "").strip()
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if enabled_only:
            clauses.append("is_enabled = 1")
        sql = "SELECT * FROM sync_connectors"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY is_enabled DESC, name COLLATE NOCASE ASC, connector_id ASC"
        return [SyncConnectorRecord.from_row(self._row_with_decrypted_secrets(row)) for row in self._fetchall(sql, tuple(params))]

    def count_connectors(self, *, org_id: Optional[str] = None) -> int:
        normalized_org_id = str(org_id or "").strip()
        if normalized_org_id:
            return self._fetchcount("SELECT COUNT(*) FROM sync_connectors WHERE org_id = ?", (normalized_org_id,))
        return self._fetchcount("SELECT COUNT(*) FROM sync_connectors")

    def upsert_connector(
        self,
        *,
        connector_id: str,
        org_id: str = "default",
        name: str,
        config_path: str,
        ldap_server: str = "",
        ldap_domain: str = "",
        ldap_username: str = "",
        ldap_password: str = "",
        ldap_use_ssl: Any = None,
        ldap_port: Any = None,
        ldap_validate_cert: Any = None,
        ldap_ca_cert_path: str = "",
        default_password: str = "",
        force_change_password: Any = None,
        password_complexity: str = "",
        root_department_ids: Iterable[int] = (),
        username_template: str = "",
        disabled_users_ou: str = "",
        group_type: str = "security",
        group_mail_domain: str = "",
        custom_group_ou_path: str = "",
        managed_tag_ids: Iterable[str] = (),
        managed_external_chat_ids: Iterable[str] = (),
        is_enabled: bool = True,
    ) -> None:
        normalized_connector = str(connector_id or "").strip()
        if not normalized_connector or normalized_connector == "default":
            raise ValueError("connector_id is required and cannot be reserved value 'default'")
        normalized_org_id = str(org_id or "").strip() or "default"
        existing_record = self.get_connector_record(normalized_connector, org_id=normalized_org_id)
        existing_values = (
            {
                "config_path": existing_record.config_path,
                "ldap_server": existing_record.ldap_server,
                "ldap_domain": existing_record.ldap_domain,
                "ldap_username": existing_record.ldap_username,
                "ldap_password": existing_record.ldap_password,
                "ldap_use_ssl": existing_record.ldap_use_ssl,
                "ldap_port": existing_record.ldap_port,
                "ldap_validate_cert": existing_record.ldap_validate_cert,
                "ldap_ca_cert_path": existing_record.ldap_ca_cert_path,
                "default_password": existing_record.default_password,
                "force_change_password": existing_record.force_change_password,
                "password_complexity": existing_record.password_complexity,
            }
            if existing_record
            else None
        )
        normalized_config = _normalize_connector_config_values(
            {
                "config_path": config_path,
                "ldap_server": ldap_server,
                "ldap_domain": ldap_domain,
                "ldap_username": ldap_username,
                "ldap_password": ldap_password,
                "ldap_use_ssl": ldap_use_ssl,
                "ldap_port": ldap_port,
                "ldap_validate_cert": ldap_validate_cert,
                "ldap_ca_cert_path": ldap_ca_cert_path,
                "default_password": default_password,
                "force_change_password": force_change_password,
                "password_complexity": password_complexity,
            },
            existing=existing_values,
            config_path=config_path,
        )
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_connectors (
                  connector_id, org_id, name, config_path,
                  ldap_server, ldap_domain, ldap_username, ldap_password, ldap_use_ssl, ldap_port,
                  ldap_validate_cert, ldap_ca_cert_path, default_password, force_change_password,
                  password_complexity, root_department_ids_json, username_template,
                  disabled_users_ou, group_type, group_mail_domain, custom_group_ou_path,
                  managed_tag_ids_json, managed_external_chat_ids_json, is_enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(connector_id) DO UPDATE SET
                  org_id = excluded.org_id,
                  name = excluded.name,
                  config_path = excluded.config_path,
                  ldap_server = excluded.ldap_server,
                  ldap_domain = excluded.ldap_domain,
                  ldap_username = excluded.ldap_username,
                  ldap_password = excluded.ldap_password,
                  ldap_use_ssl = excluded.ldap_use_ssl,
                  ldap_port = excluded.ldap_port,
                  ldap_validate_cert = excluded.ldap_validate_cert,
                  ldap_ca_cert_path = excluded.ldap_ca_cert_path,
                  default_password = excluded.default_password,
                  force_change_password = excluded.force_change_password,
                  password_complexity = excluded.password_complexity,
                  root_department_ids_json = excluded.root_department_ids_json,
                  username_template = excluded.username_template,
                  disabled_users_ou = excluded.disabled_users_ou,
                  group_type = excluded.group_type,
                  group_mail_domain = excluded.group_mail_domain,
                  custom_group_ou_path = excluded.custom_group_ou_path,
                  managed_tag_ids_json = excluded.managed_tag_ids_json,
                  managed_external_chat_ids_json = excluded.managed_external_chat_ids_json,
                  is_enabled = excluded.is_enabled,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_connector,
                    normalized_org_id,
                    str(name or normalized_connector).strip() or normalized_connector,
                    normalized_config["config_path"],
                    normalized_config["ldap_server"],
                    normalized_config["ldap_domain"],
                    normalized_config["ldap_username"],
                    _encode_secret_field(
                        "ldap_password",
                        normalized_config["ldap_password"],
                        secret_fields=CONNECTOR_SECRET_FIELDS,
                    ),
                    1 if normalized_config["ldap_use_ssl"] else 0 if normalized_config["ldap_use_ssl"] is not None else None,
                    normalized_config["ldap_port"],
                    1 if normalized_config["ldap_validate_cert"] else 0 if normalized_config["ldap_validate_cert"] is not None else None,
                    normalized_config["ldap_ca_cert_path"],
                    _encode_secret_field(
                        "default_password",
                        normalized_config["default_password"],
                        secret_fields=CONNECTOR_SECRET_FIELDS,
                    ),
                    1 if normalized_config["force_change_password"] else 0 if normalized_config["force_change_password"] is not None else None,
                    normalized_config["password_complexity"],
                    dumps_json(
                        {
                            "values": [
                                int(value)
                                for value in root_department_ids
                                if str(value).strip()
                            ]
                        }
                    ),
                    str(username_template or "").strip(),
                    str(disabled_users_ou or "").strip(),
                    str(group_type or "security").strip() or "security",
                    str(group_mail_domain or "").strip(),
                    str(custom_group_ou_path or "").strip(),
                    dumps_json({"values": [str(value).strip() for value in managed_tag_ids if str(value).strip()]}),
                    dumps_json(
                        {
                            "values": [
                                str(value).strip()
                                for value in managed_external_chat_ids
                                if str(value).strip()
                            ]
                        }
                    ),
                    1 if is_enabled else 0,
                    now,
                    now,
                ),
            )
        created_record = self.get_connector_record(normalized_connector, org_id=normalized_org_id)
        if created_record and not _record_has_connector_overrides(created_record) and created_record.config_path and os.path.exists(created_record.config_path):
            self._import_legacy_connector_config(created_record)

    def set_enabled(self, connector_id: str, enabled: bool, *, org_id: Optional[str] = None) -> None:
        normalized_org_id = str(org_id or "").strip()
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE sync_connectors
                    SET is_enabled = ?,
                        updated_at = ?
                    WHERE connector_id = ?
                      AND org_id = ?
                    """,
                    (1 if enabled else 0, utcnow_iso(), str(connector_id or "").strip(), normalized_org_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE sync_connectors
                    SET is_enabled = ?,
                        updated_at = ?
                    WHERE connector_id = ?
                    """,
                    (1 if enabled else 0, utcnow_iso(), str(connector_id or "").strip()),
                )

    def delete_connector(self, connector_id: str, *, org_id: Optional[str] = None) -> None:
        normalized_org_id = str(org_id or "").strip()
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    "DELETE FROM sync_connectors WHERE connector_id = ? AND org_id = ?",
                    (str(connector_id or "").strip(), normalized_org_id),
                )
            else:
                conn.execute("DELETE FROM sync_connectors WHERE connector_id = ?", (str(connector_id or "").strip(),))

    def delete_connectors_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM sync_connectors WHERE org_id = ?", (str(org_id or "").strip() or "default",))


class OffboardingQueueRepository(BaseRepository):
    def get_record(
        self,
        *,
        connector_id: str,
        ad_username: str,
        org_id: Optional[str] = None,
    ) -> Optional[OffboardingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM offboarding_queue
                WHERE org_id = ?
                  AND connector_id = ?
                  AND LOWER(ad_username) = LOWER(?)
                LIMIT 1
                """,
                (normalized_org_id, str(connector_id or "default").strip() or "default", str(ad_username or "").strip()),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM offboarding_queue
                WHERE connector_id = ?
                  AND LOWER(ad_username) = LOWER(?)
                ORDER BY org_id ASC, id ASC
                LIMIT 1
                """,
                (str(connector_id or "default").strip() or "default", str(ad_username or "").strip()),
            )
        if not row:
            return None
        return OffboardingRecord.from_row(row)

    def list_due_records(self, *, due_at: str, org_id: Optional[str] = None) -> list[OffboardingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                """
                SELECT *
                FROM offboarding_queue
                WHERE org_id = ?
                  AND status = 'pending'
                  AND due_at <= ?
                ORDER BY due_at ASC, id ASC
                """,
                (normalized_org_id, due_at),
            )
        else:
            rows = self._fetchall(
                """
                SELECT *
                FROM offboarding_queue
                WHERE status = 'pending'
                  AND due_at <= ?
                ORDER BY org_id ASC, due_at ASC, id ASC
                """,
                (due_at,),
            )
        return [OffboardingRecord.from_row(row) for row in rows]

    def list_pending_records(self, *, org_id: Optional[str] = None) -> list[OffboardingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                """
                SELECT *
                FROM offboarding_queue
                WHERE org_id = ?
                  AND status = 'pending'
                ORDER BY due_at ASC, id ASC
                """,
                (normalized_org_id,),
            )
        else:
            rows = self._fetchall(
                """
                SELECT *
                FROM offboarding_queue
                WHERE status = 'pending'
                ORDER BY org_id ASC, due_at ASC, id ASC
                """
            )
        return [OffboardingRecord.from_row(row) for row in rows]

    def upsert_pending(
        self,
        *,
        connector_id: str,
        wecom_userid: str,
        ad_username: str,
        due_at: str,
        org_id: Optional[str] = None,
        reason: str = "",
        manager_userids: Iterable[str] = (),
        last_job_id: str = "",
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_connector = str(connector_id or "default").strip() or "default"
        normalized_username = str(ad_username or "").strip()
        normalized_source_user_id = str(wecom_userid or "").strip()
        if not normalized_username:
            raise ValueError("ad_username is required")
        manager_values = [str(value).strip() for value in manager_userids if str(value).strip()]
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO offboarding_queue (
                  org_id, connector_id, source_user_id, ad_username, status, reason, manager_userids_json,
                  first_missing_at, due_at, last_job_id, updated_at
                ) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, connector_id, ad_username) DO UPDATE SET
                  source_user_id = excluded.source_user_id,
                  status = 'pending',
                  reason = excluded.reason,
                  manager_userids_json = excluded.manager_userids_json,
                  due_at = excluded.due_at,
                  last_job_id = excluded.last_job_id,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_connector,
                    normalized_source_user_id,
                    normalized_username,
                    str(reason or "").strip(),
                    json.dumps(manager_values, ensure_ascii=False),
                    now,
                    due_at,
                    str(last_job_id or "").strip(),
                    now,
                ),
            )

    def upsert_pending_for_source_user(
        self,
        *,
        connector_id: str,
        source_user_id: str,
        ad_username: str,
        due_at: str,
        org_id: Optional[str] = None,
        reason: str = "",
        manager_userids: Iterable[str] = (),
        last_job_id: str = "",
    ) -> None:
        self.upsert_pending(
            connector_id=connector_id,
            wecom_userid=source_user_id,
            ad_username=ad_username,
            due_at=due_at,
            org_id=org_id,
            reason=reason,
            manager_userids=manager_userids,
            last_job_id=last_job_id,
        )

    def mark_notified(self, *, connector_id: str, ad_username: str, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE offboarding_queue
                    SET notified_at = ?,
                        updated_at = ?
                    WHERE org_id = ?
                      AND connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    """,
                    (
                        utcnow_iso(),
                        utcnow_iso(),
                        normalized_org_id,
                        str(connector_id or "default").strip() or "default",
                        str(ad_username or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE offboarding_queue
                    SET notified_at = ?,
                        updated_at = ?
                    WHERE connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    """,
                    (
                        utcnow_iso(),
                        utcnow_iso(),
                        str(connector_id or "default").strip() or "default",
                        str(ad_username or "").strip(),
                    ),
                )

    def mark_disabled(
        self,
        *,
        connector_id: str,
        ad_username: str,
        org_id: Optional[str] = None,
        last_job_id: str = "",
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE offboarding_queue
                    SET status = 'disabled',
                        last_job_id = ?,
                        updated_at = ?
                    WHERE org_id = ?
                      AND connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    """,
                    (
                        str(last_job_id or "").strip(),
                        utcnow_iso(),
                        normalized_org_id,
                        str(connector_id or "default").strip() or "default",
                        str(ad_username or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE offboarding_queue
                    SET status = 'disabled',
                        last_job_id = ?,
                        updated_at = ?
                    WHERE connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    """,
                    (
                        str(last_job_id or "").strip(),
                        utcnow_iso(),
                        str(connector_id or "default").strip() or "default",
                        str(ad_username or "").strip(),
                    ),
                )

    def clear_pending(self, *, connector_id: str, ad_username: str, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    DELETE FROM offboarding_queue
                    WHERE org_id = ?
                      AND connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    """,
                    (
                        normalized_org_id,
                        str(connector_id or "default").strip() or "default",
                        str(ad_username or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM offboarding_queue
                    WHERE connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    """,
                    (str(connector_id or "default").strip() or "default", str(ad_username or "").strip()),
                )

    def delete_records_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM offboarding_queue WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )


class UserLifecycleQueueRepository(BaseRepository):
    def get_record(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        wecom_userid: str,
        org_id: Optional[str] = None,
    ) -> Optional[UserLifecycleRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM user_lifecycle_queue
                WHERE org_id = ?
                  AND lifecycle_type = ?
                  AND connector_id = ?
                  AND source_user_id = ?
                LIMIT 1
                """,
                (
                    normalized_org_id,
                    str(lifecycle_type or "").strip(),
                    str(connector_id or "default").strip() or "default",
                    str(wecom_userid or "").strip(),
                ),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM user_lifecycle_queue
                WHERE lifecycle_type = ?
                  AND connector_id = ?
                  AND source_user_id = ?
                ORDER BY org_id ASC, id ASC
                LIMIT 1
                """,
                (
                    str(lifecycle_type or "").strip(),
                    str(connector_id or "default").strip() or "default",
                    str(wecom_userid or "").strip(),
                ),
            )
        if not row:
            return None
        return UserLifecycleRecord.from_row(row)

    def get_record_for_source_user(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        org_id: Optional[str] = None,
    ) -> Optional[UserLifecycleRecord]:
        return self.get_record(
            lifecycle_type=lifecycle_type,
            connector_id=connector_id,
            wecom_userid=source_user_id,
            org_id=org_id,
        )

    def list_pending_records(
        self,
        *,
        lifecycle_type: str | None = None,
        org_id: Optional[str] = None,
    ) -> list[UserLifecycleRecord]:
        clauses = ["status = 'pending'"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        normalized_lifecycle_type = str(lifecycle_type or "").strip()
        if normalized_lifecycle_type:
            clauses.append("lifecycle_type = ?")
            params.append(normalized_lifecycle_type)
        rows = self._fetchall(
            f"""
            SELECT *
            FROM user_lifecycle_queue
            WHERE {' AND '.join(clauses)}
            ORDER BY effective_at ASC, connector_id ASC, source_user_id ASC, id ASC
            """,
            tuple(params),
        )
        return [UserLifecycleRecord.from_row(row) for row in rows]

    def upsert_pending(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        wecom_userid: str,
        effective_at: str,
        org_id: Optional[str] = None,
        ad_username: str = "",
        reason: str = "",
        employment_type: str = "",
        sponsor_userid: str = "",
        manager_userids: Iterable[str] = (),
        payload: Optional[Dict[str, Any]] = None,
        last_job_id: str = "",
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_lifecycle_type = str(lifecycle_type or "").strip()
        normalized_connector_id = str(connector_id or "default").strip() or "default"
        normalized_source_user_id = str(wecom_userid or "").strip()
        if not normalized_lifecycle_type or not normalized_source_user_id:
            raise ValueError("lifecycle_type and source_user_id are required")
        manager_values = [str(value).strip() for value in manager_userids if str(value).strip()]
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO user_lifecycle_queue (
                  org_id, lifecycle_type, connector_id, source_user_id, ad_username, status, reason,
                  employment_type, sponsor_userid, manager_userids_json, effective_at, last_job_id,
                  payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, lifecycle_type, connector_id, source_user_id) DO UPDATE SET
                  ad_username = excluded.ad_username,
                  status = 'pending',
                  reason = excluded.reason,
                  employment_type = excluded.employment_type,
                  sponsor_userid = excluded.sponsor_userid,
                  manager_userids_json = excluded.manager_userids_json,
                  effective_at = excluded.effective_at,
                  last_job_id = excluded.last_job_id,
                  payload_json = excluded.payload_json,
                  completed_at = NULL,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_lifecycle_type,
                    normalized_connector_id,
                    normalized_source_user_id,
                    str(ad_username or "").strip(),
                    str(reason or "").strip(),
                    str(employment_type or "").strip(),
                    str(sponsor_userid or "").strip(),
                    json.dumps(manager_values, ensure_ascii=False),
                    str(effective_at or "").strip(),
                    str(last_job_id or "").strip(),
                    dumps_json(payload),
                    now,
                ),
            )

    def upsert_pending_for_source_user(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        effective_at: str,
        org_id: Optional[str] = None,
        ad_username: str = "",
        reason: str = "",
        employment_type: str = "",
        sponsor_userid: str = "",
        manager_userids: Iterable[str] = (),
        payload: Optional[Dict[str, Any]] = None,
        last_job_id: str = "",
    ) -> None:
        self.upsert_pending(
            lifecycle_type=lifecycle_type,
            connector_id=connector_id,
            wecom_userid=source_user_id,
            effective_at=effective_at,
            org_id=org_id,
            ad_username=ad_username,
            reason=reason,
            employment_type=employment_type,
            sponsor_userid=sponsor_userid,
            manager_userids=manager_userids,
            payload=payload,
            last_job_id=last_job_id,
        )

    def mark_notified(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        wecom_userid: str,
        org_id: Optional[str] = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        now = utcnow_iso()
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE user_lifecycle_queue
                    SET notified_at = ?,
                        updated_at = ?
                    WHERE org_id = ?
                      AND lifecycle_type = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    """,
                    (
                        now,
                        now,
                        normalized_org_id,
                        str(lifecycle_type or "").strip(),
                        str(connector_id or "default").strip() or "default",
                        str(wecom_userid or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE user_lifecycle_queue
                    SET notified_at = ?,
                        updated_at = ?
                    WHERE lifecycle_type = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    """,
                    (
                        now,
                        now,
                        str(lifecycle_type or "").strip(),
                        str(connector_id or "default").strip() or "default",
                        str(wecom_userid or "").strip(),
                    ),
                )

    def mark_notified_for_source_user(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        org_id: Optional[str] = None,
    ) -> None:
        self.mark_notified(
            lifecycle_type=lifecycle_type,
            connector_id=connector_id,
            wecom_userid=source_user_id,
            org_id=org_id,
        )

    def mark_completed(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        wecom_userid: str,
        org_id: Optional[str] = None,
        last_job_id: str = "",
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        now = utcnow_iso()
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE user_lifecycle_queue
                    SET status = 'completed',
                        completed_at = ?,
                        last_job_id = ?,
                        updated_at = ?
                    WHERE org_id = ?
                      AND lifecycle_type = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    """,
                    (
                        now,
                        str(last_job_id or "").strip(),
                        now,
                        normalized_org_id,
                        str(lifecycle_type or "").strip(),
                        str(connector_id or "default").strip() or "default",
                        str(wecom_userid or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE user_lifecycle_queue
                    SET status = 'completed',
                        completed_at = ?,
                        last_job_id = ?,
                        updated_at = ?
                    WHERE lifecycle_type = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    """,
                    (
                        now,
                        str(last_job_id or "").strip(),
                        now,
                        str(lifecycle_type or "").strip(),
                        str(connector_id or "default").strip() or "default",
                        str(wecom_userid or "").strip(),
                    ),
                )

    def mark_completed_for_source_user(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        org_id: Optional[str] = None,
        last_job_id: str = "",
    ) -> None:
        self.mark_completed(
            lifecycle_type=lifecycle_type,
            connector_id=connector_id,
            wecom_userid=source_user_id,
            org_id=org_id,
            last_job_id=last_job_id,
        )

    def clear_pending(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        wecom_userid: str,
        org_id: Optional[str] = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    DELETE FROM user_lifecycle_queue
                    WHERE org_id = ?
                      AND lifecycle_type = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    """,
                    (
                        normalized_org_id,
                        str(lifecycle_type or "").strip(),
                        str(connector_id or "default").strip() or "default",
                        str(wecom_userid or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM user_lifecycle_queue
                    WHERE lifecycle_type = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    """,
                    (
                        str(lifecycle_type or "").strip(),
                        str(connector_id or "default").strip() or "default",
                        str(wecom_userid or "").strip(),
                    ),
                )

    def clear_pending_for_source_user(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        org_id: Optional[str] = None,
    ) -> None:
        self.clear_pending(
            lifecycle_type=lifecycle_type,
            connector_id=connector_id,
            wecom_userid=source_user_id,
            org_id=org_id,
        )

    def delete_records_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM user_lifecycle_queue WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )


class CustomManagedGroupBindingRepository(BaseRepository):
    def get_binding_record(
        self,
        *,
        connector_id: str,
        source_type: str,
        source_key: str,
        org_id: Optional[str] = None,
    ) -> Optional[CustomManagedGroupBindingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM custom_managed_group_bindings
                WHERE org_id = ?
                  AND connector_id = ?
                  AND source_type = ?
                  AND source_key = ?
                LIMIT 1
                """,
                (
                    normalized_org_id,
                    str(connector_id or "default").strip() or "default",
                    str(source_type or "").strip(),
                    str(source_key or "").strip(),
                ),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM custom_managed_group_bindings
                WHERE connector_id = ?
                  AND source_type = ?
                  AND source_key = ?
                ORDER BY org_id ASC, id ASC
                LIMIT 1
                """,
                (
                    str(connector_id or "default").strip() or "default",
                    str(source_type or "").strip(),
                    str(source_key or "").strip(),
                ),
            )
        if not row:
            return None
        return CustomManagedGroupBindingRecord.from_row(row)

    def upsert_binding(
        self,
        *,
        connector_id: str,
        source_type: str,
        source_key: str,
        org_id: Optional[str] = None,
        group_sam: str,
        group_dn: str,
        group_cn: str,
        display_name: str,
        status: str = "active",
        last_seen_at: str = "",
        archived_at: str = "",
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO custom_managed_group_bindings (
                  org_id, connector_id, source_type, source_key, group_sam, group_dn, group_cn,
                  display_name, status, last_seen_at, archived_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, connector_id, source_type, source_key) DO UPDATE SET
                  group_sam = excluded.group_sam,
                  group_dn = excluded.group_dn,
                  group_cn = excluded.group_cn,
                  display_name = excluded.display_name,
                  status = excluded.status,
                  last_seen_at = excluded.last_seen_at,
                  archived_at = excluded.archived_at,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    str(connector_id or "default").strip() or "default",
                    str(source_type or "").strip(),
                    str(source_key or "").strip(),
                    str(group_sam or "").strip(),
                    str(group_dn or "").strip(),
                    str(group_cn or "").strip(),
                    str(display_name or "").strip(),
                    str(status or "active").strip() or "active",
                    str(last_seen_at or now).strip(),
                    str(archived_at or "").strip(),
                    now,
                ),
            )

    def set_status(
        self,
        *,
        connector_id: str,
        source_type: str,
        source_key: str,
        org_id: Optional[str] = None,
        status: str,
        archived_at: str = "",
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        now = utcnow_iso()
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE custom_managed_group_bindings
                    SET status = ?,
                        archived_at = ?,
                        updated_at = ?
                    WHERE org_id = ?
                      AND connector_id = ?
                      AND source_type = ?
                      AND source_key = ?
                    """,
                    (
                        str(status or "active").strip() or "active",
                        str(archived_at or "").strip(),
                        now,
                        normalized_org_id,
                        str(connector_id or "default").strip() or "default",
                        str(source_type or "").strip(),
                        str(source_key or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE custom_managed_group_bindings
                    SET status = ?,
                        archived_at = ?,
                        updated_at = ?
                    WHERE connector_id = ?
                      AND source_type = ?
                      AND source_key = ?
                    """,
                    (
                        str(status or "active").strip() or "active",
                        str(archived_at or "").strip(),
                        now,
                        str(connector_id or "default").strip() or "default",
                        str(source_type or "").strip(),
                        str(source_key or "").strip(),
                    ),
                )

    def list_active_records(
        self,
        *,
        connector_id: str | None = None,
        org_id: Optional[str] = None,
    ) -> list[CustomManagedGroupBindingRecord]:
        clauses = ["status = 'active'"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        normalized_connector = str(connector_id or "").strip()
        if normalized_connector:
            clauses.append("connector_id = ?")
            params.append(normalized_connector)
        rows = self._fetchall(
            f"""
            SELECT *
            FROM custom_managed_group_bindings
            WHERE {' AND '.join(clauses)}
            ORDER BY connector_id ASC, source_type ASC, source_key ASC
            """,
            tuple(params),
        )
        return [CustomManagedGroupBindingRecord.from_row(row) for row in rows]

    def list_records(
        self,
        *,
        connector_id: str | None = None,
        org_id: Optional[str] = None,
    ) -> list[CustomManagedGroupBindingRecord]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        normalized_connector = str(connector_id or "").strip()
        if normalized_connector:
            clauses.append("connector_id = ?")
            params.append(normalized_connector)
        rows = self._fetchall(
            f"""
            SELECT *
            FROM custom_managed_group_bindings
            WHERE {' AND '.join(clauses)}
            ORDER BY connector_id ASC, source_type ASC, source_key ASC
            """,
            tuple(params),
        )
        return [CustomManagedGroupBindingRecord.from_row(row) for row in rows]

    def delete_bindings_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM custom_managed_group_bindings WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )


class SyncReplayRequestRepository(BaseRepository):
    def enqueue_request(
        self,
        *,
        request_type: str,
        execution_mode: str,
        requested_by: str = "",
        target_scope: str = "full",
        target_id: str = "",
        trigger_reason: str = "",
        org_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> int:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sync_replay_requests (
                  org_id, request_type, execution_mode, status, requested_by, target_scope, target_id,
                  trigger_reason, payload_json, created_at
                ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_org_id,
                    str(request_type or "").strip(),
                    str(execution_mode or "").strip(),
                    str(requested_by or "").strip(),
                    str(target_scope or "full").strip() or "full",
                    str(target_id or "").strip(),
                    str(trigger_reason or "").strip(),
                    dumps_json(payload),
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def list_request_records(
        self,
        *,
        status: str | None = None,
        org_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[SyncReplayRequestRecord]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        normalized_status = str(status or "").strip().lower()
        if normalized_status:
            clauses.append("status = ?")
            params.append(normalized_status)
        rows = self._fetchall(
            f"""
            SELECT *
            FROM sync_replay_requests
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at ASC, id ASC
            LIMIT ?
            """,
            (*params, int(limit)),
        )
        return [SyncReplayRequestRecord.from_row(row) for row in rows]

    def get_request_record(self, request_id: int) -> Optional[SyncReplayRequestRecord]:
        row = self._fetchone(
            """
            SELECT *
            FROM sync_replay_requests
            WHERE id = ?
            LIMIT 1
            """,
            (int(request_id),),
        )
        if not row:
            return None
        return SyncReplayRequestRecord.from_row(row)

    def mark_started(self, request_id: int) -> None:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE sync_replay_requests
                SET status = 'running',
                    started_at = ?,
                    finished_at = NULL
                WHERE id = ?
                """,
                (now, int(request_id)),
            )

    def mark_finished(
        self,
        request_id: int,
        *,
        status: str,
        last_job_id: str = "",
        result_summary: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE sync_replay_requests
                SET status = ?,
                    finished_at = ?,
                    last_job_id = ?,
                    result_summary_json = ?
                WHERE id = ?
                """,
                (
                    str(status or "completed").strip() or "completed",
                    utcnow_iso(),
                    str(last_job_id or "").strip(),
                    dumps_json(result_summary),
                    int(request_id),
                ),
            )

    def delete_requests_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM sync_replay_requests WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )


class WebAdminUserRepository(BaseRepository):
    def has_any_user(self) -> bool:
        row = self._fetchone("SELECT COUNT(*) AS total FROM web_admin_users")
        return bool(row and int(row["total"]) > 0)

    def count_users(self) -> int:
        row = self._fetchone("SELECT COUNT(*) AS total FROM web_admin_users")
        return int(row["total"]) if row else 0

    def get_by_username(self, username: str):
        return self._fetchone(
            """
            SELECT * FROM web_admin_users
            WHERE LOWER(username) = LOWER(?)
            LIMIT 1
            """,
            (username,),
        )

    def get_user_record_by_username(self, username: str) -> Optional[WebAdminUserRecord]:
        row = self.get_by_username(username)
        if not row:
            return None
        return WebAdminUserRecord.from_row(row)

    def get_by_id(self, user_id: int):
        return self._fetchone(
            """
            SELECT * FROM web_admin_users
            WHERE id = ?
            LIMIT 1
            """,
            (int(user_id),),
        )

    def get_user_record_by_id(self, user_id: int) -> Optional[WebAdminUserRecord]:
        row = self.get_by_id(user_id)
        if not row:
            return None
        return WebAdminUserRecord.from_row(row)

    def list_user_records(self) -> list[WebAdminUserRecord]:
        rows = self._fetchall(
            """
            SELECT * FROM web_admin_users
            ORDER BY is_enabled DESC, created_at ASC, username ASC
            """
        )
        return [WebAdminUserRecord.from_row(row) for row in rows]

    def create_user(
        self,
        username: str,
        password_hash: str,
        *,
        role: str = "super_admin",
        is_enabled: bool = True,
        must_change_password: bool = False,
    ) -> int:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO web_admin_users (
                  username, password_hash, role, is_enabled, must_change_password,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    username.strip(),
                    password_hash,
                    role,
                    1 if is_enabled else 0,
                    1 if must_change_password else 0,
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def update_last_login(self, username: str) -> None:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE web_admin_users
                SET last_login_at = ?, updated_at = ?
                WHERE LOWER(username) = LOWER(?)
                """,
                (now, now, username),
            )

    def set_password(
        self,
        username: str,
        password_hash: str,
        *,
        must_change_password: bool = False,
    ) -> None:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE web_admin_users
                SET password_hash = ?,
                    must_change_password = ?,
                    updated_at = ?
                WHERE LOWER(username) = LOWER(?)
                """,
                (password_hash, 1 if must_change_password else 0, now, username),
            )

    def set_enabled(self, user_id: int, enabled: bool) -> None:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE web_admin_users
                SET is_enabled = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (1 if enabled else 0, now, int(user_id)),
            )


class WebAuditLogRepository(BaseRepository):
    def add_log(
        self,
        *,
        org_id: Optional[str] = None,
        actor_username: Optional[str],
        action_type: str,
        result: str,
        message: str,
        target_type: Optional[str] = None,
        target_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> int:
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO web_audit_logs (
                  org_id, actor_username, action_type, target_type, target_id,
                  result, message, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalize_org_id(org_id, fallback="") or "",
                    actor_username,
                    action_type,
                    target_type,
                    target_id,
                    result,
                    message,
                    dumps_json(payload),
                    utcnow_iso(),
                ),
            )
            return int(cursor.lastrowid)

    def list_recent_logs(self, limit: int = 100) -> list[WebAuditLogRecord]:
        rows, _total = self.list_recent_logs_page(limit=limit, offset=0)
        return rows

    def delete_logs_for_org(self, org_id: str) -> int:
        normalized_org_id = normalize_org_id(org_id, fallback="")
        if not normalized_org_id:
            return 0
        with self.db.transaction() as conn:
            return int(
                conn.execute(
                    "DELETE FROM web_audit_logs WHERE org_id = ?",
                    (normalized_org_id,),
                ).rowcount
            )

    def list_recent_logs_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        query: str = "",
        org_id: Optional[str] = None,
        include_global: bool = True,
    ) -> tuple[list[WebAuditLogRecord], int]:
        normalized_query = str(query or "").strip().lower()
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = normalize_org_id(org_id, fallback="")
        if normalized_org_id:
            if include_global:
                clauses.append("(org_id = ? OR org_id = '')")
                params.append(normalized_org_id)
            else:
                clauses.append("org_id = ?")
                params.append(normalized_org_id)
        elif not include_global:
            clauses.append("org_id = ''")
        if normalized_query:
            clauses.append(
                "("
                "LOWER(COALESCE(actor_username, '')) LIKE ? OR "
                "LOWER(action_type) LIKE ? OR "
                "LOWER(COALESCE(target_type, '')) LIKE ? OR "
                "LOWER(COALESCE(target_id, '')) LIKE ? OR "
                "LOWER(message) LIKE ?"
                ")"
            )
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 5)

        where_clause = " WHERE " + " AND ".join(clauses)
        total = self._fetchcount(
            f"""
            SELECT COUNT(*)
            FROM web_audit_logs
            {where_clause}
            """,
            tuple(params),
        )
        rows = self._fetchall(
            f"""
            SELECT *
            FROM web_audit_logs
            {where_clause}
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), max(int(offset), 0)),
        )
        return [WebAuditLogRecord.from_row(row) for row in rows], total


class SyncOperationLogRepository(BaseRepository):
    def add_record(
        self,
        *,
        job_id: str,
        stage_name: str,
        object_type: str,
        operation_type: str,
        status: str,
        message: str,
        source_id: Optional[str] = None,
        department_id: Optional[str] = None,
        target_id: Optional[str] = None,
        target_dn: Optional[str] = None,
        risk_level: str = "normal",
        rule_source: Optional[str] = None,
        reason_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> int:
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sync_operation_logs (
                  job_id, stage_name, object_type, operation_type, source_id,
                  department_id, target_id, target_dn, risk_level, status,
                  message, rule_source, reason_code, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    stage_name,
                    object_type,
                    operation_type,
                    source_id,
                    department_id,
                    target_id,
                    target_dn,
                    risk_level,
                    status,
                    message,
                    rule_source,
                    reason_code,
                    dumps_json(details),
                    utcnow_iso(),
                ),
            )
            return int(cursor.lastrowid)

    def list_records_for_job(self, job_id: str, limit: int = 500) -> list[SyncOperationRecord]:
        rows, _total = self.list_records_for_job_page(job_id, limit=limit, offset=0)
        return rows

    def list_records_for_job_page(
        self,
        job_id: str,
        *,
        limit: int = 500,
        offset: int = 0,
    ) -> tuple[list[SyncOperationRecord], int]:
        total = self._fetchcount(
            """
            SELECT COUNT(*)
            FROM sync_operation_logs
            WHERE job_id = ?
            """,
            (job_id,),
        )
        rows = self._fetchall(
            """
            SELECT *
            FROM sync_operation_logs
            WHERE job_id = ?
            ORDER BY created_at ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (job_id, int(limit), max(int(offset), 0)),
        )
        return [SyncOperationRecord.from_row(row) for row in rows], total


class SyncConflictRepository(BaseRepository):
    def add_conflict(
        self,
        *,
        job_id: str,
        conflict_type: str,
        source_id: str,
        message: str,
        severity: str = "warning",
        status: str = "open",
        target_key: Optional[str] = None,
        resolution_hint: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> int:
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sync_conflicts (
                  job_id, conflict_type, severity, status, source_id,
                  target_key, message, resolution_hint, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    conflict_type,
                    severity,
                    status,
                    source_id,
                    target_key,
                    message,
                    resolution_hint,
                    dumps_json(details),
                    utcnow_iso(),
                ),
            )
            return int(cursor.lastrowid)

    def list_conflicts_for_job(self, job_id: str, limit: int = 500) -> list[SyncConflictRecord]:
        rows, _total = self.list_conflicts_for_job_page(job_id, limit=limit, offset=0)
        return rows

    def list_conflicts_for_job_page(
        self,
        job_id: str,
        *,
        limit: int = 500,
        offset: int = 0,
    ) -> tuple[list[SyncConflictRecord], int]:
        total = self._fetchcount(
            """
            SELECT COUNT(*)
            FROM sync_conflicts
            WHERE job_id = ?
            """,
            (job_id,),
        )
        rows = self._fetchall(
            """
            SELECT *
            FROM sync_conflicts
            WHERE job_id = ?
            ORDER BY created_at ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (job_id, int(limit), max(int(offset), 0)),
        )
        return [SyncConflictRecord.from_row(row) for row in rows], total

    def list_conflict_records(
        self,
        *,
        limit: int = 500,
        job_id: Optional[str] = None,
        status: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> list[SyncConflictRecord]:
        rows, _total = self.list_conflict_records_page(
            limit=limit,
            offset=0,
            job_id=job_id,
            status=status,
            org_id=org_id,
        )
        return rows

    def list_conflict_records_page(
        self,
        *,
        limit: int = 500,
        offset: int = 0,
        job_id: Optional[str] = None,
        status: Optional[str] = None,
        query: str = "",
        org_id: Optional[str] = None,
    ) -> tuple[list[SyncConflictRecord], int]:
        normalized_org_id = normalize_org_id(org_id)
        if normalized_org_id:
            sql = """
                SELECT c.*
                FROM sync_conflicts AS c
                INNER JOIN sync_jobs AS j ON j.job_id = c.job_id
                WHERE j.org_id = ?
            """
            count_sql = """
                SELECT COUNT(*)
                FROM sync_conflicts AS c
                INNER JOIN sync_jobs AS j ON j.job_id = c.job_id
                WHERE j.org_id = ?
            """
            params: list[Any] = [normalized_org_id]
        else:
            sql = """
                SELECT *
                FROM sync_conflicts
                WHERE 1 = 1
            """
            count_sql = """
                SELECT COUNT(*)
                FROM sync_conflicts
                WHERE 1 = 1
            """
            params = []
        normalized_query = str(query or "").strip().lower()
        if job_id:
            conflict_job_column = "c.job_id" if normalized_org_id else "job_id"
            sql += f" AND {conflict_job_column} = ?"
            count_sql += f" AND {conflict_job_column} = ?"
            params.append(job_id)
        if status:
            conflict_status_column = "c.status" if normalized_org_id else "status"
            sql += f" AND {conflict_status_column} = ?"
            count_sql += f" AND {conflict_status_column} = ?"
            params.append(status)
        if normalized_query:
            conflict_type_column = "c.conflict_type" if normalized_org_id else "conflict_type"
            source_id_column = "c.source_id" if normalized_org_id else "source_id"
            target_key_column = "c.target_key" if normalized_org_id else "target_key"
            message_column = "c.message" if normalized_org_id else "message"
            query_clause = (
                " AND ("
                f"LOWER({conflict_type_column}) LIKE ? OR "
                f"LOWER({source_id_column}) LIKE ? OR "
                f"LOWER(COALESCE({target_key_column}, '')) LIKE ? OR "
                f"LOWER({message_column}) LIKE ?"
                ")"
            )
            sql += query_clause
            count_sql += query_clause
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 4)
        total = self._fetchcount(count_sql, tuple(params))
        if normalized_org_id:
            sql += " ORDER BY c.created_at DESC, c.id DESC LIMIT ? OFFSET ?"
        else:
            sql += " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
        params.extend([int(limit), max(int(offset), 0)])
        rows = self._fetchall(sql, tuple(params))
        return [SyncConflictRecord.from_row(row) for row in rows], total

    def get_conflict_record(self, conflict_id: int, *, org_id: Optional[str] = None) -> Optional[SyncConflictRecord]:
        normalized_org_id = normalize_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT c.*
                FROM sync_conflicts AS c
                INNER JOIN sync_jobs AS j ON j.job_id = c.job_id
                WHERE c.id = ?
                  AND j.org_id = ?
                LIMIT 1
                """,
                (int(conflict_id), normalized_org_id),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM sync_conflicts
                WHERE id = ?
                LIMIT 1
                """,
                (int(conflict_id),),
            )
        if not row:
            return None
        return SyncConflictRecord.from_row(row)

    def count_open_conflicts_for_job(self, job_id: str) -> int:
        row = self._fetchone(
            """
            SELECT COUNT(*) AS total
            FROM sync_conflicts
            WHERE job_id = ?
              AND status = 'open'
            """,
            (job_id,),
        )
        return int(row["total"]) if row else 0

    def update_conflict_status(
        self,
        conflict_id: int,
        *,
        status: str,
        resolution_payload: Optional[Dict[str, Any]] = None,
        resolved_at: Optional[str] = None,
    ) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE sync_conflicts
                SET status = ?,
                    resolution_payload_json = ?,
                    resolved_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    dumps_json(resolution_payload),
                    resolved_at,
                    int(conflict_id),
                ),
            )

    def resolve_open_conflicts_for_source(
        self,
        *,
        job_id: str,
        source_id: str,
        resolution_payload: Optional[Dict[str, Any]] = None,
        resolved_at: Optional[str] = None,
    ) -> int:
        with self.db.transaction() as conn:
            rows = conn.execute(
                """
                SELECT id
                FROM sync_conflicts
                WHERE job_id = ?
                  AND source_id = ?
                  AND status = 'open'
                ORDER BY id ASC
                """,
                (job_id, source_id),
            ).fetchall()
            for row in rows:
                payload = dict(resolution_payload or {})
                payload["resolved_conflict_id"] = int(row["id"])
                conn.execute(
                    """
                    UPDATE sync_conflicts
                    SET status = 'resolved',
                        resolution_payload_json = ?,
                        resolved_at = ?
                    WHERE id = ?
                    """,
                    (
                        dumps_json(payload),
                        resolved_at,
                        int(row["id"]),
                    ),
                )
            return len(rows)


class SyncPlanReviewRepository(BaseRepository):
    def upsert_review_request(
        self,
        *,
        job_id: str,
        plan_fingerprint: str,
        config_snapshot_hash: str,
        high_risk_operation_count: int,
    ) -> None:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_plan_reviews (
                  job_id, plan_fingerprint, config_snapshot_hash, high_risk_operation_count,
                  status, created_at
                ) VALUES (?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(job_id) DO UPDATE SET
                  plan_fingerprint = excluded.plan_fingerprint,
                  config_snapshot_hash = excluded.config_snapshot_hash,
                  high_risk_operation_count = excluded.high_risk_operation_count,
                  status = 'pending',
                  reviewer_username = NULL,
                  review_notes = NULL,
                  reviewed_at = NULL,
                  expires_at = NULL
                """,
                (
                    job_id,
                    plan_fingerprint,
                    config_snapshot_hash,
                    int(high_risk_operation_count),
                    now,
                ),
            )

    def get_review_record_by_job_id(self, job_id: str) -> Optional[SyncPlanReviewRecord]:
        row = self._fetchone(
            """
            SELECT *
            FROM sync_plan_reviews
            WHERE job_id = ?
            LIMIT 1
            """,
            (job_id,),
        )
        if not row:
            return None
        return SyncPlanReviewRecord.from_row(row)

    def approve_review(
        self,
        job_id: str,
        *,
        reviewer_username: str,
        review_notes: str = "",
        expires_at: Optional[str] = None,
    ) -> None:
        reviewed_at = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE sync_plan_reviews
                SET status = 'approved',
                    reviewer_username = ?,
                    review_notes = ?,
                    reviewed_at = ?,
                    expires_at = ?
                WHERE job_id = ?
                """,
                (
                    reviewer_username,
                    review_notes,
                    reviewed_at,
                    expires_at,
                    job_id,
                ),
            )

    def find_matching_approved_review(
        self,
        *,
        plan_fingerprint: str,
        config_snapshot_hash: str,
        now_iso: str,
    ) -> Optional[SyncPlanReviewRecord]:
        row = self._fetchone(
            """
            SELECT *
            FROM sync_plan_reviews
            WHERE plan_fingerprint = ?
              AND config_snapshot_hash = ?
              AND status = 'approved'
              AND (expires_at IS NULL OR expires_at >= ?)
            ORDER BY reviewed_at DESC, id DESC
            LIMIT 1
            """,
            (
                plan_fingerprint,
                config_snapshot_hash,
                now_iso,
            ),
        )
        if not row:
            return None
        return SyncPlanReviewRecord.from_row(row)


class SyncExceptionRuleRepository(BaseRepository):
    @staticmethod
    def _active_rule_clause() -> str:
        return "(expires_at IS NULL OR expires_at = '' OR expires_at >= ?)"

    def _normalize_rule_inputs(
        self,
        *,
        rule_type: str,
        match_value: str,
        match_type: Optional[str] = None,
    ) -> tuple[str, str, str]:
        normalized_rule_type = normalize_exception_rule_type(rule_type)
        if not normalized_rule_type:
            raise ValueError("unsupported exception rule_type")

        expected_match_type = get_exception_rule_match_type(normalized_rule_type)
        normalized_match_type = str(match_type or expected_match_type).strip().lower()
        if normalized_match_type != expected_match_type:
            raise ValueError("exception match_type does not match rule_type")

        normalized_match_value = normalize_exception_match_value(normalized_match_type, match_value)
        if not normalized_match_value:
            raise ValueError("exception match_value is required")

        return normalized_rule_type, normalized_match_type, normalized_match_value

    def get_rule_record(self, rule_id: int, *, org_id: Optional[str] = None) -> Optional[SyncExceptionRuleRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM sync_exception_rules
                WHERE id = ?
                  AND org_id = ?
                LIMIT 1
                """,
                (int(rule_id), normalized_org_id),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM sync_exception_rules
                WHERE id = ?
                LIMIT 1
                """,
                (int(rule_id),),
            )
        if not row:
            return None
        return SyncExceptionRuleRecord.from_row(row)

    def list_rule_records(self, *, org_id: Optional[str] = None) -> list[SyncExceptionRuleRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                """
                SELECT *
                FROM sync_exception_rules
                WHERE org_id = ?
                ORDER BY is_enabled DESC, rule_type ASC, match_value ASC, id ASC
                """,
                (normalized_org_id,),
            )
        else:
            rows = self._fetchall(
                """
                SELECT *
                FROM sync_exception_rules
                ORDER BY org_id ASC, is_enabled DESC, rule_type ASC, match_value ASC, id ASC
                """
            )
        return [SyncExceptionRuleRecord.from_row(row) for row in rows]

    def list_rule_records_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        query: str = "",
        rule_type: str = "",
        status: str = "all",
        org_id: Optional[str] = None,
    ) -> tuple[list[SyncExceptionRuleRecord], int]:
        normalized_query = str(query or "").strip().lower()
        normalized_status = str(status or "all").strip().lower()
        normalized_rule_type = normalize_exception_rule_type(rule_type)
        now_iso = utcnow_iso()
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if normalized_status == "enabled":
            clauses.append("is_enabled = 1")
            clauses.append(self._active_rule_clause())
            params.append(now_iso)
        elif normalized_status == "disabled":
            clauses.append("(is_enabled = 0 OR NOT " + self._active_rule_clause() + ")")
            params.append(now_iso)
        if normalized_rule_type:
            clauses.append("rule_type = ?")
            params.append(normalized_rule_type)
        if normalized_query:
            clauses.append(
                "("
                "LOWER(rule_type) LIKE ? OR "
                "LOWER(match_type) LIKE ? OR "
                "LOWER(match_value) LIKE ? OR "
                "LOWER(COALESCE(notes, '')) LIKE ?"
                ")"
            )
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 4)
        where_clause = " WHERE " + " AND ".join(clauses)
        total = self._fetchcount(
            f"""
            SELECT COUNT(*)
            FROM sync_exception_rules
            {where_clause}
            """,
            tuple(params),
        )
        rows = self._fetchall(
            f"""
            SELECT *
            FROM sync_exception_rules
            {where_clause}
            ORDER BY is_enabled DESC, rule_type ASC, match_value ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), max(int(offset), 0)),
        )
        return [SyncExceptionRuleRecord.from_row(row) for row in rows], total

    def list_enabled_rule_records(self, *, org_id: Optional[str] = None) -> list[SyncExceptionRuleRecord]:
        now_iso = utcnow_iso()
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                f"""
                SELECT *
                FROM sync_exception_rules
                WHERE org_id = ?
                  AND is_enabled = 1
                  AND {self._active_rule_clause()}
                ORDER BY rule_type ASC, match_value ASC, id ASC
                """,
                (normalized_org_id, now_iso),
            )
        else:
            rows = self._fetchall(
                f"""
                SELECT *
                FROM sync_exception_rules
                WHERE is_enabled = 1
                  AND {self._active_rule_clause()}
                ORDER BY org_id ASC, rule_type ASC, match_value ASC, id ASC
                """,
                (now_iso,),
            )
        return [SyncExceptionRuleRecord.from_row(row) for row in rows]

    def upsert_rule(
        self,
        *,
        rule_type: str,
        match_value: str,
        notes: str = "",
        is_enabled: bool = True,
        match_type: Optional[str] = None,
        expires_at: str = "",
        is_once: bool = False,
        org_id: Optional[str] = None,
    ) -> None:
        normalized_rule_type, normalized_match_type, normalized_match_value = self._normalize_rule_inputs(
            rule_type=rule_type,
            match_type=match_type,
            match_value=match_value,
        )
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_exception_rules (
                  org_id, rule_type, match_type, match_value, notes, is_enabled, expires_at, is_once, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, rule_type, match_type, match_value) DO UPDATE SET
                  notes = excluded.notes,
                  is_enabled = excluded.is_enabled,
                  expires_at = excluded.expires_at,
                  is_once = excluded.is_once,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_rule_type,
                    normalized_match_type,
                    normalized_match_value,
                    str(notes or "").strip(),
                    1 if is_enabled else 0,
                    str(expires_at or "").strip(),
                    1 if is_once else 0,
                    now,
                    now,
                ),
            )

    def consume_rule(
        self,
        *,
        rule_type: str,
        match_value: str,
        match_type: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> None:
        normalized_rule_type, normalized_match_type, normalized_match_value = self._normalize_rule_inputs(
            rule_type=rule_type,
            match_type=match_type,
            match_value=match_value,
        )
        normalized_org_id = self._resolve_org_id(org_id)
        now = utcnow_iso()
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE sync_exception_rules
                    SET last_matched_at = ?,
                        is_enabled = CASE WHEN is_once = 1 THEN 0 ELSE is_enabled END,
                        updated_at = ?
                    WHERE org_id = ?
                      AND rule_type = ?
                      AND match_type = ?
                      AND match_value = ?
                      AND is_enabled = 1
                    """,
                    (
                        now,
                        now,
                        normalized_org_id,
                        normalized_rule_type,
                        normalized_match_type,
                        normalized_match_value,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE sync_exception_rules
                    SET last_matched_at = ?,
                        is_enabled = CASE WHEN is_once = 1 THEN 0 ELSE is_enabled END,
                        updated_at = ?
                    WHERE rule_type = ?
                      AND match_type = ?
                      AND match_value = ?
                      AND is_enabled = 1
                    """,
                    (
                        now,
                        now,
                        normalized_rule_type,
                        normalized_match_type,
                        normalized_match_value,
                    ),
                )

    def set_enabled(self, rule_id: int, enabled: bool, *, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE sync_exception_rules
                    SET is_enabled = ?,
                        updated_at = ?
                    WHERE id = ?
                      AND org_id = ?
                    """,
                    (1 if enabled else 0, utcnow_iso(), int(rule_id), normalized_org_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE sync_exception_rules
                    SET is_enabled = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (1 if enabled else 0, utcnow_iso(), int(rule_id)),
                )

    def delete_rule(self, rule_id: int, *, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    DELETE FROM sync_exception_rules
                    WHERE id = ?
                      AND org_id = ?
                    """,
                    (int(rule_id), normalized_org_id),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM sync_exception_rules
                    WHERE id = ?
                    """,
                    (int(rule_id),),
                )

    def delete_rules_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM sync_exception_rules WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )
