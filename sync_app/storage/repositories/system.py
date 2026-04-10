from __future__ import annotations

import secrets
from typing import Any, Dict, Optional

from sync_app.core.models import SyncReplayRequestRecord, WebAuditLogRecord
from sync_app.storage.local_db import BaseRepository, dumps_json, normalize_org_id, utcnow_iso
from sync_app.storage.schema import DEFAULT_APP_SETTINGS, ORG_SCOPED_APP_SETTINGS


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

    def list_org_scoped_values(self, org_id: str) -> dict[str, str]:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        prefix = f"org:{normalized_org_id}:"
        rows = self._fetchall(
            """
            SELECT key, value
            FROM app_settings
            WHERE key LIKE ?
            ORDER BY key ASC
            """,
            (f"{prefix}%",),
        )
        return {
            str(row["key"])[len(prefix) :]: str(row["value"] or "")
            for row in rows
        }

    def delete_org_scoped_values(self, org_id: str) -> int:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        with self.db.transaction() as conn:
            return int(
                conn.execute(
                    "DELETE FROM app_settings WHERE key LIKE ?",
                    (f"org:{normalized_org_id}:%",),
                ).rowcount
            )

    def all_values(self) -> dict[str, str]:
        rows = self._fetchall("SELECT key, value FROM app_settings ORDER BY key")
        return {str(row["key"]): str(row["value"] or "") for row in rows}


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
