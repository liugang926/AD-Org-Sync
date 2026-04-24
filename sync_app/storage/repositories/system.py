from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sync_app.core.models import (
    ConfigReleaseSnapshotRecord,
    DataQualitySnapshotRecord,
    IntegrationWebhookOutboxRecord,
    IntegrationWebhookSubscriptionRecord,
    SyncReplayRequestRecord,
    WebAuditLogRecord,
)
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


class ConfigReleaseSnapshotRepository(BaseRepository):
    def add_snapshot(
        self,
        *,
        org_id: Optional[str] = None,
        snapshot_name: str = "",
        trigger_action: str = "manual_release",
        created_by: str = "",
        source_snapshot_id: Optional[int] = None,
        bundle_hash: str = "",
        bundle: Dict[str, Any],
        summary: Optional[Dict[str, Any]] = None,
        created_at: Optional[str] = None,
    ) -> int:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        timestamp = str(created_at or utcnow_iso()).strip()
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO config_release_snapshots (
                  org_id, snapshot_name, trigger_action, created_by, source_snapshot_id,
                  bundle_hash, bundle_json, summary_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_org_id,
                    str(snapshot_name or "").strip(),
                    str(trigger_action or "manual_release").strip() or "manual_release",
                    str(created_by or "").strip(),
                    int(source_snapshot_id) if source_snapshot_id is not None else None,
                    str(bundle_hash or "").strip(),
                    dumps_json(bundle),
                    dumps_json(summary),
                    timestamp,
                ),
            )
            return int(cursor.lastrowid)

    def get_snapshot_record(
        self,
        snapshot_id: int,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[ConfigReleaseSnapshotRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM config_release_snapshots
                WHERE id = ?
                  AND org_id = ?
                LIMIT 1
                """,
                (int(snapshot_id), normalized_org_id),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM config_release_snapshots
                WHERE id = ?
                LIMIT 1
                """,
                (int(snapshot_id),),
            )
        if not row:
            return None
        return ConfigReleaseSnapshotRecord.from_row(row)

    def get_latest_snapshot_record(self, *, org_id: Optional[str] = None) -> Optional[ConfigReleaseSnapshotRecord]:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        row = self._fetchone(
            """
            SELECT *
            FROM config_release_snapshots
            WHERE org_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (normalized_org_id,),
        )
        if not row:
            return None
        return ConfigReleaseSnapshotRecord.from_row(row)

    def list_snapshot_records(
        self,
        *,
        org_id: Optional[str] = None,
        limit: int = 20,
    ) -> list[ConfigReleaseSnapshotRecord]:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        rows = self._fetchall(
            """
            SELECT *
            FROM config_release_snapshots
            WHERE org_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (normalized_org_id, int(limit)),
        )
        return [ConfigReleaseSnapshotRecord.from_row(row) for row in rows]


class DataQualitySnapshotRepository(BaseRepository):
    def add_snapshot(
        self,
        *,
        org_id: Optional[str] = None,
        trigger_action: str = "manual_scan",
        created_by: str = "",
        summary: Optional[Dict[str, Any]] = None,
        snapshot: Dict[str, Any],
        created_at: Optional[str] = None,
    ) -> int:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        timestamp = str(created_at or utcnow_iso()).strip()
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO data_quality_snapshots (
                  org_id, trigger_action, created_by, summary_json, snapshot_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_org_id,
                    str(trigger_action or "manual_scan").strip() or "manual_scan",
                    str(created_by or "").strip(),
                    dumps_json(summary),
                    dumps_json(snapshot),
                    timestamp,
                ),
            )
            return int(cursor.lastrowid)

    def get_snapshot_record(
        self,
        snapshot_id: int,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[DataQualitySnapshotRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM data_quality_snapshots
                WHERE id = ?
                  AND org_id = ?
                LIMIT 1
                """,
                (int(snapshot_id), normalized_org_id),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM data_quality_snapshots
                WHERE id = ?
                LIMIT 1
                """,
                (int(snapshot_id),),
            )
        if not row:
            return None
        return DataQualitySnapshotRecord.from_row(row)

    def get_latest_snapshot_record(self, *, org_id: Optional[str] = None) -> Optional[DataQualitySnapshotRecord]:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        row = self._fetchone(
            """
            SELECT *
            FROM data_quality_snapshots
            WHERE org_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (normalized_org_id,),
        )
        if not row:
            return None
        return DataQualitySnapshotRecord.from_row(row)

    def list_snapshot_records(
        self,
        *,
        org_id: Optional[str] = None,
        limit: int = 20,
    ) -> list[DataQualitySnapshotRecord]:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        rows = self._fetchall(
            """
            SELECT *
            FROM data_quality_snapshots
            WHERE org_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (normalized_org_id, int(limit)),
        )
        return [DataQualitySnapshotRecord.from_row(row) for row in rows]


class IntegrationWebhookSubscriptionRepository(BaseRepository):
    def upsert_subscription(
        self,
        *,
        org_id: Optional[str] = None,
        event_type: str,
        target_url: str,
        secret: str = "",
        description: str = "",
        is_enabled: bool = True,
    ) -> IntegrationWebhookSubscriptionRecord:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        normalized_event_type = str(event_type or "").strip().lower()
        normalized_target_url = str(target_url or "").strip()
        if not normalized_event_type:
            raise ValueError("event_type is required")
        if not normalized_target_url:
            raise ValueError("target_url is required")
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO integration_webhook_subscriptions (
                  org_id, event_type, target_url, secret, description,
                  is_enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, event_type, target_url) DO UPDATE SET
                  secret = excluded.secret,
                  description = excluded.description,
                  is_enabled = excluded.is_enabled,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_event_type,
                    normalized_target_url,
                    str(secret or "").strip(),
                    str(description or "").strip(),
                    1 if is_enabled else 0,
                    now,
                    now,
                ),
            )
        record = self.get_subscription_record_by_event_url(
            org_id=normalized_org_id,
            event_type=normalized_event_type,
            target_url=normalized_target_url,
        )
        if record is None:
            raise ValueError("subscription could not be loaded after save")
        return record

    def get_subscription_record(
        self,
        subscription_id: int,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[IntegrationWebhookSubscriptionRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM integration_webhook_subscriptions
                WHERE id = ?
                  AND org_id = ?
                LIMIT 1
                """,
                (int(subscription_id), normalized_org_id),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM integration_webhook_subscriptions
                WHERE id = ?
                LIMIT 1
                """,
                (int(subscription_id),),
            )
        if not row:
            return None
        return IntegrationWebhookSubscriptionRecord.from_row(row)

    def get_subscription_record_by_event_url(
        self,
        *,
        org_id: Optional[str] = None,
        event_type: str,
        target_url: str,
    ) -> Optional[IntegrationWebhookSubscriptionRecord]:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        row = self._fetchone(
            """
            SELECT *
            FROM integration_webhook_subscriptions
            WHERE org_id = ?
              AND event_type = ?
              AND target_url = ?
            LIMIT 1
            """,
            (
                normalized_org_id,
                str(event_type or "").strip().lower(),
                str(target_url or "").strip(),
            ),
        )
        if not row:
            return None
        return IntegrationWebhookSubscriptionRecord.from_row(row)

    def list_subscription_records(
        self,
        *,
        org_id: Optional[str] = None,
        event_type: Optional[str] = None,
        enabled_only: bool = False,
        limit: int = 100,
    ) -> list[IntegrationWebhookSubscriptionRecord]:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        clauses = ["org_id = ?"]
        params: list[Any] = [normalized_org_id]
        normalized_event_type = str(event_type or "").strip().lower()
        if normalized_event_type:
            clauses.append("event_type = ?")
            params.append(normalized_event_type)
        if enabled_only:
            clauses.append("is_enabled = 1")
        rows = self._fetchall(
            f"""
            SELECT *
            FROM integration_webhook_subscriptions
            WHERE {' AND '.join(clauses)}
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (*params, int(limit)),
        )
        return [IntegrationWebhookSubscriptionRecord.from_row(row) for row in rows]

    def delete_subscription(
        self,
        subscription_id: int,
        *,
        org_id: Optional[str] = None,
    ) -> int:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        with self.db.transaction() as conn:
            return int(
                conn.execute(
                    """
                    DELETE FROM integration_webhook_subscriptions
                    WHERE id = ?
                      AND org_id = ?
                    """,
                    (int(subscription_id), normalized_org_id),
                ).rowcount
            )

    def record_delivery_result(
        self,
        subscription_id: int,
        *,
        last_status: str,
        last_error: str = "",
        attempted_at: Optional[str] = None,
    ) -> None:
        timestamp = str(attempted_at or utcnow_iso()).strip()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE integration_webhook_subscriptions
                SET last_attempt_at = ?,
                    last_status = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    timestamp,
                    str(last_status or "").strip(),
                    str(last_error or "").strip(),
                    timestamp,
                    int(subscription_id),
                ),
            )


class IntegrationWebhookOutboxRepository(BaseRepository):
    def enqueue_delivery(
        self,
        *,
        org_id: Optional[str] = None,
        subscription_id: Optional[int] = None,
        event_type: str,
        delivery_id: str,
        target_url: str,
        secret: str = "",
        payload: Optional[Dict[str, Any]] = None,
        max_attempts: int = 5,
        next_attempt_at: Optional[str] = None,
    ) -> IntegrationWebhookOutboxRecord:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        now = utcnow_iso()
        normalized_payload = payload if isinstance(payload, dict) else {"raw": payload}
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO integration_webhook_outbox (
                  org_id, subscription_id, event_type, delivery_id, target_url, secret,
                  payload_json, status, attempt_count, max_attempts, next_attempt_at,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?)
                """,
                (
                    normalized_org_id,
                    int(subscription_id) if subscription_id is not None else None,
                    str(event_type or "").strip().lower(),
                    str(delivery_id or "").strip(),
                    str(target_url or "").strip(),
                    str(secret or "").strip(),
                    dumps_json(normalized_payload) or "{}",
                    max(int(max_attempts or 0), 1),
                    str(next_attempt_at or now).strip() or now,
                    now,
                    now,
                ),
            )
            delivery_id_value = int(cursor.lastrowid)
        record = self.get_delivery_record(delivery_id_value)
        if record is None:
            raise ValueError("webhook outbox delivery could not be loaded after insert")
        return record

    def get_delivery_record(self, delivery_id: int) -> Optional[IntegrationWebhookOutboxRecord]:
        row = self._fetchone(
            """
            SELECT *
            FROM integration_webhook_outbox
            WHERE id = ?
            LIMIT 1
            """,
            (int(delivery_id),),
        )
        if not row:
            return None
        return IntegrationWebhookOutboxRecord.from_row(row)

    def list_delivery_records(
        self,
        *,
        org_id: Optional[str] = None,
        statuses: Optional[list[str]] = None,
        limit: int = 100,
    ) -> list[IntegrationWebhookOutboxRecord]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        normalized_statuses = [
            str(item or "").strip().lower()
            for item in list(statuses or [])
            if str(item or "").strip()
        ]
        if normalized_statuses:
            placeholders = ", ".join("?" for _ in normalized_statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)
        rows = self._fetchall(
            f"""
            SELECT *
            FROM integration_webhook_outbox
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (*params, int(limit)),
        )
        return [IntegrationWebhookOutboxRecord.from_row(row) for row in rows]

    def count_delivery_records(
        self,
        *,
        org_id: Optional[str] = None,
        statuses: Optional[list[str]] = None,
    ) -> int:
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        normalized_statuses = [
            str(item or "").strip().lower()
            for item in list(statuses or [])
            if str(item or "").strip()
        ]
        if normalized_statuses:
            placeholders = ", ".join("?" for _ in normalized_statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)
        row = self._fetchone(
            f"""
            SELECT COUNT(1) AS count
            FROM integration_webhook_outbox
            WHERE {' AND '.join(clauses)}
            """,
            tuple(params),
        )
        return int(row["count"] or 0) if row else 0

    def claim_delivery_records(
        self,
        *,
        org_id: Optional[str] = None,
        limit: int = 20,
        lease_seconds: int = 60,
        now_iso: Optional[str] = None,
    ) -> list[IntegrationWebhookOutboxRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        now_text = str(now_iso or utcnow_iso()).strip()
        try:
            now_dt = datetime.fromisoformat(now_text.replace("Z", "+00:00"))
        except ValueError:
            now_dt = datetime.now(timezone.utc)
            now_text = now_dt.isoformat(timespec="seconds")
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        lease_expires_at = (now_dt.astimezone(timezone.utc) + timedelta(seconds=max(int(lease_seconds or 0), 1))).isoformat(
            timespec="seconds"
        )

        with self.db.transaction() as conn:
            clauses = [
                "attempt_count < max_attempts",
                """(
                    (status IN ('pending', 'retrying') AND (next_attempt_at = '' OR next_attempt_at <= ?))
                    OR (status = 'dispatching' AND lease_expires_at <> '' AND lease_expires_at <= ?)
                )""",
            ]
            params: list[Any] = [now_text, now_text]
            if normalized_org_id:
                clauses.append("org_id = ?")
                params.append(normalized_org_id)
            rows = conn.execute(
                f"""
                SELECT id
                FROM integration_webhook_outbox
                WHERE {' AND '.join(clauses)}
                ORDER BY created_at ASC, id ASC
                LIMIT ?
                """,
                (*params, int(limit)),
            ).fetchall()
            claimed_ids = [int(row["id"]) for row in rows if row["id"] is not None]
            if not claimed_ids:
                return []
            placeholders = ", ".join("?" for _ in claimed_ids)
            conn.execute(
                f"""
                UPDATE integration_webhook_outbox
                SET status = 'dispatching',
                    locked_at = ?,
                    lease_expires_at = ?,
                    updated_at = ?
                WHERE id IN ({placeholders})
                """,
                (now_text, lease_expires_at, now_text, *claimed_ids),
            )

        claimed_records = [self.get_delivery_record(record_id) for record_id in claimed_ids]
        return [record for record in claimed_records if record is not None]

    def mark_delivery_success(
        self,
        delivery_id: int,
        *,
        last_status: str,
        attempted_at: Optional[str] = None,
    ) -> None:
        timestamp = str(attempted_at or utcnow_iso()).strip()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE integration_webhook_outbox
                SET status = 'delivered',
                    attempt_count = attempt_count + 1,
                    next_attempt_at = '',
                    last_attempt_at = ?,
                    last_status = ?,
                    last_error = '',
                    locked_at = '',
                    lease_expires_at = '',
                    updated_at = ?
                WHERE id = ?
                """,
                (timestamp, str(last_status or "").strip(), timestamp, int(delivery_id)),
            )

    def mark_delivery_retry(
        self,
        delivery_id: int,
        *,
        last_status: str,
        last_error: str = "",
        attempted_at: Optional[str] = None,
        retry_delay_seconds: int = 60,
    ) -> None:
        record = self.get_delivery_record(int(delivery_id))
        if record is None:
            return
        timestamp = str(attempted_at or utcnow_iso()).strip()
        next_attempt_at = (
            datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(timezone.utc)
            + timedelta(seconds=max(int(retry_delay_seconds or 0), 1))
        ).isoformat(timespec="seconds")
        next_attempt_count = int(record.attempt_count or 0) + 1
        next_status = "failed" if next_attempt_count >= max(int(record.max_attempts or 0), 1) else "retrying"
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE integration_webhook_outbox
                SET status = ?,
                    attempt_count = ?,
                    next_attempt_at = ?,
                    last_attempt_at = ?,
                    last_status = ?,
                    last_error = ?,
                    locked_at = '',
                    lease_expires_at = '',
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    next_status,
                    next_attempt_count,
                    "" if next_status == "failed" else next_attempt_at,
                    timestamp,
                    str(last_status or "").strip(),
                    str(last_error or "").strip(),
                    timestamp,
                    int(delivery_id),
                ),
            )

    def requeue_delivery(
        self,
        delivery_id: int,
        *,
        org_id: Optional[str] = None,
        failed_only: bool = False,
        next_attempt_at: Optional[str] = None,
    ) -> Optional[IntegrationWebhookOutboxRecord]:
        record = self.get_delivery_record(int(delivery_id))
        if record is None:
            return None
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id and (record.org_id or "default") != normalized_org_id:
            return None
        if failed_only and str(record.status or "").strip().lower() != "failed":
            return None

        timestamp = str(next_attempt_at or utcnow_iso()).strip()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE integration_webhook_outbox
                SET status = 'pending',
                    max_attempts = CASE
                        WHEN attempt_count >= max_attempts THEN attempt_count + 1
                        ELSE max_attempts
                    END,
                    next_attempt_at = ?,
                    locked_at = '',
                    lease_expires_at = '',
                    updated_at = ?
                WHERE id = ?
                """,
                (timestamp, timestamp, int(delivery_id)),
            )
        return self.get_delivery_record(int(delivery_id))
