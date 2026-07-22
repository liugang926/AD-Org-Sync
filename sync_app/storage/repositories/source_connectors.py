from __future__ import annotations

from typing import Any, Iterable, Optional

from sync_app.core.models import SourceConnectorRecord
from sync_app.core.observability import redact_sensitive_text
from sync_app.storage.local_db import BaseRepository, dumps_json, utcnow_iso
from sync_app.storage.secret_store import protect_secret, unprotect_secret


REQUIRED_SOURCE_PERMISSIONS = {
    "wecom": ["contacts.departments.read", "contacts.users.read"],
    "dingtalk": ["contact.departments.read", "contact.users.read"],
    "feishu": ["contact:department.base:readonly", "contact:user.base:readonly"],
}


class SourceConnectorRepository(BaseRepository):
    def upsert_connector(
        self,
        *,
        org_id: str,
        connector_id: str,
        provider_id: str,
        name: str,
        corpid: str,
        agentid: str = "",
        corpsecret: str = "",
        is_enabled: bool = True,
        credentials_expires_at: str = "",
        granted_permissions: Iterable[str] = (),
        required_permissions: Optional[Iterable[str]] = None,
        authorization_scope: Optional[dict[str, Any]] = None,
    ) -> SourceConnectorRecord:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_connector_id = str(connector_id or "").strip()
        normalized_provider_id = str(provider_id or "").strip().lower()
        if not normalized_connector_id or not normalized_provider_id:
            raise ValueError("source connector ID and provider are required")
        existing = self._fetchone(
            "SELECT corpsecret FROM source_connectors WHERE org_id = ? AND connector_id = ?",
            (normalized_org_id, normalized_connector_id),
        )
        encrypted_secret = (
            protect_secret(corpsecret)
            if str(corpsecret or "")
            else str(existing["corpsecret"] or "") if existing else ""
        )
        now = utcnow_iso()
        resolved_required_permissions = list(
            required_permissions
            if required_permissions is not None
            else REQUIRED_SOURCE_PERMISSIONS.get(normalized_provider_id, [])
        )
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO source_connectors (
                  org_id, connector_id, provider_id, name, corpid, agentid,
                  corpsecret, is_enabled, credentials_expires_at,
                  granted_permissions_json, required_permissions_json,
                  authorization_scope_json, connection_status, last_tested_at,
                  last_sync_at, department_count, account_count,
                  quality_issue_count, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'not_tested', '', '', 0, 0, 0, '', ?, ?)
                ON CONFLICT(org_id, connector_id)
                DO UPDATE SET
                  provider_id = excluded.provider_id,
                  name = excluded.name,
                  corpid = excluded.corpid,
                  agentid = excluded.agentid,
                  corpsecret = excluded.corpsecret,
                  is_enabled = excluded.is_enabled,
                  credentials_expires_at = excluded.credentials_expires_at,
                  granted_permissions_json = excluded.granted_permissions_json,
                  required_permissions_json = excluded.required_permissions_json,
                  authorization_scope_json = excluded.authorization_scope_json,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_connector_id,
                    normalized_provider_id,
                    str(name or normalized_provider_id).strip(),
                    str(corpid or "").strip(),
                    str(agentid or "").strip(),
                    encrypted_secret,
                    int(bool(is_enabled)),
                    str(credentials_expires_at or "").strip(),
                    dumps_json(sorted({str(value) for value in granted_permissions if str(value).strip()})) or "[]",
                    dumps_json(sorted({str(value) for value in resolved_required_permissions if str(value).strip()})) or "[]",
                    dumps_json(authorization_scope or {}) or "{}",
                    now,
                    now,
                ),
            )
        record = self.get_connector(
            normalized_connector_id,
            org_id=normalized_org_id,
            reveal_secret=False,
        )
        if record is None:
            raise RuntimeError("source connector was not persisted")
        return record

    def get_connector(
        self,
        connector_id: str,
        *,
        org_id: str,
        reveal_secret: bool = False,
    ) -> Optional[SourceConnectorRecord]:
        row = self._fetchone(
            "SELECT * FROM source_connectors WHERE org_id = ? AND connector_id = ?",
            (self._resolve_org_id(org_id) or "default", connector_id),
        )
        if not row:
            return None
        record = SourceConnectorRecord.from_row(row)
        record.corpsecret = unprotect_secret(record.corpsecret) if reveal_secret else ""
        return record

    def list_connectors(
        self, *, org_id: str, include_disabled: bool = True
    ) -> list[SourceConnectorRecord]:
        clauses = ["org_id = ?"]
        params = [self._resolve_org_id(org_id) or "default"]
        if not include_disabled:
            clauses.append("is_enabled = 1")
        rows = self._fetchall(
            f"SELECT * FROM source_connectors WHERE {' AND '.join(clauses)} ORDER BY provider_id, connector_id",
            params,
        )
        records = [SourceConnectorRecord.from_row(row) for row in rows]
        for record in records:
            record.corpsecret = ""
        return records

    def update_connection_status(
        self,
        *,
        org_id: str,
        connector_id: str,
        connection_status: str,
        granted_permissions: Iterable[str] = (),
        authorization_scope: Optional[dict[str, Any]] = None,
        error_summary: str = "",
    ) -> None:
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE source_connectors
                SET connection_status = ?, granted_permissions_json = ?,
                    authorization_scope_json = ?, last_tested_at = ?,
                    last_error = ?, updated_at = ?
                WHERE org_id = ? AND connector_id = ?
                """,
                (
                    str(connection_status or "failed").strip(),
                    dumps_json(sorted({str(value) for value in granted_permissions if str(value).strip()})) or "[]",
                    dumps_json(authorization_scope or {}) or "{}",
                    utcnow_iso(),
                    redact_sensitive_text(str(error_summary or ""))[:500],
                    utcnow_iso(),
                    self._resolve_org_id(org_id) or "default",
                    connector_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("source connector does not exist in this organization")

    def update_snapshot_stats(
        self,
        *,
        org_id: str,
        connector_id: str,
        department_count: int,
        account_count: int,
        quality_issue_count: int,
        synced_at: str = "",
    ) -> None:
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE source_connectors
                SET last_sync_at = ?, department_count = ?, account_count = ?,
                    quality_issue_count = ?, updated_at = ?
                WHERE org_id = ? AND connector_id = ?
                """,
                (
                    synced_at or utcnow_iso(),
                    int(department_count),
                    int(account_count),
                    int(quality_issue_count),
                    utcnow_iso(),
                    self._resolve_org_id(org_id) or "default",
                    connector_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("source connector does not exist in this organization")


__all__ = ["REQUIRED_SOURCE_PERMISSIONS", "SourceConnectorRepository"]
