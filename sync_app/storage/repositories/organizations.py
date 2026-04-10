from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from sync_app.core.models import AppConfig, OrganizationRecord
from sync_app.storage.config_codec import (
    ORGANIZATION_CONFIG_VALUE_TYPES,
    build_app_config_from_org_values as _build_app_config_from_org_values,
    build_editable_org_config as _build_editable_org_config,
    load_org_config_values_from_file as _load_org_config_values_from_file,
    normalize_org_config_values as _normalize_org_config_values,
)
from sync_app.storage.local_db import BaseRepository, _decode_secret_field, _encode_secret_field, utcnow_iso
from sync_app.storage.secret_store import ORGANIZATION_SECRET_FIELDS


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
        from sync_app.storage.local_db import GroupExclusionRuleRepository

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
        existing = _normalize_org_config_values(
            stored_values,
            existing=stored_values,
            config_path=config_path or "config.ini",
        )
        normalized_values = _normalize_org_config_values(
            values,
            existing=existing,
            config_path=config_path or "config.ini",
        )
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
