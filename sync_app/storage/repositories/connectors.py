from __future__ import annotations

import os
from typing import Any, Iterable, Optional

from sync_app.core.models import AppConfig, SyncConnectorRecord
from sync_app.storage.local_db import (
    BaseRepository,
    _decode_secret_field,
    _encode_secret_field,
    _load_connector_config_values_from_file,
    _normalize_connector_config_values,
    _record_has_connector_overrides,
    _build_app_config_from_connector_record,
    dumps_json,
    utcnow_iso,
)
from sync_app.storage.secret_store import CONNECTOR_SECRET_FIELDS


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
        username_strategy: str = "custom_template",
        username_collision_policy: str = "append_employee_id",
        username_collision_template: str = "",
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
                  password_complexity, root_department_ids_json, username_strategy, username_collision_policy, username_collision_template, username_template,
                  disabled_users_ou, group_type, group_mail_domain, custom_group_ou_path,
                  managed_tag_ids_json, managed_external_chat_ids_json, is_enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                  username_strategy = excluded.username_strategy,
                  username_collision_policy = excluded.username_collision_policy,
                  username_collision_template = excluded.username_collision_template,
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
                    str(username_strategy or "custom_template").strip() or "custom_template",
                    str(username_collision_policy or "append_employee_id").strip() or "append_employee_id",
                    str(username_collision_template or "").strip(),
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
