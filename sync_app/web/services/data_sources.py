from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sync_app.providers.source import (
    get_source_provider_display_name,
    list_source_provider_schemas,
    normalize_source_provider,
)
from sync_app.storage.config_codec import to_bool_value
from sync_app.storage.local_db import (
    ADDirectorySnapshotRepository,
    OrganizationConfigRepository,
    SettingsRepository,
    SourceConnectorRepository,
    SourceDirectoryRepository,
    SyncConnectorRepository,
    WebAuditLogRepository,
)


@dataclass(slots=True)
class WebDataSourceService:
    org_config_repo: OrganizationConfigRepository
    connector_repo: SyncConnectorRepository
    audit_repo: WebAuditLogRepository
    settings_repo: SettingsRepository
    source_connector_repo: SourceConnectorRepository
    source_directory_repo: SourceDirectoryRepository
    ad_directory_snapshot_repo: ADDirectorySnapshotRepository

    def build_connector_inventory(
        self,
        *,
        org_id: str,
        config_path: str,
    ) -> dict[str, Any]:
        editable = self.org_config_repo.get_editable_config(
            org_id,
            config_path=config_path,
        )
        source_ready = all(
            (
                str(editable.get("corpid") or "").strip(),
                bool(editable.get("corpsecret_configured")),
            )
        )
        base_target_ready = all(
            (
                str(editable.get("ldap_server") or "").strip(),
                str(editable.get("ldap_domain") or "").strip(),
                str(editable.get("ldap_username") or "").strip(),
                bool(editable.get("ldap_password_configured")),
            )
        )
        connectors = self.connector_repo.list_connector_records(org_id=org_id)
        source_records = self.source_connector_repo.list_connectors(org_id=org_id)
        records_by_provider: dict[str, list[Any]] = {}
        for record in source_records:
            records_by_provider.setdefault(record.provider_id, []).append(record)
        selected_provider = str(editable.get("source_provider") or "").strip().lower()
        source_platform_cards = []
        for schema in list_source_provider_schemas(include_unimplemented=False):
            provider_records = records_by_provider.get(schema.provider_id, [])
            primary_record = provider_records[0] if provider_records else None
            snapshot = self.source_directory_repo.get_latest_successful_snapshot(
                org_id=org_id,
                provider_id=schema.provider_id,
            )
            legacy_configured = bool(
                schema.provider_id == selected_provider and source_ready
            )
            required_permissions = set(
                primary_record.required_permissions if primary_record else []
            )
            granted_permissions = set(
                primary_record.granted_permissions if primary_record else []
            )
            permissions_complete = bool(
                primary_record
                and required_permissions
                and required_permissions.issubset(granted_permissions)
            )
            quality_issue_count = (
                int(snapshot["missing_employee_id_count"] or 0)
                + int(snapshot["duplicate_employee_id_count"] or 0)
                if snapshot
                else int(primary_record.quality_issue_count if primary_record else 0)
            )
            source_platform_cards.append(
                {
                    "provider_id": schema.provider_id,
                    "display_name": schema.display_name,
                    "description": schema.description,
                    "configured": bool(primary_record or legacy_configured),
                    "enabled": (
                        bool(primary_record.is_enabled)
                        if primary_record
                        else legacy_configured
                    ),
                    "connection_status": (
                        primary_record.connection_status
                        if primary_record
                        else "not_tested"
                    ),
                    "credentials_expires_at": (
                        primary_record.credentials_expires_at if primary_record else ""
                    ),
                    "permissions_complete": permissions_complete,
                    "permissions_known": bool(primary_record and required_permissions),
                    "authorization_scope": (
                        primary_record.authorization_scope if primary_record else {}
                    ),
                    "last_sync_at": (
                        str(snapshot["completed_at"] or "")
                        if snapshot
                        else primary_record.last_sync_at if primary_record else ""
                    ),
                    "department_count": (
                        int(snapshot["department_count"] or 0)
                        if snapshot
                        else int(
                            primary_record.department_count if primary_record else 0
                        )
                    ),
                    "account_count": (
                        int(snapshot["user_count"] or 0)
                        if snapshot
                        else int(primary_record.account_count if primary_record else 0)
                    ),
                    "quality_issue_count": quality_issue_count,
                    "connector_count": len(provider_records)
                    or (1 if legacy_configured else 0),
                }
            )
        latest_ad_snapshot = (
            self.ad_directory_snapshot_repo.get_latest_successful_snapshot(
                org_id=org_id,
                connector_id="default",
            )
        )
        capability_report: dict[str, Any] = {}
        if latest_ad_snapshot:
            try:
                capability_report = json.loads(
                    latest_ad_snapshot["capability_report_json"] or "{}"
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                capability_report = {}
        return {
            "editable": editable,
            "source_provider_name": get_source_provider_display_name(
                editable.get("source_provider")
            ),
            "source_ready": source_ready,
            "base_target_ready": base_target_ready,
            "target_connectors": connectors,
            "enabled_target_connector_count": sum(
                1 for connector in connectors if connector.is_enabled
            ),
            "source_platform_cards": source_platform_cards,
            "ad_directory_mode": self.settings_repo.get_value(
                "ad_directory_mode", "writable", org_id=org_id
            )
            or "writable",
            "ad_user_search_base_dn": self.settings_repo.get_value(
                "ad_user_search_base_dn", "", org_id=org_id
            )
            or "",
            "ad_ou_search_base_dn": self.settings_repo.get_value(
                "ad_ou_search_base_dn", "", org_id=org_id
            )
            or "",
            "latest_ad_snapshot": (
                dict(latest_ad_snapshot) if latest_ad_snapshot else None
            ),
            "ad_capability_report": capability_report,
        }

    def save_base_connections(
        self,
        *,
        org_id: str,
        config_path: str,
        actor_username: str,
        source_provider: str,
        corpid: str,
        agentid: str,
        corpsecret: str,
        ldap_server: str,
        ldap_domain: str,
        ldap_username: str,
        ldap_password: str,
        ldap_use_ssl: Any,
        ldap_port: Any,
        ldap_validate_cert: Any,
        ldap_ca_cert_path: str,
        ad_directory_mode: str,
        ad_user_search_base_dn: str,
        ad_ou_search_base_dn: str,
    ) -> dict[str, Any]:
        current = self.org_config_repo.get_raw_config(
            org_id,
            config_path=config_path,
        )
        previous_provider = normalize_source_provider(current.get("source_provider"))
        next_provider = normalize_source_provider(source_provider)
        updated = dict(current)
        if previous_provider != next_provider:
            for field_name in ("corpid", "agentid", "corpsecret", "webhook_url"):
                updated[field_name] = ""
        updated.update(
            {
                "source_provider": next_provider,
                "corpid": str(corpid or "").strip(),
                "agentid": str(agentid or "").strip(),
                "ldap_server": str(ldap_server or "").strip(),
                "ldap_domain": str(ldap_domain or "").strip(),
                "ldap_username": str(ldap_username or "").strip(),
                "ldap_use_ssl": to_bool_value(ldap_use_ssl, True),
                "ldap_port": min(max(int(ldap_port or 636), 1), 65535),
                "ldap_validate_cert": to_bool_value(ldap_validate_cert, True),
                "ldap_ca_cert_path": str(ldap_ca_cert_path or "").strip(),
            }
        )
        if str(corpsecret or "").strip():
            updated["corpsecret"] = str(corpsecret).strip()
        if str(ldap_password or "").strip():
            updated["ldap_password"] = str(ldap_password).strip()
        saved = self.org_config_repo.save_config(
            org_id,
            updated,
            config_path=config_path,
        )
        self.settings_repo.set_value(
            "ad_directory_mode",
            ad_directory_mode,
            "string",
            org_id=org_id,
        )
        self.settings_repo.set_value(
            "ad_user_search_base_dn",
            str(ad_user_search_base_dn or "").strip(),
            "string",
            org_id=org_id,
        )
        self.settings_repo.set_value(
            "ad_ou_search_base_dn",
            str(ad_ou_search_base_dn or "").strip(),
            "string",
            org_id=org_id,
        )
        self.source_connector_repo.upsert_connector(
            org_id=org_id,
            connector_id=f"{next_provider}-default",
            provider_id=next_provider,
            name=get_source_provider_display_name(next_provider),
            corpid=corpid,
            agentid=agentid,
            corpsecret=corpsecret,
            is_enabled=True,
        )
        self.audit_repo.add_log(
            org_id=org_id,
            actor_username=actor_username,
            action_type="data_source.base_connections_update",
            target_type="organization_connectors",
            target_id=org_id,
            result="success",
            message="Updated source and base target connector connection settings",
            payload={
                "org_id": org_id,
                "source_provider": next_provider,
                "source_provider_changed": previous_provider != next_provider,
                "source_secret_updated": bool(str(corpsecret or "").strip()),
                "ldap_server": saved.get("ldap_server"),
                "ldap_domain": saved.get("ldap_domain"),
                "ldap_use_ssl": bool(saved.get("ldap_use_ssl")),
                "ldap_port": int(saved.get("ldap_port") or 0),
                "ldap_validate_cert": bool(saved.get("ldap_validate_cert")),
                "ldap_password_updated": bool(str(ldap_password or "").strip()),
            },
        )
        self.audit_repo.add_log(
            org_id=org_id,
            actor_username=actor_username,
            action_type="data_source.ad_directory_mode.update",
            target_type="organization_connectors",
            target_id=org_id,
            result="success",
            message="Updated AD directory access mode",
            payload={
                "directory_mode": ad_directory_mode,
                "user_search_base_configured": bool(
                    str(ad_user_search_base_dn or "").strip()
                ),
                "ou_search_base_configured": bool(
                    str(ad_ou_search_base_dn or "").strip()
                ),
            },
        )
        return saved

    def save_target_connection(
        self,
        *,
        org_id: str,
        actor_username: str,
        connector_id: str,
        name: str,
        config_path: str,
        ldap_server: str,
        ldap_domain: str,
        ldap_username: str,
        ldap_password: str,
        ldap_use_ssl: Any,
        ldap_port: Any,
        ldap_validate_cert: Any,
        ldap_ca_cert_path: str,
        is_enabled: Any,
    ) -> None:
        normalized_id = str(connector_id or "").strip()
        any_org_record = self.connector_repo.get_connector_record(normalized_id)
        if any_org_record and any_org_record.org_id != org_id:
            raise ValueError("Connector ID is already used by another organization")
        existing = self.connector_repo.get_connector_record(
            normalized_id,
            org_id=org_id,
        )
        self.connector_repo.upsert_connector(
            connector_id=normalized_id,
            org_id=org_id,
            name=str(name or normalized_id).strip() or normalized_id,
            config_path=str(config_path or (existing.config_path if existing else "")).strip(),
            ldap_server=str(ldap_server or "").strip(),
            ldap_domain=str(ldap_domain or "").strip(),
            ldap_username=str(ldap_username or "").strip(),
            ldap_password=str(ldap_password or "").strip(),
            ldap_use_ssl=ldap_use_ssl,
            ldap_port=ldap_port,
            ldap_validate_cert=ldap_validate_cert,
            ldap_ca_cert_path=str(ldap_ca_cert_path or "").strip(),
            default_password=existing.default_password if existing else "",
            force_change_password=existing.force_change_password if existing else None,
            password_complexity=existing.password_complexity if existing else "",
            root_department_ids=existing.root_department_ids if existing else (),
            username_strategy=existing.username_strategy if existing else "custom_template",
            username_collision_policy=(
                existing.username_collision_policy if existing else "append_employee_id"
            ),
            username_collision_template=(
                existing.username_collision_template if existing else ""
            ),
            username_template=existing.username_template if existing else "",
            disabled_users_ou=existing.disabled_users_ou if existing else "",
            group_type=existing.group_type if existing else "security",
            group_mail_domain=existing.group_mail_domain if existing else "",
            custom_group_ou_path=existing.custom_group_ou_path if existing else "",
            managed_tag_ids=existing.managed_tag_ids if existing else (),
            managed_external_chat_ids=(
                existing.managed_external_chat_ids if existing else ()
            ),
            is_enabled=to_bool_value(is_enabled, True),
        )
        self.audit_repo.add_log(
            org_id=org_id,
            actor_username=actor_username,
            action_type="data_source.target_connector_upsert",
            target_type="connector",
            target_id=normalized_id,
            result="success",
            message="Saved target connector connection settings",
            payload={
                "org_id": org_id,
                "connector_id": normalized_id,
                "ldap_server": str(ldap_server or "").strip(),
                "ldap_domain": str(ldap_domain or "").strip(),
                "ldap_use_ssl": to_bool_value(ldap_use_ssl, True),
                "ldap_port": int(ldap_port or 636),
                "ldap_validate_cert": to_bool_value(ldap_validate_cert, True),
                "ldap_password_updated": bool(str(ldap_password or "").strip()),
                "policy_fields_preserved": bool(existing),
            },
        )


__all__ = ["WebDataSourceService"]
