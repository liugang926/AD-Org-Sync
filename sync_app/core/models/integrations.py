from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from sync_app.core.models.base import MappingLikeModel


@dataclass(slots=True)
class IntegrationWebhookSubscriptionRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = ""
    event_type: str = ""
    target_url: str = ""
    secret: str = ""
    description: str = ""
    is_enabled: bool = True
    last_attempt_at: str = ""
    last_status: str = ""
    last_error: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "IntegrationWebhookSubscriptionRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or ""),
            event_type=str(row["event_type"] or ""),
            target_url=str(row["target_url"] or ""),
            secret=str(row["secret"] or ""),
            description=str(row["description"] or ""),
            is_enabled=bool(row["is_enabled"]),
            last_attempt_at=str(row["last_attempt_at"] or ""),
            last_status=str(row["last_status"] or ""),
            last_error=str(row["last_error"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

@dataclass(slots=True)
class IntegrationWebhookOutboxRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = ""
    subscription_id: Optional[int] = None
    event_type: str = ""
    delivery_id: str = ""
    target_url: str = ""
    secret: str = ""
    payload: Optional[Dict[str, Any]] = None
    status: str = "pending"
    attempt_count: int = 0
    max_attempts: int = 5
    next_attempt_at: str = ""
    last_attempt_at: str = ""
    last_status: str = ""
    last_error: str = ""
    locked_at: str = ""
    lease_expires_at: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "IntegrationWebhookOutboxRecord":
        payload = row["payload_json"] if "payload_json" in row.keys() else None
        if isinstance(payload, str) and payload:
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {"raw": payload}
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or ""),
            subscription_id=int(row["subscription_id"]) if row["subscription_id"] is not None else None,
            event_type=str(row["event_type"] or ""),
            delivery_id=str(row["delivery_id"] or ""),
            target_url=str(row["target_url"] or ""),
            secret=str(row["secret"] or ""),
            payload=payload if isinstance(payload, dict) or payload is None else {"raw": payload},
            status=str(row["status"] or "pending"),
            attempt_count=int(row["attempt_count"] or 0),
            max_attempts=int(row["max_attempts"] or 0),
            next_attempt_at=str(row["next_attempt_at"] or ""),
            last_attempt_at=str(row["last_attempt_at"] or ""),
            last_status=str(row["last_status"] or ""),
            last_error=str(row["last_error"] or ""),
            locked_at=str(row["locked_at"] or ""),
            lease_expires_at=str(row["lease_expires_at"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

@dataclass(slots=True)
class SyncConnectorRecord(MappingLikeModel):
    connector_id: str = ""
    org_id: str = "default"
    name: str = ""
    config_path: str = ""
    ldap_server: str = ""
    ldap_domain: str = ""
    ldap_username: str = ""
    ldap_password: str = ""
    ldap_use_ssl: Optional[bool] = None
    ldap_port: Optional[int] = None
    ldap_validate_cert: Optional[bool] = None
    ldap_ca_cert_path: str = ""
    default_password: str = ""
    force_change_password: Optional[bool] = None
    password_complexity: str = ""
    root_department_ids: list[int] = field(default_factory=list)
    username_strategy: str = "custom_template"
    username_collision_policy: str = "append_employee_id"
    username_collision_template: str = ""
    username_template: str = ""
    disabled_users_ou: str = ""
    group_type: str = "security"
    group_mail_domain: str = ""
    custom_group_ou_path: str = ""
    managed_tag_ids: list[str] = field(default_factory=list)
    managed_external_chat_ids: list[str] = field(default_factory=list)
    is_enabled: bool = True
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "SyncConnectorRecord":
        root_department_ids = row["root_department_ids_json"] if "root_department_ids_json" in row.keys() else None
        managed_tag_ids = row["managed_tag_ids_json"] if "managed_tag_ids_json" in row.keys() else None
        managed_external_chat_ids = (
            row["managed_external_chat_ids_json"]
            if "managed_external_chat_ids_json" in row.keys()
            else None
        )
        try:
            root_department_ids = json.loads(root_department_ids) if root_department_ids else []
        except json.JSONDecodeError:
            root_department_ids = []
        if isinstance(root_department_ids, dict):
            root_department_ids = root_department_ids.get("values") or []
        try:
            managed_tag_ids = json.loads(managed_tag_ids) if managed_tag_ids else []
        except json.JSONDecodeError:
            managed_tag_ids = []
        if isinstance(managed_tag_ids, dict):
            managed_tag_ids = managed_tag_ids.get("values") or []
        try:
            managed_external_chat_ids = json.loads(managed_external_chat_ids) if managed_external_chat_ids else []
        except json.JSONDecodeError:
            managed_external_chat_ids = []
        if isinstance(managed_external_chat_ids, dict):
            managed_external_chat_ids = managed_external_chat_ids.get("values") or []
        return cls(
            connector_id=str(row["connector_id"] or ""),
            org_id=str(row["org_id"] or "default"),
            name=str(row["name"] or ""),
            config_path=str(row["config_path"] or ""),
            ldap_server=str(row["ldap_server"] or "") if "ldap_server" in row.keys() else "",
            ldap_domain=str(row["ldap_domain"] or "") if "ldap_domain" in row.keys() else "",
            ldap_username=str(row["ldap_username"] or "") if "ldap_username" in row.keys() else "",
            ldap_password=str(row["ldap_password"] or "") if "ldap_password" in row.keys() else "",
            ldap_use_ssl=bool(row["ldap_use_ssl"]) if "ldap_use_ssl" in row.keys() and row["ldap_use_ssl"] is not None else None,
            ldap_port=int(row["ldap_port"]) if "ldap_port" in row.keys() and row["ldap_port"] is not None else None,
            ldap_validate_cert=bool(row["ldap_validate_cert"]) if "ldap_validate_cert" in row.keys() and row["ldap_validate_cert"] is not None else None,
            ldap_ca_cert_path=str(row["ldap_ca_cert_path"] or "") if "ldap_ca_cert_path" in row.keys() else "",
            default_password=str(row["default_password"] or "") if "default_password" in row.keys() else "",
            force_change_password=bool(row["force_change_password"]) if "force_change_password" in row.keys() and row["force_change_password"] is not None else None,
            password_complexity=str(row["password_complexity"] or "") if "password_complexity" in row.keys() else "",
            root_department_ids=[int(value) for value in root_department_ids if str(value).strip()],
            username_strategy=str(row["username_strategy"] or "custom_template")
            if "username_strategy" in row.keys()
            else "custom_template",
            username_collision_policy=str(row["username_collision_policy"] or "append_employee_id")
            if "username_collision_policy" in row.keys()
            else "append_employee_id",
            username_collision_template=str(row["username_collision_template"] or "")
            if "username_collision_template" in row.keys()
            else "",
            username_template=str(row["username_template"] or ""),
            disabled_users_ou=str(row["disabled_users_ou"] or ""),
            group_type=str(row["group_type"] or "security"),
            group_mail_domain=str(row["group_mail_domain"] or ""),
            custom_group_ou_path=str(row["custom_group_ou_path"] or ""),
            managed_tag_ids=[str(value) for value in managed_tag_ids if str(value).strip()],
            managed_external_chat_ids=[
                str(value) for value in managed_external_chat_ids if str(value).strip()
            ],
            is_enabled=bool(row["is_enabled"]),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )
