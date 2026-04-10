from __future__ import annotations

from typing import Any

from sync_app.core.config import load_sync_config
from sync_app.core.models import AttributeMappingRuleRecord, DepartmentNode
from sync_app.core.sync_policies import normalize_group_type, normalize_mapping_direction
from sync_app.storage.local_db import SyncConnectorRepository


def load_connector_specs(
    config,
    connector_repo: SyncConnectorRepository,
    *,
    connectors_enabled: bool = False,
    org_id: str = "default",
    load_sync_config_fn=load_sync_config,
) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = [
        {
            "connector_id": "default",
            "org_id": org_id,
            "name": "Default Connector",
            "config_path": config.config_path,
            "root_department_ids": [],
            "username_template": "",
            "disabled_users_ou": "Disabled Users",
            "group_type": "security",
            "group_mail_domain": "",
            "custom_group_ou_path": "Managed Groups",
            "managed_tag_ids": [],
            "managed_external_chat_ids": [],
            "config": config,
        }
    ]
    if not connectors_enabled:
        return specs
    for record in connector_repo.list_connector_records(enabled_only=True, org_id=org_id):
        connector_config = connector_repo.get_connector_app_config(
            record.connector_id,
            base_config=config,
            org_id=org_id,
        )
        if connector_config is None:
            connector_config = load_sync_config_fn(record.config_path)
        specs.append(
            {
                "connector_id": record.connector_id,
                "org_id": record.org_id,
                "name": record.name,
                "config_path": record.config_path,
                "root_department_ids": list(record.root_department_ids),
                "username_template": record.username_template,
                "disabled_users_ou": record.disabled_users_ou or "Disabled Users",
                "group_type": normalize_group_type(record.group_type),
                "group_mail_domain": record.group_mail_domain,
                "custom_group_ou_path": record.custom_group_ou_path,
                "managed_tag_ids": list(record.managed_tag_ids),
                "managed_external_chat_ids": list(record.managed_external_chat_ids),
                "config": connector_config,
            }
        )
    return specs


def build_department_connector_map(
    dept_tree: dict[int, DepartmentNode],
    connector_specs: list[dict[str, Any]],
) -> dict[int, str]:
    mapping: dict[int, str] = {}
    explicit_root_departments = {
        int(root_id): spec["connector_id"]
        for spec in connector_specs
        for root_id in spec.get("root_department_ids") or []
        if str(root_id).strip()
    }
    for department_id, department in dept_tree.items():
        selected_connector_id = "default"
        for ancestor_id in department.path_ids:
            if ancestor_id in explicit_root_departments:
                selected_connector_id = explicit_root_departments[ancestor_id]
                break
        mapping[department_id] = selected_connector_id
    return mapping


def select_mapping_rules(
    rules: list[AttributeMappingRuleRecord],
    *,
    direction: str,
    connector_id: str,
) -> list[AttributeMappingRuleRecord]:
    normalized_direction = normalize_mapping_direction(direction)
    return [
        rule
        for rule in rules
        if rule.is_enabled
        and normalize_mapping_direction(rule.direction) == normalized_direction
        and (not rule.connector_id or rule.connector_id == connector_id)
    ]


def sanitize_source_writeback_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in dict(payload or {}).items():
        if value in (None, ""):
            continue
        if key == "department":
            continue
        sanitized[key] = value
    return sanitized
