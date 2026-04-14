from __future__ import annotations

from typing import Any, Optional

from sync_app.core.config import load_sync_config
from sync_app.core.models import AttributeMappingRuleRecord, DepartmentNode
from sync_app.core.sync_policies import normalize_group_type, normalize_mapping_direction
from sync_app.storage.local_db import SyncConnectorRepository


def _normalize_root_department_ids(raw_values: Any) -> list[int]:
    values: list[int] = []
    iterable = raw_values if isinstance(raw_values, (list, tuple, set)) else str(raw_values or "").replace("\n", ",").split(",")
    for item in iterable:
        candidate = str(item).strip()
        if candidate.isdigit():
            values.append(int(candidate))
    return values


def _normalize_ou_path(raw_value: Any, *, default: str = "") -> str:
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return default
    dn_segments = [
        part.split("=", 1)[1].strip()
        for part in raw_text.split(",")
        if "=" in part and part.strip().lower().startswith("ou=") and part.split("=", 1)[1].strip()
    ]
    if dn_segments:
        segments = list(reversed(dn_segments))
    else:
        segments = [
            segment.strip()
            for segment in raw_text.replace("\\", "/").split("/")
            if segment.strip()
        ]
    normalized = "/".join(segments)
    return normalized or default


def _normalize_ou_segments(raw_value: Any) -> list[str]:
    normalized = _normalize_ou_path(raw_value)
    if not normalized:
        return []
    return [segment.strip() for segment in normalized.split("/") if segment.strip()]


def _load_connector_config(
    connector_repo: SyncConnectorRepository,
    *,
    connector_id: str,
    config: Any,
    org_id: str,
    config_path: str,
    load_sync_config_fn,
):
    connector_config = connector_repo.get_connector_app_config(
        connector_id,
        base_config=config,
        org_id=org_id,
    )
    if connector_config is None:
        connector_config = load_sync_config_fn(config_path)
    return connector_config


def _build_connector_spec(
    *,
    connector_id: str,
    org_id: str,
    name: str,
    config_path: str,
    root_department_ids: Any,
    username_strategy: str,
    username_collision_policy: str,
    username_collision_template: str,
    username_template: str,
    disabled_users_ou: str,
    group_type: str,
    group_mail_domain: str,
    custom_group_ou_path: str,
    user_root_ou_path: str,
    managed_tag_ids: Any,
    managed_external_chat_ids: Any,
    config: Any,
) -> dict[str, Any]:
    return {
        "connector_id": connector_id,
        "org_id": org_id,
        "name": name,
        "config_path": config_path,
        "root_department_ids": _normalize_root_department_ids(root_department_ids),
        "username_strategy": str(username_strategy or "custom_template").strip() or "custom_template",
        "username_collision_policy": str(username_collision_policy or "append_employee_id").strip()
        or "append_employee_id",
        "username_collision_template": str(username_collision_template or "").strip(),
        "username_template": str(username_template or "").strip(),
        "disabled_users_ou": _normalize_ou_path(disabled_users_ou, default="Disabled Users"),
        "group_type": normalize_group_type(group_type),
        "group_mail_domain": str(group_mail_domain or "").strip(),
        "custom_group_ou_path": _normalize_ou_path(custom_group_ou_path, default="Managed Groups"),
        "user_root_ou_path": _normalize_ou_path(user_root_ou_path),
        "managed_tag_ids": [str(value).strip() for value in managed_tag_ids or [] if str(value).strip()],
        "managed_external_chat_ids": [
            str(value).strip() for value in managed_external_chat_ids or [] if str(value).strip()
        ],
        "config": config,
    }


def load_connector_specs(
    config,
    connector_repo: SyncConnectorRepository,
    *,
    connectors_enabled: bool = False,
    org_id: str = "default",
    default_root_department_ids: Any = (),
    default_disabled_users_ou: str = "Disabled Users",
    default_custom_group_ou_path: str = "Managed Groups",
    default_user_root_ou_path: str = "",
    load_sync_config_fn=load_sync_config,
) -> list[dict[str, Any]]:
    default_spec = _build_connector_spec(
        connector_id="default",
        org_id=org_id,
        name="Default Connector",
        config_path=config.config_path,
        root_department_ids=default_root_department_ids,
        username_strategy="custom_template",
        username_collision_policy="append_employee_id",
        username_collision_template="",
        username_template="",
        disabled_users_ou=default_disabled_users_ou,
        group_type="security",
        group_mail_domain="",
        custom_group_ou_path=default_custom_group_ou_path,
        user_root_ou_path=default_user_root_ou_path,
        managed_tag_ids=[],
        managed_external_chat_ids=[],
        config=config,
    )
    specs: list[dict[str, Any]] = [default_spec]
    enabled_records = list(connector_repo.list_connector_records(enabled_only=True, org_id=org_id))
    if not connectors_enabled:
        if len(enabled_records) == 1:
            record = enabled_records[0]
            specs[0] = _build_connector_spec(
                connector_id="default",
                org_id=org_id,
                name=record.name or "Default Connector",
                config_path=record.config_path or config.config_path,
                root_department_ids=record.root_department_ids or default_root_department_ids,
                username_strategy=record.username_strategy,
                username_collision_policy=record.username_collision_policy,
                username_collision_template=record.username_collision_template,
                username_template=record.username_template,
                disabled_users_ou=record.disabled_users_ou or default_disabled_users_ou,
                group_type=record.group_type or "security",
                group_mail_domain=record.group_mail_domain,
                custom_group_ou_path=record.custom_group_ou_path or default_custom_group_ou_path,
                user_root_ou_path=default_user_root_ou_path,
                managed_tag_ids=record.managed_tag_ids,
                managed_external_chat_ids=record.managed_external_chat_ids,
                config=_load_connector_config(
                    connector_repo,
                    connector_id=record.connector_id,
                    config=config,
                    org_id=org_id,
                    config_path=record.config_path,
                    load_sync_config_fn=load_sync_config_fn,
                ),
            )
        return specs
    for record in enabled_records:
        specs.append(
            _build_connector_spec(
                connector_id=record.connector_id,
                org_id=record.org_id,
                name=record.name,
                config_path=record.config_path,
                root_department_ids=record.root_department_ids,
                username_strategy=record.username_strategy,
                username_collision_policy=record.username_collision_policy,
                username_collision_template=record.username_collision_template,
                username_template=record.username_template,
                disabled_users_ou=record.disabled_users_ou or "Disabled Users",
                group_type=record.group_type,
                group_mail_domain=record.group_mail_domain,
                custom_group_ou_path=record.custom_group_ou_path,
                user_root_ou_path="",
                managed_tag_ids=record.managed_tag_ids,
                managed_external_chat_ids=record.managed_external_chat_ids,
                config=_load_connector_config(
                    connector_repo,
                    connector_id=record.connector_id,
                    config=config,
                    org_id=org_id,
                    config_path=record.config_path,
                    load_sync_config_fn=load_sync_config_fn,
                ),
            )
        )
    return specs


def build_department_connector_map(
    dept_tree: dict[int, DepartmentNode],
    connector_specs: list[dict[str, Any]],
) -> dict[int, str]:
    ordered_specs = [
        *[spec for spec in connector_specs if spec.get("connector_id") != "default"],
        *[spec for spec in connector_specs if spec.get("connector_id") == "default"],
    ]
    mapping: dict[int, str] = {}
    explicit_root_departments = {
        int(root_id): spec["connector_id"]
        for spec in ordered_specs
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


def build_department_scope_root_map(
    dept_tree: dict[int, DepartmentNode],
    connector_specs: list[dict[str, Any]],
    department_connector_map: dict[int, str],
) -> dict[int, Optional[int]]:
    connector_root_ids = {
        str(spec.get("connector_id") or "default"): set(_normalize_root_department_ids(spec.get("root_department_ids")))
        for spec in connector_specs
    }
    scope_root_map: dict[int, Optional[int]] = {}
    for department_id, department in dept_tree.items():
        connector_id = department_connector_map.get(department_id, "default")
        explicit_roots = connector_root_ids.get(connector_id, set())
        matched_root_id: Optional[int] = None
        for ancestor_id in department.path_ids:
            if ancestor_id in explicit_roots:
                matched_root_id = ancestor_id
                break
        scope_root_map[department_id] = matched_root_id
    return scope_root_map


def trim_department_paths_to_scope(
    dept_tree: dict[int, DepartmentNode],
    department_scope_root_map: dict[int, Optional[int]],
) -> None:
    for department_id, scope_root_id in department_scope_root_map.items():
        if not scope_root_id:
            continue
        department = dept_tree.get(department_id)
        if not department or scope_root_id not in department.path_ids:
            continue
        root_index = department.path_ids.index(scope_root_id)
        department.set_hierarchy(department.path[root_index:], department.path_ids[root_index:])


def is_department_in_connector_scope(
    dept_info: Optional[DepartmentNode],
    *,
    connector_specs_by_id: dict[str, dict[str, Any]],
    department_connector_map: dict[int, str],
    department_scope_root_map: dict[int, Optional[int]],
) -> bool:
    if not dept_info:
        return False
    connector_id = department_connector_map.get(dept_info.department_id, "default")
    connector_spec = connector_specs_by_id.get(connector_id) or {}
    explicit_roots = _normalize_root_department_ids(connector_spec.get("root_department_ids"))
    if not explicit_roots:
        return True
    return department_scope_root_map.get(dept_info.department_id) is not None


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


def resolve_department_ou_path(
    dept_info: Optional[DepartmentNode],
    *,
    connector_id: str,
    mappings_by_connector: dict[str, list[Any]],
) -> list[str]:
    if not dept_info:
        return []

    default_path = list(dept_info.path or [])
    if not default_path:
        return []

    connector_records = list(mappings_by_connector.get(connector_id, []))
    global_records = list(mappings_by_connector.get("", []))
    candidates = connector_records + global_records
    if not candidates:
        return default_path

    mapping_by_dept_id: dict[str, list[Any]] = {}
    for record in candidates:
        mapping_by_dept_id.setdefault(str(record.source_department_id or "").strip(), []).append(record)

    exact_record = None
    current_id = str(dept_info.department_id)
    for record in mapping_by_dept_id.get(current_id, []):
        if str(getattr(record, "apply_mode", "subtree") or "subtree").strip().lower() == "exact":
            exact_record = record
            break
    if exact_record:
        return _normalize_ou_segments(exact_record.target_ou_path) or default_path

    for index in range(len(dept_info.path_ids) - 1, -1, -1):
        ancestor_id = str(dept_info.path_ids[index])
        for record in mapping_by_dept_id.get(ancestor_id, []):
            apply_mode = str(getattr(record, "apply_mode", "subtree") or "subtree").strip().lower()
            if apply_mode != "subtree":
                continue
            mapped_base = _normalize_ou_segments(record.target_ou_path)
            if not mapped_base:
                continue
            remainder = list(dept_info.path[index + 1 :])
            return [*mapped_base, *remainder]

    return default_path
