from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Callable, Dict, Optional

from sync_app.core.models import ConfigReleaseSnapshotRecord
from sync_app.services.config_bundle import export_organization_bundle, import_organization_bundle
from sync_app.storage.local_db import ConfigReleaseSnapshotRepository, DatabaseManager
from sync_app.storage.secret_store import CONNECTOR_SECRET_FIELDS, ORGANIZATION_SECRET_FIELDS

SECRET_FIELDS_BY_SECTION = {
    "organization_config": set(ORGANIZATION_SECRET_FIELDS),
    "connectors": set(CONNECTOR_SECRET_FIELDS),
}
IGNORED_TOP_LEVEL_KEYS = {"exported_at"}
IGNORED_FIELDS_BY_SECTION = {
    "organization": {"org_id"},
    "org_settings": {"source_root_unit_display_text"},
}
SECTION_TITLES = {
    "organization": "Organization Metadata",
    "organization_config": "Base Configuration",
    "org_settings": "Organization Settings",
    "connectors": "Connectors",
    "attribute_mappings": "Attribute Mappings",
    "department_ou_mappings": "Department Routing",
    "group_exclusion_rules": "Group Exclusion Rules",
}
FIELD_LABELS = {
    "source_provider": "Source Provider",
    "corpid": "CorpID",
    "agentid": "AgentID",
    "corpsecret": "CorpSecret",
    "webhook_url": "Webhook URL",
    "ldap_server": "LDAP Server",
    "ldap_domain": "LDAP Domain",
    "ldap_username": "LDAP Username",
    "ldap_password": "LDAP Password",
    "ldap_port": "LDAP Port",
    "ldap_use_ssl": "Use SSL",
    "ldap_validate_cert": "Certificate Validation",
    "ldap_ca_cert_path": "CA Certificate Path",
    "default_password": "Default Password",
    "force_change_password": "Force Password Change",
    "password_complexity": "Password Complexity",
    "schedule_time": "Daily Schedule Time",
    "retry_interval": "Retry Interval",
    "max_retries": "Max Retries",
    "group_display_separator": "Group Separator",
    "group_recursive_enabled": "Recursive Group Sync",
    "managed_relation_cleanup_enabled": "Relation Cleanup",
    "schedule_execution_mode": "Scheduled Mode",
    "web_bind_host": "Bind Host",
    "web_bind_port": "Bind Port",
    "web_public_base_url": "Public Base URL",
    "web_session_cookie_secure_mode": "Secure Cookie Policy",
    "web_trust_proxy_headers": "Trust Proxy Headers",
    "web_forwarded_allow_ips": "Forwarded Allow IPs",
    "brand_display_name": "Brand Display Name",
    "brand_mark_text": "Brand Mark Text",
    "brand_attribution": "Footer Attribution",
    "user_ou_placement_strategy": "OU Placement Strategy",
    "source_root_unit_ids": "Source Root Unit IDs",
    "directory_root_ou_path": "Target AD Root OU Path",
    "disabled_users_ou_path": "Disabled Users OU Path",
    "custom_group_ou_path": "Custom Group OU Path",
    "name": "Name",
    "description": "Description",
    "is_enabled": "Enabled",
    "config_path": "Legacy Import Path",
    "connector_id": "Connector ID",
    "root_department_ids": "Root Department IDs",
    "username_strategy": "Username Strategy",
    "username_collision_policy": "Collision Policy",
    "username_collision_template": "Collision Template",
    "username_template": "Username Template",
    "disabled_users_ou": "Disabled Users OU",
    "group_type": "Group Type",
    "group_mail_domain": "Group Mail Domain",
    "managed_tag_ids": "Managed Tag IDs",
    "managed_external_chat_ids": "Managed External Chat IDs",
    "direction": "Direction",
    "source_field": "Source Field",
    "target_field": "Target Field",
    "transform_template": "Transform Template",
    "sync_mode": "Sync Mode",
    "notes": "Notes",
    "source_department_id": "Source Department ID",
    "source_department_name": "Source Department Name",
    "target_ou_path": "Target OU Path",
    "apply_mode": "Apply Mode",
    "rule_type": "Rule Type",
    "protection_level": "Protection Level",
    "match_type": "Match Type",
    "match_value": "Match Value",
    "display_name": "Display Name",
    "source": "Source",
}
FIELD_ORDER = {
    "organization": ("name", "description", "is_enabled", "config_path"),
    "organization_config": (
        "source_provider",
        "corpid",
        "agentid",
        "corpsecret",
        "webhook_url",
        "ldap_server",
        "ldap_domain",
        "ldap_username",
        "ldap_password",
        "ldap_port",
        "ldap_use_ssl",
        "ldap_validate_cert",
        "ldap_ca_cert_path",
        "default_password",
        "force_change_password",
        "password_complexity",
    ),
    "org_settings": (
        "group_display_separator",
        "group_recursive_enabled",
        "managed_relation_cleanup_enabled",
        "schedule_execution_mode",
        "brand_display_name",
        "brand_mark_text",
        "brand_attribution",
        "user_ou_placement_strategy",
        "source_root_unit_ids",
        "directory_root_ou_path",
        "disabled_users_ou_path",
        "custom_group_ou_path",
    ),
    "connectors": (
        "name",
        "ldap_server",
        "ldap_domain",
        "ldap_username",
        "ldap_password",
        "ldap_use_ssl",
        "ldap_port",
        "ldap_validate_cert",
        "default_password",
        "password_complexity",
        "root_department_ids",
        "username_strategy",
        "username_collision_policy",
        "username_template",
        "disabled_users_ou",
        "group_type",
        "group_mail_domain",
        "custom_group_ou_path",
        "managed_tag_ids",
        "managed_external_chat_ids",
        "is_enabled",
    ),
    "attribute_mappings": (
        "direction",
        "connector_id",
        "source_field",
        "target_field",
        "transform_template",
        "sync_mode",
        "is_enabled",
        "notes",
    ),
    "department_ou_mappings": (
        "connector_id",
        "source_department_name",
        "source_department_id",
        "target_ou_path",
        "apply_mode",
        "is_enabled",
        "notes",
    ),
    "group_exclusion_rules": (
        "rule_type",
        "protection_level",
        "match_type",
        "match_value",
        "display_name",
        "is_enabled",
        "source",
    ),
}
TRIGGER_LABELS = {
    "manual_release": "Manual Publish",
    "rollback": "Rollback",
    "rollback_safety": "Pre-Rollback Backup",
}


def _humanize_field_name(field_name: str) -> str:
    normalized = str(field_name or "").strip().replace("_", " ")
    return " ".join(part.capitalize() for part in normalized.split()) or "-"


def _field_label(field_name: str) -> str:
    return FIELD_LABELS.get(field_name, _humanize_field_name(field_name))


def _section_secret_fields(section_key: str) -> set[str]:
    return set(SECRET_FIELDS_BY_SECTION.get(section_key, set()))


def _normalize_collection(
    items: list[dict[str, Any]],
    *,
    key_builder: Callable[[dict[str, Any]], str],
) -> list[dict[str, Any]]:
    return sorted(
        [copy.deepcopy(dict(item or {})) for item in list(items or [])],
        key=lambda item: key_builder(item).lower(),
    )


def _normalized_bundle_payload(bundle: dict[str, Any]) -> dict[str, Any]:
    normalized = copy.deepcopy(dict(bundle or {}))
    for key in IGNORED_TOP_LEVEL_KEYS:
        normalized.pop(key, None)
    normalized["organization"] = dict(normalized.get("organization") or {})
    normalized["organization_config"] = dict(normalized.get("organization_config") or {})
    normalized["org_settings"] = dict(normalized.get("org_settings") or {})
    normalized["connectors"] = _normalize_collection(
        list(normalized.get("connectors") or []),
        key_builder=lambda item: str(item.get("connector_id") or ""),
    )
    normalized["attribute_mappings"] = _normalize_collection(
        list(normalized.get("attribute_mappings") or []),
        key_builder=lambda item: "|".join(
            [
                str(item.get("connector_id") or ""),
                str(item.get("direction") or ""),
                str(item.get("source_field") or ""),
                str(item.get("target_field") or ""),
            ]
        ),
    )
    normalized["department_ou_mappings"] = _normalize_collection(
        list(normalized.get("department_ou_mappings") or []),
        key_builder=lambda item: "|".join(
            [
                str(item.get("connector_id") or ""),
                str(item.get("source_department_id") or ""),
            ]
        ),
    )
    normalized["group_exclusion_rules"] = _normalize_collection(
        list(normalized.get("group_exclusion_rules") or []),
        key_builder=lambda item: "|".join(
            [
                str(item.get("rule_type") or ""),
                str(item.get("protection_level") or ""),
                str(item.get("match_type") or ""),
                str(item.get("match_value") or ""),
            ]
        ),
    )
    return normalized


def build_config_release_bundle_hash(bundle: dict[str, Any]) -> str:
    normalized = _normalized_bundle_payload(bundle)
    payload = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _format_value(
    section_key: str,
    field_name: str,
    value: Any,
    *,
    previous_value: Any = None,
) -> str:
    if field_name in _section_secret_fields(section_key):
        if not value:
            return "Not configured"
        if previous_value is not None and value != previous_value:
            return "Updated"
        return "Configured"
    if isinstance(value, bool):
        return "Enabled" if value else "Disabled"
    if isinstance(value, list):
        normalized_items = [str(item).strip() for item in value if str(item).strip()]
        return ", ".join(normalized_items) if normalized_items else "None"
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    normalized = str(value or "").strip()
    return normalized or "Not set"


def _collection_item_key(section_key: str, item: dict[str, Any]) -> str:
    if section_key == "connectors":
        return str(item.get("connector_id") or "")
    if section_key == "attribute_mappings":
        return "|".join(
            [
                str(item.get("connector_id") or ""),
                str(item.get("direction") or ""),
                str(item.get("source_field") or ""),
                str(item.get("target_field") or ""),
            ]
        )
    if section_key == "department_ou_mappings":
        return "|".join(
            [
                str(item.get("connector_id") or ""),
                str(item.get("source_department_id") or ""),
            ]
        )
    if section_key == "group_exclusion_rules":
        return "|".join(
            [
                str(item.get("rule_type") or ""),
                str(item.get("protection_level") or ""),
                str(item.get("match_type") or ""),
                str(item.get("match_value") or ""),
            ]
        )
    return json.dumps(item, ensure_ascii=False, sort_keys=True)


def _collection_item_label(section_key: str, item: dict[str, Any]) -> str:
    if section_key == "connectors":
        connector_id = str(item.get("connector_id") or "").strip()
        name = str(item.get("name") or connector_id).strip()
        if connector_id and name and name != connector_id:
            return f"{name} [{connector_id}]"
        return name or connector_id or "Connector"
    if section_key == "attribute_mappings":
        direction = str(item.get("direction") or "").strip() or "mapping"
        source_field = str(item.get("source_field") or "").strip() or "-"
        target_field = str(item.get("target_field") or "").strip() or "-"
        connector_id = str(item.get("connector_id") or "").strip()
        connector_part = f"[{connector_id}] " if connector_id else ""
        return f"{connector_part}{direction}: {source_field} -> {target_field}"
    if section_key == "department_ou_mappings":
        department_id = str(item.get("source_department_id") or "").strip() or "-"
        department_name = str(item.get("source_department_name") or "").strip()
        connector_id = str(item.get("connector_id") or "").strip()
        prefix = f"[{connector_id}] " if connector_id else ""
        if department_name:
            return f"{prefix}{department_name} [{department_id}]"
        return f"{prefix}{department_id}"
    if section_key == "group_exclusion_rules":
        return (
            f"{str(item.get('rule_type') or '').strip() or '-'} / "
            f"{str(item.get('protection_level') or '').strip() or '-'}: "
            f"{str(item.get('match_value') or '').strip() or '-'}"
        )
    return _collection_item_key(section_key, item) or "Item"


def _ordered_keys(section_key: str, payload: dict[str, Any]) -> list[str]:
    preferred = [key for key in FIELD_ORDER.get(section_key, ()) if key in payload]
    remaining = sorted(key for key in payload.keys() if key not in preferred)
    return [*preferred, *remaining]


def _build_item_summary(
    section_key: str,
    item: dict[str, Any],
    *,
    previous_item: Optional[dict[str, Any]] = None,
    changed_only: bool = False,
) -> str:
    lines: list[str] = []
    for field_name in _ordered_keys(section_key, item):
        if field_name in IGNORED_FIELDS_BY_SECTION.get(section_key, set()):
            continue
        current_value = item.get(field_name)
        previous_value = previous_item.get(field_name) if previous_item else None
        if changed_only and previous_item is not None and current_value == previous_value:
            continue
        lines.append(
            f"{_field_label(field_name)}: {_format_value(section_key, field_name, current_value, previous_value=previous_value)}"
        )
    return "\n".join(lines) if lines else "No field-level changes detected."


def _build_mapping_group(
    section_key: str,
    current_values: dict[str, Any],
    baseline_values: dict[str, Any],
) -> Optional[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    added_count = 0
    removed_count = 0
    changed_count = 0
    all_keys = sorted(set(current_values.keys()) | set(baseline_values.keys()))
    for field_name in all_keys:
        if field_name in IGNORED_FIELDS_BY_SECTION.get(section_key, set()):
            continue
        current_value = current_values.get(field_name)
        baseline_value = baseline_values.get(field_name)
        if current_value == baseline_value:
            continue
        if field_name not in baseline_values:
            change_type = "added"
            added_count += 1
        elif field_name not in current_values:
            change_type = "removed"
            removed_count += 1
        else:
            change_type = "changed"
            changed_count += 1
        items.append(
            {
                "label": _field_label(field_name),
                "change_type": change_type,
                "before": _format_value(section_key, field_name, baseline_value, previous_value=current_value),
                "after": _format_value(section_key, field_name, current_value, previous_value=baseline_value),
            }
        )
    if not items:
        return None
    return {
        "title": SECTION_TITLES[section_key],
        "items": items,
        "added_count": added_count,
        "removed_count": removed_count,
        "changed_count": changed_count,
    }


def _build_collection_group(
    section_key: str,
    current_items: list[dict[str, Any]],
    baseline_items: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    current_by_key = {
        _collection_item_key(section_key, item): dict(item or {})
        for item in list(current_items or [])
    }
    baseline_by_key = {
        _collection_item_key(section_key, item): dict(item or {})
        for item in list(baseline_items or [])
    }
    rows: list[dict[str, Any]] = []
    added_count = 0
    removed_count = 0
    changed_count = 0
    all_keys = sorted(set(current_by_key.keys()) | set(baseline_by_key.keys()))
    for item_key in all_keys:
        current_item = current_by_key.get(item_key)
        baseline_item = baseline_by_key.get(item_key)
        if current_item == baseline_item:
            continue
        if baseline_item is None and current_item is not None:
            added_count += 1
            rows.append(
                {
                    "label": _collection_item_label(section_key, current_item),
                    "change_type": "added",
                    "before": "Not set",
                    "after": _build_item_summary(section_key, current_item),
                }
            )
            continue
        if current_item is None and baseline_item is not None:
            removed_count += 1
            rows.append(
                {
                    "label": _collection_item_label(section_key, baseline_item),
                    "change_type": "removed",
                    "before": _build_item_summary(section_key, baseline_item),
                    "after": "Not set",
                }
            )
            continue
        changed_count += 1
        rows.append(
            {
                "label": _collection_item_label(section_key, current_item or baseline_item or {}),
                "change_type": "changed",
                "before": _build_item_summary(section_key, baseline_item or {}, previous_item=current_item or {}, changed_only=True),
                "after": _build_item_summary(section_key, current_item or {}, previous_item=baseline_item or {}, changed_only=True),
            }
        )
    if not rows:
        return None
    return {
        "title": SECTION_TITLES[section_key],
        "items": rows,
        "added_count": added_count,
        "removed_count": removed_count,
        "changed_count": changed_count,
    }


def build_config_release_diff(
    current_bundle: Optional[dict[str, Any]],
    baseline_bundle: Optional[dict[str, Any]],
) -> dict[str, Any]:
    normalized_current = _normalized_bundle_payload(current_bundle or {})
    normalized_baseline = _normalized_bundle_payload(baseline_bundle or {})
    groups = [
        _build_mapping_group(
            "organization",
            dict(normalized_current.get("organization") or {}),
            dict(normalized_baseline.get("organization") or {}),
        ),
        _build_mapping_group(
            "organization_config",
            dict(normalized_current.get("organization_config") or {}),
            dict(normalized_baseline.get("organization_config") or {}),
        ),
        _build_mapping_group(
            "org_settings",
            dict(normalized_current.get("org_settings") or {}),
            dict(normalized_baseline.get("org_settings") or {}),
        ),
        _build_collection_group(
            "connectors",
            list(normalized_current.get("connectors") or []),
            list(normalized_baseline.get("connectors") or []),
        ),
        _build_collection_group(
            "attribute_mappings",
            list(normalized_current.get("attribute_mappings") or []),
            list(normalized_baseline.get("attribute_mappings") or []),
        ),
        _build_collection_group(
            "department_ou_mappings",
            list(normalized_current.get("department_ou_mappings") or []),
            list(normalized_baseline.get("department_ou_mappings") or []),
        ),
        _build_collection_group(
            "group_exclusion_rules",
            list(normalized_current.get("group_exclusion_rules") or []),
            list(normalized_baseline.get("group_exclusion_rules") or []),
        ),
    ]
    normalized_groups = [group for group in groups if group]
    added_count = sum(int(group["added_count"]) for group in normalized_groups)
    removed_count = sum(int(group["removed_count"]) for group in normalized_groups)
    changed_count = sum(int(group["changed_count"]) for group in normalized_groups)
    changed_item_count = sum(len(group["items"]) for group in normalized_groups)
    return {
        "changed": bool(normalized_groups),
        "groups": normalized_groups,
        "changed_group_count": len(normalized_groups),
        "changed_item_count": changed_item_count,
        "added_count": added_count,
        "removed_count": removed_count,
        "changed_count": changed_count,
    }


def _snapshot_summary(diff: dict[str, Any], baseline_snapshot: Optional[ConfigReleaseSnapshotRecord]) -> dict[str, Any]:
    return {
        "baseline_snapshot_id": baseline_snapshot.id if baseline_snapshot else None,
        "changed": bool(diff.get("changed")),
        "changed_group_count": int(diff.get("changed_group_count") or 0),
        "changed_item_count": int(diff.get("changed_item_count") or 0),
        "added_count": int(diff.get("added_count") or 0),
        "removed_count": int(diff.get("removed_count") or 0),
        "changed_count": int(diff.get("changed_count") or 0),
    }


def build_config_release_snapshot_title(snapshot: ConfigReleaseSnapshotRecord) -> str:
    normalized_name = str(snapshot.snapshot_name or "").strip()
    if normalized_name:
        return normalized_name
    return f"Release #{snapshot.id or '-'}"


def build_config_release_trigger_label(trigger_action: str) -> str:
    return TRIGGER_LABELS.get(str(trigger_action or "").strip(), _humanize_field_name(str(trigger_action or "")))


def publish_current_config_release_snapshot(
    db_manager: DatabaseManager,
    org_id: str,
    *,
    created_by: str = "",
    snapshot_name: str = "",
    trigger_action: str = "manual_release",
    source_snapshot_id: Optional[int] = None,
    force: bool = False,
) -> dict[str, Any]:
    repo = ConfigReleaseSnapshotRepository(db_manager)
    normalized_org_id = str(org_id or "").strip().lower() or "default"
    current_bundle = export_organization_bundle(db_manager, normalized_org_id)
    current_hash = build_config_release_bundle_hash(current_bundle)
    latest_snapshot = repo.get_latest_snapshot_record(org_id=normalized_org_id)
    if latest_snapshot and latest_snapshot.bundle_hash == current_hash and not force:
        return {
            "created": False,
            "snapshot": latest_snapshot,
            "bundle_hash": current_hash,
            "baseline_snapshot": latest_snapshot,
            "diff": latest_snapshot.summary or {},
        }

    diff = build_config_release_diff(
        current_bundle,
        latest_snapshot.bundle if latest_snapshot and isinstance(latest_snapshot.bundle, dict) else None,
    )
    summary = _snapshot_summary(diff, latest_snapshot)
    snapshot_id = repo.add_snapshot(
        org_id=normalized_org_id,
        snapshot_name=str(snapshot_name or "").strip(),
        trigger_action=str(trigger_action or "manual_release").strip() or "manual_release",
        created_by=str(created_by or "").strip(),
        source_snapshot_id=source_snapshot_id if source_snapshot_id is not None else (latest_snapshot.id if latest_snapshot else None),
        bundle_hash=current_hash,
        bundle=current_bundle,
        summary=summary,
    )
    snapshot = repo.get_snapshot_record(snapshot_id, org_id=normalized_org_id)
    return {
        "created": True,
        "snapshot": snapshot,
        "bundle_hash": current_hash,
        "baseline_snapshot": latest_snapshot,
        "diff": diff,
    }


def _snapshot_by_id(
    repo: ConfigReleaseSnapshotRepository,
    snapshot_id: Optional[int],
    *,
    org_id: str,
    listed_snapshots: list[ConfigReleaseSnapshotRecord],
) -> Optional[ConfigReleaseSnapshotRecord]:
    if snapshot_id is None:
        return None
    for snapshot in listed_snapshots:
        if snapshot.id == snapshot_id:
            return snapshot
    return repo.get_snapshot_record(snapshot_id, org_id=org_id)


def build_config_release_center_data(
    db_manager: DatabaseManager,
    org_id: str,
    *,
    current_snapshot_id: Optional[int] = None,
    baseline_snapshot_id: Optional[int] = None,
    snapshot_limit: int = 12,
) -> dict[str, Any]:
    repo = ConfigReleaseSnapshotRepository(db_manager)
    normalized_org_id = str(org_id or "").strip().lower() or "default"
    current_bundle = export_organization_bundle(db_manager, normalized_org_id)
    current_hash = build_config_release_bundle_hash(current_bundle)
    snapshots = repo.list_snapshot_records(org_id=normalized_org_id, limit=snapshot_limit)
    latest_snapshot = snapshots[0] if snapshots else None
    has_unpublished_changes = latest_snapshot is None or latest_snapshot.bundle_hash != current_hash

    snapshot_rows: list[dict[str, Any]] = []
    for index, snapshot in enumerate(snapshots):
        previous_snapshot = snapshots[index + 1] if index + 1 < len(snapshots) else None
        snapshot_rows.append(
            {
                "snapshot": snapshot,
                "title": build_config_release_snapshot_title(snapshot),
                "trigger_label": build_config_release_trigger_label(snapshot.trigger_action),
                "previous_snapshot_id": previous_snapshot.id if previous_snapshot else None,
                "is_latest": index == 0,
            }
        )

    comparison_mode = "none"
    comparison_title = "No snapshot comparison available yet."
    comparison_current_label = "Live Configuration"
    comparison_baseline_label = "No baseline"
    comparison_diff = build_config_release_diff(current_bundle, None)
    selected_current_snapshot = None
    selected_baseline_snapshot = None

    explicit_snapshot_compare = current_snapshot_id is not None or baseline_snapshot_id is not None
    if explicit_snapshot_compare and snapshots:
        selected_current_snapshot = _snapshot_by_id(
            repo,
            current_snapshot_id or (latest_snapshot.id if latest_snapshot else None),
            org_id=normalized_org_id,
            listed_snapshots=snapshots,
        )
        if selected_current_snapshot:
            if baseline_snapshot_id is not None:
                selected_baseline_snapshot = _snapshot_by_id(
                    repo,
                    baseline_snapshot_id,
                    org_id=normalized_org_id,
                    listed_snapshots=snapshots,
                )
            if selected_baseline_snapshot is None:
                for index, snapshot in enumerate(snapshots):
                    if snapshot.id == selected_current_snapshot.id and index + 1 < len(snapshots):
                        selected_baseline_snapshot = snapshots[index + 1]
                        break
        if selected_current_snapshot:
            comparison_mode = "snapshot"
            comparison_title = "Snapshot Comparison"
            comparison_current_label = build_config_release_snapshot_title(selected_current_snapshot)
            comparison_baseline_label = (
                build_config_release_snapshot_title(selected_baseline_snapshot)
                if selected_baseline_snapshot
                else "No baseline snapshot"
            )
            comparison_diff = build_config_release_diff(
                selected_current_snapshot.bundle if isinstance(selected_current_snapshot.bundle, dict) else {},
                selected_baseline_snapshot.bundle if selected_baseline_snapshot and isinstance(selected_baseline_snapshot.bundle, dict) else None,
            )
    elif has_unpublished_changes:
        comparison_mode = "live"
        comparison_title = "Unpublished Configuration Changes"
        comparison_current_label = "Live Configuration"
        comparison_baseline_label = (
            build_config_release_snapshot_title(latest_snapshot)
            if latest_snapshot
            else "No published snapshot"
        )
        comparison_diff = build_config_release_diff(
            current_bundle,
            latest_snapshot.bundle if latest_snapshot and isinstance(latest_snapshot.bundle, dict) else None,
        )
    elif latest_snapshot and len(snapshots) > 1:
        selected_current_snapshot = latest_snapshot
        selected_baseline_snapshot = snapshots[1]
        comparison_mode = "snapshot"
        comparison_title = "Latest Published Snapshot vs Previous"
        comparison_current_label = build_config_release_snapshot_title(selected_current_snapshot)
        comparison_baseline_label = build_config_release_snapshot_title(selected_baseline_snapshot)
        comparison_diff = build_config_release_diff(
            selected_current_snapshot.bundle if isinstance(selected_current_snapshot.bundle, dict) else {},
            selected_baseline_snapshot.bundle if isinstance(selected_baseline_snapshot.bundle, dict) else {},
        )

    return {
        "current_bundle": current_bundle,
        "current_bundle_hash": current_hash,
        "has_unpublished_changes": has_unpublished_changes,
        "latest_snapshot": latest_snapshot,
        "snapshot_count": len(snapshots),
        "snapshots": snapshot_rows,
        "comparison_mode": comparison_mode,
        "comparison_title": comparison_title,
        "comparison_current_label": comparison_current_label,
        "comparison_baseline_label": comparison_baseline_label,
        "comparison_diff": comparison_diff,
        "selected_current_snapshot": selected_current_snapshot,
        "selected_baseline_snapshot": selected_baseline_snapshot,
    }


def rollback_config_release_snapshot(
    db_manager: DatabaseManager,
    snapshot_id: int,
    *,
    org_id: Optional[str] = None,
    created_by: str = "",
) -> dict[str, Any]:
    repo = ConfigReleaseSnapshotRepository(db_manager)
    target_snapshot = repo.get_snapshot_record(snapshot_id, org_id=org_id)
    if not target_snapshot:
        raise ValueError("Configuration release snapshot not found.")
    if not isinstance(target_snapshot.bundle, dict):
        raise ValueError("Snapshot bundle payload is unavailable.")

    normalized_org_id = str(target_snapshot.org_id or org_id or "").strip().lower() or "default"
    current_bundle = export_organization_bundle(db_manager, normalized_org_id)
    current_hash = build_config_release_bundle_hash(current_bundle)
    target_hash = target_snapshot.bundle_hash or build_config_release_bundle_hash(target_snapshot.bundle)

    safety_snapshot = None
    if current_hash != target_hash:
        safety_result = publish_current_config_release_snapshot(
            db_manager,
            normalized_org_id,
            created_by=created_by,
            snapshot_name=f"Pre-rollback backup before #{target_snapshot.id}",
            trigger_action="rollback_safety",
            source_snapshot_id=target_snapshot.id,
            force=False,
        )
        safety_snapshot = safety_result.get("snapshot")

    import_organization_bundle(
        db_manager,
        target_snapshot.bundle,
        target_org_id=normalized_org_id,
        replace_existing=True,
    )
    rollback_result = publish_current_config_release_snapshot(
        db_manager,
        normalized_org_id,
        created_by=created_by,
        snapshot_name=f"Rollback to {build_config_release_snapshot_title(target_snapshot)}",
        trigger_action="rollback",
        source_snapshot_id=target_snapshot.id,
        force=True,
    )
    return {
        "target_snapshot": target_snapshot,
        "safety_snapshot": safety_snapshot,
        "rollback_snapshot": rollback_result.get("snapshot"),
    }
