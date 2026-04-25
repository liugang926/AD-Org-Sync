from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from sync_app.core.models.base import MappingLikeModel
from sync_app.core.models.utils import _normalize_mapping_direction_value


@dataclass(slots=True)
class ExclusionRuleRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    rule_type: str = ""
    protection_level: str = ""
    match_type: str = ""
    match_value: str = ""
    display_name: str = ""
    is_enabled: bool = True
    source: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "ExclusionRuleRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            rule_type=str(row["rule_type"] or ""),
            protection_level=str(row["protection_level"] or ""),
            match_type=str(row["match_type"] or ""),
            match_value=str(row["match_value"] or ""),
            display_name=str(row["display_name"] or ""),
            is_enabled=bool(row["is_enabled"]),
            source=str(row["source"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

@dataclass(slots=True)
class ManagedGroupBindingRecord(MappingLikeModel):
    org_id: str = "default"
    department_id: str = ""
    parent_department_id: Optional[str] = None
    group_sam: str = ""
    group_dn: str = ""
    group_cn: str = ""
    display_name: str = ""
    path_text: str = ""
    status: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "ManagedGroupBindingRecord":
        return cls(
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            department_id=str(row["department_id"] or ""),
            parent_department_id=str(row["parent_department_id"]) if row["parent_department_id"] is not None else None,
            group_sam=str(row["group_sam"] or ""),
            group_dn=str(row["group_dn"] or ""),
            group_cn=str(row["group_cn"] or ""),
            display_name=str(row["display_name"] or ""),
            path_text=str(row["path_text"] or ""),
            status=str(row["status"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

@dataclass(slots=True)
class ConfigReleaseSnapshotRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = ""
    snapshot_name: str = ""
    trigger_action: str = "manual_release"
    created_by: str = ""
    source_snapshot_id: Optional[int] = None
    bundle_hash: str = ""
    bundle: Optional[Dict[str, Any]] = None
    summary: Optional[Dict[str, Any]] = None
    created_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "ConfigReleaseSnapshotRecord":
        bundle = row["bundle_json"] if "bundle_json" in row.keys() else None
        if isinstance(bundle, str) and bundle:
            try:
                bundle = json.loads(bundle)
            except json.JSONDecodeError:
                bundle = {"raw": bundle}
        summary = row["summary_json"] if "summary_json" in row.keys() else None
        if isinstance(summary, str) and summary:
            try:
                summary = json.loads(summary)
            except json.JSONDecodeError:
                summary = {"raw": summary}
        source_snapshot_id = row["source_snapshot_id"] if "source_snapshot_id" in row.keys() else None
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or ""),
            snapshot_name=str(row["snapshot_name"] or ""),
            trigger_action=str(row["trigger_action"] or "manual_release"),
            created_by=str(row["created_by"] or ""),
            source_snapshot_id=int(source_snapshot_id) if source_snapshot_id not in (None, "") else None,
            bundle_hash=str(row["bundle_hash"] or ""),
            bundle=bundle if isinstance(bundle, dict) or bundle is None else {"raw": bundle},
            summary=summary if isinstance(summary, dict) or summary is None else {"raw": summary},
            created_at=str(row["created_at"] or ""),
        )

@dataclass(slots=True)
class DataQualitySnapshotRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = ""
    trigger_action: str = "manual_scan"
    created_by: str = ""
    summary: Optional[Dict[str, Any]] = None
    snapshot: Optional[Dict[str, Any]] = None
    created_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "DataQualitySnapshotRecord":
        summary = row["summary_json"] if "summary_json" in row.keys() else None
        if isinstance(summary, str) and summary:
            try:
                summary = json.loads(summary)
            except json.JSONDecodeError:
                summary = {"raw": summary}
        snapshot = row["snapshot_json"] if "snapshot_json" in row.keys() else None
        if isinstance(snapshot, str) and snapshot:
            try:
                snapshot = json.loads(snapshot)
            except json.JSONDecodeError:
                snapshot = {"raw": snapshot}
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or ""),
            trigger_action=str(row["trigger_action"] or "manual_scan"),
            created_by=str(row["created_by"] or ""),
            summary=summary if isinstance(summary, dict) or summary is None else {"raw": summary},
            snapshot=snapshot if isinstance(snapshot, dict) or snapshot is None else {"raw": snapshot},
            created_at=str(row["created_at"] or ""),
        )

@dataclass(slots=True)
class AttributeMappingRuleRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    connector_id: str = ""
    direction: str = "source_to_ad"
    source_field: str = ""
    target_field: str = ""
    transform_template: str = ""
    sync_mode: str = "replace"
    is_enabled: bool = True
    notes: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "AttributeMappingRuleRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            connector_id=str(row["connector_id"] or ""),
            direction=_normalize_mapping_direction_value(row["direction"] if "direction" in row.keys() else "source_to_ad"),
            source_field=str(row["source_field"] or ""),
            target_field=str(row["target_field"] or ""),
            transform_template=str(row["transform_template"] or ""),
            sync_mode=str(row["sync_mode"] or "replace"),
            is_enabled=bool(row["is_enabled"]),
            notes=str(row["notes"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

@dataclass(slots=True)
class OrganizationRecord(MappingLikeModel):
    org_id: str = "default"
    name: str = ""
    config_path: str = ""
    description: str = ""
    is_enabled: bool = True
    is_default: bool = False
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "OrganizationRecord":
        return cls(
            org_id=str(row["org_id"] or "default"),
            name=str(row["name"] or ""),
            config_path=str(row["config_path"] or ""),
            description=str(row["description"] or ""),
            is_enabled=bool(row["is_enabled"]),
            is_default=bool(row["is_default"]) if "is_default" in row.keys() else False,
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

@dataclass(slots=True)
class DepartmentOuMappingRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    connector_id: str = ""
    source_department_id: str = ""
    source_department_name: str = ""
    target_ou_path: str = ""
    apply_mode: str = "subtree"
    notes: str = ""
    is_enabled: bool = True
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "DepartmentOuMappingRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            connector_id=str(row["connector_id"] or ""),
            source_department_id=str(row["source_department_id"] or ""),
            source_department_name=str(row["source_department_name"] or ""),
            target_ou_path=str(row["target_ou_path"] or ""),
            apply_mode=str(row["apply_mode"] or "subtree"),
            notes=str(row["notes"] or ""),
            is_enabled=bool(row["is_enabled"]),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )
