from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Optional

from sync_app.core.models.base import MappingLikeModel


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    try:
        decoded = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return decoded if isinstance(decoded, list) else []


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        decoded = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


@dataclass(slots=True)
class SourceFieldRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    provider_id: str = ""
    source_connector_id: str = "default"
    raw_field_path: str = ""
    raw_field_name: str = ""
    canonical_field_key: str = ""
    display_label: str = ""
    category: str = "custom"
    data_type: str = "string"
    is_multi_value: bool = False
    is_sensitive: bool = False
    is_identifier_candidate: bool = False
    is_custom: bool = False
    is_derived: bool = False
    availability_status: str = "unknown"
    permission_status: str = "unknown"
    coverage_count: int = 0
    coverage_rate: float = 0.0
    masked_sample_values: list[Any] = field(default_factory=list)
    first_detected_at: str = ""
    last_detected_at: str = ""
    schema_version: int = 1
    latest_snapshot_id: Optional[int] = None

    @classmethod
    def from_row(cls, row: Any) -> "SourceFieldRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default"),
            provider_id=str(row["provider_id"] or ""),
            source_connector_id=str(row["source_connector_id"] or "default"),
            raw_field_path=str(row["raw_field_path"] or ""),
            raw_field_name=str(row["raw_field_name"] or ""),
            canonical_field_key=str(row["canonical_field_key"] or ""),
            display_label=str(row["display_label"] or ""),
            category=str(row["category"] or "custom"),
            data_type=str(row["data_type"] or "string"),
            is_multi_value=bool(row["is_multi_value"]),
            is_sensitive=bool(row["is_sensitive"]),
            is_identifier_candidate=bool(row["is_identifier_candidate"]),
            is_custom=bool(row["is_custom"]),
            is_derived=bool(row["is_derived"]),
            availability_status=str(row["availability_status"] or "unknown"),
            permission_status=str(row["permission_status"] or "unknown"),
            coverage_count=int(row["coverage_count"] or 0),
            coverage_rate=float(row["coverage_rate"] or 0.0),
            masked_sample_values=_json_list(row["masked_sample_values_json"]),
            first_detected_at=str(row["first_detected_at"] or ""),
            last_detected_at=str(row["last_detected_at"] or ""),
            schema_version=max(int(row["schema_version"] or 1), 1),
            latest_snapshot_id=(
                int(row["latest_snapshot_id"])
                if row["latest_snapshot_id"] not in (None, "")
                else None
            ),
        )


@dataclass(slots=True)
class CanonicalFieldRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "*"
    canonical_field_key: str = ""
    display_label: str = ""
    category: str = "custom"
    data_type: str = "string"
    is_multi_value: bool = False
    is_sensitive: bool = False
    is_identifier: bool = False
    is_custom: bool = False
    is_derived: bool = False
    allowed_mapping_roles: list[str] = field(default_factory=list)
    description: str = ""
    schema_version: int = 1
    is_active: bool = True
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "CanonicalFieldRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "*"),
            canonical_field_key=str(row["canonical_field_key"] or ""),
            display_label=str(row["display_label"] or ""),
            category=str(row["category"] or "custom"),
            data_type=str(row["data_type"] or "string"),
            is_multi_value=bool(row["is_multi_value"]),
            is_sensitive=bool(row["is_sensitive"]),
            is_identifier=bool(row["is_identifier"]),
            is_custom=bool(row["is_custom"]),
            is_derived=bool(row["is_derived"]),
            allowed_mapping_roles=_json_list(row["allowed_mapping_roles_json"]),
            description=str(row["description"] or ""),
            schema_version=max(int(row["schema_version"] or 1), 1),
            is_active=bool(row["is_active"]),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )


@dataclass(slots=True)
class ADTargetAttributeRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    ad_connector_id: str = "default"
    ldap_attribute: str = ""
    display_label: str = ""
    category: str = "custom"
    data_type: str = "string"
    is_multi_value: bool = False
    is_writable: bool = False
    is_read_only: bool = True
    requires_special_handler: bool = False
    special_handler_type: str = ""
    required_permissions: list[str] = field(default_factory=list)
    supported_object_classes: list[str] = field(default_factory=list)
    schema_detected: bool = False
    capability_status: str = "unknown"
    validation_rules: dict[str, Any] = field(default_factory=dict)
    schema_version: int = 1
    latest_snapshot_id: Optional[int] = None
    last_checked_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "ADTargetAttributeRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default"),
            ad_connector_id=str(row["ad_connector_id"] or "default"),
            ldap_attribute=str(row["ldap_attribute"] or ""),
            display_label=str(row["display_label"] or ""),
            category=str(row["category"] or "custom"),
            data_type=str(row["data_type"] or "string"),
            is_multi_value=bool(row["is_multi_value"]),
            is_writable=bool(row["is_writable"]),
            is_read_only=bool(row["is_read_only"]),
            requires_special_handler=bool(row["requires_special_handler"]),
            special_handler_type=str(row["special_handler_type"] or ""),
            required_permissions=_json_list(row["required_permissions_json"]),
            supported_object_classes=_json_list(row["supported_object_classes_json"]),
            schema_detected=bool(row["schema_detected"]),
            capability_status=str(row["capability_status"] or "unknown"),
            validation_rules=_json_dict(row["validation_rules_json"]),
            schema_version=max(int(row["schema_version"] or 1), 1),
            latest_snapshot_id=(
                int(row["latest_snapshot_id"])
                if row["latest_snapshot_id"] not in (None, "")
                else None
            ),
            last_checked_at=str(row["last_checked_at"] or ""),
        )


__all__ = [
    "ADTargetAttributeRecord",
    "CanonicalFieldRecord",
    "SourceFieldRecord",
]
