from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from sync_app.core.models.base import MappingLikeModel


def _json_value(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _optional_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    return int(value)


@dataclass(slots=True)
class EnterpriseIdentityRecord(MappingLikeModel):
    identity_id: str = ""
    org_id: str = "default"
    display_name: str = ""
    canonical_employee_id: str = ""
    employment_status: str = "active"
    employment_type: str = "employee"
    primary_department_id: str = ""
    canonical_fields: dict[str, Any] = field(default_factory=dict)
    field_sources: dict[str, Any] = field(default_factory=dict)
    status: str = "active"
    identity_revision: int = 1
    created_by: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "EnterpriseIdentityRecord":
        return cls(
            identity_id=str(row["identity_id"] or ""),
            org_id=str(row["org_id"] or "default"),
            display_name=str(row["display_name"] or ""),
            canonical_employee_id=str(row["canonical_employee_id"] or ""),
            employment_status=str(row["employment_status"] or "active"),
            employment_type=str(row["employment_type"] or "employee"),
            primary_department_id=str(row["primary_department_id"] or ""),
            canonical_fields=dict(_json_value(row["canonical_fields_json"], {})),
            field_sources=dict(_json_value(row["field_sources_json"], {})),
            status=str(row["status"] or "active"),
            identity_revision=int(row["identity_revision"] or 1),
            created_by=str(row["created_by"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PlatformAccountRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    provider_id: str = ""
    connector_id: str = "default"
    platform_account_id: str = ""
    display_name: str = ""
    employee_id: str = ""
    email: str = ""
    mobile: str = ""
    account_status: str = "active"
    account_type: str = "person"
    primary_department_id: str = ""
    department_ids: list[str] = field(default_factory=list)
    manager_account_id: str = ""
    custom_fields: dict[str, Any] = field(default_factory=dict)
    source_snapshot_id: Optional[int] = None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    is_excluded: bool = False
    first_seen_at: str = ""
    last_seen_at: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "PlatformAccountRecord":
        return cls(
            id=_optional_int(row["id"]),
            org_id=str(row["org_id"] or "default"),
            provider_id=str(row["provider_id"] or ""),
            connector_id=str(row["connector_id"] or "default"),
            platform_account_id=str(row["platform_account_id"] or ""),
            display_name=str(row["display_name"] or ""),
            employee_id=str(row["employee_id"] or ""),
            email=str(row["email"] or ""),
            mobile=str(row["mobile"] or ""),
            account_status=str(row["account_status"] or "active"),
            account_type=str(row["account_type"] or "person"),
            primary_department_id=str(row["primary_department_id"] or ""),
            department_ids=[str(value) for value in _json_value(row["department_ids_json"], [])],
            manager_account_id=str(row["manager_account_id"] or ""),
            custom_fields=dict(_json_value(row["custom_fields_json"], {})),
            source_snapshot_id=_optional_int(row["source_snapshot_id"]),
            raw_payload=dict(_json_value(row["raw_payload_json"], {})),
            is_excluded=bool(row["is_excluded"]),
            first_seen_at=str(row["first_seen_at"] or ""),
            last_seen_at=str(row["last_seen_at"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ADAccountRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    connector_id: str = "default"
    object_guid: str = ""
    object_sid: str = ""
    distinguished_name: str = ""
    sam_account_name: str = ""
    user_principal_name: str = ""
    employee_id: str = ""
    employee_number: str = ""
    mail: str = ""
    telephone_number: str = ""
    mobile: str = ""
    display_name: str = ""
    account_enabled: Optional[bool] = None
    manager_dn: str = ""
    group_membership: list[str] = field(default_factory=list)
    ou_path: str = ""
    extension_attributes: dict[str, Any] = field(default_factory=dict)
    when_created: str = ""
    when_changed: str = ""
    account_type: str = "person"
    is_protected: bool = False
    latest_snapshot_id: Optional[int] = None
    first_seen_at: str = ""
    last_seen_at: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "ADAccountRecord":
        raw_enabled = row["account_enabled"]
        return cls(
            id=_optional_int(row["id"]),
            org_id=str(row["org_id"] or "default"),
            connector_id=str(row["connector_id"] or "default"),
            object_guid=str(row["object_guid"] or ""),
            object_sid=str(row["object_sid"] or ""),
            distinguished_name=str(row["distinguished_name"] or ""),
            sam_account_name=str(row["sam_account_name"] or ""),
            user_principal_name=str(row["user_principal_name"] or ""),
            employee_id=str(row["employee_id"] or ""),
            employee_number=str(row["employee_number"] or ""),
            mail=str(row["mail"] or ""),
            telephone_number=str(row["telephone_number"] or ""),
            mobile=str(row["mobile"] or ""),
            display_name=str(row["display_name"] or ""),
            account_enabled=None if raw_enabled is None else bool(raw_enabled),
            manager_dn=str(row["manager_dn"] or ""),
            group_membership=[str(value) for value in _json_value(row["group_membership_json"], [])],
            ou_path=str(row["ou_path"] or ""),
            extension_attributes=dict(_json_value(row["extension_attributes_json"], {})),
            when_created=str(row["when_created"] or ""),
            when_changed=str(row["when_changed"] or ""),
            account_type=str(row["account_type"] or "person"),
            is_protected=bool(row["is_protected"]),
            latest_snapshot_id=_optional_int(row["latest_snapshot_id"]),
            first_seen_at=str(row["first_seen_at"] or ""),
            last_seen_at=str(row["last_seen_at"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class IdentityAccountLinkRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    identity_id: str = ""
    account_kind: str = "platform"
    platform_account_id: Optional[int] = None
    ad_account_id: Optional[int] = None
    account_role: str = "source"
    account_purpose: str = ""
    association_type: str = "automatic"
    status: str = "active"
    source: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)
    confidence: int = 0
    created_by: str = ""
    valid_until: str = ""
    link_revision: int = 1
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "IdentityAccountLinkRecord":
        return cls(
            id=_optional_int(row["id"]),
            org_id=str(row["org_id"] or "default"),
            identity_id=str(row["identity_id"] or ""),
            account_kind=str(row["account_kind"] or "platform"),
            platform_account_id=_optional_int(row["platform_account_id"]),
            ad_account_id=_optional_int(row["ad_account_id"]),
            account_role=str(row["account_role"] or "source"),
            account_purpose=str(row["account_purpose"] or ""),
            association_type=str(row["association_type"] or "automatic"),
            status=str(row["status"] or "active"),
            source=str(row["source"] or ""),
            evidence=dict(_json_value(row["evidence_json"], {})),
            confidence=int(row["confidence"] or 0),
            created_by=str(row["created_by"] or ""),
            valid_until=str(row["valid_until"] or ""),
            link_revision=int(row["link_revision"] or 1),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class IdentityMatchRuleRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    rule_order: int = 100
    rule_name: str = ""
    source_provider: str = "*"
    source_field: str = ""
    ad_field: str = ""
    is_required: bool = False
    case_sensitive: bool = False
    trim_whitespace: bool = True
    strip_phone_country_code: bool = False
    lowercase_email: bool = False
    allow_fallback: bool = True
    allow_auto_link: bool = False
    confidence_level: str = "medium"
    confidence_score: int = 50
    stop_on_conflict: bool = True
    is_enabled: bool = True
    rule_revision: int = 1
    created_by: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "IdentityMatchRuleRecord":
        return cls(
            id=_optional_int(row["id"]),
            org_id=str(row["org_id"] or "default"),
            rule_order=int(row["rule_order"] or 100),
            rule_name=str(row["rule_name"] or ""),
            source_provider=str(row["source_provider"] or "*"),
            source_field=str(row["source_field"] or ""),
            ad_field=str(row["ad_field"] or ""),
            is_required=bool(row["is_required"]),
            case_sensitive=bool(row["case_sensitive"]),
            trim_whitespace=bool(row["trim_whitespace"]),
            strip_phone_country_code=bool(row["strip_phone_country_code"]),
            lowercase_email=bool(row["lowercase_email"]),
            allow_fallback=bool(row["allow_fallback"]),
            allow_auto_link=bool(row["allow_auto_link"]),
            confidence_level=str(row["confidence_level"] or "medium"),
            confidence_score=int(row["confidence_score"] or 0),
            stop_on_conflict=bool(row["stop_on_conflict"]),
            is_enabled=bool(row["is_enabled"]),
            rule_revision=int(row["rule_revision"] or 1),
            created_by=str(row["created_by"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class IdentityMatchCandidateRecord(MappingLikeModel):
    id: Optional[int] = None
    run_id: str = ""
    org_id: str = "default"
    platform_account_id: int = 0
    ad_account_id: Optional[int] = None
    proposed_identity_id: str = ""
    result_level: str = "manual_confirmation"
    confidence: int = 0
    matched_rule_ids: list[int] = field(default_factory=list)
    matched_fields: dict[str, Any] = field(default_factory=dict)
    unmatched_fields: dict[str, Any] = field(default_factory=dict)
    conflict_fields: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    recommended_action: str = ""
    status: str = "pending"
    candidate_fingerprint: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "IdentityMatchCandidateRecord":
        return cls(
            id=_optional_int(row["id"]),
            run_id=str(row["run_id"] or ""),
            org_id=str(row["org_id"] or "default"),
            platform_account_id=int(row["platform_account_id"] or 0),
            ad_account_id=_optional_int(row["ad_account_id"]),
            proposed_identity_id=str(row["proposed_identity_id"] or ""),
            result_level=str(row["result_level"] or "manual_confirmation"),
            confidence=int(row["confidence"] or 0),
            matched_rule_ids=[int(value) for value in _json_value(row["matched_rule_ids_json"], [])],
            matched_fields=dict(_json_value(row["matched_fields_json"], {})),
            unmatched_fields=dict(_json_value(row["unmatched_fields_json"], {})),
            conflict_fields=dict(_json_value(row["conflict_fields_json"], {})),
            evidence=dict(_json_value(row["evidence_json"], {})),
            recommended_action=str(row["recommended_action"] or ""),
            status=str(row["status"] or "pending"),
            candidate_fingerprint=str(row["candidate_fingerprint"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class IdentityMatchDecisionRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    candidate_id: int = 0
    decision: str = ""
    reason: str = ""
    candidate_fingerprint: str = ""
    request_id: str = ""
    decision_payload: dict[str, Any] = field(default_factory=dict)
    resulting_identity_id: str = ""
    resulting_link_ids: list[int] = field(default_factory=list)
    decided_by: str = ""
    created_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "IdentityMatchDecisionRecord":
        return cls(
            id=_optional_int(row["id"]),
            org_id=str(row["org_id"] or "default"),
            candidate_id=int(row["candidate_id"] or 0),
            decision=str(row["decision"] or ""),
            reason=str(row["reason"] or ""),
            candidate_fingerprint=str(row["candidate_fingerprint"] or ""),
            request_id=str(row["request_id"] or ""),
            decision_payload=dict(_json_value(row["decision_payload_json"], {})),
            resulting_identity_id=str(row["resulting_identity_id"] or ""),
            resulting_link_ids=[
                int(value) for value in _json_value(row["resulting_link_ids_json"], [])
            ],
            decided_by=str(row["decided_by"] or ""),
            created_at=str(row["created_at"] or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class FieldAuthorityRuleRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    field_name: str = ""
    source_provider: str = "*"
    source_priority: int = 100
    sync_direction: str = "source_to_ad"
    sync_mode: str = "replace"
    prevent_loop: bool = True
    is_enabled: bool = True
    rule_revision: int = 1
    notes: str = ""
    created_by: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "FieldAuthorityRuleRecord":
        return cls(
            id=_optional_int(row["id"]),
            org_id=str(row["org_id"] or "default"),
            field_name=str(row["field_name"] or ""),
            source_provider=str(row["source_provider"] or "*"),
            source_priority=int(row["source_priority"] or 100),
            sync_direction=str(row["sync_direction"] or "source_to_ad"),
            sync_mode=str(row["sync_mode"] or "replace"),
            prevent_loop=bool(row["prevent_loop"]),
            is_enabled=bool(row["is_enabled"]),
            rule_revision=int(row["rule_revision"] or 1),
            notes=str(row["notes"] or ""),
            created_by=str(row["created_by"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


__all__ = [
    "EnterpriseIdentityRecord",
    "PlatformAccountRecord",
    "ADAccountRecord",
    "IdentityAccountLinkRecord",
    "IdentityMatchRuleRecord",
    "IdentityMatchCandidateRecord",
    "FieldAuthorityRuleRecord",
]
