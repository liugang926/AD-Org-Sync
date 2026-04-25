from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

from sync_app.core.models.base import MappingLikeModel


@dataclass(slots=True)
class SyncConflictRecord(MappingLikeModel):
    id: Optional[int] = None
    job_id: str = ""
    conflict_type: str = ""
    severity: str = "warning"
    status: str = "open"
    source_id: str = ""
    target_key: str = ""
    message: str = ""
    resolution_hint: str = ""
    details: Optional[Dict[str, Any]] = None
    resolution_payload: Optional[Dict[str, Any]] = None
    created_at: str = ""
    resolved_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "SyncConflictRecord":
        details = row["details_json"] if "details_json" in row.keys() else None
        if isinstance(details, str) and details:
            try:
                details = json.loads(details)
            except json.JSONDecodeError:
                details = {"raw": details}
        resolution_payload = row["resolution_payload_json"] if "resolution_payload_json" in row.keys() else None
        if isinstance(resolution_payload, str) and resolution_payload:
            try:
                resolution_payload = json.loads(resolution_payload)
            except json.JSONDecodeError:
                resolution_payload = {"raw": resolution_payload}
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            job_id=str(row["job_id"] or ""),
            conflict_type=str(row["conflict_type"] or ""),
            severity=str(row["severity"] or "warning"),
            status=str(row["status"] or "open"),
            source_id=str(row["source_id"] or ""),
            target_key=str(row["target_key"] or ""),
            message=str(row["message"] or ""),
            resolution_hint=str(row["resolution_hint"] or ""),
            details=details if isinstance(details, dict) or details is None else {"raw": details},
            resolution_payload=(
                resolution_payload
                if isinstance(resolution_payload, dict) or resolution_payload is None
                else {"raw": resolution_payload}
            ),
            created_at=str(row["created_at"] or ""),
            resolved_at=str(row["resolved_at"] or ""),
        )

@dataclass(slots=True)
class SyncExceptionRuleRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    rule_type: str = ""
    match_type: str = ""
    match_value: str = ""
    rule_owner: str = ""
    effective_reason: str = ""
    notes: str = ""
    is_enabled: bool = True
    expires_at: str = ""
    is_once: bool = False
    next_review_at: str = ""
    last_reviewed_at: str = ""
    hit_count: int = 0
    last_hit_at: str = ""
    last_matched_at: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "SyncExceptionRuleRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            rule_type=str(row["rule_type"] or ""),
            match_type=str(row["match_type"] or ""),
            match_value=str(row["match_value"] or ""),
            rule_owner=str(row["rule_owner"] or "") if "rule_owner" in row.keys() else "",
            effective_reason=str(row["effective_reason"] or "") if "effective_reason" in row.keys() else "",
            notes=str(row["notes"] or ""),
            is_enabled=bool(row["is_enabled"]),
            expires_at=str(row["expires_at"] or ""),
            is_once=bool(row["is_once"]) if "is_once" in row.keys() else False,
            next_review_at=str(row["next_review_at"] or "") if "next_review_at" in row.keys() else "",
            last_reviewed_at=str(row["last_reviewed_at"] or "") if "last_reviewed_at" in row.keys() else "",
            hit_count=int(row["hit_count"] or 0) if "hit_count" in row.keys() else 0,
            last_hit_at=str(row["last_hit_at"] or "") if "last_hit_at" in row.keys() else "",
            last_matched_at=str(row["last_matched_at"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

@dataclass(slots=True)
class UserIdentityBindingRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    source_user_id: str = ""
    source_display_name: str = ""
    connector_id: str = "default"
    ad_username: str = ""
    target_object_guid: str = ""
    target_object_dn: str = ""
    managed_username_base: str = ""
    source: str = ""
    rule_owner: str = ""
    effective_reason: str = ""
    notes: str = ""
    is_enabled: bool = True
    next_review_at: str = ""
    last_reviewed_at: str = ""
    hit_count: int = 0
    last_hit_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "UserIdentityBindingRecord":
        source_user_id = (
            row["source_user_id"]
            if "source_user_id" in row.keys()
            else row["wecom_userid"]
        )
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            source_user_id=str(source_user_id or ""),
            source_display_name=str(row["source_display_name"] or "") if "source_display_name" in row.keys() else "",
            connector_id=str(row["connector_id"] or "default"),
            ad_username=str(row["ad_username"] or ""),
            target_object_guid=str(row["target_object_guid"] or "") if "target_object_guid" in row.keys() else "",
            target_object_dn=str(row["target_object_dn"] or "") if "target_object_dn" in row.keys() else "",
            managed_username_base=str(row["managed_username_base"] or "") if "managed_username_base" in row.keys() else "",
            source=str(row["source"] or ""),
            rule_owner=str(row["rule_owner"] or "") if "rule_owner" in row.keys() else "",
            effective_reason=str(row["effective_reason"] or "") if "effective_reason" in row.keys() else "",
            notes=str(row["notes"] or ""),
            is_enabled=bool(row["is_enabled"]),
            next_review_at=str(row["next_review_at"] or "") if "next_review_at" in row.keys() else "",
            last_reviewed_at=str(row["last_reviewed_at"] or "") if "last_reviewed_at" in row.keys() else "",
            hit_count=int(row["hit_count"] or 0) if "hit_count" in row.keys() else 0,
            last_hit_at=str(row["last_hit_at"] or "") if "last_hit_at" in row.keys() else "",
            updated_at=str(row["updated_at"] or ""),
        )

    @property
    def wecom_userid(self) -> str:
        return self.source_user_id

    @wecom_userid.setter
    def wecom_userid(self, value: str) -> None:
        self.source_user_id = str(value or "").strip()

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["wecom_userid"] = self.source_user_id
        return data

@dataclass(slots=True)
class UserDepartmentOverrideRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    source_user_id: str = ""
    primary_department_id: str = ""
    rule_owner: str = ""
    effective_reason: str = ""
    notes: str = ""
    next_review_at: str = ""
    last_reviewed_at: str = ""
    hit_count: int = 0
    last_hit_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "UserDepartmentOverrideRecord":
        source_user_id = (
            row["source_user_id"]
            if "source_user_id" in row.keys()
            else row["wecom_userid"]
        )
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            source_user_id=str(source_user_id or ""),
            primary_department_id=str(row["primary_department_id"] or ""),
            rule_owner=str(row["rule_owner"] or "") if "rule_owner" in row.keys() else "",
            effective_reason=str(row["effective_reason"] or "") if "effective_reason" in row.keys() else "",
            notes=str(row["notes"] or ""),
            next_review_at=str(row["next_review_at"] or "") if "next_review_at" in row.keys() else "",
            last_reviewed_at=str(row["last_reviewed_at"] or "") if "last_reviewed_at" in row.keys() else "",
            hit_count=int(row["hit_count"] or 0) if "hit_count" in row.keys() else 0,
            last_hit_at=str(row["last_hit_at"] or "") if "last_hit_at" in row.keys() else "",
            updated_at=str(row["updated_at"] or ""),
        )

    @property
    def wecom_userid(self) -> str:
        return self.source_user_id

    @wecom_userid.setter
    def wecom_userid(self, value: str) -> None:
        self.source_user_id = str(value or "").strip()

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["wecom_userid"] = self.source_user_id
        return data
