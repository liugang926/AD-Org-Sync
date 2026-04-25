from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional

from sync_app.core.models.base import MappingLikeModel


@dataclass(slots=True)
class OffboardingRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    connector_id: str = "default"
    source_user_id: str = ""
    ad_username: str = ""
    status: str = "pending"
    reason: str = ""
    manager_userids: list[str] = field(default_factory=list)
    first_missing_at: str = ""
    due_at: str = ""
    notified_at: str = ""
    last_job_id: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "OffboardingRecord":
        source_user_id = (
            row["source_user_id"]
            if "source_user_id" in row.keys()
            else row["wecom_userid"]
        )
        manager_userids = row["manager_userids_json"] if "manager_userids_json" in row.keys() else None
        if isinstance(manager_userids, str) and manager_userids:
            try:
                manager_userids = json.loads(manager_userids)
            except json.JSONDecodeError:
                manager_userids = [manager_userids]
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            connector_id=str(row["connector_id"] or "default"),
            source_user_id=str(source_user_id or ""),
            ad_username=str(row["ad_username"] or ""),
            status=str(row["status"] or "pending"),
            reason=str(row["reason"] or ""),
            manager_userids=[str(value) for value in (manager_userids or []) if str(value).strip()],
            first_missing_at=str(row["first_missing_at"] or ""),
            due_at=str(row["due_at"] or ""),
            notified_at=str(row["notified_at"] or ""),
            last_job_id=str(row["last_job_id"] or ""),
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
class UserLifecycleRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    lifecycle_type: str = ""
    connector_id: str = "default"
    source_user_id: str = ""
    ad_username: str = ""
    status: str = "pending"
    reason: str = ""
    employment_type: str = ""
    sponsor_userid: str = ""
    manager_userids: list[str] = field(default_factory=list)
    effective_at: str = ""
    notified_at: str = ""
    completed_at: str = ""
    last_job_id: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "UserLifecycleRecord":
        source_user_id = (
            row["source_user_id"]
            if "source_user_id" in row.keys()
            else row["wecom_userid"]
        )
        manager_userids = row["manager_userids_json"] if "manager_userids_json" in row.keys() else None
        payload = row["payload_json"] if "payload_json" in row.keys() else None
        if isinstance(manager_userids, str) and manager_userids:
            try:
                manager_userids = json.loads(manager_userids)
            except json.JSONDecodeError:
                manager_userids = [manager_userids]
        if isinstance(payload, str) and payload:
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {"raw": payload}
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            lifecycle_type=str(row["lifecycle_type"] or ""),
            connector_id=str(row["connector_id"] or "default"),
            source_user_id=str(source_user_id or ""),
            ad_username=str(row["ad_username"] or ""),
            status=str(row["status"] or "pending"),
            reason=str(row["reason"] or ""),
            employment_type=str(row["employment_type"] or ""),
            sponsor_userid=str(row["sponsor_userid"] or ""),
            manager_userids=[str(value) for value in (manager_userids or []) if str(value).strip()],
            effective_at=str(row["effective_at"] or ""),
            notified_at=str(row["notified_at"] or ""),
            completed_at=str(row["completed_at"] or ""),
            last_job_id=str(row["last_job_id"] or ""),
            payload=dict(payload or {}),
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
class CustomManagedGroupBindingRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    connector_id: str = "default"
    source_type: str = ""
    source_key: str = ""
    group_sam: str = ""
    group_dn: str = ""
    group_cn: str = ""
    display_name: str = ""
    status: str = "active"
    last_seen_at: str = ""
    archived_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "CustomManagedGroupBindingRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            connector_id=str(row["connector_id"] or "default"),
            source_type=str(row["source_type"] or ""),
            source_key=str(row["source_key"] or ""),
            group_sam=str(row["group_sam"] or ""),
            group_dn=str(row["group_dn"] or ""),
            group_cn=str(row["group_cn"] or ""),
            display_name=str(row["display_name"] or ""),
            status=str(row["status"] or "active"),
            last_seen_at=str(row["last_seen_at"] or ""),
            archived_at=str(row["archived_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )
