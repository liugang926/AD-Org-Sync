from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

from sync_app.core.models.base import MappingLikeModel


@dataclass(slots=True)
class WebAdminUserRecord(MappingLikeModel):
    id: Optional[int] = None
    username: str = ""
    password_hash: str = ""
    role: str = "super_admin"
    is_enabled: bool = True
    must_change_password: bool = False
    created_at: str = ""
    updated_at: str = ""
    last_login_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "WebAdminUserRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            username=str(row["username"] or ""),
            password_hash=str(row["password_hash"] or ""),
            role=str(row["role"] or "super_admin"),
            is_enabled=bool(row["is_enabled"]),
            must_change_password=bool(row["must_change_password"]),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
            last_login_at=str(row["last_login_at"] or ""),
        )

@dataclass(slots=True)
class WebAuditLogRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = ""
    actor_username: str = ""
    action_type: str = ""
    target_type: str = ""
    target_id: str = ""
    result: str = ""
    message: str = ""
    payload: Optional[Dict[str, Any]] = None
    created_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "WebAuditLogRecord":
        payload = row["payload_json"] if "payload_json" in row.keys() else None
        if isinstance(payload, str) and payload:
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {"raw": payload}
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or ""),
            actor_username=str(row["actor_username"] or ""),
            action_type=str(row["action_type"] or ""),
            target_type=str(row["target_type"] or ""),
            target_id=str(row["target_id"] or ""),
            result=str(row["result"] or ""),
            message=str(row["message"] or ""),
            payload=payload if isinstance(payload, dict) or payload is None else {"raw": payload},
            created_at=str(row["created_at"] or ""),
        )
