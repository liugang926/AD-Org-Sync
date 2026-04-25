from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class SSPRPasswordResetRequest:
    org_id: str
    source_user_id: str
    new_password: str
    actor_username: str = ""
    request_ip: str = ""
    unlock_account: bool = False
    force_change_at_next_login: bool = False


@dataclass(frozen=True, slots=True)
class SSPRPasswordResetResult:
    status: str
    message: str
    org_id: str = "default"
    source_user_id: str = ""
    ad_username: str = ""
    audit_log_id: int | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == "succeeded"
