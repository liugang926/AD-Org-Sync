from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True, slots=True)
class SSPRVerificationRequest:
    org_id: str
    source_user_id: str
    provider_id: str = "wecom"
    verification_code: str = ""
    state: str = ""
    request_ip: str = ""
    user_agent: str = ""


@dataclass(frozen=True, slots=True)
class SSPRVerifiedIdentity:
    org_id: str
    source_user_id: str
    provider_id: str = "wecom"
    display_name: str = ""
    raw_claims: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SSPRVerificationSession:
    session_id: str
    org_id: str
    source_user_id: str
    provider_id: str
    issued_at: datetime
    expires_at: datetime
    request_ip: str = ""

    def is_expired(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(timezone.utc)
        return current >= self.expires_at


@dataclass(frozen=True, slots=True)
class SSPRVerificationResult:
    status: str
    message: str
    org_id: str = "default"
    source_user_id: str = ""
    session: SSPRVerificationSession | None = None
    retry_after_seconds: int = 0
    payload: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == "verified"


@dataclass(frozen=True, slots=True)
class SSPRPasswordResetRequest:
    org_id: str
    source_user_id: str
    new_password: str
    actor_username: str = ""
    request_ip: str = ""
    verification_session_id: str = ""
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
