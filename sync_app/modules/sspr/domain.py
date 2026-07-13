from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True, slots=True)
class SSPRVerificationRequest:
    """Untrusted input used to exchange a provider's one-time login code.

    ``source_user_id`` is deliberately absent.  The verified source identity is
    returned by the provider and must never be supplied by the browser.
    """

    org_id: str
    verification_code: str
    provider_id: str = "dingtalk"
    connector_id: str = ""
    expected_source_user_id: str = ""
    state: str = ""
    request_ip: str = ""
    user_agent: str = ""
    correlation_id: str = ""


@dataclass(frozen=True, slots=True)
class SSPRVerifiedIdentity:
    org_id: str
    source_user_id: str
    provider_id: str = "dingtalk"
    connector_id: str = ""
    display_name: str = ""
    raw_claims: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SSPRVerificationSession:
    """A short-lived SSPR session.

    ``session_id`` and ``csrf_token`` are plaintext capabilities returned only
    to the current request.  Persistent stores keep their hashes exclusively.
    """

    session_id: str
    org_id: str
    source_user_id: str
    provider_id: str
    connector_id: str
    issued_at: datetime
    expires_at: datetime
    display_name: str = ""
    request_ip: str = ""
    user_agent_hash: str = ""
    csrf_token: str = ""
    consumed_at: datetime | None = None
    revoked_at: datetime | None = None
    claimed_at: datetime | None = None

    def is_expired(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(timezone.utc)
        return current >= self.expires_at

    def is_active(self, now: datetime | None = None) -> bool:
        return not self.is_expired(now) and not self.consumed_at and not self.revoked_at


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
        return self.status in {"verified", "unbound"}


@dataclass(frozen=True, slots=True)
class SSPRAccountResult:
    status: str
    message: str
    org_id: str = "default"
    provider_id: str = ""
    connector_id: str = ""
    source_user_id: str = ""
    display_name: str = ""
    ad_username: str = ""
    directory_domain: str = ""
    account_status: str = "unknown"
    locked: bool | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == "ready"


@dataclass(frozen=True, slots=True)
class SSPRPasswordResetRequest:
    verification_session_id: str
    new_password: str
    confirm_password: str
    org_id: str = ""
    source_user_id: str = ""
    provider_id: str = ""
    connector_id: str = ""
    actor_username: str = ""
    request_ip: str = ""
    user_agent: str = ""
    correlation_id: str = ""
    unlock_account: bool = False
    force_change_at_next_login: bool = False
    min_password_length: int = 12
    password_complexity: str = "strong"
    disallow_identity_fragments: bool = True


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


@dataclass(frozen=True, slots=True)
class SSPROAuthTransaction:
    state: str
    org_id: str
    provider_id: str
    connector_id: str
    corp_id: str
    return_path: str
    issued_at: datetime
    expires_at: datetime
    request_ip: str = ""
    user_agent_hash: str = ""
    consumed_at: datetime | None = None

    def is_expired(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(timezone.utc)
        return current >= self.expires_at


@dataclass(frozen=True, slots=True)
class SSPRResetReceipt:
    token: str
    org_id: str
    ad_username: str
    completed_at: datetime
    expires_at: datetime
    unlock_requested: bool = False
    unlock_succeeded: bool = False
    consumed_at: datetime | None = None
