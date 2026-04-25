from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Protocol

from sync_app.modules.sspr.domain import (
    SSPRVerificationRequest,
    SSPRVerificationResult,
    SSPRVerificationSession,
    SSPRVerifiedIdentity,
)
from sync_app.modules.sspr.rate_limit import SSPRRateLimiter


class SSPRIdentityVerifier(Protocol):
    def verify(self, request: SSPRVerificationRequest) -> SSPRVerifiedIdentity | None:
        ...


class SourceProviderSSPRVerifier:
    def __init__(self, *, source_provider_resolver: Callable[[SSPRVerificationRequest], Any]) -> None:
        self.source_provider_resolver = source_provider_resolver

    def verify(self, request: SSPRVerificationRequest) -> SSPRVerifiedIdentity | None:
        provider = self.source_provider_resolver(request)
        try:
            verify_fn = getattr(provider, "verify_employee_identity", None)
            if not callable(verify_fn):
                verify_fn = getattr(provider, "verify_sspr_identity", None)
            if not callable(verify_fn):
                raise NotImplementedError("source provider does not support SSPR employee verification")
            raw_identity = verify_fn(request)
            return _coerce_verified_identity(raw_identity, request)
        finally:
            close_fn = getattr(provider, "close", None)
            if callable(close_fn):
                try:
                    close_fn()
                except Exception:
                    pass


class InMemorySSPRSessionStore:
    def __init__(
        self,
        *,
        now_factory: Callable[[], datetime] | None = None,
        token_factory: Callable[[], str] | None = None,
    ) -> None:
        self.now_factory = now_factory or (lambda: datetime.now(timezone.utc))
        self.token_factory = token_factory or (lambda: secrets.token_urlsafe(32))
        self._sessions: dict[str, SSPRVerificationSession] = {}

    def create_session(
        self,
        identity: SSPRVerifiedIdentity,
        *,
        request_ip: str = "",
        ttl_seconds: int = 600,
    ) -> SSPRVerificationSession:
        now = self.now_factory()
        session = SSPRVerificationSession(
            session_id=self.token_factory(),
            org_id=_normalize_org_id(identity.org_id),
            source_user_id=str(identity.source_user_id or "").strip(),
            provider_id=str(identity.provider_id or "wecom").strip().lower() or "wecom",
            issued_at=now,
            expires_at=now + timedelta(seconds=max(int(ttl_seconds or 1), 1)),
            request_ip=str(request_ip or "").strip(),
        )
        self._sessions[session.session_id] = session
        return session

    def validate_session(
        self,
        session_id: str,
        *,
        org_id: str,
        source_user_id: str,
        request_ip: str = "",
    ) -> SSPRVerificationSession | None:
        session = self._sessions.get(str(session_id or "").strip())
        if not session or session.is_expired(self.now_factory()):
            return None
        if session.org_id != _normalize_org_id(org_id):
            return None
        if session.source_user_id.lower() != str(source_user_id or "").strip().lower():
            return None
        normalized_ip = str(request_ip or "").strip()
        if normalized_ip and session.request_ip and normalized_ip != session.request_ip:
            return None
        return session

    def invalidate(self, session_id: str) -> None:
        self._sessions.pop(str(session_id or "").strip(), None)


class SSPRVerificationService:
    def __init__(
        self,
        *,
        identity_verifier: SSPRIdentityVerifier,
        session_store: InMemorySSPRSessionStore,
        audit_repo: Any | None = None,
        rate_limiter: SSPRRateLimiter | None = None,
        session_ttl_seconds: int = 600,
    ) -> None:
        self.identity_verifier = identity_verifier
        self.session_store = session_store
        self.audit_repo = audit_repo
        self.rate_limiter = rate_limiter or SSPRRateLimiter()
        self.session_ttl_seconds = max(int(session_ttl_seconds or 1), 1)

    def verify_employee(self, request: SSPRVerificationRequest) -> SSPRVerificationResult:
        org_id = _normalize_org_id(request.org_id)
        source_user_id = str(request.source_user_id or "").strip()
        request_ip = str(request.request_ip or "").strip()
        if not source_user_id:
            return self._result(
                request,
                status="invalid_request",
                message="source user id is required",
                org_id=org_id,
                source_user_id=source_user_id,
                audit_result="failure",
            )

        rate_decision = self.rate_limiter.check(
            org_id=org_id,
            source_user_id=source_user_id,
            request_ip=request_ip,
        )
        if rate_decision.limited:
            return self._result(
                request,
                status="rate_limited",
                message="too many failed verification attempts",
                org_id=org_id,
                source_user_id=source_user_id,
                retry_after_seconds=rate_decision.retry_after_seconds,
                audit_result="failure",
            )

        try:
            identity = self.identity_verifier.verify(request)
        except NotImplementedError as exc:
            self.rate_limiter.record_failure(org_id=org_id, source_user_id=source_user_id, request_ip=request_ip)
            return self._result(
                request,
                status="unsupported",
                message=str(exc),
                org_id=org_id,
                source_user_id=source_user_id,
                audit_result="failure",
            )
        except Exception as exc:
            self.rate_limiter.record_failure(org_id=org_id, source_user_id=source_user_id, request_ip=request_ip)
            return self._result(
                request,
                status="failed",
                message=f"employee verification failed: {exc}",
                org_id=org_id,
                source_user_id=source_user_id,
                audit_result="failure",
            )

        if not identity or identity.source_user_id.lower() != source_user_id.lower():
            limit = self.rate_limiter.record_failure(
                org_id=org_id,
                source_user_id=source_user_id,
                request_ip=request_ip,
            )
            return self._result(
                request,
                status="rate_limited" if limit.limited else "failed",
                message="employee verification failed",
                org_id=org_id,
                source_user_id=source_user_id,
                retry_after_seconds=limit.retry_after_seconds,
                audit_result="failure",
            )

        self.rate_limiter.clear(org_id=org_id, source_user_id=source_user_id, request_ip=request_ip)
        session = self.session_store.create_session(
            identity,
            request_ip=request_ip,
            ttl_seconds=self.session_ttl_seconds,
        )
        return self._result(
            request,
            status="verified",
            message="employee verification succeeded",
            org_id=org_id,
            source_user_id=source_user_id,
            session=session,
            audit_result="success",
        )

    def _result(
        self,
        request: SSPRVerificationRequest,
        *,
        status: str,
        message: str,
        org_id: str,
        source_user_id: str,
        session: SSPRVerificationSession | None = None,
        retry_after_seconds: int = 0,
        audit_result: str,
    ) -> SSPRVerificationResult:
        if self.audit_repo is not None:
            self.audit_repo.add_log(
                org_id=org_id,
                actor_username=source_user_id or "sspr",
                action_type="sspr.verify",
                target_type="source_user",
                target_id=source_user_id,
                result=audit_result,
                message=message,
                payload={
                    "provider_id": str(request.provider_id or "wecom"),
                    "request_ip": str(request.request_ip or ""),
                    "rate_limited": status == "rate_limited",
                    "retry_after_seconds": int(retry_after_seconds or 0),
                },
            )
        return SSPRVerificationResult(
            status=status,
            message=message,
            org_id=org_id,
            source_user_id=source_user_id,
            session=session,
            retry_after_seconds=int(retry_after_seconds or 0),
        )


def _coerce_verified_identity(
    value: Any,
    request: SSPRVerificationRequest,
) -> SSPRVerifiedIdentity | None:
    if value is None or value is False:
        return None
    if isinstance(value, SSPRVerifiedIdentity):
        return value
    if isinstance(value, dict):
        source_user_id = (
            value.get("source_user_id")
            or value.get("userid")
            or value.get("userId")
            or request.source_user_id
        )
        return SSPRVerifiedIdentity(
            org_id=_normalize_org_id(str(value.get("org_id") or request.org_id)),
            source_user_id=str(source_user_id or "").strip(),
            provider_id=str(value.get("provider_id") or request.provider_id or "wecom").strip().lower() or "wecom",
            display_name=str(value.get("display_name") or value.get("name") or ""),
            raw_claims=dict(value),
        )
    source_user_id = getattr(value, "source_user_id", None) or getattr(value, "userid", None)
    if source_user_id:
        return SSPRVerifiedIdentity(
            org_id=_normalize_org_id(request.org_id),
            source_user_id=str(source_user_id or "").strip(),
            provider_id=str(request.provider_id or "wecom").strip().lower() or "wecom",
            display_name=str(getattr(value, "name", "") or ""),
            raw_claims=dict(getattr(value, "raw_payload", {}) or {}),
        )
    return None


def _normalize_org_id(value: str | None) -> str:
    return str(value or "").strip().lower() or "default"
