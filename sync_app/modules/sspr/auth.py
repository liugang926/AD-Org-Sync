from __future__ import annotations

import secrets
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Protocol

from sync_app.modules.sspr.domain import (
    SSPRVerificationRequest,
    SSPRVerificationResult,
    SSPRVerificationSession,
    SSPRVerifiedIdentity,
)
from sync_app.modules.sspr.rate_limit import SSPRRateLimiter
from sync_app.modules.sspr.repositories import hash_user_agent


class SSPRIdentityVerifier(Protocol):
    def verify(self, request: SSPRVerificationRequest) -> SSPRVerifiedIdentity | None:
        ...


class SourceProviderSSPRVerifier:
    def __init__(self, *, source_provider_resolver: Callable[[SSPRVerificationRequest], Any]) -> None:
        self.source_provider_resolver = source_provider_resolver

    def verify(self, request: SSPRVerificationRequest) -> SSPRVerifiedIdentity | None:
        provider = self.source_provider_resolver(request)
        verify_fn = getattr(provider, "verify_employee_identity", None)
        if not callable(verify_fn):
            verify_fn = getattr(provider, "verify_sspr_identity", None)
        if not callable(verify_fn):
            raise NotImplementedError("source provider does not support employee verification")
        try:
            raw_identity = verify_fn(request)
        finally:
            close_fn = getattr(provider, "close", None)
            if callable(close_fn):
                close_fn()
        return _coerce_verified_identity(raw_identity, request)


class InMemorySSPRSessionStore:
    """Compatibility store for isolated tests; production Web uses SQLite."""

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
        user_agent: str = "",
        ttl_seconds: int = 600,
    ) -> SSPRVerificationSession:
        now = self.now_factory()
        session = SSPRVerificationSession(
            session_id=self.token_factory(),
            org_id=_normalize_org_id(identity.org_id),
            source_user_id=str(identity.source_user_id or "").strip(),
            provider_id=_normalize_provider_id(identity.provider_id),
            connector_id=str(identity.connector_id or "").strip(),
            display_name=str(identity.display_name or "").strip(),
            issued_at=now,
            expires_at=now + timedelta(seconds=max(int(ttl_seconds or 1), 1)),
            request_ip=str(request_ip or "").strip(),
            user_agent_hash=hash_user_agent(user_agent),
        )
        self._sessions[session.session_id] = session
        return session

    def validate_session(
        self,
        session_id: str,
        *,
        org_id: str = "",
        source_user_id: str = "",
        provider_id: str = "",
        connector_id: str = "",
        request_ip: str = "",
        user_agent: str = "",
    ) -> SSPRVerificationSession | None:
        del request_ip
        session = self._sessions.get(str(session_id or "").strip())
        if not session or not session.is_active(self.now_factory()):
            return None
        if org_id and session.org_id != _normalize_org_id(org_id):
            return None
        if provider_id and session.provider_id != _normalize_provider_id(provider_id):
            return None
        if connector_id and session.connector_id != str(connector_id or "").strip():
            return None
        if source_user_id and session.source_user_id.casefold() != str(source_user_id or "").strip().casefold():
            return None
        current_ua_hash = hash_user_agent(user_agent)
        if session.user_agent_hash and session.user_agent_hash != current_ua_hash:
            return None
        return session

    def claim_session(self, session_id: str, **context: Any) -> tuple[SSPRVerificationSession, str] | None:
        session = self.validate_session(session_id, **context)
        if session is None or session.claimed_at:
            return None
        claim_token = secrets.token_urlsafe(24)
        claimed = replace(session, claimed_at=self.now_factory())
        self._sessions[session_id] = claimed
        return claimed, claim_token

    def consume_claim(self, session_id: str, claim_token: str) -> bool:
        del claim_token
        session = self._sessions.get(session_id)
        if session is None or not session.claimed_at:
            return False
        self._sessions[session_id] = replace(session, consumed_at=self.now_factory(), claimed_at=None)
        return True

    def release_claim(self, session_id: str, claim_token: str) -> bool:
        del claim_token
        session = self._sessions.get(session_id)
        if session is None:
            return False
        self._sessions[session_id] = replace(session, claimed_at=None)
        return True

    def invalidate(self, session_id: str) -> None:
        session = self._sessions.get(str(session_id or "").strip())
        if session is not None:
            self._sessions[session.session_id] = replace(session, revoked_at=self.now_factory(), claimed_at=None)


class SSPRVerificationService:
    def __init__(
        self,
        *,
        identity_verifier: SSPRIdentityVerifier,
        session_store: Any,
        binding_repo: Any | None = None,
        audit_repo: Any | None = None,
        rate_limiter: SSPRRateLimiter | None = None,
        session_ttl_seconds: int = 600,
    ) -> None:
        self.identity_verifier = identity_verifier
        self.session_store = session_store
        self.binding_repo = binding_repo
        self.audit_repo = audit_repo
        self.rate_limiter = rate_limiter or SSPRRateLimiter()
        self.session_ttl_seconds = max(int(session_ttl_seconds or 1), 1)

    def verify_employee(self, request: SSPRVerificationRequest) -> SSPRVerificationResult:
        org_id = _normalize_org_id(request.org_id)
        provider_id = _normalize_provider_id(request.provider_id)
        request_ip = str(request.request_ip or "").strip()
        if not str(request.verification_code or "").strip():
            return self._result(
                request,
                status="invalid_auth_code",
                message="The DingTalk login code is missing or invalid.",
                org_id=org_id,
                source_user_id="",
                audit_result="failure",
                error_category="invalid_auth_code",
            )

        pre_auth_limit = self.rate_limiter.check(
            org_id=org_id,
            request_ip=request_ip,
            provider_id=provider_id,
            action="verify",
        )
        if pre_auth_limit.limited:
            return self._result(
                request,
                status="rate_limited",
                message="Too many verification attempts. Try again later.",
                org_id=org_id,
                source_user_id="",
                retry_after_seconds=pre_auth_limit.retry_after_seconds,
                audit_result="failure",
                error_category="rate_limited",
            )

        try:
            identity = self.identity_verifier.verify(request)
        except NotImplementedError:
            self._record_failure(request, source_user_id="")
            return self._result(
                request,
                status="unsupported",
                message="DingTalk employee verification is not configured.",
                org_id=org_id,
                source_user_id="",
                audit_result="failure",
                error_category="unsupported",
            )
        except Exception as exc:
            decision = self._record_failure(request, source_user_id="")
            category = _safe_error_category(exc)
            return self._result(
                request,
                status="rate_limited" if decision.limited else category,
                message=_verification_error_message(category),
                org_id=org_id,
                source_user_id="",
                retry_after_seconds=decision.retry_after_seconds,
                audit_result="failure",
                error_category=category,
            )

        if not identity or not str(identity.source_user_id or "").strip():
            decision = self._record_failure(request, source_user_id="")
            return self._result(
                request,
                status="rate_limited" if decision.limited else "invalid_response",
                message=_verification_error_message("invalid_response"),
                org_id=org_id,
                source_user_id="",
                retry_after_seconds=decision.retry_after_seconds,
                audit_result="failure",
                error_category="invalid_response",
            )

        source_user_id = str(identity.source_user_id or "").strip()
        if identity.org_id != org_id or _normalize_provider_id(identity.provider_id) != provider_id:
            self._record_failure(request, source_user_id=source_user_id)
            return self._result(
                request,
                status="organization_mismatch",
                message=_verification_error_message("organization_mismatch"),
                org_id=org_id,
                source_user_id=source_user_id,
                audit_result="failure",
                error_category="organization_mismatch",
            )
        expected_source_user_id = str(request.expected_source_user_id or "").strip()
        if expected_source_user_id and source_user_id.casefold() != expected_source_user_id.casefold():
            self._record_failure(request, source_user_id=source_user_id)
            return self._result(
                request,
                status="organization_mismatch",
                message=_verification_error_message("organization_mismatch"),
                org_id=org_id,
                source_user_id=source_user_id,
                audit_result="failure",
                error_category="organization_mismatch",
            )

        user_limit = self.rate_limiter.check(
            org_id=org_id,
            source_user_id=source_user_id,
            request_ip=request_ip,
            provider_id=provider_id,
            action="verify",
        )
        if user_limit.limited:
            return self._result(
                request,
                status="rate_limited",
                message=_verification_error_message("rate_limited"),
                org_id=org_id,
                source_user_id=source_user_id,
                retry_after_seconds=user_limit.retry_after_seconds,
                audit_result="failure",
                error_category="rate_limited",
            )

        binding = self._resolve_binding(identity, requested_connector_id=request.connector_id)
        if binding is not None:
            identity = replace(identity, connector_id=str(binding.connector_id or "").strip())
        elif str(request.connector_id or "").strip():
            identity = replace(identity, connector_id=str(request.connector_id or "").strip())

        self.rate_limiter.clear(
            org_id=org_id,
            request_ip=request_ip,
            provider_id=provider_id,
            action="verify",
        )
        self.rate_limiter.clear(
            org_id=org_id,
            source_user_id=source_user_id,
            request_ip=request_ip,
            provider_id=provider_id,
            action="verify",
        )
        session = self.session_store.create_session(
            identity,
            request_ip=request_ip,
            user_agent=request.user_agent,
            ttl_seconds=self.session_ttl_seconds,
        )
        status = "verified" if binding is not None else "unbound"
        message = (
            "DingTalk employee verification succeeded."
            if binding is not None
            else "The DingTalk account is not bound to an enabled AD account."
        )
        return self._result(
            request,
            status=status,
            message=message,
            org_id=org_id,
            source_user_id=source_user_id,
            session=session,
            audit_result="success",
            error_category="",
            payload={"binding_found": binding is not None},
        )

    def _resolve_binding(self, identity: SSPRVerifiedIdentity, *, requested_connector_id: str) -> Any | None:
        if self.binding_repo is None:
            return None
        list_fn = getattr(self.binding_repo, "list_binding_records_for_source_identity", None)
        if callable(list_fn):
            records = list_fn(
                identity.source_user_id,
                org_id=identity.org_id,
                source_provider=identity.provider_id,
                connector_id=str(requested_connector_id or "").strip() or None,
                enabled_only=True,
            )
            eligible = [item for item in records if item.is_enabled and str(item.ad_username or "").strip()]
            return eligible[0] if len(eligible) == 1 else None
        get_fn = getattr(self.binding_repo, "get_binding_record_by_source_user_id", None)
        if callable(get_fn):
            binding = get_fn(
                identity.source_user_id,
                org_id=identity.org_id,
                source_provider=identity.provider_id,
            )
            if binding and binding.is_enabled and str(binding.ad_username or "").strip():
                return binding
        return None

    def _record_failure(self, request: SSPRVerificationRequest, *, source_user_id: str):
        return self.rate_limiter.record_failure(
            org_id=_normalize_org_id(request.org_id),
            source_user_id=source_user_id,
            request_ip=str(request.request_ip or "").strip(),
            provider_id=_normalize_provider_id(request.provider_id),
            action="verify",
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
        error_category: str,
        payload: dict[str, Any] | None = None,
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
                    "provider_id": _normalize_provider_id(request.provider_id),
                    "connector_id": str(request.connector_id or "").strip(),
                    "source_user_id": source_user_id,
                    "request_ip": str(request.request_ip or ""),
                    "correlation_id": str(request.correlation_id or ""),
                    "error_category": str(error_category or ""),
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
            payload=dict(payload or {}),
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
        source_user_id = value.get("source_user_id") or value.get("userid") or value.get("userId")
        if not source_user_id:
            return None
        safe_claims = {
            key: value.get(key)
            for key in ("userid", "userId", "unionid", "unionId", "name")
            if value.get(key) not in (None, "")
        }
        return SSPRVerifiedIdentity(
            org_id=_normalize_org_id(str(value.get("org_id") or request.org_id)),
            source_user_id=str(source_user_id or "").strip(),
            provider_id=_normalize_provider_id(str(value.get("provider_id") or request.provider_id)),
            connector_id=str(value.get("connector_id") or request.connector_id or "").strip(),
            display_name=str(value.get("display_name") or value.get("name") or "").strip(),
            raw_claims=safe_claims,
        )
    source_user_id = getattr(value, "source_user_id", None) or getattr(value, "userid", None)
    if source_user_id:
        return SSPRVerifiedIdentity(
            org_id=_normalize_org_id(request.org_id),
            source_user_id=str(source_user_id or "").strip(),
            provider_id=_normalize_provider_id(request.provider_id),
            connector_id=str(request.connector_id or "").strip(),
            display_name=str(getattr(value, "name", "") or "").strip(),
            raw_claims={},
        )
    return None


def _safe_error_category(exc: Exception) -> str:
    category = str(getattr(exc, "category", "") or "").strip().lower()
    allowed = {
        "invalid_credentials",
        "invalid_auth_code",
        "expired_auth_code",
        "permission_denied",
        "organization_mismatch",
        "rate_limited",
        "network_error",
        "invalid_response",
    }
    return category if category in allowed else "network_error"


def _verification_error_message(category: str) -> str:
    messages = {
        "invalid_credentials": "DingTalk application credentials are not valid.",
        "invalid_auth_code": "The DingTalk login code is invalid. Start verification again.",
        "expired_auth_code": "The DingTalk login code expired. Start verification again.",
        "permission_denied": "The DingTalk application is not authorized for employee login.",
        "organization_mismatch": "The DingTalk organization does not match this SSPR portal.",
        "rate_limited": "Too many verification attempts. Try again later.",
        "network_error": "DingTalk verification is temporarily unavailable. Try again.",
        "invalid_response": "DingTalk returned an invalid verification response. Try again.",
    }
    return messages.get(str(category or ""), "DingTalk employee verification failed.")


def _normalize_org_id(value: str | None) -> str:
    return str(value or "").strip().lower() or "default"


def _normalize_provider_id(value: str | None) -> str:
    return str(value or "").strip().lower() or "dingtalk"
