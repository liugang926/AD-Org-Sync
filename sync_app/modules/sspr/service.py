from __future__ import annotations

import re
from typing import Any, Callable

from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.core.models import UserIdentityBindingRecord
from sync_app.modules.sspr.domain import (
    SSPRAccountResult,
    SSPRPasswordResetRequest,
    SSPRPasswordResetResult,
    SSPRVerificationSession,
)


TargetProviderResolver = Callable[[UserIdentityBindingRecord], Any]
ProtectedAccountChecker = Callable[[UserIdentityBindingRecord], bool]


class SSPRService:
    """Apply a password reset using only server-derived, verified identity context."""

    def __init__(
        self,
        *,
        binding_repo: Any,
        audit_repo: Any,
        target_provider_resolver: TargetProviderResolver,
        session_store: Any | None = None,
        protected_account_checker: ProtectedAccountChecker | None = None,
        require_verified_session: bool = True,
    ) -> None:
        self.binding_repo = binding_repo
        self.audit_repo = audit_repo
        self.target_provider_resolver = target_provider_resolver
        self.session_store = session_store
        self.protected_account_checker = protected_account_checker or (
            lambda binding: is_protected_ad_account_name(binding.ad_username)
        )
        # Kept for constructor compatibility; public SSPR always requires a session.
        self.require_verified_session = bool(require_verified_session)

    def get_account(
        self,
        verification_session_id: str,
        *,
        request_ip: str = "",
        user_agent: str = "",
    ) -> SSPRAccountResult:
        session = self._validate_session(
            verification_session_id,
            request_ip=request_ip,
            user_agent=user_agent,
        )
        if session is None:
            return SSPRAccountResult(status="invalid_session", message="Employee verification has expired.")
        binding = self._binding_for_session(session)
        if binding is None:
            return self._account_result(
                session,
                status="unbound",
                message="The DingTalk account is not bound to an enabled AD account.",
            )
        try:
            protected = self.protected_account_checker(binding)
        except Exception:
            return self._account_result(
                session,
                binding=binding,
                status="directory_unavailable",
                message="The target directory configuration could not be checked.",
            )
        if protected:
            return self._account_result(
                session,
                binding=binding,
                status="protected_account",
                message="This directory account cannot use self-service password reset.",
            )

        target = None
        try:
            target = self.target_provider_resolver(binding)
            state = self._target_account_state(target, binding.ad_username)
        except Exception:
            return self._account_result(
                session,
                binding=binding,
                status="directory_unavailable",
                message="The target directory could not be checked.",
            )
        finally:
            self._close_target(target)

        if not bool(state.get("exists")):
            return self._account_result(
                session,
                binding=binding,
                status="account_not_found",
                message="The bound directory account is unavailable.",
            )
        if state.get("enabled") is False:
            return self._account_result(
                session,
                binding=binding,
                state=state,
                status="account_disabled",
                message="The bound directory account is disabled.",
            )
        return self._account_result(
            session,
            binding=binding,
            state=state,
            status="ready",
            message="The account is ready for password reset.",
        )

    def reset_password(self, request: SSPRPasswordResetRequest) -> SSPRPasswordResetResult:
        session = self._validate_session(
            request.verification_session_id,
            request_ip=request.request_ip,
            user_agent=request.user_agent,
        )
        if session is None:
            return self._failure(
                request,
                status="invalid_session",
                message="Employee verification has expired.",
            )
        binding = self._binding_for_session(session)
        if binding is None:
            return self._failure(
                request,
                session=session,
                status="unbound",
                message="The DingTalk account is not bound to an enabled AD account.",
            )
        try:
            protected = self.protected_account_checker(binding)
        except Exception:
            return self._failure(
                request,
                session=session,
                binding=binding,
                status="directory_unavailable",
                message="The target directory configuration could not be checked.",
            )
        if protected:
            return self._failure(
                request,
                session=session,
                binding=binding,
                status="protected_account",
                message="This directory account cannot use self-service password reset.",
            )

        password_status, password_error = _validate_password(request, session=session, binding=binding)
        if password_status:
            return self._failure(
                request,
                session=session,
                binding=binding,
                status=password_status,
                message=password_error,
            )

        claim = self.session_store.claim_session(
            session.session_id,
            org_id=session.org_id,
            source_user_id=session.source_user_id,
            provider_id=session.provider_id,
            connector_id=session.connector_id,
            user_agent=request.user_agent,
        )
        if claim is None:
            return self._failure(
                request,
                session=session,
                binding=binding,
                status="session_in_use",
                message="This verification session has already been used or is being processed.",
            )
        _claimed_session, claim_token = claim

        target = None
        password_changed = False
        unlock_succeeded = False
        try:
            target = self.target_provider_resolver(binding)
            state = self._target_account_state(target, binding.ad_username)
            if not bool(state.get("exists")):
                self.session_store.release_claim(session.session_id, claim_token)
                return self._failure(
                    request,
                    session=session,
                    binding=binding,
                    status="account_not_found",
                    message="The bound directory account is unavailable.",
                )
            if state.get("enabled") is False:
                self.session_store.release_claim(session.session_id, claim_token)
                return self._failure(
                    request,
                    session=session,
                    binding=binding,
                    status="account_disabled",
                    message="The bound directory account is disabled.",
                )
            reset_fn = getattr(target, "reset_user_password", None)
            if not callable(reset_fn):
                raise NotImplementedError
            password_changed = bool(
                reset_fn(
                    binding.ad_username,
                    request.new_password,
                    force_change_at_next_login=False,
                )
            )
            if not password_changed:
                self.session_store.release_claim(session.session_id, claim_token)
                target_category = str(
                    getattr(target, "last_password_reset_error_category", "") or ""
                ).strip()
                safe_status, safe_message = _target_rejection(target_category)
                return self._failure(
                    request,
                    session=session,
                    binding=binding,
                    status=safe_status,
                    message=safe_message,
                )
            if request.unlock_account:
                unlock_fn = getattr(target, "unlock_user", None)
                unlock_succeeded = bool(callable(unlock_fn) and unlock_fn(binding.ad_username))
        except NotImplementedError:
            self.session_store.release_claim(session.session_id, claim_token)
            return self._failure(
                request,
                session=session,
                binding=binding,
                status="unsupported",
                message="The target directory does not support password reset.",
            )
        except Exception:
            if not password_changed:
                self.session_store.release_claim(session.session_id, claim_token)
            else:
                consumed = self.session_store.consume_claim(session.session_id, claim_token)
                if not consumed:
                    invalidate = getattr(self.session_store, "invalidate", None)
                    if callable(invalidate):
                        invalidate(session.session_id)
            return self._failure(
                request,
                session=session,
                binding=binding,
                status="directory_unavailable" if not password_changed else "succeeded",
                message=(
                    "The target directory could not complete the password change."
                    if not password_changed
                    else "Password changed, but the optional account unlock could not be confirmed."
                ),
                audit_result="failure" if not password_changed else "success",
                payload={
                    "password_changed": password_changed,
                    "unlock_requested": bool(request.unlock_account),
                    "unlock_succeeded": False,
                    "session_consumed": bool(consumed) if password_changed else False,
                },
            )
        finally:
            self._close_target(target)

        consumed = self.session_store.consume_claim(session.session_id, claim_token)
        if not consumed:
            invalidate = getattr(self.session_store, "invalidate", None)
            if callable(invalidate):
                invalidate(session.session_id)
        message = "Password reset completed."
        if request.unlock_account and not unlock_succeeded:
            message = "Password reset completed, but the account could not be unlocked automatically."
        audit_log_id = self._audit(
            request,
            session=session,
            binding=binding,
            result="success",
            message="SSPR password reset completed",
            payload={
                "password_changed": True,
                "unlock_requested": bool(request.unlock_account),
                "unlock_succeeded": bool(unlock_succeeded),
                "session_consumed": bool(consumed),
            },
        )
        return SSPRPasswordResetResult(
            status="succeeded",
            message=message,
            org_id=session.org_id,
            source_user_id=session.source_user_id,
            ad_username=binding.ad_username,
            audit_log_id=audit_log_id,
            payload={
                "unlock_requested": bool(request.unlock_account),
                "unlock_succeeded": bool(unlock_succeeded),
            },
        )

    def _validate_session(
        self,
        session_id: str,
        *,
        request_ip: str,
        user_agent: str,
    ) -> SSPRVerificationSession | None:
        if self.session_store is None or not str(session_id or "").strip():
            return None
        return self.session_store.validate_session(
            session_id,
            request_ip=request_ip,
            user_agent=user_agent,
        )

    def _binding_for_session(self, session: SSPRVerificationSession) -> UserIdentityBindingRecord | None:
        list_fn = getattr(self.binding_repo, "list_binding_records_for_source_identity", None)
        if callable(list_fn):
            records = list_fn(
                session.source_user_id,
                org_id=session.org_id,
                source_provider=session.provider_id,
                connector_id=session.connector_id or None,
                enabled_only=True,
            )
            eligible = [
                item
                for item in records
                if item.is_enabled and str(item.ad_username or "").strip()
            ]
            return eligible[0] if len(eligible) == 1 else None
        get_fn = getattr(self.binding_repo, "get_binding_record_by_source_user_id", None)
        if not callable(get_fn):
            return None
        try:
            binding = get_fn(
                session.source_user_id,
                org_id=session.org_id,
                source_provider=session.provider_id,
                connector_id=session.connector_id or None,
            )
        except TypeError:
            binding = get_fn(session.source_user_id, org_id=session.org_id)
        if binding and binding.is_enabled and str(binding.ad_username or "").strip():
            return binding
        return None

    @staticmethod
    def _target_account_state(target: Any, username: str) -> dict[str, Any]:
        state_fn = getattr(target, "get_user_account_state", None)
        if callable(state_fn):
            state = state_fn(username)
            if not isinstance(state, dict) or state.get("available") is False:
                raise RuntimeError("directory account state unavailable")
            return dict(state)
        details_fn = getattr(target, "get_user_details", None)
        if callable(details_fn):
            details = details_fn(username)
            return {"exists": bool(details), "enabled": True, "locked": None}
        # Compatibility for isolated target doubles; production AD provides state.
        return {"exists": True, "enabled": True, "locked": None}

    @staticmethod
    def _close_target(target: Any) -> None:
        close_fn = getattr(target, "close", None)
        if callable(close_fn):
            try:
                close_fn()
            except Exception:
                return

    def _account_result(
        self,
        session: SSPRVerificationSession,
        *,
        status: str,
        message: str,
        binding: UserIdentityBindingRecord | None = None,
        state: dict[str, Any] | None = None,
    ) -> SSPRAccountResult:
        state = state or {}
        return SSPRAccountResult(
            status=status,
            message=message,
            org_id=session.org_id,
            provider_id=session.provider_id,
            connector_id=session.connector_id,
            source_user_id=session.source_user_id,
            display_name=session.display_name,
            ad_username=binding.ad_username if binding else "",
            directory_domain=str(state.get("domain") or ""),
            account_status="enabled" if state.get("enabled") is True else status,
            locked=state.get("locked") if isinstance(state.get("locked"), bool) else None,
        )

    def _failure(
        self,
        request: SSPRPasswordResetRequest,
        *,
        status: str,
        message: str,
        session: SSPRVerificationSession | None = None,
        binding: UserIdentityBindingRecord | None = None,
        audit_result: str = "failure",
        payload: dict[str, Any] | None = None,
    ) -> SSPRPasswordResetResult:
        audit_log_id = self._audit(
            request,
            session=session,
            binding=binding,
            result=audit_result,
            message=f"SSPR password reset {status}",
            payload=payload,
        )
        return SSPRPasswordResetResult(
            status=status,
            message=message,
            org_id=session.org_id if session else "default",
            source_user_id=session.source_user_id if session else "",
            ad_username=binding.ad_username if binding else "",
            audit_log_id=audit_log_id,
            payload=dict(payload or {}),
        )

    def _audit(
        self,
        request: SSPRPasswordResetRequest,
        *,
        session: SSPRVerificationSession | None,
        binding: UserIdentityBindingRecord | None,
        result: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> int | None:
        if self.audit_repo is None:
            return None
        safe_payload = {
            "provider_id": session.provider_id if session else "",
            "connector_id": session.connector_id if session else "",
            "request_ip": str(request.request_ip or ""),
            "correlation_id": str(request.correlation_id or ""),
            "has_verification_session": bool(request.verification_session_id),
            **dict(payload or {}),
        }
        return int(
            self.audit_repo.add_log(
                org_id=session.org_id if session else "default",
                actor_username=(
                    f"sspr:{session.provider_id}:{session.source_user_id}" if session else "sspr"
                ),
                action_type="sspr.password_reset",
                target_type="ad_user" if binding else "sspr_session",
                target_id=binding.ad_username if binding else "",
                result=result,
                message=message,
                payload=safe_payload,
            )
        )


def _validate_password(
    request: SSPRPasswordResetRequest,
    *,
    session: SSPRVerificationSession,
    binding: UserIdentityBindingRecord,
) -> tuple[str, str]:
    password = str(request.new_password or "")
    if password != str(request.confirm_password or ""):
        return "password_mismatch", "The two password entries do not match."
    minimum = max(int(request.min_password_length or 12), 8)
    if len(password) < minimum:
        return "password_too_short", f"The password must contain at least {minimum} characters."
    classes = (
        any(char.isupper() for char in password),
        any(char.islower() for char in password),
        any(char.isdigit() for char in password),
        any(not char.isalnum() for char in password),
    )
    policy = str(request.password_complexity or "strong").strip().lower()
    required_classes = 4 if policy == "strong" else 3 if policy == "medium" else 2
    if sum(classes) < required_classes:
        return "password_complexity", "The password does not meet the configured complexity policy."
    if request.disallow_identity_fragments:
        password_folded = password.casefold()
        identity_values = (
            binding.ad_username,
            session.source_user_id,
            session.display_name,
        )
        fragments = {
            fragment.casefold()
            for value in identity_values
            for fragment in re.split(r"[^0-9A-Za-z\u4e00-\u9fff]+", str(value or ""))
            if len(fragment) >= 3
        }
        if any(fragment in password_folded for fragment in fragments):
            return "password_identity", "The password must not contain your account name or display name."
    return "", ""


def _target_rejection(category: str) -> tuple[str, str]:
    errors = {
        "password_complexity": (
            "password_complexity",
            "The password does not meet the directory complexity policy.",
        ),
        "password_policy_or_history": (
            "password_policy_or_history",
            "The directory rejected the password because of its policy or password history.",
        ),
        "account_not_found": (
            "account_not_found",
            "The bound directory account is unavailable.",
        ),
        "protected_account": (
            "protected_account",
            "This directory account cannot use self-service password reset.",
        ),
        "directory_unavailable": (
            "directory_unavailable",
            "The directory service is temporarily unavailable.",
        ),
    }
    return errors.get(
        str(category or "").strip(),
        ("directory_rejected", "The directory rejected the password change."),
    )
