from __future__ import annotations

from typing import Any, Callable

from sync_app.core.models import UserIdentityBindingRecord
from sync_app.modules.sspr.domain import SSPRPasswordResetRequest, SSPRPasswordResetResult


TargetProviderResolver = Callable[[UserIdentityBindingRecord], Any]


class SSPRService:
    def __init__(
        self,
        *,
        binding_repo: Any,
        audit_repo: Any,
        target_provider_resolver: TargetProviderResolver,
        session_store: Any | None = None,
        require_verified_session: bool = False,
    ) -> None:
        self.binding_repo = binding_repo
        self.audit_repo = audit_repo
        self.target_provider_resolver = target_provider_resolver
        self.session_store = session_store
        self.require_verified_session = bool(require_verified_session)

    def reset_password(self, request: SSPRPasswordResetRequest) -> SSPRPasswordResetResult:
        org_id = _normalize_org_id(request.org_id)
        source_user_id = str(request.source_user_id or "").strip()
        actor_username = str(request.actor_username or "").strip() or source_user_id or "sspr"
        if not source_user_id:
            return self._failure(
                request,
                status="invalid_request",
                message="source user id is required",
                org_id=org_id,
                actor_username=actor_username,
            )
        if not str(request.new_password or ""):
            return self._failure(
                request,
                status="invalid_request",
                message="new password is required",
                org_id=org_id,
                source_user_id=source_user_id,
                actor_username=actor_username,
            )
        if self.require_verified_session:
            if self.session_store is None:
                return self._failure(
                    request,
                    status="invalid_request",
                    message="verified session store is not configured",
                    org_id=org_id,
                    source_user_id=source_user_id,
                    actor_username=actor_username,
                )
            session = self.session_store.validate_session(
                request.verification_session_id,
                org_id=org_id,
                source_user_id=source_user_id,
                request_ip=request.request_ip,
            )
            if session is None:
                return self._failure(
                    request,
                    status="invalid_session",
                    message="valid employee verification session is required",
                    org_id=org_id,
                    source_user_id=source_user_id,
                    actor_username=actor_username,
                )

        binding = self.binding_repo.get_binding_record_by_source_user_id(
            source_user_id,
            org_id=org_id,
        )
        if not binding or not binding.is_enabled or not binding.ad_username:
            return self._failure(
                request,
                status="not_found",
                message="enabled identity binding was not found",
                org_id=org_id,
                source_user_id=source_user_id,
                actor_username=actor_username,
            )

        try:
            target_provider = self.target_provider_resolver(binding)
        except Exception as exc:
            return self._failure(
                request,
                status="failed",
                message=f"target provider resolution failed: {exc}",
                org_id=org_id,
                source_user_id=source_user_id,
                actor_username=actor_username,
                binding=binding,
            )

        reset_fn = getattr(target_provider, "reset_user_password", None)
        unlock_fn = getattr(target_provider, "unlock_user", None)
        if not callable(reset_fn) or (request.unlock_account and not callable(unlock_fn)):
            return self._failure(
                request,
                status="unsupported",
                message="target provider does not support requested SSPR capability",
                org_id=org_id,
                source_user_id=source_user_id,
                actor_username=actor_username,
                binding=binding,
            )

        try:
            reset_ok = bool(
                reset_fn(
                    binding.ad_username,
                    request.new_password,
                    force_change_at_next_login=bool(request.force_change_at_next_login),
                )
            )
            unlock_ok = True
            if request.unlock_account:
                unlock_ok = bool(unlock_fn(binding.ad_username))
        except NotImplementedError:
            return self._failure(
                request,
                status="unsupported",
                message="target provider does not support requested SSPR capability",
                org_id=org_id,
                source_user_id=source_user_id,
                actor_username=actor_username,
                binding=binding,
            )
        except Exception as exc:
            return self._failure(
                request,
                status="failed",
                message=f"password reset failed: {exc}",
                org_id=org_id,
                source_user_id=source_user_id,
                actor_username=actor_username,
                binding=binding,
            )
        finally:
            close_fn = getattr(target_provider, "close", None)
            if callable(close_fn):
                try:
                    close_fn()
                except Exception:
                    pass

        if not reset_ok or not unlock_ok:
            return self._failure(
                request,
                status="failed",
                message="target provider rejected password reset",
                org_id=org_id,
                source_user_id=source_user_id,
                actor_username=actor_username,
                binding=binding,
            )

        audit_log_id = self._audit(
            request,
            org_id=org_id,
            actor_username=actor_username,
            result="success",
            message="SSPR password reset completed",
            binding=binding,
        )
        return SSPRPasswordResetResult(
            status="succeeded",
            message="password reset completed",
            org_id=org_id,
            source_user_id=source_user_id,
            ad_username=binding.ad_username,
            audit_log_id=audit_log_id,
            payload={"unlock_account": bool(request.unlock_account)},
        )

    def _failure(
        self,
        request: SSPRPasswordResetRequest,
        *,
        status: str,
        message: str,
        org_id: str,
        actor_username: str,
        source_user_id: str = "",
        binding: UserIdentityBindingRecord | None = None,
    ) -> SSPRPasswordResetResult:
        audit_log_id = self._audit(
            request,
            org_id=org_id,
            actor_username=actor_username,
            result="failure",
            message=message,
            binding=binding,
            source_user_id=source_user_id,
        )
        return SSPRPasswordResetResult(
            status=status,
            message=message,
            org_id=org_id,
            source_user_id=source_user_id,
            ad_username=binding.ad_username if binding else "",
            audit_log_id=audit_log_id,
        )

    def _audit(
        self,
        request: SSPRPasswordResetRequest,
        *,
        org_id: str,
        actor_username: str,
        result: str,
        message: str,
        binding: UserIdentityBindingRecord | None = None,
        source_user_id: str = "",
    ) -> int:
        payload = {
            "source_user_id": binding.source_user_id if binding else source_user_id,
            "connector_id": binding.connector_id if binding else "",
            "request_ip": str(request.request_ip or ""),
            "has_verification_session": bool(request.verification_session_id),
            "unlock_account": bool(request.unlock_account),
            "force_change_at_next_login": bool(request.force_change_at_next_login),
        }
        return int(
            self.audit_repo.add_log(
                org_id=org_id,
                actor_username=actor_username,
                action_type="sspr.password_reset",
                target_type="ad_user" if binding else "source_user",
                target_id=binding.ad_username if binding else source_user_id,
                result=result,
                message=message,
                payload=payload,
            )
        )


def _normalize_org_id(value: str | None) -> str:
    return str(value or "").strip().lower() or "default"
