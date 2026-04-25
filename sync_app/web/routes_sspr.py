from __future__ import annotations

import base64
import json
from typing import Any, Callable
from urllib.parse import parse_qsl

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse

from sync_app.modules.sspr import (
    SourceProviderSSPRVerifier,
    SSPRPasswordResetRequest,
    SSPRService,
    SSPRVerificationRequest,
    SSPRVerificationResult,
    SSPRVerificationService,
)
from sync_app.web.app_state import get_web_repositories, get_web_runtime_state


ProviderFactory = Callable[..., Any]


def register_sspr_routes(
    app: FastAPI,
    *,
    build_source_provider_fn: ProviderFactory,
    build_target_provider_fn: ProviderFactory,
    flash: Callable[..., None],
    get_client_ip: Callable[[Request], str],
    logger: Any,
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    to_bool: Callable[[str | None, bool], bool],
) -> None:
    def render_sspr_page(
        request: Request,
        *,
        org_id: str = "",
        source_user_id: str = "",
        provider_id: str = "wecom",
        verification_session_id: str = "",
        verified: bool = False,
        completed: bool = False,
    ):
        org = _resolve_public_org(request, org_id)
        return render(
            request,
            "sspr.html",
            title="Employee Password Reset",
            page="sspr",
            lightweight_shell=True,
            org_id=org.org_id,
            source_user_id=str(source_user_id or "").strip(),
            provider_id=str(provider_id or "wecom").strip().lower() or "wecom",
            verification_session_id=str(verification_session_id or "").strip(),
            verified=bool(verified),
            completed=bool(completed),
        )

    def build_source_verifier() -> SourceProviderSSPRVerifier:
        def source_provider_resolver(verification_request: SSPRVerificationRequest):
            repositories = get_web_repositories(app)
            runtime_state = get_web_runtime_state(app)
            org = _resolve_public_org_from_state(repositories, runtime_state, verification_request.org_id)
            config = repositories.org_config_repo.get_app_config(
                org.org_id,
                config_path=org.config_path or runtime_state.config_path,
            )
            return build_source_provider_fn(app_config=config, logger=logger)

        return SourceProviderSSPRVerifier(source_provider_resolver=source_provider_resolver)

    def verify_employee(
        request: Request,
        *,
        org_id: str,
        source_user_id: str,
        provider_id: str,
        verification_code: str,
        state: str = "",
    ) -> tuple[SSPRVerificationRequest, SSPRVerificationResult]:
        runtime_state = get_web_runtime_state(request)
        org = _resolve_public_org(request, org_id)
        verification_request = SSPRVerificationRequest(
            org_id=org.org_id,
            source_user_id=str(source_user_id or "").strip(),
            provider_id=str(provider_id or "wecom").strip().lower() or "wecom",
            verification_code=str(verification_code or "").strip(),
            state=str(state or "").strip(),
            request_ip=get_client_ip(request),
            user_agent=str(request.headers.get("user-agent") or ""),
        )
        verification_service = SSPRVerificationService(
            identity_verifier=build_source_verifier(),
            session_store=runtime_state.sspr_session_store,
            audit_repo=get_web_repositories(request).audit_repo,
            rate_limiter=runtime_state.sspr_rate_limiter,
        )
        return verification_request, verification_service.verify_employee(verification_request)

    def target_provider_resolver(binding: Any):
        repositories = get_web_repositories(app)
        runtime_state = get_web_runtime_state(app)
        org = _resolve_public_org_from_state(repositories, runtime_state, getattr(binding, "org_id", ""))
        base_config = repositories.org_config_repo.get_app_config(
            org.org_id,
            config_path=org.config_path or runtime_state.config_path,
        )
        connector_id = str(getattr(binding, "connector_id", "") or "default").strip() or "default"
        connector_config = base_config
        if connector_id != "default":
            connector_config = repositories.connector_repo.get_connector_app_config(
                connector_id,
                base_config=base_config,
                org_id=org.org_id,
            )
            if connector_config is None:
                raise ValueError(f"connector {connector_id} is not configured")
        return build_target_provider_fn(
            server=connector_config.ldap.server,
            domain=connector_config.ldap.domain,
            username=connector_config.ldap.username,
            password=connector_config.ldap.password,
            use_ssl=connector_config.ldap.use_ssl,
            port=connector_config.ldap.port,
            exclude_departments=connector_config.exclude_departments,
            exclude_accounts=connector_config.exclude_accounts,
            default_password=connector_config.account.default_password,
            force_change_password=connector_config.account.force_change_password,
            password_complexity=connector_config.account.password_complexity,
            validate_cert=connector_config.ldap.validate_cert,
            ca_cert_path=connector_config.ldap.ca_cert_path,
            disabled_users_ou_name="Disabled Users",
            managed_group_type="security",
            managed_group_mail_domain="",
            custom_group_ou_path="Managed Groups",
            user_root_ou_path="",
        )

    @app.get("/sspr", response_class=HTMLResponse)
    def sspr_page(request: Request, org_id: str = ""):
        return render_sspr_page(request, org_id=org_id)

    @app.get("/sspr/callback/{provider_id}", response_class=HTMLResponse)
    def sspr_provider_callback(
        request: Request,
        provider_id: str,
        code: str = "",
        state: str = "",
        org_id: str = "",
        source_user_id: str = "",
    ):
        state_values = _decode_sspr_callback_state(state)
        resolved_org_id = str(org_id or state_values.get("org_id") or "").strip()
        resolved_source_user_id = str(source_user_id or state_values.get("source_user_id") or "").strip()
        normalized_provider_id = str(provider_id or state_values.get("provider_id") or "wecom").strip().lower() or "wecom"
        if not str(code or "").strip():
            flash(request, "error", "Verification callback is missing the OAuth code.")
            return render_sspr_page(
                request,
                org_id=resolved_org_id,
                source_user_id=resolved_source_user_id,
                provider_id=normalized_provider_id,
            )
        if not resolved_source_user_id:
            flash(request, "error", "Verification callback is missing the employee ID.")
            return render_sspr_page(request, org_id=resolved_org_id, provider_id=normalized_provider_id)

        verification_request, result = verify_employee(
            request,
            org_id=resolved_org_id,
            source_user_id=resolved_source_user_id,
            provider_id=normalized_provider_id,
            verification_code=code,
            state=state,
        )
        if result.ok and result.session is not None:
            flash(request, "success", "Employee identity verified. Choose a new password.")
            return render_sspr_page(
                request,
                org_id=result.org_id,
                source_user_id=result.source_user_id,
                provider_id=verification_request.provider_id,
                verification_session_id=result.session.session_id,
                verified=True,
            )

        flash(request, "error", _verification_error_message(result))
        return render_sspr_page(
            request,
            org_id=verification_request.org_id,
            source_user_id=verification_request.source_user_id,
            provider_id=verification_request.provider_id,
        )

    @app.post("/sspr/verify", response_class=HTMLResponse)
    def sspr_verify(
        request: Request,
        csrf_token: str = Form(""),
        org_id: str = Form(""),
        source_user_id: str = Form(""),
        provider_id: str = Form("wecom"),
        verification_code: str = Form(""),
    ):
        csrf_error = reject_invalid_csrf(request, csrf_token, "/sspr")
        if csrf_error:
            return csrf_error

        org = _resolve_public_org(request, org_id)
        verification_request, result = verify_employee(
            request,
            org_id=org.org_id,
            source_user_id=source_user_id,
            provider_id=provider_id,
            verification_code=verification_code,
        )
        if result.ok and result.session is not None:
            flash(request, "success", "Employee identity verified. Choose a new password.")
            return render_sspr_page(
                request,
                org_id=result.org_id,
                source_user_id=result.source_user_id,
                provider_id=verification_request.provider_id,
                verification_session_id=result.session.session_id,
                verified=True,
            )

        flash(request, "error", _verification_error_message(result))
        return render_sspr_page(
            request,
            org_id=org.org_id,
            source_user_id=verification_request.source_user_id,
            provider_id=verification_request.provider_id,
        )

    @app.post("/sspr/reset", response_class=HTMLResponse)
    def sspr_reset(
        request: Request,
        csrf_token: str = Form(""),
        org_id: str = Form(""),
        source_user_id: str = Form(""),
        provider_id: str = Form("wecom"),
        verification_session_id: str = Form(""),
        new_password: str = Form(""),
        confirm_password: str = Form(""),
        unlock_account: str | None = Form(None),
    ):
        csrf_error = reject_invalid_csrf(request, csrf_token, "/sspr")
        if csrf_error:
            return csrf_error

        normalized_source_user_id = str(source_user_id or "").strip()
        normalized_session_id = str(verification_session_id or "").strip()
        if not normalized_session_id:
            flash(request, "error", "Verify your employee identity before resetting the password.")
            return render_sspr_page(request, org_id=org_id, source_user_id=normalized_source_user_id)
        if not str(new_password or ""):
            flash(request, "error", "New password is required.")
            return render_sspr_page(
                request,
                org_id=org_id,
                source_user_id=normalized_source_user_id,
                provider_id=provider_id,
                verification_session_id=normalized_session_id,
                verified=True,
            )
        if new_password != confirm_password:
            flash(request, "error", "New passwords do not match.")
            return render_sspr_page(
                request,
                org_id=org_id,
                source_user_id=normalized_source_user_id,
                provider_id=provider_id,
                verification_session_id=normalized_session_id,
                verified=True,
            )

        runtime_state = get_web_runtime_state(request)
        org = _resolve_public_org(request, org_id)
        repositories = get_web_repositories(request)
        reset_service = SSPRService(
            binding_repo=repositories.user_binding_repo,
            audit_repo=repositories.audit_repo,
            target_provider_resolver=target_provider_resolver,
            session_store=runtime_state.sspr_session_store,
            require_verified_session=True,
        )
        result = reset_service.reset_password(
            SSPRPasswordResetRequest(
                org_id=org.org_id,
                source_user_id=normalized_source_user_id,
                actor_username=normalized_source_user_id or "sspr",
                new_password=new_password,
                request_ip=get_client_ip(request),
                verification_session_id=normalized_session_id,
                unlock_account=to_bool(unlock_account, False),
                force_change_at_next_login=False,
            )
        )
        if result.ok:
            runtime_state.sspr_session_store.invalidate(normalized_session_id)
            flash(request, "success", "Password reset completed.")
            return render_sspr_page(
                request,
                org_id=result.org_id,
                source_user_id=result.source_user_id,
                provider_id=provider_id,
                completed=True,
            )

        flash(request, "error", _reset_error_message(result.status))
        return render_sspr_page(
            request,
            org_id=org.org_id,
            source_user_id=normalized_source_user_id,
            provider_id=provider_id,
            verification_session_id=normalized_session_id if result.status != "invalid_session" else "",
            verified=result.status != "invalid_session",
        )


def _resolve_public_org(request: Request, org_id: str):
    return _resolve_public_org_from_state(
        get_web_repositories(request),
        get_web_runtime_state(request),
        org_id,
    )


def _resolve_public_org_from_state(repositories: Any, runtime_state: Any, org_id: str):
    requested_org_id = str(org_id or "").strip().lower()
    organization = (
        repositories.organization_repo.get_organization_record(requested_org_id)
        if requested_org_id
        else None
    )
    if organization and organization.is_enabled:
        return organization
    return (
        repositories.organization_repo.get_default_organization_record()
        or repositories.organization_repo.ensure_default(config_path=runtime_state.config_path)
    )


def _verification_error_message(result: SSPRVerificationResult) -> str:
    if result.status == "invalid_request":
        return "Employee ID is required."
    if result.status == "rate_limited":
        return "Too many failed verification attempts. Try again later."
    if result.status == "unsupported":
        return "Employee verification is not available for the configured source provider yet."
    return "Employee verification failed."


def _reset_error_message(status: str) -> str:
    if status == "invalid_request":
        return "Reset request is incomplete."
    if status == "invalid_session":
        return "Verification session expired. Verify your identity again."
    if status == "not_found":
        return "No enabled AD binding was found for this employee."
    if status == "unsupported":
        return "The target directory does not support self-service password reset yet."
    return "Password reset could not be completed."


def _decode_sspr_callback_state(value: str) -> dict[str, str]:
    raw_value = str(value or "").strip()
    if not raw_value:
        return {}

    query_values = {
        str(key): str(item)
        for key, item in parse_qsl(raw_value, keep_blank_values=False)
        if str(key).strip()
    }
    if query_values:
        return query_values

    padded_value = raw_value + "=" * (-len(raw_value) % 4)
    try:
        decoded = base64.urlsafe_b64decode(padded_value.encode("utf-8")).decode("utf-8")
        payload = json.loads(decoded)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(key): str(item)
        for key, item in payload.items()
        if item not in (None, "")
    }
