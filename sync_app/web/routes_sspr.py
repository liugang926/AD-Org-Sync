from __future__ import annotations

import secrets
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable
from urllib.parse import urlencode

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.modules.sspr import (
    SSPRPasswordResetRequest,
    SSPRService,
    SSPRVerificationRequest,
    SSPRVerificationService,
    SourceProviderSSPRVerifier,
)
from sync_app.providers.source import build_source_provider, normalize_source_provider
from sync_app.providers.target import build_target_provider
from sync_app.services.typed_settings import DirectoryUiSettings, SSPRSettings
from sync_app.web.app_state import get_web_repositories
from sync_app.web.i18n import detect_browser_ui_language, normalize_ui_language, translate


SSPR_SESSION_COOKIE = "ad_org_sync_sspr"
SSPR_CSRF_COOKIE = "ad_org_sync_sspr_csrf"
SSPR_OAUTH_COOKIE = "ad_org_sync_sspr_oauth"
SSPR_RECEIPT_COOKIE = "ad_org_sync_sspr_result"
SSPR_COOKIE_PATH = "/sspr"
OAUTH_TTL_SECONDS = 300


@dataclass(frozen=True, slots=True)
class _SSPROrganizationContext:
    org_id: str
    org_name: str
    corp_id: str
    connector_id: str
    app_config: Any
    settings: SSPRSettings


def register_sspr_routes(
    app: FastAPI,
    *,
    templates: Jinja2Templates,
    app_version: str,
    oauth_store: Any,
    session_store: Any,
    receipt_store: Any,
    verification_rate_limiter: Any,
    reset_rate_limiter: Any,
    get_client_ip: Callable[[Request], str],
) -> None:
    app.state.sspr_oauth_store = oauth_store
    app.state.sspr_session_store = session_store
    app.state.sspr_receipt_store = receipt_store

    def render_page(request: Request, **context: Any) -> HTMLResponse:
        language = _language(request)
        repositories = get_web_repositories(request)
        brand_name = str(
            repositories.settings_repo.get_value("brand_display_name", "AD Org Sync") or "AD Org Sync"
        ).strip()
        payload = {
            "request": request,
            "app_version": app_version,
            "static_asset_version": app_version,
            "ui_language": language,
            "t": partial(translate, language),
            "brand_display_name": brand_name,
            "language_urls": {
                code: request.url.path
                + "?"
                + urlencode(
                    {
                        **{
                            key: value
                            for key, value in request.query_params.items()
                            if key != "lang" and key in {"corpid"}
                        },
                        "lang": code,
                    }
                )
                for code in ("zh-CN", "en")
            },
            **context,
        }
        return templates.TemplateResponse(request, "sspr.html", payload)

    def resolve_context(request: Request, corp_id: str = "") -> _SSPROrganizationContext | None:
        repositories = get_web_repositories(request)
        requested_corp_id = str(corp_id or "").strip()
        candidates: list[_SSPROrganizationContext] = []
        for organization in repositories.organization_repo.list_organization_records(enabled_only=True):
            settings = SSPRSettings.load(repositories.settings_repo, org_id=organization.org_id)
            configured_corp_id = str(settings.dingtalk_corp_id or "").strip()
            if not settings.enabled or not configured_corp_id:
                continue
            if requested_corp_id and not secrets.compare_digest(requested_corp_id, configured_corp_id):
                continue
            try:
                app_config = repositories.org_config_repo.get_app_config(
                    organization.org_id,
                    config_path=organization.config_path,
                )
            except Exception:
                continue
            if normalize_source_provider(app_config.source_provider) != "dingtalk":
                continue
            candidates.append(
                _SSPROrganizationContext(
                    org_id=organization.org_id,
                    org_name=organization.name or organization.org_id,
                    corp_id=configured_corp_id,
                    connector_id="",
                    app_config=app_config,
                    settings=settings,
                )
            )
        return candidates[0] if len(candidates) == 1 else None

    def context_by_transaction(request: Request, transaction: Any) -> _SSPROrganizationContext | None:
        context = resolve_context(request, transaction.corp_id)
        if (
            context is None
            or context.org_id != transaction.org_id
            or transaction.provider_id != "dingtalk"
            or context.connector_id != transaction.connector_id
        ):
            return None
        return context

    def target_config_for_binding(request: Request, binding: Any) -> Any:
        repositories = get_web_repositories(request)
        organization = repositories.organization_repo.get_organization_record(binding.org_id)
        if organization is None or not organization.is_enabled:
            raise RuntimeError("organization unavailable")
        base_config = repositories.org_config_repo.get_app_config(
            binding.org_id,
            config_path=organization.config_path,
        )
        connector_id = str(binding.connector_id or "").strip()
        if connector_id and connector_id != "default":
            connector_config = repositories.connector_repo.get_connector_app_config(
                connector_id,
                base_config=base_config,
                org_id=binding.org_id,
            )
            if connector_config is None:
                raise RuntimeError("connector unavailable")
            return connector_config
        return base_config

    def target_provider_resolver(request: Request, binding: Any) -> Any:
        config = target_config_for_binding(request, binding)
        repositories = get_web_repositories(request)
        directory_settings = DirectoryUiSettings.load(
            repositories.settings_repo,
            org_id=binding.org_id,
        )
        return build_target_provider(
            server=config.ldap.server,
            domain=config.ldap.domain,
            username=config.ldap.username,
            password=config.ldap.password,
            use_ssl=config.ldap.use_ssl,
            port=config.ldap.port,
            exclude_departments=config.exclude_departments,
            exclude_accounts=config.exclude_accounts,
            default_password="",
            force_change_password=False,
            password_complexity=config.account.password_complexity,
            validate_cert=config.ldap.validate_cert,
            ca_cert_path=config.ldap.ca_cert_path,
            disabled_users_ou_name=directory_settings.disabled_users_ou_path,
            custom_group_ou_path=directory_settings.custom_group_ou_path,
            user_root_ou_path=directory_settings.directory_root_ou_path,
        )

    def build_service(request: Request) -> SSPRService:
        repositories = get_web_repositories(request)

        def is_protected(binding: Any) -> bool:
            config = target_config_for_binding(request, binding)
            return is_protected_ad_account_name(binding.ad_username, config.exclude_accounts)

        return SSPRService(
            binding_repo=repositories.user_binding_repo,
            audit_repo=repositories.audit_repo,
            target_provider_resolver=lambda binding: target_provider_resolver(request, binding),
            session_store=session_store,
            protected_account_checker=is_protected,
        )

    def binding_for_session(request: Request, session: Any) -> Any | None:
        records = get_web_repositories(request).user_binding_repo.list_binding_records_for_source_identity(
            session.source_user_id,
            org_id=session.org_id,
            source_provider=session.provider_id,
            connector_id=session.connector_id or None,
            enabled_only=True,
        )
        eligible = [record for record in records if record.is_enabled and str(record.ad_username or "").strip()]
        return eligible[0] if len(eligible) == 1 else None

    def audit_expired_session(request: Request, session_token: str) -> bool:
        record_fn = getattr(session_store, "get_session_record", None)
        record = record_fn(session_token) if callable(record_fn) else None
        if record is None or not record.is_expired():
            return False
        get_web_repositories(request).audit_repo.add_log(
            org_id=record.org_id,
            actor_username=f"sspr:{record.provider_id}:{record.source_user_id}",
            action_type="sspr.session.expired",
            target_type="sspr_session",
            target_id="",
            result="failure",
            message="Employee SSPR session expired",
            payload={
                "provider_id": record.provider_id,
                "connector_id": record.connector_id,
                "request_ip": get_client_ip(request),
                "correlation_id": _correlation_id(request),
            },
        )
        return True

    def render_auth_start(request: Request, corp_id: str = "", reason: str = "") -> Response:
        context = resolve_context(request, corp_id)
        if context is None:
            return render_page(
                request,
                state="error",
                error_category="configuration_error",
                message=translate(_language(request), "Self-service password reset is not enabled or is not uniquely configured."),
            )
        existing_session = session_store.validate_session(
            request.cookies.get(SSPR_SESSION_COOKIE, ""),
            user_agent=request.headers.get("user-agent", ""),
        )
        if existing_session and existing_session.org_id == context.org_id:
            return RedirectResponse(url="/sspr/account", status_code=303)
        app_key = str(context.app_config.source_connector.corpid or "").strip()
        app_secret = str(context.app_config.source_connector.corpsecret or "").strip()
        if not app_key or not app_secret:
            return render_page(
                request,
                state="error",
                error_category="configuration_error",
                message=translate(_language(request), "DingTalk employee verification is not configured."),
            )
        transaction = oauth_store.create_transaction(
            org_id=context.org_id,
            provider_id="dingtalk",
            connector_id=context.connector_id,
            corp_id=context.corp_id,
            return_path="/sspr/account",
            ttl_seconds=OAUTH_TTL_SECONDS,
            request_ip=get_client_ip(request),
            user_agent=request.headers.get("user-agent", ""),
        )
        get_web_repositories(request).audit_repo.add_log(
            org_id=context.org_id,
            actor_username="sspr:anonymous",
            action_type="sspr.oauth.started",
            target_type="source_provider",
            target_id="dingtalk",
            result="success",
            message="Started DingTalk SSPR verification",
            payload={
                "provider_id": "dingtalk",
                "connector_id": context.connector_id,
                "request_ip": get_client_ip(request),
                "correlation_id": _correlation_id(request),
            },
        )
        response = render_page(
            request,
            state="authenticating",
            organization_name=context.org_name,
            dingtalk_corp_id=context.corp_id,
            dingtalk_client_id=app_key,
            oauth_state=transaction.state,
            status_message=(
                translate(
                    _language(request),
                    "Verification session expired. Verifying your DingTalk identity again.",
                )
                if reason == "session_expired"
                else ""
            ),
        )
        response.set_cookie(
            SSPR_OAUTH_COOKIE,
            transaction.state,
            max_age=OAUTH_TTL_SECONDS,
            secure=True,
            httponly=True,
            samesite="lax",
            path="/sspr/auth/dingtalk",
        )
        return response

    @app.get("/sspr", response_class=HTMLResponse)
    def sspr_entry(
        request: Request,
        corpid: str = "",
        logged_out: str = "",
        reason: str = "",
    ):
        if logged_out == "1":
            return render_page(
                request,
                state="logged_out",
                message=translate(_language(request), "Your employee verification session has been cleared."),
            )
        context = resolve_context(request, corpid)
        if context is None:
            return render_page(
                request,
                state="error",
                error_category="disabled",
                message=translate(_language(request), "Self-service password reset is not enabled or is not uniquely configured."),
            )
        session = session_store.validate_session(
            request.cookies.get(SSPR_SESSION_COOKIE, ""),
            user_agent=request.headers.get("user-agent", ""),
        )
        if session and session.org_id == context.org_id:
            return RedirectResponse(url="/sspr/account", status_code=303)
        start_params = {"corpid": context.corp_id, "lang": _language(request)}
        if reason == "session_expired":
            start_params["reason"] = reason
        return render_page(
            request,
            state="initializing",
            oauth_start_url="/sspr/oauth/start?" + urlencode(start_params),
            status_message=(
                translate(
                    _language(request),
                    "Verification session expired. Verifying your DingTalk identity again.",
                )
                if reason == "session_expired"
                else ""
            ),
        )

    @app.get("/sspr/oauth/start", response_class=HTMLResponse)
    def sspr_oauth_start(request: Request, corpid: str = "", reason: str = ""):
        return render_auth_start(request, corpid, reason)

    @app.get("/sspr/callback/dingtalk", response_class=HTMLResponse)
    def sspr_dingtalk_callback(request: Request):
        # requestAuthCode does not require a redirect callback. This safe fallback
        # never accepts auth codes or state in the query string; both are posted.
        return render_auth_start(request)

    @app.post("/sspr/auth/dingtalk")
    async def sspr_dingtalk_auth(request: Request):
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        state = str(payload.get("state") or "").strip() if isinstance(payload, dict) else ""
        auth_code = str(payload.get("authCode") or payload.get("code") or "").strip() if isinstance(payload, dict) else ""
        cookie_state = str(request.cookies.get(SSPR_OAUTH_COOKIE) or "").strip()
        if not state or not cookie_state or not secrets.compare_digest(state, cookie_state):
            return JSONResponse(
                {"status": "invalid_state", "message": "The verification request has expired."},
                status_code=403,
            )
        transaction = oauth_store.consume_transaction(
            state,
            user_agent=request.headers.get("user-agent", ""),
        )
        if transaction is None:
            return JSONResponse(
                {"status": "invalid_state", "message": "The verification request has expired."},
                status_code=403,
            )
        context = context_by_transaction(request, transaction)
        if context is None:
            return JSONResponse(
                {"status": "organization_mismatch", "message": "The organization configuration changed. Start again."},
                status_code=403,
            )
        repositories = get_web_repositories(request)
        verification_service = SSPRVerificationService(
            identity_verifier=SourceProviderSSPRVerifier(
                source_provider_resolver=lambda verification_request: build_source_provider(
                    app_config=context.app_config
                )
            ),
            session_store=session_store,
            binding_repo=repositories.user_binding_repo,
            audit_repo=repositories.audit_repo,
            rate_limiter=verification_rate_limiter,
            session_ttl_seconds=context.settings.verification_session_ttl_seconds,
        )
        result = verification_service.verify_employee(
            SSPRVerificationRequest(
                org_id=context.org_id,
                provider_id="dingtalk",
                connector_id=context.connector_id,
                verification_code=auth_code,
                state=state,
                request_ip=get_client_ip(request),
                user_agent=request.headers.get("user-agent", ""),
                correlation_id=_correlation_id(request),
            )
        )
        if not result.ok or result.session is None:
            status_code = 429 if result.status == "rate_limited" else 400
            response = JSONResponse(
                {"status": result.status, "message": result.message, "restartUrl": "/sspr"},
                status_code=status_code,
            )
            if result.retry_after_seconds:
                response.headers["Retry-After"] = str(result.retry_after_seconds)
            response.delete_cookie(SSPR_OAUTH_COOKIE, path="/sspr/auth/dingtalk")
            return response
        response = JSONResponse({"status": result.status, "nextUrl": transaction.return_path})
        response.set_cookie(
            SSPR_SESSION_COOKIE,
            result.session.session_id,
            max_age=context.settings.verification_session_ttl_seconds,
            secure=True,
            httponly=True,
            samesite="lax",
            path=SSPR_COOKIE_PATH,
        )
        response.set_cookie(
            SSPR_CSRF_COOKIE,
            result.session.csrf_token,
            max_age=context.settings.verification_session_ttl_seconds,
            secure=True,
            httponly=True,
            samesite="lax",
            path=SSPR_COOKIE_PATH,
        )
        response.delete_cookie(SSPR_OAUTH_COOKIE, path="/sspr/auth/dingtalk")
        return response

    @app.get("/sspr/account", response_class=HTMLResponse)
    def sspr_account(request: Request, error: str = ""):
        session_token = request.cookies.get(SSPR_SESSION_COOKIE, "")
        csrf_token = request.cookies.get(SSPR_CSRF_COOKIE, "")
        if not session_store.validate_csrf_token(session_token, csrf_token):
            expired = audit_expired_session(request, session_token)
            return _clear_sspr_cookies(
                RedirectResponse(
                    url="/sspr?reason=session_expired" if expired else "/sspr",
                    status_code=303,
                )
            )
        session = session_store.validate_session(
            session_token,
            user_agent=request.headers.get("user-agent", ""),
        )
        if session is None:
            return _clear_sspr_cookies(RedirectResponse(url="/sspr", status_code=303))
        repositories = get_web_repositories(request)
        settings = SSPRSettings.load(repositories.settings_repo, org_id=session.org_id)
        organization = repositories.organization_repo.get_organization_record(session.org_id)
        try:
            source_config = (
                repositories.org_config_repo.get_app_config(
                    session.org_id,
                    config_path=organization.config_path,
                )
                if organization is not None and organization.is_enabled
                else None
            )
        except Exception:
            source_config = None
        if (
            not settings.enabled
            or source_config is None
            or normalize_source_provider(source_config.source_provider) != "dingtalk"
        ):
            session_store.invalidate(session_token)
            return _clear_sspr_cookies(RedirectResponse(url="/sspr", status_code=303))
        service = build_service(request)
        account = service.get_account(
            session_token,
            request_ip=get_client_ip(request),
            user_agent=request.headers.get("user-agent", ""),
        )
        if account.status == "invalid_session":
            return _clear_sspr_cookies(RedirectResponse(url="/sspr", status_code=303))
        error_message = _reset_error_message(_language(request), error)
        return render_page(
            request,
            state="ready" if account.ok else account.status,
            account=account,
            csrf_token=csrf_token,
            min_password_length=settings.min_password_length,
            unlock_default=settings.unlock_account_default,
            error_category=error if error_message else "",
            message=error_message or account.message,
        )

    @app.post("/sspr/password/reset")
    def sspr_password_reset(
        request: Request,
        csrf_token: str = Form(""),
        new_password: str = Form(""),
        confirm_password: str = Form(""),
        unlock_account: str = Form(""),
    ):
        session_token = request.cookies.get(SSPR_SESSION_COOKIE, "")
        csrf_cookie = request.cookies.get(SSPR_CSRF_COOKIE, "")
        session = session_store.validate_session(
            session_token,
            user_agent=request.headers.get("user-agent", ""),
        )
        if session is None:
            expired = audit_expired_session(request, session_token)
            return _clear_sspr_cookies(
                RedirectResponse(
                    url="/sspr?reason=session_expired" if expired else "/sspr",
                    status_code=303,
                )
            )
        if (
            not csrf_token
            or not csrf_cookie
            or not secrets.compare_digest(csrf_token, csrf_cookie)
            or not session_store.validate_csrf_token(session_token, csrf_token)
        ):
            response = render_page(
                request,
                state="error",
                error_category="csrf_failed",
                message=translate(_language(request), "The security check failed. Verify your identity again."),
            )
            response.status_code = 403
            return response
        repositories = get_web_repositories(request)
        settings = SSPRSettings.load(repositories.settings_repo, org_id=session.org_id)
        organization = repositories.organization_repo.get_organization_record(session.org_id)
        try:
            base_config = (
                repositories.org_config_repo.get_app_config(
                    session.org_id,
                    config_path=organization.config_path,
                )
                if organization is not None and organization.is_enabled
                else None
            )
        except Exception:
            base_config = None
        if (
            not settings.enabled
            or base_config is None
            or normalize_source_provider(base_config.source_provider) != "dingtalk"
        ):
            session_store.invalidate(session_token)
            return _clear_sspr_cookies(RedirectResponse(url="/sspr", status_code=303))
        decision = reset_rate_limiter.check(
            org_id=session.org_id,
            source_user_id=session.source_user_id,
            request_ip=get_client_ip(request),
            provider_id=session.provider_id,
            action="password_reset",
        )
        if decision.limited:
            response = render_page(
                request,
                state="error",
                error_category="rate_limited",
                message=translate(_language(request), "Too many reset attempts. Try again later."),
            )
            response.status_code = 429
            response.headers["Retry-After"] = str(decision.retry_after_seconds)
            return response
        binding = binding_for_session(request, session)
        try:
            config = target_config_for_binding(request, binding) if binding is not None else base_config
        except Exception:
            return RedirectResponse(url="/sspr/account?error=directory_unavailable", status_code=303)
        result = build_service(request).reset_password(
            SSPRPasswordResetRequest(
                verification_session_id=session_token,
                new_password=new_password,
                confirm_password=confirm_password,
                request_ip=get_client_ip(request),
                user_agent=request.headers.get("user-agent", ""),
                correlation_id=_correlation_id(request),
                unlock_account=str(unlock_account or "").lower() in {"1", "true", "yes", "on"},
                force_change_at_next_login=False,
                min_password_length=settings.min_password_length,
                password_complexity=config.account.password_complexity,
                disallow_identity_fragments=True,
            )
        )
        if not result.ok:
            limited = reset_rate_limiter.record_failure(
                org_id=session.org_id,
                source_user_id=session.source_user_id,
                request_ip=get_client_ip(request),
                provider_id=session.provider_id,
                action="password_reset",
            )
            if limited.limited:
                response = render_page(
                    request,
                    state="error",
                    error_category="rate_limited",
                    message=translate(_language(request), "Too many reset attempts. Try again later."),
                )
                response.status_code = 429
                response.headers["Retry-After"] = str(limited.retry_after_seconds)
                return response
            safe_status = result.status if result.status in _RESET_ERROR_KEYS else "directory_unavailable"
            return RedirectResponse(url=f"/sspr/account?error={safe_status}", status_code=303)

        reset_rate_limiter.clear(
            org_id=session.org_id,
            source_user_id=session.source_user_id,
            request_ip=get_client_ip(request),
            provider_id=session.provider_id,
            action="password_reset",
        )
        receipt = receipt_store.create_receipt(
            org_id=result.org_id,
            ad_username=result.ad_username,
            unlock_requested=bool(result.payload.get("unlock_requested")),
            unlock_succeeded=bool(result.payload.get("unlock_succeeded")),
        )
        response = RedirectResponse(url="/sspr/result", status_code=303)
        response.set_cookie(
            SSPR_RECEIPT_COOKIE,
            receipt.token,
            max_age=300,
            secure=True,
            httponly=True,
            samesite="lax",
            path="/sspr/result",
        )
        _clear_verification_cookies(response)
        return response

    @app.get("/sspr/result", response_class=HTMLResponse)
    def sspr_result(request: Request):
        receipt = receipt_store.consume_receipt(request.cookies.get(SSPR_RECEIPT_COOKIE, ""))
        if receipt is None:
            return RedirectResponse(url="/sspr", status_code=303)
        response = render_page(request, state="success", receipt=receipt)
        response.delete_cookie(SSPR_RECEIPT_COOKIE, path="/sspr/result")
        return response

    @app.post("/sspr/logout")
    def sspr_logout(request: Request, csrf_token: str = Form("")):
        session_token = request.cookies.get(SSPR_SESSION_COOKIE, "")
        csrf_cookie = request.cookies.get(SSPR_CSRF_COOKIE, "")
        if (
            not csrf_token
            or not csrf_cookie
            or not secrets.compare_digest(csrf_token, csrf_cookie)
            or not session_store.validate_csrf_token(session_token, csrf_token)
        ):
            return Response(status_code=403)
        session = session_store.validate_session(
            session_token,
            user_agent=request.headers.get("user-agent", ""),
        )
        session_store.invalidate(session_token)
        if session is not None:
            get_web_repositories(request).audit_repo.add_log(
                org_id=session.org_id,
                actor_username=f"sspr:{session.provider_id}:{session.source_user_id}",
                action_type="sspr.session.revoked",
                target_type="sspr_session",
                target_id="",
                result="success",
                message="Employee SSPR session revoked",
                payload={
                    "provider_id": session.provider_id,
                    "connector_id": session.connector_id,
                    "request_ip": get_client_ip(request),
                    "correlation_id": _correlation_id(request),
                },
            )
        return _clear_sspr_cookies(RedirectResponse(url="/sspr?logged_out=1", status_code=303))


def _language(request: Request) -> str:
    requested = request.query_params.get("lang")
    if requested:
        return normalize_ui_language(requested)
    return detect_browser_ui_language(request.headers.get("accept-language"))


def _correlation_id(request: Request) -> str:
    return str(
        getattr(request.state, "correlation_id", "")
        or request.headers.get("x-correlation-id", "")
    ).strip()


def _clear_verification_cookies(response: Response) -> None:
    response.delete_cookie(SSPR_SESSION_COOKIE, path=SSPR_COOKIE_PATH)
    response.delete_cookie(SSPR_CSRF_COOKIE, path=SSPR_COOKIE_PATH)
    response.delete_cookie(SSPR_OAUTH_COOKIE, path="/sspr/auth/dingtalk")


def _clear_sspr_cookies(response: Response) -> Response:
    _clear_verification_cookies(response)
    response.delete_cookie(SSPR_RECEIPT_COOKIE, path="/sspr/result")
    return response


_RESET_ERROR_KEYS = {
    "password_mismatch": "The two password entries do not match.",
    "password_too_short": "The password is shorter than the configured minimum length.",
    "password_complexity": "The password does not meet the configured complexity policy.",
    "password_identity": "The password must not contain your account name or display name.",
    "password_policy_or_history": "The new password conflicts with the directory password policy or recent password history.",
    "invalid_session": "Verification session expired. Verify your identity again.",
    "session_in_use": "This verification session has already been used.",
    "unbound": "The DingTalk account is not bound to an enabled AD account.",
    "protected_account": "This account cannot use self-service password reset.",
    "account_not_found": "The bound directory account is unavailable.",
    "account_disabled": "The bound directory account is disabled.",
    "directory_rejected": "The directory rejected the password change. Check the password policy and history.",
    "directory_unavailable": "The directory service is temporarily unavailable.",
    "unsupported": "The target directory does not support self-service password reset yet.",
}


def _reset_error_message(language: str, category: str) -> str:
    key = _RESET_ERROR_KEYS.get(str(category or "").strip())
    return translate(language, key) if key else ""


__all__ = ["register_sspr_routes"]
