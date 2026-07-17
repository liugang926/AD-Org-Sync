from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from itsdangerous import BadData, URLSafeTimedSerializer

from sync_app.web.app_state import get_web_repositories, get_web_runtime_state
from sync_app.web.oidc import OIDCError, OIDCService


RECENT_LOGIN_COOKIE = "ad_org_sync_recent_login"
RECENT_LOGIN_MAX_AGE = 90 * 24 * 60 * 60


def register_auth_routes(
    app: FastAPI,
    *,
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_client_ip: Callable[[Request], str],
    get_current_user: Callable[[Request], Any],
    hash_password: Callable[[str], str],
    normalize_role: Callable[..., str],
    oidc_service: OIDCService,
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    rotate_csrf_token: Callable[[dict[str, Any]], str],
    validate_admin_password: Callable[[Request, str], str | None],
    verify_password: Callable[[str, str], bool],
) -> None:
    def recent_login_serializer(request: Request) -> URLSafeTimedSerializer:
        return URLSafeTimedSerializer(
            get_web_runtime_state(request).session_secret,
            salt="recent-browser-login",
        )

    def read_recent_login(request: Request) -> dict[str, Any] | None:
        signed_value = str(request.cookies.get(RECENT_LOGIN_COOKIE) or "")
        if not signed_value:
            return None
        try:
            payload = recent_login_serializer(request).loads(
                signed_value,
                max_age=RECENT_LOGIN_MAX_AGE,
            )
        except BadData:
            return None
        return dict(payload) if isinstance(payload, dict) else None

    def attach_recent_login_cookie(
        request: Request,
        response: RedirectResponse,
        *,
        method: str,
        mfa_satisfied: bool,
    ) -> RedirectResponse:
        payload = {
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "method": method,
            "mfa_satisfied": bool(mfa_satisfied),
        }
        response.set_cookie(
            RECENT_LOGIN_COOKIE,
            recent_login_serializer(request).dumps(payload),
            max_age=RECENT_LOGIN_MAX_AGE,
            httponly=True,
            secure=get_web_runtime_state(request).session_cookie_secure,
            samesite="strict",
        )
        return response

    def complete_login(
        request: Request,
        *,
        user: Any,
        method: str,
        mfa_satisfied: bool,
        audit_payload: dict[str, Any],
    ) -> RedirectResponse:
        repositories = get_web_repositories(request)
        request.session.clear()
        request.session["username"] = user.username
        request.session["role"] = normalize_role(user.role, default="operator")
        request.session["auth_method"] = method
        request.session["mfa_satisfied"] = bool(mfa_satisfied)
        rotate_csrf_token(request.session)
        repositories.user_repo.update_last_login(user.username)
        repositories.audit_repo.add_log(
            actor_username=user.username,
            action_type="auth.login",
            target_type="web_admin_user",
            target_id=user.username,
            result="success",
            message="Sign-in succeeded",
            payload={"method": method, "mfa_satisfied": bool(mfa_satisfied), **audit_payload},
        )
        return attach_recent_login_cookie(
            request,
            RedirectResponse(url="/dashboard", status_code=303),
            method=method,
            mfa_satisfied=mfa_satisfied,
        )

    @app.get("/setup", response_class=HTMLResponse)
    def setup_page(request: Request):
        if get_web_repositories(request).user_repo.has_any_user():
            return RedirectResponse(url="/login", status_code=303)
        return render(
            request,
            "setup.html",
            title="Initial Administrator Setup",
            page="setup",
            lightweight_shell=True,
        )

    @app.post("/setup")
    def setup_submit(
        request: Request,
        csrf_token: str = Form(""),
        username: str = Form(...),
        password: str = Form(...),
        confirm_password: str = Form(...),
    ):
        repositories = get_web_repositories(request)
        if repositories.user_repo.has_any_user():
            return RedirectResponse(url="/login", status_code=303)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/setup")
        if csrf_error:
            return csrf_error

        username = username.strip()
        if not username:
            flash(request, "error", "Administrator username is required")
            return RedirectResponse(url="/setup", status_code=303)
        if password != confirm_password:
            flash(request, "error", "Passwords do not match")
            return RedirectResponse(url="/setup", status_code=303)
        password_error = validate_admin_password(request, password)
        if password_error:
            flash(request, "error", password_error)
            return RedirectResponse(url="/setup", status_code=303)

        repositories.user_repo.create_user(
            username=username,
            password_hash=hash_password(password),
            role="super_admin",
            is_enabled=True,
        )
        repositories.audit_repo.add_log(
            actor_username=username,
            action_type="auth.setup",
            target_type="web_admin_user",
            target_id=username,
            result="success",
            message="Completed initial administrator setup",
        )
        flash(request, "success", "Setup completed. Please sign in.")
        return RedirectResponse(url="/login", status_code=303)

    @app.get("/login", response_class=HTMLResponse)
    def login_page(request: Request):
        if not get_web_repositories(request).user_repo.has_any_user():
            return RedirectResponse(url="/setup", status_code=303)
        if get_current_user(request):
            return RedirectResponse(url="/dashboard", status_code=303)
        password_reset_url = ""
        reset_url_error = False
        try:
            password_reset_url = oidc_service.password_reset_url()
        except OIDCError:
            reset_url_error = True
        return render(
            request,
            "login.html",
            title="Sign In",
            page="login",
            lightweight_shell=True,
            login_capabilities={
                "environment_label": oidc_service.settings.environment_label,
                "sso_enabled": oidc_service.settings.configured,
                "sso_name": oidc_service.settings.display_name,
                "sso_configuration_error": oidc_service.settings.configuration_error,
                "mfa_required": oidc_service.settings.mfa_required,
                "password_reset_url": password_reset_url,
                "password_reset_url_error": reset_url_error,
                "recent_login": read_recent_login(request),
            },
        )

    @app.get("/auth/oidc/start")
    def oidc_start_legacy(request: Request):
        return RedirectResponse(url="/login", status_code=303)

    @app.post("/auth/oidc/start")
    def oidc_start(
        request: Request,
        csrf_token: str = Form(""),
    ):
        if get_current_user(request):
            return RedirectResponse(url="/dashboard", status_code=303)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/login")
        if csrf_error:
            return csrf_error
        if not oidc_service.settings.configured:
            flash_t(request, "error", "Single sign-on is not configured.")
            return RedirectResponse(url="/login", status_code=303)
        try:
            public_base_url = str(
                get_web_runtime_state(request).web_runtime_settings.get("public_base_url") or ""
            ).rstrip("/")
            default_callback_url = (
                f"{public_base_url}/auth/oidc/callback"
                if public_base_url
                else str(request.url_for("oidc_callback"))
            )
            callback_url = oidc_service.resolve_callback_url(default_callback_url)
            authorization_url, transaction = oidc_service.begin(redirect_uri=callback_url)
        except OIDCError:
            flash_t(request, "error", "Single sign-on is temporarily unavailable.")
            return RedirectResponse(url="/login", status_code=303)
        request.session["_oidc_transaction"] = transaction
        return RedirectResponse(url=authorization_url, status_code=303)

    @app.get("/auth/oidc/callback", name="oidc_callback")
    def oidc_callback(request: Request):
        response = render(
            request,
            "oidc_callback.html",
            title="Completing Single Sign-On",
            page="oidc-callback",
            lightweight_shell=True,
            oidc_query={
                "code": str(request.query_params.get("code") or ""),
                "state": str(request.query_params.get("state") or ""),
                "error": str(request.query_params.get("error") or ""),
                "error_description": str(
                    request.query_params.get("error_description") or ""
                ),
            },
        )
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    @app.post("/auth/oidc/callback")
    def oidc_callback_submit(
        request: Request,
        csrf_token: str = Form(""),
        code: str = Form(""),
        state: str = Form(""),
        error: str = Form(""),
        error_description: str = Form(""),
    ):
        csrf_error = reject_invalid_csrf(request, csrf_token, "/login")
        if csrf_error:
            return csrf_error
        transaction = request.session.pop("_oidc_transaction", None)
        if not isinstance(transaction, dict):
            flash_t(request, "error", "Single sign-on session expired. Try again.")
            return RedirectResponse(url="/login", status_code=303)
        try:
            identity = oidc_service.finish(
                query={
                    "code": str(code or ""),
                    "state": str(state or ""),
                    "error": str(error or ""),
                    "error_description": str(error_description or ""),
                },
                transaction={key: str(value) for key, value in transaction.items()},
            )
        except OIDCError as exc:
            get_web_repositories(request).audit_repo.add_log(
                actor_username=None,
                action_type="auth.login",
                target_type="web_admin_user",
                target_id="",
                result="error",
                message="Single sign-on failed",
                payload={"reason": str(exc), "ip": get_client_ip(request)},
            )
            flash_t(request, "error", "Single sign-on failed. Contact your administrator.")
            return RedirectResponse(url="/login", status_code=303)
        user = get_web_repositories(request).user_repo.get_user_record_by_username(identity.username)
        if not user or not user.is_enabled:
            get_web_repositories(request).audit_repo.add_log(
                actor_username=identity.username,
                action_type="auth.login",
                target_type="web_admin_user",
                target_id=identity.username,
                result="error",
                message="Single sign-on user is not authorized",
                payload={"issuer": identity.issuer, "subject": identity.subject, "ip": get_client_ip(request)},
            )
            flash_t(request, "error", "Your SSO account is not authorized for this console.")
            return RedirectResponse(url="/login", status_code=303)
        return complete_login(
            request,
            user=user,
            method="oidc",
            mfa_satisfied=identity.mfa_satisfied,
            audit_payload={
                "issuer": identity.issuer,
                "subject": identity.subject,
                "ip": get_client_ip(request),
            },
        )

    @app.post("/login")
    def login_submit(
        request: Request,
        csrf_token: str = Form(""),
        username: str = Form(...),
        password: str = Form(...),
    ):
        repositories = get_web_repositories(request)
        runtime_state = get_web_runtime_state(request)
        if not repositories.user_repo.has_any_user():
            return RedirectResponse(url="/setup", status_code=303)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/login")
        if csrf_error:
            return csrf_error

        login_name = username.strip()
        client_ip = get_client_ip(request)
        is_locked, retry_after = runtime_state.login_rate_limiter.check(login_name, client_ip)
        if is_locked:
            repositories.audit_repo.add_log(
                actor_username=login_name or None,
                action_type="auth.login",
                target_type="web_admin_user",
                target_id=login_name,
                result="error",
                message="Sign-in blocked by rate limiter",
                payload={"ip": client_ip, "rate_limited": True, "retry_after_seconds": retry_after},
            )
            flash_t(
                request,
                "error",
                "Too many failed login attempts. Retry in {retry_after} seconds.",
                retry_after=retry_after,
            )
            return RedirectResponse(url="/login", status_code=303)

        user = repositories.user_repo.get_user_record_by_username(login_name)
        if not user or not user.is_enabled or not verify_password(password, user.password_hash):
            locked_now, retry_after = runtime_state.login_rate_limiter.record_failure(login_name, client_ip)
            repositories.audit_repo.add_log(
                actor_username=login_name or None,
                action_type="auth.login",
                target_type="web_admin_user",
                target_id=login_name,
                result="error",
                message="Sign-in failed",
                payload={"ip": client_ip, "rate_limited": locked_now, "retry_after_seconds": retry_after},
            )
            if locked_now:
                flash_t(
                    request,
                    "error",
                    "Too many failed login attempts. Retry in {retry_after} seconds.",
                    retry_after=retry_after,
                )
            else:
                flash(request, "error", "Invalid username or password")
            return RedirectResponse(url="/login", status_code=303)

        runtime_state.login_rate_limiter.clear(user.username, client_ip)
        return complete_login(
            request,
            user=user,
            method="local_password",
            mfa_satisfied=False,
            audit_payload={"ip": client_ip},
        )

    @app.post("/logout")
    def logout(request: Request, csrf_token: str = Form("")):
        user = get_current_user(request)
        if not user:
            request.session.clear()
            return RedirectResponse(url="/login", status_code=303)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/dashboard")
        if csrf_error:
            return csrf_error

        username = user.username
        request.session.clear()
        get_web_repositories(request).audit_repo.add_log(
            actor_username=username,
            action_type="auth.logout",
            target_type="web_admin_user",
            target_id=username,
            result="success",
            message="Signed out",
        )
        return RedirectResponse(url="/login", status_code=303)
