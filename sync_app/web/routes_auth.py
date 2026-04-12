from __future__ import annotations

from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse


def register_auth_routes(
    app: FastAPI,
    *,
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_client_ip: Callable[[Request], str],
    get_current_user: Callable[[Request], Any],
    hash_password: Callable[[str], str],
    normalize_role: Callable[..., str],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    rotate_csrf_token: Callable[[dict[str, Any]], str],
    validate_admin_password: Callable[[Request, str], str | None],
    verify_password: Callable[[str, str], bool],
) -> None:
    @app.get("/setup", response_class=HTMLResponse)
    def setup_page(request: Request):
        if request.app.state.user_repo.has_any_user():
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
        if request.app.state.user_repo.has_any_user():
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

        request.app.state.user_repo.create_user(
            username=username,
            password_hash=hash_password(password),
            role="super_admin",
            is_enabled=True,
        )
        request.app.state.audit_repo.add_log(
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
        if not request.app.state.user_repo.has_any_user():
            return RedirectResponse(url="/setup", status_code=303)
        if get_current_user(request):
            return RedirectResponse(url="/dashboard", status_code=303)
        return render(
            request,
            "login.html",
            title="Sign In",
            page="login",
            lightweight_shell=True,
        )

    @app.post("/login")
    def login_submit(
        request: Request,
        csrf_token: str = Form(""),
        username: str = Form(...),
        password: str = Form(...),
    ):
        if not request.app.state.user_repo.has_any_user():
            return RedirectResponse(url="/setup", status_code=303)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/login")
        if csrf_error:
            return csrf_error

        login_name = username.strip()
        client_ip = get_client_ip(request)
        is_locked, retry_after = request.app.state.login_rate_limiter.check(login_name, client_ip)
        if is_locked:
            request.app.state.audit_repo.add_log(
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

        user = request.app.state.user_repo.get_user_record_by_username(login_name)
        if not user or not user.is_enabled or not verify_password(password, user.password_hash):
            locked_now, retry_after = request.app.state.login_rate_limiter.record_failure(login_name, client_ip)
            request.app.state.audit_repo.add_log(
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

        request.session.clear()
        request.session["username"] = user.username
        request.session["role"] = normalize_role(user.role, default="operator")
        rotate_csrf_token(request.session)
        request.app.state.login_rate_limiter.clear(user.username, client_ip)
        request.app.state.user_repo.update_last_login(user.username)
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="auth.login",
            target_type="web_admin_user",
            target_id=user.username,
            result="success",
            message="Sign-in succeeded",
            payload={"ip": client_ip},
        )
        return RedirectResponse(url="/dashboard", status_code=303)

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
        request.app.state.audit_repo.add_log(
            actor_username=username,
            action_type="auth.logout",
            target_type="web_admin_user",
            target_id=username,
            result="success",
            message="Signed out",
        )
        return RedirectResponse(url="/login", status_code=303)
