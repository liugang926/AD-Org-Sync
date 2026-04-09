from __future__ import annotations

from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.web.authz import WEB_ADMIN_ROLES, normalize_role


def register_admin_routes(
    app: FastAPI,
    *,
    fetch_page: Callable[..., Any],
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    hash_password: Callable[[str], str],
    parse_page_number: Callable[[str | None, int], int],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    resolve_remembered_filters: Callable[..., dict[str, Any]],
    validate_admin_password: Callable[[Request, str], str | None],
    verify_password: Callable[[str, str], bool],
) -> None:
    @app.get("/account", response_class=HTMLResponse)
    def account_page(request: Request):
        user = require_capability(request, "account.manage")
        if isinstance(user, RedirectResponse):
            return user
        return render(request, "account.html", page="account", title="My Account")

    @app.post("/account/password")
    def change_password(
        request: Request,
        csrf_token: str = Form(""),
        current_password: str = Form(...),
        new_password: str = Form(...),
        confirm_password: str = Form(...),
    ):
        user = require_capability(request, "account.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/account")
        if csrf_error:
            return csrf_error

        if not verify_password(current_password, user.password_hash):
            flash(request, "error", "Current password is incorrect")
            return RedirectResponse(url="/account", status_code=303)
        if new_password != confirm_password:
            flash(request, "error", "New passwords do not match")
            return RedirectResponse(url="/account", status_code=303)
        password_error = validate_admin_password(request, new_password)
        if password_error:
            flash(request, "error", password_error)
            return RedirectResponse(url="/account", status_code=303)

        request.app.state.user_repo.set_password(user.username, hash_password(new_password))
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="account.password_change",
            target_type="web_admin_user",
            target_id=user.username,
            result="success",
            message="Changed account password",
        )
        flash(request, "success", "Password updated")
        return RedirectResponse(url="/account", status_code=303)

    @app.get("/users", response_class=HTMLResponse)
    def users_page(request: Request):
        user = require_capability(request, "users.manage")
        if isinstance(user, RedirectResponse):
            return user
        return render(
            request,
            "users.html",
            page="users",
            title="Admin Users",
            users=request.app.state.user_repo.list_user_records(),
        )

    @app.post("/users")
    def create_user(
        request: Request,
        csrf_token: str = Form(""),
        username: str = Form(...),
        password: str = Form(...),
        role: str = Form("operator"),
    ):
        user = require_capability(request, "users.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/users")
        if csrf_error:
            return csrf_error

        username = username.strip()
        role = normalize_role(role, default="operator")
        if role not in WEB_ADMIN_ROLES:
            role = "operator"
        if not username:
            flash(request, "error", "Username is required")
            return RedirectResponse(url="/users", status_code=303)
        password_error = validate_admin_password(request, password)
        if password_error:
            flash(request, "error", password_error)
            return RedirectResponse(url="/users", status_code=303)
        if request.app.state.user_repo.get_user_record_by_username(username):
            flash(request, "error", "Username already exists")
            return RedirectResponse(url="/users", status_code=303)

        request.app.state.user_repo.create_user(username, hash_password(password), role=role)
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="user.create",
            target_type="web_admin_user",
            target_id=username,
            result="success",
            message="Created local administrator account",
            payload={"role": role},
        )
        flash_t(request, "success", "User {username} created", username=username)
        return RedirectResponse(url="/users", status_code=303)

    @app.post("/users/{user_id}/toggle")
    def toggle_user(
        request: Request,
        user_id: int,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "users.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/users")
        if csrf_error:
            return csrf_error

        target = request.app.state.user_repo.get_user_record_by_id(user_id)
        if not target:
            flash(request, "error", "Target account was not found")
            return RedirectResponse(url="/users", status_code=303)
        if target.username == user.username and target.is_enabled:
            flash(request, "error", "You cannot disable the account currently signed in")
            return RedirectResponse(url="/users", status_code=303)

        new_state = not target.is_enabled
        request.app.state.user_repo.set_enabled(user_id, new_state)
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="user.toggle",
            target_type="web_admin_user",
            target_id=target.username,
            result="success",
            message=f"{'Enabled' if new_state else 'Disabled'} local administrator account",
        )
        flash_t(
            request,
            "success",
            "User {username} enabled" if new_state else "User {username} disabled",
            username=target.username,
        )
        return RedirectResponse(url="/users", status_code=303)

    @app.get("/audit", response_class=HTMLResponse)
    def audit_page(request: Request):
        user = require_capability(request, "audit.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        remembered_filters = resolve_remembered_filters(
            request,
            page_name="audit",
            defaults={"q": ""},
        )
        audit_query = str(remembered_filters["q"])
        return render(
            request,
            "audit.html",
            page="audit",
            title="Audit Logs",
            logs=(audit_result := fetch_page(
                lambda *, limit, offset: request.app.state.audit_repo.list_recent_logs_page(
                    limit=limit,
                    offset=offset,
                    query=audit_query,
                    org_id=current_org.org_id,
                    include_global=True,
                ),
                page=parse_page_number(request.query_params.get("page_number"), 1),
                page_size=50,
            ))[0],
            audit_query=audit_query,
            audit_page_data=audit_result[1],
            filters_are_remembered=True,
        )
