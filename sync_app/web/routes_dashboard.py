from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse


def register_dashboard_routes(
    app: FastAPI,
    *,
    advanced_nav_pages: set[str],
    build_dashboard_data: Callable[[Request], dict[str, Any]],
    build_getting_started_view_state: Callable[..., Any],
    build_preflight_snapshot: Callable[..., dict[str, Any]],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    get_ui_mode: Callable[[Request], str],
    load_config_summary: Callable[..., tuple[Any, Any, Any]],
    merge_saved_preflight_snapshot_data: Callable[..., dict[str, Any]],
    normalize_ui_mode: Callable[[str | None], str],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    require_user: Callable[[Request], Any],
    safe_redirect_target: Callable[[str | None, str], str],
    source_provider_label: Callable[[str], str],
) -> None:
    def _advanced_page_from_url(url: str) -> str:
        path = str(url or "").split("?", 1)[0].strip("/")
        return path.split("/", 1)[0].strip()

    def _basic_mode_return_url(url: str) -> str:
        page = _advanced_page_from_url(url)
        if page not in advanced_nav_pages:
            return url
        return "/config" if page == "advanced-sync" else "/dashboard"

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard(request: Request):
        user = require_capability(request, "dashboard.read")
        if isinstance(user, RedirectResponse):
            return user
        dashboard_data = build_dashboard_data(request)
        return render(
            request,
            "dashboard.html",
            page="dashboard",
            title="Dashboard",
            dashboard=SimpleNamespace(**dashboard_data),
            **dashboard_data,
        )

    @app.get("/getting-started", response_class=HTMLResponse)
    def getting_started_page(request: Request):
        user = require_capability(request, "dashboard.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        current_config, validation_errors, security_warnings = load_config_summary(current_org)
        preflight_snapshot = merge_saved_preflight_snapshot_data(
            request.session.get("_preflight_snapshot"),
            build_preflight_snapshot(
                request,
                include_live=False,
                current_org=current_org,
                config=current_config,
                validation_errors=validation_errors,
                security_warnings=security_warnings,
            ),
        )
        return render(
            request,
            "getting_started.html",
            page="getting-started",
            title="Getting Started",
            preflight_summary=preflight_snapshot,
            getting_started=build_getting_started_view_state(
                current_org_name=current_org.name,
                preflight_snapshot=preflight_snapshot,
                source_provider_name=source_provider_label(
                    current_config.source_provider if current_config else "wecom"
                ),
                ui_mode=get_ui_mode(request),
            ),
        )

    @app.post("/preflight/run")
    def run_preflight(
        request: Request,
        csrf_token: str = Form(""),
        return_url: str = Form("/dashboard"),
    ):
        user = require_capability(request, "dashboard.read")
        if isinstance(user, RedirectResponse):
            return user
        fallback_url = safe_redirect_target(return_url, "/dashboard")
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        snapshot = build_preflight_snapshot(request, include_live=True)
        request.session["_preflight_snapshot"] = snapshot
        flash_t(
            request,
            "success"
            if snapshot["overall_status"] == "success"
            else ("warning" if snapshot["overall_status"] == "warning" else "error"),
            "Preflight finished with status {status}",
            status=str(snapshot["overall_status"]).upper(),
        )
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/ui-mode")
    def switch_ui_mode(
        request: Request,
        csrf_token: str = Form(""),
        ui_mode: str = Form("basic"),
        return_url: str = Form("/dashboard"),
    ):
        user = require_user(request)
        if isinstance(user, RedirectResponse):
            return user
        fallback_url = safe_redirect_target(return_url, "/dashboard")
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        next_ui_mode = normalize_ui_mode(ui_mode)
        request.session["ui_mode"] = next_ui_mode
        if next_ui_mode == "basic":
            fallback_url = _basic_mode_return_url(fallback_url)
        return RedirectResponse(url=fallback_url, status_code=303)
