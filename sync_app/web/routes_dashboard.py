from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Callable, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.web.app_state import get_web_repositories
from sync_app.web.dashboard_state import (
    compact_preflight_snapshot_for_session,
    count_check_statuses,
    summarize_check_status,
)
from sync_app.web.navigation import CANONICAL_ROUTE_PATHS


def register_dashboard_routes(
    app: FastAPI,
    *,
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
    @app.get(CANONICAL_ROUTE_PATHS["dashboard"], response_class=HTMLResponse)
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
        return_url: str = Form(CANONICAL_ROUTE_PATHS["dashboard"]),
        connection_kind: Optional[str] = Form(None),
    ):
        user = require_capability(request, "dashboard.read")
        if isinstance(user, RedirectResponse):
            return user
        fallback_url = safe_redirect_target(return_url, CANONICAL_ROUTE_PATHS["dashboard"])
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        normalized_connection_kind = str(connection_kind or "all").strip().lower()
        snapshot = build_preflight_snapshot(
            request,
            include_live=True,
            live_check=normalized_connection_kind,
        )
        if normalized_connection_kind in {"source", "ldap"}:
            retained_keys = (
                {"live_ldap"}
                if normalized_connection_kind == "source"
                else {"live_source", "live_wecom"}
            )
            previous_snapshot = request.session.get("_preflight_snapshot")
            if (
                isinstance(previous_snapshot, dict)
                and str(previous_snapshot.get("org_id") or "")
                == str(snapshot.get("org_id") or "")
            ):
                existing_keys = {
                    str(item.get("key") or "")
                    for item in list(snapshot.get("checks") or [])
                    if isinstance(item, dict)
                }
                retained_checks = [
                    item
                    for item in list(previous_snapshot.get("checks") or [])
                    if isinstance(item, dict)
                    and str(item.get("key") or "") in retained_keys
                    and str(item.get("key") or "") not in existing_keys
                ]
                if retained_checks:
                    snapshot["checks"] = list(snapshot.get("checks") or []) + retained_checks
                    snapshot["overall_status"] = summarize_check_status(snapshot["checks"])
                    snapshot["status_counts"] = count_check_statuses(snapshot["checks"])
        request.session["_preflight_snapshot"] = compact_preflight_snapshot_for_session(
            snapshot
        )
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        if normalized_connection_kind in {"all", "source"}:
            current_config, _, _ = load_config_summary(current_org)
            provider_id = str(
                getattr(current_config, "source_provider", "") or "wecom"
            ).strip().lower()
            connector = repositories.source_connector_repo.get_connector(
                f"{provider_id}-default",
                org_id=current_org.org_id,
            )
            source_result = next(
                (
                    item
                    for item in list(snapshot.get("checks") or [])
                    if str(item.get("key") or "") in {"live_source", "live_wecom"}
                ),
                None,
            )
            if connector is not None and source_result is not None:
                source_status = str(source_result.get("status") or "warning")
                repositories.source_connector_repo.update_connection_status(
                    org_id=current_org.org_id,
                    connector_id=connector.connector_id,
                    connection_status=(
                        "connected" if source_status == "success" else "failed"
                    ),
                    granted_permissions=connector.granted_permissions,
                    authorization_scope=connector.authorization_scope,
                    error_summary=(
                        ""
                        if source_status == "success"
                        else str(source_result.get("detail") or "")
                    ),
                )
        if normalized_connection_kind in {"all", "ldap"}:
            ldap_result = next(
                (
                    item
                    for item in list(snapshot.get("checks") or [])
                    if str(item.get("key") or "") == "live_ldap"
                ),
                None,
            )
            if ldap_result is not None:
                ldap_status = str(ldap_result.get("status") or "warning")
                repositories.settings_repo.set_value(
                    "ad_connection_status",
                    "connected" if ldap_status == "success" else "failed",
                    "string",
                    org_id=current_org.org_id,
                )
                repositories.settings_repo.set_value(
                    "ad_connection_tested_at",
                    str(snapshot.get("generated_at") or ""),
                    "string",
                    org_id=current_org.org_id,
                )
        reported_status = str(snapshot.get("overall_status") or "warning")
        if normalized_connection_kind == "source" and source_result is not None:
            reported_status = str(source_result.get("status") or "warning")
        elif normalized_connection_kind == "ldap" and ldap_result is not None:
            reported_status = str(ldap_result.get("status") or "warning")
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="preflight.run",
            target_type="organization",
            target_id=current_org.org_id,
            result=(
                "success"
                if reported_status == "success"
                else (
                    "warning"
                    if reported_status == "warning"
                    else "error"
                )
            ),
            message="Completed live execution preflight",
            payload={
                "overall_status": snapshot["overall_status"],
                "status_counts": dict(snapshot.get("status_counts") or {}),
                "generated_at": str(snapshot.get("generated_at") or ""),
            },
        )
        flash_t(
            request,
            "success"
            if reported_status == "success"
            else ("warning" if reported_status == "warning" else "error"),
            "Preflight finished with status {status}",
            status=reported_status.upper(),
        )
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/ui-mode")
    def switch_ui_mode(
        request: Request,
        csrf_token: str = Form(""),
        ui_mode: str = Form("basic"),
        return_url: str = Form(CANONICAL_ROUTE_PATHS["dashboard"]),
    ):
        user = require_user(request)
        if isinstance(user, RedirectResponse):
            return user
        fallback_url = safe_redirect_target(return_url, CANONICAL_ROUTE_PATHS["dashboard"])
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        next_ui_mode = normalize_ui_mode(ui_mode)
        request.session["ui_mode"] = next_ui_mode
        return RedirectResponse(url=fallback_url, status_code=303)
