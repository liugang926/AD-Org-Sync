from __future__ import annotations

import re
from typing import Any, Callable, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.services.notification_automation_center import build_notification_automation_center_context
from sync_app.services.typed_settings import NotificationAutomationPolicySettings
from sync_app.web.app_state import get_web_repositories, get_web_runtime_state
from sync_app.web.navigation import CANONICAL_ROUTE_PATHS


def register_automation_center_routes(
    app: FastAPI,
    *,
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    to_bool: Callable[[Optional[str], bool], bool],
) -> None:
    @app.get(CANONICAL_ROUTE_PATHS["automation-center"], response_class=HTMLResponse)
    @app.get("/automation-center", response_class=HTMLResponse)
    def automation_center_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        runtime_state = get_web_runtime_state(request)
        return render(
            request,
            "automation_center.html",
            page="automation-center",
            title="Automation And Schedules",
            current_org=current_org,
            **build_notification_automation_center_context(
                repositories.db_manager,
                current_org.org_id,
                config_path=current_org.config_path or runtime_state.config_path,
            ),
        )

    @app.post("/automation-center/policies")
    def automation_center_save(
        request: Request,
        csrf_token: str = Form(""),
        schedule_execution_mode: str = Form("apply"),
        ops_notify_dry_run_failure_enabled: Optional[str] = Form(None),
        ops_notify_conflict_backlog_enabled: Optional[str] = Form(None),
        ops_notify_conflict_backlog_threshold: int = Form(5),
        ops_notify_review_pending_enabled: Optional[str] = Form(None),
        ops_notify_rule_governance_enabled: Optional[str] = Form(None),
        ops_scheduled_apply_gate_enabled: Optional[str] = Form(None),
        ops_scheduled_apply_max_dry_run_age_hours: int = Form(24),
        ops_scheduled_apply_requires_zero_conflicts: Optional[str] = Form(None),
        ops_scheduled_apply_requires_review_approval: Optional[str] = Form(None),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/automation-center")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        settings_repo = repositories.settings_repo
        policy_settings = NotificationAutomationPolicySettings.from_mapping(
            {
                "schedule_execution_mode": schedule_execution_mode,
                "notify_dry_run_failure_enabled": to_bool(ops_notify_dry_run_failure_enabled, False),
                "notify_conflict_backlog_enabled": to_bool(ops_notify_conflict_backlog_enabled, False),
                "notify_conflict_backlog_threshold": ops_notify_conflict_backlog_threshold,
                "notify_review_pending_enabled": to_bool(ops_notify_review_pending_enabled, False),
                "notify_rule_governance_enabled": to_bool(ops_notify_rule_governance_enabled, False),
                "scheduled_apply_gate_enabled": to_bool(ops_scheduled_apply_gate_enabled, False),
                "scheduled_apply_max_dry_run_age_hours": ops_scheduled_apply_max_dry_run_age_hours,
                "scheduled_apply_requires_zero_conflicts": to_bool(
                    ops_scheduled_apply_requires_zero_conflicts,
                    False,
                ),
                "scheduled_apply_requires_review_approval": to_bool(
                    ops_scheduled_apply_requires_review_approval,
                    False,
                ),
            }
        )
        policy_settings.persist(settings_repo, org_id=current_org.org_id)
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="automation_center.policy_update",
            target_type="settings",
            target_id="automation_center",
            result="success",
            message="Updated notification and automation policies",
            payload={
                "org_id": current_org.org_id,
                **policy_settings.to_dict(),
            },
        )
        flash(request, "success", "Notification and automation policies saved.")
        return RedirectResponse(url="/automation-center", status_code=303)

    @app.post(f"{CANONICAL_ROUTE_PATHS['automation-center']}/policy")
    def automation_schedule_save(
        request: Request,
        csrf_token: str = Form(""),
        schedule_time: str = Form("03:00"),
        retry_interval: int = Form(60),
        max_retries: int = Form(3),
        schedule_execution_mode: str = Form("dry_run"),
        ops_scheduled_apply_gate_enabled: Optional[str] = Form(None),
        ops_scheduled_apply_max_dry_run_age_hours: int = Form(24),
        ops_scheduled_apply_requires_zero_conflicts: Optional[str] = Form(None),
        ops_scheduled_apply_requires_review_approval: Optional[str] = Form(None),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        return_path = CANONICAL_ROUTE_PATHS["automation-center"]
        csrf_error = reject_invalid_csrf(request, csrf_token, return_path)
        if csrf_error:
            return csrf_error

        normalized_schedule_time = str(schedule_time or "").strip()
        if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", normalized_schedule_time):
            flash_t(request, "error", "Daily schedule time must use 24-hour HH:mm format.")
            return RedirectResponse(url=return_path, status_code=303)

        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        existing = NotificationAutomationPolicySettings.load(
            repositories.settings_repo,
            org_id=current_org.org_id,
        )
        policy_settings = NotificationAutomationPolicySettings.from_mapping(
            {
                **existing.to_dict(),
                "schedule_execution_mode": schedule_execution_mode,
                "scheduled_apply_gate_enabled": to_bool(ops_scheduled_apply_gate_enabled, False),
                "scheduled_apply_max_dry_run_age_hours": ops_scheduled_apply_max_dry_run_age_hours,
                "scheduled_apply_requires_zero_conflicts": to_bool(
                    ops_scheduled_apply_requires_zero_conflicts,
                    False,
                ),
                "scheduled_apply_requires_review_approval": to_bool(
                    ops_scheduled_apply_requires_review_approval,
                    False,
                ),
            }
        )
        policy_settings.persist(repositories.settings_repo, org_id=current_org.org_id)
        raw_config = repositories.org_config_repo.get_raw_config(
            current_org.org_id,
            config_path=current_org.config_path,
        )
        repositories.org_config_repo.save_config(
            current_org.org_id,
            {
                **raw_config,
                "schedule_time": normalized_schedule_time,
                "retry_interval": max(int(retry_interval or 0), 1),
                "max_retries": max(int(max_retries or 0), 0),
            },
            config_path=current_org.config_path,
        )
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="operations_center.automation.update",
            target_type="settings",
            target_id="automation",
            result="success",
            message="Updated automation schedule and unattended apply gate",
            payload={
                "org_id": current_org.org_id,
                "schedule_time": normalized_schedule_time,
                "retry_interval": max(int(retry_interval or 0), 1),
                "max_retries": max(int(max_retries or 0), 0),
                "schedule_execution_mode": policy_settings.schedule_execution_mode,
                "scheduled_apply_gate_enabled": policy_settings.scheduled_apply_gate_enabled,
                "scheduled_apply_max_dry_run_age_hours": policy_settings.scheduled_apply_max_dry_run_age_hours,
                "scheduled_apply_requires_zero_conflicts": policy_settings.scheduled_apply_requires_zero_conflicts,
                "scheduled_apply_requires_review_approval": policy_settings.scheduled_apply_requires_review_approval,
            },
        )
        flash_t(request, "success", "Automation and schedule settings saved.")
        return RedirectResponse(url=return_path, status_code=303)
