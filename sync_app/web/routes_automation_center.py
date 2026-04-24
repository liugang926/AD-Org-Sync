from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.services.notification_automation_center import build_notification_automation_center_context
from sync_app.services.typed_settings import NotificationAutomationPolicySettings
from sync_app.web.app_state import get_web_repositories, get_web_runtime_state


def register_automation_center_routes(
    app: FastAPI,
    *,
    flash: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    to_bool: Callable[[Optional[str], bool], bool],
) -> None:
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
            title="Notification And Automation Center",
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
