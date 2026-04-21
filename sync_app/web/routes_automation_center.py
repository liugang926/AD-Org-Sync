from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.services.notification_automation_center import build_notification_automation_center_context


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
        return render(
            request,
            "automation_center.html",
            page="automation-center",
            title="Notification And Automation Center",
            current_org=current_org,
            **build_notification_automation_center_context(
                request.app.state.db_manager,
                current_org.org_id,
                config_path=current_org.config_path or request.app.state.config_path,
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
        settings_repo = request.app.state.settings_repo
        normalized_mode = "dry_run" if str(schedule_execution_mode or "").strip().lower() == "dry_run" else "apply"
        settings_repo.set_value(
            "schedule_execution_mode",
            normalized_mode,
            "string",
            org_id=current_org.org_id,
        )
        settings_repo.set_value(
            "ops_notify_dry_run_failure_enabled",
            str(to_bool(ops_notify_dry_run_failure_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        settings_repo.set_value(
            "ops_notify_conflict_backlog_enabled",
            str(to_bool(ops_notify_conflict_backlog_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        settings_repo.set_value(
            "ops_notify_conflict_backlog_threshold",
            str(max(int(ops_notify_conflict_backlog_threshold or 0), 1)),
            "int",
            org_id=current_org.org_id,
        )
        settings_repo.set_value(
            "ops_notify_review_pending_enabled",
            str(to_bool(ops_notify_review_pending_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        settings_repo.set_value(
            "ops_notify_rule_governance_enabled",
            str(to_bool(ops_notify_rule_governance_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        settings_repo.set_value(
            "ops_scheduled_apply_gate_enabled",
            str(to_bool(ops_scheduled_apply_gate_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        settings_repo.set_value(
            "ops_scheduled_apply_max_dry_run_age_hours",
            str(max(int(ops_scheduled_apply_max_dry_run_age_hours or 0), 1)),
            "int",
            org_id=current_org.org_id,
        )
        settings_repo.set_value(
            "ops_scheduled_apply_requires_zero_conflicts",
            str(to_bool(ops_scheduled_apply_requires_zero_conflicts, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        settings_repo.set_value(
            "ops_scheduled_apply_requires_review_approval",
            str(to_bool(ops_scheduled_apply_requires_review_approval, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="automation_center.policy_update",
            target_type="settings",
            target_id="automation_center",
            result="success",
            message="Updated notification and automation policies",
            payload={
                "org_id": current_org.org_id,
                "schedule_execution_mode": normalized_mode,
                "ops_notify_dry_run_failure_enabled": to_bool(ops_notify_dry_run_failure_enabled, False),
                "ops_notify_conflict_backlog_enabled": to_bool(ops_notify_conflict_backlog_enabled, False),
                "ops_notify_conflict_backlog_threshold": max(int(ops_notify_conflict_backlog_threshold or 0), 1),
                "ops_notify_review_pending_enabled": to_bool(ops_notify_review_pending_enabled, False),
                "ops_notify_rule_governance_enabled": to_bool(ops_notify_rule_governance_enabled, False),
                "ops_scheduled_apply_gate_enabled": to_bool(ops_scheduled_apply_gate_enabled, False),
                "ops_scheduled_apply_max_dry_run_age_hours": max(int(ops_scheduled_apply_max_dry_run_age_hours or 0), 1),
                "ops_scheduled_apply_requires_zero_conflicts": to_bool(
                    ops_scheduled_apply_requires_zero_conflicts,
                    False,
                ),
                "ops_scheduled_apply_requires_review_approval": to_bool(
                    ops_scheduled_apply_requires_review_approval,
                    False,
                ),
            },
        )
        flash(request, "success", "Notification and automation policies saved.")
        return RedirectResponse(url="/automation-center", status_code=303)
