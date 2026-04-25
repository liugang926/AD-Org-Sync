from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sync_app.clients.wechat_bot import mask_webhook_url
from sync_app.core.models import SyncJobRecord, SyncPlanReviewRecord
from sync_app.services.typed_settings import NotificationAutomationPolicySettings
from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories.conflicts import SyncConflictRepository, SyncExceptionRuleRepository, SyncPlanReviewRepository
from sync_app.storage.repositories.jobs import SyncJobRepository
from sync_app.storage.repositories.mappings import UserDepartmentOverrideRepository, UserIdentityBindingRepository
from sync_app.storage.repositories.organizations import OrganizationConfigRepository
from sync_app.storage.repositories.system import SettingsRepository
from sync_app.core.rule_governance import build_rule_governance_summary

GREEN_JOB_STATUSES = {"COMPLETED"}


def _normalize_job_status(value: str | None) -> str:
    return str(value or "").strip().upper()


def _job_mode(job: Optional[SyncJobRecord]) -> str:
    return str(getattr(job, "execution_mode", "") or "").strip().lower()


def _is_successful_dry_run(job: Optional[SyncJobRecord]) -> bool:
    return _job_mode(job) == "dry_run" and _normalize_job_status(getattr(job, "status", "")) in GREEN_JOB_STATUSES


def _is_apply_job(job: Optional[SyncJobRecord]) -> bool:
    return _job_mode(job) == "apply"


def _parse_timestamp(value: str | None) -> Optional[datetime]:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _latest_matching_job(
    recent_jobs: list[SyncJobRecord],
    matcher,
) -> Optional[SyncJobRecord]:
    return next((job for job in recent_jobs if matcher(job)), None)


def build_notification_automation_policy_settings(
    settings_repo: SettingsRepository,
    org_id: str,
) -> dict[str, Any]:
    return NotificationAutomationPolicySettings.load(settings_repo, org_id=org_id).to_dict()


def evaluate_scheduled_apply_readiness(
    *,
    settings_repo: SettingsRepository,
    job_repo: SyncJobRepository,
    conflict_repo: SyncConflictRepository,
    review_repo: SyncPlanReviewRepository,
    org_id: str,
    policy_settings: Optional[dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    normalized_org_id = str(org_id or "").strip().lower() or "default"
    policy = dict(policy_settings or build_notification_automation_policy_settings(settings_repo, normalized_org_id))
    recent_jobs = job_repo.list_recent_job_records(limit=200, org_id=normalized_org_id)
    latest_successful_dry_run = _latest_matching_job(recent_jobs, _is_successful_dry_run)
    most_recent_dry_run = _latest_matching_job(recent_jobs, lambda job: _job_mode(job) == "dry_run")
    latest_apply = _latest_matching_job(recent_jobs, _is_apply_job)
    pending_review_record: Optional[SyncPlanReviewRecord] = None
    review_required = False

    _conflicts, open_conflict_total = conflict_repo.list_conflict_records_page(
        limit=1,
        offset=0,
        status="open",
        org_id=normalized_org_id,
    )
    open_conflict_total = int(open_conflict_total or 0)
    reference_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    reasons: list[str] = []

    if policy["schedule_execution_mode"] != "apply":
        return {
            "mode": policy["schedule_execution_mode"],
            "gate_enabled": bool(policy["scheduled_apply_gate_enabled"]),
            "allowed": False,
            "status": "info",
            "summary": "Scheduled execution is configured for dry run only.",
            "reasons": ["Switch scheduled mode to apply before enabling unattended production runs."],
            "latest_dry_run": most_recent_dry_run,
            "latest_successful_dry_run": latest_successful_dry_run,
            "latest_apply": latest_apply,
            "open_conflict_count": open_conflict_total,
            "review_required": False,
            "review_record": None,
            "dry_run_age_hours": None,
        }

    if not policy["scheduled_apply_gate_enabled"]:
        return {
            "mode": policy["schedule_execution_mode"],
            "gate_enabled": False,
            "allowed": True,
            "status": "warning",
            "summary": "Scheduled apply safety gate is disabled.",
            "reasons": [],
            "latest_dry_run": most_recent_dry_run,
            "latest_successful_dry_run": latest_successful_dry_run,
            "latest_apply": latest_apply,
            "open_conflict_count": open_conflict_total,
            "review_required": False,
            "review_record": None,
            "dry_run_age_hours": None,
        }

    if most_recent_dry_run is not None and not _is_successful_dry_run(most_recent_dry_run):
        reasons.append("Latest dry run did not complete successfully.")
    if latest_successful_dry_run is None:
        reasons.append("No successful dry run has been recorded yet.")

    dry_run_age_hours: float | None = None
    latest_success_started_at = _parse_timestamp(getattr(latest_successful_dry_run, "started_at", ""))
    if latest_success_started_at is not None:
        dry_run_age_hours = max(
            (reference_time - latest_success_started_at).total_seconds() / 3600.0,
            0.0,
        )
        if dry_run_age_hours > float(policy["scheduled_apply_max_dry_run_age_hours"]):
            reasons.append(
                f"Latest successful dry run is older than {int(policy['scheduled_apply_max_dry_run_age_hours'])} hours."
            )

    if policy["scheduled_apply_requires_zero_conflicts"] and open_conflict_total > 0:
        reasons.append(f"{open_conflict_total} open conflict(s) still need review.")

    if latest_successful_dry_run is not None:
        latest_summary = dict(getattr(latest_successful_dry_run, "summary", {}) or {})
        review_required = bool(latest_summary.get("review_required") or False)
        if review_required:
            pending_review_record = review_repo.get_review_record_by_job_id(latest_successful_dry_run.job_id)
            if (
                policy["scheduled_apply_requires_review_approval"]
                and (
                    pending_review_record is None
                    or str(getattr(pending_review_record, "status", "") or "").strip().lower() != "approved"
                )
            ):
                reasons.append("Latest high-risk dry run still needs approval before scheduled apply can continue.")

    allowed = not reasons
    return {
        "mode": policy["schedule_execution_mode"],
        "gate_enabled": True,
        "allowed": allowed,
        "status": "success" if allowed else "error",
        "summary": (
            "Scheduled apply is ready to run."
            if allowed
            else "Scheduled apply is blocked until the safety checks are green."
        ),
        "reasons": reasons,
        "latest_dry_run": most_recent_dry_run,
        "latest_successful_dry_run": latest_successful_dry_run,
        "latest_apply": latest_apply,
        "open_conflict_count": open_conflict_total,
        "review_required": review_required,
        "review_record": pending_review_record,
        "dry_run_age_hours": dry_run_age_hours,
    }


def build_notification_automation_center_context(
    db_manager: DatabaseManager,
    org_id: str,
    *,
    config_path: str = "",
) -> dict[str, Any]:
    normalized_org_id = str(org_id or "").strip().lower() or "default"
    settings_repo = SettingsRepository(db_manager)
    org_config_repo = OrganizationConfigRepository(db_manager)
    job_repo = SyncJobRepository(db_manager)
    conflict_repo = SyncConflictRepository(db_manager)
    review_repo = SyncPlanReviewRepository(db_manager)
    binding_repo = UserIdentityBindingRepository(db_manager)
    override_repo = UserDepartmentOverrideRepository(db_manager)
    exception_repo = SyncExceptionRuleRepository(db_manager)

    policy_settings = build_notification_automation_policy_settings(settings_repo, normalized_org_id)
    app_config = org_config_repo.get_app_config(normalized_org_id, config_path=config_path)
    editable_config = org_config_repo.get_editable_config(normalized_org_id, config_path=config_path)
    pending_review_records = review_repo.list_review_records(status="pending", limit=20, org_id=normalized_org_id)
    governance = build_rule_governance_summary(
        bindings=binding_repo.list_binding_records(org_id=normalized_org_id),
        overrides=override_repo.list_override_records(org_id=normalized_org_id),
        exception_rules=exception_repo.list_rule_records(org_id=normalized_org_id),
    )
    scheduled_apply_guard = evaluate_scheduled_apply_readiness(
        settings_repo=settings_repo,
        job_repo=job_repo,
        conflict_repo=conflict_repo,
        review_repo=review_repo,
        org_id=normalized_org_id,
        policy_settings=policy_settings,
    )

    latest_dry_run = scheduled_apply_guard["latest_dry_run"]
    latest_successful_dry_run = scheduled_apply_guard["latest_successful_dry_run"]
    latest_apply = scheduled_apply_guard["latest_apply"]
    latest_failed_dry_run = latest_dry_run if latest_dry_run is not None and not _is_successful_dry_run(latest_dry_run) else None
    open_conflict_count = int(scheduled_apply_guard["open_conflict_count"] or 0)
    pending_review_count = len(pending_review_records)
    rule_governance_issue_count = (
        int(governance.get("expiring_exception_count") or 0)
        + int(governance.get("expired_exception_count") or 0)
        + int(governance.get("review_due_count") or 0)
    )
    webhook_configured = bool(str(app_config.webhook_url or "").strip())

    signals = [
        {
            "key": "dry_run_failure",
            "label": "Dry-Run Failure Alerts",
            "status": "error" if latest_failed_dry_run is not None else "success",
            "enabled": bool(policy_settings["notify_dry_run_failure_enabled"]),
            "value": getattr(latest_failed_dry_run, "job_id", "") or "None",
            "description": (
                f"Latest dry run {latest_failed_dry_run.job_id} finished with status "
                f"{getattr(latest_failed_dry_run, 'status', '-') or '-'}."
                if latest_failed_dry_run is not None
                else "Latest dry run is healthy."
            ),
            "action": "Inspect the failing dry run before the next apply window."
            if latest_failed_dry_run is not None
            else "Keep using dry run as the release gate.",
        },
        {
            "key": "conflict_backlog",
            "label": "Conflict Backlog",
            "status": (
                "error"
                if open_conflict_count >= int(policy_settings["notify_conflict_backlog_threshold"])
                else "warning"
                if open_conflict_count > 0
                else "success"
            ),
            "enabled": bool(policy_settings["notify_conflict_backlog_enabled"]),
            "value": str(open_conflict_count),
            "description": (
                f"{open_conflict_count} open conflict(s) are still pending review."
                if open_conflict_count
                else "No open conflicts are blocking the rollout."
            ),
            "action": (
                f"Send reminders once backlog reaches {int(policy_settings['notify_conflict_backlog_threshold'])}."
                if open_conflict_count
                else "Keep the queue empty before each apply run."
            ),
        },
        {
            "key": "review_pending",
            "label": "High-Risk Approval Queue",
            "status": "warning" if pending_review_count > 0 else "success",
            "enabled": bool(policy_settings["notify_review_pending_enabled"]),
            "value": str(pending_review_count),
            "description": (
                f"{pending_review_count} high-risk plan review(s) are still waiting for approval."
                if pending_review_count
                else "No high-risk dry run approval is currently pending."
            ),
            "action": (
                "Approve the matching dry run before the next scheduled apply."
                if pending_review_count
                else "Keep approval response time short for unattended windows."
            ),
        },
        {
            "key": "rule_governance",
            "label": "Rule Expiry And Review",
            "status": (
                "error"
                if int(governance.get("expired_exception_count") or 0) > 0
                else "warning"
                if rule_governance_issue_count > 0
                else "success"
            ),
            "enabled": bool(policy_settings["notify_rule_governance_enabled"]),
            "value": str(rule_governance_issue_count),
            "description": (
                f"{rule_governance_issue_count} rule governance reminder(s) are active."
                if rule_governance_issue_count
                else "No expiring or overdue manual-rule review is currently pending."
            ),
            "action": (
                "Review expiring exception rules and overdue manual-rule reviews."
                if rule_governance_issue_count
                else "Keep owners and review dates current so reminders stay quiet."
            ),
        },
        {
            "key": "scheduled_apply_gate",
            "label": "Scheduled Apply Gate",
            "status": str(scheduled_apply_guard.get("status") or "info"),
            "enabled": bool(policy_settings["scheduled_apply_gate_enabled"]),
            "value": (
                "Dry Run Only"
                if scheduled_apply_guard.get("mode") != "apply"
                else "Ready"
                if scheduled_apply_guard.get("allowed")
                else "Blocked"
            ),
            "description": str(scheduled_apply_guard.get("summary") or ""),
            "action": (
                "Clear the blocking conditions before the next unattended apply window."
                if not scheduled_apply_guard.get("allowed")
                else "Recent dry run, conflicts, and approval state all look healthy."
            ),
        },
    ]
    active_signal_count = sum(1 for item in signals if item["status"] in {"warning", "error"})

    notification_warnings: list[str] = []
    if not webhook_configured and any(item["enabled"] for item in signals[:-1]):
        notification_warnings.append("Webhook notifications are enabled in policy, but the organization has no webhook URL configured.")

    return {
        "policy_settings": policy_settings,
        "notification_channel": {
            "webhook_configured": webhook_configured,
            "webhook_masked": mask_webhook_url(app_config.webhook_url),
            "schedule_time": str(editable_config.get("schedule_time") or "03:00"),
            "schedule_execution_mode": policy_settings["schedule_execution_mode"],
        },
        "latest_dry_run": latest_dry_run,
        "latest_successful_dry_run": latest_successful_dry_run,
        "latest_apply": latest_apply,
        "latest_failed_dry_run": latest_failed_dry_run,
        "open_conflict_count": open_conflict_count,
        "pending_review_records": pending_review_records,
        "pending_review_count": pending_review_count,
        "governance": governance,
        "rule_governance_issue_count": rule_governance_issue_count,
        "scheduled_apply_guard": scheduled_apply_guard,
        "signals": signals,
        "active_signal_count": active_signal_count,
        "notification_warnings": notification_warnings,
    }
