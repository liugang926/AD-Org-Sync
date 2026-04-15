from __future__ import annotations

import time
import traceback
from datetime import datetime
from typing import Any

from sync_app.core.common import format_time_duration
from sync_app.core.models import SyncJobSummary
from sync_app.services.runtime_context import SyncContext


def _build_failure_guidance(
    *,
    error_type: str,
    error_message: str,
    traceback_text: str,
) -> dict[str, Any]:
    normalized_text = "\n".join(
        [
            str(error_type or ""),
            str(error_message or ""),
            str(traceback_text or ""),
        ]
    ).lower()

    def matches(*patterns: str) -> bool:
        return any(pattern in normalized_text for pattern in patterns)

    if matches("certificate", "ssl", "tls", "hostname mismatch", "self signed"):
        return {
            "error_category": "certificate",
            "error_category_label": "TLS / Certificate",
            "diagnostic_summary": "The failure looks like a TLS or certificate validation problem.",
            "diagnostic_actions": [
                "Verify the LDAP certificate chain, hostname, and CA certificate path.",
                "Review LDAP SSL and certificate-validation settings before rerunning apply.",
            ],
        }
    if matches("invalid credentials", "invalidcredential", "logon failure", "authentication", "bind failed"):
        return {
            "error_category": "authentication",
            "error_category_label": "Authentication",
            "diagnostic_summary": "The failure looks like a source or LDAP authentication problem.",
            "diagnostic_actions": [
                "Recheck the source connector secret and LDAP service-account credentials.",
                "Run dashboard preflight again to confirm both source and LDAP authentication from this server.",
            ],
        }
    if matches(
        "connection refused",
        "timed out",
        "timeout",
        "server down",
        "can't contact ldap server",
        "unable to access",
        "name or service not known",
        "temporary failure in name resolution",
    ):
        return {
            "error_category": "connectivity",
            "error_category_label": "Connectivity",
            "diagnostic_summary": "The failure looks like a network or endpoint reachability problem.",
            "diagnostic_actions": [
                "Check DNS, firewall, and port reachability to the source system and LDAP endpoint.",
                "Confirm the configured server, port, and proxy settings from the organization config page.",
            ],
        }
    if matches("insufficient access", "access is denied", "unwillingtoperform", "constraint violation"):
        return {
            "error_category": "permissions",
            "error_category_label": "Permissions",
            "diagnostic_summary": "The service account appears to be authenticated but not authorized for the requested change.",
            "diagnostic_actions": [
                "Verify AD permissions for the target OU, attributes, and group-management actions.",
                "Retry with a dry run after confirming delegated rights for the affected connector scope.",
            ],
        }
    if matches("validation failed", "config validation", "missing", "required", "incomplete", "not implemented"):
        return {
            "error_category": "configuration",
            "error_category_label": "Configuration",
            "diagnostic_summary": "The failure looks like a configuration or required-field problem.",
            "diagnostic_actions": [
                "Review the organization config and advanced sync settings for missing or invalid values.",
                "Run preflight again after saving config changes to confirm the environment is ready.",
            ],
        }
    if matches("conflict", "connector", "department", "ou", "username", "already bound", "duplicate"):
        return {
            "error_category": "routing_or_naming",
            "error_category_label": "Routing / Naming",
            "diagnostic_summary": "The failure looks related to connector routing, naming policy, or identity conflict handling.",
            "diagnostic_actions": [
                "Use the Advanced Sync username previewer and identity route explainer to verify connector scope and account naming.",
                "Review the conflict queue and advanced routing rules before rerunning apply.",
            ],
        }
    return {
        "error_category": "unknown",
        "error_category_label": "Unknown",
        "diagnostic_summary": "The failure did not match a known category. Use the traceback and log file for deeper analysis.",
        "diagnostic_actions": [
            "Open the referenced log file and traceback to inspect the full runtime context.",
            "Re-run dry run first after correcting the suspected cause so the next failure is easier to isolate.",
        ],
    }


def _build_failure_details(ctx: SyncContext, sync_error: Exception) -> dict[str, Any]:
    traceback_text = "".join(
        traceback.format_exception(type(sync_error), sync_error, sync_error.__traceback__)
    ).strip()
    guidance = _build_failure_guidance(
        error_type=type(sync_error).__name__,
        error_message=str(sync_error),
        traceback_text=traceback_text,
    )
    return {
        "job_id": ctx.job_id,
        "org_id": ctx.organization.org_id,
        "mode": ctx.execution_mode,
        "error": str(sync_error),
        "error_type": type(sync_error).__name__,
        "error_traceback": traceback_text,
        "log_file": str(ctx.sync_stats.get("log_file") or ""),
        **guidance,
    }


def finalize_successful_sync(ctx: SyncContext) -> dict[str, Any]:
    duration = format_time_duration(time.time() - ctx.start_time)
    if ctx.hooks.stats_callback:
        ctx.hooks.stats_callback('sync_duration', duration)

    if ctx.environment.bot:
        result_line = (
            'SUCCESS'
            if ctx.sync_stats['error_count'] == 0
            else f"COMPLETED WITH {ctx.sync_stats['error_count']} ERRORS"
        )
        ctx.environment.bot.send_message(
            f'## {ctx.environment.source_provider_name}-AD sync finished (LDAPS)\n\n'
            f"> Finish time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"> Duration: {duration}\n"
            f"> Result: {result_line}\n"
            f"> Planned operations: {ctx.planned_count}\n"
            f"> Executed operations: {ctx.executed_count}\n"
            f"> Conflicts: {ctx.sync_stats['conflict_count']}\n"
            f"> High-risk operations: {ctx.high_risk_operation_count}\n"
            f"> Users created/updated/disabled: {ctx.sync_stats['operations']['users_created']}/{ctx.sync_stats['operations']['users_updated']}/{ctx.sync_stats['operations']['users_disabled']}"
        )

    ctx.sync_stats['skip_detail_report'] = ctx.hooks.generate_skip_detail_report(ctx.sync_stats)
    ctx.sync_stats['operation_log_report'] = ctx.hooks.generate_sync_operation_log(
        ctx.sync_stats,
        ctx.start_time,
        ctx.config,
    )
    current_source_ad_usernames = sorted(
        {
            f"{connector_id}:{username}"
            for connector_id, usernames in ctx.working.current_source_ad_usernames_by_connector.items()
            for username in usernames
        }
    )
    managed_missing_ad_usernames = sorted(
        {
            f"{connector_id}:{username}"
            for connector_id, connector_enabled_users in ctx.working.enabled_ad_users_by_connector.items()
            for username in connector_enabled_users
            if (connector_id, username) in ctx.working.managed_ad_identities
            and username not in ctx.working.current_source_ad_usernames_by_connector.get(connector_id, set())
        }
    )
    ctx.sync_stats['validation_report'] = ctx.hooks.generate_sync_validation_report(
        ctx.sync_stats,
        current_source_ad_usernames,
        managed_missing_ad_usernames,
    )

    summary = {
        'org_id': ctx.organization.org_id,
        'organization_name': ctx.organization.name,
        'mode': ctx.execution_mode,
        'planned_operation_count': ctx.planned_count,
        'executed_operation_count': ctx.executed_count,
        'error_count': ctx.sync_stats['error_count'],
        'duration': duration,
        'conflict_count': ctx.sync_stats['conflict_count'],
        'high_risk_operation_count': ctx.high_risk_operation_count,
        'review_required': False,
        'approved_review_job_id': ctx.plan.approved_review.job_id if ctx.plan.approved_review else '',
        'plan_fingerprint': ctx.plan.plan_fingerprint,
        'field_ownership_policy': dict(ctx.sync_stats['field_ownership_policy']),
        'skipped_operation_count': ctx.sync_stats['skipped_operations']['total'],
        'skipped_by_action': dict(ctx.sync_stats['skipped_operations']['by_action']),
        'automatic_replay_request_count': len(ctx.plan.started_replay_requests),
        'automatic_replay_request_ids': [
            int(request.id) for request in ctx.plan.started_replay_requests if request.id is not None
        ],
        'log_file': str(ctx.sync_stats.get('log_file') or ''),
        'skip_detail_report': str(ctx.sync_stats.get('skip_detail_report') or ''),
        'operation_log_report': str(ctx.sync_stats.get('operation_log_report') or ''),
        'validation_report': str(ctx.sync_stats.get('validation_report') or ''),
    }
    try:
        summary['history_cleanup'] = ctx.hooks.run_history_cleanup()
    except Exception as cleanup_error:
        ctx.logger.warning("history cleanup failed: %s", cleanup_error)
        ctx.hooks.record_event(
            'WARNING',
            'history_cleanup_failed',
            f"failed to prune old history: {cleanup_error}",
            stage_name='finalize',
            payload={'error': str(cleanup_error)},
        )
        summary['history_cleanup'] = {'error': str(cleanup_error)}

    if ctx.plan.started_replay_requests:
        replay_result_summary = {
            'job_id': ctx.job_id,
            'org_id': ctx.organization.org_id,
            'mode': ctx.execution_mode,
            'status': 'completed' if ctx.sync_stats['error_count'] == 0 else 'completed_with_errors',
            'planned_operation_count': ctx.planned_count,
            'executed_operation_count': ctx.executed_count,
            'error_count': ctx.sync_stats['error_count'],
            'conflict_count': ctx.sync_stats['conflict_count'],
        }
        for replay_request in ctx.plan.started_replay_requests:
            ctx.repositories.replay_request_repo.mark_finished(
                int(replay_request.id),
                status='completed',
                last_job_id=ctx.job_id,
                result_summary=replay_result_summary,
            )

    ctx.sync_stats['summary'] = summary
    ctx.sync_stats['job_summary'] = SyncJobSummary.from_sync_stats(ctx.sync_stats).to_dict()
    ctx.hooks.mark_job(
        'COMPLETED' if ctx.sync_stats['error_count'] == 0 else 'COMPLETED_WITH_ERRORS',
        ended=True,
        summary=summary,
    )
    return ctx.sync_stats.to_dict()


def finalize_interrupted_sync(ctx: SyncContext, interrupted_error: InterruptedError) -> dict[str, Any]:
    ctx.sync_stats['error_count'] += 1
    interruption_details = {
        "job_id": ctx.job_id,
        "org_id": ctx.organization.org_id,
        "mode": ctx.execution_mode,
        "error": str(interrupted_error),
        "error_type": type(interrupted_error).__name__,
        "log_file": str(ctx.sync_stats.get("log_file") or ""),
        "error_category": "canceled",
        "error_category_label": "Canceled",
        "diagnostic_summary": "The run was canceled before completion.",
        "diagnostic_actions": [
            "Confirm there is no active job lease or operator cancellation still blocking this organization.",
            "Re-run dry run or apply after the blocking condition is cleared.",
        ],
    }
    ctx.hooks.record_event(
        'WARNING',
        'sync_canceled',
        f"sync canceled: {interrupted_error}",
        stage_name='finalize',
        payload=interruption_details,
    )
    ctx.hooks.record_operation(
        stage_name='finalize',
        object_type='job',
        operation_type='sync_job',
        status='canceled',
        message=f"sync canceled: {interrupted_error}",
        target_id=ctx.job_id,
        risk_level='normal',
        reason_code='sync_canceled',
        details=interruption_details,
    )
    for replay_request in ctx.plan.started_replay_requests:
        ctx.repositories.replay_request_repo.mark_finished(
            int(replay_request.id),
            status='canceled',
            last_job_id=ctx.job_id,
            result_summary={'mode': ctx.execution_mode, 'error': str(interrupted_error)},
        )
    ctx.hooks.mark_job(
        'CANCELED',
        ended=True,
        summary=interruption_details,
    )
    ctx.sync_stats['job_summary'] = SyncJobSummary.from_sync_stats(ctx.sync_stats).to_dict()
    return ctx.sync_stats.to_dict()


def finalize_failed_sync(ctx: SyncContext, sync_error: Exception) -> None:
    ctx.sync_stats['error_count'] += 1
    failure_details = _build_failure_details(ctx, sync_error)
    ctx.hooks.record_event(
        'ERROR',
        'sync_failed',
        f"sync failed: {sync_error}",
        stage_name='finalize',
        payload=failure_details,
    )
    ctx.hooks.record_operation(
        stage_name='finalize',
        object_type='job',
        operation_type='sync_job',
        status='error',
        message=f"sync failed: {sync_error}",
        target_id=ctx.job_id,
        risk_level='high',
        reason_code='sync_failed',
        details=failure_details,
    )
    for replay_request in ctx.plan.started_replay_requests:
        ctx.repositories.replay_request_repo.mark_finished(
            int(replay_request.id),
            status='failed',
            last_job_id=ctx.job_id,
            result_summary={'mode': ctx.execution_mode, 'error': str(sync_error)},
        )
    ctx.hooks.mark_job(
        'FAILED',
        ended=True,
        summary=failure_details,
    )
    ctx.sync_stats['job_summary'] = SyncJobSummary.from_sync_stats(ctx.sync_stats).to_dict()
