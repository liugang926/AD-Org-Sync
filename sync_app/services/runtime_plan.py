from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Callable

from sync_app.core.models import SyncJobSummary


def compute_plan_fingerprint(items: list[dict[str, Any]]) -> str:
    normalized = []
    for item in items:
        normalized.append(
            {
                "object_type": str(item.get("object_type") or ""),
                "operation_type": str(item.get("operation_type") or ""),
                "source_id": str(item.get("source_id") or ""),
                "department_id": str(item.get("department_id") or ""),
                "target_dn": str(item.get("target_dn") or ""),
                "risk_level": str(item.get("risk_level") or "normal"),
            }
        )
    normalized = sorted(
        normalized,
        key=lambda item: (
            item["object_type"],
            item["operation_type"],
            item["source_id"],
            item["department_id"],
            item["target_dn"],
            item["risk_level"],
        ),
    )
    payload = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def handle_plan_review_gate(
    *,
    execution_mode: str,
    settings_repo: Any,
    review_repo: Any,
    sync_stats: dict[str, Any],
    organization: Any,
    config_hash: str,
    plan_fingerprint: str,
    job_id: str,
    planned_count: int,
    high_risk_operation_count: int,
    disable_action_count: int,
    disable_breaker_triggered: bool,
    disable_breaker_requires_approval: bool,
    disable_breaker_threshold: int,
    disable_breaker_percent: int,
    managed_user_baseline: int,
    source_provider_name: str,
    bot: Any | None,
    mark_job: Callable[..., None],
    record_event: Callable[..., None],
    record_operation: Callable[..., None],
) -> tuple[Any | None, dict[str, Any] | None, bool]:
    review_required_for_high_risk = settings_repo.get_bool('high_risk_apply_requires_review', True)
    approved_review = None

    if disable_breaker_triggered and disable_breaker_requires_approval:
        sync_stats['review_required'] = True
        breaker_summary = {
            'org_id': organization.org_id,
            'organization_name': organization.name,
            'mode': execution_mode,
            'pending_disable_count': disable_action_count,
            'threshold_count': disable_breaker_threshold,
            'percent_threshold': disable_breaker_percent,
            'managed_user_baseline': managed_user_baseline,
            'review_required': True,
            'plan_fingerprint': plan_fingerprint,
            'reason': 'disable_circuit_breaker',
        }
        if execution_mode == 'dry_run':
            review_repo.upsert_review_request(
                job_id=job_id,
                plan_fingerprint=plan_fingerprint,
                config_snapshot_hash=config_hash,
                high_risk_operation_count=high_risk_operation_count,
            )
        else:
            sync_stats['summary'] = breaker_summary
            sync_stats['job_summary'] = SyncJobSummary.from_sync_stats(sync_stats).to_dict()
            mark_job('REVIEW_REQUIRED', ended=True, summary=breaker_summary)
            record_operation(
                stage_name='plan',
                object_type='review',
                operation_type='disable_circuit_breaker',
                status='review_required',
                message='apply blocked by disable circuit breaker policy',
                source_id=job_id,
                risk_level='high',
                reason_code='disable_circuit_breaker',
                details=breaker_summary,
            )
            if bot:
                bot.send_message(
                    f"## {source_provider_name}-AD sync blocked by circuit breaker\n\n"
                    f"> Pending disables: {disable_action_count}\n"
                    f"> Threshold: {disable_breaker_threshold}\n"
                    f"> Managed user baseline: {managed_user_baseline}"
                )
            return None, sync_stats.to_dict(), review_required_for_high_risk

    if high_risk_operation_count and review_required_for_high_risk:
        sync_stats['review_required'] = True
        if execution_mode == 'dry_run':
            review_repo.upsert_review_request(
                job_id=job_id,
                plan_fingerprint=plan_fingerprint,
                config_snapshot_hash=config_hash,
                high_risk_operation_count=high_risk_operation_count,
            )
            record_event(
                'WARNING',
                'high_risk_review_pending',
                f"dry-run generated {high_risk_operation_count} high-risk operations and requires approval before apply",
                stage_name='plan',
                payload={
                    'plan_fingerprint': plan_fingerprint,
                    'high_risk_operation_count': high_risk_operation_count,
                },
            )
            record_operation(
                stage_name='plan',
                object_type='review',
                operation_type='require_high_risk_review',
                status='pending',
                message='dry-run generated high-risk operations and created a pending review request',
                source_id=job_id,
                risk_level='high',
                reason_code='high_risk_review_required',
                details={
                    'plan_fingerprint': plan_fingerprint,
                    'high_risk_operation_count': high_risk_operation_count,
                },
            )
        else:
            approved_review = review_repo.find_matching_approved_review(
                plan_fingerprint=plan_fingerprint,
                config_snapshot_hash=config_hash,
                now_iso=datetime.now(timezone.utc).isoformat(timespec='seconds'),
            )
            if not approved_review:
                summary = {
                    'org_id': organization.org_id,
                    'organization_name': organization.name,
                    'mode': execution_mode,
                    'planned_operation_count': planned_count,
                    'executed_operation_count': 0,
                    'error_count': sync_stats['error_count'],
                    'conflict_count': sync_stats['conflict_count'],
                    'high_risk_operation_count': high_risk_operation_count,
                    'review_required': True,
                    'plan_fingerprint': plan_fingerprint,
                    'review_hint': 'Approve the matching dry-run review before rerunning apply',
                }
                sync_stats['summary'] = summary
                sync_stats['job_summary'] = SyncJobSummary.from_sync_stats(sync_stats).to_dict()
                mark_job('REVIEW_REQUIRED', ended=True, summary=summary)
                record_event(
                    'WARNING',
                    'review_required',
                    f"apply blocked: {high_risk_operation_count} high-risk operations require approved dry-run review",
                    stage_name='plan',
                    payload={
                        'plan_fingerprint': plan_fingerprint,
                        'high_risk_operation_count': high_risk_operation_count,
                    },
                )
                record_operation(
                    stage_name='plan',
                    object_type='review',
                    operation_type='require_high_risk_review',
                    status='review_required',
                    message='apply blocked until a matching dry-run plan is approved',
                    source_id=job_id,
                    risk_level='high',
                    reason_code='high_risk_review_required',
                    details=summary,
                )
                if bot:
                    bot.send_message(
                        f'## {source_provider_name}-AD sync review required\n\n'
                        f"> Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"> High-risk operations: {high_risk_operation_count}\n"
                        '> Result: blocked pending dry-run approval'
                    )
                return None, sync_stats.to_dict(), review_required_for_high_risk

    return approved_review, None, review_required_for_high_risk


def complete_dry_run(
    *,
    sync_stats: dict[str, Any],
    organization: Any,
    execution_mode: str,
    planned_count: int,
    conflict_count: int,
    high_risk_operation_count: int,
    plan_fingerprint: str,
    review_required_for_high_risk: bool,
    department_action_count: int,
    user_action_count: int,
    membership_action_count: int,
    group_hierarchy_action_count: int,
    group_cleanup_action_count: int,
    disable_action_count: int,
    disabled_users: list[str],
    field_ownership_policy: dict[str, Any],
    generate_skip_detail_report: Callable[[dict[str, Any]], Any],
    mark_job: Callable[..., None],
) -> dict[str, Any]:
    sync_stats['skip_detail_report'] = generate_skip_detail_report(sync_stats)
    sync_stats['review_required'] = bool(high_risk_operation_count and review_required_for_high_risk)
    summary = {
        'org_id': organization.org_id,
        'organization_name': organization.name,
        'mode': execution_mode,
        'planned_operation_count': planned_count,
        'executed_operation_count': 0,
        'department_actions': department_action_count,
        'user_actions': user_action_count,
        'membership_actions': membership_action_count,
        'group_hierarchy_actions': group_hierarchy_action_count,
        'group_cleanup_actions': group_cleanup_action_count,
        'disable_actions': disable_action_count,
        'conflict_count': conflict_count,
        'high_risk_operation_count': high_risk_operation_count,
        'review_required': sync_stats['review_required'],
        'plan_fingerprint': plan_fingerprint,
        'field_ownership_policy': dict(field_ownership_policy),
        'skipped_operation_count': sync_stats['skipped_operations']['total'],
        'skipped_by_action': dict(sync_stats['skipped_operations']['by_action']),
    }
    sync_stats['summary'] = summary
    sync_stats['job_summary'] = SyncJobSummary.from_sync_stats(sync_stats).to_dict()
    sync_stats['disabled_users'] = list(disabled_users)
    mark_job('COMPLETED', ended=True, summary=summary)
    return sync_stats.to_dict()
