from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sync_app.core.models import (
    OffboardingRecord,
    SyncExceptionRuleRecord,
    SyncReplayRequestRecord,
    UserLifecycleRecord,
)
from sync_app.storage.local_db import DatabaseManager, utcnow_iso
from sync_app.storage.repositories.conflicts import SyncExceptionRuleRepository
from sync_app.storage.repositories.lifecycle import (
    OffboardingQueueRepository,
    UserLifecycleQueueRepository,
)
from sync_app.storage.repositories.system import SyncReplayRequestRepository


def _normalize_org_id(org_id: str | None) -> str:
    normalized = str(org_id or "").strip().lower()
    return normalized or "default"


def _parse_datetime(value: str | None) -> Optional[datetime]:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    candidate = normalized.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def _build_exception_maps(
    exception_rules: list[SyncExceptionRuleRecord],
) -> tuple[dict[str, SyncExceptionRuleRecord], dict[str, SyncExceptionRuleRecord]]:
    skip_sync_by_user: dict[str, SyncExceptionRuleRecord] = {}
    skip_disable_by_user: dict[str, SyncExceptionRuleRecord] = {}
    for rule in exception_rules:
        if not rule.is_enabled:
            continue
        normalized_match_value = str(rule.match_value or "").strip().lower()
        if not normalized_match_value:
            continue
        if rule.rule_type == "skip_user_sync" and normalized_match_value not in skip_sync_by_user:
            skip_sync_by_user[normalized_match_value] = rule
        elif rule.rule_type == "skip_user_disable" and normalized_match_value not in skip_disable_by_user:
            skip_disable_by_user[normalized_match_value] = rule
    return skip_sync_by_user, skip_disable_by_user


def _build_manual_hold(rule: Optional[SyncExceptionRuleRecord]) -> Optional[dict[str, Any]]:
    if not rule:
        return None
    expires_at = str(rule.expires_at or "").strip()
    return {
        "rule_id": rule.id,
        "rule_type": rule.rule_type,
        "label": ("Deferred" if expires_at else "Skipped"),
        "expires_at": expires_at,
        "owner": rule.rule_owner,
        "reason": rule.effective_reason or rule.notes,
    }


def _build_state(
    *,
    due_at: Optional[datetime],
    now: datetime,
    manual_hold: Optional[dict[str, Any]],
    ready_label: str,
    scheduled_label: str,
) -> dict[str, str]:
    if manual_hold:
        return {
            "label": str(manual_hold.get("label") or "Deferred"),
            "level": "warning",
        }
    if due_at and due_at <= now:
        return {"label": ready_label, "level": "warning"}
    return {"label": scheduled_label, "level": "info"}


def _decorate_offboarding_record(
    record: OffboardingRecord,
    *,
    now: datetime,
    manual_hold_rule: Optional[SyncExceptionRuleRecord],
) -> dict[str, Any]:
    due_at = _parse_datetime(record.due_at)
    manual_hold = _build_manual_hold(manual_hold_rule)
    return {
        "record": record,
        "due_at_dt": due_at,
        "is_due": bool(due_at and due_at <= now),
        "manual_hold": manual_hold,
        "state": _build_state(
            due_at=due_at,
            now=now,
            manual_hold=manual_hold,
            ready_label="Grace Elapsed",
            scheduled_label="Grace Running",
        ),
    }


def _decorate_lifecycle_record(
    record: UserLifecycleRecord,
    *,
    now: datetime,
    manual_hold_rule: Optional[SyncExceptionRuleRecord],
) -> dict[str, Any]:
    effective_at = _parse_datetime(record.effective_at)
    manual_hold = _build_manual_hold(manual_hold_rule)
    return {
        "record": record,
        "effective_at_dt": effective_at,
        "is_due": bool(effective_at and effective_at <= now),
        "manual_hold": manual_hold,
        "state": _build_state(
            due_at=effective_at,
            now=now,
            manual_hold=manual_hold,
            ready_label="Ready",
            scheduled_label="Scheduled",
        ),
    }


def _decorate_replay_request(record: SyncReplayRequestRecord) -> dict[str, Any]:
    return {
        "record": record,
        "state": {
            "label": "Pending",
            "level": "info",
        },
    }


def _enqueue_replay_request(
    replay_repo: SyncReplayRequestRepository,
    *,
    org_id: str,
    actor_username: str,
    request_type: str,
    target_scope: str,
    target_id: str,
    trigger_reason: str,
    payload: Optional[dict[str, Any]] = None,
    execution_mode: str = "apply",
) -> int:
    return replay_repo.enqueue_request(
        request_type=request_type,
        execution_mode=execution_mode,
        requested_by=actor_username,
        org_id=org_id,
        target_scope=target_scope,
        target_id=target_id,
        trigger_reason=trigger_reason,
        payload=payload,
    )


def _persist_exception_rule(
    exception_repo: SyncExceptionRuleRepository,
    *,
    org_id: str,
    actor_username: str,
    rule_type: str,
    source_user_id: str,
    note: str,
    effective_reason: str,
    expires_at: str = "",
) -> None:
    exception_repo.upsert_rule(
        rule_type=rule_type,
        match_value=source_user_id,
        org_id=org_id,
        notes=note,
        is_enabled=True,
        expires_at=expires_at,
        is_once=False,
    )
    exception_repo.update_governance_metadata(
        rule_type=rule_type,
        match_value=source_user_id,
        org_id=org_id,
        rule_owner=actor_username,
        effective_reason=effective_reason,
        next_review_at=expires_at,
        last_reviewed_at=utcnow_iso(),
    )


def _disable_active_exception_rule(
    exception_repo: SyncExceptionRuleRepository,
    *,
    org_id: str,
    rule_type: str,
    source_user_id: str,
) -> bool:
    normalized_source_user_id = str(source_user_id or "").strip().lower()
    if not normalized_source_user_id:
        return False
    for rule in exception_repo.list_enabled_rule_records(org_id=org_id):
        if rule.rule_type != rule_type:
            continue
        if str(rule.match_value or "").strip().lower() != normalized_source_user_id:
            continue
        if rule.id is None:
            continue
        exception_repo.set_enabled(rule.id, False, org_id=org_id)
        return True
    return False


def _defer_until(base_value: str, delay_days: int, *, now: datetime) -> str:
    if delay_days <= 0:
        raise ValueError("Delay days must be greater than zero.")
    base_dt = _parse_datetime(base_value) or now
    if base_dt < now:
        base_dt = now
    return _to_iso(base_dt + timedelta(days=delay_days))


def build_lifecycle_workbench_data(
    db_manager: DatabaseManager,
    org_id: str,
    *,
    replay_limit: int = 30,
) -> dict[str, Any]:
    normalized_org_id = _normalize_org_id(org_id)
    now = datetime.now(timezone.utc)

    offboarding_repo = OffboardingQueueRepository(db_manager)
    lifecycle_repo = UserLifecycleQueueRepository(db_manager)
    replay_repo = SyncReplayRequestRepository(db_manager)
    exception_repo = SyncExceptionRuleRepository(db_manager)

    enabled_rules = exception_repo.list_enabled_rule_records(org_id=normalized_org_id)
    skip_sync_by_user, skip_disable_by_user = _build_exception_maps(enabled_rules)

    offboarding_rows = [
        _decorate_offboarding_record(
            record,
            now=now,
            manual_hold_rule=skip_disable_by_user.get(str(record.source_user_id or "").strip().lower()),
        )
        for record in offboarding_repo.list_pending_records(org_id=normalized_org_id)
    ]

    future_onboarding_rows: list[dict[str, Any]] = []
    contractor_expiry_rows: list[dict[str, Any]] = []
    for record in lifecycle_repo.list_pending_records(org_id=normalized_org_id):
        normalized_source_user_id = str(record.source_user_id or "").strip().lower()
        if record.lifecycle_type == "future_onboarding":
            future_onboarding_rows.append(
                _decorate_lifecycle_record(
                    record,
                    now=now,
                    manual_hold_rule=skip_sync_by_user.get(normalized_source_user_id),
                )
            )
            continue
        if record.lifecycle_type == "contractor_expiry":
            contractor_expiry_rows.append(
                _decorate_lifecycle_record(
                    record,
                    now=now,
                    manual_hold_rule=skip_disable_by_user.get(normalized_source_user_id),
                )
            )

    replay_rows = [
        _decorate_replay_request(record)
        for record in replay_repo.list_request_records(
            status="pending",
            limit=max(int(replay_limit), 1),
            org_id=normalized_org_id,
        )
    ]

    manual_hold_count = sum(1 for row in [*offboarding_rows, *future_onboarding_rows, *contractor_expiry_rows] if row["manual_hold"])
    actionable_now_count = sum(
        1
        for row in [*offboarding_rows, *future_onboarding_rows, *contractor_expiry_rows]
        if row["is_due"] and not row["manual_hold"]
    ) + len(replay_rows)

    return {
        "summary": {
            "future_onboarding_count": len(future_onboarding_rows),
            "contractor_expiry_count": len(contractor_expiry_rows),
            "offboarding_count": len(offboarding_rows),
            "replay_request_count": len(replay_rows),
            "manual_hold_count": manual_hold_count,
            "actionable_now_count": actionable_now_count,
        },
        "future_onboarding_rows": future_onboarding_rows,
        "contractor_expiry_rows": contractor_expiry_rows,
        "offboarding_rows": offboarding_rows,
        "replay_rows": replay_rows,
    }


def apply_offboarding_bulk_action(
    db_manager: DatabaseManager,
    org_id: str,
    *,
    actor_username: str,
    action: str,
    record_ids: list[int],
    delay_days: int = 0,
) -> dict[str, Any]:
    normalized_org_id = _normalize_org_id(org_id)
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"approve", "defer", "skip", "retry"}:
        raise ValueError("Unsupported offboarding action.")
    if normalized_action == "defer" and delay_days <= 0:
        raise ValueError("Delay days must be greater than zero.")

    now = datetime.now(timezone.utc)
    offboarding_repo = OffboardingQueueRepository(db_manager)
    replay_repo = SyncReplayRequestRepository(db_manager)
    exception_repo = SyncExceptionRuleRepository(db_manager)

    processed_count = 0
    replay_request_count = 0
    exception_rule_count = 0
    hold_cleared_count = 0
    skipped_count = 0
    unsupported_count = 0

    for record_id in record_ids:
        record = offboarding_repo.get_record_by_id(record_id, org_id=normalized_org_id)
        if not record or record.status != "pending":
            skipped_count += 1
            continue

        if normalized_action == "approve":
            if record.source_user_id:
                hold_cleared_count += int(
                    _disable_active_exception_rule(
                        exception_repo,
                        org_id=normalized_org_id,
                        rule_type="skip_user_disable",
                        source_user_id=record.source_user_id,
                    )
                )
            offboarding_repo.upsert_pending_for_source_user(
                connector_id=record.connector_id,
                source_user_id=record.source_user_id,
                ad_username=record.ad_username,
                due_at=_to_iso(now),
                org_id=normalized_org_id,
                reason=record.reason,
                manager_userids=record.manager_userids,
                last_job_id=record.last_job_id,
            )
            replay_request_count += 1
            _enqueue_replay_request(
                replay_repo,
                org_id=normalized_org_id,
                actor_username=actor_username,
                request_type="lifecycle_workbench_offboarding",
                target_scope=("source_user" if record.source_user_id else "ad_user"),
                target_id=record.source_user_id or record.ad_username,
                trigger_reason="offboarding_approved",
                payload={
                    "queue_record_id": record.id,
                    "connector_id": record.connector_id,
                    "ad_username": record.ad_username,
                    "source_user_id": record.source_user_id,
                },
            )
        elif normalized_action == "defer":
            offboarding_repo.upsert_pending_for_source_user(
                connector_id=record.connector_id,
                source_user_id=record.source_user_id,
                ad_username=record.ad_username,
                due_at=_defer_until(record.due_at, delay_days, now=now),
                org_id=normalized_org_id,
                reason=record.reason,
                manager_userids=record.manager_userids,
                last_job_id=record.last_job_id,
            )
        elif normalized_action == "skip":
            if not record.source_user_id:
                unsupported_count += 1
                continue
            _persist_exception_rule(
                exception_repo,
                org_id=normalized_org_id,
                actor_username=actor_username,
                rule_type="skip_user_disable",
                source_user_id=record.source_user_id,
                note="Skipped from lifecycle workbench offboarding queue",
                effective_reason="Manual skip from lifecycle workbench offboarding queue",
            )
            exception_rule_count += 1
        elif normalized_action == "retry":
            replay_request_count += 1
            _enqueue_replay_request(
                replay_repo,
                org_id=normalized_org_id,
                actor_username=actor_username,
                request_type="lifecycle_workbench_offboarding",
                target_scope=("source_user" if record.source_user_id else "ad_user"),
                target_id=record.source_user_id or record.ad_username,
                trigger_reason="offboarding_retry_requested",
                payload={
                    "queue_record_id": record.id,
                    "connector_id": record.connector_id,
                    "ad_username": record.ad_username,
                    "source_user_id": record.source_user_id,
                },
            )

        processed_count += 1

    return {
        "action": normalized_action,
        "processed_count": processed_count,
        "skipped_count": skipped_count,
        "replay_request_count": replay_request_count,
        "exception_rule_count": exception_rule_count,
        "hold_cleared_count": hold_cleared_count,
        "unsupported_count": unsupported_count,
    }


def apply_lifecycle_bulk_action(
    db_manager: DatabaseManager,
    org_id: str,
    *,
    actor_username: str,
    lifecycle_type: str,
    action: str,
    record_ids: list[int],
    delay_days: int = 0,
) -> dict[str, Any]:
    normalized_org_id = _normalize_org_id(org_id)
    normalized_lifecycle_type = str(lifecycle_type or "").strip().lower()
    normalized_action = str(action or "").strip().lower()
    if normalized_lifecycle_type not in {"future_onboarding", "contractor_expiry"}:
        raise ValueError("Unsupported lifecycle queue.")
    if normalized_action not in {"approve", "defer", "skip", "retry"}:
        raise ValueError("Unsupported lifecycle action.")
    if normalized_action == "defer" and delay_days <= 0:
        raise ValueError("Delay days must be greater than zero.")

    now = datetime.now(timezone.utc)
    lifecycle_repo = UserLifecycleQueueRepository(db_manager)
    replay_repo = SyncReplayRequestRepository(db_manager)
    exception_repo = SyncExceptionRuleRepository(db_manager)

    processed_count = 0
    skipped_count = 0
    replay_request_count = 0
    exception_rule_count = 0
    hold_cleared_count = 0
    unsupported_count = 0

    exception_rule_type = (
        "skip_user_sync" if normalized_lifecycle_type == "future_onboarding" else "skip_user_disable"
    )

    for record_id in record_ids:
        record = lifecycle_repo.get_record_by_id(record_id, org_id=normalized_org_id)
        if (
            not record
            or record.status != "pending"
            or record.lifecycle_type != normalized_lifecycle_type
        ):
            skipped_count += 1
            continue
        if not record.source_user_id:
            unsupported_count += 1
            continue

        if normalized_action == "approve":
            if normalized_lifecycle_type == "future_onboarding":
                unsupported_count += 1
                continue
            lifecycle_repo.upsert_pending(
                lifecycle_type=record.lifecycle_type,
                connector_id=record.connector_id,
                source_user_id=record.source_user_id,
                ad_username=record.ad_username,
                effective_at=now.isoformat(timespec="seconds"),
                org_id=normalized_org_id,
                reason=record.reason,
                employment_type=record.employment_type,
                sponsor_userid=record.sponsor_userid,
                manager_userids=record.manager_userids,
                payload={
                    **dict(record.payload or {}),
                    "approved_by": actor_username,
                    "approved_at": now.isoformat(timespec="seconds"),
                    "previous_effective_at": record.effective_at,
                },
                last_job_id=record.last_job_id,
            )
            hold_cleared_count += int(
                _disable_active_exception_rule(
                    exception_repo,
                    org_id=normalized_org_id,
                    rule_type=exception_rule_type,
                    source_user_id=record.source_user_id,
                )
            )
            replay_request_count += 1
            _enqueue_replay_request(
                replay_repo,
                org_id=normalized_org_id,
                actor_username=actor_username,
                request_type="lifecycle_workbench_lifecycle",
                target_scope="source_user",
                target_id=record.source_user_id,
                trigger_reason=f"{normalized_lifecycle_type}_approved",
                payload={
                    "queue_record_id": record.id,
                    "lifecycle_type": normalized_lifecycle_type,
                    "connector_id": record.connector_id,
                    "ad_username": record.ad_username,
                    "source_user_id": record.source_user_id,
                },
            )
        elif normalized_action == "defer":
            deferred_until = _defer_until(record.effective_at, delay_days, now=now)
            _persist_exception_rule(
                exception_repo,
                org_id=normalized_org_id,
                actor_username=actor_username,
                rule_type=exception_rule_type,
                source_user_id=record.source_user_id,
                note=f"Deferred from lifecycle workbench ({normalized_lifecycle_type})",
                effective_reason=f"Manual defer from lifecycle workbench {normalized_lifecycle_type}",
                expires_at=deferred_until,
            )
            exception_rule_count += 1
        elif normalized_action == "skip":
            _persist_exception_rule(
                exception_repo,
                org_id=normalized_org_id,
                actor_username=actor_username,
                rule_type=exception_rule_type,
                source_user_id=record.source_user_id,
                note=f"Skipped from lifecycle workbench ({normalized_lifecycle_type})",
                effective_reason=f"Manual skip from lifecycle workbench {normalized_lifecycle_type}",
            )
            exception_rule_count += 1
        elif normalized_action == "retry":
            replay_request_count += 1
            _enqueue_replay_request(
                replay_repo,
                org_id=normalized_org_id,
                actor_username=actor_username,
                request_type="lifecycle_workbench_lifecycle",
                target_scope="source_user",
                target_id=record.source_user_id,
                trigger_reason=f"{normalized_lifecycle_type}_retry_requested",
                payload={
                    "queue_record_id": record.id,
                    "lifecycle_type": normalized_lifecycle_type,
                    "connector_id": record.connector_id,
                    "ad_username": record.ad_username,
                    "source_user_id": record.source_user_id,
                },
            )

        processed_count += 1

    return {
        "action": normalized_action,
        "lifecycle_type": normalized_lifecycle_type,
        "processed_count": processed_count,
        "skipped_count": skipped_count,
        "replay_request_count": replay_request_count,
        "exception_rule_count": exception_rule_count,
        "hold_cleared_count": hold_cleared_count,
        "unsupported_count": unsupported_count,
    }


def apply_replay_bulk_action(
    db_manager: DatabaseManager,
    org_id: str,
    *,
    actor_username: str,
    action: str,
    request_ids: list[int],
) -> dict[str, Any]:
    normalized_org_id = _normalize_org_id(org_id)
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"retry", "skip"}:
        raise ValueError("Unsupported replay action.")

    replay_repo = SyncReplayRequestRepository(db_manager)
    processed_count = 0
    skipped_count = 0
    replay_request_count = 0

    for request_id in request_ids:
        record = replay_repo.get_request_record(request_id)
        if (
            not record
            or record.org_id != normalized_org_id
            or record.status != "pending"
        ):
            skipped_count += 1
            continue

        if normalized_action == "retry":
            replacement_request_id = _enqueue_replay_request(
                replay_repo,
                org_id=normalized_org_id,
                actor_username=actor_username,
                request_type=record.request_type,
                target_scope=record.target_scope,
                target_id=record.target_id,
                trigger_reason=f"retry:{record.trigger_reason or record.request_type}",
                payload={
                    **dict(record.payload or {}),
                    "retried_from_request_id": record.id,
                },
                execution_mode=record.execution_mode or "apply",
            )
            replay_request_count += 1
            replay_repo.mark_finished(
                request_id,
                status="superseded",
                result_summary={
                    "replacement_request_id": replacement_request_id,
                    "actor_username": actor_username,
                    "source": "lifecycle_workbench",
                },
            )
        elif normalized_action == "skip":
            replay_repo.mark_finished(
                request_id,
                status="cancelled",
                result_summary={
                    "actor_username": actor_username,
                    "source": "lifecycle_workbench",
                },
            )

        processed_count += 1

    return {
        "action": normalized_action,
        "processed_count": processed_count,
        "skipped_count": skipped_count,
        "replay_request_count": replay_request_count,
    }
