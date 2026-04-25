from __future__ import annotations

from typing import Any, Optional
from datetime import datetime, timedelta, timezone

from sync_app.core.models import (
    SyncExceptionRuleRecord,
    UserDepartmentOverrideRecord,
    UserIdentityBindingRecord,
)

EXPIRING_SOON_WINDOW_DAYS = 14
STALE_RULE_WINDOW_DAYS = 90
RECENT_HIT_WINDOW_DAYS = 30


def _parse_timestamp(value: str) -> Optional[datetime]:
    normalized_value = str(value or "").strip()
    if not normalized_value:
        return None
    candidate = normalized_value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _record_timestamp(*values: str) -> Optional[datetime]:
    for value in values:
        parsed = _parse_timestamp(value)
        if parsed is not None:
            return parsed
    return None


def _build_issue(
    *,
    key: str,
    label: str,
    severity: str,
    description: str,
    action: str,
    samples: list[dict[str, str]],
    count: Optional[int] = None,
) -> Optional[dict[str, object]]:
    normalized_count = int(count if count is not None else len(samples))
    if normalized_count <= 0:
        return None
    return {
        "key": key,
        "label": label,
        "severity": severity,
        "count": normalized_count,
        "description": description,
        "action": action,
        "samples": list(samples[:5]),
    }


def _record_reason(record: Any) -> str:
    return str(getattr(record, "effective_reason", "") or getattr(record, "notes", "") or "").strip()


def _record_owner(record: Any) -> str:
    return str(getattr(record, "rule_owner", "") or "").strip()


def _binding_title(record: UserIdentityBindingRecord) -> str:
    return f"{record.source_user_id or '-'} -> {record.ad_username or '-'}"


def _override_title(record: UserDepartmentOverrideRecord) -> str:
    return f"{record.source_user_id or '-'} -> {record.primary_department_id or '-'}"


def _exception_title(record: SyncExceptionRuleRecord) -> str:
    return f"{record.rule_type or '-'}: {record.match_value or '-'}"


def _build_review_due_sample(
    *,
    title: str,
    next_review_at: str,
    review_anchor_label: str,
) -> dict[str, str]:
    if next_review_at:
        return {
            "title": title,
            "detail": f"Next review was due at {next_review_at}. Confirm whether the rule is still needed.",
        }
    anchor_value = review_anchor_label if review_anchor_label else "an unknown time"
    return {
        "title": title,
        "detail": f"Rule has not been reviewed since {anchor_value}.",
    }


def build_rule_governance_summary(
    *,
    bindings: list[UserIdentityBindingRecord],
    overrides: list[UserDepartmentOverrideRecord],
    exception_rules: list[SyncExceptionRuleRecord],
    now: Optional[datetime] = None,
) -> dict[str, object]:
    reference_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    expiring_deadline = reference_time + timedelta(days=EXPIRING_SOON_WINDOW_DAYS)
    stale_deadline = reference_time - timedelta(days=STALE_RULE_WINDOW_DAYS)
    recent_hit_deadline = reference_time - timedelta(days=RECENT_HIT_WINDOW_DAYS)

    missing_notes_samples: list[dict[str, str]] = []
    missing_owner_samples: list[dict[str, str]] = []
    stale_rule_samples: list[dict[str, str]] = []
    expiring_exception_samples: list[dict[str, str]] = []
    expired_exception_samples: list[dict[str, str]] = []
    total_hit_count = 0
    recently_hit_rule_count = 0

    for record in bindings:
        total_hit_count += int(record.hit_count or 0)
        last_hit_at = _record_timestamp(record.last_hit_at)
        if last_hit_at is not None and last_hit_at >= recent_hit_deadline:
            recently_hit_rule_count += 1
        if not _record_reason(record):
            missing_notes_samples.append(
                {
                    "title": _binding_title(record),
                    "detail": "Identity binding has no explanation recorded.",
                }
            )
        if not _record_owner(record):
            missing_owner_samples.append(
                {
                    "title": _binding_title(record),
                    "detail": "Identity binding has no owner assigned.",
                }
            )
        review_anchor = _record_timestamp(record.last_reviewed_at, record.updated_at)
        next_review_at = _parse_timestamp(record.next_review_at)
        if (
            next_review_at is not None and next_review_at <= reference_time
        ) or (
            next_review_at is None and review_anchor is not None and review_anchor < stale_deadline
        ):
            stale_rule_samples.append(
                _build_review_due_sample(
                    title=_binding_title(record),
                    next_review_at=record.next_review_at,
                    review_anchor_label=record.last_reviewed_at or record.updated_at,
                )
            )

    for record in overrides:
        total_hit_count += int(record.hit_count or 0)
        last_hit_at = _record_timestamp(record.last_hit_at)
        if last_hit_at is not None and last_hit_at >= recent_hit_deadline:
            recently_hit_rule_count += 1
        if not _record_reason(record):
            missing_notes_samples.append(
                {
                    "title": _override_title(record),
                    "detail": "Department override has no reason recorded.",
                }
            )
        if not _record_owner(record):
            missing_owner_samples.append(
                {
                    "title": _override_title(record),
                    "detail": "Department override has no owner assigned.",
                }
            )
        review_anchor = _record_timestamp(record.last_reviewed_at, record.updated_at)
        next_review_at = _parse_timestamp(record.next_review_at)
        if (
            next_review_at is not None and next_review_at <= reference_time
        ) or (
            next_review_at is None and review_anchor is not None and review_anchor < stale_deadline
        ):
            stale_rule_samples.append(
                _build_review_due_sample(
                    title=_override_title(record),
                    next_review_at=record.next_review_at,
                    review_anchor_label=record.last_reviewed_at or record.updated_at,
                )
            )

    for record in exception_rules:
        total_hit_count += int(record.hit_count or 0)
        last_hit_at = _record_timestamp(record.last_hit_at, record.last_matched_at)
        if last_hit_at is not None and last_hit_at >= recent_hit_deadline:
            recently_hit_rule_count += 1
        if not _record_reason(record):
            missing_notes_samples.append(
                {
                    "title": _exception_title(record),
                    "detail": "Exception rule has no justification recorded.",
                }
            )
        if not _record_owner(record):
            missing_owner_samples.append(
                {
                    "title": _exception_title(record),
                    "detail": "Exception rule has no owner assigned.",
                }
            )
        review_anchor = _record_timestamp(record.last_reviewed_at, record.updated_at, record.created_at)
        next_review_at = _parse_timestamp(record.next_review_at)
        if (
            next_review_at is not None and next_review_at <= reference_time
        ) or (
            next_review_at is None and review_anchor is not None and review_anchor < stale_deadline
        ):
            stale_rule_samples.append(
                _build_review_due_sample(
                    title=_exception_title(record),
                    next_review_at=record.next_review_at,
                    review_anchor_label=record.last_reviewed_at or record.updated_at or record.created_at,
                )
            )
        expires_at = _parse_timestamp(record.expires_at)
        if not record.is_enabled or expires_at is None:
            continue
        if expires_at < reference_time:
            expired_exception_samples.append(
                {
                    "title": _exception_title(record),
                    "detail": f"Expired at {record.expires_at}. Remove it or extend the review window.",
                }
            )
            continue
        if expires_at <= expiring_deadline:
            expiring_exception_samples.append(
                {
                    "title": _exception_title(record),
                    "detail": f"Expires at {record.expires_at}. Review whether it should be renewed or removed.",
                }
            )

    issues = [
        _build_issue(
            key="expired_exceptions",
            label="Expired exception rules still enabled",
            severity="error",
            description="These exception rules are already past their expiration time and should be reviewed immediately.",
            action="Delete or extend the expired exception rules before the next production run.",
            samples=expired_exception_samples,
        ),
        _build_issue(
            key="expiring_exceptions",
            label="Exception rules expiring soon",
            severity="warning",
            description=f"These exception rules will expire within the next {EXPIRING_SOON_WINDOW_DAYS} days.",
            action="Review whether each exception is still needed or should be allowed to expire.",
            samples=expiring_exception_samples,
        ),
        _build_issue(
            key="missing_notes",
            label="Manual rules without notes",
            severity="warning",
            description="These bindings, overrides, or exceptions have no recorded reason, which makes later troubleshooting much harder.",
            action="Add a short explanation so future operators know why the rule exists.",
            samples=missing_notes_samples,
        ),
        _build_issue(
            key="missing_owner",
            label="Manual rules without owner",
            severity="warning",
            description="These bindings, overrides, or exceptions do not identify who is responsible for keeping them accurate.",
            action="Assign a rule owner so upcoming reviews and cleanup have a clear accountable contact.",
            samples=missing_owner_samples,
        ),
        _build_issue(
            key="stale_rules",
            label="Rules pending review",
            severity="warning",
            description=f"These rules have not been reviewed for at least {STALE_RULE_WINDOW_DAYS} days.",
            action="Confirm the rule is still valid, update its notes if needed, or remove it.",
            samples=stale_rule_samples,
        ),
    ]
    normalized_issues = [item for item in issues if item]
    severity_rank = {"error": 0, "warning": 1, "info": 2, "success": 3}
    normalized_issues.sort(
        key=lambda item: (
            severity_rank.get(str(item.get("severity") or "warning"), 9),
            -int(item.get("count") or 0),
            str(item.get("label") or ""),
        )
    )
    error_issue_count = sum(1 for item in normalized_issues if item["severity"] == "error")
    warning_issue_count = sum(1 for item in normalized_issues if item["severity"] == "warning")

    return {
        "binding_count": len(bindings),
        "override_count": len(overrides),
        "exception_count": len(exception_rules),
        "missing_notes_count": len(missing_notes_samples),
        "missing_owner_count": len(missing_owner_samples),
        "review_due_count": len(stale_rule_samples),
        "stale_rule_count": len(stale_rule_samples),
        "expiring_exception_count": len(expiring_exception_samples),
        "expired_exception_count": len(expired_exception_samples),
        "total_hit_count": total_hit_count,
        "recently_hit_rule_count": recently_hit_rule_count,
        "error_issue_count": error_issue_count,
        "warning_issue_count": warning_issue_count,
        "expiring_soon_window_days": EXPIRING_SOON_WINDOW_DAYS,
        "stale_rule_window_days": STALE_RULE_WINDOW_DAYS,
        "recent_hit_window_days": RECENT_HIT_WINDOW_DAYS,
        "issues": normalized_issues,
        "overall_status": (
            "error"
            if error_issue_count
            else "warning"
            if warning_issue_count
            else "success"
        ),
    }
