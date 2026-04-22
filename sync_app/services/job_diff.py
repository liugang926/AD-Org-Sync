from __future__ import annotations

import json
from typing import Any, Callable


def _stable_json(value: Any) -> str:
    if value is None:
        return ""
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        return str(value)


def _list_all_rows(fetch_page: Callable[..., tuple[list[Any], int]], *, page_size: int = 500) -> list[Any]:
    rows: list[Any] = []
    offset = 0
    while True:
        page_rows, total = fetch_page(limit=page_size, offset=offset)
        normalized_rows = list(page_rows or [])
        rows.extend(normalized_rows)
        offset += len(normalized_rows)
        if not normalized_rows or offset >= int(total or 0):
            break
    return rows


def _operation_key(item: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(item.get("object_type") or ""),
        str(item.get("operation_type") or ""),
        str(item.get("source_id") or ""),
        str(item.get("department_id") or ""),
        str(item.get("target_dn") or ""),
    )


def _operation_signature(item: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(item.get("risk_level") or "normal"),
        str(item.get("status") or "planned"),
        _stable_json(item.get("desired_state")),
    )


def _conflict_key(item: Any) -> tuple[str, str, str]:
    return (
        str(getattr(item, "conflict_type", "") or ""),
        str(getattr(item, "source_id", "") or ""),
        str(getattr(item, "target_key", "") or ""),
    )


def _conflict_signature(item: Any) -> tuple[str, str, str, str, str]:
    return (
        str(getattr(item, "severity", "") or "warning"),
        str(getattr(item, "status", "") or "open"),
        str(getattr(item, "message", "") or ""),
        str(getattr(item, "resolution_hint", "") or ""),
        _stable_json(getattr(item, "details", None) or {}),
    )


def _format_operation_title(item: dict[str, Any]) -> str:
    identity = str(item.get("source_id") or item.get("department_id") or item.get("target_dn") or "-")
    return f"{item.get('object_type') or '-'} / {item.get('operation_type') or '-'} / {identity}"


def _format_operation_detail(item: dict[str, Any]) -> str:
    detail_parts: list[str] = []
    target_dn = str(item.get("target_dn") or "").strip()
    risk_level = str(item.get("risk_level") or "normal").strip()
    if target_dn:
        detail_parts.append(f"target: {target_dn}")
    if risk_level:
        detail_parts.append(f"risk: {risk_level}")
    return "; ".join(detail_parts) or "planned operation"


def _build_changed_operation_sample(
    before_item: dict[str, Any],
    after_item: dict[str, Any],
) -> dict[str, str]:
    changes: list[str] = []
    if str(before_item.get("risk_level") or "normal") != str(after_item.get("risk_level") or "normal"):
        changes.append(
            f"risk {before_item.get('risk_level') or 'normal'} -> {after_item.get('risk_level') or 'normal'}"
        )
    if str(before_item.get("status") or "planned") != str(after_item.get("status") or "planned"):
        changes.append(
            f"status {before_item.get('status') or 'planned'} -> {after_item.get('status') or 'planned'}"
        )
    if _stable_json(before_item.get("desired_state")) != _stable_json(after_item.get("desired_state")):
        changes.append("desired state updated")
    return {
        "title": _format_operation_title(after_item),
        "detail": "; ".join(changes) or "operation payload changed",
    }


def _format_conflict_title(item: Any) -> str:
    return (
        f"{getattr(item, 'conflict_type', '') or '-'} / "
        f"{getattr(item, 'source_id', '') or '-'} / "
        f"{getattr(item, 'target_key', '') or '-'}"
    )


def _format_conflict_detail(item: Any) -> str:
    severity = str(getattr(item, "severity", "") or "warning")
    message = str(getattr(item, "message", "") or "").strip()
    if message:
        return f"severity: {severity}; {message}"
    return f"severity: {severity}"


def _build_changed_conflict_sample(before_item: Any, after_item: Any) -> dict[str, str]:
    changes: list[str] = []
    if str(getattr(before_item, "severity", "") or "warning") != str(getattr(after_item, "severity", "") or "warning"):
        changes.append(
            f"severity {getattr(before_item, 'severity', '') or 'warning'} -> "
            f"{getattr(after_item, 'severity', '') or 'warning'}"
        )
    if str(getattr(before_item, "status", "") or "open") != str(getattr(after_item, "status", "") or "open"):
        changes.append(
            f"status {getattr(before_item, 'status', '') or 'open'} -> "
            f"{getattr(after_item, 'status', '') or 'open'}"
        )
    if str(getattr(before_item, "message", "") or "") != str(getattr(after_item, "message", "") or ""):
        changes.append("message updated")
    if _stable_json(getattr(before_item, "details", None) or {}) != _stable_json(getattr(after_item, "details", None) or {}):
        changes.append("details updated")
    return {
        "title": _format_conflict_title(after_item),
        "detail": "; ".join(changes) or "conflict payload changed",
    }


def _build_breakdown() -> dict[str, dict[str, int]]:
    return {}


def _record_breakdown(
    breakdown: dict[str, dict[str, int]],
    *,
    key: str,
    bucket: str,
) -> None:
    normalized_key = str(key or "-").strip() or "-"
    item = breakdown.setdefault(
        normalized_key,
        {
            "name": normalized_key,
            "added_count": 0,
            "removed_count": 0,
            "changed_count": 0,
            "total_count": 0,
        },
    )
    item[bucket] += 1
    item["total_count"] += 1


def _finalize_breakdown(breakdown: dict[str, dict[str, int]]) -> list[dict[str, int | str]]:
    return sorted(
        breakdown.values(),
        key=lambda item: (-int(item["total_count"]), str(item["name"])),
    )


def _build_highlights(
    *,
    operation_diff: dict[str, Any],
    conflict_diff: dict[str, Any],
    sample_limit: int,
) -> list[dict[str, str]]:
    highlights: list[dict[str, str]] = []
    for sample in list(operation_diff.get("added_samples") or [])[:2]:
        highlights.append({"title": f"New planned change: {sample['title']}", "detail": sample["detail"]})
    for sample in list(operation_diff.get("changed_samples") or [])[:2]:
        highlights.append({"title": f"Changed planned change: {sample['title']}", "detail": sample["detail"]})
    for sample in list(conflict_diff.get("added_samples") or [])[:2]:
        highlights.append({"title": f"New conflict: {sample['title']}", "detail": sample["detail"]})
    return highlights[: max(int(sample_limit or 5), 1)]


def _summary_metric(job: Any, key: str, *, fallback_attribute: str | None = None) -> int:
    summary = dict(getattr(job, "summary", {}) or {})
    if key in summary:
        return int(summary.get(key) or 0)
    if fallback_attribute:
        return int(getattr(job, fallback_attribute, 0) or 0)
    return 0


def build_job_comparison_summary(
    *,
    current_job: Any,
    baseline_job: Any,
    planned_operation_repo: Any,
    conflict_repo: Any,
    sample_limit: int = 5,
) -> dict[str, Any]:
    current_operations = _list_all_rows(
        lambda *, limit, offset: planned_operation_repo.list_operations_for_job_page(
            current_job.job_id,
            limit=limit,
            offset=offset,
        )
    )
    baseline_operations = _list_all_rows(
        lambda *, limit, offset: planned_operation_repo.list_operations_for_job_page(
            baseline_job.job_id,
            limit=limit,
            offset=offset,
        )
    )
    current_conflicts = _list_all_rows(
        lambda *, limit, offset: conflict_repo.list_conflicts_for_job_page(
            current_job.job_id,
            limit=limit,
            offset=offset,
        )
    )
    baseline_conflicts = _list_all_rows(
        lambda *, limit, offset: conflict_repo.list_conflicts_for_job_page(
            baseline_job.job_id,
            limit=limit,
            offset=offset,
        )
    )

    current_operation_map = {_operation_key(item): item for item in current_operations}
    baseline_operation_map = {_operation_key(item): item for item in baseline_operations}
    current_conflict_map = {_conflict_key(item): item for item in current_conflicts}
    baseline_conflict_map = {_conflict_key(item): item for item in baseline_conflicts}

    operation_breakdown = _build_breakdown()
    conflict_breakdown = _build_breakdown()

    added_operation_samples: list[dict[str, str]] = []
    removed_operation_samples: list[dict[str, str]] = []
    changed_operation_samples: list[dict[str, str]] = []
    unchanged_operation_count = 0
    high_risk_added_count = 0
    high_risk_removed_count = 0
    high_risk_changed_count = 0

    for key, item in current_operation_map.items():
        baseline_item = baseline_operation_map.get(key)
        if baseline_item is None:
            _record_breakdown(
                operation_breakdown,
                key=str(item.get("object_type") or "-"),
                bucket="added_count",
            )
            added_operation_samples.append(
                {
                    "title": _format_operation_title(item),
                    "detail": _format_operation_detail(item),
                }
            )
            if str(item.get("risk_level") or "normal") == "high":
                high_risk_added_count += 1
            continue
        if _operation_signature(item) != _operation_signature(baseline_item):
            _record_breakdown(
                operation_breakdown,
                key=str(item.get("object_type") or "-"),
                bucket="changed_count",
            )
            changed_operation_samples.append(_build_changed_operation_sample(baseline_item, item))
            before_risk = str(baseline_item.get("risk_level") or "normal")
            after_risk = str(item.get("risk_level") or "normal")
            if before_risk != "high" and after_risk == "high":
                high_risk_changed_count += 1
            continue
        unchanged_operation_count += 1

    for key, item in baseline_operation_map.items():
        if key in current_operation_map:
            continue
        _record_breakdown(
            operation_breakdown,
            key=str(item.get("object_type") or "-"),
            bucket="removed_count",
        )
        removed_operation_samples.append(
            {
                "title": _format_operation_title(item),
                "detail": _format_operation_detail(item),
            }
        )
        if str(item.get("risk_level") or "normal") == "high":
            high_risk_removed_count += 1

    added_conflict_samples: list[dict[str, str]] = []
    removed_conflict_samples: list[dict[str, str]] = []
    changed_conflict_samples: list[dict[str, str]] = []
    unchanged_conflict_count = 0

    for key, item in current_conflict_map.items():
        baseline_item = baseline_conflict_map.get(key)
        if baseline_item is None:
            _record_breakdown(
                conflict_breakdown,
                key=str(getattr(item, "conflict_type", "") or "-"),
                bucket="added_count",
            )
            added_conflict_samples.append(
                {
                    "title": _format_conflict_title(item),
                    "detail": _format_conflict_detail(item),
                }
            )
            continue
        if _conflict_signature(item) != _conflict_signature(baseline_item):
            _record_breakdown(
                conflict_breakdown,
                key=str(getattr(item, "conflict_type", "") or "-"),
                bucket="changed_count",
            )
            changed_conflict_samples.append(_build_changed_conflict_sample(baseline_item, item))
            continue
        unchanged_conflict_count += 1

    for key, item in baseline_conflict_map.items():
        if key in current_conflict_map:
            continue
        _record_breakdown(
            conflict_breakdown,
            key=str(getattr(item, "conflict_type", "") or "-"),
            bucket="removed_count",
        )
        removed_conflict_samples.append(
            {
                "title": _format_conflict_title(item),
                "detail": _format_conflict_detail(item),
            }
        )

    operation_diff = {
        "total_current": len(current_operations),
        "total_baseline": len(baseline_operations),
        "added_count": len(added_operation_samples),
        "removed_count": len(removed_operation_samples),
        "changed_count": len(changed_operation_samples),
        "unchanged_count": unchanged_operation_count,
        "high_risk_added_count": high_risk_added_count,
        "high_risk_removed_count": high_risk_removed_count,
        "high_risk_changed_count": high_risk_changed_count,
        "object_type_breakdown": _finalize_breakdown(operation_breakdown),
        "added_samples": added_operation_samples[: max(int(sample_limit or 5), 1)],
        "removed_samples": removed_operation_samples[: max(int(sample_limit or 5), 1)],
        "changed_samples": changed_operation_samples[: max(int(sample_limit or 5), 1)],
    }
    conflict_diff = {
        "total_current": len(current_conflicts),
        "total_baseline": len(baseline_conflicts),
        "added_count": len(added_conflict_samples),
        "removed_count": len(removed_conflict_samples),
        "changed_count": len(changed_conflict_samples),
        "unchanged_count": unchanged_conflict_count,
        "conflict_type_breakdown": _finalize_breakdown(conflict_breakdown),
        "added_samples": added_conflict_samples[: max(int(sample_limit or 5), 1)],
        "removed_samples": removed_conflict_samples[: max(int(sample_limit or 5), 1)],
        "changed_samples": changed_conflict_samples[: max(int(sample_limit or 5), 1)],
    }

    summary_delta = {
        "planned_operation_delta": _summary_metric(
            current_job,
            "planned_operation_count",
            fallback_attribute="planned_operation_count",
        )
        - _summary_metric(
            baseline_job,
            "planned_operation_count",
            fallback_attribute="planned_operation_count",
        ),
        "conflict_delta": _summary_metric(current_job, "conflict_count")
        - _summary_metric(baseline_job, "conflict_count"),
        "high_risk_operation_delta": _summary_metric(current_job, "high_risk_operation_count")
        - _summary_metric(baseline_job, "high_risk_operation_count"),
        "error_delta": _summary_metric(current_job, "error_count", fallback_attribute="error_count")
        - _summary_metric(baseline_job, "error_count", fallback_attribute="error_count"),
    }

    changed = bool(
        operation_diff["added_count"]
        or operation_diff["removed_count"]
        or operation_diff["changed_count"]
        or conflict_diff["added_count"]
        or conflict_diff["removed_count"]
        or conflict_diff["changed_count"]
    )
    if operation_diff["high_risk_added_count"] or conflict_diff["added_count"]:
        overall_status = "warning"
    elif changed:
        overall_status = "info"
    else:
        overall_status = "success"

    return {
        "current_job_id": str(getattr(current_job, "job_id", "") or ""),
        "baseline_job_id": str(getattr(baseline_job, "job_id", "") or ""),
        "baseline_mode": str(getattr(baseline_job, "execution_mode", "") or ""),
        "baseline_started_at": str(getattr(baseline_job, "started_at", "") or ""),
        "changed": changed,
        "overall_status": overall_status,
        "operation_diff": operation_diff,
        "conflict_diff": conflict_diff,
        "summary_delta": summary_delta,
        "highlights": _build_highlights(
            operation_diff=operation_diff,
            conflict_diff=conflict_diff,
            sample_limit=sample_limit,
        ),
    }
