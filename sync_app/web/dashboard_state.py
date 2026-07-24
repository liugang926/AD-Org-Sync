from __future__ import annotations

from typing import Any

from sync_app.web.ui_mode import get_ui_mode_presentation


_SESSION_LIVE_CHECK_KEYS = {"live_source", "live_wecom", "live_ldap"}
_SESSION_CHECK_TEXT_LIMIT = 512


def _bounded_session_text(value: Any, *, limit: int = _SESSION_CHECK_TEXT_LIMIT) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1]}…"


def _bounded_session_params(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        _bounded_session_text(key, limit=64): _bounded_session_text(item, limit=128)
        for key, item in list(value.items())[:8]
    }


def summarize_check_status(checks: list[dict[str, Any]]) -> str:
    if any(str(item.get("status") or "") == "error" for item in checks):
        return "error"
    if any(str(item.get("status") or "") == "warning" for item in checks):
        return "warning"
    return "success"


def count_check_statuses(checks: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"success": 0, "warning": 0, "error": 0}
    for item in checks:
        status = str(item.get("status") or "success")
        if status in counts:
            counts[status] += 1
    return counts


def compact_preflight_snapshot_for_session(
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    """Keep only bounded live-check data in the signed cookie session."""
    live_checks: list[dict[str, Any]] = []
    for item in list(snapshot.get("checks") or []):
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "")
        if key not in _SESSION_LIVE_CHECK_KEYS:
            continue
        status = str(item.get("status") or "warning")
        if status not in {"success", "warning", "error"}:
            status = "warning"
        live_checks.append(
            {
                "key": key,
                "label": _bounded_session_text(item.get("label"), limit=160),
                "label_params": _bounded_session_params(item.get("label_params")),
                "status": status,
                "detail": _bounded_session_text(item.get("detail")),
                "detail_params": _bounded_session_params(item.get("detail_params")),
                "action_url": "/data-sources/connectors",
            }
        )

    return {
        "org_id": _bounded_session_text(snapshot.get("org_id"), limit=128),
        "generated_at": _bounded_session_text(snapshot.get("generated_at"), limit=64),
        "checks": live_checks,
        "overall_status": summarize_check_status(live_checks),
        "status_counts": count_check_statuses(live_checks),
        "has_live_checks": bool(live_checks),
    }


def merge_saved_preflight_snapshot(
    saved_snapshot: Any,
    base_snapshot: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(saved_snapshot, dict):
        return base_snapshot
    if str(saved_snapshot.get("org_id") or "") != str(base_snapshot.get("org_id") or ""):
        return base_snapshot
    saved_checks = [
        item
        for item in list(saved_snapshot.get("checks") or [])
        if isinstance(item, dict) and str(item.get("key") or "").startswith("live_")
    ]
    if not saved_checks:
        return base_snapshot
    merged = dict(base_snapshot)
    merged_checks = list(base_snapshot.get("checks") or []) + saved_checks
    merged["checks"] = merged_checks
    merged["overall_status"] = summarize_check_status(merged_checks)
    merged["status_counts"] = count_check_statuses(merged_checks)
    merged["live_ran_at"] = str(saved_snapshot.get("generated_at") or "")
    merged["has_live_checks"] = True
    return merged


def build_getting_started_data(
    *,
    current_org_name: str,
    preflight_snapshot: dict[str, Any],
    source_provider_name: str,
    ui_mode: str,
) -> dict[str, Any]:
    rollout_readiness = preflight_snapshot.get("rollout_readiness")
    if isinstance(rollout_readiness, dict) and rollout_readiness.get("steps"):
        steps = [
            dict(step)
            for step in list(rollout_readiness.get("steps") or [])
            if isinstance(step, dict)
        ]
        next_step = dict(rollout_readiness.get("next_step") or {})
        next_key = str(next_step.get("key") or "")
        for step in steps:
            step["done"] = str(step.get("status") or "") == "complete"
            step["available"] = str(step.get("status") or "") not in {
                "blocked",
                "not_started",
            }
            step["is_recommended"] = str(step.get("key") or "") == next_key
            step["href"] = str(step.get("action_url") or "")
            step["detail"] = str(step.get("summary") or "")
        return {
            "current_org_name": current_org_name,
            "steps": steps,
            "completed_steps": int(rollout_readiness.get("completed_count") or 0),
            "total_steps": int(rollout_readiness.get("required_count") or len(steps)),
            "completion_percent": int(rollout_readiness.get("completion_percent") or 0),
            "blocker_count": int(rollout_readiness.get("blocker_count") or 0),
            "current_phase": str(rollout_readiness.get("current_phase") or ""),
            "next_step": next_step,
            "phases": _group_rollout_phases(steps),
        }

    presentation = get_ui_mode_presentation(ui_mode)
    check_index = {
        str(item.get("key") or ""): item for item in list(preflight_snapshot.get("checks") or []) if isinstance(item, dict)
    }
    config_ready = str(check_index.get("config", {}).get("status") or "") == "success"
    live_source_ok = str(
        (check_index.get("live_source") or check_index.get("live_wecom") or {}).get("status") or ""
    ) == "success"
    live_ldap_ok = str(check_index.get("live_ldap", {}).get("status") or "") == "success"
    live_ready = live_source_ok and live_ldap_ok
    source_snapshot_ready = bool(preflight_snapshot.get("source_snapshot_ready"))
    scope_ready = bool(preflight_snapshot.get("scope_ready"))
    release_ready = bool(preflight_snapshot.get("release_ready"))
    dry_run_ready = bool(preflight_snapshot.get("dry_run_completed"))
    conflicts_ready = dry_run_ready and int(preflight_snapshot.get("open_conflict_count") or 0) == 0
    review_ready = bool(preflight_snapshot.get("review_ready"))
    apply_ready = bool(preflight_snapshot.get("apply_completed"))

    steps = [
        {
            "title": "Configure connectors",
            "detail": "Complete the {provider} source and target AD connection settings for the current organization.",
            "detail_params": {"provider": source_provider_name},
            "href": "/data-sources/connectors",
            "action_label": "Configure connectors",
            "capability": "config.read",
            "done": config_ready,
            "available": True,
        },
        {
            "title": "Test saved connections",
            "detail": (
                "Verify both {provider} and LDAP from this server before the first synchronization run."
                if not live_ready
                else "Live {provider} and LDAP connectivity checks both passed."
            ),
            "detail_params": {"provider": source_provider_name},
            "href": "/data-sources/connectors#connection-tests",
            "action_label": "Run Preflight",
            "capability": "config.read",
            "done": live_ready,
            "available": config_ready,
        },
        {
            "title": "Refresh Source Directory",
            "detail": "Create the first immutable source snapshot after both saved connections pass.",
            "href": "/data-sources/source-directory",
            "action_label": "Refresh Source Directory",
            "capability": "config.read",
            "done": source_snapshot_ready,
            "available": live_ready,
        },
        {
            "title": "Check data quality",
            "detail": "Review source identity, naming, and routing risks before selecting the synchronization boundary.",
            "href": "/data-sources/data-quality",
            "action_label": "Check Data Quality",
            "capability": "config.read",
            "done": source_snapshot_ready,
            "available": source_snapshot_ready,
        },
        {
            "title": "Review sync scope",
            "detail": presentation.choose(
                "Choose all active users, departments, checked users, or one replay identity and confirm the estimated scope.",
                "Review connectors, mappings, and lifecycle policies before the first rollout.",
            ),
            "href": "/sync-policies/scope",
            "action_label": "Review Sync Scope",
            "capability": "config.read",
            "done": scope_ready,
            "available": source_snapshot_ready,
        },
        {
            "title": "Publish sync policy",
            "detail": "Publish the reviewed configuration snapshot before generating a new Dry Run plan.",
            "href": "/sync-policies/releases",
            "action_label": "Publish Policy",
            "capability": "config.read",
            "done": release_ready,
            "available": scope_ready,
        },
        {
            "title": "Run the first dry run",
            "detail": (
                "A successful dry run is already recorded."
                if dry_run_ready
                else "Preview planned changes before applying them to AD."
            ),
            "href": "/execution-center/dry-run",
            "action_label": "Run Dry Run",
            "capability": "jobs.read",
            "done": dry_run_ready,
            "available": release_ready,
        },
        {
            "title": "Resolve conflicts",
            "detail": (
                "No unresolved identity conflicts remain for the latest plan."
                if conflicts_ready
                else "Resolve open conflicts before reviewing the plan."
            ),
            "href": "/identity-governance/conflicts",
            "action_label": "Resolve Conflicts",
            "capability": "jobs.read",
            "done": conflicts_ready,
            "available": dry_run_ready,
        },
        {
            "title": "Review the plan",
            "detail": "Review the exact Dry Run evidence and record an auditable approval before Apply.",
            "href": "/execution-center/plan-review",
            "action_label": "Review Plan",
            "capability": "jobs.read",
            "done": review_ready,
            "available": conflicts_ready,
        },
        {
            "title": "Run Apply",
            "detail": (
                "Apply is already successful for this organization."
                if apply_ready
                else "Run the approved plan only after every safety gate passes."
            ),
            "href": "/execution-center/apply",
            "action_label": "Run Apply",
            "capability": "jobs.read",
            "done": apply_ready,
            "available": review_ready,
        },
    ]

    current_assigned = False
    completed_steps = 0
    for step in steps:
        if step["done"]:
            step["status"] = "complete"
            completed_steps += 1
        elif not current_assigned and step.get("available", True):
            step["status"] = "current"
            current_assigned = True
        elif not step.get("available", True):
            step["status"] = "blocked"
        else:
            step["status"] = "upcoming"

    next_step = next(
        (step for step in steps if step["status"] == "current"),
        next((step for step in steps if not step["done"]), steps[-1]),
    )
    return {
        "current_org_name": current_org_name,
        "steps": steps,
        "completed_steps": completed_steps,
        "total_steps": len(steps),
        "next_step": next_step,
    }


def _group_rollout_phases(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    phases: list[dict[str, Any]] = []
    by_name: dict[str, dict[str, Any]] = {}
    for step in steps:
        phase_name = str(step.get("phase") or "Rollout")
        phase = by_name.get(phase_name)
        if phase is None:
            phase = {
                "name": phase_name,
                "steps": [],
                "completed_count": 0,
                "required_count": 0,
                "has_recommended": False,
            }
            by_name[phase_name] = phase
            phases.append(phase)
        phase["steps"].append(step)
        if bool(step.get("whether_required", True)):
            phase["required_count"] += 1
            if str(step.get("status") or "") == "complete":
                phase["completed_count"] += 1
        if step.get("is_recommended"):
            phase["has_recommended"] = True
    return phases
