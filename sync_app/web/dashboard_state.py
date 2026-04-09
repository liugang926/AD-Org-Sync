from __future__ import annotations

from typing import Any


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
    ui_mode: str,
) -> dict[str, Any]:
    check_index = {
        str(item.get("key") or ""): item for item in list(preflight_snapshot.get("checks") or []) if isinstance(item, dict)
    }
    config_ready = str(check_index.get("config", {}).get("status") or "") == "success"
    live_wecom_ok = str(check_index.get("live_wecom", {}).get("status") or "") == "success"
    live_ldap_ok = str(check_index.get("live_ldap", {}).get("status") or "") == "success"
    live_ready = live_wecom_ok and live_ldap_ok
    dry_run_ready = bool(preflight_snapshot.get("dry_run_completed"))
    conflicts_ready = dry_run_ready and int(preflight_snapshot.get("open_conflict_count") or 0) == 0
    apply_ready = bool(preflight_snapshot.get("apply_completed"))

    steps = [
        {
            "title": "Configure organization settings",
            "detail": "Complete the source connector and LDAP values for the current organization.",
            "href": "/config",
            "action_label": "Open Config",
            "capability": "config.read",
            "done": config_ready,
        },
        {
            "title": "Run live connectivity preflight",
            "detail": (
                "Verify both the source connector and LDAP from this server before the first synchronization run."
                if not live_ready
                else "Live source connector and LDAP connectivity checks both passed."
            ),
            "href": "/dashboard#preflight",
            "action_label": "Run Preflight",
            "capability": "dashboard.read",
            "done": live_ready,
        },
        {
            "title": "Review sync scope",
            "detail": (
                "Basic mode keeps the default single-organization flow. Switch to Advanced mode only if you need routing, write-back, or lifecycle controls."
                if ui_mode == "basic"
                else "Review connectors, mappings, and lifecycle policies before the first rollout."
            ),
            "href": "/config" if ui_mode == "basic" else "/advanced-sync",
            "action_label": "Review Scope",
            "capability": "config.read",
            "done": config_ready,
        },
        {
            "title": "Run the first dry run",
            "detail": (
                "A successful dry run is already recorded."
                if dry_run_ready
                else "Preview planned changes before applying them to AD."
            ),
            "href": "/jobs",
            "action_label": "Open Jobs",
            "capability": "jobs.read",
            "done": dry_run_ready,
        },
        {
            "title": "Clear blockers and run apply",
            "detail": (
                "Apply is already successful for this organization."
                if apply_ready
                else (
                    "Resolve open conflicts before the first apply run."
                    if dry_run_ready and not conflicts_ready
                    else "Run the first apply after the dry run looks safe."
                )
            ),
            "href": "/conflicts" if dry_run_ready and not conflicts_ready else "/jobs",
            "action_label": "Resolve Conflicts" if dry_run_ready and not conflicts_ready else "Run Apply",
            "capability": "conflicts.read" if dry_run_ready and not conflicts_ready else "jobs.read",
            "done": apply_ready,
        },
    ]

    current_assigned = False
    completed_steps = 0
    for step in steps:
        if step["done"]:
            step["status"] = "complete"
            completed_steps += 1
        elif not current_assigned:
            step["status"] = "current"
            current_assigned = True
        else:
            step["status"] = "upcoming"

    next_step = next((step for step in steps if step["status"] == "current"), steps[-1])
    return {
        "current_org_name": current_org_name,
        "steps": steps,
        "completed_steps": completed_steps,
        "total_steps": len(steps),
        "next_step": next_step,
    }
