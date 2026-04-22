from __future__ import annotations

from typing import Any, Optional

from sync_app.core.models import DataQualitySnapshotRecord
from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories.system import DataQualitySnapshotRepository


def build_data_quality_snapshot_title(snapshot: DataQualitySnapshotRecord) -> str:
    snapshot_id = str(snapshot.id or "").strip() or "-"
    return f"Snapshot #{snapshot_id}"


def _summary_from_snapshot(snapshot: Optional[DataQualitySnapshotRecord]) -> dict[str, Any]:
    if snapshot is None:
        return {}
    summary: dict[str, Any]
    if isinstance(snapshot.summary, dict):
        summary = dict(snapshot.summary)
    elif isinstance(snapshot.snapshot, dict) and isinstance(snapshot.snapshot.get("summary"), dict):
        summary = dict(snapshot.snapshot.get("summary") or {})
    else:
        summary = {}
    summary["department_anomaly_count"] = _department_anomaly_count(summary)
    summary["naming_risk_count"] = _naming_risk_count(summary)
    return summary


def _int_metric(summary: dict[str, Any], key: str) -> int:
    try:
        return int(summary.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _department_anomaly_count(summary: dict[str, Any]) -> int:
    explicit = _int_metric(summary, "department_anomaly_count")
    if explicit > 0:
        return explicit
    return (
        _int_metric(summary, "users_without_departments")
        + _int_metric(summary, "placement_unresolved_count")
        + _int_metric(summary, "routing_ambiguity_count")
    )


def _naming_risk_count(summary: dict[str, Any]) -> int:
    explicit = _int_metric(summary, "naming_risk_count")
    if explicit > 0:
        return explicit
    return (
        _int_metric(summary, "naming_prerequisite_gap_count")
        + _int_metric(summary, "managed_username_collision_count")
    )


def persist_data_quality_snapshot(
    db_manager: DatabaseManager,
    org_id: str,
    *,
    created_by: str,
    snapshot: dict[str, Any],
    trigger_action: str = "manual_scan",
) -> dict[str, Any]:
    repo = DataQualitySnapshotRepository(db_manager)
    snapshot_id = repo.add_snapshot(
        org_id=org_id,
        trigger_action=trigger_action,
        created_by=created_by,
        summary=dict(snapshot.get("summary") or {}),
        snapshot=snapshot,
        created_at=str(snapshot.get("generated_at") or "").strip() or None,
    )
    snapshot_record = repo.get_snapshot_record(snapshot_id, org_id=org_id)
    return {
        "snapshot": snapshot_record,
    }


def build_data_quality_center_context(
    db_manager: DatabaseManager,
    org_id: str,
    *,
    snapshot_id: Optional[int] = None,
    snapshot_limit: int = 12,
) -> dict[str, Any]:
    repo = DataQualitySnapshotRepository(db_manager)
    snapshots = repo.list_snapshot_records(org_id=org_id, limit=snapshot_limit)
    latest_snapshot = snapshots[0] if snapshots else None

    selected_snapshot = None
    if snapshot_id is not None:
        for candidate in snapshots:
            if candidate.id == snapshot_id:
                selected_snapshot = candidate
                break
        if selected_snapshot is None:
            selected_snapshot = repo.get_snapshot_record(snapshot_id, org_id=org_id)
    if selected_snapshot is None:
        selected_snapshot = latest_snapshot

    selected_summary = _summary_from_snapshot(selected_snapshot)
    selected_payload = (
        dict(selected_snapshot.snapshot)
        if selected_snapshot is not None and isinstance(selected_snapshot.snapshot, dict)
        else {}
    )
    selected_issues = list(selected_payload.get("issues") or [])
    selected_notes = list(selected_payload.get("analysis_notes") or [])
    selected_connector_breakdown = list(selected_payload.get("connector_breakdown") or [])
    selected_repair_items = list(selected_payload.get("repair_items") or [])

    previous_snapshot = None
    if selected_snapshot is not None:
        for index, candidate in enumerate(snapshots):
            if candidate.id == selected_snapshot.id and index + 1 < len(snapshots):
                previous_snapshot = snapshots[index + 1]
                break
    previous_summary = _summary_from_snapshot(previous_snapshot)

    trend_rows: list[dict[str, Any]] = []
    for snapshot in snapshots:
        summary = _summary_from_snapshot(snapshot)
        trend_rows.append(
            {
                "snapshot": snapshot,
                "title": build_data_quality_snapshot_title(snapshot),
                "summary": summary,
                "department_anomaly_count": _department_anomaly_count(summary),
                "naming_risk_count": _naming_risk_count(summary),
                "duplicate_identifier_count": (
                    _int_metric(summary, "duplicate_email_count")
                    + _int_metric(summary, "duplicate_employee_id_count")
                ),
                "is_selected": bool(selected_snapshot and snapshot.id == selected_snapshot.id),
            }
        )

    high_risk_items = [
        item
        for item in selected_repair_items
        if str(item.get("severity") or "").strip().lower() == "error"
        or str(item.get("key") or "").strip().lower() in {"placement_unresolved", "naming_prerequisite_gap"}
    ]

    return {
        "has_snapshots": bool(snapshots),
        "snapshot_count": len(snapshots),
        "latest_snapshot": latest_snapshot,
        "latest_snapshot_title": (
            build_data_quality_snapshot_title(latest_snapshot)
            if latest_snapshot is not None
            else ""
        ),
        "selected_snapshot": selected_snapshot,
        "selected_snapshot_title": (
            build_data_quality_snapshot_title(selected_snapshot)
            if selected_snapshot is not None
            else ""
        ),
        "selected_summary": selected_summary,
        "selected_issues": selected_issues,
        "selected_analysis_notes": selected_notes,
        "selected_connector_breakdown": selected_connector_breakdown,
        "selected_repair_items": selected_repair_items,
        "selected_repair_items_preview": selected_repair_items[:15],
        "selected_repair_item_count": len(selected_repair_items),
        "high_risk_items": high_risk_items[:12],
        "snapshots": trend_rows,
        "selected_delta": {
            "missing_email": _int_metric(selected_summary, "users_missing_email")
            - _int_metric(previous_summary, "users_missing_email"),
            "missing_employee_id": _int_metric(selected_summary, "users_missing_employee_id")
            - _int_metric(previous_summary, "users_missing_employee_id"),
            "department_anomaly_count": _department_anomaly_count(selected_summary)
            - _department_anomaly_count(previous_summary),
            "naming_risk_count": _naming_risk_count(selected_summary)
            - _naming_risk_count(previous_summary),
            "duplicate_identifier_count": (
                _int_metric(selected_summary, "duplicate_email_count")
                + _int_metric(selected_summary, "duplicate_employee_id_count")
                - _int_metric(previous_summary, "duplicate_email_count")
                - _int_metric(previous_summary, "duplicate_employee_id_count")
            ),
        },
    }


def build_data_quality_export_rows(snapshot: Optional[DataQualitySnapshotRecord]) -> list[list[str]]:
    if snapshot is None or not isinstance(snapshot.snapshot, dict):
        return []
    rows: list[list[str]] = []
    for item in list(snapshot.snapshot.get("repair_items") or []):
        rows.append(
            [
                str(item.get("key") or ""),
                str(item.get("label") or ""),
                str(item.get("severity") or ""),
                str(item.get("title") or ""),
                str(item.get("source_user_id") or ""),
                ", ".join(
                    str(value or "").strip()
                    for value in list(item.get("source_user_ids") or [])
                    if str(value or "").strip()
                ),
                str(item.get("display_name") or ""),
                str(item.get("connector_id") or ""),
                str(item.get("connector_name") or ""),
                str(item.get("detail") or ""),
                str(item.get("action") or ""),
            ]
        )
    return rows
