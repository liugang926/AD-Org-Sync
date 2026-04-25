from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional

from sync_app.core.models.base import MappingLikeModel
from sync_app.core.models.directory import (
    DepartmentGroupInfo,
    GroupPolicyEvaluation,
    SourceDirectoryUser,
)


@dataclass(slots=True)
class SkipOperationSummary(MappingLikeModel):
    total: int = 0
    by_action: Dict[str, int] = field(default_factory=dict)
    samples: list[Dict[str, Any]] = field(default_factory=list)
    details: list[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_mapping(cls, value: Any) -> "SkipOperationSummary":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            return cls()
        return cls(
            total=int(value.get("total") or 0),
            by_action=dict(value.get("by_action") or {}),
            samples=list(value.get("samples") or []),
            details=list(value.get("details") or []),
        )

@dataclass(slots=True)
class SyncErrorBuckets(MappingLikeModel):
    department_errors: list[Dict[str, Any]] = field(default_factory=list)
    user_create_errors: list[Dict[str, Any]] = field(default_factory=list)
    user_update_errors: list[Dict[str, Any]] = field(default_factory=list)
    group_add_errors: list[Dict[str, Any]] = field(default_factory=list)
    group_hierarchy_errors: list[Dict[str, Any]] = field(default_factory=list)
    group_relation_cleanup_errors: list[Dict[str, Any]] = field(default_factory=list)
    user_disable_errors: list[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_mapping(cls, value: Any) -> "SyncErrorBuckets":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            return cls()
        return cls(
            department_errors=list(value.get("department_errors") or []),
            user_create_errors=list(value.get("user_create_errors") or []),
            user_update_errors=list(value.get("user_update_errors") or []),
            group_add_errors=list(value.get("group_add_errors") or []),
            group_hierarchy_errors=list(value.get("group_hierarchy_errors") or []),
            group_relation_cleanup_errors=list(value.get("group_relation_cleanup_errors") or []),
            user_disable_errors=list(value.get("user_disable_errors") or []),
        )

@dataclass(slots=True)
class SyncOperationCounters(MappingLikeModel):
    departments_created: int = 0
    departments_existed: int = 0
    users_created: int = 0
    users_updated: int = 0
    users_disabled: int = 0
    groups_assigned: int = 0
    groups_nested: int = 0
    group_relations_removed: int = 0

    @classmethod
    def from_mapping(cls, value: Any) -> "SyncOperationCounters":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            return cls()
        return cls(
            departments_created=int(value.get("departments_created") or 0),
            departments_existed=int(value.get("departments_existed") or 0),
            users_created=int(value.get("users_created") or 0),
            users_updated=int(value.get("users_updated") or 0),
            users_disabled=int(value.get("users_disabled") or 0),
            groups_assigned=int(value.get("groups_assigned") or 0),
            groups_nested=int(value.get("groups_nested") or 0),
            group_relations_removed=int(value.get("group_relations_removed") or 0),
        )

@dataclass(slots=True)
class SyncRunStats(MappingLikeModel):
    execution_mode: str = "apply"
    org_id: str = "default"
    organization_name: str = ""
    organization_config_path: str = ""
    total_users: int = 0
    processed_users: int = 0
    disabled_users: list[str] = field(default_factory=list)
    error_count: int = 0
    log_file: str = ""
    skip_detail_report: str = ""
    operation_log_report: str = ""
    validation_report: str = ""
    db_path: str = ""
    db_backup_dir: str = ""
    db_startup_snapshot_path: str = ""
    db_migration_source_path: str = ""
    db_integrity_check: Dict[str, Any] = field(default_factory=dict)
    job_id: str = ""
    planned_operation_count: int = 0
    executed_operation_count: int = 0
    high_risk_operation_count: int = 0
    conflict_count: int = 0
    review_required: bool = False
    automatic_replay_request_count: int = 0
    automatic_replay_request_ids: list[int] = field(default_factory=list)
    skipped_operations: SkipOperationSummary = field(default_factory=SkipOperationSummary)
    errors: SyncErrorBuckets = field(default_factory=SyncErrorBuckets)
    operations: SyncOperationCounters = field(default_factory=SyncOperationCounters)
    field_ownership_policy: Dict[str, str] = field(default_factory=dict)
    summary: Optional[Dict[str, Any]] = None
    job_summary: Optional[Dict[str, Any]] = None

    @classmethod
    def from_mapping(cls, value: Any) -> "SyncRunStats":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            return cls()
        return cls(
            execution_mode=str(value.get("execution_mode") or "apply"),
            org_id=str(value.get("org_id") or "default"),
            organization_name=str(value.get("organization_name") or ""),
            organization_config_path=str(value.get("organization_config_path") or ""),
            total_users=int(value.get("total_users") or 0),
            processed_users=int(value.get("processed_users") or value.get("users_processed") or 0),
            disabled_users=list(value.get("disabled_users") or []),
            error_count=int(value.get("error_count") or 0),
            log_file=str(value.get("log_file") or ""),
            skip_detail_report=str(value.get("skip_detail_report") or ""),
            operation_log_report=str(value.get("operation_log_report") or ""),
            validation_report=str(value.get("validation_report") or ""),
            db_path=str(value.get("db_path") or ""),
            db_backup_dir=str(value.get("db_backup_dir") or ""),
            db_startup_snapshot_path=str(value.get("db_startup_snapshot_path") or ""),
            db_migration_source_path=str(value.get("db_migration_source_path") or ""),
            db_integrity_check=dict(value.get("db_integrity_check") or {}),
            job_id=str(value.get("job_id") or ""),
            planned_operation_count=int(value.get("planned_operation_count") or 0),
            executed_operation_count=int(value.get("executed_operation_count") or 0),
            high_risk_operation_count=int(value.get("high_risk_operation_count") or 0),
            conflict_count=int(value.get("conflict_count") or 0),
            review_required=bool(value.get("review_required") or False),
            automatic_replay_request_count=int(value.get("automatic_replay_request_count") or 0),
            automatic_replay_request_ids=[
                int(item) for item in list(value.get("automatic_replay_request_ids") or []) if str(item).isdigit()
            ],
            skipped_operations=SkipOperationSummary.from_mapping(value.get("skipped_operations")),
            errors=SyncErrorBuckets.from_mapping(value.get("errors")),
            operations=SyncOperationCounters.from_mapping(value.get("operations")),
            field_ownership_policy=dict(value.get("field_ownership_policy") or {}),
            summary=dict(value.get("summary")) if isinstance(value.get("summary"), dict) else value.get("summary"),
            job_summary=dict(value.get("job_summary")) if isinstance(value.get("job_summary"), dict) else value.get("job_summary"),
        )

@dataclass(slots=True)
class SyncJobSummary:
    job_id: str
    mode: str
    error_count: int
    planned_operation_count: int
    executed_operation_count: int
    high_risk_operation_count: int = 0
    conflict_count: int = 0
    review_required: bool = False
    log_file: str = ""
    db_path: str = ""
    db_backup_dir: str = ""
    db_startup_snapshot_path: str = ""
    db_migration_source_path: str = ""
    summary: Optional[Dict[str, Any]] = None

    @classmethod
    def from_sync_stats(cls, sync_stats: Dict[str, Any]) -> "SyncJobSummary":
        return cls(
            job_id=str(sync_stats.get("job_id") or ""),
            mode=str(sync_stats.get("execution_mode") or ""),
            error_count=int(sync_stats.get("error_count") or 0),
            planned_operation_count=int(sync_stats.get("planned_operation_count") or 0),
            executed_operation_count=int(sync_stats.get("executed_operation_count") or 0),
            high_risk_operation_count=int(sync_stats.get("high_risk_operation_count") or 0),
            conflict_count=int(sync_stats.get("conflict_count") or 0),
            review_required=bool(sync_stats.get("review_required") or False),
            log_file=str(sync_stats.get("log_file") or ""),
            db_path=str(sync_stats.get("db_path") or ""),
            db_backup_dir=str(sync_stats.get("db_backup_dir") or ""),
            db_startup_snapshot_path=str(sync_stats.get("db_startup_snapshot_path") or ""),
            db_migration_source_path=str(sync_stats.get("db_migration_source_path") or ""),
            summary=sync_stats.get("summary"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass(slots=True)
class SyncJobRecord(MappingLikeModel):
    job_id: str = ""
    org_id: str = "default"
    trigger_type: str = ""
    execution_mode: str = ""
    status: str = ""
    requested_by: str = ""
    requested_config_path: str = ""
    plan_source_job_id: str = ""
    app_version: str = ""
    config_snapshot_hash: str = ""
    lease_owner: str = ""
    lease_expires_at: str = ""
    planned_operation_count: int = 0
    executed_operation_count: int = 0
    error_count: int = 0
    summary: Optional[Dict[str, Any]] = None
    started_at: str = ""
    ended_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "SyncJobRecord":
        summary = None
        if "summary" in row.keys():
            summary = row["summary"]
        elif "summary_json" in row.keys():
            summary = row["summary_json"]
        if isinstance(summary, str) and summary:
            try:
                summary = json.loads(summary)
            except json.JSONDecodeError:
                pass
        return cls(
            job_id=str(row["job_id"] or ""),
            org_id=str(row["org_id"] or "default"),
            trigger_type=str(row["trigger_type"] or ""),
            execution_mode=str(row["execution_mode"] or ""),
            status=str(row["status"] or ""),
            requested_by=str(row["requested_by"] or ""),
            requested_config_path=str(row["requested_config_path"] or ""),
            plan_source_job_id=str(row["plan_source_job_id"] or ""),
            app_version=str(row["app_version"] or ""),
            config_snapshot_hash=str(row["config_snapshot_hash"] or ""),
            lease_owner=str(row["lease_owner"] or ""),
            lease_expires_at=str(row["lease_expires_at"] or ""),
            planned_operation_count=int(row["planned_operation_count"] or 0),
            executed_operation_count=int(row["executed_operation_count"] or 0),
            error_count=int(row["error_count"] or 0),
            summary=summary if isinstance(summary, dict) or summary is None else {"raw": summary},
            started_at=str(row["started_at"] or ""),
            ended_at=str(row["ended_at"] or ""),
        )

@dataclass(slots=True)
class SyncOperationRecord(MappingLikeModel):
    id: Optional[int] = None
    job_id: str = ""
    stage_name: str = ""
    object_type: str = ""
    operation_type: str = ""
    source_id: str = ""
    department_id: str = ""
    target_id: str = ""
    target_dn: str = ""
    risk_level: str = "normal"
    status: str = ""
    message: str = ""
    rule_source: str = ""
    reason_code: str = ""
    details: Optional[Dict[str, Any]] = None
    created_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "SyncOperationRecord":
        details = row["details_json"] if "details_json" in row.keys() else None
        if isinstance(details, str) and details:
            try:
                details = json.loads(details)
            except json.JSONDecodeError:
                details = {"raw": details}
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            job_id=str(row["job_id"] or ""),
            stage_name=str(row["stage_name"] or ""),
            object_type=str(row["object_type"] or ""),
            operation_type=str(row["operation_type"] or ""),
            source_id=str(row["source_id"] or ""),
            department_id=str(row["department_id"] or ""),
            target_id=str(row["target_id"] or ""),
            target_dn=str(row["target_dn"] or ""),
            risk_level=str(row["risk_level"] or "normal"),
            status=str(row["status"] or ""),
            message=str(row["message"] or ""),
            rule_source=str(row["rule_source"] or ""),
            reason_code=str(row["reason_code"] or ""),
            details=details if isinstance(details, dict) or details is None else {"raw": details},
            created_at=str(row["created_at"] or ""),
        )

@dataclass(slots=True)
class SyncPlanReviewRecord(MappingLikeModel):
    id: Optional[int] = None
    job_id: str = ""
    plan_fingerprint: str = ""
    config_snapshot_hash: str = ""
    high_risk_operation_count: int = 0
    status: str = "pending"
    reviewer_username: str = ""
    review_notes: str = ""
    created_at: str = ""
    reviewed_at: str = ""
    expires_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "SyncPlanReviewRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            job_id=str(row["job_id"] or ""),
            plan_fingerprint=str(row["plan_fingerprint"] or ""),
            config_snapshot_hash=str(row["config_snapshot_hash"] or ""),
            high_risk_operation_count=int(row["high_risk_operation_count"] or 0),
            status=str(row["status"] or "pending"),
            reviewer_username=str(row["reviewer_username"] or ""),
            review_notes=str(row["review_notes"] or ""),
            created_at=str(row["created_at"] or ""),
            reviewed_at=str(row["reviewed_at"] or ""),
            expires_at=str(row["expires_at"] or ""),
        )

@dataclass(slots=True)
class SyncReplayRequestRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    request_type: str = ""
    execution_mode: str = ""
    status: str = "pending"
    requested_by: str = ""
    target_scope: str = ""
    target_id: str = ""
    trigger_reason: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    started_at: str = ""
    finished_at: str = ""
    last_job_id: str = ""
    result_summary: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: Any) -> "SyncReplayRequestRecord":
        payload = row["payload_json"] if "payload_json" in row.keys() else None
        result_summary = row["result_summary_json"] if "result_summary_json" in row.keys() else None
        try:
            payload = json.loads(payload) if payload else {}
        except json.JSONDecodeError:
            payload = {}
        try:
            result_summary = json.loads(result_summary) if result_summary else {}
        except json.JSONDecodeError:
            result_summary = {}
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            request_type=str(row["request_type"] or ""),
            execution_mode=str(row["execution_mode"] or ""),
            status=str(row["status"] or "pending"),
            requested_by=str(row["requested_by"] or ""),
            target_scope=str(row["target_scope"] or ""),
            target_id=str(row["target_id"] or ""),
            trigger_reason=str(row["trigger_reason"] or ""),
            payload=dict(payload or {}),
            created_at=str(row["created_at"] or ""),
            started_at=str(row["started_at"] or ""),
            finished_at=str(row["finished_at"] or ""),
            last_job_id=str(row["last_job_id"] or ""),
            result_summary=dict(result_summary or {}),
        )
