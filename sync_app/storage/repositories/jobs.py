from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sync_app.core.models import SyncJobRecord, SyncOperationRecord
from sync_app.storage.local_db import BaseRepository, dumps_json, utcnow_iso


class SyncJobRepository(BaseRepository):
    QUEUED_STATUSES = {"QUEUED"}
    EXECUTION_STATUSES = {"LEASED", "CREATED", "PLANNING", "READY", "RUNNING", "CANCELING"}
    ACTIVE_STATUSES = QUEUED_STATUSES | EXECUTION_STATUSES

    @staticmethod
    def _normalize_status_values(statuses: set[str]) -> tuple[str, list[Any]]:
        normalized_statuses = sorted({str(status or "").strip().upper() for status in statuses if str(status or "").strip()})
        placeholders = ",".join(["?"] * len(normalized_statuses))
        return placeholders, list(normalized_statuses)

    def _get_job_by_statuses(
        self,
        *,
        statuses: set[str],
        org_id: Optional[str] = None,
    ):
        placeholders, params = self._normalize_status_values(statuses)
        where_clauses = [f"status IN ({placeholders})"]
        normalized_org_id = str(org_id or "").strip()
        if normalized_org_id:
            where_clauses.append("org_id = ?")
            params.append(normalized_org_id)
        return self._fetchone(
            f"""
            SELECT * FROM sync_jobs
            WHERE {' AND '.join(where_clauses)}
            ORDER BY started_at DESC
            LIMIT 1
            """,
            tuple(params),
        )

    def get_active_job(self, *, org_id: Optional[str] = None):
        return self._get_job_by_statuses(statuses=self.ACTIVE_STATUSES, org_id=org_id)

    def get_active_job_record(self, *, org_id: Optional[str] = None) -> Optional[SyncJobRecord]:
        row = self.get_active_job(org_id=org_id)
        if not row:
            return None
        return SyncJobRecord.from_row(row)

    def get_execution_job(self, *, org_id: Optional[str] = None):
        return self._get_job_by_statuses(statuses=self.EXECUTION_STATUSES, org_id=org_id)

    def get_execution_job_record(self, *, org_id: Optional[str] = None) -> Optional[SyncJobRecord]:
        row = self.get_execution_job(org_id=org_id)
        if not row:
            return None
        return SyncJobRecord.from_row(row)

    def get_job(self, job_id: str):
        return self._fetchone(
            """
            SELECT * FROM sync_jobs
            WHERE job_id = ?
            LIMIT 1
            """,
            (job_id,),
        )

    def get_job_record(self, job_id: str) -> Optional[SyncJobRecord]:
        row = self.get_job(job_id)
        if not row:
            return None
        return SyncJobRecord.from_row(row)

    def list_recent_jobs(self, limit: int = 20, *, org_id: Optional[str] = None):
        normalized_org_id = str(org_id or "").strip()
        if normalized_org_id:
            return self._fetchall(
                """
                SELECT * FROM sync_jobs
                WHERE org_id = ?
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (normalized_org_id, int(limit)),
            )
        return self._fetchall(
            """
            SELECT * FROM sync_jobs
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (int(limit),),
        )

    def list_recent_job_records(self, limit: int = 20, *, org_id: Optional[str] = None) -> list[SyncJobRecord]:
        return [SyncJobRecord.from_row(row) for row in self.list_recent_jobs(limit=limit, org_id=org_id)]

    def count_jobs(self, *, org_id: Optional[str] = None) -> int:
        normalized_org_id = str(org_id or "").strip()
        if normalized_org_id:
            return self._fetchcount("SELECT COUNT(*) FROM sync_jobs WHERE org_id = ?", (normalized_org_id,))
        return self._fetchcount("SELECT COUNT(*) FROM sync_jobs")

    def create_job(
        self,
        job_id: str,
        trigger_type: str,
        execution_mode: str,
        status: str,
        org_id: str = "default",
        requested_by: str = "",
        requested_config_path: str = "",
        app_version: Optional[str] = None,
        plan_source_job_id: Optional[str] = None,
        config_snapshot_hash: Optional[str] = None,
        lease_owner: str = "",
        lease_expires_at: str = "",
        started_at: Optional[str] = None,
    ):
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_jobs (
                  job_id, org_id, trigger_type, execution_mode, status, requested_by, requested_config_path,
                  plan_source_job_id, app_version, config_snapshot_hash, lease_owner, lease_expires_at, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    str(org_id or "").strip() or "default",
                    str(trigger_type or "").strip(),
                    str(execution_mode or "").strip(),
                    str(status or "").strip().upper(),
                    str(requested_by or "").strip(),
                    str(requested_config_path or "").strip(),
                    plan_source_job_id,
                    app_version,
                    config_snapshot_hash,
                    str(lease_owner or "").strip(),
                    str(lease_expires_at or "").strip(),
                    str(started_at or utcnow_iso()),
                ),
            )

    def update_job(
        self,
        job_id: str,
        *,
        status: Optional[str] = None,
        planned_operation_count: Optional[int] = None,
        executed_operation_count: Optional[int] = None,
        error_count: Optional[int] = None,
        summary: Optional[Dict[str, Any]] = None,
        ended: bool = False,
        trigger_type: Optional[str] = None,
        execution_mode: Optional[str] = None,
        app_version: Optional[str] = None,
        config_snapshot_hash: Optional[str] = None,
        requested_by: Optional[str] = None,
        requested_config_path: Optional[str] = None,
        lease_owner: Optional[str] = None,
        lease_expires_at: Optional[str] = None,
        clear_lease: bool = False,
        started_at: Optional[str] = None,
        clear_summary: bool = False,
    ):
        updates = []
        params: list[Any] = []
        if status is not None:
            updates.append("status = ?")
            params.append(str(status or "").strip().upper())
        if planned_operation_count is not None:
            updates.append("planned_operation_count = ?")
            params.append(planned_operation_count)
        if executed_operation_count is not None:
            updates.append("executed_operation_count = ?")
            params.append(executed_operation_count)
        if error_count is not None:
            updates.append("error_count = ?")
            params.append(error_count)
        if summary is not None:
            updates.append("summary_json = ?")
            params.append(dumps_json(summary))
        elif clear_summary:
            updates.append("summary_json = NULL")
        if trigger_type is not None:
            updates.append("trigger_type = ?")
            params.append(str(trigger_type or "").strip())
        if execution_mode is not None:
            updates.append("execution_mode = ?")
            params.append(str(execution_mode or "").strip())
        if app_version is not None:
            updates.append("app_version = ?")
            params.append(str(app_version or "").strip())
        if config_snapshot_hash is not None:
            updates.append("config_snapshot_hash = ?")
            params.append(str(config_snapshot_hash or "").strip())
        if requested_by is not None:
            updates.append("requested_by = ?")
            params.append(str(requested_by or "").strip())
        if requested_config_path is not None:
            updates.append("requested_config_path = ?")
            params.append(str(requested_config_path or "").strip())
        if started_at is not None:
            updates.append("started_at = ?")
            params.append(str(started_at or "").strip())
        if lease_owner is not None:
            updates.append("lease_owner = ?")
            params.append(str(lease_owner or "").strip())
        if lease_expires_at is not None:
            updates.append("lease_expires_at = ?")
            params.append(str(lease_expires_at or "").strip())
        if clear_lease:
            updates.append("lease_owner = ''")
            updates.append("lease_expires_at = ''")
        if ended:
            updates.append("ended_at = ?")
            params.append(utcnow_iso())

        if not updates:
            return

        params.append(job_id)
        with self.db.transaction() as conn:
            conn.execute(
                f"UPDATE sync_jobs SET {', '.join(updates)} WHERE job_id = ?",
                tuple(params),
            )

    def claim_job(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_seconds: int = 300,
    ) -> Optional[SyncJobRecord]:
        normalized_job_id = str(job_id or "").strip()
        normalized_worker_id = str(worker_id or "").strip()
        if not normalized_job_id or not normalized_worker_id:
            return None

        lease_expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=max(int(lease_seconds or 0), 1))
        ).isoformat(timespec="seconds")
        placeholders, params = self._normalize_status_values(self.EXECUTION_STATUSES)
        with self.db.transaction() as conn:
            blocking_row = conn.execute(
                f"""
                SELECT job_id
                FROM sync_jobs
                WHERE status IN ({placeholders})
                  AND job_id != ?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (*params, normalized_job_id),
            ).fetchone()
            if blocking_row:
                return None
            updated = conn.execute(
                """
                UPDATE sync_jobs
                SET status = 'LEASED',
                    lease_owner = ?,
                    lease_expires_at = ?
                WHERE job_id = ?
                  AND status = 'QUEUED'
                """,
                (normalized_worker_id, lease_expires_at, normalized_job_id),
            ).rowcount
            if not updated:
                row = conn.execute(
                    """
                    SELECT *
                    FROM sync_jobs
                    WHERE job_id = ?
                    LIMIT 1
                    """,
                    (normalized_job_id,),
                ).fetchone()
                if not row:
                    return None
                if str(row["status"] or "").strip().upper() != "LEASED":
                    return None
                if str(row["lease_owner"] or "").strip() != normalized_worker_id:
                    return None
                return SyncJobRecord.from_row(row)

            row = conn.execute(
                """
                SELECT *
                FROM sync_jobs
                WHERE job_id = ?
                LIMIT 1
                """,
                (normalized_job_id,),
            ).fetchone()
        return SyncJobRecord.from_row(row) if row else None

    def claim_next_queued_job(
        self,
        *,
        worker_id: str,
        lease_seconds: int = 300,
    ) -> Optional[SyncJobRecord]:
        normalized_worker_id = str(worker_id or "").strip()
        if not normalized_worker_id:
            return None

        self.fail_expired_execution_jobs()
        if self.get_execution_job():
            return None

        rows = self._fetchall(
            """
            SELECT job_id
            FROM sync_jobs
            WHERE status = 'QUEUED'
            ORDER BY started_at ASC, job_id ASC
            LIMIT 20
            """
        )
        for row in rows:
            claimed = self.claim_job(
                str(row["job_id"] or ""),
                worker_id=normalized_worker_id,
                lease_seconds=lease_seconds,
            )
            if claimed:
                return claimed
        return None

    def renew_lease(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_seconds: int = 300,
    ) -> bool:
        normalized_job_id = str(job_id or "").strip()
        normalized_worker_id = str(worker_id or "").strip()
        if not normalized_job_id or not normalized_worker_id:
            return False

        lease_expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=max(int(lease_seconds or 0), 1))
        ).isoformat(timespec="seconds")
        placeholders, params = self._normalize_status_values(self.EXECUTION_STATUSES)
        with self.db.transaction() as conn:
            updated = conn.execute(
                f"""
                UPDATE sync_jobs
                SET lease_expires_at = ?
                WHERE job_id = ?
                  AND lease_owner = ?
                  AND status IN ({placeholders})
                """,
                (lease_expires_at, normalized_job_id, normalized_worker_id, *params),
            ).rowcount
        return bool(updated)

    def fail_expired_execution_jobs(self) -> list[str]:
        now = utcnow_iso()
        placeholders, params = self._normalize_status_values(self.EXECUTION_STATUSES)
        rows = self._fetchall(
            f"""
            SELECT job_id
            FROM sync_jobs
            WHERE status IN ({placeholders})
              AND lease_expires_at != ''
              AND lease_expires_at < ?
            ORDER BY started_at ASC, job_id ASC
            """,
            (*params, now),
        )
        expired_job_ids = [str(row["job_id"] or "").strip() for row in rows if str(row["job_id"] or "").strip()]
        for expired_job_id in expired_job_ids:
            self.update_job(
                expired_job_id,
                status="FAILED",
                ended=True,
                summary={"error": "job lease expired before completion"},
                clear_lease=True,
            )
        return expired_job_ids


class SyncEventRepository(BaseRepository):
    def add_event(
        self,
        job_id: str,
        level: str,
        event_type: str,
        message: str,
        *,
        stage_name: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ):
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_events (
                  job_id, stage_name, level, event_type, message, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    stage_name,
                    level,
                    event_type,
                    message,
                    dumps_json(payload),
                    utcnow_iso(),
                ),
            )

    def list_events_for_job(self, job_id: str, limit: int = 100):
        return self._fetchall(
            """
            SELECT * FROM sync_events
            WHERE job_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (job_id, int(limit)),
        )

    def list_events_for_job_page(
        self,
        job_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        total = self._fetchcount(
            """
            SELECT COUNT(*)
            FROM sync_events
            WHERE job_id = ?
            """,
            (job_id,),
        )
        rows = self._fetchall(
            """
            SELECT *
            FROM sync_events
            WHERE job_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (job_id, int(limit), max(int(offset), 0)),
        )
        return [dict(row) for row in rows], total


class PlannedOperationRepository(BaseRepository):
    def add_operation(
        self,
        job_id: str,
        object_type: str,
        operation_type: str,
        *,
        source_id: Optional[str] = None,
        department_id: Optional[str] = None,
        target_dn: Optional[str] = None,
        desired_state: Optional[Dict[str, Any]] = None,
        risk_level: str = "normal",
        status: str = "planned",
    ) -> int:
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO planned_operations (
                  job_id, object_type, source_id, department_id, target_dn,
                  operation_type, desired_state_json, risk_level, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    object_type,
                    source_id,
                    department_id,
                    target_dn,
                    operation_type,
                    dumps_json(desired_state),
                    risk_level,
                    status,
                    utcnow_iso(),
                ),
            )
            return int(cursor.lastrowid)

    def list_operations_for_job(self, job_id: str, limit: int = 500) -> list[dict[str, Any]]:
        rows, _total = self.list_operations_for_job_page(job_id, limit=limit, offset=0)
        return rows

    def list_operations_for_job_page(
        self,
        job_id: str,
        *,
        limit: int = 500,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        total = self._fetchcount(
            """
            SELECT COUNT(*)
            FROM planned_operations
            WHERE job_id = ?
            """,
            (job_id,),
        )
        rows = self._fetchall(
            """
            SELECT *
            FROM planned_operations
            WHERE job_id = ?
            ORDER BY created_at ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (job_id, int(limit), max(int(offset), 0)),
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            desired_state = row["desired_state_json"] if "desired_state_json" in row.keys() else None
            if isinstance(desired_state, str) and desired_state:
                try:
                    desired_state = json.loads(desired_state)
                except json.JSONDecodeError:
                    desired_state = {"raw": desired_state}
            result.append(
                {
                    "id": int(row["id"]),
                    "job_id": str(row["job_id"] or ""),
                    "object_type": str(row["object_type"] or ""),
                    "source_id": str(row["source_id"] or ""),
                    "department_id": str(row["department_id"] or ""),
                    "target_dn": str(row["target_dn"] or ""),
                    "operation_type": str(row["operation_type"] or ""),
                    "desired_state": desired_state
                    if isinstance(desired_state, dict) or desired_state is None
                    else {"raw": desired_state},
                    "risk_level": str(row["risk_level"] or "normal"),
                    "status": str(row["status"] or "planned"),
                    "created_at": str(row["created_at"] or ""),
                }
            )
        return result, total


class SyncOperationLogRepository(BaseRepository):
    def add_record(
        self,
        *,
        job_id: str,
        stage_name: str,
        object_type: str,
        operation_type: str,
        status: str,
        message: str,
        source_id: Optional[str] = None,
        department_id: Optional[str] = None,
        target_id: Optional[str] = None,
        target_dn: Optional[str] = None,
        risk_level: str = "normal",
        rule_source: Optional[str] = None,
        reason_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> int:
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sync_operation_logs (
                  job_id, stage_name, object_type, operation_type, source_id,
                  department_id, target_id, target_dn, risk_level, status,
                  message, rule_source, reason_code, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    stage_name,
                    object_type,
                    operation_type,
                    source_id,
                    department_id,
                    target_id,
                    target_dn,
                    risk_level,
                    status,
                    message,
                    rule_source,
                    reason_code,
                    dumps_json(details),
                    utcnow_iso(),
                ),
            )
            return int(cursor.lastrowid)

    def list_records_for_job(self, job_id: str, limit: int = 500) -> list[SyncOperationRecord]:
        rows, _total = self.list_records_for_job_page(job_id, limit=limit, offset=0)
        return rows

    def list_records_for_job_page(
        self,
        job_id: str,
        *,
        limit: int = 500,
        offset: int = 0,
    ) -> tuple[list[SyncOperationRecord], int]:
        total = self._fetchcount(
            """
            SELECT COUNT(*)
            FROM sync_operation_logs
            WHERE job_id = ?
            """,
            (job_id,),
        )
        rows = self._fetchall(
            """
            SELECT *
            FROM sync_operation_logs
            WHERE job_id = ?
            ORDER BY created_at ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (job_id, int(limit), max(int(offset), 0)),
        )
        return [SyncOperationRecord.from_row(row) for row in rows], total
