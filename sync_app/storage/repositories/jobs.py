from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

from sync_app.core.models import SyncJobRecord, SyncOperationRecord
from sync_app.storage.local_db import BaseRepository, dumps_json, utcnow_iso


class SyncJobRepository(BaseRepository):
    QUEUED_STATUSES = {"QUEUED"}
    EXECUTION_STATUSES = {"LEASED", "CREATED", "PLANNING", "READY", "RUNNING", "CANCELING"}
    ACTIVE_STATUSES = QUEUED_STATUSES | EXECUTION_STATUSES
    PRE_APPLY_PHASES = {"replay", "prepare", "plan"}

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

    def get_apply_job_for_plan_source(
        self,
        plan_source_job_id: str,
        *,
        org_id: str,
    ) -> Optional[SyncJobRecord]:
        row = self._fetchone(
            """
            SELECT * FROM sync_jobs
            WHERE org_id = ?
              AND execution_mode = 'apply'
              AND plan_source_job_id = ?
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (
                str(org_id or "").strip() or "default",
                str(plan_source_job_id or "").strip(),
            ),
        )
        return SyncJobRecord.from_row(row) if row else None

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

    def create_apply_job_once(
        self,
        *,
        job_id: str,
        org_id: str,
        trigger_type: str,
        status: str,
        requested_by: str,
        requested_config_path: str,
        plan_source_job_id: str,
        started_at: Optional[str] = None,
    ) -> tuple[SyncJobRecord, bool, bool]:
        """Atomically bind one Apply job to one reviewed Dry Run plan."""

        normalized_org_id = str(org_id or "").strip() or "default"
        normalized_plan_source_job_id = str(plan_source_job_id or "").strip()
        if not normalized_plan_source_job_id:
            raise ValueError("plan_source_job_id is required for Apply")
        active_placeholders, active_params = self._normalize_status_values(
            self.ACTIVE_STATUSES
        )
        with self.db.transaction() as conn:
            active_row = conn.execute(
                f"""
                SELECT * FROM sync_jobs
                WHERE org_id = ? AND status IN ({active_placeholders})
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (normalized_org_id, *active_params),
            ).fetchone()
            if active_row:
                return SyncJobRecord.from_row(active_row), False, False
            existing_row = conn.execute(
                """
                SELECT * FROM sync_jobs
                WHERE org_id = ?
                  AND execution_mode = 'apply'
                  AND plan_source_job_id = ?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (normalized_org_id, normalized_plan_source_job_id),
            ).fetchone()
            if existing_row:
                return SyncJobRecord.from_row(existing_row), False, True
            conn.execute(
                """
                INSERT INTO sync_jobs (
                  job_id, org_id, trigger_type, execution_mode, status,
                  requested_by, requested_config_path, plan_source_job_id,
                  started_at
                ) VALUES (?, ?, ?, 'apply', ?, ?, ?, ?, ?)
                """,
                (
                    str(job_id or "").strip(),
                    normalized_org_id,
                    str(trigger_type or "").strip(),
                    str(status or "").strip().upper(),
                    str(requested_by or "").strip(),
                    str(requested_config_path or "").strip(),
                    normalized_plan_source_job_id,
                    str(started_at or utcnow_iso()),
                ),
            )
            created_row = conn.execute(
                "SELECT * FROM sync_jobs WHERE job_id = ? LIMIT 1",
                (str(job_id or "").strip(),),
            ).fetchone()
            if created_row is None:
                raise RuntimeError("Apply job could not be reloaded")
            return SyncJobRecord.from_row(created_row), True, False

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
        plan_source_job_id: Optional[str] = None,
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
        if plan_source_job_id is not None:
            updates.append("plan_source_job_id = ?")
            params.append(str(plan_source_job_id or "").strip())
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

    def mark_phase_started(self, job_id: str, phase_name: str) -> None:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE sync_jobs
                SET current_phase = ?,
                    phase_started_at = ?,
                    phase_updated_at = ?,
                    recovery_hint = ''
                WHERE job_id = ?
                """,
                (str(phase_name or "").strip().lower(), now, now, str(job_id or "").strip()),
            )

    def mark_phase_completed(self, job_id: str, phase_name: str) -> None:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE sync_jobs
                SET current_phase = '',
                    last_completed_phase = ?,
                    phase_updated_at = ?,
                    recovery_hint = ''
                WHERE job_id = ?
                """,
                (str(phase_name or "").strip().lower(), now, str(job_id or "").strip()),
            )

    def mark_phase_failed(self, job_id: str, phase_name: str, recovery_hint: str) -> None:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE sync_jobs
                SET current_phase = ?,
                    phase_updated_at = ?,
                    recovery_hint = ?
                WHERE job_id = ?
                """,
                (
                    str(phase_name or "").strip().lower(),
                    now,
                    str(recovery_hint or "").strip(),
                    str(job_id or "").strip(),
                ),
            )

    def fail_expired_execution_jobs(self) -> list[str]:
        now = utcnow_iso()
        placeholders, params = self._normalize_status_values(self.EXECUTION_STATUSES)
        rows = self._fetchall(
            f"""
            SELECT job_id, current_phase, last_completed_phase
            FROM sync_jobs
            WHERE status IN ({placeholders})
              AND lease_expires_at != ''
              AND lease_expires_at < ?
            ORDER BY started_at ASC, job_id ASC
            """,
            (*params, now),
        )
        expired_job_ids: list[str] = []
        for row in rows:
            expired_job_id = str(row["job_id"] or "").strip()
            if not expired_job_id:
                continue
            expired_job_ids.append(expired_job_id)
            current_phase = str(row["current_phase"] or "").strip().lower()
            last_completed_phase = str(row["last_completed_phase"] or "").strip().lower()
            recovery_action = (
                "enqueue a fresh run; no apply phase was entered"
                if current_phase in self.PRE_APPLY_PHASES or (not current_phase and last_completed_phase != "apply")
                else "inspect operation logs and target state before running a new dry run"
            )
            self.update_job(
                expired_job_id,
                status="FAILED",
                ended=True,
                summary={
                    "error": "job lease expired before completion",
                    "recovery_required": True,
                    "current_phase": current_phase,
                    "last_completed_phase": last_completed_phase,
                    "recovery_action": recovery_action,
                },
                clear_lease=True,
            )
            self.mark_phase_failed(expired_job_id, current_phase or "unknown", recovery_action)
        return expired_job_ids


class SyncEventRepository(BaseRepository):
    @staticmethod
    def _row_to_event(row: Any) -> dict[str, Any]:
        event = dict(row)
        payload = event.get("payload_json")
        if isinstance(payload, str) and payload:
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {"raw": payload}
        elif payload is not None and not isinstance(payload, dict):
            payload = {"raw": payload}
        event["payload"] = payload
        return event

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
        rows = self._fetchall(
            """
            SELECT * FROM sync_events
            WHERE job_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (job_id, int(limit)),
        )
        return [self._row_to_event(row) for row in rows]

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
        return [self._row_to_event(row) for row in rows], total


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

    def count_operation_types_for_job(self, job_id: str) -> dict[str, int]:
        rows = self._fetchall(
            """
            SELECT operation_type, COUNT(*) AS operation_count
            FROM planned_operations
            WHERE job_id = ?
            GROUP BY operation_type
            """,
            (job_id,),
        )
        return {
            str(row["operation_type"] or ""): int(row["operation_count"] or 0)
            for row in rows
        }

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

    def list_user_operations_for_jobs(
        self,
        job_ids: Iterable[str],
        *,
        source_user_ids: Iterable[str] = (),
    ) -> list[dict[str, Any]]:
        normalized_jobs = sorted(
            {str(value or "").strip() for value in job_ids if str(value or "").strip()}
        )
        if not normalized_jobs:
            return []
        normalized_users = sorted(
            {str(value or "").strip() for value in source_user_ids if str(value or "").strip()}
        )
        rows: list[Any] = []
        user_chunks = [normalized_users[index : index + 300] for index in range(0, len(normalized_users), 300)] or [[]]
        for job_index in range(0, len(normalized_jobs), 200):
            job_chunk = normalized_jobs[job_index : job_index + 200]
            for user_chunk in user_chunks:
                clauses = [
                    f"job_id IN ({','.join('?' for _ in job_chunk)})",
                    "object_type IN ('user', 'user_binding')",
                ]
                params: list[Any] = [*job_chunk]
                if user_chunk:
                    clauses.append(
                        f"source_id IN ({','.join('?' for _ in user_chunk)})"
                    )
                    params.extend(user_chunk)
                rows.extend(
                    self._fetchall(
                        f"""
                        SELECT * FROM planned_operations
                        WHERE {" AND ".join(clauses)}
                        ORDER BY created_at ASC, id ASC
                        """,
                        tuple(params),
                    )
                )
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            desired_state = item.pop("desired_state_json", None)
            if isinstance(desired_state, str) and desired_state:
                try:
                    desired_state = json.loads(desired_state)
                except json.JSONDecodeError:
                    desired_state = {}
            item["desired_state"] = desired_state if isinstance(desired_state, dict) else {}
            result.append(item)
        return result


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

    @staticmethod
    def _identity_evidence_row(row: Any) -> dict[str, Any]:
        item = dict(row)
        details = item.pop("details_json", None)
        summary = item.pop("summary_json", None)
        for key, value in (("details", details), ("job_summary", summary)):
            if isinstance(value, str) and value:
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    value = {}
            item[key] = value if isinstance(value, dict) else {}
        return item

    def list_identity_resolution_evidence_for_job(
        self,
        job_id: str,
        *,
        org_id: str,
    ) -> list[dict[str, Any]]:
        rows = self._fetchall(
            """
            SELECT l.*, j.org_id, j.execution_mode, j.status AS job_status,
                   j.started_at AS job_started_at, j.ended_at AS job_ended_at,
                   j.config_snapshot_hash, j.summary_json,
                   COALESCE(s.provider_id, '') AS provider_id,
                   COALESCE(s.connector_id, '') AS scope_connector_id,
                   COALESCE(s.source_snapshot_fingerprint, '') AS source_snapshot_fingerprint,
                   COALESCE(s.selection_fingerprint, '') AS selection_fingerprint
            FROM sync_operation_logs AS l
            JOIN sync_jobs AS j ON j.job_id = l.job_id
            LEFT JOIN sync_job_source_scopes AS s ON s.job_id = l.job_id
            WHERE l.job_id = ?
              AND j.org_id = ?
              AND l.operation_type = 'resolve_identity_binding'
            ORDER BY l.created_at ASC, l.id ASC
            """,
            (str(job_id or "").strip(), self._resolve_org_id(org_id) or "default"),
        )
        return [self._identity_evidence_row(row) for row in rows]

    def list_latest_identity_resolution_evidence(
        self,
        source_user_ids: Iterable[str],
        *,
        org_id: str,
        source_provider: str,
        connector_ids: Iterable[str] = (),
        execution_mode: str | None = None,
        successful_only: bool = False,
        chunk_size: int = 350,
    ) -> list[dict[str, Any]]:
        """Return structured resolution rows for bounded source identities.

        Results include all recent candidates so the presentation service can
        detect ambiguity and choose the newest admissible job without parsing
        human-readable log messages.
        """

        normalized_ids = sorted(
            {str(value or "").strip() for value in source_user_ids if str(value or "").strip()}
        )
        if not normalized_ids:
            return []
        normalized_connectors = {
            str(value or "").strip() for value in connector_ids if str(value or "").strip()
        }
        normalized_mode = str(execution_mode or "").strip().lower()
        provider = str(source_provider or "").strip().lower()
        safe_chunk_size = min(max(int(chunk_size or 350), 1), 350)
        evidence: list[dict[str, Any]] = []
        for index in range(0, len(normalized_ids), safe_chunk_size):
            chunk = normalized_ids[index : index + safe_chunk_size]
            clauses = [
                "j.org_id = ?",
                "l.operation_type = 'resolve_identity_binding'",
                f"l.source_id IN ({','.join('?' for _ in chunk)})",
            ]
            params: list[Any] = [self._resolve_org_id(org_id) or "default", *chunk]
            if normalized_mode:
                clauses.append("LOWER(j.execution_mode) = ?")
                params.append(normalized_mode)
            if successful_only:
                clauses.append("j.status = 'COMPLETED'")
            rows = self._fetchall(
                f"""
                SELECT l.*, j.org_id, j.execution_mode, j.status AS job_status,
                       j.started_at AS job_started_at, j.ended_at AS job_ended_at,
                       j.config_snapshot_hash, j.summary_json,
                       COALESCE(s.provider_id, '') AS provider_id,
                       COALESCE(s.connector_id, '') AS scope_connector_id,
                       COALESCE(s.source_snapshot_fingerprint, '') AS source_snapshot_fingerprint,
                       COALESCE(s.selection_fingerprint, '') AS selection_fingerprint
                FROM sync_operation_logs AS l
                JOIN sync_jobs AS j ON j.job_id = l.job_id
                LEFT JOIN sync_job_source_scopes AS s ON s.job_id = l.job_id
                WHERE {" AND ".join(clauses)}
                ORDER BY j.started_at DESC, l.created_at DESC, l.id DESC
                """,
                tuple(params),
            )
            for row in rows:
                item = self._identity_evidence_row(row)
                details = dict(item.get("details") or {})
                item_provider = str(
                    item.get("provider_id") or details.get("source_provider") or ""
                ).strip().lower()
                item_connector = str(
                    details.get("connector_id")
                    or item.get("scope_connector_id")
                    or "default"
                ).strip()
                if provider and item_provider != provider:
                    continue
                if normalized_connectors and item_connector not in normalized_connectors:
                    continue
                item["resolved_connector_id"] = item_connector
                evidence.append(item)
        return evidence

    def list_user_operation_evidence_for_jobs(
        self,
        job_ids: Iterable[str],
        *,
        org_id: str,
        source_user_ids: Iterable[str] = (),
    ) -> list[dict[str, Any]]:
        normalized_jobs = sorted(
            {str(value or "").strip() for value in job_ids if str(value or "").strip()}
        )
        if not normalized_jobs:
            return []
        normalized_users = sorted(
            {str(value or "").strip() for value in source_user_ids if str(value or "").strip()}
        )
        rows: list[Any] = []
        user_chunks = [normalized_users[index : index + 300] for index in range(0, len(normalized_users), 300)] or [[]]
        for job_index in range(0, len(normalized_jobs), 200):
            job_chunk = normalized_jobs[job_index : job_index + 200]
            for user_chunk in user_chunks:
                clauses = [
                    "j.org_id = ?",
                    f"l.job_id IN ({','.join('?' for _ in job_chunk)})",
                    "l.object_type = 'user'",
                    "l.stage_name = 'apply'",
                ]
                params: list[Any] = [
                    self._resolve_org_id(org_id) or "default",
                    *job_chunk,
                ]
                if user_chunk:
                    clauses.append(
                        f"l.source_id IN ({','.join('?' for _ in user_chunk)})"
                    )
                    params.extend(user_chunk)
                rows.extend(
                    self._fetchall(
                        f"""
                        SELECT l.*, j.execution_mode, j.status AS job_status,
                               j.started_at AS job_started_at, j.ended_at AS job_ended_at,
                               j.summary_json
                        FROM sync_operation_logs AS l
                        JOIN sync_jobs AS j ON j.job_id = l.job_id
                        WHERE {" AND ".join(clauses)}
                        ORDER BY j.started_at DESC, l.created_at DESC, l.id DESC
                        """,
                        tuple(params),
                    )
                )
        rows.sort(
            key=lambda row: (
                str(row["job_started_at"] or ""),
                str(row["created_at"] or ""),
                int(row["id"] or 0),
            ),
            reverse=True,
        )
        return [self._identity_evidence_row(row) for row in rows]
