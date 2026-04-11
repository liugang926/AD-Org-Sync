from __future__ import annotations

from typing import Any, Dict, Optional

from sync_app.core.exception_rules import (
    get_exception_rule_match_type,
    normalize_exception_match_value,
    normalize_exception_rule_type,
)
from sync_app.core.models import SyncConflictRecord, SyncExceptionRuleRecord, SyncPlanReviewRecord
from sync_app.storage.local_db import BaseRepository, dumps_json, normalize_org_id, utcnow_iso


class SyncConflictRepository(BaseRepository):
    def add_conflict(
        self,
        *,
        job_id: str,
        conflict_type: str,
        source_id: str,
        message: str,
        severity: str = "warning",
        status: str = "open",
        target_key: Optional[str] = None,
        resolution_hint: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> int:
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sync_conflicts (
                  job_id, conflict_type, severity, status, source_id,
                  target_key, message, resolution_hint, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    conflict_type,
                    severity,
                    status,
                    source_id,
                    target_key,
                    message,
                    resolution_hint,
                    dumps_json(details),
                    utcnow_iso(),
                ),
            )
            return int(cursor.lastrowid)

    def list_conflicts_for_job(self, job_id: str, limit: int = 500) -> list[SyncConflictRecord]:
        rows, _total = self.list_conflicts_for_job_page(job_id, limit=limit, offset=0)
        return rows

    def list_conflicts_for_job_page(
        self,
        job_id: str,
        *,
        limit: int = 500,
        offset: int = 0,
    ) -> tuple[list[SyncConflictRecord], int]:
        total = self._fetchcount(
            """
            SELECT COUNT(*)
            FROM sync_conflicts
            WHERE job_id = ?
            """,
            (job_id,),
        )
        rows = self._fetchall(
            """
            SELECT *
            FROM sync_conflicts
            WHERE job_id = ?
            ORDER BY created_at ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (job_id, int(limit), max(int(offset), 0)),
        )
        return [SyncConflictRecord.from_row(row) for row in rows], total

    def list_conflict_records(
        self,
        *,
        limit: int = 500,
        job_id: Optional[str] = None,
        status: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> list[SyncConflictRecord]:
        rows, _total = self.list_conflict_records_page(
            limit=limit,
            offset=0,
            job_id=job_id,
            status=status,
            org_id=org_id,
        )
        return rows

    def list_conflict_records_page(
        self,
        *,
        limit: int = 500,
        offset: int = 0,
        job_id: Optional[str] = None,
        status: Optional[str] = None,
        query: str = "",
        org_id: Optional[str] = None,
    ) -> tuple[list[SyncConflictRecord], int]:
        normalized_org_id = normalize_org_id(org_id)
        if normalized_org_id:
            sql = """
                SELECT c.*
                FROM sync_conflicts AS c
                INNER JOIN sync_jobs AS j ON j.job_id = c.job_id
                WHERE j.org_id = ?
            """
            count_sql = """
                SELECT COUNT(*)
                FROM sync_conflicts AS c
                INNER JOIN sync_jobs AS j ON j.job_id = c.job_id
                WHERE j.org_id = ?
            """
            params: list[Any] = [normalized_org_id]
        else:
            sql = """
                SELECT *
                FROM sync_conflicts
                WHERE 1 = 1
            """
            count_sql = """
                SELECT COUNT(*)
                FROM sync_conflicts
                WHERE 1 = 1
            """
            params = []
        normalized_query = str(query or "").strip().lower()
        if job_id:
            conflict_job_column = "c.job_id" if normalized_org_id else "job_id"
            sql += f" AND {conflict_job_column} = ?"
            count_sql += f" AND {conflict_job_column} = ?"
            params.append(job_id)
        if status:
            conflict_status_column = "c.status" if normalized_org_id else "status"
            sql += f" AND {conflict_status_column} = ?"
            count_sql += f" AND {conflict_status_column} = ?"
            params.append(status)
        if normalized_query:
            conflict_type_column = "c.conflict_type" if normalized_org_id else "conflict_type"
            source_id_column = "c.source_id" if normalized_org_id else "source_id"
            target_key_column = "c.target_key" if normalized_org_id else "target_key"
            message_column = "c.message" if normalized_org_id else "message"
            query_clause = (
                " AND ("
                f"LOWER({conflict_type_column}) LIKE ? OR "
                f"LOWER({source_id_column}) LIKE ? OR "
                f"LOWER(COALESCE({target_key_column}, '')) LIKE ? OR "
                f"LOWER({message_column}) LIKE ?"
                ")"
            )
            sql += query_clause
            count_sql += query_clause
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 4)
        total = self._fetchcount(count_sql, tuple(params))
        if normalized_org_id:
            sql += " ORDER BY c.created_at DESC, c.id DESC LIMIT ? OFFSET ?"
        else:
            sql += " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
        params.extend([int(limit), max(int(offset), 0)])
        rows = self._fetchall(sql, tuple(params))
        return [SyncConflictRecord.from_row(row) for row in rows], total

    def get_conflict_record(self, conflict_id: int, *, org_id: Optional[str] = None) -> Optional[SyncConflictRecord]:
        normalized_org_id = normalize_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT c.*
                FROM sync_conflicts AS c
                INNER JOIN sync_jobs AS j ON j.job_id = c.job_id
                WHERE c.id = ?
                  AND j.org_id = ?
                LIMIT 1
                """,
                (int(conflict_id), normalized_org_id),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM sync_conflicts
                WHERE id = ?
                LIMIT 1
                """,
                (int(conflict_id),),
            )
        if not row:
            return None
        return SyncConflictRecord.from_row(row)

    def count_open_conflicts_for_job(self, job_id: str) -> int:
        row = self._fetchone(
            """
            SELECT COUNT(*) AS total
            FROM sync_conflicts
            WHERE job_id = ?
              AND status = 'open'
            """,
            (job_id,),
        )
        return int(row["total"]) if row else 0

    def update_conflict_status(
        self,
        conflict_id: int,
        *,
        status: str,
        resolution_payload: Optional[Dict[str, Any]] = None,
        resolved_at: Optional[str] = None,
    ) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE sync_conflicts
                SET status = ?,
                    resolution_payload_json = ?,
                    resolved_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    dumps_json(resolution_payload),
                    resolved_at,
                    int(conflict_id),
                ),
            )

    def resolve_open_conflicts_for_source(
        self,
        *,
        job_id: str,
        source_id: str,
        resolution_payload: Optional[Dict[str, Any]] = None,
        resolved_at: Optional[str] = None,
    ) -> int:
        with self.db.transaction() as conn:
            rows = conn.execute(
                """
                SELECT id
                FROM sync_conflicts
                WHERE job_id = ?
                  AND source_id = ?
                  AND status = 'open'
                ORDER BY id ASC
                """,
                (job_id, source_id),
            ).fetchall()
            for row in rows:
                payload = dict(resolution_payload or {})
                payload["resolved_conflict_id"] = int(row["id"])
                conn.execute(
                    """
                    UPDATE sync_conflicts
                    SET status = 'resolved',
                        resolution_payload_json = ?,
                        resolved_at = ?
                    WHERE id = ?
                    """,
                    (
                        dumps_json(payload),
                        resolved_at,
                        int(row["id"]),
                    ),
                )
            return len(rows)


class SyncPlanReviewRepository(BaseRepository):
    def upsert_review_request(
        self,
        *,
        job_id: str,
        plan_fingerprint: str,
        config_snapshot_hash: str,
        high_risk_operation_count: int,
    ) -> None:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_plan_reviews (
                  job_id, plan_fingerprint, config_snapshot_hash, high_risk_operation_count,
                  status, created_at
                ) VALUES (?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(job_id) DO UPDATE SET
                  plan_fingerprint = excluded.plan_fingerprint,
                  config_snapshot_hash = excluded.config_snapshot_hash,
                  high_risk_operation_count = excluded.high_risk_operation_count,
                  status = 'pending',
                  reviewer_username = NULL,
                  review_notes = NULL,
                  reviewed_at = NULL,
                  expires_at = NULL
                """,
                (
                    job_id,
                    plan_fingerprint,
                    config_snapshot_hash,
                    int(high_risk_operation_count),
                    now,
                ),
            )

    def get_review_record_by_job_id(self, job_id: str) -> Optional[SyncPlanReviewRecord]:
        row = self._fetchone(
            """
            SELECT *
            FROM sync_plan_reviews
            WHERE job_id = ?
            LIMIT 1
            """,
            (job_id,),
        )
        if not row:
            return None
        return SyncPlanReviewRecord.from_row(row)

    def approve_review(
        self,
        job_id: str,
        *,
        reviewer_username: str,
        review_notes: str = "",
        expires_at: Optional[str] = None,
    ) -> None:
        reviewed_at = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE sync_plan_reviews
                SET status = 'approved',
                    reviewer_username = ?,
                    review_notes = ?,
                    reviewed_at = ?,
                    expires_at = ?
                WHERE job_id = ?
                """,
                (
                    reviewer_username,
                    review_notes,
                    reviewed_at,
                    expires_at,
                    job_id,
                ),
            )

    def find_matching_approved_review(
        self,
        *,
        plan_fingerprint: str,
        config_snapshot_hash: str,
        now_iso: str,
    ) -> Optional[SyncPlanReviewRecord]:
        row = self._fetchone(
            """
            SELECT *
            FROM sync_plan_reviews
            WHERE plan_fingerprint = ?
              AND config_snapshot_hash = ?
              AND status = 'approved'
              AND (expires_at IS NULL OR expires_at >= ?)
            ORDER BY reviewed_at DESC, id DESC
            LIMIT 1
            """,
            (
                plan_fingerprint,
                config_snapshot_hash,
                now_iso,
            ),
        )
        if not row:
            return None
        return SyncPlanReviewRecord.from_row(row)


class SyncExceptionRuleRepository(BaseRepository):
    @staticmethod
    def _active_rule_clause() -> str:
        return "(expires_at IS NULL OR expires_at = '' OR expires_at >= ?)"

    def _normalize_rule_inputs(
        self,
        *,
        rule_type: str,
        match_value: str,
        match_type: Optional[str] = None,
    ) -> tuple[str, str, str]:
        normalized_rule_type = normalize_exception_rule_type(rule_type)
        if not normalized_rule_type:
            raise ValueError("unsupported exception rule_type")

        expected_match_type = get_exception_rule_match_type(normalized_rule_type)
        normalized_match_type = str(match_type or expected_match_type).strip().lower()
        if normalized_match_type != expected_match_type:
            raise ValueError("exception match_type does not match rule_type")

        normalized_match_value = normalize_exception_match_value(normalized_match_type, match_value)
        if not normalized_match_value:
            raise ValueError("exception match_value is required")

        return normalized_rule_type, normalized_match_type, normalized_match_value

    def get_rule_record(self, rule_id: int, *, org_id: Optional[str] = None) -> Optional[SyncExceptionRuleRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM sync_exception_rules
                WHERE id = ?
                  AND org_id = ?
                LIMIT 1
                """,
                (int(rule_id), normalized_org_id),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM sync_exception_rules
                WHERE id = ?
                LIMIT 1
                """,
                (int(rule_id),),
            )
        if not row:
            return None
        return SyncExceptionRuleRecord.from_row(row)

    def list_rule_records(self, *, org_id: Optional[str] = None) -> list[SyncExceptionRuleRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                """
                SELECT *
                FROM sync_exception_rules
                WHERE org_id = ?
                ORDER BY is_enabled DESC, rule_type ASC, match_value ASC, id ASC
                """,
                (normalized_org_id,),
            )
        else:
            rows = self._fetchall(
                """
                SELECT *
                FROM sync_exception_rules
                ORDER BY org_id ASC, is_enabled DESC, rule_type ASC, match_value ASC, id ASC
                """
            )
        return [SyncExceptionRuleRecord.from_row(row) for row in rows]

    def list_rule_records_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        query: str = "",
        rule_type: str = "",
        status: str = "all",
        org_id: Optional[str] = None,
    ) -> tuple[list[SyncExceptionRuleRecord], int]:
        normalized_query = str(query or "").strip().lower()
        normalized_status = str(status or "all").strip().lower()
        normalized_rule_type = normalize_exception_rule_type(rule_type)
        now_iso = utcnow_iso()
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if normalized_status == "enabled":
            clauses.append("is_enabled = 1")
            clauses.append(self._active_rule_clause())
            params.append(now_iso)
        elif normalized_status == "disabled":
            clauses.append("(is_enabled = 0 OR NOT " + self._active_rule_clause() + ")")
            params.append(now_iso)
        if normalized_rule_type:
            clauses.append("rule_type = ?")
            params.append(normalized_rule_type)
        if normalized_query:
            clauses.append(
                "("
                "LOWER(rule_type) LIKE ? OR "
                "LOWER(match_type) LIKE ? OR "
                "LOWER(match_value) LIKE ? OR "
                "LOWER(COALESCE(notes, '')) LIKE ?"
                ")"
            )
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 4)
        where_clause = " WHERE " + " AND ".join(clauses)
        total = self._fetchcount(
            f"""
            SELECT COUNT(*)
            FROM sync_exception_rules
            {where_clause}
            """,
            tuple(params),
        )
        rows = self._fetchall(
            f"""
            SELECT *
            FROM sync_exception_rules
            {where_clause}
            ORDER BY is_enabled DESC, rule_type ASC, match_value ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), max(int(offset), 0)),
        )
        return [SyncExceptionRuleRecord.from_row(row) for row in rows], total

    def list_enabled_rule_records(self, *, org_id: Optional[str] = None) -> list[SyncExceptionRuleRecord]:
        now_iso = utcnow_iso()
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                f"""
                SELECT *
                FROM sync_exception_rules
                WHERE org_id = ?
                  AND is_enabled = 1
                  AND {self._active_rule_clause()}
                ORDER BY rule_type ASC, match_value ASC, id ASC
                """,
                (normalized_org_id, now_iso),
            )
        else:
            rows = self._fetchall(
                f"""
                SELECT *
                FROM sync_exception_rules
                WHERE is_enabled = 1
                  AND {self._active_rule_clause()}
                ORDER BY org_id ASC, rule_type ASC, match_value ASC, id ASC
                """,
                (now_iso,),
            )
        return [SyncExceptionRuleRecord.from_row(row) for row in rows]

    def upsert_rule(
        self,
        *,
        rule_type: str,
        match_value: str,
        notes: str = "",
        is_enabled: bool = True,
        match_type: Optional[str] = None,
        expires_at: str = "",
        is_once: bool = False,
        org_id: Optional[str] = None,
    ) -> None:
        normalized_rule_type, normalized_match_type, normalized_match_value = self._normalize_rule_inputs(
            rule_type=rule_type,
            match_type=match_type,
            match_value=match_value,
        )
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_exception_rules (
                  org_id, rule_type, match_type, match_value, notes, is_enabled, expires_at, is_once, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, rule_type, match_type, match_value) DO UPDATE SET
                  notes = excluded.notes,
                  is_enabled = excluded.is_enabled,
                  expires_at = excluded.expires_at,
                  is_once = excluded.is_once,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_rule_type,
                    normalized_match_type,
                    normalized_match_value,
                    str(notes or "").strip(),
                    1 if is_enabled else 0,
                    str(expires_at or "").strip(),
                    1 if is_once else 0,
                    now,
                    now,
                ),
            )

    def consume_rule(
        self,
        *,
        rule_type: str,
        match_value: str,
        match_type: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> None:
        normalized_rule_type, normalized_match_type, normalized_match_value = self._normalize_rule_inputs(
            rule_type=rule_type,
            match_type=match_type,
            match_value=match_value,
        )
        normalized_org_id = self._resolve_org_id(org_id)
        now = utcnow_iso()
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE sync_exception_rules
                    SET last_matched_at = ?,
                        is_enabled = CASE WHEN is_once = 1 THEN 0 ELSE is_enabled END,
                        updated_at = ?
                    WHERE org_id = ?
                      AND rule_type = ?
                      AND match_type = ?
                      AND match_value = ?
                      AND is_enabled = 1
                    """,
                    (
                        now,
                        now,
                        normalized_org_id,
                        normalized_rule_type,
                        normalized_match_type,
                        normalized_match_value,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE sync_exception_rules
                    SET last_matched_at = ?,
                        is_enabled = CASE WHEN is_once = 1 THEN 0 ELSE is_enabled END,
                        updated_at = ?
                    WHERE rule_type = ?
                      AND match_type = ?
                      AND match_value = ?
                      AND is_enabled = 1
                    """,
                    (
                        now,
                        now,
                        normalized_rule_type,
                        normalized_match_type,
                        normalized_match_value,
                    ),
                )

    def set_enabled(self, rule_id: int, enabled: bool, *, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE sync_exception_rules
                    SET is_enabled = ?,
                        updated_at = ?
                    WHERE id = ?
                      AND org_id = ?
                    """,
                    (1 if enabled else 0, utcnow_iso(), int(rule_id), normalized_org_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE sync_exception_rules
                    SET is_enabled = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (1 if enabled else 0, utcnow_iso(), int(rule_id)),
                )

    def delete_rule(self, rule_id: int, *, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    DELETE FROM sync_exception_rules
                    WHERE id = ?
                      AND org_id = ?
                    """,
                    (int(rule_id), normalized_org_id),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM sync_exception_rules
                    WHERE id = ?
                    """,
                    (int(rule_id),),
                )

    def delete_rules_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM sync_exception_rules WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )
