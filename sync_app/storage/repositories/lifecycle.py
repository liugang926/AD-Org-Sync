from __future__ import annotations

import json
from typing import Any, Dict, Iterable, Optional

from sync_app.core.models import OffboardingRecord, UserLifecycleRecord
from sync_app.storage.local_db import BaseRepository, dumps_json, utcnow_iso


class OffboardingQueueRepository(BaseRepository):
    def get_record(
        self,
        *,
        connector_id: str,
        ad_username: str,
        org_id: Optional[str] = None,
    ) -> Optional[OffboardingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM offboarding_queue
                WHERE org_id = ?
                  AND connector_id = ?
                  AND LOWER(ad_username) = LOWER(?)
                LIMIT 1
                """,
                (normalized_org_id, str(connector_id or "default").strip() or "default", str(ad_username or "").strip()),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM offboarding_queue
                WHERE connector_id = ?
                  AND LOWER(ad_username) = LOWER(?)
                ORDER BY org_id ASC, id ASC
                LIMIT 1
                """,
                (str(connector_id or "default").strip() or "default", str(ad_username or "").strip()),
            )
        if not row:
            return None
        return OffboardingRecord.from_row(row)

    def list_due_records(self, *, due_at: str, org_id: Optional[str] = None) -> list[OffboardingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                """
                SELECT *
                FROM offboarding_queue
                WHERE org_id = ?
                  AND status = 'pending'
                  AND due_at <= ?
                ORDER BY due_at ASC, id ASC
                """,
                (normalized_org_id, due_at),
            )
        else:
            rows = self._fetchall(
                """
                SELECT *
                FROM offboarding_queue
                WHERE status = 'pending'
                  AND due_at <= ?
                ORDER BY org_id ASC, due_at ASC, id ASC
                """,
                (due_at,),
            )
        return [OffboardingRecord.from_row(row) for row in rows]

    def list_pending_records(self, *, org_id: Optional[str] = None) -> list[OffboardingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                """
                SELECT *
                FROM offboarding_queue
                WHERE org_id = ?
                  AND status = 'pending'
                ORDER BY due_at ASC, id ASC
                """,
                (normalized_org_id,),
            )
        else:
            rows = self._fetchall(
                """
                SELECT *
                FROM offboarding_queue
                WHERE status = 'pending'
                ORDER BY org_id ASC, due_at ASC, id ASC
                """
            )
        return [OffboardingRecord.from_row(row) for row in rows]

    def upsert_pending(
        self,
        *,
        connector_id: str,
        source_user_id: str,
        ad_username: str,
        due_at: str,
        org_id: Optional[str] = None,
        reason: str = "",
        manager_userids: Iterable[str] = (),
        last_job_id: str = "",
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_connector = str(connector_id or "default").strip() or "default"
        normalized_username = str(ad_username or "").strip()
        normalized_source_user_id = str(source_user_id or "").strip()
        if not normalized_username:
            raise ValueError("ad_username is required")
        manager_values = [str(value).strip() for value in manager_userids if str(value).strip()]
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO offboarding_queue (
                  org_id, connector_id, source_user_id, ad_username, status, reason, manager_userids_json,
                  first_missing_at, due_at, last_job_id, updated_at
                ) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, connector_id, ad_username) DO UPDATE SET
                  source_user_id = excluded.source_user_id,
                  status = 'pending',
                  reason = excluded.reason,
                  manager_userids_json = excluded.manager_userids_json,
                  due_at = excluded.due_at,
                  last_job_id = excluded.last_job_id,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_connector,
                    normalized_source_user_id,
                    normalized_username,
                    str(reason or "").strip(),
                    json.dumps(manager_values, ensure_ascii=False),
                    now,
                    due_at,
                    str(last_job_id or "").strip(),
                    now,
                ),
            )

    def upsert_pending_for_source_user(
        self,
        *,
        connector_id: str,
        source_user_id: str,
        ad_username: str,
        due_at: str,
        org_id: Optional[str] = None,
        reason: str = "",
        manager_userids: Iterable[str] = (),
        last_job_id: str = "",
    ) -> None:
        self.upsert_pending(
            connector_id=connector_id,
            source_user_id=source_user_id,
            ad_username=ad_username,
            due_at=due_at,
            org_id=org_id,
            reason=reason,
            manager_userids=manager_userids,
            last_job_id=last_job_id,
        )

    def mark_notified(self, *, connector_id: str, ad_username: str, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE offboarding_queue
                    SET notified_at = ?,
                        updated_at = ?
                    WHERE org_id = ?
                      AND connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    """,
                    (
                        utcnow_iso(),
                        utcnow_iso(),
                        normalized_org_id,
                        str(connector_id or "default").strip() or "default",
                        str(ad_username or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE offboarding_queue
                    SET notified_at = ?,
                        updated_at = ?
                    WHERE connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    """,
                    (
                        utcnow_iso(),
                        utcnow_iso(),
                        str(connector_id or "default").strip() or "default",
                        str(ad_username or "").strip(),
                    ),
                )

    def mark_disabled(
        self,
        *,
        connector_id: str,
        ad_username: str,
        org_id: Optional[str] = None,
        last_job_id: str = "",
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE offboarding_queue
                    SET status = 'disabled',
                        last_job_id = ?,
                        updated_at = ?
                    WHERE org_id = ?
                      AND connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    """,
                    (
                        str(last_job_id or "").strip(),
                        utcnow_iso(),
                        normalized_org_id,
                        str(connector_id or "default").strip() or "default",
                        str(ad_username or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE offboarding_queue
                    SET status = 'disabled',
                        last_job_id = ?,
                        updated_at = ?
                    WHERE connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    """,
                    (
                        str(last_job_id or "").strip(),
                        utcnow_iso(),
                        str(connector_id or "default").strip() or "default",
                        str(ad_username or "").strip(),
                    ),
                )

    def clear_pending(self, *, connector_id: str, ad_username: str, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    DELETE FROM offboarding_queue
                    WHERE org_id = ?
                      AND connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    """,
                    (
                        normalized_org_id,
                        str(connector_id or "default").strip() or "default",
                        str(ad_username or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM offboarding_queue
                    WHERE connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    """,
                    (str(connector_id or "default").strip() or "default", str(ad_username or "").strip()),
                )

    def delete_records_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM offboarding_queue WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )


class UserLifecycleQueueRepository(BaseRepository):
    def get_record(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        org_id: Optional[str] = None,
    ) -> Optional[UserLifecycleRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM user_lifecycle_queue
                WHERE org_id = ?
                  AND lifecycle_type = ?
                  AND connector_id = ?
                  AND source_user_id = ?
                LIMIT 1
                """,
                (
                    normalized_org_id,
                    str(lifecycle_type or "").strip(),
                    str(connector_id or "default").strip() or "default",
                    str(source_user_id or "").strip(),
                ),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM user_lifecycle_queue
                WHERE lifecycle_type = ?
                  AND connector_id = ?
                  AND source_user_id = ?
                ORDER BY org_id ASC, id ASC
                LIMIT 1
                """,
                (
                    str(lifecycle_type or "").strip(),
                    str(connector_id or "default").strip() or "default",
                    str(source_user_id or "").strip(),
                ),
            )
        if not row:
            return None
        return UserLifecycleRecord.from_row(row)

    def get_record_for_source_user(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        org_id: Optional[str] = None,
    ) -> Optional[UserLifecycleRecord]:
        return self.get_record(
            lifecycle_type=lifecycle_type,
            connector_id=connector_id,
            source_user_id=source_user_id,
            org_id=org_id,
        )

    def list_pending_records(
        self,
        *,
        lifecycle_type: str | None = None,
        org_id: Optional[str] = None,
    ) -> list[UserLifecycleRecord]:
        clauses = ["status = 'pending'"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        normalized_lifecycle_type = str(lifecycle_type or "").strip()
        if normalized_lifecycle_type:
            clauses.append("lifecycle_type = ?")
            params.append(normalized_lifecycle_type)
        rows = self._fetchall(
            f"""
            SELECT *
            FROM user_lifecycle_queue
            WHERE {' AND '.join(clauses)}
            ORDER BY effective_at ASC, connector_id ASC, source_user_id ASC, id ASC
            """,
            tuple(params),
        )
        return [UserLifecycleRecord.from_row(row) for row in rows]

    def upsert_pending(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        effective_at: str,
        org_id: Optional[str] = None,
        ad_username: str = "",
        reason: str = "",
        employment_type: str = "",
        sponsor_userid: str = "",
        manager_userids: Iterable[str] = (),
        payload: Optional[Dict[str, Any]] = None,
        last_job_id: str = "",
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_lifecycle_type = str(lifecycle_type or "").strip()
        normalized_connector_id = str(connector_id or "default").strip() or "default"
        normalized_source_user_id = str(source_user_id or "").strip()
        if not normalized_lifecycle_type or not normalized_source_user_id:
            raise ValueError("lifecycle_type and source_user_id are required")
        manager_values = [str(value).strip() for value in manager_userids if str(value).strip()]
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO user_lifecycle_queue (
                  org_id, lifecycle_type, connector_id, source_user_id, ad_username, status, reason,
                  employment_type, sponsor_userid, manager_userids_json, effective_at, last_job_id,
                  payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, lifecycle_type, connector_id, source_user_id) DO UPDATE SET
                  ad_username = excluded.ad_username,
                  status = 'pending',
                  reason = excluded.reason,
                  employment_type = excluded.employment_type,
                  sponsor_userid = excluded.sponsor_userid,
                  manager_userids_json = excluded.manager_userids_json,
                  effective_at = excluded.effective_at,
                  last_job_id = excluded.last_job_id,
                  payload_json = excluded.payload_json,
                  completed_at = NULL,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_lifecycle_type,
                    normalized_connector_id,
                    normalized_source_user_id,
                    str(ad_username or "").strip(),
                    str(reason or "").strip(),
                    str(employment_type or "").strip(),
                    str(sponsor_userid or "").strip(),
                    json.dumps(manager_values, ensure_ascii=False),
                    str(effective_at or "").strip(),
                    str(last_job_id or "").strip(),
                    dumps_json(payload),
                    now,
                ),
            )

    def upsert_pending_for_source_user(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        effective_at: str,
        org_id: Optional[str] = None,
        ad_username: str = "",
        reason: str = "",
        employment_type: str = "",
        sponsor_userid: str = "",
        manager_userids: Iterable[str] = (),
        payload: Optional[Dict[str, Any]] = None,
        last_job_id: str = "",
    ) -> None:
        self.upsert_pending(
            lifecycle_type=lifecycle_type,
            connector_id=connector_id,
            source_user_id=source_user_id,
            effective_at=effective_at,
            org_id=org_id,
            ad_username=ad_username,
            reason=reason,
            employment_type=employment_type,
            sponsor_userid=sponsor_userid,
            manager_userids=manager_userids,
            payload=payload,
            last_job_id=last_job_id,
        )

    def mark_notified(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        org_id: Optional[str] = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        now = utcnow_iso()
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE user_lifecycle_queue
                    SET notified_at = ?,
                        updated_at = ?
                    WHERE org_id = ?
                      AND lifecycle_type = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    """,
                    (
                        now,
                        now,
                        normalized_org_id,
                        str(lifecycle_type or "").strip(),
                        str(connector_id or "default").strip() or "default",
                        str(source_user_id or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE user_lifecycle_queue
                    SET notified_at = ?,
                        updated_at = ?
                    WHERE lifecycle_type = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    """,
                    (
                        now,
                        now,
                        str(lifecycle_type or "").strip(),
                        str(connector_id or "default").strip() or "default",
                        str(source_user_id or "").strip(),
                    ),
                )

    def mark_notified_for_source_user(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        org_id: Optional[str] = None,
    ) -> None:
        self.mark_notified(
            lifecycle_type=lifecycle_type,
            connector_id=connector_id,
            source_user_id=source_user_id,
            org_id=org_id,
        )

    def mark_completed(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        org_id: Optional[str] = None,
        last_job_id: str = "",
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        now = utcnow_iso()
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE user_lifecycle_queue
                    SET status = 'completed',
                        completed_at = ?,
                        last_job_id = ?,
                        updated_at = ?
                    WHERE org_id = ?
                      AND lifecycle_type = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    """,
                    (
                        now,
                        str(last_job_id or "").strip(),
                        now,
                        normalized_org_id,
                        str(lifecycle_type or "").strip(),
                        str(connector_id or "default").strip() or "default",
                        str(source_user_id or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE user_lifecycle_queue
                    SET status = 'completed',
                        completed_at = ?,
                        last_job_id = ?,
                        updated_at = ?
                    WHERE lifecycle_type = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    """,
                    (
                        now,
                        str(last_job_id or "").strip(),
                        now,
                        str(lifecycle_type or "").strip(),
                        str(connector_id or "default").strip() or "default",
                        str(source_user_id or "").strip(),
                    ),
                )

    def mark_completed_for_source_user(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        org_id: Optional[str] = None,
        last_job_id: str = "",
    ) -> None:
        self.mark_completed(
            lifecycle_type=lifecycle_type,
            connector_id=connector_id,
            source_user_id=source_user_id,
            org_id=org_id,
            last_job_id=last_job_id,
        )

    def clear_pending(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        org_id: Optional[str] = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    DELETE FROM user_lifecycle_queue
                    WHERE org_id = ?
                      AND lifecycle_type = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    """,
                    (
                        normalized_org_id,
                        str(lifecycle_type or "").strip(),
                        str(connector_id or "default").strip() or "default",
                        str(source_user_id or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM user_lifecycle_queue
                    WHERE lifecycle_type = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    """,
                    (
                        str(lifecycle_type or "").strip(),
                        str(connector_id or "default").strip() or "default",
                        str(source_user_id or "").strip(),
                    ),
                )

    def clear_pending_for_source_user(
        self,
        *,
        lifecycle_type: str,
        connector_id: str,
        source_user_id: str,
        org_id: Optional[str] = None,
    ) -> None:
        self.clear_pending(
            lifecycle_type=lifecycle_type,
            connector_id=connector_id,
            source_user_id=source_user_id,
            org_id=org_id,
        )

    def delete_records_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM user_lifecycle_queue WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )
