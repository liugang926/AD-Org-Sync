from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from sync_app.storage.local_db import BaseRepository, dumps_json, utcnow_iso


class ObjectStateRepository(BaseRepository):
    def upsert_state(
        self,
        source_type: str,
        object_type: str,
        source_id: str,
        source_hash: str,
        *,
        org_id: Optional[str] = None,
        display_name: Optional[str] = None,
        target_dn: Optional[str] = None,
        last_job_id: Optional[str] = None,
        last_action: Optional[str] = None,
        last_status: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ):
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO object_sync_state (
                  org_id, source_type, object_type, source_id, source_hash, display_name,
                  target_dn, last_seen_at, last_job_id, last_action, last_status, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, source_type, object_type, source_id) DO UPDATE SET
                  source_hash = excluded.source_hash,
                  display_name = excluded.display_name,
                  target_dn = excluded.target_dn,
                  last_seen_at = excluded.last_seen_at,
                  last_job_id = excluded.last_job_id,
                  last_action = excluded.last_action,
                  last_status = excluded.last_status,
                  extra_json = excluded.extra_json
                """,
                (
                    normalized_org_id,
                    source_type,
                    object_type,
                    source_id,
                    source_hash,
                    display_name,
                    target_dn,
                    utcnow_iso(),
                    last_job_id,
                    last_action,
                    last_status,
                    dumps_json(extra),
                ),
            )

    def get_state(
        self,
        source_type: str,
        object_type: str,
        source_id: str,
        *,
        org_id: Optional[str] = None,
    ):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchone(
                """
                SELECT * FROM object_sync_state
                WHERE org_id = ? AND source_type = ? AND object_type = ? AND source_id = ?
                """,
                (normalized_org_id, source_type, object_type, source_id),
            )
        return self._fetchone(
            """
            SELECT * FROM object_sync_state
            WHERE source_type = ? AND object_type = ? AND source_id = ?
            ORDER BY org_id ASC, id ASC
            LIMIT 1
            """,
            (source_type, object_type, source_id),
        )

    def count_by_type(self, source_type: str, object_type: str, *, org_id: Optional[str] = None) -> int:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT COUNT(*) AS total FROM object_sync_state
                WHERE org_id = ? AND source_type = ? AND object_type = ?
                """,
                (normalized_org_id, source_type, object_type),
            )
        else:
            row = self._fetchone(
                """
                SELECT COUNT(*) AS total FROM object_sync_state
                WHERE source_type = ? AND object_type = ?
                """,
                (source_type, object_type),
            )
        return int(row["total"]) if row else 0

    def delete_missing(
        self,
        source_type: str,
        object_type: str,
        current_ids: Iterable[str],
        *,
        org_id: Optional[str] = None,
    ) -> int:
        current_ids = list(current_ids)
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if current_ids:
                placeholders = ",".join(["?"] * len(current_ids))
                if normalized_org_id:
                    cursor = conn.execute(
                        f"""
                        DELETE FROM object_sync_state
                        WHERE org_id = ? AND source_type = ? AND object_type = ? AND source_id NOT IN ({placeholders})
                        """,
                        (normalized_org_id, source_type, object_type, *current_ids),
                    )
                else:
                    cursor = conn.execute(
                        f"""
                        DELETE FROM object_sync_state
                        WHERE source_type = ? AND object_type = ? AND source_id NOT IN ({placeholders})
                        """,
                        (source_type, object_type, *current_ids),
                    )
            else:
                if normalized_org_id:
                    cursor = conn.execute(
                        """
                        DELETE FROM object_sync_state
                        WHERE org_id = ? AND source_type = ? AND object_type = ?
                        """,
                        (normalized_org_id, source_type, object_type),
                    )
                else:
                    cursor = conn.execute(
                        """
                        DELETE FROM object_sync_state
                        WHERE source_type = ? AND object_type = ?
                        """,
                        (source_type, object_type),
                    )
            return cursor.rowcount

    def delete_states_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM object_sync_state WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )
