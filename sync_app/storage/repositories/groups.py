from __future__ import annotations

from typing import Optional

from sync_app.core.models import CustomManagedGroupBindingRecord, ManagedGroupBindingRecord
from sync_app.storage.local_db import BaseRepository, utcnow_iso


class ManagedGroupBindingRepository(BaseRepository):
    def upsert_binding(
        self,
        department_id: str,
        group_sam: str,
        *,
        org_id: Optional[str] = None,
        parent_department_id: Optional[str] = None,
        group_dn: Optional[str] = None,
        group_cn: Optional[str] = None,
        display_name: Optional[str] = None,
        path_text: Optional[str] = None,
        status: str = "active",
    ):
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO managed_group_bindings (
                  org_id, department_id, parent_department_id, group_sam, group_dn, group_cn,
                  display_name, path_text, status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, department_id) DO UPDATE SET
                  parent_department_id = excluded.parent_department_id,
                  group_sam = excluded.group_sam,
                  group_dn = excluded.group_dn,
                  group_cn = excluded.group_cn,
                  display_name = excluded.display_name,
                  path_text = excluded.path_text,
                  status = excluded.status,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    department_id,
                    parent_department_id,
                    group_sam,
                    group_dn,
                    group_cn,
                    display_name,
                    path_text,
                    status,
                    utcnow_iso(),
                ),
            )

    def get_by_department_id(self, department_id: str, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchone(
                "SELECT * FROM managed_group_bindings WHERE org_id = ? AND department_id = ?",
                (normalized_org_id, department_id),
            )
        return self._fetchone(
            "SELECT * FROM managed_group_bindings WHERE department_id = ? ORDER BY org_id ASC LIMIT 1",
            (department_id,),
        )

    def get_binding_record_by_department_id(
        self,
        department_id: str,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[ManagedGroupBindingRecord]:
        row = self.get_by_department_id(department_id, org_id=org_id)
        if not row:
            return None
        return ManagedGroupBindingRecord.from_row(row)

    def get_by_group_sam(self, group_sam: str, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchone(
                "SELECT * FROM managed_group_bindings WHERE org_id = ? AND group_sam = ?",
                (normalized_org_id, group_sam),
            )
        return self._fetchone(
            "SELECT * FROM managed_group_bindings WHERE group_sam = ? ORDER BY org_id ASC LIMIT 1",
            (group_sam,),
        )

    def get_binding_record_by_group_sam(
        self,
        group_sam: str,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[ManagedGroupBindingRecord]:
        row = self.get_by_group_sam(group_sam, org_id=org_id)
        if not row:
            return None
        return ManagedGroupBindingRecord.from_row(row)

    def list_active_bindings(self, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchall(
                """
                SELECT * FROM managed_group_bindings
                WHERE org_id = ?
                  AND status = 'active'
                ORDER BY department_id
                """,
                (normalized_org_id,),
            )
        return self._fetchall(
            """
            SELECT * FROM managed_group_bindings
            WHERE status = 'active'
            ORDER BY org_id ASC, department_id
            """
        )

    def list_active_binding_records(self, *, org_id: Optional[str] = None) -> list[ManagedGroupBindingRecord]:
        return [ManagedGroupBindingRecord.from_row(row) for row in self.list_active_bindings(org_id=org_id)]

    def delete_bindings_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM managed_group_bindings WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )


class CustomManagedGroupBindingRepository(BaseRepository):
    def get_binding_record(
        self,
        *,
        connector_id: str,
        source_type: str,
        source_key: str,
        org_id: Optional[str] = None,
    ) -> Optional[CustomManagedGroupBindingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM custom_managed_group_bindings
                WHERE org_id = ?
                  AND connector_id = ?
                  AND source_type = ?
                  AND source_key = ?
                LIMIT 1
                """,
                (
                    normalized_org_id,
                    str(connector_id or "default").strip() or "default",
                    str(source_type or "").strip(),
                    str(source_key or "").strip(),
                ),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM custom_managed_group_bindings
                WHERE connector_id = ?
                  AND source_type = ?
                  AND source_key = ?
                ORDER BY org_id ASC, id ASC
                LIMIT 1
                """,
                (
                    str(connector_id or "default").strip() or "default",
                    str(source_type or "").strip(),
                    str(source_key or "").strip(),
                ),
            )
        if not row:
            return None
        return CustomManagedGroupBindingRecord.from_row(row)

    def upsert_binding(
        self,
        *,
        connector_id: str,
        source_type: str,
        source_key: str,
        org_id: Optional[str] = None,
        group_sam: str,
        group_dn: str,
        group_cn: str,
        display_name: str,
        status: str = "active",
        last_seen_at: str = "",
        archived_at: str = "",
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO custom_managed_group_bindings (
                  org_id, connector_id, source_type, source_key, group_sam, group_dn, group_cn,
                  display_name, status, last_seen_at, archived_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, connector_id, source_type, source_key) DO UPDATE SET
                  group_sam = excluded.group_sam,
                  group_dn = excluded.group_dn,
                  group_cn = excluded.group_cn,
                  display_name = excluded.display_name,
                  status = excluded.status,
                  last_seen_at = excluded.last_seen_at,
                  archived_at = excluded.archived_at,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    str(connector_id or "default").strip() or "default",
                    str(source_type or "").strip(),
                    str(source_key or "").strip(),
                    str(group_sam or "").strip(),
                    str(group_dn or "").strip(),
                    str(group_cn or "").strip(),
                    str(display_name or "").strip(),
                    str(status or "active").strip() or "active",
                    str(last_seen_at or now).strip(),
                    str(archived_at or "").strip(),
                    now,
                ),
            )

    def set_status(
        self,
        *,
        connector_id: str,
        source_type: str,
        source_key: str,
        org_id: Optional[str] = None,
        status: str,
        archived_at: str = "",
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        now = utcnow_iso()
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE custom_managed_group_bindings
                    SET status = ?,
                        archived_at = ?,
                        updated_at = ?
                    WHERE org_id = ?
                      AND connector_id = ?
                      AND source_type = ?
                      AND source_key = ?
                    """,
                    (
                        str(status or "active").strip() or "active",
                        str(archived_at or "").strip(),
                        now,
                        normalized_org_id,
                        str(connector_id or "default").strip() or "default",
                        str(source_type or "").strip(),
                        str(source_key or "").strip(),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE custom_managed_group_bindings
                    SET status = ?,
                        archived_at = ?,
                        updated_at = ?
                    WHERE connector_id = ?
                      AND source_type = ?
                      AND source_key = ?
                    """,
                    (
                        str(status or "active").strip() or "active",
                        str(archived_at or "").strip(),
                        now,
                        str(connector_id or "default").strip() or "default",
                        str(source_type or "").strip(),
                        str(source_key or "").strip(),
                    ),
                )

    def list_active_records(
        self,
        *,
        connector_id: str | None = None,
        org_id: Optional[str] = None,
    ) -> list[CustomManagedGroupBindingRecord]:
        clauses = ["status = 'active'"]
        params: list[object] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        normalized_connector = str(connector_id or "").strip()
        if normalized_connector:
            clauses.append("connector_id = ?")
            params.append(normalized_connector)
        rows = self._fetchall(
            f"""
            SELECT *
            FROM custom_managed_group_bindings
            WHERE {' AND '.join(clauses)}
            ORDER BY connector_id ASC, source_type ASC, source_key ASC
            """,
            tuple(params),
        )
        return [CustomManagedGroupBindingRecord.from_row(row) for row in rows]

    def list_records(
        self,
        *,
        connector_id: str | None = None,
        org_id: Optional[str] = None,
    ) -> list[CustomManagedGroupBindingRecord]:
        clauses = ["1 = 1"]
        params: list[object] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        normalized_connector = str(connector_id or "").strip()
        if normalized_connector:
            clauses.append("connector_id = ?")
            params.append(normalized_connector)
        rows = self._fetchall(
            f"""
            SELECT *
            FROM custom_managed_group_bindings
            WHERE {' AND '.join(clauses)}
            ORDER BY connector_id ASC, source_type ASC, source_key ASC
            """,
            tuple(params),
        )
        return [CustomManagedGroupBindingRecord.from_row(row) for row in rows]

    def delete_bindings_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM custom_managed_group_bindings WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )
