from __future__ import annotations

from typing import Any, Optional

from sync_app.core.models import (
    AttributeMappingRuleRecord,
    UserDepartmentOverrideRecord,
    UserIdentityBindingRecord,
)
from sync_app.core.sync_policies import normalize_mapping_direction
from sync_app.storage.local_db import BaseRepository, utcnow_iso


class UserIdentityBindingRepository(BaseRepository):
    def get_by_source_user_id(self, source_user_id: str, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchone(
                """
                SELECT * FROM user_identity_bindings
                WHERE org_id = ?
                  AND source_user_id = ?
                LIMIT 1
                """,
                (normalized_org_id, source_user_id),
            )
        return self._fetchone(
            """
            SELECT * FROM user_identity_bindings
            WHERE source_user_id = ?
            ORDER BY org_id ASC, id ASC
            LIMIT 1
            """,
            (source_user_id,),
        )

    def get_by_wecom_userid(self, wecom_userid: str, *, org_id: Optional[str] = None):
        return self.get_by_source_user_id(wecom_userid, org_id=org_id)

    def get_binding_record_by_source_user_id(
        self,
        source_user_id: str,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[UserIdentityBindingRecord]:
        row = self.get_by_source_user_id(source_user_id, org_id=org_id)
        if not row:
            return None
        return UserIdentityBindingRecord.from_row(row)

    def get_binding_record_by_wecom_userid(
        self,
        wecom_userid: str,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[UserIdentityBindingRecord]:
        return self.get_binding_record_by_source_user_id(wecom_userid, org_id=org_id)

    def get_by_ad_username(
        self,
        ad_username: str,
        *,
        connector_id: str | None = None,
        org_id: Optional[str] = None,
    ):
        normalized_connector_id = str(connector_id or "").strip()
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_connector_id:
            if normalized_org_id:
                return self._fetchone(
                    """
                    SELECT * FROM user_identity_bindings
                    WHERE org_id = ?
                      AND connector_id = ?
                      AND LOWER(ad_username) = LOWER(?)
                    LIMIT 1
                    """,
                    (normalized_org_id, normalized_connector_id, ad_username),
                )
            return self._fetchone(
                """
                SELECT * FROM user_identity_bindings
                WHERE connector_id = ?
                  AND LOWER(ad_username) = LOWER(?)
                ORDER BY org_id ASC, id ASC
                LIMIT 1
                """,
                (normalized_connector_id, ad_username),
            )
        if normalized_org_id:
            return self._fetchone(
                """
                SELECT * FROM user_identity_bindings
                WHERE org_id = ?
                  AND LOWER(ad_username) = LOWER(?)
                ORDER BY connector_id ASC, id ASC
                LIMIT 1
                """,
                (normalized_org_id, ad_username),
            )
        return self._fetchone(
            """
            SELECT * FROM user_identity_bindings
            WHERE LOWER(ad_username) = LOWER(?)
            ORDER BY org_id ASC, connector_id ASC, id ASC
            LIMIT 1
            """,
            (ad_username,),
        )

    def get_binding_record_by_ad_username(
        self,
        ad_username: str,
        *,
        connector_id: str | None = None,
        org_id: Optional[str] = None,
    ) -> Optional[UserIdentityBindingRecord]:
        row = self.get_by_ad_username(ad_username, connector_id=connector_id, org_id=org_id)
        if not row:
            return None
        return UserIdentityBindingRecord.from_row(row)

    def list_enabled_binding_records(self, *, org_id: Optional[str] = None) -> list[UserIdentityBindingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                """
                SELECT * FROM user_identity_bindings
                WHERE org_id = ?
                  AND is_enabled = 1
                ORDER BY source_user_id ASC
                """,
                (normalized_org_id,),
            )
        else:
            rows = self._fetchall(
                """
                SELECT * FROM user_identity_bindings
                WHERE is_enabled = 1
                ORDER BY org_id ASC, source_user_id ASC
                """
            )
        return [UserIdentityBindingRecord.from_row(row) for row in rows]

    def list_binding_records(self, *, org_id: Optional[str] = None) -> list[UserIdentityBindingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                """
                SELECT * FROM user_identity_bindings
                WHERE org_id = ?
                ORDER BY is_enabled DESC, source_user_id ASC
                """,
                (normalized_org_id,),
            )
        else:
            rows = self._fetchall(
                """
                SELECT * FROM user_identity_bindings
                ORDER BY org_id ASC, is_enabled DESC, source_user_id ASC
                """
            )
        return [UserIdentityBindingRecord.from_row(row) for row in rows]

    def list_binding_records_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        query: str = "",
        status: str = "all",
        org_id: Optional[str] = None,
    ) -> tuple[list[UserIdentityBindingRecord], int]:
        normalized_query = str(query or "").strip().lower()
        normalized_status = str(status or "all").strip().lower()
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if normalized_status == "enabled":
            clauses.append("is_enabled = 1")
        elif normalized_status == "disabled":
            clauses.append("is_enabled = 0")
        if normalized_query:
            clauses.append(
                "("
                "LOWER(source_user_id) LIKE ? OR "
                "LOWER(connector_id) LIKE ? OR "
                "LOWER(ad_username) LIKE ? OR "
                "LOWER(COALESCE(notes, '')) LIKE ?"
                ")"
            )
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 4)
        where_clause = " WHERE " + " AND ".join(clauses)
        total = self._fetchcount(
            f"""
            SELECT COUNT(*)
            FROM user_identity_bindings
            {where_clause}
            """,
            tuple(params),
        )
        rows = self._fetchall(
            f"""
            SELECT *
            FROM user_identity_bindings
            {where_clause}
            ORDER BY is_enabled DESC, connector_id ASC, source_user_id ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), max(int(offset), 0)),
        )
        return [UserIdentityBindingRecord.from_row(row) for row in rows], total

    def upsert_binding(
        self,
        source_user_id: str,
        ad_username: str,
        *,
        org_id: Optional[str] = None,
        connector_id: str = "default",
        source: str = "derived_default",
        notes: str = "",
        is_enabled: bool = True,
        preserve_manual: bool = True,
    ) -> None:
        source_user_id = str(source_user_id).strip()
        ad_username = str(ad_username).strip()
        connector_id = str(connector_id or "default").strip() or "default"
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        if not source_user_id or not ad_username:
            raise ValueError("source_user_id and ad_username are required")

        now = utcnow_iso()
        existing = self.get_binding_record_by_source_user_id(source_user_id, org_id=normalized_org_id)
        if existing and preserve_manual and existing.source == "manual":
            return

        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO user_identity_bindings (
                  org_id, source_user_id, connector_id, ad_username, source, notes, is_enabled, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, source_user_id) DO UPDATE SET
                  connector_id = excluded.connector_id,
                  ad_username = excluded.ad_username,
                  source = excluded.source,
                  notes = excluded.notes,
                  is_enabled = excluded.is_enabled,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    source_user_id,
                    connector_id,
                    ad_username,
                    source,
                    notes,
                    1 if is_enabled else 0,
                    now,
                ),
            )

    def upsert_binding_for_source_user(
        self,
        source_user_id: str,
        ad_username: str,
        *,
        connector_id: str = "default",
        source: str = "derived_default",
        notes: str = "",
        is_enabled: bool = True,
        preserve_manual: bool = True,
        org_id: Optional[str] = None,
    ) -> None:
        self.upsert_binding(
            source_user_id=source_user_id,
            ad_username=ad_username,
            connector_id=connector_id,
            source=source,
            notes=notes,
            is_enabled=is_enabled,
            preserve_manual=preserve_manual,
            org_id=org_id,
        )

    def set_enabled(self, source_user_id: str, enabled: bool, *, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE user_identity_bindings
                    SET is_enabled = ?,
                        updated_at = ?
                    WHERE org_id = ?
                      AND source_user_id = ?
                    """,
                    (1 if enabled else 0, utcnow_iso(), normalized_org_id, source_user_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE user_identity_bindings
                    SET is_enabled = ?,
                        updated_at = ?
                    WHERE source_user_id = ?
                    """,
                    (1 if enabled else 0, utcnow_iso(), source_user_id),
                )

    def set_enabled_for_source_user(
        self,
        source_user_id: str,
        enabled: bool,
        *,
        org_id: Optional[str] = None,
    ) -> None:
        self.set_enabled(source_user_id, enabled, org_id=org_id)

    def delete_bindings_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM user_identity_bindings WHERE org_id = ?", (self._resolve_org_id(org_id, default="default"),))


class UserDepartmentOverrideRepository(BaseRepository):
    def get_by_source_user_id(self, source_user_id: str, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchone(
                """
                SELECT * FROM user_department_overrides
                WHERE org_id = ?
                  AND source_user_id = ?
                LIMIT 1
                """,
                (normalized_org_id, source_user_id),
            )
        return self._fetchone(
            """
            SELECT * FROM user_department_overrides
            WHERE source_user_id = ?
            ORDER BY org_id ASC, id ASC
            LIMIT 1
            """,
            (source_user_id,),
        )

    def get_by_wecom_userid(self, wecom_userid: str, *, org_id: Optional[str] = None):
        return self.get_by_source_user_id(wecom_userid, org_id=org_id)

    def get_override_record_by_source_user_id(
        self,
        source_user_id: str,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[UserDepartmentOverrideRecord]:
        row = self.get_by_source_user_id(source_user_id, org_id=org_id)
        if not row:
            return None
        return UserDepartmentOverrideRecord.from_row(row)

    def get_override_record_by_wecom_userid(
        self,
        wecom_userid: str,
        *,
        org_id: Optional[str] = None,
    ) -> Optional[UserDepartmentOverrideRecord]:
        return self.get_override_record_by_source_user_id(wecom_userid, org_id=org_id)

    def list_override_records(self, *, org_id: Optional[str] = None) -> list[UserDepartmentOverrideRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            rows = self._fetchall(
                """
                SELECT * FROM user_department_overrides
                WHERE org_id = ?
                ORDER BY source_user_id ASC
                """,
                (normalized_org_id,),
            )
        else:
            rows = self._fetchall(
                """
                SELECT * FROM user_department_overrides
                ORDER BY org_id ASC, source_user_id ASC
                """
            )
        return [UserDepartmentOverrideRecord.from_row(row) for row in rows]

    def list_override_records_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        query: str = "",
        org_id: Optional[str] = None,
    ) -> tuple[list[UserDepartmentOverrideRecord], int]:
        normalized_query = str(query or "").strip().lower()
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if normalized_query:
            clauses.append(
                "("
                "LOWER(source_user_id) LIKE ? OR "
                "LOWER(primary_department_id) LIKE ? OR "
                "LOWER(COALESCE(notes, '')) LIKE ?"
                ")"
            )
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 3)
        where_clause = " WHERE " + " AND ".join(clauses)
        total = self._fetchcount(
            f"""
            SELECT COUNT(*)
            FROM user_department_overrides
            {where_clause}
            """,
            tuple(params),
        )
        rows = self._fetchall(
            f"""
            SELECT *
            FROM user_department_overrides
            {where_clause}
            ORDER BY source_user_id ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), max(int(offset), 0)),
        )
        return [UserDepartmentOverrideRecord.from_row(row) for row in rows], total

    def upsert_override(
        self,
        source_user_id: str,
        primary_department_id: str,
        *,
        org_id: Optional[str] = None,
        notes: str = "",
    ) -> None:
        source_user_id = str(source_user_id).strip()
        primary_department_id = str(primary_department_id).strip()
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        if not source_user_id or not primary_department_id:
            raise ValueError("source_user_id and primary_department_id are required")

        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO user_department_overrides (
                  org_id, source_user_id, primary_department_id, notes, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(org_id, source_user_id) DO UPDATE SET
                  primary_department_id = excluded.primary_department_id,
                  notes = excluded.notes,
                  updated_at = excluded.updated_at
                """,
                (normalized_org_id, source_user_id, primary_department_id, notes, utcnow_iso()),
            )

    def upsert_override_for_source_user(
        self,
        source_user_id: str,
        primary_department_id: str,
        *,
        org_id: Optional[str] = None,
        notes: str = "",
    ) -> None:
        self.upsert_override(
            source_user_id=source_user_id,
            primary_department_id=primary_department_id,
            org_id=org_id,
            notes=notes,
        )

    def delete_override(self, source_user_id: str, *, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    DELETE FROM user_department_overrides
                    WHERE org_id = ?
                      AND source_user_id = ?
                    """,
                    (normalized_org_id, source_user_id),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM user_department_overrides
                    WHERE source_user_id = ?
                    """,
                    (source_user_id,),
                )

    def delete_override_for_source_user(self, source_user_id: str, *, org_id: Optional[str] = None) -> None:
        self.delete_override(source_user_id, org_id=org_id)

    def delete_overrides_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM user_department_overrides WHERE org_id = ?", (self._resolve_org_id(org_id, default="default"),))


class AttributeMappingRuleRepository(BaseRepository):
    def get_rule_record(self, rule_id: int, *, org_id: Optional[str] = None) -> Optional[AttributeMappingRuleRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM attribute_mapping_rules
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
                FROM attribute_mapping_rules
                WHERE id = ?
                LIMIT 1
                """,
                (int(rule_id),),
            )
        if not row:
            return None
        return AttributeMappingRuleRecord.from_row(row)

    def list_rule_records(
        self,
        *,
        direction: str | None = None,
        connector_id: str | None = None,
        enabled_only: bool = False,
        org_id: Optional[str] = None,
    ) -> list[AttributeMappingRuleRecord]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        normalized_direction = (
            normalize_mapping_direction(direction)
            if str(direction or "").strip()
            else ""
        )
        normalized_connector = str(connector_id or "").strip()
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if normalized_direction:
            clauses.append("direction = ?")
            params.append(normalized_direction)
        if normalized_connector:
            clauses.append("(connector_id = '' OR connector_id = ?)")
            params.append(normalized_connector)
        if enabled_only:
            clauses.append("is_enabled = 1")
        rows = self._fetchall(
            f"""
            SELECT *
            FROM attribute_mapping_rules
            WHERE {' AND '.join(clauses)}
            ORDER BY CASE WHEN connector_id = '' THEN 0 ELSE 1 END ASC, target_field ASC, id ASC
            """,
            tuple(params),
        )
        return [AttributeMappingRuleRecord.from_row(row) for row in rows]

    def list_rule_records_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        direction: str = "",
        connector_id: str = "",
        query: str = "",
        org_id: Optional[str] = None,
    ) -> tuple[list[AttributeMappingRuleRecord], int]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        normalized_direction = (
            normalize_mapping_direction(direction)
            if str(direction or "").strip()
            else ""
        )
        normalized_connector = str(connector_id or "").strip()
        normalized_query = str(query or "").strip().lower()
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if normalized_direction:
            clauses.append("direction = ?")
            params.append(normalized_direction)
        if normalized_connector:
            clauses.append("connector_id = ?")
            params.append(normalized_connector)
        if normalized_query:
            clauses.append(
                "("
                "LOWER(connector_id) LIKE ? OR "
                "LOWER(source_field) LIKE ? OR "
                "LOWER(target_field) LIKE ? OR "
                "LOWER(COALESCE(transform_template, '')) LIKE ? OR "
                "LOWER(COALESCE(notes, '')) LIKE ?"
                ")"
            )
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 5)
        where_clause = " WHERE " + " AND ".join(clauses)
        total = self._fetchcount(
            f"""
            SELECT COUNT(*)
            FROM attribute_mapping_rules
            {where_clause}
            """,
            tuple(params),
        )
        rows = self._fetchall(
            f"""
            SELECT *
            FROM attribute_mapping_rules
            {where_clause}
            ORDER BY direction ASC, connector_id ASC, target_field ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (*params, int(limit), max(int(offset), 0)),
        )
        return [AttributeMappingRuleRecord.from_row(row) for row in rows], total

    def upsert_rule(
        self,
        *,
        direction: str,
        source_field: str,
        target_field: str,
        connector_id: str = "",
        transform_template: str = "",
        sync_mode: str = "replace",
        is_enabled: bool = True,
        notes: str = "",
        org_id: Optional[str] = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_direction = normalize_mapping_direction(direction)
        normalized_source = str(source_field or "").strip()
        normalized_target = str(target_field or "").strip()
        normalized_connector = str(connector_id or "").strip()
        normalized_mode = str(sync_mode or "replace").strip().lower()
        if normalized_direction not in {"source_to_ad", "ad_to_source"}:
            raise ValueError("unsupported mapping direction")
        if normalized_mode not in {"replace", "fill_if_empty", "preserve"}:
            raise ValueError("unsupported mapping sync_mode")
        if not normalized_source or not normalized_target:
            raise ValueError("source_field and target_field are required")

        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO attribute_mapping_rules (
                  org_id, connector_id, direction, source_field, target_field, transform_template,
                  sync_mode, is_enabled, notes, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, connector_id, direction, source_field, target_field) DO UPDATE SET
                  transform_template = excluded.transform_template,
                  sync_mode = excluded.sync_mode,
                  is_enabled = excluded.is_enabled,
                  notes = excluded.notes,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_connector,
                    normalized_direction,
                    normalized_source,
                    normalized_target,
                    str(transform_template or "").strip(),
                    normalized_mode,
                    1 if is_enabled else 0,
                    str(notes or "").strip(),
                    now,
                    now,
                ),
            )

    def delete_rule(self, rule_id: int, *, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    "DELETE FROM attribute_mapping_rules WHERE id = ? AND org_id = ?",
                    (int(rule_id), normalized_org_id),
                )
            else:
                conn.execute("DELETE FROM attribute_mapping_rules WHERE id = ?", (int(rule_id),))

    def delete_rules_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM attribute_mapping_rules WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )
