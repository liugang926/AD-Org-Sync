from __future__ import annotations

from typing import Any, Iterable, Optional

from sync_app.core.models import (
    AttributeMappingRuleRecord,
    DepartmentOuMappingRecord,
    UserDepartmentOverrideRecord,
    UserIdentityBindingRecord,
)
from sync_app.core.sync_policies import normalize_mapping_direction
from sync_app.storage.local_db import BaseRepository, utcnow_iso


class UserIdentityBindingRepository(BaseRepository):
    def list_binding_records_for_source_identities(
        self,
        source_user_ids: Iterable[str],
        *,
        org_id: str,
        source_provider: str,
        connector_ids: Iterable[str] = (),
        enabled_only: bool | None = None,
        chunk_size: int = 400,
    ) -> list[UserIdentityBindingRecord]:
        """Batch-load every binding within an exact source identity boundary.

        The method deliberately returns all matching rows. Callers must decide
        whether multiple connector candidates are ambiguous instead of silently
        selecting the first record.
        """

        normalized_ids = sorted(
            {str(value or "").strip() for value in source_user_ids if str(value or "").strip()}
        )
        if not normalized_ids:
            return []
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_provider = str(source_provider or "").strip().lower()
        if not normalized_provider:
            return []
        normalized_connectors = sorted(
            {str(value or "").strip() for value in connector_ids if str(value or "").strip()}
        )
        safe_chunk_size = min(max(int(chunk_size or 400), 1), 400)
        records: list[UserIdentityBindingRecord] = []
        for index in range(0, len(normalized_ids), safe_chunk_size):
            id_chunk = normalized_ids[index : index + safe_chunk_size]
            clauses = [
                "org_id = ?",
                "source_provider = ?",
                f"source_user_id IN ({','.join('?' for _ in id_chunk)})",
            ]
            params: list[Any] = [normalized_org_id, normalized_provider, *id_chunk]
            if normalized_connectors:
                clauses.append(
                    f"connector_id IN ({','.join('?' for _ in normalized_connectors)})"
                )
                params.extend(normalized_connectors)
            if enabled_only is True:
                clauses.append("is_enabled = 1")
            elif enabled_only is False:
                clauses.append("is_enabled = 0")
            rows = self._fetchall(
                f"""
                SELECT * FROM user_identity_bindings
                WHERE {" AND ".join(clauses)}
                ORDER BY source_user_id ASC, connector_id ASC, is_enabled DESC, id ASC
                """,
                tuple(params),
            )
            records.extend(UserIdentityBindingRecord.from_row(row) for row in rows)
        return records

    def get_by_source_user_id(
        self,
        source_user_id: str,
        *,
        org_id: Optional[str] = None,
        source_provider: str | None = None,
        connector_id: str | None = None,
    ):
        normalized_org_id = self._resolve_org_id(org_id)
        normalized_provider = str(source_provider or "").strip().lower()
        normalized_connector_id = str(connector_id or "").strip()
        if normalized_org_id:
            if normalized_provider and normalized_connector_id:
                return self._fetchone(
                    """
                    SELECT * FROM user_identity_bindings
                    WHERE org_id = ?
                      AND source_provider = ?
                      AND connector_id = ?
                      AND source_user_id = ?
                    LIMIT 1
                    """,
                    (
                        normalized_org_id,
                        normalized_provider,
                        normalized_connector_id,
                        source_user_id,
                    ),
                )
            if normalized_provider:
                rows = self._fetchall(
                    """
                    SELECT * FROM user_identity_bindings
                    WHERE org_id = ? AND source_provider = ? AND source_user_id = ?
                    ORDER BY connector_id ASC, id ASC LIMIT 2
                    """,
                    (normalized_org_id, normalized_provider, source_user_id),
                )
                return rows[0] if len(rows) == 1 else None
            rows = self._fetchall(
                """
                SELECT * FROM user_identity_bindings
                WHERE org_id = ?
                  AND source_user_id = ?
                ORDER BY source_provider ASC, connector_id ASC, id ASC LIMIT 2
                """,
                (normalized_org_id, source_user_id),
            )
            return rows[0] if len(rows) == 1 else None
        rows = self._fetchall(
            """
            SELECT * FROM user_identity_bindings
            WHERE source_user_id = ?
            ORDER BY org_id ASC, id ASC
            LIMIT 2
            """,
            (source_user_id,),
        )
        return rows[0] if len(rows) == 1 else None

    def get_by_wecom_userid(self, wecom_userid: str, *, org_id: Optional[str] = None):
        return self.get_by_source_user_id(wecom_userid, org_id=org_id)

    def get_binding_record_by_source_user_id(
        self,
        source_user_id: str,
        *,
        org_id: Optional[str] = None,
        source_provider: str | None = None,
        connector_id: str | None = None,
    ) -> Optional[UserIdentityBindingRecord]:
        row = self.get_by_source_user_id(
            source_user_id,
            org_id=org_id,
            source_provider=source_provider,
            connector_id=connector_id,
        )
        if not row:
            return None
        return UserIdentityBindingRecord.from_row(row)

    def list_binding_records_for_source_identity(
        self,
        source_user_id: str,
        *,
        org_id: str,
        source_provider: str,
        connector_id: str | None = None,
        enabled_only: bool = False,
    ) -> list[UserIdentityBindingRecord]:
        """Return every exact candidate so callers can fail closed on ambiguity."""

        clauses = [
            "org_id = ?",
            "source_provider = ?",
            "source_user_id = ?",
        ]
        params: list[Any] = [
            self._resolve_org_id(org_id) or "default",
            str(source_provider or "").strip().lower(),
            str(source_user_id or "").strip(),
        ]
        normalized_connector_id = str(connector_id or "").strip()
        if normalized_connector_id:
            clauses.append("connector_id = ?")
            params.append(normalized_connector_id)
        if enabled_only:
            clauses.append("is_enabled = 1")
        rows = self._fetchall(
            f"""
            SELECT * FROM user_identity_bindings
            WHERE {" AND ".join(clauses)}
            ORDER BY connector_id ASC, id ASC
            """,
            tuple(params),
        )
        return [UserIdentityBindingRecord.from_row(row) for row in rows]

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

    def list_enabled_binding_records(
        self, *, org_id: Optional[str] = None, source_provider: str | None = None
    ) -> list[UserIdentityBindingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        normalized_provider = str(source_provider or "").strip().lower()
        if normalized_org_id:
            if normalized_provider:
                rows = self._fetchall(
                    """
                    SELECT * FROM user_identity_bindings
                    WHERE org_id = ? AND source_provider = ? AND is_enabled = 1
                    ORDER BY source_user_id ASC
                    """,
                    (normalized_org_id, normalized_provider),
                )
                return [UserIdentityBindingRecord.from_row(row) for row in rows]
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

    def list_binding_records(
        self, *, org_id: Optional[str] = None, source_provider: str | None = None
    ) -> list[UserIdentityBindingRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        normalized_provider = str(source_provider or "").strip().lower()
        if normalized_org_id:
            if normalized_provider:
                rows = self._fetchall(
                    """
                    SELECT * FROM user_identity_bindings
                    WHERE org_id = ? AND source_provider = ?
                    ORDER BY is_enabled DESC, source_user_id ASC
                    """,
                    (normalized_org_id, normalized_provider),
                )
                return [UserIdentityBindingRecord.from_row(row) for row in rows]
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
        source_provider: str = "",
        connector_id: str = "",
        binding_source: str = "",
        apply_status: str = "",
        org_id: Optional[str] = None,
    ) -> tuple[list[UserIdentityBindingRecord], int]:
        normalized_query = str(query or "").strip().lower()
        normalized_status = str(status or "all").strip().lower()
        normalized_provider = str(source_provider or "").strip().lower()
        normalized_connector = str(connector_id or "").strip()
        normalized_binding_source = str(binding_source or "").strip().lower()
        normalized_apply_status = str(apply_status or "").strip().lower()
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
        if normalized_provider:
            clauses.append("source_provider = ?")
            params.append(normalized_provider)
        if normalized_connector:
            clauses.append("connector_id = ?")
            params.append(normalized_connector)
        if normalized_binding_source:
            clauses.append("LOWER(source) = ?")
            params.append(normalized_binding_source)
        if normalized_apply_status in {"succeeded", "failed", "not_applied"}:
            exists_sql = """
                SELECT 1
                FROM sync_operation_logs AS operation
                JOIN sync_jobs AS job ON job.job_id = operation.job_id
                JOIN sync_job_source_scopes AS source_scope
                  ON source_scope.job_id = job.job_id
                WHERE job.org_id = user_identity_bindings.org_id
                  AND source_scope.org_id = user_identity_bindings.org_id
                  AND source_scope.provider_id = user_identity_bindings.source_provider
                  AND COALESCE(
                        json_extract(operation.details_json, '$.connector_id'),
                        source_scope.connector_id,
                        'default'
                      ) = user_identity_bindings.connector_id
                  AND job.execution_mode = 'apply'
                  AND operation.stage_name = 'apply'
                  AND operation.object_type = 'user'
                  AND operation.source_id = user_identity_bindings.source_user_id
                  AND LOWER(operation.target_id) = LOWER(user_identity_bindings.ad_username)
            """
            if normalized_apply_status == "succeeded":
                clauses.append(
                    f"EXISTS ({exists_sql} AND job.status = 'COMPLETED' AND operation.status = 'succeeded')"
                )
            elif normalized_apply_status == "failed":
                clauses.append(
                    f"EXISTS ({exists_sql} AND operation.status IN ('error', 'failed', 'canceled'))"
                )
            else:
                clauses.append(f"NOT EXISTS ({exists_sql})")
        if normalized_query:
            clauses.append(
                "("
                "LOWER(source_user_id) LIKE ? OR "
                "LOWER(COALESCE(source_display_name, '')) LIKE ? OR "
                "LOWER(COALESCE(source_provider, '')) LIKE ? OR "
                "LOWER(connector_id) LIKE ? OR "
                "LOWER(ad_username) LIKE ? OR "
                "LOWER(COALESCE(rule_owner, '')) LIKE ? OR "
                "LOWER(COALESCE(effective_reason, '')) LIKE ? OR "
                "LOWER(COALESCE(notes, '')) LIKE ? OR "
                "EXISTS ("
                "  SELECT 1 FROM source_user_snapshots AS source_user "
                "  JOIN source_directory_snapshots AS snapshot "
                "    ON snapshot.id = source_user.snapshot_id "
                "  WHERE source_user.org_id = user_identity_bindings.org_id "
                "    AND source_user.provider_id = user_identity_bindings.source_provider "
                "    AND source_user.source_user_id = user_identity_bindings.source_user_id "
                "    AND snapshot.status = 'succeeded' "
                "    AND snapshot.id = ("
                "      SELECT latest.id FROM source_directory_snapshots AS latest "
                "      WHERE latest.org_id = user_identity_bindings.org_id "
                "        AND latest.provider_id = user_identity_bindings.source_provider "
                "        AND latest.status = 'succeeded' "
                "      ORDER BY latest.completed_at DESC, latest.id DESC LIMIT 1"
                "    ) "
                "    AND LOWER(source_user.display_name) LIKE ?"
                ")"
                ")"
            )
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 9)
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
        source_display_name: str = "",
        target_object_guid: str = "",
        target_object_dn: str = "",
        managed_username_base: str = "",
        source: str = "derived_default",
        notes: str = "",
        is_enabled: bool = True,
        preserve_manual: bool = True,
        source_provider: str = "wecom",
    ) -> None:
        source_user_id = str(source_user_id).strip()
        ad_username = str(ad_username).strip()
        connector_id = str(connector_id or "default").strip() or "default"
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        source_provider = str(source_provider or "wecom").strip().lower() or "wecom"
        if not source_user_id or not ad_username:
            raise ValueError("source_user_id and ad_username are required")

        now = utcnow_iso()
        existing = self.get_binding_record_by_source_user_id(
            source_user_id,
            org_id=normalized_org_id,
            source_provider=source_provider,
            connector_id=connector_id,
        )
        if existing and preserve_manual and existing.source == "manual":
            return

        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO user_identity_bindings (
                  org_id, source_provider, source_user_id, source_display_name, connector_id, ad_username,
                  target_object_guid, target_object_dn, managed_username_base,
                  source, notes, is_enabled, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, source_provider, connector_id, source_user_id) DO UPDATE SET
                  source_display_name = excluded.source_display_name,
                  ad_username = excluded.ad_username,
                  target_object_guid = excluded.target_object_guid,
                  target_object_dn = excluded.target_object_dn,
                  managed_username_base = excluded.managed_username_base,
                  source = excluded.source,
                  notes = excluded.notes,
                  is_enabled = excluded.is_enabled,
                  binding_revision = user_identity_bindings.binding_revision + 1,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    source_provider,
                    source_user_id,
                    str(source_display_name or "").strip(),
                    connector_id,
                    ad_username,
                    str(target_object_guid or "").strip(),
                    str(target_object_dn or "").strip(),
                    str(managed_username_base or "").strip(),
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
        source_display_name: str = "",
        target_object_guid: str = "",
        target_object_dn: str = "",
        managed_username_base: str = "",
        source: str = "derived_default",
        notes: str = "",
        is_enabled: bool = True,
        preserve_manual: bool = True,
        org_id: Optional[str] = None,
        source_provider: str = "wecom",
    ) -> None:
        self.upsert_binding(
            source_user_id=source_user_id,
            ad_username=ad_username,
            connector_id=connector_id,
            source_display_name=source_display_name,
            target_object_guid=target_object_guid,
            target_object_dn=target_object_dn,
            managed_username_base=managed_username_base,
            source=source,
            notes=notes,
            is_enabled=is_enabled,
            preserve_manual=preserve_manual,
            org_id=org_id,
            source_provider=source_provider,
        )

    def update_governance_metadata_for_source_user(
        self,
        source_user_id: str,
        *,
        org_id: Optional[str] = None,
        rule_owner: str | None = None,
        effective_reason: str | None = None,
        next_review_at: str | None = None,
        last_reviewed_at: str | None = None,
        source_provider: str | None = None,
        connector_id: str | None = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        updates: list[str] = []
        params: list[Any] = []
        if rule_owner is not None:
            updates.append("rule_owner = ?")
            params.append(str(rule_owner or "").strip())
        if effective_reason is not None:
            updates.append("effective_reason = ?")
            params.append(str(effective_reason or "").strip())
        if next_review_at is not None:
            updates.append("next_review_at = ?")
            params.append(str(next_review_at or "").strip())
        if last_reviewed_at is not None:
            updates.append("last_reviewed_at = ?")
            params.append(str(last_reviewed_at or "").strip())
        if not updates:
            return
        updates.append("binding_revision = binding_revision + 1")
        updates.append("updated_at = ?")
        params.append(utcnow_iso())
        normalized_provider = str(source_provider or "").strip().lower()
        normalized_connector_id = str(connector_id or "").strip()
        with self.db.transaction() as conn:
            if normalized_org_id:
                identity_clauses = ["org_id = ?", "source_user_id = ?"]
                identity_params: list[Any] = [normalized_org_id, str(source_user_id or "").strip()]
                if normalized_provider:
                    identity_clauses.append("source_provider = ?")
                    identity_params.append(normalized_provider)
                if normalized_connector_id:
                    identity_clauses.append("connector_id = ?")
                    identity_params.append(normalized_connector_id)
                conn.execute(
                    f"""
                    UPDATE user_identity_bindings
                    SET {", ".join(updates)}
                    WHERE {" AND ".join(identity_clauses)}
                    """,
                    (*params, *identity_params),
                )
            else:
                conn.execute(
                    f"""
                    UPDATE user_identity_bindings
                    SET {", ".join(updates)}
                    WHERE source_user_id = ?
                    """,
                    (*params, str(source_user_id or "").strip()),
                )

    def record_rule_hit_for_source_user(
        self,
        source_user_id: str,
        *,
        org_id: Optional[str] = None,
        hit_at: str | None = None,
        source_provider: str | None = None,
        connector_id: str | None = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        normalized_hit_at = str(hit_at or utcnow_iso()).strip()
        normalized_provider = str(source_provider or "").strip().lower()
        normalized_connector_id = str(connector_id or "").strip()
        with self.db.transaction() as conn:
            if normalized_org_id:
                identity_clauses = ["org_id = ?", "source_user_id = ?"]
                identity_params: list[Any] = [normalized_org_id, str(source_user_id or "").strip()]
                if normalized_provider:
                    identity_clauses.append("source_provider = ?")
                    identity_params.append(normalized_provider)
                if normalized_connector_id:
                    identity_clauses.append("connector_id = ?")
                    identity_params.append(normalized_connector_id)
                conn.execute(
                    f"""
                    UPDATE user_identity_bindings
                    SET hit_count = COALESCE(hit_count, 0) + 1,
                        last_hit_at = ?
                    WHERE {" AND ".join(identity_clauses)}
                    """,
                    (normalized_hit_at, *identity_params),
                )
            else:
                conn.execute(
                    """
                    UPDATE user_identity_bindings
                    SET hit_count = COALESCE(hit_count, 0) + 1,
                        last_hit_at = ?
                    WHERE source_user_id = ?
                    """,
                    (normalized_hit_at, str(source_user_id or "").strip()),
                )

    def update_binding_anchor(
        self,
        source_user_id: str,
        *,
        org_id: Optional[str] = None,
        source_display_name: str | None = None,
        target_object_guid: str | None = None,
        target_object_dn: str | None = None,
        managed_username_base: str | None = None,
        source_provider: str | None = None,
        connector_id: str | None = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        assignments: list[str] = []
        params: list[Any] = []
        if source_display_name is not None:
            assignments.append("source_display_name = ?")
            params.append(str(source_display_name or "").strip())
        if target_object_guid is not None:
            assignments.append("target_object_guid = ?")
            params.append(str(target_object_guid or "").strip())
        if target_object_dn is not None:
            assignments.append("target_object_dn = ?")
            params.append(str(target_object_dn or "").strip())
        if managed_username_base is not None:
            assignments.append("managed_username_base = ?")
            params.append(str(managed_username_base or "").strip())
        if not assignments:
            return
        assignments.append("binding_revision = binding_revision + 1")
        assignments.append("updated_at = ?")
        params.append(utcnow_iso())
        normalized_provider = str(source_provider or "").strip().lower()
        normalized_connector_id = str(connector_id or "").strip()
        with self.db.transaction() as conn:
            if normalized_org_id:
                identity_clauses = ["org_id = ?", "source_user_id = ?"]
                identity_params: list[Any] = [normalized_org_id, str(source_user_id or "").strip()]
                if normalized_provider:
                    identity_clauses.append("source_provider = ?")
                    identity_params.append(normalized_provider)
                if normalized_connector_id:
                    identity_clauses.append("connector_id = ?")
                    identity_params.append(normalized_connector_id)
                conn.execute(
                    f"""
                    UPDATE user_identity_bindings
                    SET {", ".join(assignments)}
                    WHERE {" AND ".join(identity_clauses)}
                    """,
                    (*params, *identity_params),
                )
            else:
                conn.execute(
                    f"""
                    UPDATE user_identity_bindings
                    SET {", ".join(assignments)}
                    WHERE source_user_id = ?
                    """,
                    (*params, str(source_user_id or "").strip()),
                )

    def set_enabled(
        self,
        source_user_id: str,
        enabled: bool,
        *,
        org_id: Optional[str] = None,
        source_provider: str | None = None,
        connector_id: str | None = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        normalized_provider = str(source_provider or "").strip().lower()
        normalized_connector_id = str(connector_id or "").strip()
        with self.db.transaction() as conn:
            if normalized_org_id:
                identity_clauses = ["org_id = ?", "source_user_id = ?"]
                identity_params: list[Any] = [normalized_org_id, source_user_id]
                if normalized_provider:
                    identity_clauses.append("source_provider = ?")
                    identity_params.append(normalized_provider)
                if normalized_connector_id:
                    identity_clauses.append("connector_id = ?")
                    identity_params.append(normalized_connector_id)
                conn.execute(
                    f"""
                    UPDATE user_identity_bindings
                    SET is_enabled = ?,
                        last_reviewed_at = ?,
                        binding_revision = binding_revision + 1,
                        updated_at = ?
                    WHERE {" AND ".join(identity_clauses)}
                    """,
                    (1 if enabled else 0, utcnow_iso(), utcnow_iso(), *identity_params),
                )
            else:
                conn.execute(
                    """
                    UPDATE user_identity_bindings
                    SET is_enabled = ?,
                        last_reviewed_at = ?,
                        binding_revision = binding_revision + 1,
                        updated_at = ?
                    WHERE source_user_id = ?
                    """,
                    (1 if enabled else 0, utcnow_iso(), utcnow_iso(), source_user_id),
                )

    def set_enabled_for_source_user(
        self,
        source_user_id: str,
        enabled: bool,
        *,
        org_id: Optional[str] = None,
        source_provider: str | None = None,
        connector_id: str | None = None,
    ) -> None:
        self.set_enabled(
            source_user_id,
            enabled,
            org_id=org_id,
            source_provider=source_provider,
            connector_id=connector_id,
        )

    def apply_successful_identity_bindings(
        self,
        bindings: Iterable[dict[str, Any]],
        *,
        org_id: str,
        source_provider: str,
    ) -> None:
        """Atomically confirm bindings after an error-free Apply job."""

        rows = [dict(item) for item in bindings]
        if not rows:
            return
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_provider = str(source_provider or "").strip().lower()
        if not normalized_provider:
            raise ValueError("source_provider is required")
        now = utcnow_iso()
        with self.db.transaction() as conn:
            for item in rows:
                source_user_id = str(item.get("source_user_id") or "").strip()
                ad_username = str(item.get("ad_username") or "").strip()
                connector_id = str(item.get("connector_id") or "default").strip() or "default"
                if not source_user_id or not ad_username:
                    raise ValueError("source_user_id and ad_username are required")
                existing = conn.execute(
                    """
                    SELECT source FROM user_identity_bindings
                    WHERE org_id = ? AND source_provider = ? AND connector_id = ? AND source_user_id = ?
                    """,
                    (normalized_org_id, normalized_provider, connector_id, source_user_id),
                ).fetchone()
                source = str(item.get("source") or "managed_generated").strip()
                if existing and str(existing["source"] or "") == "manual":
                    source = "manual"
                conn.execute(
                    """
                    INSERT INTO user_identity_bindings (
                      org_id, source_provider, source_user_id, source_display_name, connector_id,
                      ad_username, target_object_guid, target_object_dn, managed_username_base,
                      source, notes, is_enabled, hit_count, last_hit_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)
                    ON CONFLICT(org_id, source_provider, connector_id, source_user_id) DO UPDATE SET
                      source_display_name = excluded.source_display_name,
                      ad_username = excluded.ad_username,
                      target_object_guid = excluded.target_object_guid,
                      target_object_dn = excluded.target_object_dn,
                      managed_username_base = excluded.managed_username_base,
                      source = CASE WHEN user_identity_bindings.source = 'manual' THEN 'manual' ELSE excluded.source END,
                      notes = CASE WHEN user_identity_bindings.source = 'manual' THEN user_identity_bindings.notes ELSE excluded.notes END,
                      is_enabled = 1,
                      hit_count = COALESCE(user_identity_bindings.hit_count, 0) + 1,
                      last_hit_at = excluded.last_hit_at,
                      binding_revision = user_identity_bindings.binding_revision + 1,
                      updated_at = excluded.updated_at
                    """,
                    (
                        normalized_org_id,
                        normalized_provider,
                        source_user_id,
                        str(item.get("source_display_name") or "").strip(),
                        connector_id,
                        ad_username,
                        str(item.get("target_object_guid") or "").strip(),
                        str(item.get("target_object_dn") or "").strip(),
                        str(item.get("managed_username_base") or "").strip(),
                        source,
                        str(item.get("notes") or "").strip(),
                        now,
                        now,
                    ),
                )

    def delete_binding_if_target_matches(
        self,
        source_user_id: str,
        ad_username: str,
        *,
        org_id: str,
        source_provider: str,
        connector_id: str,
        binding_revision: int | None = None,
    ) -> bool:
        """Delete one exact binding only when its persisted target is unchanged.

        Callers must verify the target directory state before invoking this
        compare-and-delete operation. The complete identity boundary prevents
        one provider or connector from removing another one's binding, while
        the target comparison closes the race with a concurrent binding edit.
        """

        normalized_source_user_id = str(source_user_id or "").strip()
        normalized_ad_username = str(ad_username or "").strip()
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_provider = str(source_provider or "").strip().lower()
        normalized_connector_id = str(connector_id or "").strip()
        normalized_revision = (
            max(int(binding_revision), 1)
            if binding_revision is not None
            else None
        )
        if not all(
            (
                normalized_source_user_id,
                normalized_ad_username,
                normalized_provider,
                normalized_connector_id,
            )
        ):
            return False

        revision_clause = (
            " AND binding_revision = ?" if normalized_revision is not None else ""
        )
        params: list[Any] = [
            normalized_org_id,
            normalized_provider,
            normalized_connector_id,
            normalized_source_user_id,
            normalized_ad_username,
        ]
        if normalized_revision is not None:
            params.append(normalized_revision)
        with self.db.transaction() as conn:
            cursor = conn.execute(
                f"""
                DELETE FROM user_identity_bindings
                WHERE org_id = ?
                  AND source_provider = ?
                  AND connector_id = ?
                  AND source_user_id = ?
                  AND LOWER(ad_username) = LOWER(?)
                  {revision_clause}
                """,
                tuple(params),
            )
        return cursor.rowcount == 1

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
                "LOWER(COALESCE(rule_owner, '')) LIKE ? OR "
                "LOWER(COALESCE(effective_reason, '')) LIKE ? OR "
                "LOWER(COALESCE(notes, '')) LIKE ?"
                ")"
            )
            like_pattern = f"%{normalized_query}%"
            params.extend([like_pattern] * 5)
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

    def update_governance_metadata_for_source_user(
        self,
        source_user_id: str,
        *,
        org_id: Optional[str] = None,
        rule_owner: str | None = None,
        effective_reason: str | None = None,
        next_review_at: str | None = None,
        last_reviewed_at: str | None = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        updates: list[str] = []
        params: list[Any] = []
        if rule_owner is not None:
            updates.append("rule_owner = ?")
            params.append(str(rule_owner or "").strip())
        if effective_reason is not None:
            updates.append("effective_reason = ?")
            params.append(str(effective_reason or "").strip())
        if next_review_at is not None:
            updates.append("next_review_at = ?")
            params.append(str(next_review_at or "").strip())
        if last_reviewed_at is not None:
            updates.append("last_reviewed_at = ?")
            params.append(str(last_reviewed_at or "").strip())
        if not updates:
            return
        updates.append("updated_at = ?")
        params.append(utcnow_iso())
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    f"""
                    UPDATE user_department_overrides
                    SET {", ".join(updates)}
                    WHERE org_id = ?
                      AND source_user_id = ?
                    """,
                    (*params, normalized_org_id, str(source_user_id or "").strip()),
                )
            else:
                conn.execute(
                    f"""
                    UPDATE user_department_overrides
                    SET {", ".join(updates)}
                    WHERE source_user_id = ?
                    """,
                    (*params, str(source_user_id or "").strip()),
                )

    def record_rule_hit_for_source_user(
        self,
        source_user_id: str,
        *,
        org_id: Optional[str] = None,
        hit_at: str | None = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        normalized_hit_at = str(hit_at or utcnow_iso()).strip()
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    UPDATE user_department_overrides
                    SET hit_count = COALESCE(hit_count, 0) + 1,
                        last_hit_at = ?
                    WHERE org_id = ?
                      AND source_user_id = ?
                    """,
                    (normalized_hit_at, normalized_org_id, str(source_user_id or "").strip()),
                )
            else:
                conn.execute(
                    """
                    UPDATE user_department_overrides
                    SET hit_count = COALESCE(hit_count, 0) + 1,
                        last_hit_at = ?
                    WHERE source_user_id = ?
                    """,
                    (normalized_hit_at, str(source_user_id or "").strip()),
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


class DepartmentOuMappingRepository(BaseRepository):
    def get_mapping_record(
        self,
        source_department_id: str,
        *,
        connector_id: str = "",
        org_id: Optional[str] = None,
    ) -> Optional[DepartmentOuMappingRecord]:
        normalized_department_id = str(source_department_id or "").strip()
        normalized_connector_id = str(connector_id or "").strip()
        normalized_org_id = self._resolve_org_id(org_id)
        if not normalized_department_id:
            return None
        if normalized_org_id:
            row = self._fetchone(
                """
                SELECT *
                FROM department_ou_mappings
                WHERE org_id = ?
                  AND connector_id = ?
                  AND source_department_id = ?
                LIMIT 1
                """,
                (normalized_org_id, normalized_connector_id, normalized_department_id),
            )
        else:
            row = self._fetchone(
                """
                SELECT *
                FROM department_ou_mappings
                WHERE connector_id = ?
                  AND source_department_id = ?
                ORDER BY org_id ASC, id ASC
                LIMIT 1
                """,
                (normalized_connector_id, normalized_department_id),
            )
        if not row:
            return None
        return DepartmentOuMappingRecord.from_row(row)

    def list_mapping_records(
        self,
        *,
        connector_id: Optional[str] = None,
        enabled_only: bool = False,
        org_id: Optional[str] = None,
    ) -> list[DepartmentOuMappingRecord]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if connector_id is not None:
            clauses.append("connector_id = ?")
            params.append(str(connector_id or "").strip())
        if enabled_only:
            clauses.append("is_enabled = 1")
        rows = self._fetchall(
            f"""
            SELECT *
            FROM department_ou_mappings
            WHERE {' AND '.join(clauses)}
            ORDER BY connector_id ASC, source_department_name COLLATE NOCASE ASC, source_department_id ASC, id ASC
            """,
            tuple(params),
        )
        return [DepartmentOuMappingRecord.from_row(row) for row in rows]

    def upsert_mapping(
        self,
        *,
        source_department_id: str,
        target_ou_path: str,
        connector_id: str = "",
        source_department_name: str = "",
        apply_mode: str = "subtree",
        notes: str = "",
        is_enabled: bool = True,
        org_id: Optional[str] = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_department_id = str(source_department_id or "").strip()
        normalized_connector_id = str(connector_id or "").strip()
        normalized_target_ou_path = str(target_ou_path or "").strip()
        normalized_apply_mode = str(apply_mode or "subtree").strip().lower() or "subtree"
        if normalized_apply_mode not in {"subtree", "exact"}:
            normalized_apply_mode = "subtree"
        if not normalized_department_id or not normalized_target_ou_path:
            raise ValueError("source_department_id and target_ou_path are required")

        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO department_ou_mappings (
                  org_id, connector_id, source_department_id, source_department_name, target_ou_path,
                  apply_mode, notes, is_enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, connector_id, source_department_id) DO UPDATE SET
                  source_department_name = excluded.source_department_name,
                  target_ou_path = excluded.target_ou_path,
                  apply_mode = excluded.apply_mode,
                  notes = excluded.notes,
                  is_enabled = excluded.is_enabled,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_connector_id,
                    normalized_department_id,
                    str(source_department_name or "").strip(),
                    normalized_target_ou_path,
                    normalized_apply_mode,
                    str(notes or "").strip(),
                    1 if is_enabled else 0,
                    now,
                    now,
                ),
            )

    def delete_mapping(
        self,
        source_department_id: str,
        *,
        connector_id: str = "",
        org_id: Optional[str] = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id)
        normalized_department_id = str(source_department_id or "").strip()
        normalized_connector_id = str(connector_id or "").strip()
        with self.db.transaction() as conn:
            if normalized_org_id:
                conn.execute(
                    """
                    DELETE FROM department_ou_mappings
                    WHERE org_id = ?
                      AND connector_id = ?
                      AND source_department_id = ?
                    """,
                    (normalized_org_id, normalized_connector_id, normalized_department_id),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM department_ou_mappings
                    WHERE connector_id = ?
                      AND source_department_id = ?
                    """,
                    (normalized_connector_id, normalized_department_id),
                )

    def delete_mappings_for_org(self, org_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM department_ou_mappings WHERE org_id = ?",
                (self._resolve_org_id(org_id, default="default"),),
            )
