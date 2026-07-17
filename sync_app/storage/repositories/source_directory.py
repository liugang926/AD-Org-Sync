from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from sync_app.core.fingerprints import fingerprint_json
from sync_app.core.observability import redact_sensitive_text
from sync_app.storage.local_db import BaseRepository, utcnow_iso


VALID_SCOPE_TYPES = {"full", "department", "selected_users", "source_user"}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    try:
        decoded = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return decoded if isinstance(decoded, list) else []


class SourceDirectoryRepository(BaseRepository):
    def start_refresh(
        self,
        *,
        org_id: str,
        provider_id: str,
        connector_id: str = "default",
        created_by: str = "",
    ) -> int:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO source_directory_snapshots (
                  org_id, provider_id, connector_id, status, started_at, created_by
                ) VALUES (?, ?, ?, 'refreshing', ?, ?)
                """,
                (org_id, provider_id, connector_id or "default", now, created_by),
            )
            return int(cursor.lastrowid)

    def fail_refresh(self, snapshot_id: int, error_summary: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE source_directory_snapshots
                SET status = 'failed', completed_at = ?, error_summary = ?
                WHERE id = ?
                """,
                (
                    utcnow_iso(),
                    redact_sensitive_text(error_summary)[:500],
                    int(snapshot_id),
                ),
            )

    def replace_snapshot(
        self,
        snapshot_id: int,
        *,
        departments: Iterable[dict[str, Any]],
        users: Iterable[dict[str, Any]],
        fields: Iterable[dict[str, Any]],
        fingerprint: str,
        warning_summary: str = "",
        ttl_minutes: int = 60,
    ) -> None:
        department_rows = list(departments)
        user_rows = list(users)
        field_rows = list(fields)
        employee_counts: dict[str, int] = {}
        for user in user_rows:
            employee_id = str(user.get("employee_id") or "").strip().lower()
            if employee_id:
                employee_counts[employee_id] = employee_counts.get(employee_id, 0) + 1
        now = datetime.now(timezone.utc)
        completed_at = now.isoformat(timespec="seconds")
        expires_at = (now + timedelta(minutes=max(int(ttl_minutes or 60), 1))).isoformat(timespec="seconds")
        with self.db.transaction() as conn:
            snapshot = conn.execute(
                "SELECT org_id, provider_id FROM source_directory_snapshots WHERE id = ?",
                (int(snapshot_id),),
            ).fetchone()
            if not snapshot:
                raise ValueError("source directory snapshot not found")
            org_id = str(snapshot["org_id"])
            provider_id = str(snapshot["provider_id"])
            conn.execute("DELETE FROM source_department_snapshots WHERE snapshot_id = ?", (snapshot_id,))
            conn.execute("DELETE FROM source_user_snapshots WHERE snapshot_id = ?", (snapshot_id,))
            conn.execute("DELETE FROM source_field_catalogs WHERE snapshot_id = ?", (snapshot_id,))
            conn.executemany(
                """
                INSERT INTO source_department_snapshots (
                  snapshot_id, org_id, provider_id, source_department_id, name,
                  parent_department_id, path_names_json, path_ids_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        snapshot_id, org_id, provider_id,
                        str(row.get("source_department_id") or row.get("department_id") or ""),
                        str(row.get("name") or ""),
                        str(row.get("parent_department_id") or row.get("parent_id") or ""),
                        json.dumps(row.get("path_names") or [], ensure_ascii=False),
                        json.dumps(row.get("path_ids") or [], ensure_ascii=False),
                    )
                    for row in department_rows
                ],
            )
            conn.executemany(
                """
                INSERT INTO source_user_snapshots (
                  snapshot_id, org_id, provider_id, source_user_id, display_name, employee_id,
                  email, mobile_masked, position, department_ids_json, department_names_json,
                  primary_department_id, account_status, is_active, raw_payload_json, search_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        snapshot_id, org_id, provider_id, str(row.get("source_user_id") or ""),
                        str(row.get("display_name") or ""), str(row.get("employee_id") or ""),
                        str(row.get("email") or ""), str(row.get("mobile_masked") or ""),
                        str(row.get("position") or ""),
                        json.dumps([str(value) for value in row.get("department_ids") or []], ensure_ascii=False),
                        json.dumps(row.get("department_names") or [], ensure_ascii=False),
                        str(row.get("primary_department_id") or ""),
                        str(row.get("account_status") or "active"), 1 if row.get("is_active", True) else 0,
                        json.dumps(row.get("raw_payload") or {}, ensure_ascii=False, sort_keys=True),
                        str(row.get("search_text") or "")[:2000],
                    )
                    for row in user_rows
                ],
            )
            conn.executemany(
                """
                INSERT INTO source_field_catalogs (
                  snapshot_id, org_id, provider_id, field_name, field_label,
                  data_type, coverage_count, sample_values_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        snapshot_id, org_id, provider_id, str(row.get("name") or ""),
                        str(row.get("label") or row.get("name") or ""),
                        str(row.get("data_type") or "string"), int(row.get("coverage") or 0),
                        json.dumps(row.get("samples") or [], ensure_ascii=False),
                    )
                    for row in field_rows
                ],
            )
            conn.execute(
                """
                UPDATE source_directory_snapshots
                SET status = 'succeeded', completed_at = ?, last_success_at = ?, expires_at = ?,
                    error_summary = '', warning_summary = ?, department_count = ?, user_count = ?,
                    field_count = ?, missing_employee_id_count = ?, duplicate_employee_id_count = ?,
                    snapshot_fingerprint = ?
                WHERE id = ?
                """,
                (
                    completed_at, completed_at, expires_at, str(warning_summary or "")[:500],
                    len(department_rows), len(user_rows), len(field_rows),
                    sum(1 for row in user_rows if not str(row.get("employee_id") or "").strip()),
                    sum(count for count in employee_counts.values() if count > 1),
                    fingerprint, snapshot_id,
                ),
            )

    def get_snapshot(self, snapshot_id: int, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchone(
                "SELECT * FROM source_directory_snapshots WHERE id = ? AND org_id = ?",
                (int(snapshot_id), normalized_org_id),
            )
        return self._fetchone("SELECT * FROM source_directory_snapshots WHERE id = ?", (int(snapshot_id),))

    def get_latest_successful_snapshot(
        self, *, org_id: str, provider_id: str, connector_id: str = "default"
    ):
        return self._fetchone(
            """
            SELECT * FROM source_directory_snapshots
            WHERE org_id = ? AND provider_id = ? AND connector_id = ? AND status = 'succeeded'
            ORDER BY completed_at DESC, id DESC LIMIT 1
            """,
            (org_id, provider_id, connector_id or "default"),
        )

    def get_latest_refresh(self, *, org_id: str, provider_id: str, connector_id: str = "default"):
        return self._fetchone(
            """
            SELECT * FROM source_directory_snapshots
            WHERE org_id = ? AND provider_id = ? AND connector_id = ?
            ORDER BY id DESC LIMIT 1
            """,
            (org_id, provider_id, connector_id or "default"),
        )

    def list_snapshots(
        self,
        *,
        org_id: str,
        provider_id: str = "",
        status: str = "",
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        clauses = ["org_id = ?"]
        params: list[Any] = [self._resolve_org_id(org_id, default="default")]
        normalized_provider = str(provider_id or "").strip().lower()
        normalized_status = str(status or "").strip().lower()
        if normalized_provider:
            clauses.append("provider_id = ?")
            params.append(normalized_provider)
        if normalized_status:
            clauses.append("status = ?")
            params.append(normalized_status)
        where = " AND ".join(clauses)
        total = self._fetchcount(
            f"SELECT COUNT(1) FROM source_directory_snapshots WHERE {where}",
            params,
        )
        normalized_limit = min(max(int(limit or 50), 1), 200)
        normalized_offset = max(int(offset or 0), 0)
        rows = self._fetchall(
            f"""
            SELECT *
            FROM source_directory_snapshots
            WHERE {where}
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            [*params, normalized_limit, normalized_offset],
        )
        return {
            "items": [dict(row) for row in rows],
            "total": total,
            "limit": normalized_limit,
            "offset": normalized_offset,
        }

    def list_departments(self, snapshot_id: int, *, org_id: str) -> list[dict[str, Any]]:
        rows = self._fetchall(
            """
            SELECT * FROM source_department_snapshots
            WHERE snapshot_id = ? AND org_id = ? ORDER BY path_names_json, name, source_department_id
            """,
            (int(snapshot_id), org_id),
        )
        return [dict(row) | {"path_names": _json_list(row["path_names_json"]), "path_ids": _json_list(row["path_ids_json"])} for row in rows]

    def get_department_user_counts(
        self,
        snapshot_id: int,
        *,
        org_id: str,
        selected_source_user_ids: Optional[Iterable[str]] = None,
    ) -> dict[str, dict[str, int]]:
        selected_ids = (
            {
                str(value).strip()
                for value in selected_source_user_ids
                if str(value).strip()
            }
            if selected_source_user_ids is not None
            else None
        )
        rows = self._fetchall(
            """
            SELECT source_user_id, department_ids_json
            FROM source_user_snapshots
            WHERE snapshot_id = ? AND org_id = ?
            """,
            (int(snapshot_id), org_id),
        )
        counts: dict[str, dict[str, int]] = {}
        for row in rows:
            source_user_id = str(row["source_user_id"] or "")
            for department_id in {
                str(value).strip()
                for value in _json_list(row["department_ids_json"])
                if str(value).strip()
            }:
                item = counts.setdefault(
                    department_id,
                    {"total": 0, "selected": 0},
                )
                item["total"] += 1
                if selected_ids is not None and source_user_id in selected_ids:
                    item["selected"] += 1
        return counts

    def list_users(
        self,
        snapshot_id: int,
        *,
        org_id: str,
        provider_id: str,
        search: str = "",
        department_id: str = "",
        status: str = "",
        employee_id_state: str = "",
        source_user_ids: Optional[Iterable[str]] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        clauses = ["snapshot_id = ?", "org_id = ?", "provider_id = ?"]
        params: list[Any] = [int(snapshot_id), org_id, provider_id]
        normalized_search = str(search or "").strip().lower()
        if normalized_search:
            clauses.append("LOWER(search_text) LIKE ?")
            params.append(f"%{normalized_search}%")
        if department_id:
            clauses.append("department_ids_json LIKE ?")
            params.append(f'%"{str(department_id)}"%')
        if status == "active":
            clauses.append("is_active = 1")
        elif status == "inactive":
            clauses.append("is_active = 0")
        if employee_id_state == "missing":
            clauses.append("TRIM(employee_id) = ''")
        elif employee_id_state == "present":
            clauses.append("TRIM(employee_id) <> ''")
        elif employee_id_state == "duplicate":
            clauses.append("TRIM(employee_id) <> ''")
            clauses.append(
                """
                LOWER(TRIM(employee_id)) IN (
                  SELECT LOWER(TRIM(duplicate_user.employee_id))
                  FROM source_user_snapshots AS duplicate_user
                  WHERE duplicate_user.snapshot_id = ?
                    AND duplicate_user.org_id = ?
                    AND duplicate_user.provider_id = ?
                    AND TRIM(duplicate_user.employee_id) <> ''
                  GROUP BY LOWER(TRIM(duplicate_user.employee_id))
                  HAVING COUNT(1) > 1
                )
                """
            )
            params.extend([int(snapshot_id), org_id, provider_id])
        selected_ids = sorted({str(value).strip() for value in source_user_ids or [] if str(value).strip()})
        if selected_ids:
            placeholders = ",".join("?" for _ in selected_ids)
            clauses.append(f"source_user_id IN ({placeholders})")
            params.extend(selected_ids)
        where = " AND ".join(clauses)
        total = self._fetchcount(f"SELECT COUNT(1) FROM source_user_snapshots WHERE {where}", params)
        rows = self._fetchall(
            f"""
            SELECT * FROM source_user_snapshots WHERE {where}
            ORDER BY LOWER(display_name), LOWER(source_user_id) LIMIT ? OFFSET ?
            """,
            [*params, min(max(int(limit or 50), 1), 200), max(int(offset or 0), 0)],
        )
        items = []
        for row in rows:
            item = dict(row)
            item["department_ids"] = _json_list(row["department_ids_json"])
            item["department_names"] = _json_list(row["department_names_json"])
            item["raw_payload"] = json.loads(str(row["raw_payload_json"] or "{}"))
            item["is_active"] = bool(row["is_active"])
            items.append(item)
        return {"items": items, "total": total, "limit": limit, "offset": offset}

    def list_field_catalog(self, snapshot_id: int, *, org_id: str) -> list[dict[str, Any]]:
        rows = self._fetchall(
            """
            SELECT * FROM source_field_catalogs
            WHERE snapshot_id = ? AND org_id = ? ORDER BY field_name
            """,
            (int(snapshot_id), org_id),
        )
        return [dict(row) | {"samples": _json_list(row["sample_values_json"])} for row in rows]

    def save_scope_selection(
        self,
        *,
        org_id: str,
        provider_id: str,
        connector_id: str = "default",
        scope_type: str,
        selected_department_ids: Iterable[str] = (),
        selected_source_user_ids: Iterable[str] = (),
        username_strategy: str = "userid",
        username_template: str = "",
        source_field: str = "source_user_id",
        snapshot_id: Optional[int] = None,
        requested_by: str = "",
    ) -> dict[str, Any]:
        normalized_scope = str(scope_type or "full").strip().lower()
        if normalized_scope not in VALID_SCOPE_TYPES:
            raise ValueError(f"unsupported sync scope: {scope_type}")
        departments = sorted({str(value).strip() for value in selected_department_ids if str(value).strip()})
        users = sorted({str(value).strip() for value in selected_source_user_ids if str(value).strip()})
        snapshot = self.get_snapshot(int(snapshot_id), org_id=org_id) if snapshot_id else self.get_latest_successful_snapshot(
            org_id=org_id, provider_id=provider_id, connector_id=connector_id
        )
        if not snapshot:
            raise ValueError("A successful source directory snapshot is required")
        if normalized_scope == "department" and not departments:
            raise ValueError("Department scope requires at least one department")
        if normalized_scope == "selected_users" and not users:
            raise ValueError("Selected-users scope requires at least one source user")
        if normalized_scope == "source_user" and len(users) != 1:
            raise ValueError("Source-user replay requires exactly one source user")
        if departments:
            placeholders = ",".join("?" for _ in departments)
            existing_department_count = self._fetchcount(
                f"""
                SELECT COUNT(1) FROM source_department_snapshots
                WHERE snapshot_id = ? AND org_id = ? AND source_department_id IN ({placeholders})
                """,
                [int(snapshot["id"]), org_id, *departments],
            )
            if existing_department_count != len(departments):
                raise ValueError("One or more selected departments are not in the active snapshot")
        if users:
            placeholders = ",".join("?" for _ in users)
            existing_user_count = self._fetchcount(
                f"""
                SELECT COUNT(1) FROM source_user_snapshots
                WHERE snapshot_id = ? AND org_id = ? AND source_user_id IN ({placeholders})
                """,
                [int(snapshot["id"]), org_id, *users],
            )
            if existing_user_count != len(users):
                raise ValueError("One or more selected source users are not in the active snapshot")
        source_fingerprint = str(snapshot["snapshot_fingerprint"] or "")
        payload = {
            "org_id": org_id, "provider_id": provider_id, "connector_id": connector_id or "default",
            "scope_type": normalized_scope, "selected_department_ids": departments,
            "selected_source_user_ids": users, "username_strategy": username_strategy,
            "username_template": username_template, "source_field": source_field,
            "snapshot_id": int(snapshot["id"]), "source_snapshot_fingerprint": source_fingerprint,
        }
        selection_fingerprint = fingerprint_json(payload, namespace="sync-scope-selection")
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_scope_selections (
                  org_id, provider_id, connector_id, scope_type, selected_department_ids_json,
                  selected_source_user_ids_json, username_strategy, username_template, source_field,
                  snapshot_id, source_snapshot_fingerprint, selection_fingerprint, requested_by, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, provider_id, connector_id) DO UPDATE SET
                  scope_type=excluded.scope_type,
                  selected_department_ids_json=excluded.selected_department_ids_json,
                  selected_source_user_ids_json=excluded.selected_source_user_ids_json,
                  username_strategy=excluded.username_strategy,
                  username_template=excluded.username_template,
                  source_field=excluded.source_field,
                  snapshot_id=excluded.snapshot_id,
                  source_snapshot_fingerprint=excluded.source_snapshot_fingerprint,
                  selection_fingerprint=excluded.selection_fingerprint,
                  requested_by=excluded.requested_by,
                  updated_at=excluded.updated_at
                """,
                (
                    org_id, provider_id, connector_id or "default", normalized_scope,
                    json.dumps(departments), json.dumps(users), username_strategy, username_template,
                    source_field, int(snapshot["id"]), source_fingerprint, selection_fingerprint,
                    requested_by, utcnow_iso(),
                ),
            )
        return payload | {"selection_fingerprint": selection_fingerprint}

    def get_scope_selection(self, *, org_id: str, provider_id: str, connector_id: str = "default") -> Optional[dict[str, Any]]:
        row = self._fetchone(
            """
            SELECT * FROM sync_scope_selections
            WHERE org_id = ? AND provider_id = ? AND connector_id = ?
            """,
            (org_id, provider_id, connector_id or "default"),
        )
        if not row:
            return None
        return dict(row) | {
            "selected_department_ids": _json_list(row["selected_department_ids_json"]),
            "selected_source_user_ids": _json_list(row["selected_source_user_ids_json"]),
        }

    def list_scope_selections(self, *, org_id: str) -> list[dict[str, Any]]:
        rows = self._fetchall(
            """
            SELECT * FROM sync_scope_selections
            WHERE org_id = ?
            ORDER BY provider_id, connector_id
            """,
            (str(org_id or "").strip(),),
        )
        return [
            dict(row)
            | {
                "selected_department_ids": _json_list(
                    row["selected_department_ids_json"]
                ),
                "selected_source_user_ids": _json_list(
                    row["selected_source_user_ids_json"]
                ),
            }
            for row in rows
        ]

    def delete_scope_selections_for_org(self, org_id: str) -> int:
        with self.db.transaction() as conn:
            cursor = conn.execute(
                "DELETE FROM sync_scope_selections WHERE org_id = ?",
                (str(org_id or "").strip(),),
            )
            return int(cursor.rowcount or 0)

    def bind_job_scope(
        self,
        *,
        job_id: str,
        execution_mode: str,
        config_fingerprint: str,
        selection: dict[str, Any],
        requested_by: str = "",
    ) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO sync_job_source_scopes (
                  job_id, org_id, provider_id, connector_id, execution_mode, scope_type,
                  selected_department_ids_json, selected_source_user_ids_json, requested_by,
                  config_fingerprint, source_snapshot_fingerprint, selection_fingerprint,
                  snapshot_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id, selection["org_id"], selection["provider_id"], selection.get("connector_id") or "default",
                    execution_mode, selection["scope_type"],
                    json.dumps(selection.get("selected_department_ids") or []),
                    json.dumps(selection.get("selected_source_user_ids") or []), requested_by,
                    config_fingerprint, selection.get("source_snapshot_fingerprint") or "",
                    selection.get("selection_fingerprint") or "", selection.get("snapshot_id"), utcnow_iso(),
                ),
            )

    def get_job_scope(
        self,
        job_id: str,
        *,
        org_id: str,
    ) -> Optional[dict[str, Any]]:
        row = self._fetchone(
            """
            SELECT * FROM sync_job_source_scopes
            WHERE job_id = ? AND org_id = ?
            LIMIT 1
            """,
            (str(job_id or "").strip(), str(org_id or "").strip()),
        )
        if not row:
            return None
        return dict(row) | {
            "selected_department_ids": _json_list(
                row["selected_department_ids_json"]
            ),
            "selected_source_user_ids": _json_list(
                row["selected_source_user_ids_json"]
            ),
        }


__all__ = ["SourceDirectoryRepository", "VALID_SCOPE_TYPES"]
