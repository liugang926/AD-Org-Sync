from __future__ import annotations

import json
import uuid
from typing import Any, Iterable, Optional

from sync_app.storage.local_db import BaseRepository, dumps_json, utcnow_iso


class AccountTakeoverRepository(BaseRepository):
    def save_preview(
        self,
        *,
        org_id: str,
        original_filename: str,
        file_fingerprint: str,
        preview_fingerprint: str,
        rows: Iterable[dict[str, Any]],
        created_by: str,
        batch_id: str = "",
    ) -> dict[str, Any]:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        resolved_batch_id = str(batch_id or "").strip() or f"takeover_{uuid.uuid4().hex}"
        row_items = list(rows)
        valid_count = sum(
            1 for item in row_items if item.get("validation_status") == "valid"
        )
        conflict_count = sum(
            1 for item in row_items if item.get("validation_status") == "conflict"
        )
        overwrite_count = sum(
            1 for item in row_items if item.get("proposed_action") == "would_overwrite"
        )
        status = "ready" if conflict_count == 0 and row_items else "conflicts"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO account_takeover_batches (
                  batch_id, org_id, status, original_filename, file_fingerprint,
                  row_count, valid_count, conflict_count, overwrite_count,
                  preview_fingerprint, created_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    resolved_batch_id,
                    normalized_org_id,
                    status,
                    str(original_filename or "")[:255],
                    file_fingerprint,
                    len(row_items),
                    valid_count,
                    conflict_count,
                    overwrite_count,
                    preview_fingerprint,
                    str(created_by or "").strip(),
                    now,
                    now,
                ),
            )
            for item in row_items:
                conn.execute(
                    """
                    INSERT INTO account_takeover_rows (
                      batch_id, org_id, row_number, provider_id, connector_id,
                      platform_account_id, ad_account_key, validation_status,
                      proposed_action, existing_identity_id, conflict_codes_json,
                      normalized_payload_json, result_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?)
                    """,
                    (
                        resolved_batch_id,
                        normalized_org_id,
                        int(item.get("row_number") or 0),
                        str(item.get("provider_id") or "").strip().lower(),
                        str(item.get("connector_id") or "default").strip() or "default",
                        str(item.get("platform_account_id") or "").strip(),
                        str(item.get("ad_account_key") or "").strip(),
                        str(item.get("validation_status") or "conflict"),
                        str(item.get("proposed_action") or ""),
                        str(item.get("existing_identity_id") or ""),
                        dumps_json(item.get("conflict_codes") or []) or "[]",
                        dumps_json(item.get("normalized_payload") or {}) or "{}",
                        now,
                        now,
                    ),
                )
        batch = self.get_batch(resolved_batch_id, org_id=normalized_org_id)
        if batch is None:
            raise RuntimeError("account takeover preview was not persisted")
        return batch

    def get_batch(self, batch_id: str, *, org_id: str) -> Optional[dict[str, Any]]:
        row = self._fetchone(
            "SELECT * FROM account_takeover_batches WHERE org_id = ? AND batch_id = ?",
            (self._resolve_org_id(org_id) or "default", str(batch_id or "").strip()),
        )
        return dict(row) if row else None

    def list_batches(self, *, org_id: str, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._fetchall(
            """
            SELECT * FROM account_takeover_batches
            WHERE org_id = ? ORDER BY created_at DESC, batch_id DESC LIMIT ?
            """,
            (
                self._resolve_org_id(org_id) or "default",
                min(max(int(limit or 50), 1), 200),
            ),
        )
        return [dict(row) for row in rows]

    def list_rows(self, batch_id: str, *, org_id: str) -> list[dict[str, Any]]:
        rows = self._fetchall(
            """
            SELECT * FROM account_takeover_rows
            WHERE org_id = ? AND batch_id = ? ORDER BY row_number, id
            """,
            (self._resolve_org_id(org_id) or "default", str(batch_id or "").strip()),
        )
        return [dict(row) for row in rows]

    def get_legacy_binding(
        self,
        *,
        org_id: str,
        provider_id: str,
        connector_id: str,
        platform_account_id: str,
    ) -> Optional[dict[str, Any]]:
        row = self._fetchone(
            """
            SELECT * FROM user_identity_bindings
            WHERE org_id = ? AND source_provider = ? AND connector_id = ?
              AND source_user_id = ? AND is_enabled = 1
            """,
            (
                self._resolve_org_id(org_id) or "default",
                str(provider_id or "").strip().lower(),
                str(connector_id or "default").strip() or "default",
                str(platform_account_id or "").strip(),
            ),
        )
        return dict(row) if row else None

    def approve(
        self,
        batch_id: str,
        *,
        org_id: str,
        expected_preview_fingerprint: str,
        approved_by: str,
    ) -> dict[str, Any]:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM account_takeover_batches WHERE org_id = ? AND batch_id = ?",
                (normalized_org_id, str(batch_id or "").strip()),
            ).fetchone()
            if not row:
                raise ValueError("takeover batch does not exist in this organization")
            if str(row["status"] or "") == "approved":
                if str(row["approved_by"] or "") == str(approved_by or "").strip():
                    return dict(row)
                raise ValueError("takeover batch is already approved")
            if str(row["status"] or "") != "ready" or int(row["conflict_count"] or 0):
                raise ValueError("takeover conflicts must be resolved before approval")
            if str(row["preview_fingerprint"] or "") != str(
                expected_preview_fingerprint or ""
            ).strip():
                raise ValueError("takeover preview changed; validate the import again")
            actor = str(approved_by or "").strip()
            if not actor:
                raise ValueError("approver is required")
            if actor.casefold() == str(row["created_by"] or "").casefold():
                raise ValueError("takeover submitter and approver must be different users")
            conn.execute(
                """
                UPDATE account_takeover_batches
                SET status = 'approved', approved_by = ?, approved_at = ?, updated_at = ?
                WHERE org_id = ? AND batch_id = ?
                """,
                (actor, now, now, normalized_org_id, str(batch_id or "").strip()),
            )
        approved = self.get_batch(batch_id, org_id=normalized_org_id)
        if approved is None:
            raise RuntimeError("approved takeover batch disappeared")
        return approved

    def apply(
        self,
        batch_id: str,
        *,
        org_id: str,
        expected_preview_fingerprint: str,
        applied_by: str,
    ) -> dict[str, Any]:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_batch_id = str(batch_id or "").strip()
        actor = str(applied_by or "").strip()
        now = utcnow_iso()
        with self.db.transaction() as conn:
            batch = conn.execute(
                "SELECT * FROM account_takeover_batches WHERE org_id = ? AND batch_id = ?",
                (normalized_org_id, normalized_batch_id),
            ).fetchone()
            if not batch:
                raise ValueError("takeover batch does not exist in this organization")
            if str(batch["status"] or "") == "applied":
                return dict(batch)
            if str(batch["status"] or "") != "approved":
                raise ValueError("takeover batch must be approved before Apply")
            if str(batch["preview_fingerprint"] or "") != str(
                expected_preview_fingerprint or ""
            ).strip():
                raise ValueError("takeover preview changed; validate the import again")
            if not actor:
                raise ValueError("executor is required")
            if actor.casefold() == str(batch["approved_by"] or "").casefold():
                raise ValueError("takeover approver and executor must be different users")

            rows = conn.execute(
                """
                SELECT * FROM account_takeover_rows
                WHERE org_id = ? AND batch_id = ? ORDER BY row_number, id
                """,
                (normalized_org_id, normalized_batch_id),
            ).fetchall()
            for row in rows:
                if str(row["validation_status"] or "") != "valid":
                    raise ValueError("takeover batch contains a row that is not valid")
                payload = json.loads(str(row["normalized_payload_json"] or "{}"))
                platform_id = int(payload.get("platform_db_id") or 0)
                ad_id = int(payload.get("ad_db_id") or 0)
                platform = conn.execute(
                    "SELECT * FROM platform_accounts WHERE org_id = ? AND id = ?",
                    (normalized_org_id, platform_id),
                ).fetchone()
                ad_account = conn.execute(
                    "SELECT * FROM ad_accounts WHERE org_id = ? AND id = ?",
                    (normalized_org_id, ad_id),
                ).fetchone()
                if not platform or not ad_account:
                    raise ValueError("takeover evidence is stale; an account disappeared")
                if bool(platform["is_excluded"]) or bool(ad_account["is_protected"]):
                    raise ValueError("takeover evidence is stale; account protection changed")
                if str(platform["account_type"] or "person") in {"service", "shared", "test"}:
                    raise ValueError("non-person platform account cannot be taken over")
                if str(ad_account["account_type"] or "person") in {"service", "shared", "test"}:
                    raise ValueError("non-person AD account cannot be taken over")

                platform_link = conn.execute(
                    """
                    SELECT * FROM identity_account_links
                    WHERE org_id = ? AND account_kind = 'platform'
                      AND platform_account_id = ? AND status = 'active'
                    """,
                    (normalized_org_id, platform_id),
                ).fetchone()
                ad_link = conn.execute(
                    """
                    SELECT * FROM identity_account_links
                    WHERE org_id = ? AND account_kind = 'ad'
                      AND ad_account_id = ? AND status = 'active'
                    """,
                    (normalized_org_id, ad_id),
                ).fetchone()
                identities = {
                    str(value)
                    for value in (
                        platform_link["identity_id"] if platform_link else "",
                        ad_link["identity_id"] if ad_link else "",
                    )
                    if str(value or "").strip()
                }
                if len(identities) > 1:
                    raise ValueError("takeover would overwrite an active account relationship")
                identity_id = next(iter(identities)) if identities else f"eid_{uuid.uuid4().hex}"
                identity = conn.execute(
                    "SELECT identity_id FROM enterprise_identities WHERE org_id = ? AND identity_id = ? AND status = 'active'",
                    (normalized_org_id, identity_id),
                ).fetchone()
                if not identity:
                    canonical_fields = {
                        "display_name": str(platform["display_name"] or ""),
                        "employee_id": str(platform["employee_id"] or ""),
                        "email": str(platform["email"] or ""),
                        "mobile": str(platform["mobile"] or ""),
                    }
                    conn.execute(
                        """
                        INSERT INTO enterprise_identities (
                          identity_id, org_id, display_name, canonical_employee_id,
                          employment_status, employment_type, primary_department_id,
                          canonical_fields_json, field_sources_json, status,
                          identity_revision, created_by, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, 'employee', ?, ?, ?, 'active', 1, ?, ?, ?)
                        """,
                        (
                            identity_id,
                            normalized_org_id,
                            str(platform["display_name"] or ""),
                            str(platform["employee_id"] or ""),
                            str(platform["account_status"] or "active"),
                            str(platform["primary_department_id"] or ""),
                            dumps_json(canonical_fields) or "{}",
                            dumps_json({"takeover_batch_id": normalized_batch_id}) or "{}",
                            actor,
                            now,
                            now,
                        ),
                    )
                link_ids: list[int] = []
                evidence = dumps_json(
                    {
                        "batch_id": normalized_batch_id,
                        "row_number": int(row["row_number"]),
                        "preview_fingerprint": str(batch["preview_fingerprint"] or ""),
                    }
                ) or "{}"
                if platform_link:
                    link_ids.append(int(platform_link["id"]))
                else:
                    cursor = conn.execute(
                        """
                        INSERT INTO identity_account_links (
                          org_id, identity_id, account_kind, platform_account_id,
                          ad_account_id, account_role, account_purpose,
                          association_type, status, source, evidence_json,
                          confidence, created_by, valid_until, link_revision,
                          created_at, updated_at
                        ) VALUES (?, ?, 'platform', ?, NULL, 'source', '',
                                  'manual_permanent', 'active', 'account_takeover',
                                  ?, 100, ?, '', 1, ?, ?)
                        """,
                        (normalized_org_id, identity_id, platform_id, evidence, actor, now, now),
                    )
                    link_ids.append(int(cursor.lastrowid))
                if ad_link:
                    link_ids.append(int(ad_link["id"]))
                else:
                    primary = conn.execute(
                        """
                        SELECT id FROM identity_account_links
                        WHERE org_id = ? AND identity_id = ? AND account_kind = 'ad'
                          AND account_role = 'primary_ad' AND status = 'active'
                        """,
                        (normalized_org_id, identity_id),
                    ).fetchone()
                    if primary:
                        raise ValueError("takeover would add a second primary AD account")
                    cursor = conn.execute(
                        """
                        INSERT INTO identity_account_links (
                          org_id, identity_id, account_kind, platform_account_id,
                          ad_account_id, account_role, account_purpose,
                          association_type, status, source, evidence_json,
                          confidence, created_by, valid_until, link_revision,
                          created_at, updated_at
                        ) VALUES (?, ?, 'ad', NULL, ?, 'primary_ad', '',
                                  'manual_permanent', 'active', 'account_takeover',
                                  ?, 100, ?, '', 1, ?, ?)
                        """,
                        (normalized_org_id, identity_id, ad_id, evidence, actor, now, now),
                    )
                    link_ids.append(int(cursor.lastrowid))

                legacy = conn.execute(
                    """
                    SELECT * FROM user_identity_bindings
                    WHERE org_id = ? AND source_provider = ? AND connector_id = ?
                      AND source_user_id = ? AND is_enabled = 1
                    """,
                    (
                        normalized_org_id,
                        str(platform["provider_id"] or ""),
                        str(platform["connector_id"] or "default"),
                        str(platform["platform_account_id"] or ""),
                    ),
                ).fetchone()
                if legacy and str(legacy["ad_username"] or "").casefold() != str(
                    ad_account["sam_account_name"] or ""
                ).casefold():
                    raise ValueError("takeover would overwrite an active legacy binding")
                conn.execute(
                    """
                    INSERT INTO user_identity_bindings (
                      org_id, source_provider, source_user_id, source_display_name,
                      connector_id, ad_username, target_object_guid, target_object_dn,
                      managed_username_base, source, notes, is_enabled, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', 'manual', ?, 1, ?)
                    ON CONFLICT(org_id, source_provider, connector_id, source_user_id)
                    DO UPDATE SET source_display_name = excluded.source_display_name,
                      target_object_guid = excluded.target_object_guid,
                      target_object_dn = excluded.target_object_dn,
                      source = 'manual', notes = excluded.notes,
                      is_enabled = 1,
                      binding_revision = user_identity_bindings.binding_revision + 1,
                      updated_at = excluded.updated_at
                    """,
                    (
                        normalized_org_id,
                        str(platform["provider_id"] or ""),
                        str(platform["platform_account_id"] or ""),
                        str(platform["display_name"] or ""),
                        str(platform["connector_id"] or "default"),
                        str(ad_account["sam_account_name"] or ""),
                        str(ad_account["object_guid"] or ""),
                        str(ad_account["distinguished_name"] or ""),
                        f"Takeover batch {normalized_batch_id}",
                        now,
                    ),
                )
                conn.execute(
                    """
                    UPDATE account_takeover_rows
                    SET result_json = ?, updated_at = ? WHERE id = ?
                    """,
                    (
                        dumps_json(
                            {
                                "identity_id": identity_id,
                                "link_ids": link_ids,
                                "status": "applied",
                            }
                        )
                        or "{}",
                        now,
                        int(row["id"]),
                    ),
                )
            conn.execute(
                """
                UPDATE account_takeover_batches
                SET status = 'applied', applied_by = ?, applied_at = ?, updated_at = ?
                WHERE org_id = ? AND batch_id = ?
                """,
                (actor, now, now, normalized_org_id, normalized_batch_id),
            )
        applied = self.get_batch(normalized_batch_id, org_id=normalized_org_id)
        if applied is None:
            raise RuntimeError("applied takeover batch disappeared")
        return applied


__all__ = ["AccountTakeoverRepository"]
