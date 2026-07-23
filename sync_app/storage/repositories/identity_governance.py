from __future__ import annotations

import uuid
from typing import Any, Iterable, Optional

from sync_app.core.models import (
    ADAccountRecord,
    EnterpriseIdentityRecord,
    FieldAuthorityRuleRecord,
    IdentityAccountLinkRecord,
    IdentityMatchCandidateRecord,
    IdentityMatchDecisionRecord,
    IdentityMatchRuleRecord,
    PlatformAccountRecord,
)
from sync_app.storage.local_db import BaseRepository, dumps_json, utcnow_iso


DEFAULT_IDENTITY_MATCH_RULES = (
    {
        "rule_order": 10,
        "rule_name": "employee_id_to_employee_id",
        "source_field": "employee_id",
        "ad_field": "employee_id",
        "allow_auto_link": True,
        "confidence_level": "certain",
        "confidence_score": 100,
    },
    {
        "rule_order": 20,
        "rule_name": "employee_id_to_employee_number",
        "source_field": "employee_id",
        "ad_field": "employee_number",
        "allow_auto_link": True,
        "confidence_level": "high",
        "confidence_score": 95,
    },
    {
        "rule_order": 30,
        "rule_name": "email_to_mail",
        "source_field": "email",
        "ad_field": "mail",
        "lowercase_email": True,
        "allow_auto_link": False,
        "confidence_level": "high",
        "confidence_score": 80,
    },
    {
        "rule_order": 40,
        "rule_name": "email_localpart_to_sam",
        "source_field": "email_localpart",
        "ad_field": "sam_account_name",
        "lowercase_email": True,
        "allow_auto_link": False,
        "confidence_level": "medium",
        "confidence_score": 70,
    },
    {
        "rule_order": 50,
        "rule_name": "mobile_to_mobile",
        "source_field": "mobile",
        "ad_field": "mobile",
        "strip_phone_country_code": True,
        "allow_auto_link": False,
        "confidence_level": "medium",
        "confidence_score": 75,
    },
)

DEFAULT_FIELD_AUTHORITY_RULES = (
    ("employee_id", "dingtalk", 10, "source_to_ad"),
    ("employee_id", "wecom", 20, "source_to_ad"),
    ("employee_id", "feishu", 30, "source_to_ad"),
    ("mobile", "wecom", 10, "source_to_ad"),
    ("mobile", "dingtalk", 20, "source_to_ad"),
    ("mobile", "feishu", 30, "source_to_ad"),
    ("manager_account_id", "feishu", 10, "source_to_ad"),
    ("manager_account_id", "dingtalk", 20, "source_to_ad"),
    ("manager_account_id", "wecom", 30, "source_to_ad"),
    ("primary_department_id", "dingtalk", 10, "source_to_ad"),
    ("primary_department_id", "wecom", 20, "source_to_ad"),
    ("primary_department_id", "feishu", 30, "source_to_ad"),
    ("display_name", "dingtalk", 10, "source_to_ad"),
    ("display_name", "wecom", 20, "source_to_ad"),
    ("display_name", "feishu", 30, "source_to_ad"),
)

FIELD_AUTHORITY_DIRECTIONS = {
    "source_to_ad",
    "ad_to_source",
    "bidirectional",
    "compare_only",
    "manual",
    "create_only",
}
FIELD_AUTHORITY_SYNC_MODES = {
    "replace",
    "fill_if_empty",
    "preserve",
    "compare_only",
    "manual",
    "create_only",
}


class EnterpriseIdentityRepository(BaseRepository):
    def create_identity(
        self,
        *,
        org_id: str,
        display_name: str = "",
        canonical_employee_id: str = "",
        employment_status: str = "active",
        employment_type: str = "employee",
        primary_department_id: str = "",
        canonical_fields: Optional[dict[str, Any]] = None,
        field_sources: Optional[dict[str, Any]] = None,
        created_by: str = "",
        identity_id: str = "",
    ) -> EnterpriseIdentityRecord:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        resolved_identity_id = str(identity_id or "").strip() or f"eid_{uuid.uuid4().hex}"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO enterprise_identities (
                  identity_id, org_id, display_name, canonical_employee_id,
                  employment_status, employment_type, primary_department_id,
                  canonical_fields_json, field_sources_json, status,
                  identity_revision, created_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', 1, ?, ?, ?)
                """,
                (
                    resolved_identity_id,
                    normalized_org_id,
                    str(display_name or "").strip(),
                    str(canonical_employee_id or "").strip(),
                    str(employment_status or "active").strip(),
                    str(employment_type or "employee").strip(),
                    str(primary_department_id or "").strip(),
                    dumps_json(canonical_fields or {}) or "{}",
                    dumps_json(field_sources or {}) or "{}",
                    str(created_by or "").strip(),
                    now,
                    now,
                ),
            )
        record = self.get_identity(resolved_identity_id, org_id=normalized_org_id)
        if record is None:
            raise RuntimeError("enterprise identity was not persisted")
        return record

    def get_identity(
        self, identity_id: str, *, org_id: Optional[str] = None
    ) -> Optional[EnterpriseIdentityRecord]:
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            row = self._fetchone(
                "SELECT * FROM enterprise_identities WHERE org_id = ? AND identity_id = ?",
                (normalized_org_id, identity_id),
            )
        else:
            row = self._fetchone(
                "SELECT * FROM enterprise_identities WHERE identity_id = ?",
                (identity_id,),
            )
        return EnterpriseIdentityRecord.from_row(row) if row else None

    def list_identities(
        self, *, org_id: str, status: str = "active"
    ) -> list[EnterpriseIdentityRecord]:
        clauses = ["org_id = ?"]
        params: list[Any] = [self._resolve_org_id(org_id) or "default"]
        if status and status != "all":
            clauses.append("status = ?")
            params.append(status)
        rows = self._fetchall(
            f"SELECT * FROM enterprise_identities WHERE {' AND '.join(clauses)} ORDER BY display_name, identity_id",
            params,
        )
        return [EnterpriseIdentityRecord.from_row(row) for row in rows]

    def update_canonical_fields(
        self,
        identity_id: str,
        *,
        org_id: str,
        expected_revision: int,
        display_name: str,
        canonical_employee_id: str,
        primary_department_id: str,
        canonical_fields: dict[str, Any],
        field_sources: dict[str, Any],
    ) -> EnterpriseIdentityRecord:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE enterprise_identities
                SET display_name = ?, canonical_employee_id = ?,
                    primary_department_id = ?, canonical_fields_json = ?,
                    field_sources_json = ?, identity_revision = identity_revision + 1,
                    updated_at = ?
                WHERE org_id = ? AND identity_id = ? AND identity_revision = ?
                  AND status = 'active'
                """,
                (
                    str(display_name or "").strip(),
                    str(canonical_employee_id or "").strip(),
                    str(primary_department_id or "").strip(),
                    dumps_json(canonical_fields) or "{}",
                    dumps_json(field_sources) or "{}",
                    utcnow_iso(),
                    normalized_org_id,
                    identity_id,
                    int(expected_revision),
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("enterprise identity changed or is unavailable")
        record = self.get_identity(identity_id, org_id=normalized_org_id)
        if record is None:
            raise RuntimeError("enterprise identity was not found after update")
        return record

    def list_links(
        self,
        *,
        org_id: str,
        identity_id: str = "",
        status: str = "active",
    ) -> list[IdentityAccountLinkRecord]:
        clauses = ["org_id = ?"]
        params: list[Any] = [self._resolve_org_id(org_id) or "default"]
        if identity_id:
            clauses.append("identity_id = ?")
            params.append(identity_id)
        if status and status != "all":
            clauses.append("status = ?")
            params.append(status)
        rows = self._fetchall(
            f"SELECT * FROM identity_account_links WHERE {' AND '.join(clauses)} ORDER BY identity_id, account_kind, id",
            params,
        )
        return [IdentityAccountLinkRecord.from_row(row) for row in rows]

    def get_active_link_for_platform_account(
        self, platform_account_id: int, *, org_id: str
    ) -> Optional[IdentityAccountLinkRecord]:
        row = self._fetchone(
            """
            SELECT * FROM identity_account_links
            WHERE org_id = ? AND account_kind = 'platform'
              AND platform_account_id = ? AND status = 'active'
            """,
            (self._resolve_org_id(org_id) or "default", int(platform_account_id)),
        )
        return IdentityAccountLinkRecord.from_row(row) if row else None

    def get_active_link_for_ad_account(
        self, ad_account_id: int, *, org_id: str
    ) -> Optional[IdentityAccountLinkRecord]:
        row = self._fetchone(
            """
            SELECT * FROM identity_account_links
            WHERE org_id = ? AND account_kind = 'ad'
              AND ad_account_id = ? AND status = 'active'
            """,
            (self._resolve_org_id(org_id) or "default", int(ad_account_id)),
        )
        return IdentityAccountLinkRecord.from_row(row) if row else None

    def link_account(
        self,
        *,
        org_id: str,
        identity_id: str,
        account_kind: str,
        platform_account_id: Optional[int] = None,
        ad_account_id: Optional[int] = None,
        account_role: str = "source",
        account_purpose: str = "",
        association_type: str = "automatic",
        source: str = "",
        evidence: Optional[dict[str, Any]] = None,
        confidence: int = 0,
        created_by: str = "",
        valid_until: str = "",
    ) -> IdentityAccountLinkRecord:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_kind = str(account_kind or "").strip().lower()
        if normalized_kind not in {"platform", "ad"}:
            raise ValueError("account_kind must be platform or ad")
        if normalized_kind == "platform" and (platform_account_id is None or ad_account_id is not None):
            raise ValueError("platform link requires only platform_account_id")
        if normalized_kind == "ad" and (ad_account_id is None or platform_account_id is not None):
            raise ValueError("AD link requires only ad_account_id")
        now = utcnow_iso()
        with self.db.transaction() as conn:
            identity_row = conn.execute(
                "SELECT identity_id FROM enterprise_identities WHERE org_id = ? AND identity_id = ? AND status = 'active'",
                (normalized_org_id, identity_id),
            ).fetchone()
            if not identity_row:
                raise ValueError("enterprise identity does not exist in this organization")
            if normalized_kind == "platform":
                account_row = conn.execute(
                    "SELECT id FROM platform_accounts WHERE org_id = ? AND id = ?",
                    (normalized_org_id, int(platform_account_id or 0)),
                ).fetchone()
                conflict = conn.execute(
                    """
                    SELECT * FROM identity_account_links
                    WHERE org_id = ? AND account_kind = 'platform'
                      AND platform_account_id = ? AND status = 'active'
                    """,
                    (normalized_org_id, int(platform_account_id or 0)),
                ).fetchone()
            else:
                account_row = conn.execute(
                    "SELECT id FROM ad_accounts WHERE org_id = ? AND id = ?",
                    (normalized_org_id, int(ad_account_id or 0)),
                ).fetchone()
                conflict = conn.execute(
                    """
                    SELECT * FROM identity_account_links
                    WHERE org_id = ? AND account_kind = 'ad'
                      AND ad_account_id = ? AND status = 'active'
                    """,
                    (normalized_org_id, int(ad_account_id or 0)),
                ).fetchone()
            if not account_row:
                raise ValueError("account does not exist in this organization")
            if conflict:
                existing = IdentityAccountLinkRecord.from_row(conflict)
                if existing.identity_id != identity_id:
                    raise ValueError("account is already linked to another enterprise identity")
                return existing
            if normalized_kind == "ad" and account_role == "primary_ad":
                primary = conn.execute(
                    """
                    SELECT id FROM identity_account_links
                    WHERE org_id = ? AND identity_id = ? AND account_kind = 'ad'
                      AND account_role = 'primary_ad' AND status = 'active'
                    """,
                    (normalized_org_id, identity_id),
                ).fetchone()
                if primary:
                    raise ValueError("enterprise identity already has a primary AD account")
            cursor = conn.execute(
                """
                INSERT INTO identity_account_links (
                  org_id, identity_id, account_kind, platform_account_id,
                  ad_account_id, account_role, account_purpose,
                  association_type, status, source, evidence_json, confidence,
                  created_by, valid_until, link_revision, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    normalized_org_id,
                    identity_id,
                    normalized_kind,
                    platform_account_id,
                    ad_account_id,
                    account_role,
                    account_purpose,
                    association_type,
                    source,
                    dumps_json(evidence or {}) or "{}",
                    min(max(int(confidence or 0), 0), 100),
                    created_by,
                    valid_until,
                    now,
                    now,
                ),
            )
            link_id = int(cursor.lastrowid)
        row = self._fetchone("SELECT * FROM identity_account_links WHERE id = ?", (link_id,))
        if not row:
            raise RuntimeError("identity account link was not persisted")
        return IdentityAccountLinkRecord.from_row(row)


class PlatformAccountRepository(BaseRepository):
    def upsert_account(
        self,
        *,
        org_id: str,
        provider_id: str,
        connector_id: str,
        platform_account_id: str,
        display_name: str = "",
        employee_id: str = "",
        email: str = "",
        mobile: str = "",
        account_status: str = "active",
        account_type: str = "person",
        primary_department_id: str = "",
        department_ids: Iterable[Any] = (),
        manager_account_id: str = "",
        custom_fields: Optional[dict[str, Any]] = None,
        source_snapshot_id: Optional[int] = None,
        raw_payload: Optional[dict[str, Any]] = None,
        is_excluded: bool = False,
    ) -> PlatformAccountRecord:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_provider = str(provider_id or "").strip().lower()
        normalized_connector = str(connector_id or "default").strip() or "default"
        normalized_account_id = str(platform_account_id or "").strip()
        if not normalized_provider or not normalized_account_id:
            raise ValueError("provider_id and stable platform_account_id are required")
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO platform_accounts (
                  org_id, provider_id, connector_id, platform_account_id,
                  display_name, employee_id, email, mobile, account_status,
                  account_type, primary_department_id, department_ids_json,
                  manager_account_id, custom_fields_json, source_snapshot_id,
                  raw_payload_json, is_excluded, first_seen_at, last_seen_at,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, provider_id, connector_id, platform_account_id)
                DO UPDATE SET
                  display_name = excluded.display_name,
                  employee_id = excluded.employee_id,
                  email = excluded.email,
                  mobile = excluded.mobile,
                  account_status = excluded.account_status,
                  account_type = excluded.account_type,
                  primary_department_id = excluded.primary_department_id,
                  department_ids_json = excluded.department_ids_json,
                  manager_account_id = excluded.manager_account_id,
                  custom_fields_json = excluded.custom_fields_json,
                  source_snapshot_id = excluded.source_snapshot_id,
                  raw_payload_json = excluded.raw_payload_json,
                  is_excluded = excluded.is_excluded,
                  last_seen_at = excluded.last_seen_at,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_provider,
                    normalized_connector,
                    normalized_account_id,
                    str(display_name or "").strip(),
                    str(employee_id or "").strip(),
                    str(email or "").strip(),
                    str(mobile or "").strip(),
                    str(account_status or "active").strip(),
                    str(account_type or "person").strip(),
                    str(primary_department_id or "").strip(),
                    dumps_json([str(value) for value in department_ids]) or "[]",
                    str(manager_account_id or "").strip(),
                    dumps_json(custom_fields or {}) or "{}",
                    source_snapshot_id,
                    dumps_json(raw_payload or {}) or "{}",
                    int(bool(is_excluded)),
                    now,
                    now,
                    now,
                    now,
                ),
            )
        record = self.get_account_by_stable_id(
            org_id=normalized_org_id,
            provider_id=normalized_provider,
            connector_id=normalized_connector,
            platform_account_id=normalized_account_id,
        )
        if record is None:
            raise RuntimeError("platform account was not persisted")
        return record

    def get_account(self, account_id: int, *, org_id: str) -> Optional[PlatformAccountRecord]:
        row = self._fetchone(
            "SELECT * FROM platform_accounts WHERE org_id = ? AND id = ?",
            (self._resolve_org_id(org_id) or "default", int(account_id)),
        )
        return PlatformAccountRecord.from_row(row) if row else None

    def get_account_by_stable_id(
        self,
        *,
        org_id: str,
        provider_id: str,
        connector_id: str,
        platform_account_id: str,
    ) -> Optional[PlatformAccountRecord]:
        row = self._fetchone(
            """
            SELECT * FROM platform_accounts
            WHERE org_id = ? AND provider_id = ? AND connector_id = ?
              AND platform_account_id = ?
            """,
            (
                self._resolve_org_id(org_id) or "default",
                str(provider_id or "").strip().lower(),
                str(connector_id or "default").strip() or "default",
                str(platform_account_id or "").strip(),
            ),
        )
        return PlatformAccountRecord.from_row(row) if row else None

    def list_accounts(self, *, org_id: str) -> list[PlatformAccountRecord]:
        rows = self._fetchall(
            "SELECT * FROM platform_accounts WHERE org_id = ? ORDER BY provider_id, connector_id, platform_account_id",
            (self._resolve_org_id(org_id) or "default",),
        )
        return [PlatformAccountRecord.from_row(row) for row in rows]


class ADAccountRepository(BaseRepository):
    _FIELDS = (
        "object_guid", "object_sid", "distinguished_name", "sam_account_name",
        "user_principal_name", "employee_id", "employee_number", "mail",
        "telephone_number", "mobile", "display_name", "manager_dn", "ou_path",
        "when_created", "when_changed", "account_type",
    )

    def upsert_account(
        self,
        *,
        org_id: str,
        connector_id: str,
        object_guid: str = "",
        object_sid: str = "",
        distinguished_name: str = "",
        sam_account_name: str = "",
        user_principal_name: str = "",
        employee_id: str = "",
        employee_number: str = "",
        mail: str = "",
        telephone_number: str = "",
        mobile: str = "",
        display_name: str = "",
        account_enabled: Optional[bool] = None,
        manager_dn: str = "",
        group_membership: Iterable[Any] = (),
        ou_path: str = "",
        extension_attributes: Optional[dict[str, Any]] = None,
        when_created: str = "",
        when_changed: str = "",
        account_type: str = "person",
        is_protected: bool = False,
        latest_snapshot_id: Optional[int] = None,
    ) -> ADAccountRecord:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_connector = str(connector_id or "default").strip() or "default"
        guid = str(object_guid or "").strip()
        sam = str(sam_account_name or "").strip()
        if not guid and not sam:
            raise ValueError("AD account requires objectGUID or sAMAccountName")
        now = utcnow_iso()
        scalar_values = {
            "object_guid": guid,
            "object_sid": str(object_sid or "").strip(),
            "distinguished_name": str(distinguished_name or "").strip(),
            "sam_account_name": sam,
            "user_principal_name": str(user_principal_name or "").strip(),
            "employee_id": str(employee_id or "").strip(),
            "employee_number": str(employee_number or "").strip(),
            "mail": str(mail or "").strip(),
            "telephone_number": str(telephone_number or "").strip(),
            "mobile": str(mobile or "").strip(),
            "display_name": str(display_name or "").strip(),
            "manager_dn": str(manager_dn or "").strip(),
            "ou_path": str(ou_path or "").strip(),
            "when_created": str(when_created or "").strip(),
            "when_changed": str(when_changed or "").strip(),
            "account_type": str(account_type or "person").strip(),
        }
        with self.db.transaction() as conn:
            if guid:
                row = conn.execute(
                    "SELECT id FROM ad_accounts WHERE org_id = ? AND connector_id = ? AND object_guid = ?",
                    (normalized_org_id, normalized_connector, guid),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT id FROM ad_accounts WHERE org_id = ? AND connector_id = ? AND LOWER(sam_account_name) = LOWER(?)",
                    (normalized_org_id, normalized_connector, sam),
                ).fetchone()
            values = [scalar_values[field_name] for field_name in self._FIELDS]
            if row:
                account_id = int(row["id"])
                assignments = ", ".join(f"{field_name} = ?" for field_name in self._FIELDS)
                conn.execute(
                    f"""
                    UPDATE ad_accounts SET {assignments}, account_enabled = ?,
                      group_membership_json = ?, extension_attributes_json = ?,
                      is_protected = ?, latest_snapshot_id = ?, last_seen_at = ?, updated_at = ?
                    WHERE org_id = ? AND id = ?
                    """,
                    (
                        *values,
                        None if account_enabled is None else int(bool(account_enabled)),
                        dumps_json([str(value) for value in group_membership]) or "[]",
                        dumps_json(extension_attributes or {}) or "{}",
                        int(bool(is_protected)),
                        latest_snapshot_id,
                        now,
                        now,
                        normalized_org_id,
                        account_id,
                    ),
                )
            else:
                cursor = conn.execute(
                    f"""
                    INSERT INTO ad_accounts (
                      org_id, connector_id, {', '.join(self._FIELDS)}, account_enabled,
                      group_membership_json, extension_attributes_json, is_protected,
                      latest_snapshot_id, first_seen_at, last_seen_at, created_at, updated_at
                    ) VALUES ({', '.join('?' for _ in range(2 + len(self._FIELDS) + 9))})
                    """,
                    (
                        normalized_org_id,
                        normalized_connector,
                        *values,
                        None if account_enabled is None else int(bool(account_enabled)),
                        dumps_json([str(value) for value in group_membership]) or "[]",
                        dumps_json(extension_attributes or {}) or "{}",
                        int(bool(is_protected)),
                        latest_snapshot_id,
                        now,
                        now,
                        now,
                        now,
                    ),
                )
                account_id = int(cursor.lastrowid)
        record = self.get_account(account_id, org_id=normalized_org_id)
        if record is None:
            raise RuntimeError("AD account was not persisted")
        return record

    def get_account(self, account_id: int, *, org_id: str) -> Optional[ADAccountRecord]:
        row = self._fetchone(
            "SELECT * FROM ad_accounts WHERE org_id = ? AND id = ?",
            (self._resolve_org_id(org_id) or "default", int(account_id)),
        )
        return ADAccountRecord.from_row(row) if row else None

    def get_account_by_stable_key(
        self, *, org_id: str, connector_id: str, account_key: str
    ) -> Optional[ADAccountRecord]:
        normalized_key = str(account_key or "").strip()
        if not normalized_key:
            return None
        rows = self._fetchall(
            """
            SELECT * FROM ad_accounts
            WHERE org_id = ? AND connector_id = ?
              AND (
                object_guid = ? OR object_sid = ?
                OR LOWER(sam_account_name) = LOWER(?)
                OR LOWER(user_principal_name) = LOWER(?)
              )
            ORDER BY
              CASE WHEN object_guid = ? THEN 1 WHEN object_sid = ? THEN 2
                   WHEN LOWER(sam_account_name) = LOWER(?) THEN 3 ELSE 4 END,
              id
            LIMIT 2
            """,
            (
                self._resolve_org_id(org_id) or "default",
                str(connector_id or "default").strip() or "default",
                normalized_key,
                normalized_key,
                normalized_key,
                normalized_key,
                normalized_key,
                normalized_key,
                normalized_key,
            ),
        )
        if len(rows) > 1:
            raise ValueError("AD account key is ambiguous in this connector")
        return ADAccountRecord.from_row(rows[0]) if rows else None

    def list_accounts(
        self, *, org_id: str, connector_id: str = ""
    ) -> list[ADAccountRecord]:
        clauses = ["org_id = ?"]
        params: list[Any] = [self._resolve_org_id(org_id) or "default"]
        if connector_id:
            clauses.append("connector_id = ?")
            params.append(connector_id)
        rows = self._fetchall(
            f"SELECT * FROM ad_accounts WHERE {' AND '.join(clauses)} ORDER BY connector_id, sam_account_name, id",
            params,
        )
        return [ADAccountRecord.from_row(row) for row in rows]


class ADDirectorySnapshotRepository(BaseRepository):
    def start_snapshot(
        self, *, org_id: str, connector_id: str, created_by: str = ""
    ) -> int:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO ad_directory_snapshots (
                  org_id, connector_id, status, started_at, created_by
                ) VALUES (?, ?, 'refreshing', ?, ?)
                """,
                (
                    self._resolve_org_id(org_id) or "default",
                    str(connector_id or "default").strip() or "default",
                    now,
                    str(created_by or "").strip(),
                ),
            )
            return int(cursor.lastrowid)

    def replace_ous(
        self,
        snapshot_id: int,
        *,
        org_id: str,
        connector_id: str,
        organizational_units: Iterable[dict[str, Any]],
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_connector = str(connector_id or "default").strip() or "default"
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM ad_ou_snapshots WHERE snapshot_id = ?", (int(snapshot_id),))
            for item in organizational_units:
                dn = str(item.get("dn") or item.get("distinguished_name") or "").strip()
                if not dn:
                    continue
                path_value = item.get("path") or []
                if isinstance(path_value, (list, tuple)):
                    path = "/".join(str(value) for value in path_value if str(value).strip())
                else:
                    path = str(path_value or "")
                conn.execute(
                    """
                    INSERT INTO ad_ou_snapshots (
                      snapshot_id, org_id, connector_id, object_guid,
                      distinguished_name, name, parent_distinguished_name,
                      path, when_created, when_changed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(snapshot_id),
                        normalized_org_id,
                        normalized_connector,
                        str(item.get("guid") or item.get("object_guid") or ""),
                        dn,
                        str(item.get("name") or ""),
                        str(item.get("parent_dn") or ""),
                        path,
                        str(item.get("when_created") or ""),
                        str(item.get("when_changed") or ""),
                    ),
                )

    def complete_snapshot(
        self,
        snapshot_id: int,
        *,
        org_id: str,
        user_count: int,
        ou_count: int,
        duplicate_employee_id_count: int,
        duplicate_employee_number_count: int,
        snapshot_fingerprint: str,
        capability_report: Optional[dict[str, Any]] = None,
        metadata: Optional[dict[str, Any]] = None,
        expires_at: str = "",
    ) -> None:
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE ad_directory_snapshots
                SET status = 'success', completed_at = ?, expires_at = ?,
                    user_count = ?, ou_count = ?, duplicate_employee_id_count = ?,
                    duplicate_employee_number_count = ?, snapshot_fingerprint = ?,
                    capability_report_json = ?, metadata_json = ?, error_summary = ''
                WHERE id = ? AND org_id = ?
                """,
                (
                    utcnow_iso(),
                    expires_at,
                    int(user_count),
                    int(ou_count),
                    int(duplicate_employee_id_count),
                    int(duplicate_employee_number_count),
                    snapshot_fingerprint,
                    dumps_json(capability_report or {}) or "{}",
                    dumps_json(metadata or {}) or "{}",
                    int(snapshot_id),
                    self._resolve_org_id(org_id) or "default",
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("AD snapshot does not exist in this organization")

    def fail_snapshot(self, snapshot_id: int, *, org_id: str, error_summary: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE ad_directory_snapshots
                SET status = 'failed', completed_at = ?, error_summary = ?
                WHERE id = ? AND org_id = ?
                """,
                (
                    utcnow_iso(),
                    str(error_summary or "")[:1000],
                    int(snapshot_id),
                    self._resolve_org_id(org_id) or "default",
                ),
            )

    def get_snapshot(self, snapshot_id: int, *, org_id: str):
        return self._fetchone(
            "SELECT * FROM ad_directory_snapshots WHERE id = ? AND org_id = ?",
            (int(snapshot_id), self._resolve_org_id(org_id) or "default"),
        )

    def get_latest_successful_snapshot(self, *, org_id: str, connector_id: str):
        return self._fetchone(
            """
            SELECT * FROM ad_directory_snapshots
            WHERE org_id = ? AND connector_id = ? AND status = 'success'
            ORDER BY completed_at DESC, id DESC LIMIT 1
            """,
            (
                self._resolve_org_id(org_id) or "default",
                str(connector_id or "default").strip() or "default",
            ),
        )

    def list_ous(self, snapshot_id: int, *, org_id: str) -> list[dict[str, Any]]:
        rows = self._fetchall(
            """
            SELECT * FROM ad_ou_snapshots
            WHERE snapshot_id = ? AND org_id = ?
            ORDER BY path, distinguished_name
            """,
            (int(snapshot_id), self._resolve_org_id(org_id) or "default"),
        )
        return [dict(row) for row in rows]


class IdentityMatchRuleRepository(BaseRepository):
    def delete_rules_for_org(self, org_id: str) -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM identity_match_rules WHERE org_id = ?",
                (normalized_org_id,),
            )

    def seed_defaults(self, *, org_id: str, created_by: str = "system") -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            for spec in DEFAULT_IDENTITY_MATCH_RULES:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO identity_match_rules (
                      org_id, rule_order, rule_name, source_provider,
                      source_field, ad_field, is_required, case_sensitive,
                      trim_whitespace, strip_phone_country_code, lowercase_email,
                      allow_fallback, allow_auto_link, confidence_level,
                      confidence_score, stop_on_conflict, is_enabled,
                      rule_revision, created_by, created_at, updated_at
                    ) VALUES (?, ?, ?, '*', ?, ?, 0, 0, 1, ?, ?, 1, ?, ?, ?, 1, 1, 1, ?, ?, ?)
                    """,
                    (
                        normalized_org_id,
                        spec["rule_order"],
                        spec["rule_name"],
                        spec["source_field"],
                        spec["ad_field"],
                        int(bool(spec.get("strip_phone_country_code"))),
                        int(bool(spec.get("lowercase_email"))),
                        int(bool(spec.get("allow_auto_link"))),
                        spec["confidence_level"],
                        int(spec["confidence_score"]),
                        created_by,
                        now,
                        now,
                    ),
                )

    def list_enabled_rules(self, *, org_id: str) -> list[IdentityMatchRuleRecord]:
        rows = self._fetchall(
            """
            SELECT * FROM identity_match_rules
            WHERE org_id = ? AND is_enabled = 1
            ORDER BY rule_order, id
            """,
            (self._resolve_org_id(org_id) or "default",),
        )
        return [IdentityMatchRuleRecord.from_row(row) for row in rows]

    def list_rules(self, *, org_id: str) -> list[IdentityMatchRuleRecord]:
        rows = self._fetchall(
            """
            SELECT * FROM identity_match_rules
            WHERE org_id = ?
            ORDER BY rule_order, id
            """,
            (self._resolve_org_id(org_id) or "default",),
        )
        return [IdentityMatchRuleRecord.from_row(row) for row in rows]

    def upsert_rule(
        self,
        *,
        org_id: str,
        rule_order: int,
        rule_name: str,
        source_provider: str,
        source_field: str,
        ad_field: str,
        is_required: bool = False,
        case_sensitive: bool = False,
        trim_whitespace: bool = True,
        strip_phone_country_code: bool = False,
        lowercase_email: bool = False,
        allow_fallback: bool = True,
        allow_auto_link: bool = False,
        confidence_level: str = "medium",
        confidence_score: int = 50,
        stop_on_conflict: bool = True,
        is_enabled: bool = True,
        created_by: str = "",
    ) -> IdentityMatchRuleRecord:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_name = str(rule_name or "").strip()
        normalized_source_field = str(source_field or "").strip()
        normalized_ad_field = str(ad_field or "").strip()
        normalized_confidence = str(confidence_level or "medium").strip().lower()
        if not normalized_name or not normalized_source_field or not normalized_ad_field:
            raise ValueError("rule name, source field, and AD field are required")
        if not 1 <= int(rule_order) <= 10000:
            raise ValueError("rule_order must be between 1 and 10000")
        if not 0 <= int(confidence_score) <= 100:
            raise ValueError("confidence_score must be between 0 and 100")
        if normalized_confidence not in {"low", "medium", "high", "certain"}:
            raise ValueError("unsupported confidence level")
        if allow_auto_link and normalized_source_field not in {
            "employee_id",
            "employee_number",
        }:
            raise ValueError(
                "automatic linking is restricted to strong employee identifiers"
            )
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO identity_match_rules (
                  org_id, rule_order, rule_name, source_provider,
                  source_field, ad_field, is_required, case_sensitive,
                  trim_whitespace, strip_phone_country_code, lowercase_email,
                  allow_fallback, allow_auto_link, confidence_level,
                  confidence_score, stop_on_conflict, is_enabled,
                  rule_revision, created_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                ON CONFLICT(org_id, rule_name)
                DO UPDATE SET
                  rule_order = excluded.rule_order,
                  source_provider = excluded.source_provider,
                  source_field = excluded.source_field,
                  ad_field = excluded.ad_field,
                  is_required = excluded.is_required,
                  case_sensitive = excluded.case_sensitive,
                  trim_whitespace = excluded.trim_whitespace,
                  strip_phone_country_code = excluded.strip_phone_country_code,
                  lowercase_email = excluded.lowercase_email,
                  allow_fallback = excluded.allow_fallback,
                  allow_auto_link = excluded.allow_auto_link,
                  confidence_level = excluded.confidence_level,
                  confidence_score = excluded.confidence_score,
                  stop_on_conflict = excluded.stop_on_conflict,
                  is_enabled = excluded.is_enabled,
                  rule_revision = identity_match_rules.rule_revision + 1,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    int(rule_order),
                    normalized_name,
                    str(source_provider or "*").strip().lower() or "*",
                    normalized_source_field,
                    normalized_ad_field,
                    int(bool(is_required)),
                    int(bool(case_sensitive)),
                    int(bool(trim_whitespace)),
                    int(bool(strip_phone_country_code)),
                    int(bool(lowercase_email)),
                    int(bool(allow_fallback)),
                    int(bool(allow_auto_link)),
                    normalized_confidence,
                    int(confidence_score),
                    int(bool(stop_on_conflict)),
                    int(bool(is_enabled)),
                    str(created_by or ""),
                    now,
                    now,
                ),
            )
        row = self._fetchone(
            "SELECT * FROM identity_match_rules WHERE org_id = ? AND rule_name = ?",
            (normalized_org_id, normalized_name),
        )
        if not row:
            raise RuntimeError("identity match rule was not persisted")
        return IdentityMatchRuleRecord.from_row(row)


class FieldAuthorityRuleRepository(BaseRepository):
    def delete_rules_for_org(self, org_id: str) -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM field_authority_rules WHERE org_id = ?",
                (normalized_org_id,),
            )

    def seed_defaults(self, *, org_id: str, created_by: str = "system") -> None:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            for field_name, provider_id, priority, direction in DEFAULT_FIELD_AUTHORITY_RULES:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO field_authority_rules (
                      org_id, field_name, source_provider, source_priority,
                      sync_direction, sync_mode, prevent_loop, is_enabled,
                      rule_revision, notes, created_by, created_at, updated_at,
                      authority_mode, provider_priority_json, conflict_policy,
                      null_policy, manual_override_policy, effective_version
                    ) VALUES (
                      ?, ?, ?, ?, ?, 'replace', 1, 1, 1, ?, ?, ?, ?,
                      'PROVIDER_PRIORITY', ?, 'PROVIDER_PRIORITY',
                      'PRESERVE_TARGET', 'REQUIRE_REVIEW', 1
                    )
                    """,
                    (
                        normalized_org_id,
                        field_name,
                        provider_id,
                        int(priority),
                        direction,
                        "Recommended initial priority; review before production Apply",
                        created_by,
                        now,
                        now,
                        dumps_json([provider_id]) or "[]",
                    ),
                )

    def upsert_rule(
        self,
        *,
        org_id: str,
        field_name: str,
        source_provider: str,
        source_priority: int,
        sync_direction: str,
        sync_mode: str = "replace",
        prevent_loop: bool = True,
        is_enabled: bool = True,
        notes: str = "",
        created_by: str = "",
        authority_mode: str = "PROVIDER_PRIORITY",
        authoritative_connector_id: str = "",
        provider_priority: Iterable[str] = (),
        conflict_policy: str = "PROVIDER_PRIORITY",
        null_policy: str = "PRESERVE_TARGET",
        manual_override_policy: str = "REQUIRE_REVIEW",
    ) -> FieldAuthorityRuleRecord:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_field_name = str(field_name or "").strip()
        normalized_direction = str(sync_direction or "source_to_ad").strip().lower()
        normalized_sync_mode = str(sync_mode or "replace").strip().lower()
        if not normalized_field_name:
            raise ValueError("field_name is required")
        if normalized_direction not in FIELD_AUTHORITY_DIRECTIONS:
            raise ValueError("unsupported field authority sync direction")
        if normalized_sync_mode not in FIELD_AUTHORITY_SYNC_MODES:
            raise ValueError("unsupported field authority sync mode")
        if not 1 <= int(source_priority) <= 10000:
            raise ValueError("source_priority must be between 1 and 10000")
        normalized_provider = str(source_provider or "*").strip().lower() or "*"
        normalized_authority_mode = str(
            authority_mode or "PROVIDER_PRIORITY"
        ).strip().upper()
        normalized_conflict_policy = str(
            conflict_policy or "PROVIDER_PRIORITY"
        ).strip().upper()
        normalized_null_policy = str(null_policy or "PRESERVE_TARGET").strip().upper()
        normalized_override_policy = str(
            manual_override_policy or "REQUIRE_REVIEW"
        ).strip().upper()
        if normalized_authority_mode not in {
            "SINGLE_SOURCE",
            "PROVIDER_PRIORITY",
            "PRESERVE_AD",
            "MANUAL_ONLY",
            "FIRST_NON_EMPTY",
            "REJECT_ON_CONFLICT",
        }:
            raise ValueError("unsupported authority_mode")
        if normalized_conflict_policy not in {
            "REJECT_ON_CONFLICT",
            "PRESERVE_AD",
            "PROVIDER_PRIORITY",
            "MANUAL_REVIEW",
        }:
            raise ValueError("unsupported conflict_policy")
        if normalized_null_policy not in {
            "IGNORE",
            "PRESERVE_TARGET",
            "CLEAR",
            "USE_DEFAULT",
            "BLOCK",
        }:
            raise ValueError("unsupported null_policy")
        if normalized_override_policy not in {
            "ALLOW",
            "REQUIRE_REVIEW",
            "PRESERVE",
            "REJECT",
        }:
            raise ValueError("unsupported manual_override_policy")
        normalized_priority = [
            str(value).strip().lower()
            for value in provider_priority
            if str(value).strip()
        ] or [normalized_provider]
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO field_authority_rules (
                  org_id, field_name, source_provider, source_priority,
                  sync_direction, sync_mode, prevent_loop, is_enabled,
                  rule_revision, notes, created_by, created_at, updated_at,
                  authority_mode, authoritative_connector_id, provider_priority_json,
                  conflict_policy, null_policy, manual_override_policy, effective_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(org_id, field_name, source_provider)
                DO UPDATE SET
                  source_priority = excluded.source_priority,
                  sync_direction = excluded.sync_direction,
                  sync_mode = excluded.sync_mode,
                  prevent_loop = excluded.prevent_loop,
                  is_enabled = excluded.is_enabled,
                  rule_revision = field_authority_rules.rule_revision + 1,
                  authority_mode = excluded.authority_mode,
                  authoritative_connector_id = excluded.authoritative_connector_id,
                  provider_priority_json = excluded.provider_priority_json,
                  conflict_policy = excluded.conflict_policy,
                  null_policy = excluded.null_policy,
                  manual_override_policy = excluded.manual_override_policy,
                  effective_version = field_authority_rules.effective_version + 1,
                  notes = excluded.notes,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_field_name,
                    normalized_provider,
                    int(source_priority),
                    normalized_direction,
                    normalized_sync_mode,
                    int(bool(prevent_loop)),
                    int(bool(is_enabled)),
                    str(notes or ""),
                    str(created_by or ""),
                    now,
                    now,
                    normalized_authority_mode,
                    str(authoritative_connector_id or "").strip(),
                    dumps_json(normalized_priority) or "[]",
                    normalized_conflict_policy,
                    normalized_null_policy,
                    normalized_override_policy,
                ),
            )
        row = self._fetchone(
            """
            SELECT * FROM field_authority_rules
            WHERE org_id = ? AND field_name = ? AND source_provider = ?
            """,
            (
                normalized_org_id,
                normalized_field_name,
                normalized_provider,
            ),
        )
        if not row:
            raise RuntimeError("field authority rule was not persisted")
        return FieldAuthorityRuleRecord.from_row(row)

    def list_rules(self, *, org_id: str) -> list[FieldAuthorityRuleRecord]:
        rows = self._fetchall(
            """
            SELECT * FROM field_authority_rules
            WHERE org_id = ?
            ORDER BY field_name, source_priority, id
            """,
            (self._resolve_org_id(org_id) or "default",),
        )
        return [FieldAuthorityRuleRecord.from_row(row) for row in rows]

    def list_enabled_rules(self, *, org_id: str) -> list[FieldAuthorityRuleRecord]:
        rows = self._fetchall(
            """
            SELECT * FROM field_authority_rules
            WHERE org_id = ? AND is_enabled = 1
            ORDER BY field_name, source_priority, id
            """,
            (self._resolve_org_id(org_id) or "default",),
        )
        return [FieldAuthorityRuleRecord.from_row(row) for row in rows]


class IdentityMatchRunRepository(BaseRepository):
    def create_run(
        self,
        *,
        org_id: str,
        source_snapshot_ids: Iterable[int] = (),
        ad_snapshot_id: Optional[int] = None,
        rules_fingerprint: str = "",
        config_fingerprint: str = "",
        created_by: str = "",
        run_id: str = "",
    ) -> str:
        resolved_run_id = str(run_id or "").strip() or f"match_{uuid.uuid4().hex}"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO identity_match_runs (
                  run_id, org_id, source_snapshot_ids_json, ad_snapshot_id,
                  rules_fingerprint, config_fingerprint, status, summary_json,
                  created_by, started_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'running', '{}', ?, ?, '')
                """,
                (
                    resolved_run_id,
                    self._resolve_org_id(org_id) or "default",
                    dumps_json([int(value) for value in source_snapshot_ids]) or "[]",
                    ad_snapshot_id,
                    rules_fingerprint,
                    config_fingerprint,
                    created_by,
                    now,
                ),
            )
        return resolved_run_id

    def replace_candidates(
        self,
        run_id: str,
        *,
        org_id: str,
        candidates: Iterable[IdentityMatchCandidateRecord],
        summary: dict[str, Any],
    ) -> list[IdentityMatchCandidateRecord]:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        now = utcnow_iso()
        candidate_rows = list(candidates)
        with self.db.transaction() as conn:
            run = conn.execute(
                "SELECT run_id FROM identity_match_runs WHERE run_id = ? AND org_id = ?",
                (run_id, normalized_org_id),
            ).fetchone()
            if not run:
                raise ValueError("match run does not exist in this organization")
            conn.execute("DELETE FROM identity_match_candidates WHERE run_id = ?", (run_id,))
            for candidate in candidate_rows:
                conn.execute(
                    """
                    INSERT INTO identity_match_candidates (
                      run_id, org_id, platform_account_id, ad_account_id,
                      proposed_identity_id, result_level, confidence,
                      matched_rule_ids_json, matched_fields_json,
                      unmatched_fields_json, conflict_fields_json,
                      evidence_json, recommended_action, status,
                      candidate_fingerprint, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        normalized_org_id,
                        candidate.platform_account_id,
                        candidate.ad_account_id,
                        candidate.proposed_identity_id or None,
                        candidate.result_level,
                        candidate.confidence,
                        dumps_json(candidate.matched_rule_ids) or "[]",
                        dumps_json(candidate.matched_fields) or "{}",
                        dumps_json(candidate.unmatched_fields) or "{}",
                        dumps_json(candidate.conflict_fields) or "{}",
                        dumps_json(candidate.evidence) or "{}",
                        candidate.recommended_action,
                        candidate.status,
                        candidate.candidate_fingerprint,
                        now,
                        now,
                    ),
                )
            conn.execute(
                """
                UPDATE identity_match_runs
                SET status = 'completed', summary_json = ?, completed_at = ?
                WHERE run_id = ? AND org_id = ?
                """,
                (dumps_json(summary) or "{}", now, run_id, normalized_org_id),
            )
        rows = self._fetchall(
            "SELECT * FROM identity_match_candidates WHERE run_id = ? ORDER BY id",
            (run_id,),
        )
        return [IdentityMatchCandidateRecord.from_row(row) for row in rows]

    def get_latest_completed_run(self, *, org_id: str):
        return self._fetchone(
            """
            SELECT * FROM identity_match_runs
            WHERE org_id = ? AND status = 'completed'
            ORDER BY completed_at DESC, started_at DESC LIMIT 1
            """,
            (self._resolve_org_id(org_id) or "default",),
        )

    def list_candidates(
        self,
        *,
        org_id: str,
        run_id: str,
        result_level: str = "",
        status: str = "",
        limit: int = 200,
        offset: int = 0,
    ) -> list[IdentityMatchCandidateRecord]:
        clauses = ["org_id = ?", "run_id = ?"]
        params: list[Any] = [self._resolve_org_id(org_id) or "default", run_id]
        if result_level:
            clauses.append("result_level = ?")
            params.append(result_level)
        if status:
            clauses.append("status = ?")
            params.append(status)
        rows = self._fetchall(
            f"""
            SELECT * FROM identity_match_candidates
            WHERE {' AND '.join(clauses)}
            ORDER BY
              CASE result_level
                WHEN 'blocked' THEN 1
                WHEN 'manual_confirmation' THEN 2
                WHEN 'suggested_link' THEN 3
                ELSE 4
              END,
              id
            LIMIT ? OFFSET ?
            """,
            [*params, min(max(int(limit or 200), 1), 500), max(int(offset or 0), 0)],
        )
        return [IdentityMatchCandidateRecord.from_row(row) for row in rows]


class IdentityMatchDecisionRepository(BaseRepository):
    """Commit a reviewed match conclusion as one atomic, replay-safe unit of work."""

    DECISIONS = frozenset(
        {
            "link_once",
            "link_permanently",
            "create_new_ad_account",
            "not_same_person",
            "defer",
            "exclude",
        }
    )
    BLOCKED_DECISIONS = frozenset(
        {"link_once", "link_permanently", "create_new_ad_account"}
    )

    def get_candidate(
        self, candidate_id: int, *, org_id: str
    ) -> Optional[IdentityMatchCandidateRecord]:
        row = self._fetchone(
            "SELECT * FROM identity_match_candidates WHERE org_id = ? AND id = ?",
            (self._resolve_org_id(org_id) or "default", int(candidate_id)),
        )
        return IdentityMatchCandidateRecord.from_row(row) if row else None

    def get_decision_by_request_id(
        self, request_id: str, *, org_id: str
    ) -> Optional[IdentityMatchDecisionRecord]:
        normalized_request_id = str(request_id or "").strip()
        if not normalized_request_id:
            return None
        row = self._fetchone(
            "SELECT * FROM identity_match_decisions WHERE org_id = ? AND request_id = ?",
            (self._resolve_org_id(org_id) or "default", normalized_request_id),
        )
        return IdentityMatchDecisionRecord.from_row(row) if row else None

    def list_decisions(
        self, *, org_id: str, candidate_id: Optional[int] = None, limit: int = 200
    ) -> list[IdentityMatchDecisionRecord]:
        clauses = ["org_id = ?"]
        params: list[Any] = [self._resolve_org_id(org_id) or "default"]
        if candidate_id is not None:
            clauses.append("candidate_id = ?")
            params.append(int(candidate_id))
        rows = self._fetchall(
            f"""
            SELECT * FROM identity_match_decisions
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at DESC, id DESC LIMIT ?
            """,
            [*params, min(max(int(limit or 200), 1), 500)],
        )
        return [IdentityMatchDecisionRecord.from_row(row) for row in rows]

    def apply_decision(
        self,
        *,
        org_id: str,
        candidate_id: int,
        expected_fingerprint: str,
        decision: str,
        request_id: str,
        decided_by: str,
        reason: str = "",
        decision_payload: Optional[dict[str, Any]] = None,
    ) -> IdentityMatchDecisionRecord:
        normalized_org_id = self._resolve_org_id(org_id) or "default"
        normalized_decision = str(decision or "").strip().lower()
        normalized_request_id = str(request_id or "").strip()
        fingerprint = str(expected_fingerprint or "").strip()
        actor = str(decided_by or "").strip()
        if normalized_decision not in self.DECISIONS:
            raise ValueError("unsupported identity match decision")
        if not normalized_request_id:
            raise ValueError("request_id is required for idempotent decision processing")
        if not fingerprint:
            raise ValueError("candidate fingerprint is required")
        if not actor:
            raise ValueError("decision actor is required")

        existing = self.get_decision_by_request_id(
            normalized_request_id, org_id=normalized_org_id
        )
        if existing is not None:
            if (
                existing.candidate_id != int(candidate_id)
                or existing.decision != normalized_decision
                or existing.candidate_fingerprint != fingerprint
            ):
                raise ValueError("idempotency key was already used for another decision")
            return existing

        now = utcnow_iso()
        decision_id = 0
        with self.db.transaction() as conn:
            candidate = conn.execute(
                "SELECT * FROM identity_match_candidates WHERE org_id = ? AND id = ?",
                (normalized_org_id, int(candidate_id)),
            ).fetchone()
            if not candidate:
                raise ValueError("match candidate does not exist in this organization")
            if str(candidate["candidate_fingerprint"] or "") != fingerprint:
                raise ValueError("match candidate evidence changed; run matching again")
            if str(candidate["status"] or "") not in {"pending", "deferred"}:
                raise ValueError("match candidate was already decided")
            latest_run = conn.execute(
                """
                SELECT run_id FROM identity_match_runs
                WHERE org_id = ? AND status = 'completed'
                ORDER BY completed_at DESC, started_at DESC LIMIT 1
                """,
                (normalized_org_id,),
            ).fetchone()
            if latest_run and str(latest_run["run_id"] or "") != str(candidate["run_id"]):
                raise ValueError("a newer match run superseded this candidate")
            if (
                str(candidate["result_level"] or "") == "blocked"
                and normalized_decision in self.BLOCKED_DECISIONS
            ):
                raise ValueError("blocked candidates cannot create or link identities")

            platform = conn.execute(
                "SELECT * FROM platform_accounts WHERE org_id = ? AND id = ?",
                (normalized_org_id, int(candidate["platform_account_id"])),
            ).fetchone()
            if not platform:
                raise ValueError("platform account is no longer available")
            if bool(platform["is_excluded"]) and normalized_decision != "exclude":
                raise ValueError("excluded platform accounts cannot be linked")
            if (
                str(platform["account_type"] or "person") in {"service", "shared", "test"}
                and normalized_decision in self.BLOCKED_DECISIONS
            ):
                raise ValueError("non-person platform accounts require explicit maintenance")

            ad_account = None
            if candidate["ad_account_id"] is not None:
                ad_account = conn.execute(
                    "SELECT * FROM ad_accounts WHERE org_id = ? AND id = ?",
                    (normalized_org_id, int(candidate["ad_account_id"])),
                ).fetchone()
            if normalized_decision in {"link_once", "link_permanently"}:
                if not ad_account:
                    raise ValueError("link decisions require an available AD account")
                if bool(ad_account["is_protected"]) or str(
                    ad_account["account_type"] or "person"
                ) in {"service", "shared", "test"}:
                    raise ValueError("protected or non-person AD accounts cannot be linked")
            if normalized_decision == "create_new_ad_account" and ad_account:
                raise ValueError("create-new decision is valid only without an AD candidate")
            legacy_binding = None
            if normalized_decision == "link_permanently":
                legacy_binding = conn.execute(
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
                if legacy_binding and str(
                    legacy_binding["ad_username"] or ""
                ).casefold() != str(ad_account["sam_account_name"] or "").casefold():
                    raise ValueError("permanent decision would overwrite an active legacy binding")

            resulting_identity_id = ""
            resulting_link_ids: list[int] = []
            if normalized_decision in {"link_permanently", "create_new_ad_account"}:
                platform_link = conn.execute(
                    """
                    SELECT * FROM identity_account_links
                    WHERE org_id = ? AND account_kind = 'platform'
                      AND platform_account_id = ? AND status = 'active'
                    """,
                    (normalized_org_id, int(candidate["platform_account_id"])),
                ).fetchone()
                ad_link = None
                if ad_account:
                    ad_link = conn.execute(
                        """
                        SELECT * FROM identity_account_links
                        WHERE org_id = ? AND account_kind = 'ad'
                          AND ad_account_id = ? AND status = 'active'
                        """,
                        (normalized_org_id, int(candidate["ad_account_id"])),
                    ).fetchone()
                identity_candidates = {
                    str(value)
                    for value in (
                        candidate["proposed_identity_id"],
                        platform_link["identity_id"] if platform_link else "",
                        ad_link["identity_id"] if ad_link else "",
                    )
                    if str(value or "").strip()
                }
                if len(identity_candidates) > 1:
                    raise ValueError("candidate accounts are owned by different identities")
                resulting_identity_id = (
                    next(iter(identity_candidates))
                    if identity_candidates
                    else f"eid_{uuid.uuid4().hex}"
                )
                identity = conn.execute(
                    "SELECT * FROM enterprise_identities WHERE org_id = ? AND identity_id = ? AND status = 'active'",
                    (normalized_org_id, resulting_identity_id),
                ).fetchone()
                if not identity:
                    canonical_fields = {
                        "display_name": str(platform["display_name"] or ""),
                        "employee_id": str(platform["employee_id"] or ""),
                        "email": str(platform["email"] or ""),
                        "mobile": str(platform["mobile"] or ""),
                    }
                    field_sources = {
                        key: {
                            "provider_id": str(platform["provider_id"] or ""),
                            "platform_account_id": str(platform["platform_account_id"] or ""),
                        }
                        for key, value in canonical_fields.items()
                        if value
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
                            resulting_identity_id,
                            normalized_org_id,
                            str(platform["display_name"] or ""),
                            str(platform["employee_id"] or ""),
                            str(platform["account_status"] or "active"),
                            str(platform["primary_department_id"] or ""),
                            dumps_json(canonical_fields) or "{}",
                            dumps_json(field_sources) or "{}",
                            actor,
                            now,
                            now,
                        ),
                    )

                evidence = {
                    "candidate_id": int(candidate_id),
                    "candidate_fingerprint": fingerprint,
                    "run_id": str(candidate["run_id"] or ""),
                    "decision": normalized_decision,
                }
                if platform_link:
                    if str(platform_link["identity_id"]) != resulting_identity_id:
                        raise ValueError("platform account is already owned by another identity")
                    resulting_link_ids.append(int(platform_link["id"]))
                else:
                    cursor = conn.execute(
                        """
                        INSERT INTO identity_account_links (
                          org_id, identity_id, account_kind, platform_account_id,
                          ad_account_id, account_role, account_purpose,
                          association_type, status, source, evidence_json, confidence,
                          created_by, valid_until, link_revision, created_at, updated_at
                        ) VALUES (?, ?, 'platform', ?, NULL, 'source', '',
                                  'manual_permanent', 'active', 'identity_match_decision',
                                  ?, ?, ?, '', 1, ?, ?)
                        """,
                        (
                            normalized_org_id,
                            resulting_identity_id,
                            int(candidate["platform_account_id"]),
                            dumps_json(evidence) or "{}",
                            int(candidate["confidence"] or 0),
                            actor,
                            now,
                            now,
                        ),
                    )
                    resulting_link_ids.append(int(cursor.lastrowid))
                if normalized_decision == "link_permanently":
                    if ad_link:
                        if str(ad_link["identity_id"]) != resulting_identity_id:
                            raise ValueError("AD account is already owned by another identity")
                        resulting_link_ids.append(int(ad_link["id"]))
                    else:
                        cursor = conn.execute(
                            """
                            INSERT INTO identity_account_links (
                              org_id, identity_id, account_kind, platform_account_id,
                              ad_account_id, account_role, account_purpose,
                              association_type, status, source, evidence_json, confidence,
                              created_by, valid_until, link_revision, created_at, updated_at
                            ) VALUES (?, ?, 'ad', NULL, ?, 'primary_ad', '',
                                      'manual_permanent', 'active', 'identity_match_decision',
                                      ?, ?, ?, '', 1, ?, ?)
                            """,
                            (
                                normalized_org_id,
                                resulting_identity_id,
                                int(candidate["ad_account_id"]),
                                dumps_json(evidence) or "{}",
                                int(candidate["confidence"] or 0),
                                actor,
                                now,
                                now,
                            ),
                        )
                        resulting_link_ids.append(int(cursor.lastrowid))
                    conn.execute(
                        """
                        INSERT INTO user_identity_bindings (
                          org_id, source_provider, source_user_id,
                          source_display_name, connector_id, ad_username,
                          target_object_guid, target_object_dn,
                          managed_username_base, source, notes, is_enabled,
                          updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', 'manual', ?, 1, ?)
                        ON CONFLICT(org_id, source_provider, connector_id, source_user_id)
                        DO UPDATE SET source_display_name = excluded.source_display_name,
                          ad_username = excluded.ad_username,
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
                            f"Identity decision candidate {int(candidate_id)}",
                            now,
                        ),
                    )

            if normalized_decision == "exclude":
                conn.execute(
                    "UPDATE platform_accounts SET is_excluded = 1, updated_at = ? WHERE org_id = ? AND id = ?",
                    (now, normalized_org_id, int(candidate["platform_account_id"])),
                )

            status_by_decision = {
                "link_once": "linked_once",
                "link_permanently": "permanently_linked",
                "create_new_ad_account": "create_requested",
                "not_same_person": "rejected",
                "defer": "deferred",
                "exclude": "excluded",
            }
            cursor = conn.execute(
                """
                INSERT INTO identity_match_decisions (
                  org_id, candidate_id, decision, reason, candidate_fingerprint,
                  request_id, decision_payload_json, resulting_identity_id,
                  resulting_link_ids_json, decided_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_org_id,
                    int(candidate_id),
                    normalized_decision,
                    str(reason or "").strip(),
                    fingerprint,
                    normalized_request_id,
                    dumps_json(decision_payload or {}) or "{}",
                    resulting_identity_id or None,
                    dumps_json(resulting_link_ids) or "[]",
                    actor,
                    now,
                ),
            )
            decision_id = int(cursor.lastrowid)
            updated = conn.execute(
                """
                UPDATE identity_match_candidates SET status = ?, updated_at = ?
                WHERE org_id = ? AND id = ? AND candidate_fingerprint = ?
                  AND status IN ('pending', 'deferred')
                """,
                (
                    status_by_decision[normalized_decision],
                    now,
                    normalized_org_id,
                    int(candidate_id),
                    fingerprint,
                ),
            )
            if updated.rowcount != 1:
                raise ValueError("match candidate changed while the decision was being saved")

        row = self._fetchone(
            "SELECT * FROM identity_match_decisions WHERE id = ? AND org_id = ?",
            (decision_id, normalized_org_id),
        )
        if not row:
            raise RuntimeError("identity match decision was not persisted")
        return IdentityMatchDecisionRecord.from_row(row)


__all__ = [
    "DEFAULT_IDENTITY_MATCH_RULES",
    "EnterpriseIdentityRepository",
    "PlatformAccountRepository",
    "ADAccountRepository",
    "ADDirectorySnapshotRepository",
    "IdentityMatchRuleRepository",
    "IdentityMatchRunRepository",
    "IdentityMatchDecisionRepository",
    "FieldAuthorityRuleRepository",
]
