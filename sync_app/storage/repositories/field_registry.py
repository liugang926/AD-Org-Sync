from __future__ import annotations

import json
import re
from typing import Any, Iterable, Optional

from sync_app.core.models import (
    ADTargetAttributeRecord,
    CanonicalFieldRecord,
    SourceFieldRecord,
)
from sync_app.storage.local_db import BaseRepository, utcnow_iso


SECRET_FIELD_MARKERS = (
    "password",
    "secret",
    "credential",
    "token",
    "api_key",
    "access_token",
    "refresh_token",
    "private_key",
    "unicodepwd",
    "ldap_password",
    "corpsecret",
    "webhook",
)
SENSITIVE_FIELD_MARKERS = (
    "email",
    "mobile",
    "phone",
    "telephone",
    "address",
    "street",
    "identity_card",
    "id_card",
    "passport",
)
IDENTIFIER_FIELDS = {
    "platform_account_id",
    "platform_union_id",
    "platform_open_id",
    "source_user_id",
    "employee_id",
    "employee_number",
    "personnel_number",
}

CANONICAL_FIELD_GROUPS: dict[str, tuple[str, ...]] = {
    "technical_identity": (
        "platform_account_id", "platform_union_id", "platform_open_id", "source_user_id",
    ),
    "person_identifier": ("employee_id", "employee_number", "personnel_number"),
    "name": ("display_name", "given_name", "family_name", "english_name", "preferred_name"),
    "contact": ("email", "enterprise_email", "alternate_email", "mobile", "telephone"),
    "employment": (
        "job_title", "position", "job_level", "job_family", "employee_type",
        "employment_category",
    ),
    "organization": (
        "department_ids", "primary_department_id", "department_name", "department_path",
        "company", "legal_entity", "cost_center", "business_unit",
    ),
    "relationship": ("manager_account_id", "manager_employee_id", "manager_source_user_id"),
    "location": (
        "office", "work_station", "address", "street", "city", "province", "state",
        "country", "postal_code", "timezone",
    ),
    "lifecycle": (
        "hire_date", "probation_end_date", "leave_date", "employment_status",
        "account_status", "is_active",
    ),
    "personal": ("gender", "locale", "preferred_language"),
}
MULTI_VALUE_CANONICAL_FIELDS = {"department_ids"}
RELATIONSHIP_FIELDS = {
    "manager_account_id",
    "manager_employee_id",
    "manager_source_user_id",
}
LIFECYCLE_FIELDS = {
    "hire_date",
    "probation_end_date",
    "leave_date",
    "employment_status",
    "account_status",
    "is_active",
}

CANONICAL_ALIASES = {
    "userid": "source_user_id",
    "user_id": "source_user_id",
    "staffid": "platform_account_id",
    "staff_id": "platform_account_id",
    "unionid": "platform_union_id",
    "union_id": "platform_union_id",
    "openid": "platform_open_id",
    "open_id": "platform_open_id",
    "job_number": "employee_id",
    "jobnumber": "employee_id",
    "employee_no": "employee_id",
    "employeeno": "employee_id",
    "employeeid": "employee_id",
    "name": "display_name",
    "org_email": "enterprise_email",
    "biz_mail": "enterprise_email",
    "work_email": "enterprise_email",
    "phone": "mobile",
    "title": "job_title",
    "department": "department_ids",
    "departments": "department_ids",
    "leader_user_id": "manager_source_user_id",
    "manager_userid": "manager_source_user_id",
}


def is_secret_field(field_path: str) -> bool:
    normalized = str(field_path or "").strip().lower()
    return any(marker in normalized for marker in SECRET_FIELD_MARKERS)


def is_sensitive_field(field_path: str) -> bool:
    normalized = str(field_path or "").strip().lower()
    return any(marker in normalized for marker in SENSITIVE_FIELD_MARKERS)


def canonical_field_for_path(field_path: str) -> str:
    normalized = str(field_path or "").strip().lower()
    leaf = normalized.rsplit(".", 1)[-1]
    leaf = re.sub(r"[^a-z0-9_]+", "_", leaf).strip("_")
    if leaf in {field for fields in CANONICAL_FIELD_GROUPS.values() for field in fields}:
        return leaf
    return CANONICAL_ALIASES.get(leaf, "")


def category_for_canonical_field(field_key: str) -> str:
    normalized = str(field_key or "").strip().lower()
    for category, fields in CANONICAL_FIELD_GROUPS.items():
        if normalized in fields:
            return category
    return "custom"


def _masked_samples(field_path: str, values: Iterable[Any]) -> list[str]:
    if is_secret_field(field_path):
        return []
    result: list[str] = []
    sensitive = is_sensitive_field(field_path)
    for value in values:
        text = str(value or "").strip()[:80]
        if not text:
            continue
        if sensitive:
            if "@" in text:
                local, domain = text.split("@", 1)
                text = f"{local[:1]}***@{domain}"
            elif len(text) > 6:
                text = f"{text[:2]}***{text[-2:]}"
            else:
                text = "*" * len(text)
        elif len(text) > 2:
            text = f"{text[:2]}***{text[-2:]}" if len(text) > 6 else f"{text[:1]}***{text[-1:]}"
        else:
            text = "*" * len(text)
        if text not in result:
            result.append(text)
        if len(result) >= 3:
            break
    return result


def _allowed_roles(field_key: str) -> list[str]:
    if field_key in IDENTIFIER_FIELDS:
        return ["PRIMARY_ASSOCIATION", "SUGGESTION", "READ_ONLY_REFERENCE"]
    if field_key in RELATIONSHIP_FIELDS:
        return ["RELATIONSHIP", "READ_ONLY_REFERENCE"]
    if field_key in LIFECYCLE_FIELDS:
        return ["LIFECYCLE", "READ_ONLY_REFERENCE"]
    if field_key in {"department_ids", "primary_department_id", "department_path"}:
        return ["DIRECTORY_ROUTING", "ATTRIBUTE_SYNC", "READ_ONLY_REFERENCE"]
    if field_key.startswith("platform_") or field_key == "source_user_id":
        return ["READ_ONLY_REFERENCE", "SUGGESTION"]
    return ["ATTRIBUTE_SYNC", "SUGGESTION", "READ_ONLY_REFERENCE"]


class CanonicalFieldRegistryRepository(BaseRepository):
    def seed_defaults(self, *, connection: Any = None) -> int:
        now = utcnow_iso()
        inserted = 0
        with self._write_connection(connection) as conn:
            for category, fields in CANONICAL_FIELD_GROUPS.items():
                for field_key in fields:
                    cursor = conn.execute(
                        """
                        INSERT OR IGNORE INTO canonical_field_registry (
                          org_id, canonical_field_key, display_label, category, data_type,
                          is_multi_value, is_sensitive, is_identifier, is_custom, is_derived,
                          allowed_mapping_roles_json, description, schema_version, is_active,
                          created_at, updated_at
                        ) VALUES ('*', ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, 1, 1, ?, ?)
                        """,
                        (
                            field_key,
                            field_key.replace("_", " ").title(),
                            category,
                            "boolean" if field_key == "is_active" else "date" if field_key.endswith("_date") else "string",
                            1 if field_key in MULTI_VALUE_CANONICAL_FIELDS else 0,
                            1 if is_sensitive_field(field_key) else 0,
                            1 if field_key in IDENTIFIER_FIELDS else 0,
                            json.dumps(_allowed_roles(field_key)),
                            f"Provider-independent canonical field: {field_key}",
                            now,
                            now,
                        ),
                    )
                    inserted += int(cursor.rowcount or 0)
        return inserted

    def list_fields(self, *, org_id: Optional[str] = None, active_only: bool = True) -> list[CanonicalFieldRecord]:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        clauses = ["org_id IN ('*', ?)"]
        params: list[Any] = [normalized_org_id]
        if active_only:
            clauses.append("is_active = 1")
        rows = self._fetchall(
            f"""
            SELECT * FROM canonical_field_registry
            WHERE {' AND '.join(clauses)}
            ORDER BY CASE WHEN org_id = '*' THEN 0 ELSE 1 END, category, canonical_field_key
            """,
            params,
        )
        by_key: dict[str, CanonicalFieldRecord] = {}
        for row in rows:
            record = CanonicalFieldRecord.from_row(row)
            by_key[record.canonical_field_key] = record
        return list(by_key.values())

    def register_custom_field(
        self,
        *,
        org_id: str,
        canonical_field_key: str,
        display_label: str,
        data_type: str = "string",
        is_multi_value: bool = False,
        is_sensitive: bool = False,
        description: str = "",
    ) -> CanonicalFieldRecord:
        field_key = str(canonical_field_key or "").strip().lower()
        if not re.fullmatch(r"custom\.[a-z0-9_]+\.[a-z0-9_]+", field_key):
            raise ValueError("custom canonical fields must use custom.<namespace>.<field_key>")
        if is_secret_field(field_key):
            raise ValueError("credential and secret fields cannot be registered")
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO canonical_field_registry (
                  org_id, canonical_field_key, display_label, category, data_type,
                  is_multi_value, is_sensitive, is_identifier, is_custom, is_derived,
                  allowed_mapping_roles_json, description, schema_version, is_active,
                  created_at, updated_at
                ) VALUES (?, ?, ?, 'custom', ?, ?, ?, 0, 1, 0, ?, ?, 1, 1, ?, ?)
                ON CONFLICT(org_id, canonical_field_key) DO UPDATE SET
                  display_label = excluded.display_label,
                  data_type = excluded.data_type,
                  is_multi_value = excluded.is_multi_value,
                  is_sensitive = excluded.is_sensitive,
                  description = excluded.description,
                  schema_version = canonical_field_registry.schema_version + 1,
                  is_active = 1,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    field_key,
                    str(display_label or field_key).strip(),
                    str(data_type or "string").strip().lower(),
                    1 if is_multi_value else 0,
                    1 if is_sensitive else 0,
                    json.dumps(_allowed_roles(field_key)),
                    str(description or "").strip(),
                    now,
                    now,
                ),
            )
        row = self._fetchone(
            "SELECT * FROM canonical_field_registry WHERE org_id = ? AND canonical_field_key = ?",
            (normalized_org_id, field_key),
        )
        if not row:
            raise RuntimeError("custom canonical field was not persisted")
        return CanonicalFieldRecord.from_row(row)


class SourceFieldRegistryRepository(BaseRepository):
    def sync_snapshot_catalog(
        self,
        *,
        org_id: str,
        provider_id: str,
        source_connector_id: str,
        snapshot_id: int,
        user_count: int,
        fields: Iterable[dict[str, Any]],
        connection: Any = None,
    ) -> int:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        normalized_provider = str(provider_id or "").strip().lower()
        normalized_connector = str(source_connector_id or "default").strip() or "default"
        now = utcnow_iso()
        field_rows = [dict(item) for item in fields]
        seen_paths: set[str] = set()
        with self._write_connection(connection) as conn:
            conn.execute(
                """
                UPDATE source_field_registry
                SET availability_status = 'not_returned', permission_status = 'unknown',
                    coverage_count = 0, coverage_rate = 0
                WHERE org_id = ? AND provider_id = ? AND source_connector_id = ?
                """,
                (normalized_org_id, normalized_provider, normalized_connector),
            )
            for item in field_rows:
                field_path = str(item.get("raw_field_path") or item.get("name") or "").strip()
                if not field_path or is_secret_field(field_path):
                    continue
                seen_paths.add(field_path)
                existing = conn.execute(
                    """
                    SELECT data_type, is_multi_value, canonical_field_key, schema_version
                    FROM source_field_registry
                    WHERE org_id = ? AND provider_id = ? AND source_connector_id = ?
                      AND raw_field_path = ?
                    """,
                    (normalized_org_id, normalized_provider, normalized_connector, field_path),
                ).fetchone()
                data_type = str(item.get("data_type") or "string").strip().lower()
                is_multi = bool(item.get("is_multi_value", data_type in {"array", "list"}))
                canonical_key = str(item.get("canonical_field_key") or canonical_field_for_path(field_path)).strip()
                availability = str(item.get("availability_status") or "available").strip().lower()
                if availability not in {
                    "available", "unavailable_by_permission", "not_returned", "deprecated",
                    "type_conflict", "unknown",
                }:
                    availability = "unknown"
                schema_version = 1
                if existing:
                    changed = (
                        str(existing["data_type"] or "string") != data_type
                        or bool(existing["is_multi_value"]) != is_multi
                        or str(existing["canonical_field_key"] or "") != canonical_key
                    )
                    schema_version = max(int(existing["schema_version"] or 1), 1) + (1 if changed else 0)
                    if str(existing["data_type"] or "string") != data_type or bool(existing["is_multi_value"]) != is_multi:
                        availability = "type_conflict"
                coverage_count = max(int(item.get("coverage_count", item.get("coverage", 0)) or 0), 0)
                coverage_rate = (
                    round(coverage_count / max(int(user_count or 0), 1), 6)
                    if user_count
                    else 0.0
                )
                samples = _masked_samples(field_path, item.get("masked_sample_values") or item.get("samples") or [])
                category = str(item.get("category") or category_for_canonical_field(canonical_key)).strip().lower()
                permission_status = str(item.get("permission_status") or "granted").strip().lower()
                conn.execute(
                    """
                    INSERT INTO source_field_registry (
                      org_id, provider_id, source_connector_id, raw_field_path, raw_field_name,
                      canonical_field_key, display_label, category, data_type, is_multi_value,
                      is_sensitive, is_identifier_candidate, is_custom, is_derived,
                      availability_status, permission_status, coverage_count, coverage_rate,
                      masked_sample_values_json, first_detected_at, last_detected_at,
                      schema_version, latest_snapshot_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(org_id, provider_id, source_connector_id, raw_field_path) DO UPDATE SET
                      raw_field_name = excluded.raw_field_name,
                      canonical_field_key = excluded.canonical_field_key,
                      display_label = excluded.display_label,
                      category = excluded.category,
                      data_type = excluded.data_type,
                      is_multi_value = excluded.is_multi_value,
                      is_sensitive = excluded.is_sensitive,
                      is_identifier_candidate = excluded.is_identifier_candidate,
                      is_custom = excluded.is_custom,
                      is_derived = excluded.is_derived,
                      availability_status = excluded.availability_status,
                      permission_status = excluded.permission_status,
                      coverage_count = excluded.coverage_count,
                      coverage_rate = excluded.coverage_rate,
                      masked_sample_values_json = excluded.masked_sample_values_json,
                      last_detected_at = excluded.last_detected_at,
                      schema_version = excluded.schema_version,
                      latest_snapshot_id = excluded.latest_snapshot_id
                    """,
                    (
                        normalized_org_id,
                        normalized_provider,
                        normalized_connector,
                        field_path,
                        str(item.get("raw_field_name") or field_path.rsplit(".", 1)[-1]),
                        canonical_key,
                        str(item.get("display_label") or item.get("label") or field_path),
                        category,
                        data_type,
                        1 if is_multi else 0,
                        1 if bool(item.get("is_sensitive", is_sensitive_field(field_path))) else 0,
                        1 if bool(item.get("is_identifier_candidate", canonical_key in IDENTIFIER_FIELDS)) else 0,
                        1 if bool(item.get("is_custom", not canonical_key)) else 0,
                        1 if bool(item.get("is_derived", False)) else 0,
                        availability,
                        permission_status,
                        coverage_count,
                        coverage_rate,
                        json.dumps(samples, ensure_ascii=False),
                        now,
                        now,
                        schema_version,
                        int(snapshot_id),
                    ),
                )
        return len(seen_paths)

    def list_fields(
        self,
        *,
        org_id: str,
        provider_id: str = "",
        source_connector_id: str = "",
        current_snapshot_id: Optional[int] = None,
    ) -> list[SourceFieldRecord]:
        clauses = ["org_id = ?"]
        params: list[Any] = [self._resolve_org_id(org_id, default="default") or "default"]
        if provider_id:
            clauses.append("provider_id = ?")
            params.append(str(provider_id).strip().lower())
        if source_connector_id:
            clauses.append("source_connector_id = ?")
            params.append(str(source_connector_id).strip())
        if current_snapshot_id is not None:
            clauses.append("latest_snapshot_id = ?")
            params.append(int(current_snapshot_id))
        rows = self._fetchall(
            f"""
            SELECT * FROM source_field_registry
            WHERE {' AND '.join(clauses)}
            ORDER BY provider_id, source_connector_id, category, raw_field_path
            """,
            params,
        )
        return [SourceFieldRecord.from_row(row) for row in rows]


AD_ATTRIBUTE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "sAMAccountName": {"category": "account", "identifier": True},
    "userPrincipalName": {"category": "account"},
    "employeeID": {"category": "person_identifier"},
    "employeeNumber": {"category": "person_identifier"},
    "displayName": {"category": "name"},
    "givenName": {"category": "name"},
    "sn": {"category": "name"},
    "cn": {"category": "name", "special": "rename_object"},
    "mail": {"category": "contact"},
    "proxyAddresses": {"category": "contact", "multi": True, "special": "proxy_addresses"},
    "mailNickname": {"category": "contact"},
    "mobile": {"category": "contact"},
    "telephoneNumber": {"category": "contact"},
    "title": {"category": "employment"},
    "department": {"category": "organization"},
    "company": {"category": "organization"},
    "manager": {"category": "relationship", "special": "manager_dn"},
    "physicalDeliveryOfficeName": {"category": "location"},
    "streetAddress": {"category": "location"},
    "l": {"category": "location"},
    "st": {"category": "location"},
    "postalCode": {"category": "location"},
    "c": {"category": "location"},
    "description": {"category": "description"},
    "objectGUID": {"category": "technical_identity", "readonly": True},
    "objectSid": {"category": "technical_identity", "readonly": True},
    "distinguishedName": {"category": "technical_identity", "readonly": True},
    "whenCreated": {"category": "technical_identity", "readonly": True, "type": "datetime"},
    "whenChanged": {"category": "technical_identity", "readonly": True, "type": "datetime"},
    "nTSecurityDescriptor": {"category": "security", "readonly": True},
}
for _index in range(1, 16):
    AD_ATTRIBUTE_DEFINITIONS[f"extensionAttribute{_index}"] = {"category": "custom"}


class ADTargetAttributeRegistryRepository(BaseRepository):
    def sync_snapshot_catalog(
        self,
        *,
        org_id: str,
        ad_connector_id: str,
        snapshot_id: int,
        capability_report: dict[str, Any],
        discovered_attributes: Iterable[str] = (),
        connection: Any = None,
    ) -> int:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        normalized_connector = str(ad_connector_id or "default").strip() or "default"
        report = dict(capability_report or {})
        capabilities = dict(report.get("capabilities") or report)
        update_capability = dict(capabilities.get("update_user") or {})
        update_status = str(update_capability.get("status") or "not_tested").strip().lower()
        update_verified = bool(update_capability.get("verified", False))
        writable_verified = update_status == "success" and update_verified
        reported_schema = report.get("schema_attributes") or report.get("attributes") or []
        detected = {str(value).strip().lower() for value in [*discovered_attributes, *reported_schema] if str(value).strip()}
        definitions = dict(AD_ATTRIBUTE_DEFINITIONS)
        for attribute in sorted(detected):
            if attribute and attribute not in {key.lower() for key in definitions} and not is_secret_field(attribute):
                definitions[attribute] = {"category": "custom"}
        now = utcnow_iso()
        with self._write_connection(connection) as conn:
            for attribute, definition in definitions.items():
                lowered = attribute.lower()
                read_only = bool(definition.get("readonly", False))
                schema_detected = bool(
                    lowered in detected
                    or (
                        not detected
                        and not attribute.startswith("extensionAttribute")
                        and definition.get("category") != "custom"
                    )
                )
                is_writable = bool(writable_verified and schema_detected and not read_only)
                if read_only:
                    capability_status = "read_only"
                elif not schema_detected:
                    capability_status = "not_detected"
                elif not writable_verified:
                    capability_status = "unavailable_by_permission"
                else:
                    capability_status = "available"
                conn.execute(
                    """
                    INSERT INTO ad_target_attribute_registry (
                      org_id, ad_connector_id, ldap_attribute, display_label, category,
                      data_type, is_multi_value, is_writable, is_read_only,
                      requires_special_handler, special_handler_type,
                      required_permissions_json, supported_object_classes_json,
                      schema_detected, capability_status, validation_rules_json,
                      schema_version, latest_snapshot_id, last_checked_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                    ON CONFLICT(org_id, ad_connector_id, ldap_attribute) DO UPDATE SET
                      display_label = excluded.display_label,
                      category = excluded.category,
                      data_type = excluded.data_type,
                      is_multi_value = excluded.is_multi_value,
                      is_writable = excluded.is_writable,
                      is_read_only = excluded.is_read_only,
                      requires_special_handler = excluded.requires_special_handler,
                      special_handler_type = excluded.special_handler_type,
                      required_permissions_json = excluded.required_permissions_json,
                      supported_object_classes_json = excluded.supported_object_classes_json,
                      schema_detected = excluded.schema_detected,
                      capability_status = excluded.capability_status,
                      validation_rules_json = excluded.validation_rules_json,
                      schema_version = CASE
                        WHEN ad_target_attribute_registry.schema_detected <> excluded.schema_detected
                          OR ad_target_attribute_registry.is_writable <> excluded.is_writable
                          OR ad_target_attribute_registry.data_type <> excluded.data_type
                          OR ad_target_attribute_registry.is_multi_value <> excluded.is_multi_value
                        THEN ad_target_attribute_registry.schema_version + 1
                        ELSE ad_target_attribute_registry.schema_version
                      END,
                      latest_snapshot_id = excluded.latest_snapshot_id,
                      last_checked_at = excluded.last_checked_at
                    """,
                    (
                        normalized_org_id,
                        normalized_connector,
                        attribute,
                        re.sub(r"(?<!^)(?=[A-Z])", " ", attribute).title(),
                        str(definition.get("category") or "custom"),
                        str(definition.get("type") or "string"),
                        1 if definition.get("multi") else 0,
                        1 if is_writable else 0,
                        1 if read_only else 0,
                        1 if definition.get("special") else 0,
                        str(definition.get("special") or ""),
                        json.dumps([f"write_property:{attribute}"] if not read_only else []),
                        json.dumps(["user"]),
                        1 if schema_detected else 0,
                        capability_status,
                        json.dumps({"reject_arbitrary_script": True}),
                        int(snapshot_id),
                        now,
                    ),
                )
        return len(definitions)

    def list_attributes(
        self,
        *,
        org_id: str,
        ad_connector_id: str = "",
        current_snapshot_id: Optional[int] = None,
        writable_only: bool = False,
    ) -> list[ADTargetAttributeRecord]:
        clauses = ["org_id = ?"]
        params: list[Any] = [self._resolve_org_id(org_id, default="default") or "default"]
        if ad_connector_id:
            clauses.append("ad_connector_id = ?")
            params.append(str(ad_connector_id).strip())
        if current_snapshot_id is not None:
            clauses.append("latest_snapshot_id = ?")
            params.append(int(current_snapshot_id))
        if writable_only:
            clauses.append("is_writable = 1")
        rows = self._fetchall(
            f"""
            SELECT * FROM ad_target_attribute_registry
            WHERE {' AND '.join(clauses)}
            ORDER BY category, ldap_attribute
            """,
            params,
        )
        return [ADTargetAttributeRecord.from_row(row) for row in rows]


__all__ = [
    "ADTargetAttributeRegistryRepository",
    "CanonicalFieldRegistryRepository",
    "SourceFieldRegistryRepository",
    "canonical_field_for_path",
    "is_secret_field",
    "is_sensitive_field",
]
