from __future__ import annotations

from typing import Any


SOURCE_MATCH_FIELD_DEFINITIONS = (
    ("platform_account_id", "Platform User ID"),
    ("employee_id", "Employee ID"),
    ("employee_number", "Employee Number"),
    ("email", "Email"),
    ("email_localpart", "Email local part"),
    ("mobile", "Mobile"),
    ("display_name", "Display Name"),
    ("primary_department_id", "Primary Department ID"),
    ("manager_account_id", "Manager account ID"),
)
AD_MATCH_FIELD_DEFINITIONS = (
    ("sam_account_name", "AD logon name (sAMAccountName)"),
    ("user_principal_name", "User principal name (userPrincipalName / UPN)"),
    ("employee_id", "Employee ID (employeeID)"),
    ("employee_number", "Employee Number (employeeNumber)"),
    ("mail", "Email"),
    ("mobile", "Mobile"),
    ("telephone_number", "Telephone number"),
    ("display_name", "Display Name"),
    ("object_guid", "Object GUID"),
    ("object_sid", "Object SID"),
    ("distinguished_name", "Distinguished name"),
    ("manager_dn", "Manager distinguished name"),
    ("ou_path", "OU path"),
)
SOURCE_PROVIDER_ID_HINTS = {
    "dingtalk": "userid",
    "wecom": "userid",
    "feishu": "open_id / user_id",
}
AD_LDAP_TO_ACCOUNT_FIELD = {
    "samaccountname": "sam_account_name",
    "userprincipalname": "user_principal_name",
    "employeeid": "employee_id",
    "employeenumber": "employee_number",
    "telephonenumber": "telephone_number",
    "displayname": "display_name",
    "objectguid": "object_guid",
    "objectsid": "object_sid",
    "distinguishedname": "distinguished_name",
    "manager": "manager_dn",
}


def _field_value(item: Any, key: str, default: Any = "") -> Any:
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def build_source_match_field_options(
    *,
    provider_id: str,
    detected_fields: list[dict[str, Any]],
    existing_rules: list[Any],
) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_option(
        value: Any,
        label: Any,
        *,
        group: str,
        api_hint: str = "",
        coverage_count: int | None = None,
        coverage_rate: float | None = None,
        data_type: str = "string",
        is_multi_value: bool = False,
        availability_status: str = "available",
        permission_status: str = "granted",
    ) -> None:
        normalized_value = str(value or "").strip()
        if not normalized_value:
            return
        if normalized_value in seen:
            if group == "detected":
                existing = next(
                    item for item in options if item["value"] == normalized_value
                )
                existing.update(
                    coverage_count=coverage_count,
                    coverage_rate=coverage_rate,
                    data_type=data_type,
                    is_multi_value=bool(is_multi_value),
                    availability_status=availability_status,
                    permission_status=permission_status,
                    disabled=(
                        availability_status not in {"available", "type_conflict"}
                        or permission_status in {"denied", "unavailable"}
                    ),
                )
            return
        seen.add(normalized_value)
        options.append(
            {
                "value": normalized_value,
                "label": str(label or normalized_value).strip() or normalized_value,
                "group": group,
                "api_hint": str(api_hint or "").strip(),
                "coverage_count": coverage_count,
                "coverage_rate": coverage_rate,
                "data_type": data_type,
                "is_multi_value": bool(is_multi_value),
                "availability_status": availability_status,
                "permission_status": permission_status,
                "disabled": availability_status not in {"available", "type_conflict"}
                or permission_status in {"denied", "unavailable"},
            }
        )

    for value, label in SOURCE_MATCH_FIELD_DEFINITIONS:
        add_option(
            value,
            label,
            group="standard",
            api_hint=(
                SOURCE_PROVIDER_ID_HINTS.get(provider_id, "")
                if value == "platform_account_id"
                else ""
            ),
        )
    for field in detected_fields:
        field_path = _field_value(field, "raw_field_path") or _field_value(
            field, "field_name"
        )
        add_option(
            field_path,
            _field_value(field, "display_label")
            or _field_value(field, "field_label")
            or field_path,
            group="detected",
            coverage_count=int(_field_value(field, "coverage_count", 0) or 0),
            coverage_rate=float(_field_value(field, "coverage_rate", 0.0) or 0.0),
            data_type=str(_field_value(field, "data_type", "string") or "string"),
            is_multi_value=bool(_field_value(field, "is_multi_value", False)),
            availability_status=str(
                _field_value(field, "availability_status", "available") or "available"
            ),
            permission_status=str(
                _field_value(field, "permission_status", "granted") or "granted"
            ),
        )
    for rule in existing_rules:
        add_option(
            getattr(rule, "source_field", ""),
            getattr(rule, "source_field", ""),
            group="existing",
        )
    return options


def build_ad_match_field_options(
    existing_rules: list[Any],
    detected_attributes: list[Any] | None = None,
) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_option(
        value: Any,
        label: Any,
        *,
        group: str,
        ldap_attribute: str = "",
        capability_status: str = "available",
        schema_detected: bool = True,
        is_read_only: bool = False,
    ) -> None:
        normalized_value = str(value or "").strip()
        if not normalized_value:
            return
        if normalized_value in seen:
            if group == "detected":
                existing = next(
                    item for item in options if item["value"] == normalized_value
                )
                existing.update(
                    ldap_attribute=ldap_attribute,
                    capability_status=capability_status,
                    schema_detected=bool(schema_detected),
                    is_read_only=bool(is_read_only),
                    disabled=capability_status
                    in {"not_detected", "unavailable_by_permission", "unknown"},
                )
            return
        seen.add(normalized_value)
        options.append(
            {
                "value": normalized_value,
                "label": str(label or normalized_value).strip() or normalized_value,
                "group": group,
                "ldap_attribute": ldap_attribute,
                "capability_status": capability_status,
                "schema_detected": bool(schema_detected),
                "is_read_only": bool(is_read_only),
                "disabled": capability_status in {
                    "not_detected",
                    "unavailable_by_permission",
                    "unknown",
                },
            }
        )

    for value, label in AD_MATCH_FIELD_DEFINITIONS:
        add_option(value, label, group="standard")
    for index in range(1, 16):
        value = f"extensionAttribute{index}"
        add_option(value, value, group="extension")
    for attribute in detected_attributes or []:
        ldap_attribute = str(_field_value(attribute, "ldap_attribute") or "").strip()
        if not ldap_attribute:
            continue
        account_field = AD_LDAP_TO_ACCOUNT_FIELD.get(
            ldap_attribute.casefold(), ldap_attribute
        )
        add_option(
            account_field,
            _field_value(attribute, "display_label") or ldap_attribute,
            group="detected",
            ldap_attribute=ldap_attribute,
            capability_status=str(
                _field_value(attribute, "capability_status", "unknown") or "unknown"
            ),
            schema_detected=bool(_field_value(attribute, "schema_detected", False)),
            is_read_only=bool(_field_value(attribute, "is_read_only", False)),
        )
    for rule in existing_rules:
        add_option(
            getattr(rule, "ad_field", ""),
            getattr(rule, "ad_field", ""),
            group="existing",
        )
    return options
