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
    ) -> None:
        normalized_value = str(value or "").strip()
        if not normalized_value or normalized_value in seen:
            return
        seen.add(normalized_value)
        options.append(
            {
                "value": normalized_value,
                "label": str(label or normalized_value).strip() or normalized_value,
                "group": group,
                "api_hint": str(api_hint or "").strip(),
                "coverage_count": coverage_count,
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
        add_option(
            field.get("field_name"),
            field.get("field_label") or field.get("field_name"),
            group="detected",
            coverage_count=int(field.get("coverage_count") or 0),
        )
    for rule in existing_rules:
        add_option(
            getattr(rule, "source_field", ""),
            getattr(rule, "source_field", ""),
            group="existing",
        )
    return options


def build_ad_match_field_options(existing_rules: list[Any]) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_option(value: Any, label: Any, *, group: str) -> None:
        normalized_value = str(value or "").strip()
        if not normalized_value or normalized_value in seen:
            return
        seen.add(normalized_value)
        options.append(
            {
                "value": normalized_value,
                "label": str(label or normalized_value).strip() or normalized_value,
                "group": group,
            }
        )

    for value, label in AD_MATCH_FIELD_DEFINITIONS:
        add_option(value, label, group="standard")
    for index in range(1, 16):
        value = f"extensionAttribute{index}"
        add_option(value, value, group="extension")
    for rule in existing_rules:
        add_option(
            getattr(rule, "ad_field", ""),
            getattr(rule, "ad_field", ""),
            group="existing",
        )
    return options
