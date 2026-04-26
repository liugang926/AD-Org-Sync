from __future__ import annotations

from typing import Any

from sync_app.providers.source import get_source_provider_schema, list_source_provider_schemas


def build_source_provider_field_models(editable: dict[str, Any], fields: tuple[Any, ...]) -> list[dict[str, Any]]:
    field_models: list[dict[str, Any]] = []
    for field in fields:
        configured = bool(editable.get(f"{field.name}_configured")) if field.secret else bool(editable.get(field.name))
        placeholder = field.placeholder
        help_text = field.help_text
        if field.secret:
            placeholder = "********" if configured else (field.placeholder or "Enter value")
            if configured:
                configured_hint = "Leave blank to keep current"
                help_text = f"{field.help_text} {configured_hint}".strip() if field.help_text else configured_hint
        field_models.append(
            {
                "name": field.name,
                "label": field.label,
                "value": "" if field.secret else editable.get(field.name, ""),
                "type": field.input_type,
                "help_text": help_text,
                "placeholder": placeholder,
                "required": field.required,
                "configured": configured,
                "class_name": "field-span-full" if field.width == "full" else "",
                "autocomplete": field.autocomplete,
                "secret": field.secret,
            }
        )
    return field_models


def build_source_provider_fields(editable: dict[str, Any]) -> list[dict[str, Any]]:
    provider_schema = get_source_provider_schema(editable.get("source_provider"))
    return build_source_provider_field_models(
        editable,
        (*provider_schema.connection_fields, *provider_schema.notification_fields),
    )


def build_source_provider_ui_catalog(request_support: Any, ui_language: str) -> dict[str, Any]:
    catalog: dict[str, Any] = {}
    for schema in list_source_provider_schemas(include_unimplemented=True):
        localized_provider_name = request_support.translate_text(ui_language, schema.display_name)
        description = request_support.translate_text(
            ui_language,
            schema.implementation_status or schema.description or "",
        )
        field_catalog: dict[str, Any] = {}
        for field in (*schema.connection_fields, *schema.notification_fields):
            field_catalog[field.name] = {
                "label": request_support.translate_text(ui_language, field.label),
                "helpText": request_support.translate_text(ui_language, field.help_text),
                "placeholder": request_support.translate_text(ui_language, field.placeholder),
                "required": bool(field.required),
                "secret": bool(field.secret),
            }
        catalog[schema.provider_id] = {
            "displayName": localized_provider_name,
            "description": description,
            "pageTitle": request_support.translate_text(
                ui_language,
                "{provider} Connector Configuration",
                provider=localized_provider_name,
            ),
            "pageSummary": request_support.translate_text(
                ui_language,
                "Configure {provider} and shared organization settings one section at a time.",
                provider=localized_provider_name,
            ),
            "connectorTitle": request_support.translate_text(
                ui_language,
                "{provider} Source Connector",
                provider=localized_provider_name,
            ),
            "connectorDescription": request_support.translate_text(
                ui_language,
                "Enter the credentials required by {provider}. Notification delivery is optional.",
                provider=localized_provider_name,
            ),
            "sourceGuidance": request_support.translate_text(
                ui_language,
                "Select the source provider and complete the credentials required by {provider}.",
                provider=localized_provider_name,
            ),
            "fields": field_catalog,
        }
    return catalog


def build_config_preview_groups(provider_schema: Any) -> tuple[tuple[str, tuple[tuple[str, str, str], ...]], ...]:
    source_fields = [
        ("source_provider", "Source Provider", "source_provider"),
    ]
    for field in provider_schema.connection_fields:
        source_fields.append(
            (
                field.name,
                field.label,
                "secret" if field.secret else ("number" if field.input_type == "number" else "text"),
            )
        )
    notification_fields = tuple(
        (
            field.name,
            field.label,
            "secret" if field.secret else ("number" if field.input_type == "number" else "text"),
        )
        for field in provider_schema.notification_fields
    )
    groups: list[tuple[str, tuple[tuple[str, str, str], ...]]] = [
        (
            "Connection Settings",
            (
                *source_fields,
                ("ldap_server", "LDAP Server", "text"),
                ("ldap_domain", "LDAP Domain", "text"),
                ("ldap_username", "LDAP Username", "text"),
                ("ldap_password", "LDAP Password", "secret"),
                ("ldap_port", "LDAP Port", "number"),
                ("ldap_use_ssl", "Use SSL", "bool"),
            ),
        ),
    ]
    if notification_fields:
        groups.append(("Optional Notifications", notification_fields))
    groups.extend(
        [
            (
                "LDAP Security",
                (
                    ("ldap_validate_cert", "Certificate Validation", "bool"),
                    ("ldap_ca_cert_path", "CA Certificate Path", "text"),
                ),
            ),
            (
                "Account Policy",
                (
                    ("default_password", "Default Password", "secret"),
                    ("force_change_password", "Force Password Change", "bool"),
                    ("password_complexity", "Password Complexity", "password_complexity"),
                ),
            ),
            (
                "Sync Scope And OU Mapping",
                (
                    ("source_root_unit_ids", "Source Root Unit IDs Filter", "source_root_units"),
                    ("directory_root_ou_path", "Target AD Root OU Path / DN", "ou_path"),
                    ("disabled_users_ou_path", "Disabled Users OU Path / DN", "ou_path"),
                    ("custom_group_ou_path", "Custom Group OU Path / DN", "ou_path"),
                ),
            ),
            (
                "Runtime Policy",
                (
                    ("schedule_time", "Daily Schedule Time", "text"),
                    ("retry_interval", "Retry Interval (min)", "number"),
                    ("max_retries", "Max Retries", "number"),
                    ("group_display_separator", "Group Separator", "group_separator"),
                    ("group_recursive_enabled", "Recursive Group Sync", "bool"),
                    ("managed_relation_cleanup_enabled", "Relation Cleanup", "bool"),
                    ("schedule_execution_mode", "Scheduled Mode", "schedule_execution_mode"),
                    ("user_ou_placement_strategy", "OU Placement Strategy", "placement_strategy"),
                ),
            ),
            (
                "Web Deployment",
                (
                    ("web_bind_host", "Bind Host", "text"),
                    ("web_bind_port", "Bind Port", "number"),
                    ("web_public_base_url", "Public Base URL", "text"),
                    ("web_session_cookie_secure_mode", "Secure Cookie Policy", "secure_cookie_mode"),
                    ("web_trust_proxy_headers", "Trust Proxy Headers", "bool"),
                    ("web_forwarded_allow_ips", "Forwarded Allow IPs", "text"),
                ),
            ),
            (
                "Branding",
                (
                    ("brand_display_name", "Brand Display Name", "text"),
                    ("brand_mark_text", "Brand Mark Text", "text"),
                    ("brand_attribution", "Footer Attribution", "text"),
                ),
            ),
            (
                "Group Rules",
                (
                    ("soft_excluded_groups", "Soft Excluded Groups", "multiline"),
                ),
            ),
        ]
    )
    return tuple(groups)


def _normalize_config_change_choice_key(value: Any) -> str:
    return str(value or "").strip().lower()


def format_config_change_value(
    field_type: str,
    value: Any,
    *,
    previous_value: Any = None,
    source_provider_label: Any,
    placement_strategies: dict[str, str],
    split_csv_values: Any,
) -> tuple[str, bool]:
    if field_type == "secret":
        if not value:
            return "Not configured", True
        if previous_value and previous_value != value:
            return "Updated", True
        return "Configured", True
    if field_type == "bool":
        return ("Enabled" if bool(value) else "Disabled"), True
    if field_type == "number":
        return str(value), False
    if field_type == "source_provider":
        return source_provider_label(value), False
    if field_type == "password_complexity":
        return {
            "strong": "Strong",
            "medium": "Medium",
            "basic": "Basic",
        }.get(_normalize_config_change_choice_key(value), str(value or "-")), True
    if field_type == "schedule_execution_mode":
        return ("Dry Run" if _normalize_config_change_choice_key(value) == "dry_run" else "Apply"), True
    if field_type == "placement_strategy":
        return placement_strategies.get(str(value or ""), str(value or "-")), True
    if field_type == "secure_cookie_mode":
        return {
            "auto": "auto",
            "always": "always",
            "never": "never",
        }.get(_normalize_config_change_choice_key(value), str(value or "-")), False
    if field_type == "group_separator":
        return ("Space", True) if str(value or "") == " " else (str(value or "-"), False)
    if field_type == "source_root_units":
        normalized_items = split_csv_values(str(value or ""))
        if not normalized_items:
            return "All departments", True
        return ", ".join(normalized_items), False
    if field_type == "ou_path":
        normalized_value = str(value or "").strip()
        if not normalized_value:
            return "Domain root", True
        return normalized_value, False
    if field_type == "multiline":
        normalized_lines = [line.strip() for line in str(value or "").splitlines() if line.strip()]
        if not normalized_lines:
            return "None", True
        return ", ".join(normalized_lines), False
    normalized_value = str(value or "").strip()
    if not normalized_value:
        return "Not set", True
    return normalized_value, False
