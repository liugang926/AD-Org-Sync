from __future__ import annotations

from typing import Any, Mapping

from sync_app.services.typed_settings import AdvancedSyncPolicySettings


USERNAME_STRATEGY_BY_SOURCE_FIELD: dict[str, str] = {
    "source_user_id": "userid",
    "employee_id": "employee_id",
    "email_localpart": "email_localpart",
    "pinyin_initials_employee_id": "pinyin_initials_employee_id",
    "pinyin_full_employee_id": "pinyin_full_employee_id",
    "family_name_pinyin_given_initials": "family_name_pinyin_given_initials",
    "family_name_pinyin_given_name_pinyin": "family_name_pinyin_given_name_pinyin",
    "custom_template": "custom_template",
}


POLICY_SECTION_FIELDS: dict[str, frozenset[str]] = {
    "attribute_mappings": frozenset({"attribute_mapping_enabled", "write_back_enabled"}),
    "group_rules": frozenset(
        {
            "custom_group_sync_enabled",
            "managed_group_type",
            "managed_group_mail_domain",
            "custom_group_ou_path",
        }
    ),
    "lifecycle": frozenset(
        {
            "offboarding_grace_days",
            "offboarding_notify_managers",
            "offboarding_lifecycle_enabled",
            "rehire_restore_enabled",
            "automatic_replay_enabled",
            "future_onboarding_enabled",
            "future_onboarding_start_field",
            "contractor_lifecycle_enabled",
            "lifecycle_employment_type_field",
            "contractor_end_field",
            "lifecycle_sponsor_field",
            "contractor_type_values",
        }
    ),
    "security": frozenset(
        {
            "advanced_connector_routing_enabled",
            "disable_circuit_breaker_enabled",
            "disable_circuit_breaker_percent",
            "disable_circuit_breaker_min_count",
            "disable_circuit_breaker_requires_approval",
            "first_sync_identity_claim_mode",
        }
    ),
}


CONNECTOR_POLICY_SECTION_FIELDS: dict[str, frozenset[str]] = {
    "scope": frozenset({"root_department_ids"}),
    "account_naming": frozenset(
        {
            "username_strategy",
            "username_collision_policy",
            "username_collision_template",
            "username_template",
        }
    ),
    "group_rules": frozenset(
        {
            "group_type",
            "group_mail_domain",
            "custom_group_ou_path",
            "managed_tag_ids",
            "managed_external_chat_ids",
        }
    ),
    "lifecycle": frozenset({"disabled_users_ou"}),
    "security": frozenset({"force_change_password", "password_complexity"}),
}


def update_policy_section(
    current: AdvancedSyncPolicySettings,
    section: str,
    values: Mapping[str, Any],
) -> AdvancedSyncPolicySettings:
    allowed_fields = POLICY_SECTION_FIELDS.get(str(section or "").strip())
    if allowed_fields is None:
        raise ValueError(f"Unknown sync policy section: {section}")
    unknown_fields = set(values) - set(allowed_fields)
    if unknown_fields:
        raise ValueError(
            "Fields do not belong to this sync policy section: "
            + ", ".join(sorted(unknown_fields))
        )
    merged = current.to_dict()
    merged.update({field: values[field] for field in allowed_fields if field in values})
    return AdvancedSyncPolicySettings.from_mapping(merged)


def build_connector_policy_upsert(
    record: Any,
    section: str,
    values: Mapping[str, Any],
) -> dict[str, Any]:
    allowed_fields = CONNECTOR_POLICY_SECTION_FIELDS.get(str(section or "").strip())
    if allowed_fields is None:
        raise ValueError(f"Unknown connector policy section: {section}")
    unknown_fields = set(values) - set(allowed_fields)
    if unknown_fields:
        raise ValueError(
            "Fields do not belong to this connector policy section: "
            + ", ".join(sorted(unknown_fields))
        )
    payload = {
        "connector_id": record.connector_id,
        "org_id": record.org_id,
        "name": record.name,
        "config_path": record.config_path,
        "ldap_server": record.ldap_server,
        "ldap_domain": record.ldap_domain,
        "ldap_username": record.ldap_username,
        "ldap_password": record.ldap_password,
        "ldap_use_ssl": record.ldap_use_ssl,
        "ldap_port": record.ldap_port,
        "ldap_validate_cert": record.ldap_validate_cert,
        "ldap_ca_cert_path": record.ldap_ca_cert_path,
        "default_password": record.default_password,
        "force_change_password": record.force_change_password,
        "password_complexity": record.password_complexity,
        "root_department_ids": list(record.root_department_ids),
        "username_strategy": record.username_strategy,
        "username_collision_policy": record.username_collision_policy,
        "username_collision_template": record.username_collision_template,
        "username_template": record.username_template,
        "disabled_users_ou": record.disabled_users_ou,
        "group_type": record.group_type,
        "group_mail_domain": record.group_mail_domain,
        "custom_group_ou_path": record.custom_group_ou_path,
        "managed_tag_ids": list(record.managed_tag_ids),
        "managed_external_chat_ids": list(record.managed_external_chat_ids),
        "is_enabled": record.is_enabled,
    }
    payload.update({field: values[field] for field in allowed_fields if field in values})
    return payload
