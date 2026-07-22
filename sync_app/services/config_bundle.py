from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sync_app.storage.local_db import (
    DEFAULT_APP_SETTINGS,
    ORG_SCOPED_APP_SETTINGS,
    AttributeMappingRuleRepository,
    DepartmentOuMappingRepository,
    DatabaseManager,
    GroupExclusionRuleRepository,
    FieldAuthorityRuleRepository,
    IdentityMatchRuleRepository,
    OrganizationConfigRepository,
    OrganizationRepository,
    SettingsRepository,
    SyncConnectorRepository,
)

BUNDLE_TYPE = "organization_config_bundle"
BUNDLE_VERSION = 1
VOLATILE_ORG_SETTINGS = {"last_sync_time", "last_sync_success"}
SENSITIVE_ORG_SETTINGS = {"integration_api_token"}
EXPORTABLE_ORG_SETTINGS = tuple(
    key
    for key in sorted(ORG_SCOPED_APP_SETTINGS)
    if key not in VOLATILE_ORG_SETTINGS | SENSITIVE_ORG_SETTINGS
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _normalize_bundle_org_id(raw_org_id: Optional[str], fallback: str = "default") -> str:
    normalized = str(raw_org_id or "").strip().lower()
    return normalized or fallback


def export_organization_bundle(db_manager: DatabaseManager, org_id: str) -> Dict[str, Any]:
    organization_repo = OrganizationRepository(db_manager)
    organization = organization_repo.get_organization_record(org_id)
    if not organization:
        raise ValueError(f"organization not found: {org_id}")

    org_config_repo = OrganizationConfigRepository(db_manager)
    settings_repo = SettingsRepository(db_manager)
    connector_repo = SyncConnectorRepository(db_manager)
    mapping_repo = AttributeMappingRuleRepository(db_manager)
    department_ou_mapping_repo = DepartmentOuMappingRepository(db_manager)
    exclusion_repo = GroupExclusionRuleRepository(db_manager)
    identity_match_rule_repo = IdentityMatchRuleRepository(db_manager)
    field_authority_rule_repo = FieldAuthorityRuleRepository(db_manager)

    org_settings: Dict[str, Any] = {}
    for key in EXPORTABLE_ORG_SETTINGS:
        default_value, value_type = DEFAULT_APP_SETTINGS.get(key, ("", "string"))
        raw_value = settings_repo.get_value(
            key,
            default_value,
            org_id=organization.org_id,
            fallback_to_global=False,
        )
        if raw_value is None:
            raw_value = default_value
        if value_type == "bool":
            org_settings[key] = str(raw_value).strip().lower() in {"1", "true", "yes", "on"}
        elif value_type == "int":
            org_settings[key] = int(raw_value)
        elif value_type == "float":
            org_settings[key] = float(raw_value)
        else:
            org_settings[key] = str(raw_value)

    connectors = [
        {
            "connector_id": record.connector_id,
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
        for record in connector_repo.list_connector_records(org_id=organization.org_id)
    ]

    attribute_mappings = [
        {
            "connector_id": record.connector_id,
            "direction": record.direction,
            "source_field": record.source_field,
            "target_field": record.target_field,
            "transform_template": record.transform_template,
            "sync_mode": record.sync_mode,
            "is_enabled": record.is_enabled,
            "notes": record.notes,
        }
        for record in mapping_repo.list_rule_records(org_id=organization.org_id)
    ]

    department_ou_mappings = [
        {
            "connector_id": record.connector_id,
            "source_department_id": record.source_department_id,
            "source_department_name": record.source_department_name,
            "target_ou_path": record.target_ou_path,
            "apply_mode": record.apply_mode,
            "is_enabled": record.is_enabled,
            "notes": record.notes,
        }
        for record in department_ou_mapping_repo.list_mapping_records(org_id=organization.org_id)
    ]

    group_exclusion_rules = [
        {
            "rule_type": record.rule_type,
            "protection_level": record.protection_level,
            "match_type": record.match_type,
            "match_value": record.match_value,
            "display_name": record.display_name,
            "is_enabled": record.is_enabled,
            "source": record.source,
        }
        for record in exclusion_repo.list_rule_records(org_id=organization.org_id)
    ]

    identity_match_rules = [
        {
            "rule_order": record.rule_order,
            "rule_name": record.rule_name,
            "source_provider": record.source_provider,
            "source_field": record.source_field,
            "ad_field": record.ad_field,
            "is_required": record.is_required,
            "case_sensitive": record.case_sensitive,
            "trim_whitespace": record.trim_whitespace,
            "strip_phone_country_code": record.strip_phone_country_code,
            "lowercase_email": record.lowercase_email,
            "allow_fallback": record.allow_fallback,
            "allow_auto_link": record.allow_auto_link,
            "confidence_level": record.confidence_level,
            "confidence_score": record.confidence_score,
            "stop_on_conflict": record.stop_on_conflict,
            "is_enabled": record.is_enabled,
        }
        for record in identity_match_rule_repo.list_rules(org_id=organization.org_id)
    ]

    field_authority_rules = [
        {
            "field_name": record.field_name,
            "source_provider": record.source_provider,
            "source_priority": record.source_priority,
            "sync_direction": record.sync_direction,
            "sync_mode": record.sync_mode,
            "prevent_loop": record.prevent_loop,
            "is_enabled": record.is_enabled,
            "notes": record.notes,
        }
        for record in field_authority_rule_repo.list_rules(org_id=organization.org_id)
    ]

    raw_org_config = org_config_repo.get_raw_config(
        organization.org_id,
        config_path=organization.config_path,
    )
    raw_org_config.pop("config_path", None)

    return {
        "bundle_type": BUNDLE_TYPE,
        "bundle_version": BUNDLE_VERSION,
        "exported_at": _utcnow_iso(),
        "organization": {
            "org_id": organization.org_id,
            "name": organization.name,
            "description": organization.description,
            "is_enabled": organization.is_enabled,
            "config_path": organization.config_path,
        },
        "organization_config": raw_org_config,
        "org_settings": org_settings,
        "connectors": connectors,
        "attribute_mappings": attribute_mappings,
        "department_ou_mappings": department_ou_mappings,
        "group_exclusion_rules": group_exclusion_rules,
        "identity_match_rules": identity_match_rules,
        "field_authority_rules": field_authority_rules,
    }


def import_organization_bundle(
    db_manager: DatabaseManager,
    bundle: Dict[str, Any],
    *,
    target_org_id: str | None = None,
    replace_existing: bool = False,
) -> Dict[str, Any]:
    if not isinstance(bundle, dict):
        raise ValueError("bundle must be a JSON object")
    if str(bundle.get("bundle_type") or "") != BUNDLE_TYPE:
        raise ValueError("unsupported bundle_type")
    if int(bundle.get("bundle_version") or 0) != BUNDLE_VERSION:
        raise ValueError("unsupported bundle_version")

    organization_data = dict(bundle.get("organization") or {})
    source_org_id = _normalize_bundle_org_id(organization_data.get("org_id"), fallback="default")
    effective_org_id = _normalize_bundle_org_id(target_org_id, fallback=source_org_id)

    organization_repo = OrganizationRepository(db_manager)
    org_config_repo = OrganizationConfigRepository(db_manager)
    settings_repo = SettingsRepository(db_manager)
    connector_repo = SyncConnectorRepository(db_manager)
    mapping_repo = AttributeMappingRuleRepository(db_manager)
    department_ou_mapping_repo = DepartmentOuMappingRepository(db_manager)
    exclusion_repo = GroupExclusionRuleRepository(db_manager)
    identity_match_rule_repo = IdentityMatchRuleRepository(db_manager)
    field_authority_rule_repo = FieldAuthorityRuleRepository(db_manager)

    config_path = str(organization_data.get("config_path") or "").strip()
    organization_repo.upsert_organization(
        org_id=effective_org_id,
        name=str(organization_data.get("name") or effective_org_id).strip() or effective_org_id,
        config_path=config_path,
        description=str(organization_data.get("description") or "").strip(),
        is_enabled=bool(organization_data.get("is_enabled", True)),
    )

    if replace_existing:
        settings_repo.delete_org_scoped_values(effective_org_id)
        connector_repo.delete_connectors_for_org(effective_org_id)
        mapping_repo.delete_rules_for_org(effective_org_id)
        department_ou_mapping_repo.delete_mappings_for_org(effective_org_id)
        exclusion_repo.delete_rules_for_org(effective_org_id)
        if "identity_match_rules" in bundle:
            identity_match_rule_repo.delete_rules_for_org(effective_org_id)
        if "field_authority_rules" in bundle:
            field_authority_rule_repo.delete_rules_for_org(effective_org_id)

    org_config_values = dict(bundle.get("organization_config") or {})
    org_config_repo.save_config(effective_org_id, org_config_values, config_path=config_path)

    org_settings = dict(bundle.get("org_settings") or {})
    imported_settings = 0
    for key in EXPORTABLE_ORG_SETTINGS:
        if key in org_settings:
            default_value, value_type = DEFAULT_APP_SETTINGS.get(key, ("", "string"))
            value = org_settings.get(key, default_value)
            settings_repo.set_value(key, value, value_type, org_id=effective_org_id)
            imported_settings += 1

    imported_connectors = 0
    for connector in list(bundle.get("connectors") or []):
        connector_repo.upsert_connector(
            connector_id=str(connector.get("connector_id") or "").strip(),
            org_id=effective_org_id,
            name=str(connector.get("name") or connector.get("connector_id") or "").strip(),
            config_path=str(connector.get("config_path") or "").strip(),
            ldap_server=str(connector.get("ldap_server") or "").strip(),
            ldap_domain=str(connector.get("ldap_domain") or "").strip(),
            ldap_username=str(connector.get("ldap_username") or "").strip(),
            ldap_password=str(connector.get("ldap_password") or "").strip(),
            ldap_use_ssl=connector.get("ldap_use_ssl"),
            ldap_port=connector.get("ldap_port"),
            ldap_validate_cert=connector.get("ldap_validate_cert"),
            ldap_ca_cert_path=str(connector.get("ldap_ca_cert_path") or "").strip(),
            default_password=str(connector.get("default_password") or "").strip(),
            force_change_password=connector.get("force_change_password"),
            password_complexity=str(connector.get("password_complexity") or "").strip(),
            root_department_ids=list(connector.get("root_department_ids") or []),
            username_strategy=str(connector.get("username_strategy") or "custom_template").strip(),
            username_collision_policy=str(connector.get("username_collision_policy") or "append_employee_id").strip(),
            username_collision_template=str(connector.get("username_collision_template") or "").strip(),
            username_template=str(connector.get("username_template") or "").strip(),
            disabled_users_ou=str(connector.get("disabled_users_ou") or "").strip(),
            group_type=str(connector.get("group_type") or "security").strip(),
            group_mail_domain=str(connector.get("group_mail_domain") or "").strip(),
            custom_group_ou_path=str(connector.get("custom_group_ou_path") or "").strip(),
            managed_tag_ids=list(connector.get("managed_tag_ids") or []),
            managed_external_chat_ids=list(connector.get("managed_external_chat_ids") or []),
            is_enabled=bool(connector.get("is_enabled", True)),
        )
        imported_connectors += 1

    imported_mappings = 0
    for mapping in list(bundle.get("attribute_mappings") or []):
        mapping_repo.upsert_rule(
            org_id=effective_org_id,
            connector_id=str(mapping.get("connector_id") or "").strip(),
            direction=str(mapping.get("direction") or "source_to_ad").strip(),
            source_field=str(mapping.get("source_field") or "").strip(),
            target_field=str(mapping.get("target_field") or "").strip(),
            transform_template=str(mapping.get("transform_template") or "").strip(),
            sync_mode=str(mapping.get("sync_mode") or "replace").strip(),
            is_enabled=bool(mapping.get("is_enabled", True)),
            notes=str(mapping.get("notes") or "").strip(),
        )
        imported_mappings += 1

    imported_department_ou_mappings = 0
    for mapping in list(bundle.get("department_ou_mappings") or []):
        department_ou_mapping_repo.upsert_mapping(
            org_id=effective_org_id,
            connector_id=str(mapping.get("connector_id") or "").strip(),
            source_department_id=str(mapping.get("source_department_id") or "").strip(),
            source_department_name=str(mapping.get("source_department_name") or "").strip(),
            target_ou_path=str(mapping.get("target_ou_path") or "").strip(),
            apply_mode=str(mapping.get("apply_mode") or "subtree").strip(),
            is_enabled=bool(mapping.get("is_enabled", True)),
            notes=str(mapping.get("notes") or "").strip(),
        )
        imported_department_ou_mappings += 1

    imported_group_rules = 0
    for rule in list(bundle.get("group_exclusion_rules") or []):
        exclusion_repo.upsert_rule(
            org_id=effective_org_id,
            rule_type=str(rule.get("rule_type") or "").strip(),
            protection_level=str(rule.get("protection_level") or "").strip(),
            match_type=str(rule.get("match_type") or "").strip(),
            match_value=str(rule.get("match_value") or "").strip(),
            display_name=str(rule.get("display_name") or "").strip(),
            is_enabled=bool(rule.get("is_enabled", True)),
            source=str(rule.get("source") or "import").strip() or "import",
        )
        imported_group_rules += 1

    imported_identity_match_rules = 0
    for rule in list(bundle.get("identity_match_rules") or []):
        identity_match_rule_repo.upsert_rule(
            org_id=effective_org_id,
            rule_order=int(rule.get("rule_order") or 100),
            rule_name=str(rule.get("rule_name") or "").strip(),
            source_provider=str(rule.get("source_provider") or "*").strip(),
            source_field=str(rule.get("source_field") or "").strip(),
            ad_field=str(rule.get("ad_field") or "").strip(),
            is_required=bool(rule.get("is_required", False)),
            case_sensitive=bool(rule.get("case_sensitive", False)),
            trim_whitespace=bool(rule.get("trim_whitespace", True)),
            strip_phone_country_code=bool(
                rule.get("strip_phone_country_code", False)
            ),
            lowercase_email=bool(rule.get("lowercase_email", False)),
            allow_fallback=bool(rule.get("allow_fallback", True)),
            allow_auto_link=bool(rule.get("allow_auto_link", False)),
            confidence_level=str(rule.get("confidence_level") or "medium"),
            confidence_score=int(rule.get("confidence_score") or 50),
            stop_on_conflict=bool(rule.get("stop_on_conflict", True)),
            is_enabled=bool(rule.get("is_enabled", True)),
            created_by="config_bundle_import",
        )
        imported_identity_match_rules += 1

    imported_field_authority_rules = 0
    for rule in list(bundle.get("field_authority_rules") or []):
        field_authority_rule_repo.upsert_rule(
            org_id=effective_org_id,
            field_name=str(rule.get("field_name") or "").strip(),
            source_provider=str(rule.get("source_provider") or "*").strip(),
            source_priority=int(rule.get("source_priority") or 100),
            sync_direction=str(
                rule.get("sync_direction") or "source_to_ad"
            ).strip(),
            sync_mode=str(rule.get("sync_mode") or "replace").strip(),
            prevent_loop=bool(rule.get("prevent_loop", True)),
            is_enabled=bool(rule.get("is_enabled", True)),
            notes=str(rule.get("notes") or "").strip(),
            created_by="config_bundle_import",
        )
        imported_field_authority_rules += 1

    return {
        "org_id": effective_org_id,
        "source_org_id": source_org_id,
        "replace_existing": bool(replace_existing),
        "imported_settings": imported_settings,
        "imported_connectors": imported_connectors,
        "imported_mappings": imported_mappings,
        "imported_department_ou_mappings": imported_department_ou_mappings,
        "imported_group_rules": imported_group_rules,
        "imported_identity_match_rules": imported_identity_match_rules,
        "imported_field_authority_rules": imported_field_authority_rules,
    }
