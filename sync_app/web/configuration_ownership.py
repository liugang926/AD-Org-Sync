from __future__ import annotations

from dataclasses import dataclass

from sync_app.web.navigation import CANONICAL_ROUTE_PATHS


ORGANIZATION_SCOPE = "organization"
GLOBAL_SCOPE = "global"


@dataclass(frozen=True, slots=True)
class ConfigurationAuthority:
    section: str
    path: str
    scope: str


CONNECTORS = ConfigurationAuthority(
    section="connectors",
    path=CANONICAL_ROUTE_PATHS["config"],
    scope=ORGANIZATION_SCOPE,
)
ACCOUNT_NAMING = ConfigurationAuthority(
    section="sync_policy.account_naming",
    path=CANONICAL_ROUTE_PATHS["sync-account-naming"],
    scope=ORGANIZATION_SCOPE,
)
SYNC_SCOPE = ConfigurationAuthority(
    section="sync_policy.scope",
    path=CANONICAL_ROUTE_PATHS["sync-scope"],
    scope=ORGANIZATION_SCOPE,
)
ROUTING = ConfigurationAuthority(
    section="sync_policy.routing",
    path=CANONICAL_ROUTE_PATHS["sync-department-ou-routing"],
    scope=ORGANIZATION_SCOPE,
)
GROUP_RULES = ConfigurationAuthority(
    section="sync_policy.group_rules",
    path=CANONICAL_ROUTE_PATHS["sync-group-rules"],
    scope=ORGANIZATION_SCOPE,
)
EMPLOYEE_SERVICES = ConfigurationAuthority(
    section="employee_services",
    path=CANONICAL_ROUTE_PATHS["employee-self-service"],
    scope=ORGANIZATION_SCOPE,
)
AUTOMATION = ConfigurationAuthority(
    section="automation",
    path=CANONICAL_ROUTE_PATHS["automation-center"],
    scope=ORGANIZATION_SCOPE,
)
NOTIFICATIONS = ConfigurationAuthority(
    section="automation.notifications",
    path=CANONICAL_ROUTE_PATHS["integrations"],
    scope=ORGANIZATION_SCOPE,
)
DEPLOYMENT = ConfigurationAuthority(
    section="system_settings.deployment",
    path=CANONICAL_ROUTE_PATHS["deployment"],
    scope=GLOBAL_SCOPE,
)
BRANDING = ConfigurationAuthority(
    section="system_settings.branding",
    path=CANONICAL_ROUTE_PATHS["branding"],
    scope=GLOBAL_SCOPE,
)


# This registry is the migration contract for fields that were historically
# editable together on /config. A field must have exactly one canonical owner.
LEGACY_CONFIG_FIELD_AUTHORITIES: dict[str, ConfigurationAuthority] = {
    **{
        field: CONNECTORS
        for field in (
            "source_provider",
            "corpid",
            "agentid",
            "corpsecret",
            "ldap_server",
            "ldap_domain",
            "ldap_username",
            "ldap_password",
            "ldap_port",
            "ldap_use_ssl",
            "ldap_validate_cert",
            "ldap_ca_cert_path",
        )
    },
    "webhook_url": NOTIFICATIONS,
    "default_password": ACCOUNT_NAMING,
    "force_change_password": ACCOUNT_NAMING,
    "password_complexity": ACCOUNT_NAMING,
    "schedule_time": AUTOMATION,
    "retry_interval": AUTOMATION,
    "max_retries": AUTOMATION,
    "schedule_execution_mode": AUTOMATION,
    "group_display_separator": GROUP_RULES,
    "group_recursive_enabled": GROUP_RULES,
    "managed_relation_cleanup_enabled": GROUP_RULES,
    "custom_group_ou_path": GROUP_RULES,
    "user_ou_placement_strategy": ROUTING,
    "source_root_unit_ids": ROUTING,
    "source_root_unit_display_text": ROUTING,
    "directory_root_ou_path": ROUTING,
    "disabled_users_ou_path": ROUTING,
    "soft_excluded_groups": GROUP_RULES,
    "sspr_enabled": EMPLOYEE_SERVICES,
    "sspr_dingtalk_corp_id": EMPLOYEE_SERVICES,
    "sspr_min_password_length": EMPLOYEE_SERVICES,
    "sspr_unlock_account_default": EMPLOYEE_SERVICES,
    "sspr_verification_session_ttl_seconds": EMPLOYEE_SERVICES,
    "web_bind_host": DEPLOYMENT,
    "web_bind_port": DEPLOYMENT,
    "web_public_base_url": DEPLOYMENT,
    "web_session_cookie_secure_mode": DEPLOYMENT,
    "web_trust_proxy_headers": DEPLOYMENT,
    "web_forwarded_allow_ips": DEPLOYMENT,
    "brand_display_name": BRANDING,
    "brand_mark_text": BRANDING,
    "brand_attribution": BRANDING,
}


__all__ = [
    "ConfigurationAuthority",
    "GLOBAL_SCOPE",
    "LEGACY_CONFIG_FIELD_AUTHORITIES",
    "ORGANIZATION_SCOPE",
]
