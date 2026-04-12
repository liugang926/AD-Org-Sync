from __future__ import annotations

from typing import Any, Callable, Optional

from sync_app.core.models import AccountConfig, AppConfig, LDAPConfig, SourceConnectorConfig
from sync_app.providers.source import normalize_source_provider
from sync_app.storage.config_codec import normalize_org_config_values as _normalize_org_config_values
from sync_app.web.runtime import normalize_secure_cookie_mode


def build_current_config_state_from_sources(
    current_org_values: dict[str, Any],
    *,
    settings_values: dict[str, Any],
    soft_excluded_groups: str,
) -> dict[str, Any]:
    return {
        **current_org_values,
        **settings_values,
        "soft_excluded_groups": soft_excluded_groups,
    }


def normalize_config_submission_values(
    *,
    existing_org_values: dict[str, Any],
    config_path: str,
    source_provider: str = "wecom",
    corpid: str = "",
    agentid: str = "",
    corpsecret: str = "",
    webhook_url: str = "",
    ldap_server: str = "",
    ldap_domain: str = "",
    ldap_username: str = "",
    ldap_password: str = "",
    ldap_port: int = 636,
    ldap_use_ssl: Optional[str] = None,
    ldap_validate_cert: Optional[str] = None,
    ldap_ca_cert_path: str = "",
    default_password: str = "",
    force_change_password: Optional[str] = None,
    password_complexity: str = "strong",
    schedule_time: str = "03:00",
    retry_interval: int = 60,
    max_retries: int = 3,
    group_display_separator: str = "-",
    group_recursive_enabled: Optional[str] = None,
    managed_relation_cleanup_enabled: Optional[str] = None,
    schedule_execution_mode: str = "apply",
    web_bind_host: str = "127.0.0.1",
    web_bind_port: int = 8000,
    web_public_base_url: str = "",
    web_session_cookie_secure_mode: str = "auto",
    web_trust_proxy_headers: Optional[str] = None,
    web_forwarded_allow_ips: str = "127.0.0.1",
    brand_display_name: str = "",
    brand_mark_text: str = "",
    brand_attribution: str = "",
    user_ou_placement_strategy: str = "source_primary_department",
    source_root_unit_ids: str = "",
    directory_root_ou_path: str = "",
    disabled_users_ou_path: str = "Disabled Users",
    custom_group_ou_path: str = "Managed Groups",
    default_brand_display_name: str,
    default_brand_mark_text: str,
    default_brand_attribution: str,
    to_bool: Callable[[Optional[str], bool], bool],
    normalize_source_root_unit_ids_text: Callable[[str | None], str],
    normalize_ou_path_text: Callable[..., str],
    clean_public_base_url: Callable[[str | None], str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized_source_provider = normalize_source_provider(
        source_provider if isinstance(source_provider, str) else None
    )
    existing_values = dict(existing_org_values or {})
    if normalized_source_provider != normalize_source_provider(existing_values.get("source_provider")):
        for provider_specific_field in ("corpid", "agentid", "corpsecret", "webhook_url"):
            existing_values[provider_specific_field] = ""
    normalized_org_values = _normalize_org_config_values(
        {
            "source_provider": normalized_source_provider,
            "corpid": corpid,
            "agentid": agentid,
            "corpsecret": corpsecret,
            "webhook_url": webhook_url,
            "ldap_server": ldap_server,
            "ldap_domain": ldap_domain,
            "ldap_username": ldap_username,
            "ldap_password": ldap_password,
            "ldap_port": ldap_port,
            "ldap_use_ssl": to_bool(ldap_use_ssl, True),
            "ldap_validate_cert": to_bool(ldap_validate_cert, True),
            "ldap_ca_cert_path": ldap_ca_cert_path.strip(),
            "default_password": default_password,
            "force_change_password": to_bool(force_change_password, True),
            "password_complexity": password_complexity,
            "schedule_time": schedule_time,
            "retry_interval": retry_interval,
            "max_retries": max_retries,
        },
        existing=existing_values,
        config_path=config_path,
    )
    normalized_settings = {
        "group_display_separator": group_display_separator,
        "group_recursive_enabled": to_bool(group_recursive_enabled, True),
        "managed_relation_cleanup_enabled": to_bool(managed_relation_cleanup_enabled, False),
        "schedule_execution_mode": "dry_run" if schedule_execution_mode == "dry_run" else "apply",
        "web_bind_host": web_bind_host.strip() or "127.0.0.1",
        "web_bind_port": max(int(web_bind_port or 8000), 1),
        "web_public_base_url": clean_public_base_url(web_public_base_url),
        "web_session_cookie_secure_mode": normalize_secure_cookie_mode(web_session_cookie_secure_mode),
        "web_trust_proxy_headers": to_bool(web_trust_proxy_headers, False),
        "web_forwarded_allow_ips": web_forwarded_allow_ips.strip() or "127.0.0.1",
        "brand_display_name": str(brand_display_name or "").strip() or default_brand_display_name,
        "brand_mark_text": str(brand_mark_text or "").strip() or default_brand_mark_text,
        "brand_attribution": str(brand_attribution or "").strip() or default_brand_attribution,
        "user_ou_placement_strategy": user_ou_placement_strategy,
        "source_root_unit_ids": normalize_source_root_unit_ids_text(source_root_unit_ids),
        "directory_root_ou_path": normalize_ou_path_text(directory_root_ou_path),
        "disabled_users_ou_path": normalize_ou_path_text(disabled_users_ou_path, default="Disabled Users"),
        "custom_group_ou_path": normalize_ou_path_text(custom_group_ou_path, default="Managed Groups"),
    }
    return normalized_org_values, normalized_settings


def build_preview_app_config_from_values(
    *,
    current_config: AppConfig,
    org_values: dict[str, Any],
    config_path: str,
) -> AppConfig:
    return AppConfig(
        source_connector=SourceConnectorConfig(
            corpid=str(org_values.get("corpid") or ""),
            corpsecret=str(org_values.get("corpsecret") or ""),
            agentid=str(org_values.get("agentid") or "") or None,
        ),
        ldap=LDAPConfig(
            server=str(org_values.get("ldap_server") or ""),
            domain=str(org_values.get("ldap_domain") or ""),
            username=str(org_values.get("ldap_username") or ""),
            password=str(org_values.get("ldap_password") or ""),
            use_ssl=bool(org_values.get("ldap_use_ssl", True)),
            port=int(org_values.get("ldap_port") or 636),
            validate_cert=bool(org_values.get("ldap_validate_cert", True)),
            ca_cert_path=str(org_values.get("ldap_ca_cert_path") or ""),
        ),
        domain=str(org_values.get("ldap_domain") or current_config.domain or ""),
        source_provider=str(org_values.get("source_provider") or current_config.source_provider or "wecom"),
        account=AccountConfig(
            default_password=str(org_values.get("default_password") or ""),
            force_change_password=bool(org_values.get("force_change_password", True)),
            password_complexity=str(org_values.get("password_complexity") or "strong"),
        ),
        exclude_departments=list(current_config.exclude_departments),
        exclude_accounts=list(current_config.exclude_accounts),
        webhook_url=str(org_values.get("webhook_url") or ""),
        config_path=config_path,
    )
