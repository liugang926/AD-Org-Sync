from __future__ import annotations

from typing import Any, Optional

from fastapi import Request

from sync_app.core.models import AppConfig, OrganizationRecord, WebAdminUserRecord
from sync_app.services.typed_settings import BrandingSettings, DirectoryUiSettings, WebRuntimeSettings
from sync_app.web.config_domain import (
    build_current_config_state_from_sources,
    build_preview_app_config_from_values,
    normalize_config_submission_values,
)
from sync_app.providers.source import normalize_source_provider
from sync_app.web.app_state import get_web_repositories, get_web_runtime_state
from sync_app.web.config_persistence import apply_config_submission as _apply_config_submission
from sync_app.web.config_preview import (
    build_config_change_preview as _build_config_change_preview,
    build_config_editable_override as _build_config_editable_override,
    build_config_page_context as _build_config_page_context,
)


def build_current_config_state(support: Any, request: Request, current_org: OrganizationRecord) -> dict[str, Any]:
    repositories = get_web_repositories(request)
    runtime_state = get_web_runtime_state(request)
    current_org_config_path = current_org.config_path or runtime_state.config_path
    current_org_values = repositories.org_config_repo.get_raw_config(
        current_org.org_id,
        config_path=current_org_config_path,
    )
    directory_ui_settings = DirectoryUiSettings.load(
        repositories.settings_repo,
        org_id=current_org.org_id,
    )
    web_runtime_settings = WebRuntimeSettings.load(repositories.settings_repo)
    branding_settings = BrandingSettings.load(
        repositories.settings_repo,
        default_display_name=support.default_brand_display_name,
        default_mark_text=support.default_brand_mark_text,
        default_attribution=support.default_brand_attribution,
    )
    settings_values = {
        "group_display_separator": directory_ui_settings.group_display_separator,
        "group_recursive_enabled": directory_ui_settings.group_recursive_enabled,
        "managed_relation_cleanup_enabled": directory_ui_settings.managed_relation_cleanup_enabled,
        "schedule_execution_mode": directory_ui_settings.schedule_execution_mode,
        "web_bind_host": web_runtime_settings.bind_host,
        "web_bind_port": web_runtime_settings.bind_port,
        "web_public_base_url": web_runtime_settings.public_base_url,
        "web_session_cookie_secure_mode": web_runtime_settings.session_cookie_secure_mode,
        "web_trust_proxy_headers": web_runtime_settings.trust_proxy_headers,
        "web_forwarded_allow_ips": web_runtime_settings.forwarded_allow_ips,
        "brand_display_name": branding_settings.brand_display_name,
        "brand_mark_text": branding_settings.brand_mark_text,
        "brand_attribution": branding_settings.brand_attribution,
        "user_ou_placement_strategy": directory_ui_settings.user_ou_placement_strategy,
        "source_root_unit_ids": directory_ui_settings.source_root_unit_ids,
        "source_root_unit_display_text": directory_ui_settings.source_root_unit_display_text,
        "directory_root_ou_path": directory_ui_settings.directory_root_ou_path,
        "disabled_users_ou_path": directory_ui_settings.disabled_users_ou_path,
        "custom_group_ou_path": directory_ui_settings.custom_group_ou_path,
    }
    soft_excluded_groups = support.request_support.normalize_soft_excluded_groups_text(
        "\n".join(
            repositories.exclusion_repo.list_soft_excluded_group_names(
                enabled_only=False,
                org_id=current_org.org_id,
            )
        )
    )
    return build_current_config_state_from_sources(
        current_org_values,
        settings_values=settings_values,
        soft_excluded_groups=soft_excluded_groups,
    )


def build_config_submission(
    support: Any,
    request: Request,
    *,
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
    source_root_unit_display_text: str = "",
    directory_root_ou_path: str = "",
    disabled_users_ou_path: str = "Disabled Users",
    custom_group_ou_path: str = "Managed Groups",
    soft_excluded_groups: str = "",
) -> dict[str, Any]:
    current_org = support.request_support.get_current_org(request)
    repositories = get_web_repositories(request)
    runtime_state = get_web_runtime_state(request)
    current_org_config_path = current_org.config_path or runtime_state.config_path
    current_org_values = repositories.org_config_repo.get_raw_config(
        current_org.org_id,
        config_path=current_org_config_path,
    )
    if user_ou_placement_strategy not in support.placement_strategies:
        user_ou_placement_strategy = "source_primary_department"
    if password_complexity not in {"basic", "medium", "strong"}:
        password_complexity = "strong"
    normalized_org_values, normalized_settings = normalize_config_submission_values(
        existing_org_values=current_org_values,
        config_path=current_org_config_path,
        source_provider=normalize_source_provider(
            source_provider if isinstance(source_provider, str) else None
        ),
        corpid=corpid,
        agentid=agentid,
        corpsecret=corpsecret,
        webhook_url=webhook_url,
        ldap_server=ldap_server,
        ldap_domain=ldap_domain,
        ldap_username=ldap_username,
        ldap_password=ldap_password,
        ldap_port=ldap_port,
        ldap_use_ssl=ldap_use_ssl,
        ldap_validate_cert=ldap_validate_cert,
        ldap_ca_cert_path=ldap_ca_cert_path,
        default_password=default_password,
        force_change_password=force_change_password,
        password_complexity=password_complexity,
        schedule_time=schedule_time,
        retry_interval=retry_interval,
        max_retries=max_retries,
        group_display_separator=group_display_separator,
        group_recursive_enabled=group_recursive_enabled,
        managed_relation_cleanup_enabled=managed_relation_cleanup_enabled,
        schedule_execution_mode=schedule_execution_mode,
        web_bind_host=web_bind_host,
        web_bind_port=web_bind_port,
        web_public_base_url=web_public_base_url,
        web_session_cookie_secure_mode=web_session_cookie_secure_mode,
        web_trust_proxy_headers=web_trust_proxy_headers,
        web_forwarded_allow_ips=web_forwarded_allow_ips,
        brand_display_name=brand_display_name,
        brand_mark_text=brand_mark_text,
        brand_attribution=brand_attribution,
        user_ou_placement_strategy=user_ou_placement_strategy,
        source_root_unit_ids=source_root_unit_ids,
        source_root_unit_display_text=source_root_unit_display_text,
        directory_root_ou_path=directory_root_ou_path,
        disabled_users_ou_path=disabled_users_ou_path,
        custom_group_ou_path=custom_group_ou_path,
        default_brand_display_name=support.default_brand_display_name,
        default_brand_mark_text=support.default_brand_mark_text,
        default_brand_attribution=support.default_brand_attribution,
        to_bool=support.to_bool,
        normalize_source_root_unit_ids_text=support.normalize_source_root_unit_ids_text,
        normalize_ou_path_text=support.normalize_ou_path_text,
        clean_public_base_url=support.clean_public_base_url,
    )
    return {
        "org_id": current_org.org_id,
        "legacy_config_path": current_org_config_path,
        "org_values": normalized_org_values,
        "settings_values": normalized_settings,
        "soft_excluded_groups": support.request_support.normalize_soft_excluded_groups_text(soft_excluded_groups),
    }


def build_preview_app_config(support: Any, request: Request, submission: dict[str, Any]) -> AppConfig:
    current_org = support.request_support.get_current_org(request)
    current_config = get_web_repositories(request).org_config_repo.get_app_config(
        current_org.org_id,
        config_path=submission["legacy_config_path"],
    )
    return build_preview_app_config_from_values(
        current_config=current_config,
        org_values=submission["org_values"],
        config_path=submission["legacy_config_path"],
    )


def build_config_change_preview(support: Any, request: Request, submission: dict[str, Any]) -> dict[str, Any]:
    return _build_config_change_preview(support, request, submission)


def build_config_editable_override(support: Any, request: Request, submission: dict[str, Any]) -> dict[str, Any]:
    return _build_config_editable_override(support, request, submission)


def build_config_page_context(
    support: Any,
    request: Request,
    *,
    editable_override: Optional[dict[str, Any]] = None,
    config_change_preview: Optional[dict[str, Any]] = None,
    preview_token: str = "",
) -> dict[str, Any]:
    return _build_config_page_context(
        support,
        request,
        editable_override=editable_override,
        config_change_preview=config_change_preview,
        preview_token=preview_token,
    )


def apply_config_submission(
    support: Any,
    request: Request,
    *,
    user: WebAdminUserRecord,
    submission: dict[str, Any],
) -> None:
    _apply_config_submission(support, request, user=user, submission=submission)
