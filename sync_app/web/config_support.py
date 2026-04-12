from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import FastAPI, Request

from sync_app.core.models import OrganizationRecord, WebAdminUserRecord
from sync_app.web.config_catalog import (
    build_source_unit_catalog as _build_source_unit_catalog,
    build_target_ou_catalog as _build_target_ou_catalog,
)
from sync_app.web.config_submission import (
    apply_config_submission as _apply_config_submission,
    build_config_change_preview as _build_config_change_preview,
    build_config_editable_override as _build_config_editable_override,
    build_config_page_context as _build_config_page_context,
    build_config_submission as _build_config_submission,
    build_current_config_state as _build_current_config_state,
    build_preview_app_config as _build_preview_app_config,
)
from sync_app.web.request_support import RequestSupport


class ConfigSupport:
    def __init__(
        self,
        *,
        app: FastAPI,
        logger: Any,
        request_support: RequestSupport,
        default_brand_display_name: str,
        default_brand_mark_text: str,
        default_brand_attribution: str,
        placement_strategies: dict[str, str],
        build_source_provider_fn: Callable[..., Any],
        build_target_provider_fn: Callable[..., Any],
        normalize_source_root_unit_ids_text: Callable[[str | None], str],
        normalize_ou_path_text: Callable[..., str],
        clean_public_base_url: Callable[[str | None], str],
        to_bool: Callable[[Optional[str], bool], bool],
        split_csv_values: Callable[[str | None], list[str]],
        translate: Callable[..., str],
    ) -> None:
        self.app = app
        self.logger = logger
        self.request_support = request_support
        self.default_brand_display_name = default_brand_display_name
        self.default_brand_mark_text = default_brand_mark_text
        self.default_brand_attribution = default_brand_attribution
        self.placement_strategies = placement_strategies
        self.build_source_provider = build_source_provider_fn
        self.build_target_provider = build_target_provider_fn
        self.normalize_source_root_unit_ids_text = normalize_source_root_unit_ids_text
        self.normalize_ou_path_text = normalize_ou_path_text
        self.clean_public_base_url = clean_public_base_url
        self.to_bool = to_bool
        self.split_csv_values = split_csv_values
        self.translate = translate

    def build_current_config_state(self, request: Request, current_org: OrganizationRecord) -> dict[str, Any]:
        return _build_current_config_state(self, request, current_org)

    def build_config_submission(
        self,
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
        return _build_config_submission(
            self,
            request,
            source_provider=source_provider,
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
            soft_excluded_groups=soft_excluded_groups,
        )

    def build_preview_app_config(self, request: Request, submission: dict[str, Any]) -> AppConfig:
        return _build_preview_app_config(self, request, submission)

    def build_source_unit_catalog(
        self,
        request: Request,
        *,
        source_provider: str = "wecom",
        corpid: str = "",
        agentid: str = "",
        corpsecret: str = "",
    ) -> dict[str, Any]:
        return _build_source_unit_catalog(
            self,
            request,
            source_provider=source_provider,
            corpid=corpid,
            agentid=agentid,
            corpsecret=corpsecret,
        )

    def build_target_ou_catalog(
        self,
        request: Request,
        *,
        ldap_server: str = "",
        ldap_domain: str = "",
        ldap_username: str = "",
        ldap_password: str = "",
        ldap_port: int = 636,
        ldap_use_ssl: Optional[str] = None,
        ldap_validate_cert: Optional[str] = None,
        ldap_ca_cert_path: str = "",
    ) -> dict[str, Any]:
        return _build_target_ou_catalog(
            self,
            request,
            ldap_server=ldap_server,
            ldap_domain=ldap_domain,
            ldap_username=ldap_username,
            ldap_password=ldap_password,
            ldap_port=ldap_port,
            ldap_use_ssl=ldap_use_ssl,
            ldap_validate_cert=ldap_validate_cert,
            ldap_ca_cert_path=ldap_ca_cert_path,
        )

    def build_config_change_preview(self, request: Request, submission: dict[str, Any]) -> dict[str, Any]:
        return _build_config_change_preview(self, request, submission)

    def build_config_editable_override(self, request: Request, submission: dict[str, Any]) -> dict[str, Any]:
        return _build_config_editable_override(self, request, submission)

    def build_config_page_context(
        self,
        request: Request,
        *,
        editable_override: Optional[dict[str, Any]] = None,
        config_change_preview: Optional[dict[str, Any]] = None,
        preview_token: str = "",
    ) -> dict[str, Any]:
        return _build_config_page_context(
            self,
            request,
            editable_override=editable_override,
            config_change_preview=config_change_preview,
            preview_token=preview_token,
        )

    def apply_config_submission(self, request: Request, *, user: WebAdminUserRecord, submission: dict[str, Any]) -> None:
        _apply_config_submission(self, request, user=user, submission=submission)
