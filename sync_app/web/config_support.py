from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import FastAPI, Request

from sync_app.core.models import (
    AccountConfig,
    AppConfig,
    LDAPConfig,
    OrganizationRecord,
    SourceConnectorConfig,
    WebAdminUserRecord,
)
from sync_app.providers.source import (
    build_source_provider,
    get_source_provider_display_name,
    get_source_provider_schema,
    list_source_provider_options,
    list_source_provider_schemas,
    normalize_source_provider,
)
from sync_app.providers.target import build_target_provider
from sync_app.storage.config_codec import normalize_org_config_values as _normalize_org_config_values
from sync_app.web.request_support import RequestSupport
from sync_app.web.runtime import normalize_secure_cookie_mode, resolve_web_runtime_settings, web_runtime_requires_restart


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

    def build_source_provider_field_models(
        self,
        editable: dict[str, Any],
        fields: tuple[Any, ...],
    ) -> list[dict[str, Any]]:
        field_models: list[dict[str, Any]] = []
        for field in fields:
            configured = bool(editable.get(f"{field.name}_configured")) if field.secret else bool(editable.get(field.name))
            placeholder = field.placeholder
            if field.secret:
                placeholder = "Leave blank to keep current" if configured else (field.placeholder or "Enter value")
            field_models.append(
                {
                    "name": field.name,
                    "label": field.label,
                    "value": "" if field.secret else editable.get(field.name, ""),
                    "type": field.input_type,
                    "help_text": field.help_text,
                    "placeholder": placeholder,
                    "required": field.required,
                    "configured": configured,
                    "class_name": "field-span-full" if field.width == "full" else "",
                    "autocomplete": field.autocomplete,
                    "secret": field.secret,
                }
            )
        return field_models

    def build_source_provider_fields(self, editable: dict[str, Any]) -> list[dict[str, Any]]:
        provider_schema = get_source_provider_schema(editable.get("source_provider"))
        return self.build_source_provider_field_models(
            editable,
            (*provider_schema.connection_fields, *provider_schema.notification_fields),
        )

    def build_source_provider_ui_catalog(self, ui_language: str) -> dict[str, Any]:
        catalog: dict[str, Any] = {}
        for schema in list_source_provider_schemas(include_unimplemented=True):
            localized_provider_name = self.request_support.translate_text(ui_language, schema.display_name)
            description = self.request_support.translate_text(
                ui_language,
                schema.implementation_status or schema.description or "",
            )
            field_catalog: dict[str, Any] = {}
            for field in (*schema.connection_fields, *schema.notification_fields):
                field_catalog[field.name] = {
                    "label": self.request_support.translate_text(ui_language, field.label),
                    "helpText": self.request_support.translate_text(ui_language, field.help_text),
                    "placeholder": self.request_support.translate_text(ui_language, field.placeholder),
                    "required": bool(field.required),
                    "secret": bool(field.secret),
                }
            catalog[schema.provider_id] = {
                "displayName": localized_provider_name,
                "description": description,
                "pageTitle": self.request_support.translate_text(
                    ui_language,
                    "{provider} Connector Configuration",
                    provider=localized_provider_name,
                ),
                "pageSummary": self.request_support.translate_text(
                    ui_language,
                    "This organization currently uses {provider} as its source provider. This is the shared organization settings page, so LDAP, password policy, runtime, and web deployment sections remain consistent across providers.",
                    provider=localized_provider_name,
                ),
                "connectorTitle": self.request_support.translate_text(
                    ui_language,
                    "{provider} Source Connector",
                    provider=localized_provider_name,
                ),
                "connectorDescription": self.request_support.translate_text(
                    ui_language,
                    "Enter the credentials required by {provider}. Notification delivery is optional.",
                    provider=localized_provider_name,
                ),
                "sourceGuidance": self.request_support.translate_text(
                    ui_language,
                    "Select the source provider and complete the credentials required by {provider}.",
                    provider=localized_provider_name,
                ),
                "fields": field_catalog,
            }
        return catalog

    def build_config_preview_groups(self, provider_schema) -> tuple[tuple[str, tuple[tuple[str, str, str], ...]], ...]:
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

    def build_current_config_state(self, request: Request, current_org: OrganizationRecord) -> dict[str, Any]:
        current_org_config_path = current_org.config_path or request.app.state.config_path
        current_org_values = request.app.state.org_config_repo.get_raw_config(
            current_org.org_id,
            config_path=current_org_config_path,
        )
        return {
            **current_org_values,
            "group_display_separator": request.app.state.settings_repo.get_value(
                "group_display_separator",
                "-",
                org_id=current_org.org_id,
            ),
            "group_recursive_enabled": request.app.state.settings_repo.get_bool(
                "group_recursive_enabled",
                True,
                org_id=current_org.org_id,
            ),
            "managed_relation_cleanup_enabled": request.app.state.settings_repo.get_bool(
                "managed_relation_cleanup_enabled",
                False,
                org_id=current_org.org_id,
            ),
            "schedule_execution_mode": request.app.state.settings_repo.get_value(
                "schedule_execution_mode",
                "apply",
                org_id=current_org.org_id,
            ),
            "web_bind_host": request.app.state.settings_repo.get_value("web_bind_host", "127.0.0.1"),
            "web_bind_port": request.app.state.settings_repo.get_int("web_bind_port", 8000),
            "web_public_base_url": request.app.state.settings_repo.get_value("web_public_base_url", ""),
            "web_session_cookie_secure_mode": request.app.state.settings_repo.get_value(
                "web_session_cookie_secure_mode",
                "auto",
            ),
            "web_trust_proxy_headers": request.app.state.settings_repo.get_bool("web_trust_proxy_headers", False),
            "web_forwarded_allow_ips": request.app.state.settings_repo.get_value("web_forwarded_allow_ips", "127.0.0.1"),
            "brand_display_name": request.app.state.settings_repo.get_value(
                "brand_display_name",
                self.default_brand_display_name,
            ),
            "brand_mark_text": request.app.state.settings_repo.get_value(
                "brand_mark_text",
                self.default_brand_mark_text,
            ),
            "brand_attribution": request.app.state.settings_repo.get_value(
                "brand_attribution",
                self.default_brand_attribution,
            ),
            "user_ou_placement_strategy": request.app.state.settings_repo.get_value(
                "user_ou_placement_strategy",
                "source_primary_department",
                org_id=current_org.org_id,
            ),
            "source_root_unit_ids": request.app.state.settings_repo.get_value(
                "source_root_unit_ids",
                "",
                org_id=current_org.org_id,
            ),
            "directory_root_ou_path": request.app.state.settings_repo.get_value(
                "directory_root_ou_path",
                "",
                org_id=current_org.org_id,
            ),
            "disabled_users_ou_path": request.app.state.settings_repo.get_value(
                "disabled_users_ou_path",
                "Disabled Users",
                org_id=current_org.org_id,
            ),
            "custom_group_ou_path": request.app.state.settings_repo.get_value(
                "custom_group_ou_path",
                "Managed Groups",
                org_id=current_org.org_id,
            ),
            "soft_excluded_groups": self.request_support.normalize_soft_excluded_groups_text(
                "\n".join(
                    request.app.state.exclusion_repo.list_soft_excluded_group_names(
                        enabled_only=False,
                        org_id=current_org.org_id,
                    )
                )
            ),
        }

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
        directory_root_ou_path: str = "",
        disabled_users_ou_path: str = "Disabled Users",
        custom_group_ou_path: str = "Managed Groups",
        soft_excluded_groups: str = "",
    ) -> dict[str, Any]:
        current_org = self.request_support.get_current_org(request)
        current_org_config_path = current_org.config_path or request.app.state.config_path
        current_org_values = request.app.state.org_config_repo.get_raw_config(
            current_org.org_id,
            config_path=current_org_config_path,
        )
        if user_ou_placement_strategy not in self.placement_strategies:
            user_ou_placement_strategy = "source_primary_department"
        if password_complexity not in {"basic", "medium", "strong"}:
            password_complexity = "strong"
        normalized_org_values = _normalize_org_config_values(
            {
                "source_provider": normalize_source_provider(
                    source_provider if isinstance(source_provider, str) else None
                ),
                "corpid": corpid,
                "agentid": agentid,
                "corpsecret": corpsecret,
                "webhook_url": webhook_url,
                "ldap_server": ldap_server,
                "ldap_domain": ldap_domain,
                "ldap_username": ldap_username,
                "ldap_password": ldap_password,
                "ldap_port": ldap_port,
                "ldap_use_ssl": self.to_bool(ldap_use_ssl, True),
                "ldap_validate_cert": self.to_bool(ldap_validate_cert, True),
                "ldap_ca_cert_path": ldap_ca_cert_path.strip(),
                "default_password": default_password,
                "force_change_password": self.to_bool(force_change_password, True),
                "password_complexity": password_complexity,
                "schedule_time": schedule_time,
                "retry_interval": retry_interval,
                "max_retries": max_retries,
            },
            existing=current_org_values,
            config_path=current_org_config_path,
        )
        normalized_settings = {
            "group_display_separator": group_display_separator,
            "group_recursive_enabled": self.to_bool(group_recursive_enabled, True),
            "managed_relation_cleanup_enabled": self.to_bool(managed_relation_cleanup_enabled, False),
            "schedule_execution_mode": "dry_run" if schedule_execution_mode == "dry_run" else "apply",
            "web_bind_host": web_bind_host.strip() or "127.0.0.1",
            "web_bind_port": max(int(web_bind_port or 8000), 1),
            "web_public_base_url": self.clean_public_base_url(web_public_base_url),
            "web_session_cookie_secure_mode": normalize_secure_cookie_mode(web_session_cookie_secure_mode),
            "web_trust_proxy_headers": self.to_bool(web_trust_proxy_headers, False),
            "web_forwarded_allow_ips": web_forwarded_allow_ips.strip() or "127.0.0.1",
            "brand_display_name": str(brand_display_name or "").strip() or self.default_brand_display_name,
            "brand_mark_text": str(brand_mark_text or "").strip() or self.default_brand_mark_text,
            "brand_attribution": str(brand_attribution or "").strip() or self.default_brand_attribution,
            "user_ou_placement_strategy": user_ou_placement_strategy,
            "source_root_unit_ids": self.normalize_source_root_unit_ids_text(source_root_unit_ids),
            "directory_root_ou_path": self.normalize_ou_path_text(directory_root_ou_path),
            "disabled_users_ou_path": self.normalize_ou_path_text(disabled_users_ou_path, default="Disabled Users"),
            "custom_group_ou_path": self.normalize_ou_path_text(custom_group_ou_path, default="Managed Groups"),
        }
        return {
            "org_id": current_org.org_id,
            "legacy_config_path": current_org_config_path,
            "org_values": normalized_org_values,
            "settings_values": normalized_settings,
            "soft_excluded_groups": self.request_support.normalize_soft_excluded_groups_text(soft_excluded_groups),
        }

    def build_preview_app_config(self, request: Request, submission: dict[str, Any]) -> AppConfig:
        current_org = self.request_support.get_current_org(request)
        current_config = request.app.state.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=submission["legacy_config_path"],
        )
        org_values = submission["org_values"]
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
            config_path=submission["legacy_config_path"],
        )

    def build_source_unit_catalog(
        self,
        request: Request,
        *,
        source_provider: str = "wecom",
        corpid: str = "",
        agentid: str = "",
        corpsecret: str = "",
    ) -> dict[str, Any]:
        submission = self.build_config_submission(
            request,
            source_provider=source_provider,
            corpid=corpid,
            agentid=agentid,
            corpsecret=corpsecret,
        )
        preview_config = self.build_preview_app_config(request, submission)
        provider_schema = get_source_provider_schema(preview_config.source_provider)
        missing_fields = [
            field.label
            for field in provider_schema.connection_fields
            if field.required and not str(getattr(preview_config.source_connector, field.name, "") or "").strip()
        ]
        if missing_fields:
            return {
                "ok": False,
                "error": self.translate(
                    "Complete the required source connector fields first: {fields}",
                    self.request_support.get_ui_language(request),
                    fields=", ".join(missing_fields),
                ),
            }
        try:
            source_provider_client = self.build_source_provider(app_config=preview_config, logger=self.logger)
            try:
                departments = source_provider_client.list_departments()
            finally:
                source_provider_client.close()
        except Exception as exc:
            self.logger.warning("failed to load source unit catalog: %s", exc)
            return {
                "ok": False,
                "error": str(exc) or self.translate("Unable to load source departments.", self.request_support.get_ui_language(request)),
            }

        dept_tree = {item.department_id: item for item in departments if item.department_id}
        for dept_id in list(dept_tree):
            path_names: list[str] = []
            path_ids: list[int] = []
            current_id = dept_id
            seen: set[int] = set()
            while current_id and current_id in dept_tree and current_id not in seen:
                seen.add(current_id)
                current_node = dept_tree[current_id]
                path_names.insert(0, current_node.name)
                path_ids.insert(0, current_node.department_id)
                current_id = current_node.parent_id
            dept_tree[dept_id].set_hierarchy(path_names, path_ids)

        selected_ids = {
            candidate
            for candidate in self.split_csv_values(submission["settings_values"].get("source_root_unit_ids", ""))
            if candidate.isdigit()
        }
        items = [
            {
                "department_id": str(node.department_id),
                "name": node.name,
                "path_display": " / ".join(node.path or [node.name]),
                "level": max(len(node.path) - 1, 0),
                "selected": str(node.department_id) in selected_ids,
            }
            for node in sorted(
                dept_tree.values(),
                key=lambda item: (len(item.path_ids), [str(part).lower() for part in item.path or [item.name]], item.department_id),
            )
        ]
        return {
            "ok": True,
            "provider": get_source_provider_display_name(preview_config.source_provider),
            "items": items,
        }

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
        submission = self.build_config_submission(
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
        preview_config = self.build_preview_app_config(request, submission)
        required_values = {
            "LDAP Server": preview_config.ldap.server,
            "LDAP Domain": preview_config.ldap.domain,
            "LDAP Username": preview_config.ldap.username,
            "LDAP Password": preview_config.ldap.password,
        }
        missing_fields = [label for label, value in required_values.items() if not str(value or "").strip()]
        if missing_fields:
            return {
                "ok": False,
                "error": self.translate(
                    "Complete the required LDAP fields first: {fields}",
                    self.request_support.get_ui_language(request),
                    fields=", ".join(missing_fields),
                ),
            }
        try:
            target_provider = self.build_target_provider(
                server=preview_config.ldap.server,
                domain=preview_config.ldap.domain,
                username=preview_config.ldap.username,
                password=preview_config.ldap.password,
                use_ssl=preview_config.ldap.use_ssl,
                port=preview_config.ldap.port,
                validate_cert=preview_config.ldap.validate_cert,
                ca_cert_path=preview_config.ldap.ca_cert_path,
            )
            try:
                organizational_units = target_provider.list_organizational_units()
            finally:
                close_fn = getattr(getattr(target_provider, "client", None), "close", None)
                if callable(close_fn):
                    close_fn()
        except Exception as exc:
            self.logger.warning("failed to load target OU catalog: %s", exc)
            return {
                "ok": False,
                "error": str(exc) or self.translate("Unable to load AD OU list.", self.request_support.get_ui_language(request)),
            }

        ui_language = self.request_support.get_ui_language(request)
        items = [
            {
                "name": str(item.get("name") or ""),
                "dn": str(item.get("dn") or ""),
                "guid": str(item.get("guid") or ""),
                "path": list(item.get("path") or []),
                "path_value": "/".join(item.get("path") or []),
                "path_display": " / ".join(item.get("path") or []) or self.translate("Domain Root", ui_language),
                "level": max(len(item.get("path") or []), 0),
            }
            for item in organizational_units
        ]
        return {
            "ok": True,
            "provider": "AD / LDAPS",
            "base_dn": preview_config.ldap.domain,
            "items": items,
        }

    def _normalize_config_change_choice_key(self, value: Any) -> str:
        return str(value or "").strip().lower()

    def _format_secret_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        if not value:
            return "Not configured", True
        if previous_value and previous_value != value:
            return "Updated", True
        return "Configured", True

    def _format_bool_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        return ("Enabled" if bool(value) else "Disabled"), True

    def _format_number_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        return str(value), False

    def _format_source_provider_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        return self.request_support.source_provider_label(value), False

    def _format_password_complexity_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        return {
            "strong": "Strong",
            "medium": "Medium",
            "basic": "Basic",
        }.get(self._normalize_config_change_choice_key(value), str(value or "-")), True

    def _format_schedule_execution_mode_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        return ("Dry Run" if self._normalize_config_change_choice_key(value) == "dry_run" else "Apply"), True

    def _format_placement_strategy_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        return self.placement_strategies.get(str(value or ""), str(value or "-")), True

    def _format_secure_cookie_mode_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        return {
            "auto": "auto",
            "always": "always",
            "never": "never",
        }.get(self._normalize_config_change_choice_key(value), str(value or "-")), False

    def _format_group_separator_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        return ("Space", True) if str(value or "") == " " else (str(value or "-"), False)

    def _format_source_root_units_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        normalized_items = self.split_csv_values(str(value or ""))
        if not normalized_items:
            return "All departments", True
        return ", ".join(normalized_items), False

    def _format_ou_path_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        normalized_value = str(value or "").strip()
        if not normalized_value:
            return "Domain root", True
        return normalized_value, False

    def _format_multiline_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        normalized_lines = [line.strip() for line in str(value or "").splitlines() if line.strip()]
        if not normalized_lines:
            return "None", True
        return ", ".join(normalized_lines), False

    def _format_default_config_change_value(self, value: Any, previous_value: Any = None) -> tuple[str, bool]:
        normalized_value = str(value or "").strip()
        if not normalized_value:
            return "Not set", True
        return normalized_value, False

    def format_config_change_value(
        self,
        field_name: str,
        field_type: str,
        value: Any,
        *,
        previous_value: Any = None,
    ) -> tuple[str, bool]:
        del field_name
        formatter = {
            "secret": self._format_secret_config_change_value,
            "bool": self._format_bool_config_change_value,
            "number": self._format_number_config_change_value,
            "source_provider": self._format_source_provider_config_change_value,
            "password_complexity": self._format_password_complexity_config_change_value,
            "schedule_execution_mode": self._format_schedule_execution_mode_config_change_value,
            "placement_strategy": self._format_placement_strategy_config_change_value,
            "secure_cookie_mode": self._format_secure_cookie_mode_config_change_value,
            "group_separator": self._format_group_separator_config_change_value,
            "source_root_units": self._format_source_root_units_config_change_value,
            "ou_path": self._format_ou_path_config_change_value,
            "multiline": self._format_multiline_config_change_value,
        }.get(field_type, self._format_default_config_change_value)
        return formatter(value, previous_value)

    def build_config_change_preview(self, request: Request, submission: dict[str, Any]) -> dict[str, Any]:
        current_org = self.request_support.get_current_org(request)
        current_state = self.build_current_config_state(request, current_org)
        proposed_state = {
            **submission["org_values"],
            **submission["settings_values"],
            "soft_excluded_groups": submission["soft_excluded_groups"],
        }
        groups: list[dict[str, Any]] = []
        changed_count = 0
        provider_schema = get_source_provider_schema(submission["org_values"].get("source_provider"))
        for group_title, fields in self.build_config_preview_groups(provider_schema):
            group_changes: list[dict[str, Any]] = []
            for field_name, label, field_type in fields:
                current_value = current_state.get(field_name)
                proposed_value = proposed_state.get(field_name)
                if current_value == proposed_value:
                    continue
                before_display, before_translate = self.format_config_change_value(
                    field_name,
                    field_type,
                    current_value,
                )
                after_display, after_translate = self.format_config_change_value(
                    field_name,
                    field_type,
                    proposed_value,
                    previous_value=current_value,
                )
                group_changes.append(
                    {
                        "field_name": field_name,
                        "label": label,
                        "before": before_display,
                        "after": after_display,
                        "translate_before": before_translate,
                        "translate_after": after_translate,
                    }
                )
            if group_changes:
                groups.append({"title": group_title, "changes": group_changes})
                changed_count += len(group_changes)

        proposed_runtime_settings = resolve_web_runtime_settings(
            request.app.state.settings_repo,
            bind_host=str(submission["settings_values"]["web_bind_host"]),
            bind_port=int(submission["settings_values"]["web_bind_port"]),
            public_base_url=str(submission["settings_values"]["web_public_base_url"]),
            session_cookie_secure_mode=str(submission["settings_values"]["web_session_cookie_secure_mode"]),
            trust_proxy_headers=bool(submission["settings_values"]["web_trust_proxy_headers"]),
            forwarded_allow_ips=str(submission["settings_values"]["web_forwarded_allow_ips"]),
        )
        return {
            "groups": groups,
            "changed_count": changed_count,
            "restart_required": web_runtime_requires_restart(
                request.app.state.web_runtime_settings,
                proposed_runtime_settings,
            ),
        }

    def build_config_editable_override(self, request: Request, submission: dict[str, Any]) -> dict[str, Any]:
        current_org = self.request_support.get_current_org(request)
        editable = request.app.state.org_config_repo.get_editable_config(
            current_org.org_id,
            config_path=self.request_support.get_org_config_path(request),
        )
        editable.update(
            {
                "source_provider": submission["org_values"]["source_provider"],
                "corpid": submission["org_values"]["corpid"],
                "agentid": submission["org_values"]["agentid"],
                "corpsecret": "",
                "corpsecret_configured": bool(submission["org_values"]["corpsecret"]),
                "webhook_url": "",
                "webhook_url_configured": bool(submission["org_values"]["webhook_url"]),
                "ldap_server": submission["org_values"]["ldap_server"],
                "ldap_domain": submission["org_values"]["ldap_domain"],
                "ldap_username": submission["org_values"]["ldap_username"],
                "ldap_password": "",
                "ldap_password_configured": bool(submission["org_values"]["ldap_password"]),
                "ldap_port": submission["org_values"]["ldap_port"],
                "ldap_use_ssl": submission["org_values"]["ldap_use_ssl"],
                "ldap_validate_cert": submission["org_values"]["ldap_validate_cert"],
                "ldap_ca_cert_path": submission["org_values"]["ldap_ca_cert_path"],
                "default_password": "",
                "default_password_configured": bool(submission["org_values"]["default_password"]),
                "force_change_password": submission["org_values"]["force_change_password"],
                "password_complexity": submission["org_values"]["password_complexity"],
                "schedule_time": submission["org_values"]["schedule_time"],
                "retry_interval": submission["org_values"]["retry_interval"],
                "max_retries": submission["org_values"]["max_retries"],
                "protected_accounts": list(submission["org_values"]["exclude_accounts"]),
                "group_display_separator": submission["settings_values"]["group_display_separator"],
                "group_recursive_enabled": submission["settings_values"]["group_recursive_enabled"],
                "managed_relation_cleanup_enabled": submission["settings_values"]["managed_relation_cleanup_enabled"],
                "schedule_execution_mode": submission["settings_values"]["schedule_execution_mode"],
                "web_bind_host": submission["settings_values"]["web_bind_host"],
                "web_bind_port": submission["settings_values"]["web_bind_port"],
                "web_public_base_url": submission["settings_values"]["web_public_base_url"],
                "web_session_cookie_secure_mode": submission["settings_values"]["web_session_cookie_secure_mode"],
                "web_trust_proxy_headers": submission["settings_values"]["web_trust_proxy_headers"],
                "web_forwarded_allow_ips": submission["settings_values"]["web_forwarded_allow_ips"],
                "brand_display_name": submission["settings_values"]["brand_display_name"],
                "brand_mark_text": submission["settings_values"]["brand_mark_text"],
                "brand_attribution": submission["settings_values"]["brand_attribution"],
                "user_ou_placement_strategy": submission["settings_values"]["user_ou_placement_strategy"],
                "source_root_unit_ids": submission["settings_values"]["source_root_unit_ids"],
                "directory_root_ou_path": submission["settings_values"]["directory_root_ou_path"],
                "disabled_users_ou_path": submission["settings_values"]["disabled_users_ou_path"],
                "custom_group_ou_path": submission["settings_values"]["custom_group_ou_path"],
                "soft_excluded_groups": submission["soft_excluded_groups"],
            }
        )
        return editable

    def build_config_page_context(
        self,
        request: Request,
        *,
        editable_override: Optional[dict[str, Any]] = None,
        config_change_preview: Optional[dict[str, Any]] = None,
        preview_token: str = "",
    ) -> dict[str, Any]:
        current_org = self.request_support.get_current_org(request)
        editable = editable_override or request.app.state.org_config_repo.get_editable_config(
            current_org.org_id,
            config_path=self.request_support.get_org_config_path(request),
        )
        if "protected_accounts" not in editable:
            effective_config = request.app.state.org_config_repo.get_app_config(
                current_org.org_id,
                config_path=self.request_support.get_org_config_path(request),
            )
            editable["protected_accounts"] = list(effective_config.exclude_accounts)
        editable.setdefault(
            "brand_display_name",
            request.app.state.settings_repo.get_value("brand_display_name", self.default_brand_display_name),
        )
        editable.setdefault(
            "brand_mark_text",
            request.app.state.settings_repo.get_value("brand_mark_text", self.default_brand_mark_text),
        )
        editable.setdefault(
            "brand_attribution",
            request.app.state.settings_repo.get_value("brand_attribution", self.default_brand_attribution),
        )
        editable.setdefault(
            "source_root_unit_ids",
            request.app.state.settings_repo.get_value("source_root_unit_ids", "", org_id=current_org.org_id),
        )
        editable.setdefault(
            "directory_root_ou_path",
            request.app.state.settings_repo.get_value("directory_root_ou_path", "", org_id=current_org.org_id),
        )
        editable.setdefault(
            "disabled_users_ou_path",
            request.app.state.settings_repo.get_value("disabled_users_ou_path", "Disabled Users", org_id=current_org.org_id),
        )
        editable.setdefault(
            "custom_group_ou_path",
            request.app.state.settings_repo.get_value("custom_group_ou_path", "Managed Groups", org_id=current_org.org_id),
        )
        current_source_provider = normalize_source_provider(editable.get("source_provider"))
        provider_schema = get_source_provider_schema(current_source_provider)
        source_provider_name = self.request_support.source_provider_label(current_source_provider)
        source_provider_options = list_source_provider_options(include_unimplemented=True)
        source_provider_ui_catalog = self.build_source_provider_ui_catalog(self.request_support.get_ui_language(request))
        protected_rules = request.app.state.exclusion_repo.list_rules(
            rule_type="protect",
            protection_level="hard",
            org_id=current_org.org_id,
        )
        return {
            "page": "config",
            "title": f"{source_provider_name} Configuration",
            "editable": editable,
            "current_org": current_org,
            "source_provider_name": source_provider_name,
            "source_provider_options": source_provider_options,
            "source_provider_schema": provider_schema,
            "source_provider_ui_catalog": source_provider_ui_catalog,
            "source_connection_fields": self.build_source_provider_field_models(editable, provider_schema.connection_fields),
            "source_notification_fields": self.build_source_provider_field_models(editable, provider_schema.notification_fields),
            "source_provider_fields": self.build_source_provider_fields(editable),
            "protected_rules": protected_rules,
            "config_change_preview": config_change_preview,
            "config_preview_token": preview_token,
            "filters_are_remembered": True,
        }

    def apply_config_submission(self, request: Request, *, user: WebAdminUserRecord, submission: dict[str, Any]) -> None:
        current_org = self.request_support.get_current_org(request)
        if current_org.org_id != str(submission.get("org_id") or current_org.org_id):
            raise ValueError("Pending configuration preview no longer matches the selected organization.")

        request.app.state.org_config_repo.save_config(
            current_org.org_id,
            submission["org_values"],
            config_path=str(submission["legacy_config_path"]),
        )
        request.app.state.settings_repo.set_value("group_display_separator", submission["settings_values"]["group_display_separator"], "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("group_recursive_enabled", str(bool(submission["settings_values"]["group_recursive_enabled"])).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("group_recursive_enabled_user_override", "true", "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("managed_relation_cleanup_enabled", str(bool(submission["settings_values"]["managed_relation_cleanup_enabled"])).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("schedule_execution_mode", str(submission["settings_values"]["schedule_execution_mode"]), "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("web_bind_host", str(submission["settings_values"]["web_bind_host"]), "string")
        request.app.state.settings_repo.set_value("web_bind_port", str(submission["settings_values"]["web_bind_port"]), "int")
        request.app.state.settings_repo.set_value("web_public_base_url", str(submission["settings_values"]["web_public_base_url"]), "string")
        request.app.state.settings_repo.set_value("web_session_cookie_secure_mode", str(submission["settings_values"]["web_session_cookie_secure_mode"]), "string")
        request.app.state.settings_repo.set_value("web_trust_proxy_headers", str(bool(submission["settings_values"]["web_trust_proxy_headers"])).lower(), "bool")
        request.app.state.settings_repo.set_value("web_forwarded_allow_ips", str(submission["settings_values"]["web_forwarded_allow_ips"]), "string")
        request.app.state.settings_repo.set_value("brand_display_name", str(submission["settings_values"]["brand_display_name"]), "string")
        request.app.state.settings_repo.set_value("brand_mark_text", str(submission["settings_values"]["brand_mark_text"]), "string")
        request.app.state.settings_repo.set_value("brand_attribution", str(submission["settings_values"]["brand_attribution"]), "string")
        request.app.state.settings_repo.set_value("user_ou_placement_strategy", str(submission["settings_values"]["user_ou_placement_strategy"]), "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("source_root_unit_ids", str(submission["settings_values"]["source_root_unit_ids"]), "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("directory_root_ou_path", str(submission["settings_values"]["directory_root_ou_path"]), "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("disabled_users_ou_path", str(submission["settings_values"]["disabled_users_ou_path"]), "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("custom_group_ou_path", str(submission["settings_values"]["custom_group_ou_path"]), "string", org_id=current_org.org_id)
        request.app.state.exclusion_repo.replace_soft_excluded_rules(
            (
                {
                    "match_value": line.strip(),
                    "display_name": line.strip(),
                    "is_enabled": True,
                    "source": "web_ui",
                }
                for line in str(submission["soft_excluded_groups"]).splitlines()
                if line.strip()
            ),
            org_id=current_org.org_id,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="config.update",
            target_type="organization_config",
            target_id=current_org.org_id,
            result="success",
            message="Updated system configuration",
            payload={
                "org_id": current_org.org_id,
                "legacy_config_path": str(submission["legacy_config_path"]),
                "user_ou_placement_strategy": submission["settings_values"]["user_ou_placement_strategy"],
                "web_bind_host": submission["settings_values"]["web_bind_host"],
                "web_bind_port": submission["settings_values"]["web_bind_port"],
                "web_public_base_url": submission["settings_values"]["web_public_base_url"],
                "web_session_cookie_secure_mode": submission["settings_values"]["web_session_cookie_secure_mode"],
                "web_trust_proxy_headers": bool(submission["settings_values"]["web_trust_proxy_headers"]),
                "web_forwarded_allow_ips": submission["settings_values"]["web_forwarded_allow_ips"],
                "ldap_validate_cert": bool(submission["org_values"]["ldap_validate_cert"]),
                "force_change_password": bool(submission["org_values"]["force_change_password"]),
                "password_complexity": submission["org_values"]["password_complexity"],
                "source_root_unit_ids": submission["settings_values"]["source_root_unit_ids"],
                "directory_root_ou_path": submission["settings_values"]["directory_root_ou_path"],
                "disabled_users_ou_path": submission["settings_values"]["disabled_users_ou_path"],
                "custom_group_ou_path": submission["settings_values"]["custom_group_ou_path"],
            },
        )
