from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urlencode

from fastapi import Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from sync_app.core.models import OrganizationRecord, WebAdminUserRecord
from sync_app.providers.source import get_source_provider_display_name, normalize_source_provider
from sync_app.services.typed_settings import WebSecuritySettings
from sync_app.web.app_state import get_web_repositories, get_web_runtime_state
from sync_app.web.authz import has_capability, role_capabilities
from sync_app.web.i18n import (
    SUPPORTED_UI_LANGUAGES,
    detect_browser_ui_language,
    normalize_ui_language,
    translate,
)
from sync_app.web.security import ensure_csrf_token, validate_admin_password_strength, validate_csrf_token


class RequestSupport:
    def __init__(
        self,
        *,
        templates: Jinja2Templates,
        app_version: str,
        default_brand_display_name: str,
        default_brand_mark_text: str,
        default_brand_attribution: str,
        supported_ui_modes: dict[str, str],
        placement_strategies: dict[str, str],
        advanced_nav_pages: set[str],
        session_filter_prefix: str,
    ) -> None:
        self.templates = templates
        self.app_version = app_version
        self.default_brand_display_name = default_brand_display_name
        self.default_brand_mark_text = default_brand_mark_text
        self.default_brand_attribution = default_brand_attribution
        self.supported_ui_modes = supported_ui_modes
        self.placement_strategies = placement_strategies
        self.advanced_nav_pages = advanced_nav_pages
        self.session_filter_prefix = session_filter_prefix

    def flash(self, request: Request, level: str, message: str) -> None:
        request.session["_flash"] = {"level": level, "message": message}

    def flash_t(self, request: Request, level: str, key: str, **params: Any) -> None:
        request.session["_flash"] = {"level": level, "message": {"key": key, "params": params}}

    def pop_flash(self, request: Request) -> Optional[dict[str, Any]]:
        return request.session.pop("_flash", None)

    def get_ui_language(self, request: Request) -> str:
        requested_language = request.query_params.get("lang")
        if requested_language is not None:
            ui_language = normalize_ui_language(requested_language)
            request.session["ui_language"] = ui_language
            return ui_language
        session_language = str(request.session.get("ui_language") or "").strip()
        if session_language:
            return normalize_ui_language(session_language)
        return detect_browser_ui_language(request.headers.get("accept-language"))

    def normalize_ui_mode(self, value: Any) -> str:
        normalized = str(value or "").strip().lower()
        return normalized if normalized in self.supported_ui_modes else "basic"

    def get_ui_mode(self, request: Request) -> str:
        return self.normalize_ui_mode(request.session.get("ui_mode"))

    def translate_text(self, ui_language: str, text: str, **params: Any) -> str:
        return translate(ui_language, text, **params)

    def localize_flash_message(self, ui_language: str, flash_record: Optional[dict[str, Any]]) -> Optional[dict[str, str]]:
        if not flash_record:
            return None
        payload = flash_record.get("message")
        if isinstance(payload, dict):
            message = self.translate_text(
                ui_language,
                str(payload.get("key") or ""),
                **dict(payload.get("params") or {}),
            )
        else:
            message = self.translate_text(ui_language, str(payload or ""))
        return {
            "level": str(flash_record.get("level") or "info"),
            "message": message,
        }

    def get_current_user(self, request: Request) -> Optional[WebAdminUserRecord]:
        username = str(request.session.get("username") or "").strip()
        if not username:
            return None
        repositories = get_web_repositories(request)
        user = repositories.user_repo.get_user_record_by_username(username)
        if not user or not user.is_enabled:
            request.session.clear()
            return None
        return user

    def get_current_org(self, request: Request) -> OrganizationRecord:
        repositories = get_web_repositories(request)
        runtime_state = get_web_runtime_state(request)
        org_repo = repositories.organization_repo
        selected_org_id = str(request.session.get("selected_org_id") or "").strip().lower()
        organization = org_repo.get_organization_record(selected_org_id) if selected_org_id else None
        if not organization or not organization.is_enabled:
            organization = org_repo.get_default_organization_record() or org_repo.ensure_default(
                config_path=runtime_state.config_path
            )
            request.session["selected_org_id"] = organization.org_id
        return organization

    def get_org_config_path(self, request: Request) -> str:
        organization = self.get_current_org(request)
        runtime_state = get_web_runtime_state(request)
        return organization.config_path or runtime_state.config_path

    def list_org_connector_records(self, request: Request) -> list[Any]:
        organization = self.get_current_org(request)
        repositories = get_web_repositories(request)
        return repositories.connector_repo.list_connector_records(org_id=organization.org_id)

    def connector_has_database_overrides(self, record: Any) -> bool:
        return any(
            [
                bool(getattr(record, "ldap_server", "")),
                bool(getattr(record, "ldap_domain", "")),
                bool(getattr(record, "ldap_username", "")),
                bool(getattr(record, "ldap_password", "")),
                getattr(record, "ldap_use_ssl", None) is not None,
                getattr(record, "ldap_port", None) is not None,
                getattr(record, "ldap_validate_cert", None) is not None,
                bool(getattr(record, "ldap_ca_cert_path", "")),
                bool(getattr(record, "default_password", "")),
                getattr(record, "force_change_password", None) is not None,
                bool(getattr(record, "password_complexity", "")),
            ]
        )

    def describe_connector_config_source(self, record: Any) -> str:
        if self.connector_has_database_overrides(record):
            return "Database Overrides"
        if getattr(record, "config_path", ""):
            return "Legacy Import Path"
        return "Inherited Organization Settings"

    def list_org_attribute_mapping_rules(self, request: Request) -> list[Any]:
        organization = self.get_current_org(request)
        repositories = get_web_repositories(request)
        connector_ids = {record.connector_id for record in self.list_org_connector_records(request)}
        rules = repositories.attribute_mapping_repo.list_rule_records(org_id=organization.org_id)
        return [rule for rule in rules if not rule.connector_id or rule.connector_id in connector_ids]

    def get_org_setting_value(self, request: Request, key: str, default: Optional[str] = None) -> Optional[str]:
        repositories = get_web_repositories(request)
        return repositories.settings_repo.get_value(key, default, org_id=self.get_current_org(request).org_id)

    def get_org_setting_bool(self, request: Request, key: str, default: bool = False) -> bool:
        repositories = get_web_repositories(request)
        return repositories.settings_repo.get_bool(key, default, org_id=self.get_current_org(request).org_id)

    def get_org_setting_int(self, request: Request, key: str, default: int = 0) -> int:
        repositories = get_web_repositories(request)
        return repositories.settings_repo.get_int(key, default, org_id=self.get_current_org(request).org_id)

    def get_org_setting_float(self, request: Request, key: str, default: float = 0.0) -> float:
        repositories = get_web_repositories(request)
        return repositories.settings_repo.get_float(key, default, org_id=self.get_current_org(request).org_id)

    def get_page_filter_session_key(self, page_name: str) -> str:
        return f"{self.session_filter_prefix}:{str(page_name or '').strip().lower()}"

    def resolve_remembered_filters(
        self,
        request: Request,
        *,
        page_name: str,
        defaults: dict[str, str],
        to_text: Any,
        to_bool: Any,
    ) -> dict[str, str]:
        session_key = self.get_page_filter_session_key(page_name)
        if to_bool(request.query_params.get("clear_filters"), False):
            request.session.pop(session_key, None)
            return dict(defaults)

        explicit_values: dict[str, str] = {}
        has_explicit_filters = False
        for field_name, default_value in defaults.items():
            if field_name in request.query_params:
                has_explicit_filters = True
                explicit_values[field_name] = to_text(request.query_params.get(field_name), default_value)

        if has_explicit_filters:
            resolved = {field_name: explicit_values.get(field_name, default_value) for field_name, default_value in defaults.items()}
            request.session[session_key] = dict(resolved)
            return resolved

        stored = request.session.get(session_key)
        if isinstance(stored, dict):
            return {
                field_name: to_text(stored.get(field_name), default_value)
                for field_name, default_value in defaults.items()
            }
        return dict(defaults)

    def normalize_soft_excluded_groups_text(self, value: str) -> str:
        normalized_lines = [line.strip() for line in str(value or "").splitlines() if line.strip()]
        return "\n".join(normalized_lines)

    def source_provider_label(self, value: Any) -> str:
        normalized_value = normalize_source_provider(str(value or "").strip() or None)
        return get_source_provider_display_name(normalized_value)

    def render(self, request: Request, template_name: str, **context: Any):
        repositories = get_web_repositories(request)
        current_user = context.setdefault("current_user", self.get_current_user(request))
        current_org = context.setdefault("current_org", self.get_current_org(request) if current_user else None)
        csrf_token = ensure_csrf_token(request.session)
        current_role = current_user.role if current_user else None
        ui_language = self.get_ui_language(request)
        ui_mode = self.get_ui_mode(request)
        brand_display_name_raw = repositories.settings_repo.get_value(
            "brand_display_name",
            self.default_brand_display_name,
        )
        brand_display_name = (
            self.translate_text(ui_language, self.default_brand_display_name)
            if str(brand_display_name_raw or "").strip() == self.default_brand_display_name
            else str(brand_display_name_raw or "").strip()
        )
        brand_mark_text = repositories.settings_repo.get_value(
            "brand_mark_text",
            self.default_brand_mark_text,
        )
        brand_attribution = repositories.settings_repo.get_value(
            "brand_attribution",
            self.default_brand_attribution,
        )
        current_path = request.url.path
        if request.url.query:
            current_path = f"{current_path}?{request.url.query}"
        current_page = str(context.get("page") or "").strip()
        language_urls = {
            code: (
                request.url.path
                + (
                    "?"
                    + urlencode(
                        {
                            **{key: value for key, value in request.query_params.items() if key != "lang"},
                            "lang": code,
                        }
                    )
                )
            )
            for code in SUPPORTED_UI_LANGUAGES
        }
        localized_flash = self.localize_flash_message(ui_language, self.pop_flash(request))
        context.setdefault("request", request)
        context.setdefault("flash", localized_flash)
        context.setdefault("app_version", self.app_version)
        context.setdefault("brand_display_name", brand_display_name)
        context.setdefault("brand_mark_text", str(brand_mark_text or "").strip() or self.default_brand_mark_text)
        context.setdefault("brand_attribution", str(brand_attribution or "").strip() or self.default_brand_attribution)
        context.setdefault("has_users", repositories.user_repo.has_any_user())
        context.setdefault(
            "organizations",
            repositories.organization_repo.list_organization_records() if current_user else [],
        )
        context.setdefault(
            "enabled_organizations",
            repositories.organization_repo.list_organization_records(enabled_only=True) if current_user else [],
        )
        context.setdefault("placement_strategy_options", self.placement_strategies)
        context.setdefault(
            "translated_placement_strategy_options",
            {key: self.translate_text(ui_language, value) for key, value in self.placement_strategies.items()},
        )
        context.setdefault("csrf_token", csrf_token)
        context.setdefault("ui_language", ui_language)
        context.setdefault("language_options", SUPPORTED_UI_LANGUAGES)
        context.setdefault("language_urls", language_urls)
        context.setdefault("current_path", current_path)
        context.setdefault("ui_mode", ui_mode)
        context.setdefault("ui_mode_options", self.supported_ui_modes)
        context.setdefault(
            "show_advanced_navigation",
            ui_mode == "advanced" or current_page in self.advanced_nav_pages,
        )
        context.setdefault("is_advanced_page", current_page in self.advanced_nav_pages)
        context.setdefault("t", lambda text, **params: self.translate_text(ui_language, str(text or ""), **params))
        context.setdefault("current_capabilities", role_capabilities(current_role))
        context.setdefault("can", lambda capability: has_capability(current_role, capability))
        if "title" in context and isinstance(context["title"], str):
            context["title"] = self.translate_text(ui_language, context["title"])
        return self.templates.TemplateResponse(request, template_name, context)

    def require_user(self, request: Request):
        repositories = get_web_repositories(request)
        if not repositories.user_repo.has_any_user():
            return RedirectResponse(url="/setup", status_code=303)
        user = self.get_current_user(request)
        if not user:
            return RedirectResponse(url="/login", status_code=303)
        return user

    def require_capability(self, request: Request, capability: str):
        user = self.require_user(request)
        if isinstance(user, RedirectResponse):
            return user
        if not has_capability(user.role, capability):
            self.flash(request, "error", "Current account is not allowed to access this function")
            return RedirectResponse(url="/dashboard", status_code=303)
        return user

    def reject_invalid_csrf(self, request: Request, submitted_token: str, fallback_url: str):
        if validate_csrf_token(request.session, submitted_token):
            return None
        self.flash(request, "error", "Request validation failed. Refresh the page and try again.")
        return RedirectResponse(url=fallback_url, status_code=303)

    def get_client_ip(self, request: Request) -> str:
        client = getattr(request, "client", None)
        if client and getattr(client, "host", None):
            return str(client.host)
        return "unknown"

    def validate_admin_password(self, request: Request, password: str) -> Optional[str]:
        repositories = get_web_repositories(request)
        min_length = WebSecuritySettings.load(repositories.settings_repo).admin_password_min_length
        return validate_admin_password_strength(password, min_length=min_length)
