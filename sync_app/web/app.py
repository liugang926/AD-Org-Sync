from __future__ import annotations

import csv
import io
import json
import logging
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Iterable, Optional
from urllib.parse import urlencode

from fastapi import FastAPI, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from sync_app.core.common import APP_VERSION
from sync_app.core.config import (
    load_sync_config,
    run_config_security_self_check,
    test_ldap_connection,
    test_source_connection,
    validate_config,
)
from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.core.conflict_recommendations import (
    recommend_conflict_resolution,
    recommendation_requires_confirmation,
)
from sync_app.core.sync_policies import (
    ATTRIBUTE_SYNC_MODES,
    MANAGED_GROUP_TYPES,
    normalize_mapping_direction,
)
from sync_app.core.models import AppConfig, OrganizationRecord, SyncJobRecord, WebAdminUserRecord
from sync_app.providers.source import (
    build_source_provider,
    get_source_provider_display_name,
    get_source_provider_schema,
    list_source_provider_options,
    normalize_source_provider,
)
from sync_app.services.config_bundle import export_organization_bundle, import_organization_bundle
from sync_app.storage.config_codec import normalize_org_config_values as _normalize_org_config_values
from sync_app.storage.local_db import (
    AttributeMappingRuleRepository,
    CustomManagedGroupBindingRepository,
    DatabaseManager,
    GroupExclusionRuleRepository,
    OffboardingQueueRepository,
    OrganizationConfigRepository,
    OrganizationRepository,
    SettingsRepository,
    SyncReplayRequestRepository,
    SyncConnectorRepository,
    SyncConflictRepository,
    SyncExceptionRuleRepository,
    SyncEventRepository,
    SyncJobRepository,
    SyncOperationLogRepository,
    SyncPlanReviewRepository,
    UserDepartmentOverrideRepository,
    UserIdentityBindingRepository,
    UserLifecycleQueueRepository,
    WebAdminUserRepository,
    WebAuditLogRepository,
    PlannedOperationRepository,
)
from sync_app.web.authz import has_capability, normalize_role, role_capabilities
from sync_app.web.dashboard_state import (
    build_getting_started_data as build_getting_started_view_state,
    count_check_statuses,
    merge_saved_preflight_snapshot as merge_saved_preflight_snapshot_data,
    summarize_check_status,
)
from sync_app.web.helpers import parse_bulk_bindings
from sync_app.web.i18n import (
    DEFAULT_UI_LANGUAGE,
    SUPPORTED_UI_LANGUAGES,
    detect_browser_ui_language,
    normalize_ui_language,
    translate,
)
from sync_app.web.routes_admin import register_admin_routes
from sync_app.web.routes_conflicts import register_conflict_routes
from sync_app.web.routes_config import register_config_routes
from sync_app.web.routes_exceptions import register_exception_routes
from sync_app.web.routes_jobs import register_job_routes
from sync_app.web.routes_mappings import register_mapping_routes
from sync_app.web.routes_organizations import register_organization_routes
from sync_app.web.runtime import (
    LoginRateLimiter,
    WebSyncRunner,
    normalize_secure_cookie_mode,
    resolve_web_runtime_settings,
    web_runtime_requires_restart,
)
from sync_app.web.security import (
    ensure_csrf_token,
    hash_password,
    rotate_csrf_token,
    validate_admin_password_strength,
    validate_csrf_token,
    verify_password,
)

LOGGER = logging.getLogger(__name__)
TEMPLATES = Jinja2Templates(directory=str(Path(__file__).with_name("templates")))
APP_ROOT = Path(__file__).resolve().parents[2]
STATIC_DIR = Path(__file__).with_name("static")
FAVICON_PATH = STATIC_DIR / "favicon.ico"
LEGACY_FAVICON_PATH = APP_ROOT / "icon.ico"
DEFAULT_BRAND_DISPLAY_NAME = "AD Org Sync"
DEFAULT_BRAND_MARK_TEXT = "AD"
DEFAULT_BRAND_ATTRIBUTION = "微信公众号：大刘讲IT"
PLACEMENT_STRATEGIES = {
    "source_primary_department": "Prefer source primary department",
    "wecom_primary_department": "Prefer source primary department",
    "lowest_department_id": "Pick the lowest department ID",
    "shortest_path": "Pick the shortest department path",
    "first_non_excluded_department": "Pick the first valid department in source order",
}
SUPPORTED_UI_MODES = {
    "basic": "Basic",
    "advanced": "Advanced",
}
ATTRIBUTE_MAPPING_DIRECTION_LABELS = {
    "source_to_ad": "Source -> AD",
    "ad_to_source": "AD -> Source",
    "wecom_to_ad": "Source -> AD",
    "ad_to_wecom": "AD -> Source",
}
ADVANCED_NAV_PAGES = {
    "advanced-sync",
    "organizations",
    "mappings",
    "exceptions",
    "database",
    "users",
    "audit",
}
SESSION_FILTER_PREFIX = "_page_filters"
CONFIG_PREVIEW_SESSION_KEY = "_config_preview"


class CsvStreamingResponse(StreamingResponse):
    def __init__(self, iterator_factory: Callable[[], Iterable[bytes]], **kwargs: Any) -> None:
        self._iterator_factory = iterator_factory
        super().__init__(iterator_factory(), **kwargs)

    def render_for_test(self) -> bytes:
        return b"".join(self._iterator_factory())


def _safe_redirect_target(value: str | None, default: str) -> str:
    candidate = str(value or "").strip()
    if not candidate.startswith("/") or candidate.startswith("//"):
        return default
    return candidate


def _to_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _to_text(value: Any, default: str = "") -> str:
    if isinstance(value, str):
        return value.strip()
    return default


def _clean_public_base_url(value: Optional[str]) -> str:
    return str(value or "").strip().rstrip("/")


def _split_csv_values(value: str | None) -> list[str]:
    items: list[str] = []
    for raw_item in str(value or "").replace("\n", ",").split(","):
        candidate = raw_item.strip()
        if candidate:
            items.append(candidate)
    return items


def create_app(
    *,
    db_path: str | None = None,
    config_path: str = "config.ini",
    bind_host: str | None = None,
    bind_port: int | None = None,
    public_base_url: str | None = None,
    session_cookie_secure_mode: str | None = None,
    trust_proxy_headers: bool | None = None,
    forwarded_allow_ips: str | None = None,
) -> FastAPI:
    db_manager = DatabaseManager(db_path=db_path)
    db_manager.initialize()
    settings_repo = SettingsRepository(db_manager)
    organization_repo = OrganizationRepository(db_manager)
    org_config_repo = OrganizationConfigRepository(db_manager)
    exclusion_repo = GroupExclusionRuleRepository(db_manager)
    connector_repo = SyncConnectorRepository(db_manager)
    attribute_mapping_repo = AttributeMappingRuleRepository(db_manager)
    custom_group_binding_repo = CustomManagedGroupBindingRepository(db_manager)
    offboarding_repo = OffboardingQueueRepository(db_manager)
    lifecycle_repo = UserLifecycleQueueRepository(db_manager)
    replay_request_repo = SyncReplayRequestRepository(db_manager)
    job_repo = SyncJobRepository(db_manager)
    event_repo = SyncEventRepository(db_manager)
    planned_operation_repo = PlannedOperationRepository(db_manager)
    operation_log_repo = SyncOperationLogRepository(db_manager)
    conflict_repo = SyncConflictRepository(db_manager)
    review_repo = SyncPlanReviewRepository(db_manager)
    exception_rule_repo = SyncExceptionRuleRepository(db_manager)
    user_repo = WebAdminUserRepository(db_manager)
    audit_repo = WebAuditLogRepository(db_manager)
    user_binding_repo = UserIdentityBindingRepository(db_manager)
    department_override_repo = UserDepartmentOverrideRepository(db_manager)
    organization_repo.ensure_default(config_path=config_path)
    org_config_repo.ensure_loaded("default", config_path=config_path)

    session_secret = settings_repo.get_value("web_session_secret", "") or ""
    if not session_secret:
        session_secret = secrets.token_urlsafe(48)
        settings_repo.set_value("web_session_secret", session_secret, "string")
    session_minutes = max(settings_repo.get_int("web_session_idle_minutes", 30), 1)
    startup_persisted_web_runtime_settings = resolve_web_runtime_settings(settings_repo)
    web_runtime_settings = resolve_web_runtime_settings(
        settings_repo,
        bind_host=bind_host,
        bind_port=bind_port,
        public_base_url=public_base_url,
        session_cookie_secure_mode=session_cookie_secure_mode,
        trust_proxy_headers=trust_proxy_headers,
        forwarded_allow_ips=forwarded_allow_ips,
    )
    login_rate_limiter = LoginRateLimiter(
        max_attempts=settings_repo.get_int("web_login_max_attempts", 5),
        window_seconds=settings_repo.get_int("web_login_window_seconds", 300),
        lockout_seconds=settings_repo.get_int("web_login_lockout_seconds", 300),
    )

    app = FastAPI(title="AD Org Sync Web", version=APP_VERSION)
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.add_middleware(
        SessionMiddleware,
        secret_key=session_secret,
        same_site="strict",
        https_only=web_runtime_settings["session_cookie_secure"],
        max_age=session_minutes * 60,
    )

    app.state.db_manager = db_manager
    app.state.settings_repo = settings_repo
    app.state.organization_repo = organization_repo
    app.state.org_config_repo = org_config_repo
    app.state.exclusion_repo = exclusion_repo
    app.state.connector_repo = connector_repo
    app.state.attribute_mapping_repo = attribute_mapping_repo
    app.state.custom_group_binding_repo = custom_group_binding_repo
    app.state.offboarding_repo = offboarding_repo
    app.state.lifecycle_repo = lifecycle_repo
    app.state.replay_request_repo = replay_request_repo
    app.state.job_repo = job_repo
    app.state.event_repo = event_repo
    app.state.planned_operation_repo = planned_operation_repo
    app.state.operation_log_repo = operation_log_repo
    app.state.conflict_repo = conflict_repo
    app.state.review_repo = review_repo
    app.state.exception_rule_repo = exception_rule_repo
    app.state.user_repo = user_repo
    app.state.audit_repo = audit_repo
    app.state.user_binding_repo = user_binding_repo
    app.state.department_override_repo = department_override_repo
    app.state.config_path = config_path
    app.state.login_rate_limiter = login_rate_limiter
    app.state.session_cookie_secure = web_runtime_settings["session_cookie_secure"]
    app.state.web_runtime_settings = web_runtime_settings
    app.state.startup_persisted_web_runtime_settings = startup_persisted_web_runtime_settings
    app.state.sync_runner = WebSyncRunner(
        db_path=db_manager.db_path,
        audit_repo=audit_repo,
    )
    department_name_cache: dict[str, Any] = {
        "expires_at": 0.0,
        "config_fingerprint": "",
        "value": {},
    }

    def flash(request: Request, level: str, message: str) -> None:
        request.session["_flash"] = {"level": level, "message": message}

    def flash_t(request: Request, level: str, key: str, **params: Any) -> None:
        request.session["_flash"] = {"level": level, "message": {"key": key, "params": params}}

    def pop_flash(request: Request) -> Optional[dict[str, Any]]:
        return request.session.pop("_flash", None)

    def get_ui_language(request: Request) -> str:
        requested_language = request.query_params.get("lang")
        if requested_language is not None:
            ui_language = normalize_ui_language(requested_language)
            request.session["ui_language"] = ui_language
            return ui_language
        session_language = str(request.session.get("ui_language") or "").strip()
        if session_language:
            return normalize_ui_language(session_language)
        return detect_browser_ui_language(request.headers.get("accept-language"))

    def normalize_ui_mode(value: Any) -> str:
        normalized = str(value or "").strip().lower()
        return normalized if normalized in SUPPORTED_UI_MODES else "basic"

    def get_ui_mode(request: Request) -> str:
        return normalize_ui_mode(request.session.get("ui_mode"))

    def translate_text(ui_language: str, text: str, **params: Any) -> str:
        return translate(ui_language, text, **params)

    def localize_flash_message(ui_language: str, flash_record: Optional[dict[str, Any]]) -> Optional[dict[str, str]]:
        if not flash_record:
            return None
        payload = flash_record.get("message")
        if isinstance(payload, dict):
            message = translate_text(
                ui_language,
                str(payload.get("key") or ""),
                **dict(payload.get("params") or {}),
            )
        else:
            message = translate_text(ui_language, str(payload or ""))
        return {
            "level": str(flash_record.get("level") or "info"),
            "message": message,
        }

    def get_current_user(request: Request) -> Optional[WebAdminUserRecord]:
        username = str(request.session.get("username") or "").strip()
        if not username:
            return None
        user = request.app.state.user_repo.get_user_record_by_username(username)
        if not user or not user.is_enabled:
            request.session.clear()
            return None
        return user

    def get_current_org(request: Request) -> OrganizationRecord:
        org_repo: OrganizationRepository = request.app.state.organization_repo
        selected_org_id = str(request.session.get("selected_org_id") or "").strip().lower()
        organization = org_repo.get_organization_record(selected_org_id) if selected_org_id else None
        if not organization or not organization.is_enabled:
            organization = (
                org_repo.get_default_organization_record()
                or org_repo.ensure_default(config_path=request.app.state.config_path)
            )
            request.session["selected_org_id"] = organization.org_id
        return organization

    def get_org_config_path(request: Request) -> str:
        organization = get_current_org(request)
        return organization.config_path or request.app.state.config_path

    def list_org_connector_records(request: Request) -> list[Any]:
        organization = get_current_org(request)
        return request.app.state.connector_repo.list_connector_records(org_id=organization.org_id)

    def connector_has_database_overrides(record: Any) -> bool:
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

    def describe_connector_config_source(record: Any) -> str:
        if connector_has_database_overrides(record):
            return "Database Overrides"
        if getattr(record, "config_path", ""):
            return "Legacy Import Path"
        return "Inherited Organization Settings"

    def list_org_attribute_mapping_rules(request: Request) -> list[Any]:
        organization = get_current_org(request)
        connector_ids = {record.connector_id for record in list_org_connector_records(request)}
        rules = request.app.state.attribute_mapping_repo.list_rule_records(org_id=organization.org_id)
        return [rule for rule in rules if not rule.connector_id or rule.connector_id in connector_ids]

    def get_org_setting_value(request: Request, key: str, default: Optional[str] = None) -> Optional[str]:
        return request.app.state.settings_repo.get_value(key, default, org_id=get_current_org(request).org_id)

    def get_org_setting_bool(request: Request, key: str, default: bool = False) -> bool:
        return request.app.state.settings_repo.get_bool(key, default, org_id=get_current_org(request).org_id)

    def get_org_setting_int(request: Request, key: str, default: int = 0) -> int:
        return request.app.state.settings_repo.get_int(key, default, org_id=get_current_org(request).org_id)

    def get_org_setting_float(request: Request, key: str, default: float = 0.0) -> float:
        return request.app.state.settings_repo.get_float(key, default, org_id=get_current_org(request).org_id)

    def get_page_filter_session_key(page_name: str) -> str:
        return f"{SESSION_FILTER_PREFIX}:{str(page_name or '').strip().lower()}"

    def resolve_remembered_filters(
        request: Request,
        *,
        page_name: str,
        defaults: dict[str, str],
    ) -> dict[str, str]:
        session_key = get_page_filter_session_key(page_name)
        if _to_bool(request.query_params.get("clear_filters"), False):
            request.session.pop(session_key, None)
            return dict(defaults)

        explicit_values: dict[str, str] = {}
        has_explicit_filters = False
        for field_name, default_value in defaults.items():
            if field_name in request.query_params:
                has_explicit_filters = True
                explicit_values[field_name] = _to_text(request.query_params.get(field_name), default_value)

        if has_explicit_filters:
            resolved = {field_name: explicit_values.get(field_name, default_value) for field_name, default_value in defaults.items()}
            request.session[session_key] = dict(resolved)
            return resolved

        stored = request.session.get(session_key)
        if isinstance(stored, dict):
            return {
                field_name: _to_text(stored.get(field_name), default_value)
                for field_name, default_value in defaults.items()
            }
        return dict(defaults)

    def normalize_soft_excluded_groups_text(value: str) -> str:
        normalized_lines = [line.strip() for line in str(value or "").splitlines() if line.strip()]
        return "\n".join(normalized_lines)

    def source_provider_label(value: Any) -> str:
        normalized_value = normalize_source_provider(str(value or "").strip() or None)
        return get_source_provider_display_name(normalized_value)

    def build_source_provider_field_models(
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

    def build_source_provider_fields(editable: dict[str, Any]) -> list[dict[str, Any]]:
        provider_schema = get_source_provider_schema(editable.get("source_provider"))
        return build_source_provider_field_models(
            editable,
            (*provider_schema.connection_fields, *provider_schema.notification_fields),
        )

    def build_config_preview_groups(provider_schema) -> tuple[tuple[str, tuple[tuple[str, str, str], ...]], ...]:
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
            groups.append(
                (
                    "Optional Notifications",
                    notification_fields,
                )
            )
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

    def build_current_config_state(request: Request, current_org: OrganizationRecord) -> dict[str, Any]:
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
            "web_bind_host": request.app.state.settings_repo.get_value(
                "web_bind_host",
                "127.0.0.1",
            ),
            "web_bind_port": request.app.state.settings_repo.get_int(
                "web_bind_port",
                8000,
            ),
            "web_public_base_url": request.app.state.settings_repo.get_value(
                "web_public_base_url",
                "",
            ),
            "web_session_cookie_secure_mode": request.app.state.settings_repo.get_value(
                "web_session_cookie_secure_mode",
                "auto",
            ),
            "web_trust_proxy_headers": request.app.state.settings_repo.get_bool(
                "web_trust_proxy_headers",
                False,
            ),
            "web_forwarded_allow_ips": request.app.state.settings_repo.get_value(
                "web_forwarded_allow_ips",
                "127.0.0.1",
            ),
            "brand_display_name": request.app.state.settings_repo.get_value(
                "brand_display_name",
                DEFAULT_BRAND_DISPLAY_NAME,
            ),
            "brand_mark_text": request.app.state.settings_repo.get_value(
                "brand_mark_text",
                DEFAULT_BRAND_MARK_TEXT,
            ),
            "brand_attribution": request.app.state.settings_repo.get_value(
                "brand_attribution",
                DEFAULT_BRAND_ATTRIBUTION,
            ),
            "user_ou_placement_strategy": request.app.state.settings_repo.get_value(
                "user_ou_placement_strategy",
                "source_primary_department",
                org_id=current_org.org_id,
            ),
            "soft_excluded_groups": normalize_soft_excluded_groups_text(
                "\n".join(
                    request.app.state.exclusion_repo.list_soft_excluded_group_names(
                        enabled_only=False,
                        org_id=current_org.org_id,
                    )
                )
            ),
        }

    def build_config_submission(
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
        brand_display_name: str = DEFAULT_BRAND_DISPLAY_NAME,
        brand_mark_text: str = DEFAULT_BRAND_MARK_TEXT,
        brand_attribution: str = DEFAULT_BRAND_ATTRIBUTION,
        user_ou_placement_strategy: str = "source_primary_department",
        soft_excluded_groups: str = "",
    ) -> dict[str, Any]:
        current_org = get_current_org(request)
        current_org_config_path = current_org.config_path or request.app.state.config_path
        current_org_values = request.app.state.org_config_repo.get_raw_config(
            current_org.org_id,
            config_path=current_org_config_path,
        )
        if user_ou_placement_strategy not in PLACEMENT_STRATEGIES:
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
                "ldap_use_ssl": _to_bool(ldap_use_ssl, True),
                "ldap_validate_cert": _to_bool(ldap_validate_cert, True),
                "ldap_ca_cert_path": ldap_ca_cert_path.strip(),
                "default_password": default_password,
                "force_change_password": _to_bool(force_change_password, True),
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
            "group_recursive_enabled": _to_bool(group_recursive_enabled, True),
            "managed_relation_cleanup_enabled": _to_bool(managed_relation_cleanup_enabled, False),
            "schedule_execution_mode": "dry_run" if schedule_execution_mode == "dry_run" else "apply",
            "web_bind_host": web_bind_host.strip() or "127.0.0.1",
            "web_bind_port": max(int(web_bind_port or 8000), 1),
            "web_public_base_url": _clean_public_base_url(web_public_base_url),
            "web_session_cookie_secure_mode": normalize_secure_cookie_mode(web_session_cookie_secure_mode),
            "web_trust_proxy_headers": _to_bool(web_trust_proxy_headers, False),
            "web_forwarded_allow_ips": web_forwarded_allow_ips.strip() or "127.0.0.1",
            "brand_display_name": str(brand_display_name or "").strip() or DEFAULT_BRAND_DISPLAY_NAME,
            "brand_mark_text": str(brand_mark_text or "").strip() or DEFAULT_BRAND_MARK_TEXT,
            "brand_attribution": str(brand_attribution or "").strip() or DEFAULT_BRAND_ATTRIBUTION,
            "user_ou_placement_strategy": user_ou_placement_strategy,
        }
        return {
            "org_id": current_org.org_id,
            "legacy_config_path": current_org_config_path,
            "org_values": normalized_org_values,
            "settings_values": normalized_settings,
            "soft_excluded_groups": normalize_soft_excluded_groups_text(soft_excluded_groups),
        }

    def format_config_change_value(
        field_name: str,
        field_type: str,
        value: Any,
        *,
        previous_value: Any = None,
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
            }.get(str(value or "").strip().lower(), str(value or "-")), True
        if field_type == "schedule_execution_mode":
            return ("Dry Run" if str(value or "").strip().lower() == "dry_run" else "Apply"), True
        if field_type == "placement_strategy":
            return PLACEMENT_STRATEGIES.get(str(value or ""), str(value or "-")), True
        if field_type == "secure_cookie_mode":
            return {
                "auto": "auto",
                "always": "always",
                "never": "never",
            }.get(str(value or "").strip().lower(), str(value or "-")), False
        if field_type == "group_separator":
            return ("Space", True) if str(value or "") == " " else (str(value or "-"), False)
        if field_type == "multiline":
            normalized_lines = [line.strip() for line in str(value or "").splitlines() if line.strip()]
            if not normalized_lines:
                return "None", True
            return ", ".join(normalized_lines), False
        normalized_value = str(value or "").strip()
        if not normalized_value:
            return "Not set", True
        return normalized_value, False

    def build_config_change_preview(request: Request, submission: dict[str, Any]) -> dict[str, Any]:
        current_org = get_current_org(request)
        current_state = build_current_config_state(request, current_org)
        proposed_state = {
            **submission["org_values"],
            **submission["settings_values"],
            "soft_excluded_groups": submission["soft_excluded_groups"],
        }
        groups: list[dict[str, Any]] = []
        changed_count = 0
        provider_schema = get_source_provider_schema(submission["org_values"].get("source_provider"))
        for group_title, fields in build_config_preview_groups(provider_schema):
            group_changes: list[dict[str, Any]] = []
            for field_name, label, field_type in fields:
                current_value = current_state.get(field_name)
                proposed_value = proposed_state.get(field_name)
                if current_value == proposed_value:
                    continue
                before_display, before_translate = format_config_change_value(
                    field_name,
                    field_type,
                    current_value,
                )
                after_display, after_translate = format_config_change_value(
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

    def build_config_editable_override(request: Request, submission: dict[str, Any]) -> dict[str, Any]:
        current_org = get_current_org(request)
        editable = request.app.state.org_config_repo.get_editable_config(
            current_org.org_id,
            config_path=get_org_config_path(request),
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
                "soft_excluded_groups": submission["soft_excluded_groups"],
            }
        )
        return editable

    def build_config_page_context(
        request: Request,
        *,
        editable_override: Optional[dict[str, Any]] = None,
        config_change_preview: Optional[dict[str, Any]] = None,
        preview_token: str = "",
    ) -> dict[str, Any]:
        current_org = get_current_org(request)
        editable = editable_override or request.app.state.org_config_repo.get_editable_config(
            current_org.org_id,
            config_path=get_org_config_path(request),
        )
        if "protected_accounts" not in editable:
            effective_config = request.app.state.org_config_repo.get_app_config(
                current_org.org_id,
                config_path=get_org_config_path(request),
            )
            editable["protected_accounts"] = list(effective_config.exclude_accounts)
        editable.setdefault(
            "brand_display_name",
            request.app.state.settings_repo.get_value(
                "brand_display_name",
                DEFAULT_BRAND_DISPLAY_NAME,
            ),
        )
        editable.setdefault(
            "brand_mark_text",
            request.app.state.settings_repo.get_value(
                "brand_mark_text",
                DEFAULT_BRAND_MARK_TEXT,
            ),
        )
        editable.setdefault(
            "brand_attribution",
            request.app.state.settings_repo.get_value(
                "brand_attribution",
                DEFAULT_BRAND_ATTRIBUTION,
            ),
        )
        current_source_provider = normalize_source_provider(editable.get("source_provider"))
        provider_schema = get_source_provider_schema(current_source_provider)
        source_provider_name = source_provider_label(current_source_provider)
        source_provider_options = list_source_provider_options(include_unimplemented=True)
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
            "source_connection_fields": build_source_provider_field_models(editable, provider_schema.connection_fields),
            "source_notification_fields": build_source_provider_field_models(editable, provider_schema.notification_fields),
            "source_provider_fields": build_source_provider_fields(editable),
            "protected_rules": protected_rules,
            "config_change_preview": config_change_preview,
            "config_preview_token": preview_token,
            "filters_are_remembered": True,
        }

    def apply_config_submission(request: Request, *, user: WebAdminUserRecord, submission: dict[str, Any]) -> None:
        current_org = get_current_org(request)
        if current_org.org_id != str(submission.get("org_id") or current_org.org_id):
            raise ValueError("Pending configuration preview no longer matches the selected organization.")

        request.app.state.org_config_repo.save_config(
            current_org.org_id,
            submission["org_values"],
            config_path=str(submission["legacy_config_path"]),
        )
        request.app.state.settings_repo.set_value(
            "group_display_separator",
            submission["settings_values"]["group_display_separator"],
            "string",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "group_recursive_enabled",
            str(bool(submission["settings_values"]["group_recursive_enabled"])).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "group_recursive_enabled_user_override",
            "true",
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "managed_relation_cleanup_enabled",
            str(bool(submission["settings_values"]["managed_relation_cleanup_enabled"])).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "schedule_execution_mode",
            str(submission["settings_values"]["schedule_execution_mode"]),
            "string",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "web_bind_host",
            str(submission["settings_values"]["web_bind_host"]),
            "string",
        )
        request.app.state.settings_repo.set_value(
            "web_bind_port",
            str(submission["settings_values"]["web_bind_port"]),
            "int",
        )
        request.app.state.settings_repo.set_value(
            "web_public_base_url",
            str(submission["settings_values"]["web_public_base_url"]),
            "string",
        )
        request.app.state.settings_repo.set_value(
            "web_session_cookie_secure_mode",
            str(submission["settings_values"]["web_session_cookie_secure_mode"]),
            "string",
        )
        request.app.state.settings_repo.set_value(
            "web_trust_proxy_headers",
            str(bool(submission["settings_values"]["web_trust_proxy_headers"])).lower(),
            "bool",
        )
        request.app.state.settings_repo.set_value(
            "web_forwarded_allow_ips",
            str(submission["settings_values"]["web_forwarded_allow_ips"]),
            "string",
        )
        request.app.state.settings_repo.set_value(
            "brand_display_name",
            str(submission["settings_values"]["brand_display_name"]),
            "string",
        )
        request.app.state.settings_repo.set_value(
            "brand_mark_text",
            str(submission["settings_values"]["brand_mark_text"]),
            "string",
        )
        request.app.state.settings_repo.set_value(
            "brand_attribution",
            str(submission["settings_values"]["brand_attribution"]),
            "string",
        )
        request.app.state.settings_repo.set_value(
            "user_ou_placement_strategy",
            str(submission["settings_values"]["user_ou_placement_strategy"]),
            "string",
            org_id=current_org.org_id,
        )
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
            },
        )

    def render(request: Request, template_name: str, **context: Any):
        current_user = context.setdefault("current_user", get_current_user(request))
        current_org = context.setdefault("current_org", get_current_org(request) if current_user else None)
        csrf_token = ensure_csrf_token(request.session)
        current_role = current_user.role if current_user else None
        ui_language = get_ui_language(request)
        ui_mode = get_ui_mode(request)
        brand_display_name_raw = request.app.state.settings_repo.get_value(
            "brand_display_name",
            DEFAULT_BRAND_DISPLAY_NAME,
        )
        brand_display_name = (
            translate_text(ui_language, DEFAULT_BRAND_DISPLAY_NAME)
            if str(brand_display_name_raw or "").strip() == DEFAULT_BRAND_DISPLAY_NAME
            else str(brand_display_name_raw or "").strip()
        )
        brand_mark_text = request.app.state.settings_repo.get_value(
            "brand_mark_text",
            DEFAULT_BRAND_MARK_TEXT,
        )
        brand_attribution = request.app.state.settings_repo.get_value(
            "brand_attribution",
            DEFAULT_BRAND_ATTRIBUTION,
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
        localized_flash = localize_flash_message(ui_language, pop_flash(request))
        context.setdefault("request", request)
        context.setdefault("flash", localized_flash)
        context.setdefault("app_version", APP_VERSION)
        context.setdefault("brand_display_name", brand_display_name)
        context.setdefault("brand_mark_text", str(brand_mark_text or "").strip() or DEFAULT_BRAND_MARK_TEXT)
        context.setdefault("brand_attribution", str(brand_attribution or "").strip() or DEFAULT_BRAND_ATTRIBUTION)
        context.setdefault("has_users", request.app.state.user_repo.has_any_user())
        context.setdefault(
            "organizations",
            request.app.state.organization_repo.list_organization_records() if current_user else [],
        )
        context.setdefault(
            "enabled_organizations",
            request.app.state.organization_repo.list_organization_records(enabled_only=True) if current_user else [],
        )
        context.setdefault("placement_strategy_options", PLACEMENT_STRATEGIES)
        context.setdefault(
            "translated_placement_strategy_options",
            {key: translate_text(ui_language, value) for key, value in PLACEMENT_STRATEGIES.items()},
        )
        context.setdefault("csrf_token", csrf_token)
        context.setdefault("ui_language", ui_language)
        context.setdefault("language_options", SUPPORTED_UI_LANGUAGES)
        context.setdefault("language_urls", language_urls)
        context.setdefault("current_path", current_path)
        context.setdefault("ui_mode", ui_mode)
        context.setdefault("ui_mode_options", SUPPORTED_UI_MODES)
        context.setdefault(
            "show_advanced_navigation",
            ui_mode == "advanced" or current_page in ADVANCED_NAV_PAGES,
        )
        context.setdefault("is_advanced_page", current_page in ADVANCED_NAV_PAGES)
        context.setdefault(
            "t",
            lambda text, **params: translate_text(ui_language, str(text or ""), **params),
        )
        context.setdefault("current_capabilities", role_capabilities(current_role))
        context.setdefault(
            "can",
            lambda capability: has_capability(current_role, capability),
        )
        if "title" in context and isinstance(context["title"], str):
            context["title"] = translate_text(ui_language, context["title"])
        return TEMPLATES.TemplateResponse(request, template_name, context)

    def require_user(request: Request):
        if not request.app.state.user_repo.has_any_user():
            return RedirectResponse(url="/setup", status_code=303)
        user = get_current_user(request)
        if not user:
            return RedirectResponse(url="/login", status_code=303)
        return user

    def require_capability(request: Request, capability: str):
        user = require_user(request)
        if isinstance(user, RedirectResponse):
            return user
        if not has_capability(user.role, capability):
            flash(request, "error", "Current account is not allowed to access this function")
            return RedirectResponse(url="/dashboard", status_code=303)
        return user

    def reject_invalid_csrf(request: Request, submitted_token: str, fallback_url: str):
        if validate_csrf_token(request.session, submitted_token):
            return None
        flash(request, "error", "Request validation failed. Refresh the page and try again.")
        return RedirectResponse(url=fallback_url, status_code=303)

    def get_client_ip(request: Request) -> str:
        client = getattr(request, "client", None)
        if client and getattr(client, "host", None):
            return str(client.host)
        return "unknown"

    def validate_admin_password(request: Request, password: str) -> Optional[str]:
        min_length = request.app.state.settings_repo.get_int("web_admin_password_min_length", 8)
        return validate_admin_password_strength(password, min_length=min_length)

    def parse_page_number(raw_value: Optional[str], default: int = 1) -> int:
        try:
            return max(int(raw_value or default), 1)
        except (TypeError, ValueError):
            return default

    def build_page_context(*, items: list[Any], total_items: int, page: int, page_size: int) -> dict[str, Any]:
        normalized_page_size = max(int(page_size or 1), 1)
        normalized_total_items = max(int(total_items or 0), 0)
        total_pages = max((normalized_total_items + normalized_page_size - 1) // normalized_page_size, 1)
        normalized_page = min(max(int(page or 1), 1), total_pages)
        return {
            "items": items,
            "page": normalized_page,
            "page_size": normalized_page_size,
            "total_items": normalized_total_items,
            "total_pages": total_pages,
            "has_previous": normalized_page > 1,
            "has_next": normalized_page < total_pages,
            "previous_page": normalized_page - 1 if normalized_page > 1 else 1,
            "next_page": normalized_page + 1 if normalized_page < total_pages else total_pages,
        }

    def fetch_page(
        fetcher: Callable[..., tuple[list[Any], int]],
        *,
        page: int,
        page_size: int,
    ) -> tuple[list[Any], dict[str, Any]]:
        normalized_page = max(int(page or 1), 1)
        normalized_page_size = max(int(page_size or 1), 1)
        offset = (normalized_page - 1) * normalized_page_size
        items, total_items = fetcher(limit=normalized_page_size, offset=offset)
        total_pages = max((max(int(total_items or 0), 0) + normalized_page_size - 1) // normalized_page_size, 1)
        if total_items and normalized_page > total_pages:
            normalized_page = total_pages
            offset = (normalized_page - 1) * normalized_page_size
            items, total_items = fetcher(limit=normalized_page_size, offset=offset)
        return items, build_page_context(
            items=items,
            total_items=total_items,
            page=normalized_page,
            page_size=normalized_page_size,
        )

    def iter_all_pages(
        fetcher: Callable[..., tuple[list[Any], int]],
        *,
        page_size: int = 500,
    ):
        offset = 0
        normalized_page_size = max(int(page_size or 1), 1)
        while True:
            batch, total_items = fetcher(limit=normalized_page_size, offset=offset)
            if not batch:
                break
            for item in batch:
                yield item
            offset += len(batch)
            if offset >= max(int(total_items or 0), 0):
                break

    def stream_csv(
        *,
        header: list[str],
        row_iterable: Iterable[list[str]],
        filename: str,
    ) -> CsvStreamingResponse:
        def iterator():
            yield "\ufeff".encode("utf-8")
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow(header)
            yield buffer.getvalue().encode("utf-8")
            buffer.seek(0)
            buffer.truncate(0)
            for row in row_iterable:
                writer.writerow(row)
                yield buffer.getvalue().encode("utf-8")
                buffer.seek(0)
                buffer.truncate(0)

        return CsvStreamingResponse(
            iterator_factory=iterator,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    def build_preflight_snapshot(request: Request, *, include_live: bool = False) -> dict[str, Any]:
        current_org = get_current_org(request)
        config, validation_errors, security_warnings = load_config_summary(current_org)
        recent_jobs = request.app.state.job_repo.list_recent_job_records(limit=100, org_id=current_org.org_id)
        connector_count = request.app.state.connector_repo.count_connectors(org_id=current_org.org_id)
        open_conflicts_total = request.app.state.conflict_repo.list_conflict_records_page(
            limit=1,
            offset=0,
            status="open",
            org_id=current_org.org_id,
        )[1]
        dry_run_completed = any(
            str(job.execution_mode).lower() == "dry_run" and str(job.status).lower() == "success"
            for job in recent_jobs
        )
        apply_completed = any(
            str(job.execution_mode).lower() == "apply" and str(job.status).lower() == "success"
            for job in recent_jobs
        )
        checks: list[dict[str, Any]] = []
        source_provider_name = source_provider_label(config.source_provider if config else "wecom")

        if config and not validation_errors:
            checks.append(
                {
                    "key": "config",
                    "label": "Organization configuration",
                    "status": "success",
                    "detail": "Required {provider} and LDAP settings are complete.",
                    "detail_params": {"provider": source_provider_name},
                    "action_url": "/config",
                }
            )
        else:
            checks.append(
                {
                    "key": "config",
                    "label": "Organization configuration",
                    "status": "error",
                    "detail": validation_errors[0] if validation_errors else "Organization configuration is incomplete.",
                    "action_url": "/config",
                }
            )

        connector_detail = (
            "Organization has {count} dedicated connector(s)."
            if connector_count
            else "No dedicated connectors are configured. The organization will use its primary directory settings."
        )
        checks.append(
            {
                "key": "connectors",
                "label": "Connector routing",
                "status": "success",
                "detail": connector_detail,
                "detail_params": {"count": connector_count} if connector_count else {},
                "action_url": "/advanced-sync",
            }
        )

        breaker_enabled = request.app.state.settings_repo.get_bool(
            "disable_circuit_breaker_enabled",
            False,
            org_id=current_org.org_id,
        )
        checks.append(
            {
                "key": "circuit_breaker",
                "label": "Safety breaker",
                "status": "success" if breaker_enabled else "warning",
                "detail": (
                    "Disable-user circuit breaker is enabled."
                    if breaker_enabled
                    else "Disable-user circuit breaker is still off. Enable it before unattended production runs."
                ),
                "action_url": "/advanced-sync",
            }
        )

        checks.append(
            {
                "key": "dry_run",
                "label": "First dry run",
                "status": "success" if dry_run_completed else "warning",
                "detail": (
                    "At least one successful dry run has been recorded."
                    if dry_run_completed
                    else "No successful dry run has been recorded yet."
                ),
                "action_url": "/jobs",
            }
        )
        checks.append(
            {
                "key": "conflicts",
                "label": "Open conflict queue",
                    "status": "success" if open_conflicts_total == 0 else "warning",
                    "detail": (
                        "No unresolved identity conflicts are waiting."
                        if open_conflicts_total == 0
                        else "There are {count} unresolved conflict(s) that still need review."
                    ),
                    "detail_params": {"count": open_conflicts_total} if open_conflicts_total else {},
                "action_url": "/conflicts",
            }
        )
        checks.append(
            {
                "key": "apply",
                "label": "First apply",
                "status": "success" if apply_completed else "warning",
                "detail": (
                    "At least one successful apply run has been recorded."
                    if apply_completed
                    else "No successful apply run has been recorded yet."
                ),
                "action_url": "/jobs",
            }
        )

        for warning in security_warnings[:2]:
            checks.append(
                {
                    "key": f"security_{len(checks)}",
                    "label": "Security recommendation",
                    "status": "warning",
                    "detail": warning,
                    "action_url": "/config",
                }
            )

        if include_live:
            if (
                config
                and not validation_errors
                and config.source_connector.corpid
                and config.source_connector.corpsecret
            ):
                source_ok, source_message = test_source_connection(
                    config.source_connector.corpid,
                    config.source_connector.corpsecret,
                    config.source_connector.agentid,
                    source_provider=config.source_provider,
                )
                checks.append(
                    {
                        "key": "live_source",
                        "label": "Live {provider} connection",
                        "label_params": {"provider": source_provider_name},
                        "status": "success" if source_ok else "error",
                        "detail": source_message,
                        "action_url": "/config",
                    }
                )
            else:
                if config and not get_source_provider_schema(config.source_provider).implemented:
                    live_source_detail = "Skipped because {provider} is not implemented in this build."
                    live_source_detail_params = {"provider": source_provider_name}
                else:
                    live_source_detail = "Skipped because {provider} credentials are incomplete or still invalid."
                    live_source_detail_params = {"provider": source_provider_name}
                checks.append(
                    {
                        "key": "live_source",
                        "label": "Live {provider} connection",
                        "label_params": {"provider": source_provider_name},
                        "status": "warning",
                        "detail": live_source_detail,
                        "detail_params": live_source_detail_params,
                        "action_url": "/config",
                    }
                )
            if config and not validation_errors and config.ldap.server and config.ldap.domain and config.ldap.username and config.ldap.password:
                ldap_ok, ldap_message = test_ldap_connection(
                    config.ldap.server,
                    config.ldap.domain,
                    config.ldap.username,
                    config.ldap.password,
                    use_ssl=config.ldap.use_ssl,
                    port=config.ldap.port,
                    validate_cert=config.ldap.validate_cert,
                    ca_cert_path=config.ldap.ca_cert_path,
                )
                checks.append(
                    {
                        "key": "live_ldap",
                        "label": "Live LDAP connection",
                        "status": "success" if ldap_ok else "error",
                        "detail": ldap_message,
                        "action_url": "/config",
                    }
                )
            else:
                checks.append(
                    {
                        "key": "live_ldap",
                        "label": "Live LDAP connection",
                        "status": "warning",
                        "detail": "Skipped because LDAP credentials are incomplete or still invalid.",
                        "action_url": "/config",
                    }
                )

        overall_status = summarize_check_status(checks)
        if str(checks[0].get("status")) == "error":
            next_action_url = "/config"
            next_action_label = "Open Organization Config"
        elif include_live and any(
            str(item.get("key") or "") in {"live_source", "live_wecom", "live_ldap"}
            and str(item.get("status") or "") == "error"
            for item in checks
        ):
            next_action_url = "/config"
            next_action_label = "Fix Connectivity"
        elif not dry_run_completed:
            next_action_url = "/jobs"
            next_action_label = "Run First Dry Run"
        elif open_conflicts_total > 0:
            next_action_url = "/conflicts"
            next_action_label = "Review Conflict Queue"
        elif not apply_completed:
            next_action_url = "/jobs"
            next_action_label = "Run First Apply"
        else:
            next_action_url = "/dashboard"
            next_action_label = "Environment Ready"
        return {
            "org_id": current_org.org_id,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "checks": checks,
            "overall_status": overall_status,
            "status_counts": count_check_statuses(checks),
            "has_live_checks": include_live,
            "next_action_url": next_action_url,
            "next_action_label": next_action_label,
            "dry_run_completed": dry_run_completed,
            "apply_completed": apply_completed,
            "open_conflict_count": open_conflicts_total,
        }

    def load_config_summary(
        organization: Optional[OrganizationRecord] = None,
        *,
        config_path_override: Optional[str] = None,
    ) -> tuple[Optional[AppConfig], list[str], list[str]]:
        try:
            if organization is not None:
                config = app.state.org_config_repo.get_app_config(
                    organization.org_id,
                    config_path=config_path_override or organization.config_path or config_path,
                )
            else:
                config = load_sync_config(config_path_override or config_path)
        except Exception as exc:
            return None, [f"Failed to load configuration: {exc}"], []
        is_valid, errors = validate_config(config)
        warnings = run_config_security_self_check(config)
        return config, ([] if is_valid else errors), warnings

    def build_dashboard_data(request: Request) -> dict[str, Any]:
        current_org = get_current_org(request)
        config, validation_errors, security_warnings = load_config_summary(current_org)
        persisted_web_runtime_settings = resolve_web_runtime_settings(request.app.state.settings_repo)
        web_runtime_settings = dict(request.app.state.web_runtime_settings)
        web_runtime_warnings = list(web_runtime_settings.get("warnings", []))
        if web_runtime_requires_restart(
            request.app.state.startup_persisted_web_runtime_settings,
            persisted_web_runtime_settings,
        ):
            web_runtime_warnings.append(
                "Web deployment settings changed in storage. Restart the web process to apply proxy and cookie updates."
            )
        recent_jobs = request.app.state.job_repo.list_recent_job_records(limit=10, org_id=current_org.org_id)
        active_job = request.app.state.job_repo.get_active_job_record(org_id=current_org.org_id)
        db_info = request.app.state.db_manager.runtime_info()
        enabled_rules = request.app.state.exclusion_repo.list_enabled_rule_records(org_id=current_org.org_id)
        bindings = request.app.state.user_binding_repo.list_enabled_binding_records(org_id=current_org.org_id)
        overrides = request.app.state.department_override_repo.list_override_records(org_id=current_org.org_id)
        exception_rules = request.app.state.exception_rule_repo.list_enabled_rule_records(org_id=current_org.org_id)
        preflight_snapshot = merge_saved_preflight_snapshot_data(
            request.session.get("_preflight_snapshot"),
            build_preflight_snapshot(request, include_live=False),
        )
        open_conflicts_count = int(preflight_snapshot.get("open_conflict_count") or 0)
        return {
            "active_job": active_job,
            "recent_jobs": recent_jobs,
            "current_org": current_org,
            "current_org_connector_count": request.app.state.connector_repo.count_connectors(org_id=current_org.org_id),
            "current_org_job_count": request.app.state.job_repo.count_jobs(org_id=current_org.org_id),
            "enabled_organization_count": len(request.app.state.organization_repo.list_organization_records(enabled_only=True)),
            "config_public": config.to_public_dict() if config else None,
            "config_validation_errors": validation_errors,
            "config_security_warnings": security_warnings,
            "db_info": db_info,
            "enabled_rule_count": len(enabled_rules),
            "exception_rule_count": len(exception_rules),
            "open_conflicts_count": open_conflicts_count,
            "user_count": request.app.state.user_repo.count_users(),
            "binding_count": len(bindings),
            "override_count": len(overrides),
            "preflight_summary": preflight_snapshot,
            "getting_started": build_getting_started_view_state(
                current_org_name=current_org.name,
                preflight_snapshot=preflight_snapshot,
                source_provider_name=source_provider_label(config.source_provider if config else "wecom"),
                ui_mode=get_ui_mode(request),
            ),
            "placement_strategy": request.app.state.settings_repo.get_value(
                "user_ou_placement_strategy",
                "source_primary_department",
                org_id=current_org.org_id,
            ),
            "web_runtime": web_runtime_settings,
            "web_runtime_warnings": web_runtime_warnings,
            "sync_runner_error": request.app.state.sync_runner.last_error,
        }

    def validate_binding_target(request: Request, source_user_id: str, ad_username: str) -> Optional[str]:
        current_org = get_current_org(request)
        config = request.app.state.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=get_org_config_path(request),
        )
        if is_protected_ad_account_name(ad_username, config.exclude_accounts):
            return f"AD account {ad_username} is system-protected and cannot be managed by sync."
        existing_by_ad = user_binding_repo.get_binding_record_by_ad_username(
            ad_username,
            org_id=current_org.org_id,
        )
        if existing_by_ad and existing_by_ad.source_user_id != source_user_id:
            return (
                f"AD account {ad_username} is already bound to source user "
                f"{existing_by_ad.source_user_id}. Resolve the existing binding first."
            )
        return None

    def department_exists_in_source_provider(request: Request, department_id: str) -> tuple[bool, Optional[str]]:
        try:
            int(department_id)
        except (TypeError, ValueError):
            return False, "Primary department ID must be an integer"

        try:
            current_org = get_current_org(request)
            config = request.app.state.org_config_repo.get_app_config(
                current_org.org_id,
                config_path=get_org_config_path(request),
            )
            is_valid, _errors = validate_config(config)
            if not is_valid:
                return True, None
            source_provider_name = get_source_provider_display_name(config.source_provider)
            source_provider = build_source_provider(
                app_config=config,
                logger=LOGGER,
            )
            try:
                department_ids = {
                    str(item.department_id)
                    for item in source_provider.list_departments()
                    if item.department_id
                }
            finally:
                source_provider.close()
            if department_id not in department_ids:
                return False, f"{source_provider_name} department ID {department_id} does not exist"
        except Exception as exc:
            LOGGER.warning("failed to validate department existence via source provider: %s", exc)

        return True, None

    def load_department_name_map(request: Request) -> dict[str, str]:
        try:
            organization = get_current_org(request)
            config = request.app.state.org_config_repo.get_app_config(
                organization.org_id,
                config_path=organization.config_path or request.app.state.config_path,
            )
            is_valid, _errors = validate_config(config)
            if not is_valid:
                return {}
            config_fingerprint = json.dumps(
                {"org_id": organization.org_id, "config": config.to_public_dict()},
                ensure_ascii=False,
                sort_keys=True,
            )
            cache_ttl = max(settings_repo.get_int("wecom_department_cache_ttl_seconds", 300), 0)
            now = time.time()
            if (
                cache_ttl > 0
                and department_name_cache["value"]
                and department_name_cache["config_fingerprint"] == config_fingerprint
                and department_name_cache["expires_at"] > now
            ):
                return dict(department_name_cache["value"])
            source_provider = build_source_provider(
                app_config=config,
                logger=LOGGER,
            )
            try:
                department_name_map = {
                    str(item.department_id): str(item.name or "")
                    for item in source_provider.list_departments()
                    if item.department_id
                }
            finally:
                source_provider.close()
            department_name_cache["value"] = dict(department_name_map)
            department_name_cache["config_fingerprint"] = config_fingerprint
            department_name_cache["expires_at"] = now + cache_ttl
            return department_name_map
        except Exception as exc:
            LOGGER.warning("failed to load department names via source provider: %s", exc)
            return {}

    def parse_bulk_exception_rules(raw_text: str) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        errors: list[str] = []
        reader = csv.reader(io.StringIO(raw_text or ""))
        for line_number, columns in enumerate(reader, start=1):
            trimmed_columns = [str(item or "").strip() for item in columns]
            if not any(trimmed_columns):
                continue
            if line_number == 1 and trimmed_columns[:2] == ["rule_type", "match_value"]:
                continue
            if len(trimmed_columns) < 2:
                errors.append(f"Line {line_number}: expected at least rule_type,match_value")
                continue
            enabled_value = trimmed_columns[3] if len(trimmed_columns) >= 4 else "true"
            rows.append(
                {
                    "line_number": line_number,
                    "rule_type": trimmed_columns[0],
                    "match_value": trimmed_columns[1],
                    "notes": trimmed_columns[2] if len(trimmed_columns) >= 3 else "",
                    "is_enabled": _to_bool(enabled_value, True),
                    "expires_at": trimmed_columns[4] if len(trimmed_columns) >= 5 else "",
                    "is_once": _to_bool(trimmed_columns[5], False) if len(trimmed_columns) >= 6 else False,
                }
            )
        return rows, errors

    def normalize_optional_datetime_input(value: str) -> str:
        normalized_value = str(value or "").strip()
        if not normalized_value:
            return ""
        candidate = normalized_value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise ValueError("Invalid date/time format. Use ISO 8601 or datetime-local input.") from exc
        if parsed.tzinfo is None:
            local_tz = datetime.now().astimezone().tzinfo or timezone.utc
            parsed = parsed.replace(tzinfo=local_tz)
        return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")

    def enqueue_replay_request(
        *,
        app: FastAPI,
        request_type: str,
        requested_by: str,
        org_id: str,
        target_scope: str = "full",
        target_id: str = "",
        trigger_reason: str = "",
        payload: Optional[dict[str, Any]] = None,
        execution_mode: str = "apply",
    ) -> Optional[int]:
        if not app.state.settings_repo.get_bool("automatic_replay_enabled", False, org_id=org_id):
            return None
        return app.state.replay_request_repo.enqueue_request(
            request_type=request_type,
            execution_mode=execution_mode,
            requested_by=requested_by,
            org_id=org_id,
            target_scope=target_scope,
            target_id=target_id,
            trigger_reason=trigger_reason,
            payload=payload,
        )

    def build_conflicts_return_url(query: str, status: str, job_id: str) -> str:
        query_parts: dict[str, str] = {}
        if query:
            query_parts["q"] = query
        if status:
            query_parts["status"] = status
        if job_id:
            query_parts["job_id"] = job_id
        if not query_parts:
            return "/conflicts"
        return "/conflicts?" + urlencode(query_parts)

    def resolve_conflict_records_for_source(
        *,
        app: FastAPI,
        job_id: str,
        source_id: str,
        resolution_payload: dict[str, Any],
        actor_username: str,
    ) -> int:
        resolved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        return app.state.conflict_repo.resolve_open_conflicts_for_source(
            job_id=job_id,
            source_id=source_id,
            resolution_payload={
                **resolution_payload,
                "actor_username": actor_username,
            },
            resolved_at=resolved_at,
        )

    def apply_conflict_manual_binding(
        *,
        app: FastAPI,
        conflict: Any,
        ad_username: str,
        actor_username: str,
        org_id: str,
        notes: str = "",
    ) -> tuple[bool, str, int]:
        normalized_ad_username = str(ad_username or "").strip()
        if not conflict.source_id or not normalized_ad_username:
            return False, "Conflict does not support manual binding", 0

        conflict_message = None
        config = app.state.org_config_repo.get_app_config(org_id, config_path="")
        if is_protected_ad_account_name(normalized_ad_username, config.exclude_accounts):
            conflict_message = (
                f"AD account {normalized_ad_username} is system-protected and cannot be managed by sync."
            )
        else:
            existing_by_ad = app.state.user_binding_repo.get_binding_record_by_ad_username(
                normalized_ad_username,
                org_id=org_id,
            )
            if existing_by_ad and existing_by_ad.source_user_id != conflict.source_id:
                conflict_message = (
                    f"AD account {normalized_ad_username} is already bound to source user "
                    f"{existing_by_ad.source_user_id}. Resolve the existing binding first."
                )
        if conflict_message:
            return False, conflict_message, 0

        binding_notes = str(notes or "").strip() or f"resolved from conflict {conflict.id}"
        app.state.user_binding_repo.upsert_binding_for_source_user(
            conflict.source_id,
            normalized_ad_username,
            org_id=org_id,
            source="manual",
            notes=binding_notes,
            preserve_manual=False,
        )
        resolved_count = resolve_conflict_records_for_source(
            app=app,
            job_id=conflict.job_id,
            source_id=conflict.source_id,
            resolution_payload={
                "action": "manual_binding",
                "ad_username": normalized_ad_username,
                "notes": binding_notes,
                "source_conflict_id": conflict.id,
            },
            actor_username=actor_username,
        )
        enqueue_replay_request(
            app=app,
            request_type="conflict_resolution",
            requested_by=actor_username,
            org_id=org_id,
            target_scope="source_user",
            target_id=conflict.source_id,
            trigger_reason="manual_binding_resolved",
            payload={
                "conflict_id": conflict.id,
                "job_id": conflict.job_id,
                "action": "manual_binding",
                "ad_username": normalized_ad_username,
            },
        )
        return True, normalized_ad_username, resolved_count

    def apply_conflict_skip_user_sync(
        *,
        app: FastAPI,
        conflict: Any,
        actor_username: str,
        org_id: str,
        notes: str = "",
    ) -> tuple[bool, str, int]:
        if not conflict.source_id:
            return False, "Conflict does not have a source user to whitelist", 0

        rule_notes = str(notes or "").strip() or f"resolved from conflict {conflict.id}"
        app.state.exception_rule_repo.upsert_rule(
            rule_type="skip_user_sync",
            match_value=conflict.source_id,
            org_id=org_id,
            notes=rule_notes,
            is_enabled=True,
        )
        resolved_count = resolve_conflict_records_for_source(
            app=app,
            job_id=conflict.job_id,
            source_id=conflict.source_id,
            resolution_payload={
                "action": "skip_user_sync",
                "notes": rule_notes,
                "source_conflict_id": conflict.id,
            },
            actor_username=actor_username,
        )
        enqueue_replay_request(
            app=app,
            request_type="conflict_resolution",
            requested_by=actor_username,
            org_id=org_id,
            target_scope="source_user",
            target_id=conflict.source_id,
            trigger_reason="skip_user_sync_added",
            payload={
                "conflict_id": conflict.id,
                "job_id": conflict.job_id,
                "action": "skip_user_sync",
            },
        )
        return True, rule_notes, resolved_count

    def apply_conflict_recommendation(
        *,
        app: FastAPI,
        conflict: Any,
        actor_username: str,
        org_id: str,
        confirmation_reason: str = "",
    ) -> tuple[bool, str, int, Optional[dict[str, Any]]]:
        recommendation = recommend_conflict_resolution(conflict)
        if not recommendation:
            return False, "No recommendation is available for this conflict", 0, None

        action = str(recommendation.get("action") or "").strip().lower()
        reason = str(recommendation.get("reason") or "").strip()
        normalized_confirmation_reason = str(confirmation_reason or "").strip()
        if recommendation_requires_confirmation(recommendation) and not normalized_confirmation_reason:
            return False, "This recommendation requires a confirmation reason before it can be applied", 0, recommendation

        notes = normalized_confirmation_reason or reason or f"recommended resolution from conflict {conflict.id}"
        if action == "manual_binding":
            ok, detail, resolved_count = apply_conflict_manual_binding(
                app=app,
                conflict=conflict,
                ad_username=str(recommendation.get("ad_username") or ""),
                actor_username=actor_username,
                org_id=org_id,
                notes=notes,
            )
            return ok, detail, resolved_count, recommendation
        if action == "skip_user_sync":
            ok, detail, resolved_count = apply_conflict_skip_user_sync(
                app=app,
                conflict=conflict,
                actor_username=actor_username,
                org_id=org_id,
                notes=notes,
            )
            return ok, detail, resolved_count, recommendation
        return False, f"Unsupported recommendation action: {action or '-'}", 0, recommendation

    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "version": APP_VERSION}

    @app.get("/readyz")
    def readyz(request: Request):
        db_ok = False
        db_error = ""
        try:
            with request.app.state.db_manager.connection() as conn:
                conn.execute("SELECT 1").fetchone()
            db_ok = True
        except Exception as exc:  # pragma: no cover - defensive reporting path
            db_error = str(exc)

        default_org = request.app.state.organization_repo.get_organization_record("default")
        static_assets_ok = STATIC_DIR.exists()
        admin_bootstrapped = request.app.state.user_repo.has_any_user()
        ready = db_ok and static_assets_ok and default_org is not None and admin_bootstrapped
        status = "ready" if ready else ("setup_required" if db_ok and static_assets_ok and default_org else "degraded")
        payload = {
            "status": status,
            "version": APP_VERSION,
            "checks": {
                "database": db_ok,
                "static_assets": static_assets_ok,
                "default_organization": default_org is not None,
                "admin_bootstrapped": admin_bootstrapped,
            },
            "db_path": request.app.state.db_manager.db_path,
            "setup_url": "/setup" if not admin_bootstrapped else "",
        }
        if db_error:
            payload["database_error"] = db_error
        return JSONResponse(payload, status_code=200 if ready else 503)

    @app.get("/favicon.ico")
    def favicon(request: Request):
        if FAVICON_PATH.exists():
            return FileResponse(str(FAVICON_PATH), media_type="image/x-icon")
        if LEGACY_FAVICON_PATH.exists():
            return FileResponse(str(LEGACY_FAVICON_PATH), media_type="image/x-icon")
        return Response(status_code=204)

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        if not request.app.state.user_repo.has_any_user():
            return RedirectResponse(url="/setup", status_code=303)
        if get_current_user(request):
            return RedirectResponse(url="/dashboard", status_code=303)
        return RedirectResponse(url="/login", status_code=303)

    @app.get("/setup", response_class=HTMLResponse)
    def setup_page(request: Request):
        if request.app.state.user_repo.has_any_user():
            return RedirectResponse(url="/login", status_code=303)
        return render(request, "setup.html", title="Initial Administrator Setup")

    @app.post("/setup")
    def setup_submit(
        request: Request,
        csrf_token: str = Form(""),
        username: str = Form(...),
        password: str = Form(...),
        confirm_password: str = Form(...),
    ):
        if request.app.state.user_repo.has_any_user():
            return RedirectResponse(url="/login", status_code=303)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/setup")
        if csrf_error:
            return csrf_error

        username = username.strip()
        if not username:
            flash(request, "error", "Administrator username is required")
            return RedirectResponse(url="/setup", status_code=303)
        if password != confirm_password:
            flash(request, "error", "Passwords do not match")
            return RedirectResponse(url="/setup", status_code=303)
        password_error = validate_admin_password(request, password)
        if password_error:
            flash(request, "error", password_error)
            return RedirectResponse(url="/setup", status_code=303)

        request.app.state.user_repo.create_user(
            username=username,
            password_hash=hash_password(password),
            role="super_admin",
            is_enabled=True,
        )
        request.app.state.audit_repo.add_log(
            actor_username=username,
            action_type="auth.setup",
            target_type="web_admin_user",
            target_id=username,
            result="success",
            message="Completed initial administrator setup",
        )
        flash(request, "success", "Setup completed. Please sign in.")
        return RedirectResponse(url="/login", status_code=303)

    @app.get("/login", response_class=HTMLResponse)
    def login_page(request: Request):
        if not request.app.state.user_repo.has_any_user():
            return RedirectResponse(url="/setup", status_code=303)
        if get_current_user(request):
            return RedirectResponse(url="/dashboard", status_code=303)
        return render(request, "login.html", title="Sign In")

    @app.post("/login")
    def login_submit(
        request: Request,
        csrf_token: str = Form(""),
        username: str = Form(...),
        password: str = Form(...),
    ):
        if not request.app.state.user_repo.has_any_user():
            return RedirectResponse(url="/setup", status_code=303)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/login")
        if csrf_error:
            return csrf_error

        login_name = username.strip()
        client_ip = get_client_ip(request)
        is_locked, retry_after = request.app.state.login_rate_limiter.check(login_name, client_ip)
        if is_locked:
            request.app.state.audit_repo.add_log(
                actor_username=login_name or None,
                action_type="auth.login",
                target_type="web_admin_user",
                target_id=login_name,
                result="error",
                message="Sign-in blocked by rate limiter",
                payload={"ip": client_ip, "rate_limited": True, "retry_after_seconds": retry_after},
            )
            flash_t(request, "error", "Too many failed login attempts. Retry in {retry_after} seconds.", retry_after=retry_after)
            return RedirectResponse(url="/login", status_code=303)

        user = request.app.state.user_repo.get_user_record_by_username(login_name)
        if not user or not user.is_enabled or not verify_password(password, user.password_hash):
            locked_now, retry_after = request.app.state.login_rate_limiter.record_failure(login_name, client_ip)
            request.app.state.audit_repo.add_log(
                actor_username=login_name or None,
                action_type="auth.login",
                target_type="web_admin_user",
                target_id=login_name,
                result="error",
                message="Sign-in failed",
                payload={"ip": client_ip, "rate_limited": locked_now, "retry_after_seconds": retry_after},
            )
            if locked_now:
                flash_t(
                    request,
                    "error",
                    "Too many failed login attempts. Retry in {retry_after} seconds.",
                    retry_after=retry_after,
                )
            else:
                flash(request, "error", "Invalid username or password")
            return RedirectResponse(url="/login", status_code=303)

        request.session.clear()
        request.session["username"] = user.username
        request.session["role"] = normalize_role(user.role, default="operator")
        rotate_csrf_token(request.session)
        request.app.state.login_rate_limiter.clear(user.username, client_ip)
        request.app.state.user_repo.update_last_login(user.username)
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="auth.login",
            target_type="web_admin_user",
            target_id=user.username,
            result="success",
            message="Sign-in succeeded",
            payload={"ip": client_ip},
        )
        return RedirectResponse(url="/dashboard", status_code=303)

    @app.post("/logout")
    def logout(request: Request, csrf_token: str = Form("")):
        user = get_current_user(request)
        if not user:
            request.session.clear()
            return RedirectResponse(url="/login", status_code=303)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/dashboard")
        if csrf_error:
            return csrf_error

        username = user.username
        request.session.clear()
        request.app.state.audit_repo.add_log(
            actor_username=username,
            action_type="auth.logout",
            target_type="web_admin_user",
            target_id=username,
            result="success",
            message="Signed out",
        )
        return RedirectResponse(url="/login", status_code=303)

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard(request: Request):
        user = require_capability(request, "dashboard.read")
        if isinstance(user, RedirectResponse):
            return user
        dashboard_data = build_dashboard_data(request)
        return render(
            request,
            "dashboard.html",
            page="dashboard",
            title="Dashboard",
            dashboard=SimpleNamespace(**dashboard_data),
            **dashboard_data,
        )

    @app.get("/getting-started", response_class=HTMLResponse)
    def getting_started_page(request: Request):
        user = require_capability(request, "dashboard.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        current_config, _, _ = load_config_summary(current_org)
        preflight_snapshot = merge_saved_preflight_snapshot_data(
            request.session.get("_preflight_snapshot"),
            build_preflight_snapshot(request, include_live=False),
        )
        return render(
            request,
            "getting_started.html",
            page="getting-started",
            title="Getting Started",
            preflight_summary=preflight_snapshot,
            getting_started=build_getting_started_view_state(
                current_org_name=current_org.name,
                preflight_snapshot=preflight_snapshot,
                source_provider_name=source_provider_label(current_config.source_provider if current_config else "wecom"),
                ui_mode=get_ui_mode(request),
            ),
        )

    @app.post("/preflight/run")
    def run_preflight(
        request: Request,
        csrf_token: str = Form(""),
        return_url: str = Form("/dashboard"),
    ):
        user = require_capability(request, "dashboard.read")
        if isinstance(user, RedirectResponse):
            return user
        fallback_url = _safe_redirect_target(return_url, "/dashboard")
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        snapshot = build_preflight_snapshot(request, include_live=True)
        request.session["_preflight_snapshot"] = snapshot
        flash_t(
            request,
            "success" if snapshot["overall_status"] == "success" else ("warning" if snapshot["overall_status"] == "warning" else "error"),
            "Preflight finished with status {status}",
            status=str(snapshot["overall_status"]).upper(),
        )
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/ui-mode")
    def switch_ui_mode(
        request: Request,
        csrf_token: str = Form(""),
        ui_mode: str = Form("basic"),
        return_url: str = Form("/dashboard"),
    ):
        user = require_user(request)
        if isinstance(user, RedirectResponse):
            return user
        fallback_url = _safe_redirect_target(return_url, "/dashboard")
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        request.session["ui_mode"] = normalize_ui_mode(ui_mode)
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.get("/advanced-sync", response_class=HTMLResponse)
    def advanced_sync_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        connectors = list_org_connector_records(request)

        return render(
            request,
            "advanced_sync.html",
            page="advanced-sync",
            title="Advanced Sync",
            connectors=connectors,
            connector_config_sources={
                record.connector_id: describe_connector_config_source(record)
                for record in connectors
            },
            attribute_mappings=list_org_attribute_mapping_rules(request),
            custom_group_bindings=request.app.state.custom_group_binding_repo.list_active_records(org_id=current_org.org_id),
            offboarding_records=request.app.state.offboarding_repo.list_pending_records(org_id=current_org.org_id),
            lifecycle_records=request.app.state.lifecycle_repo.list_pending_records(org_id=current_org.org_id),
            replay_requests=request.app.state.replay_request_repo.list_request_records(
                status="pending",
                limit=20,
                org_id=current_org.org_id,
            ),
            current_org=current_org,
            policy_settings={
                "offboarding_grace_days": request.app.state.settings_repo.get_int(
                    "offboarding_grace_days",
                    0,
                    org_id=current_org.org_id,
                ),
                "offboarding_notify_managers": request.app.state.settings_repo.get_bool(
                    "offboarding_notify_managers",
                    False,
                    org_id=current_org.org_id,
                ),
                "advanced_connector_routing_enabled": request.app.state.settings_repo.get_bool(
                    "advanced_connector_routing_enabled",
                    False,
                    org_id=current_org.org_id,
                ),
                "attribute_mapping_enabled": request.app.state.settings_repo.get_bool(
                    "attribute_mapping_enabled",
                    False,
                    org_id=current_org.org_id,
                ),
                "write_back_enabled": request.app.state.settings_repo.get_bool(
                    "write_back_enabled",
                    False,
                    org_id=current_org.org_id,
                ),
                "custom_group_sync_enabled": request.app.state.settings_repo.get_bool(
                    "custom_group_sync_enabled",
                    False,
                    org_id=current_org.org_id,
                ),
                "offboarding_lifecycle_enabled": request.app.state.settings_repo.get_bool(
                    "offboarding_lifecycle_enabled",
                    False,
                    org_id=current_org.org_id,
                ),
                "rehire_restore_enabled": request.app.state.settings_repo.get_bool(
                    "rehire_restore_enabled",
                    False,
                    org_id=current_org.org_id,
                ),
                "automatic_replay_enabled": request.app.state.settings_repo.get_bool(
                    "automatic_replay_enabled",
                    False,
                    org_id=current_org.org_id,
                ),
                "future_onboarding_enabled": request.app.state.settings_repo.get_bool(
                    "future_onboarding_enabled",
                    False,
                    org_id=current_org.org_id,
                ),
                "future_onboarding_start_field": request.app.state.settings_repo.get_value(
                    "future_onboarding_start_field",
                    "hire_date",
                    org_id=current_org.org_id,
                ),
                "contractor_lifecycle_enabled": request.app.state.settings_repo.get_bool(
                    "contractor_lifecycle_enabled",
                    False,
                    org_id=current_org.org_id,
                ),
                "lifecycle_employment_type_field": request.app.state.settings_repo.get_value(
                    "lifecycle_employment_type_field",
                    "employment_type",
                    org_id=current_org.org_id,
                ),
                "contractor_end_field": request.app.state.settings_repo.get_value(
                    "contractor_end_field",
                    "contract_end_date",
                    org_id=current_org.org_id,
                ),
                "lifecycle_sponsor_field": request.app.state.settings_repo.get_value(
                    "lifecycle_sponsor_field",
                    "sponsor_userid",
                    org_id=current_org.org_id,
                ),
                "contractor_type_values": request.app.state.settings_repo.get_value(
                    "contractor_type_values",
                    "contractor,intern,vendor,temp",
                    org_id=current_org.org_id,
                ),
                "disable_circuit_breaker_enabled": request.app.state.settings_repo.get_bool(
                    "disable_circuit_breaker_enabled",
                    False,
                    org_id=current_org.org_id,
                ),
                "disable_circuit_breaker_percent": request.app.state.settings_repo.get_float(
                    "disable_circuit_breaker_percent",
                    5.0,
                    org_id=current_org.org_id,
                ),
                "disable_circuit_breaker_min_count": request.app.state.settings_repo.get_int(
                    "disable_circuit_breaker_min_count",
                    10,
                    org_id=current_org.org_id,
                ),
                "disable_circuit_breaker_requires_approval": request.app.state.settings_repo.get_bool(
                    "disable_circuit_breaker_requires_approval",
                    True,
                    org_id=current_org.org_id,
                ),
                "managed_group_type": request.app.state.settings_repo.get_value(
                    "managed_group_type",
                    "security",
                    org_id=current_org.org_id,
                ),
                "managed_group_mail_domain": request.app.state.settings_repo.get_value(
                    "managed_group_mail_domain",
                    "",
                    org_id=current_org.org_id,
                ),
                "custom_group_ou_path": request.app.state.settings_repo.get_value(
                    "custom_group_ou_path",
                    "Managed Groups",
                    org_id=current_org.org_id,
                ),
            },
            mapping_direction_options=[
                ("source_to_ad", ATTRIBUTE_MAPPING_DIRECTION_LABELS["source_to_ad"]),
                ("ad_to_source", ATTRIBUTE_MAPPING_DIRECTION_LABELS["ad_to_source"]),
            ],
            mapping_direction_labels=ATTRIBUTE_MAPPING_DIRECTION_LABELS,
            mapping_mode_options=[(value, value) for value in ATTRIBUTE_SYNC_MODES],
            group_type_options=[(value, value.replace("_", " ").title()) for value in MANAGED_GROUP_TYPES],
        )

    @app.post("/advanced-sync/policies")
    def advanced_sync_policy_submit(
        request: Request,
        csrf_token: str = Form(""),
        offboarding_grace_days: int = Form(0),
        offboarding_notify_managers: Optional[str] = Form(None),
        advanced_connector_routing_enabled: Optional[str] = Form(None),
        attribute_mapping_enabled: Optional[str] = Form(None),
        write_back_enabled: Optional[str] = Form(None),
        custom_group_sync_enabled: Optional[str] = Form(None),
        offboarding_lifecycle_enabled: Optional[str] = Form(None),
        rehire_restore_enabled: Optional[str] = Form(None),
        automatic_replay_enabled: Optional[str] = Form(None),
        future_onboarding_enabled: Optional[str] = Form(None),
        future_onboarding_start_field: str = Form("hire_date"),
        contractor_lifecycle_enabled: Optional[str] = Form(None),
        lifecycle_employment_type_field: str = Form("employment_type"),
        contractor_end_field: str = Form("contract_end_date"),
        lifecycle_sponsor_field: str = Form("sponsor_userid"),
        contractor_type_values: str = Form("contractor,intern,vendor,temp"),
        disable_circuit_breaker_enabled: Optional[str] = Form(None),
        disable_circuit_breaker_percent: float = Form(5.0),
        disable_circuit_breaker_min_count: int = Form(10),
        disable_circuit_breaker_requires_approval: Optional[str] = Form(None),
        managed_group_type: str = Form("security"),
        managed_group_mail_domain: str = Form(""),
        custom_group_ou_path: str = Form("Managed Groups"),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        request.app.state.settings_repo.set_value(
            "offboarding_grace_days",
            str(max(int(offboarding_grace_days or 0), 0)),
            "int",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "offboarding_notify_managers",
            str(_to_bool(offboarding_notify_managers, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "advanced_connector_routing_enabled",
            str(_to_bool(advanced_connector_routing_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "attribute_mapping_enabled",
            str(_to_bool(attribute_mapping_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "write_back_enabled",
            str(_to_bool(write_back_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "custom_group_sync_enabled",
            str(_to_bool(custom_group_sync_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "offboarding_lifecycle_enabled",
            str(_to_bool(offboarding_lifecycle_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "rehire_restore_enabled",
            str(_to_bool(rehire_restore_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "automatic_replay_enabled",
            str(_to_bool(automatic_replay_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "future_onboarding_enabled",
            str(_to_bool(future_onboarding_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "future_onboarding_start_field",
            future_onboarding_start_field.strip() or "hire_date",
            "string",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "contractor_lifecycle_enabled",
            str(_to_bool(contractor_lifecycle_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "lifecycle_employment_type_field",
            lifecycle_employment_type_field.strip() or "employment_type",
            "string",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "contractor_end_field",
            contractor_end_field.strip() or "contract_end_date",
            "string",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "lifecycle_sponsor_field",
            lifecycle_sponsor_field.strip() or "sponsor_userid",
            "string",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "contractor_type_values",
            contractor_type_values.strip() or "contractor,intern,vendor,temp",
            "string",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "disable_circuit_breaker_enabled",
            str(_to_bool(disable_circuit_breaker_enabled, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "disable_circuit_breaker_percent",
            str(max(float(disable_circuit_breaker_percent or 0.0), 0.0)),
            "float",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "disable_circuit_breaker_min_count",
            str(max(int(disable_circuit_breaker_min_count or 0), 0)),
            "int",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "disable_circuit_breaker_requires_approval",
            str(_to_bool(disable_circuit_breaker_requires_approval, False)).lower(),
            "bool",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "managed_group_type",
            managed_group_type,
            "string",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "managed_group_mail_domain",
            managed_group_mail_domain.strip(),
            "string",
            org_id=current_org.org_id,
        )
        request.app.state.settings_repo.set_value(
            "custom_group_ou_path",
            custom_group_ou_path.strip(),
            "string",
            org_id=current_org.org_id,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="advanced_sync.policy_update",
            target_type="settings",
            target_id="advanced_sync",
            result="success",
            message="Updated advanced sync policies",
            payload={
                "org_id": current_org.org_id,
                "offboarding_grace_days": max(int(offboarding_grace_days or 0), 0),
                "advanced_connector_routing_enabled": _to_bool(advanced_connector_routing_enabled, False),
                "attribute_mapping_enabled": _to_bool(attribute_mapping_enabled, False),
                "write_back_enabled": _to_bool(write_back_enabled, False),
                "custom_group_sync_enabled": _to_bool(custom_group_sync_enabled, False),
                "offboarding_lifecycle_enabled": _to_bool(offboarding_lifecycle_enabled, False),
                "rehire_restore_enabled": _to_bool(rehire_restore_enabled, False),
                "automatic_replay_enabled": _to_bool(automatic_replay_enabled, False),
                "future_onboarding_enabled": _to_bool(future_onboarding_enabled, False),
                "future_onboarding_start_field": future_onboarding_start_field.strip() or "hire_date",
                "contractor_lifecycle_enabled": _to_bool(contractor_lifecycle_enabled, False),
                "lifecycle_employment_type_field": lifecycle_employment_type_field.strip() or "employment_type",
                "contractor_end_field": contractor_end_field.strip() or "contract_end_date",
                "lifecycle_sponsor_field": lifecycle_sponsor_field.strip() or "sponsor_userid",
                "contractor_type_values": contractor_type_values.strip() or "contractor,intern,vendor,temp",
                "offboarding_notify_managers": _to_bool(offboarding_notify_managers, False),
                "disable_circuit_breaker_enabled": _to_bool(disable_circuit_breaker_enabled, False),
                "disable_circuit_breaker_percent": max(float(disable_circuit_breaker_percent or 0.0), 0.0),
                "disable_circuit_breaker_min_count": max(int(disable_circuit_breaker_min_count or 0), 0),
                "disable_circuit_breaker_requires_approval": _to_bool(
                    disable_circuit_breaker_requires_approval,
                    False,
                ),
                "managed_group_type": managed_group_type,
            },
        )
        flash_t(request, "success", "Advanced sync policies saved")
        return RedirectResponse(url="/advanced-sync", status_code=303)

    @app.post("/advanced-sync/connectors")
    def advanced_sync_connector_submit(
        request: Request,
        csrf_token: str = Form(""),
        connector_id: str = Form(""),
        name: str = Form(""),
        config_path: str = Form(""),
        ldap_server: str = Form(""),
        ldap_domain: str = Form(""),
        ldap_username: str = Form(""),
        ldap_password: str = Form(""),
        ldap_use_ssl: str = Form(""),
        ldap_port: str = Form(""),
        ldap_validate_cert: str = Form(""),
        ldap_ca_cert_path: str = Form(""),
        default_password: str = Form(""),
        force_change_password: str = Form(""),
        password_complexity: str = Form(""),
        root_department_ids: str = Form(""),
        username_template: str = Form(""),
        disabled_users_ou: str = Form("Disabled Users"),
        group_type: str = Form("security"),
        group_mail_domain: str = Form(""),
        custom_group_ou_path: str = Form("Managed Groups"),
        managed_tag_ids: str = Form(""),
        managed_external_chat_ids: str = Form(""),
        is_enabled: Optional[str] = Form(None),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        try:
            request.app.state.connector_repo.upsert_connector(
                connector_id=connector_id.strip(),
                org_id=current_org.org_id,
                name=name.strip() or connector_id.strip(),
                config_path=config_path.strip(),
                ldap_server=ldap_server.strip(),
                ldap_domain=ldap_domain.strip(),
                ldap_username=ldap_username.strip(),
                ldap_password=ldap_password.strip(),
                ldap_use_ssl=ldap_use_ssl.strip(),
                ldap_port=ldap_port.strip(),
                ldap_validate_cert=ldap_validate_cert.strip(),
                ldap_ca_cert_path=ldap_ca_cert_path.strip(),
                default_password=default_password.strip(),
                force_change_password=force_change_password.strip(),
                password_complexity=password_complexity.strip(),
                root_department_ids=[int(item) for item in _split_csv_values(root_department_ids)],
                username_template=username_template.strip(),
                disabled_users_ou=disabled_users_ou.strip(),
                group_type=group_type.strip(),
                group_mail_domain=group_mail_domain.strip(),
                custom_group_ou_path=custom_group_ou_path.strip(),
                managed_tag_ids=_split_csv_values(managed_tag_ids),
                managed_external_chat_ids=_split_csv_values(managed_external_chat_ids),
                is_enabled=_to_bool(is_enabled, True),
            )
        except Exception as exc:
            flash_t(request, "error", "Failed to save connector: {error}", error=str(exc))
            return RedirectResponse(url="/advanced-sync", status_code=303)
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="advanced_sync.connector_upsert",
            target_type="connector",
            target_id=connector_id.strip(),
            result="success",
            message="Saved connector definition",
            payload={
                "org_id": current_org.org_id,
                "root_department_ids": _split_csv_values(root_department_ids),
                "legacy_import_path": config_path.strip(),
                "ldap_server": ldap_server.strip(),
                "ldap_domain": ldap_domain.strip(),
                "has_database_overrides": any(
                    [
                        ldap_server.strip(),
                        ldap_domain.strip(),
                        ldap_username.strip(),
                        ldap_password.strip(),
                        ldap_use_ssl.strip(),
                        ldap_port.strip(),
                        ldap_validate_cert.strip(),
                        ldap_ca_cert_path.strip(),
                        default_password.strip(),
                        force_change_password.strip(),
                        password_complexity.strip(),
                    ]
                ),
            },
        )
        flash_t(request, "success", "Connector {connector_id} saved", connector_id=connector_id.strip())
        return RedirectResponse(url="/advanced-sync", status_code=303)

    @app.post("/advanced-sync/connectors/{connector_id}/toggle")
    def advanced_sync_connector_toggle(
        request: Request,
        connector_id: str,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error
        record = request.app.state.connector_repo.get_connector_record(connector_id, org_id=get_current_org(request).org_id)
        if not record:
            flash(request, "error", "Connector not found")
            return RedirectResponse(url="/advanced-sync", status_code=303)
        request.app.state.connector_repo.set_enabled(
            connector_id,
            not record.is_enabled,
            org_id=get_current_org(request).org_id,
        )
        flash_t(
            request,
            "success",
            "Connector {connector_id} enabled" if not record.is_enabled else "Connector {connector_id} disabled",
            connector_id=connector_id,
        )
        return RedirectResponse(url="/advanced-sync", status_code=303)

    @app.post("/advanced-sync/connectors/{connector_id}/delete")
    def advanced_sync_connector_delete(
        request: Request,
        connector_id: str,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error
        request.app.state.connector_repo.delete_connector(connector_id, org_id=get_current_org(request).org_id)
        flash_t(request, "success", "Connector {connector_id} deleted", connector_id=connector_id)
        return RedirectResponse(url="/advanced-sync", status_code=303)

    @app.post("/advanced-sync/mappings")
    def advanced_sync_mapping_submit(
        request: Request,
        csrf_token: str = Form(""),
        connector_id: str = Form(""),
        direction: str = Form("source_to_ad"),
        source_field: str = Form(""),
        target_field: str = Form(""),
        transform_template: str = Form(""),
        sync_mode: str = Form("replace"),
        notes: str = Form(""),
        is_enabled: Optional[str] = Form(None),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        normalized_connector_id = connector_id.strip()
        if normalized_connector_id and not request.app.state.connector_repo.get_connector_record(
            normalized_connector_id,
            org_id=current_org.org_id,
        ):
            flash_t(request, "error", "Connector {connector_id} was not found in the selected organization", connector_id=normalized_connector_id)
            return RedirectResponse(url="/advanced-sync", status_code=303)
        try:
            request.app.state.attribute_mapping_repo.upsert_rule(
                connector_id=normalized_connector_id,
                direction=normalize_mapping_direction(direction),
                source_field=source_field.strip(),
                target_field=target_field.strip(),
                transform_template=transform_template.strip(),
                sync_mode=sync_mode.strip(),
                notes=notes.strip(),
                is_enabled=_to_bool(is_enabled, True),
                org_id=current_org.org_id,
            )
        except Exception as exc:
            flash_t(request, "error", "Failed to save mapping rule: {error}", error=str(exc))
            return RedirectResponse(url="/advanced-sync", status_code=303)
        flash_t(request, "success", "Mapping rule saved")
        return RedirectResponse(url="/advanced-sync", status_code=303)

    @app.post("/advanced-sync/mappings/{rule_id}/delete")
    def advanced_sync_mapping_delete(
        request: Request,
        rule_id: int,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        if not request.app.state.attribute_mapping_repo.get_rule_record(rule_id, org_id=current_org.org_id):
            flash_t(request, "error", "Mapping rule not found in the selected organization")
            return RedirectResponse(url="/advanced-sync", status_code=303)
        request.app.state.attribute_mapping_repo.delete_rule(rule_id, org_id=current_org.org_id)
        flash_t(request, "success", "Mapping rule deleted")
        return RedirectResponse(url="/advanced-sync", status_code=303)

    register_job_routes(
        app,
        enqueue_replay_request=enqueue_replay_request,
        fetch_page=fetch_page,
        flash=flash,
        flash_t=flash_t,
        get_current_org=get_current_org,
        get_ui_language=get_ui_language,
        parse_page_number=parse_page_number,
        reject_invalid_csrf=reject_invalid_csrf,
        render=render,
        require_capability=require_capability,
        translate_text=translate_text,
    )

    register_organization_routes(
        app,
        export_organization_bundle=export_organization_bundle,
        flash=flash,
        flash_t=flash_t,
        import_organization_bundle=import_organization_bundle,
        reject_invalid_csrf=reject_invalid_csrf,
        render=render,
        require_capability=require_capability,
        require_user=require_user,
        safe_redirect_target=_safe_redirect_target,
        to_bool=_to_bool,
    )

    register_config_routes(
        app,
        apply_config_submission=apply_config_submission,
        build_config_change_preview=build_config_change_preview,
        build_config_editable_override=build_config_editable_override,
        build_config_page_context=build_config_page_context,
        build_config_submission=build_config_submission,
        config_preview_session_key=CONFIG_PREVIEW_SESSION_KEY,
        flash=flash,
        reject_invalid_csrf=reject_invalid_csrf,
        render=render,
        require_capability=require_capability,
        resolve_web_runtime_settings=resolve_web_runtime_settings,
        web_runtime_requires_restart=web_runtime_requires_restart,
    )

    register_mapping_routes(
        app,
        department_exists_in_source_provider=department_exists_in_source_provider,
        fetch_page=fetch_page,
        flash=flash,
        flash_t=flash_t,
        get_current_org=get_current_org,
        iter_all_pages=iter_all_pages,
        load_department_name_map=load_department_name_map,
        parse_bulk_bindings=parse_bulk_bindings,
        parse_page_number=parse_page_number,
        reject_invalid_csrf=reject_invalid_csrf,
        render=render,
        require_capability=require_capability,
        resolve_remembered_filters=resolve_remembered_filters,
        stream_csv=stream_csv,
        to_bool=_to_bool,
        validate_binding_target=validate_binding_target,
    )

    register_exception_routes(
        app,
        department_exists_in_source_provider=department_exists_in_source_provider,
        enqueue_replay_request=enqueue_replay_request,
        fetch_page=fetch_page,
        flash=flash,
        flash_t=flash_t,
        get_current_org=get_current_org,
        iter_all_pages=iter_all_pages,
        load_department_name_map=load_department_name_map,
        normalize_optional_datetime_input=normalize_optional_datetime_input,
        parse_bulk_exception_rules=parse_bulk_exception_rules,
        parse_page_number=parse_page_number,
        reject_invalid_csrf=reject_invalid_csrf,
        render=render,
        require_capability=require_capability,
        resolve_remembered_filters=resolve_remembered_filters,
        stream_csv=stream_csv,
        to_bool=_to_bool,
    )

    register_conflict_routes(
        app,
        apply_conflict_manual_binding=apply_conflict_manual_binding,
        apply_conflict_recommendation=apply_conflict_recommendation,
        apply_conflict_skip_user_sync=apply_conflict_skip_user_sync,
        build_conflicts_return_url=build_conflicts_return_url,
        fetch_page=fetch_page,
        flash=flash,
        flash_t=flash_t,
        get_current_org=get_current_org,
        parse_page_number=parse_page_number,
        reject_invalid_csrf=reject_invalid_csrf,
        render=render,
        require_capability=require_capability,
        resolve_remembered_filters=resolve_remembered_filters,
        to_text=_to_text,
    )

    register_admin_routes(
        app,
        fetch_page=fetch_page,
        flash=flash,
        flash_t=flash_t,
        get_current_org=get_current_org,
        hash_password=hash_password,
        parse_page_number=parse_page_number,
        reject_invalid_csrf=reject_invalid_csrf,
        render=render,
        require_capability=require_capability,
        resolve_remembered_filters=resolve_remembered_filters,
        validate_admin_password=validate_admin_password,
        verify_password=verify_password,
    )

    return app
