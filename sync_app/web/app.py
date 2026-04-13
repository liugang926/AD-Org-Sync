from __future__ import annotations

import logging
import secrets
from pathlib import Path
from typing import Any, Callable, Optional

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from sync_app.core.common import APP_VERSION
from sync_app.core.config import test_ldap_connection, test_source_connection, validate_config
from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.core.conflict_recommendations import (
    recommend_conflict_resolution,
    recommendation_requires_confirmation,
)
from sync_app.core.sync_policies import normalize_mapping_direction
from sync_app.core.models import (
    AppConfig,
    OrganizationRecord,
    WebAdminUserRecord,
)
from sync_app.providers.source import (
    build_source_provider,
    get_source_provider_display_name,
)
from sync_app.providers.target import build_target_provider
from sync_app.services.config_bundle import export_organization_bundle, import_organization_bundle
from sync_app.storage.local_db import (
    AttributeMappingRuleRepository,
    CustomManagedGroupBindingRepository,
    DatabaseManager,
    DepartmentOuMappingRepository,
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
from sync_app.web.authz import normalize_role
from sync_app.web.dashboard_state import (
    build_getting_started_data as build_getting_started_view_state,
    merge_saved_preflight_snapshot as merge_saved_preflight_snapshot_data,
)
from sync_app.web.helpers import parse_bulk_bindings
from sync_app.web.config_support import ConfigSupport
from sync_app.web.i18n import translate
from sync_app.web.pagination import (
    fetch_page,
    iter_all_pages,
    parse_page_number,
    stream_csv,
)
from sync_app.web.preflight_support import DashboardSupport
from sync_app.web.request_support import RequestSupport
from sync_app.web.routes_admin import register_admin_routes
from sync_app.web.routes_advanced_sync import register_advanced_sync_routes
from sync_app.web.routes_auth import register_auth_routes
from sync_app.web.routes_conflicts import register_conflict_routes
from sync_app.web.routes_config import register_config_routes
from sync_app.web.routes_dashboard import register_dashboard_routes
from sync_app.web.routes_exceptions import register_exception_routes
from sync_app.web.routes_jobs import register_job_routes
from sync_app.web.routes_mappings import register_mapping_routes
from sync_app.web.routes_metadata import register_metadata_routes
from sync_app.web.routes_organizations import register_organization_routes
from sync_app.web.routes_public import register_public_routes
from sync_app.web.sync_support import SyncSupport
from sync_app.web.runtime import (
    LoginRateLimiter,
    WebSyncRunner,
    normalize_secure_cookie_mode,
    resolve_web_runtime_settings,
    web_runtime_requires_restart,
)
from sync_app.web.security import (
    hash_password,
    rotate_csrf_token,
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


def _normalize_source_root_unit_ids_text(value: str | None) -> str:
    normalized_values: list[str] = []
    seen: set[str] = set()
    for candidate in _split_csv_values(value):
        if not candidate.isdigit():
            continue
        normalized = str(int(candidate))
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_values.append(normalized)
    return ", ".join(normalized_values)


def _normalize_ou_path_text(value: str | None, *, default: str = "") -> str:
    raw_text = str(value or "").strip()
    if not raw_text:
        return default
    dn_segments = [
        part.split("=", 1)[1].strip()
        for part in raw_text.split(",")
        if "=" in part and part.strip().lower().startswith("ou=") and part.split("=", 1)[1].strip()
    ]
    if dn_segments:
        segments = list(reversed(dn_segments))
    else:
        segments = [
            segment.strip()
            for segment in raw_text.replace("\\", "/").split("/")
            if segment.strip()
        ]
    normalized = "/".join(segments)
    return normalized or default


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
    department_ou_mapping_repo = DepartmentOuMappingRepository(db_manager)
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
    app.state.department_ou_mapping_repo = department_ou_mapping_repo
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
    request_support = RequestSupport(
        templates=TEMPLATES,
        app_version=APP_VERSION,
        default_brand_display_name=DEFAULT_BRAND_DISPLAY_NAME,
        default_brand_mark_text=DEFAULT_BRAND_MARK_TEXT,
        default_brand_attribution=DEFAULT_BRAND_ATTRIBUTION,
        supported_ui_modes=SUPPORTED_UI_MODES,
        placement_strategies=PLACEMENT_STRATEGIES,
        advanced_nav_pages=ADVANCED_NAV_PAGES,
        session_filter_prefix=SESSION_FILTER_PREFIX,
    )
    flash = request_support.flash
    flash_t = request_support.flash_t
    pop_flash = request_support.pop_flash
    get_ui_language = request_support.get_ui_language
    normalize_ui_mode = request_support.normalize_ui_mode
    get_ui_mode = request_support.get_ui_mode
    translate_text = request_support.translate_text
    localize_flash_message = request_support.localize_flash_message
    get_current_user = request_support.get_current_user
    get_current_org = request_support.get_current_org
    get_org_config_path = request_support.get_org_config_path
    list_org_connector_records = request_support.list_org_connector_records
    connector_has_database_overrides = request_support.connector_has_database_overrides
    describe_connector_config_source = request_support.describe_connector_config_source
    list_org_attribute_mapping_rules = request_support.list_org_attribute_mapping_rules
    get_org_setting_value = request_support.get_org_setting_value
    get_org_setting_bool = request_support.get_org_setting_bool
    get_org_setting_int = request_support.get_org_setting_int
    get_org_setting_float = request_support.get_org_setting_float
    get_page_filter_session_key = request_support.get_page_filter_session_key

    def resolve_remembered_filters(
        request: Request,
        *,
        page_name: str,
        defaults: dict[str, str],
    ) -> dict[str, str]:
        return request_support.resolve_remembered_filters(
            request,
            page_name=page_name,
            defaults=defaults,
            to_text=_to_text,
            to_bool=_to_bool,
        )

    normalize_soft_excluded_groups_text = request_support.normalize_soft_excluded_groups_text
    source_provider_label = request_support.source_provider_label
    config_support = ConfigSupport(
        app=app,
        logger=LOGGER,
        request_support=request_support,
        default_brand_display_name=DEFAULT_BRAND_DISPLAY_NAME,
        default_brand_mark_text=DEFAULT_BRAND_MARK_TEXT,
        default_brand_attribution=DEFAULT_BRAND_ATTRIBUTION,
        placement_strategies=PLACEMENT_STRATEGIES,
        build_source_provider_fn=lambda *args, **kwargs: build_source_provider(*args, **kwargs),
        build_target_provider_fn=lambda *args, **kwargs: build_target_provider(*args, **kwargs),
        normalize_source_root_unit_ids_text=_normalize_source_root_unit_ids_text,
        normalize_ou_path_text=_normalize_ou_path_text,
        clean_public_base_url=_clean_public_base_url,
        to_bool=_to_bool,
        split_csv_values=_split_csv_values,
        translate=translate,
    )

    def build_source_provider_field_models(
        editable: dict[str, Any],
        fields: tuple[Any, ...],
    ) -> list[dict[str, Any]]:
        return config_support.build_source_provider_field_models(editable, fields)

    def build_source_provider_fields(editable: dict[str, Any]) -> list[dict[str, Any]]:
        return config_support.build_source_provider_fields(editable)

    def build_source_provider_ui_catalog(ui_language: str) -> dict[str, Any]:
        return config_support.build_source_provider_ui_catalog(ui_language)

    def build_config_preview_groups(provider_schema) -> tuple[tuple[str, tuple[tuple[str, str, str], ...]], ...]:
        return config_support.build_config_preview_groups(provider_schema)

    def build_current_config_state(request: Request, current_org: OrganizationRecord) -> dict[str, Any]:
        return config_support.build_current_config_state(request, current_org)

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
        source_root_unit_ids: str = "",
        source_root_unit_display_text: str = "",
        directory_root_ou_path: str = "",
        disabled_users_ou_path: str = "Disabled Users",
        custom_group_ou_path: str = "Managed Groups",
        soft_excluded_groups: str = "",
    ) -> dict[str, Any]:
        return config_support.build_config_submission(
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

    def build_preview_app_config(request: Request, submission: dict[str, Any]) -> AppConfig:
        return config_support.build_preview_app_config(request, submission)

    def build_source_unit_catalog(
        request: Request,
        *,
        source_provider: str = "wecom",
        corpid: str = "",
        agentid: str = "",
        corpsecret: str = "",
    ) -> dict[str, Any]:
        return config_support.build_source_unit_catalog(
            request,
            source_provider=source_provider,
            corpid=corpid,
            agentid=agentid,
            corpsecret=corpsecret,
        )

    def build_target_ou_catalog(
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
        return config_support.build_target_ou_catalog(
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

    def format_config_change_value(
        field_name: str,
        field_type: str,
        value: Any,
        *,
        previous_value: Any = None,
    ) -> tuple[str, bool]:
        return config_support.format_config_change_value(
            field_name,
            field_type,
            value,
            previous_value=previous_value,
        )

    def build_config_change_preview(request: Request, submission: dict[str, Any]) -> dict[str, Any]:
        return config_support.build_config_change_preview(request, submission)

    def build_config_editable_override(request: Request, submission: dict[str, Any]) -> dict[str, Any]:
        return config_support.build_config_editable_override(request, submission)

    def build_config_page_context(
        request: Request,
        *,
        editable_override: Optional[dict[str, Any]] = None,
        config_change_preview: Optional[dict[str, Any]] = None,
        preview_token: str = "",
    ) -> dict[str, Any]:
        return config_support.build_config_page_context(
            request,
            editable_override=editable_override,
            config_change_preview=config_change_preview,
            preview_token=preview_token,
        )

    def apply_config_submission(request: Request, *, user: WebAdminUserRecord, submission: dict[str, Any]) -> None:
        config_support.apply_config_submission(request, user=user, submission=submission)

    render = request_support.render
    require_user = request_support.require_user
    require_capability = request_support.require_capability
    reject_invalid_csrf = request_support.reject_invalid_csrf
    get_client_ip = request_support.get_client_ip
    validate_admin_password = request_support.validate_admin_password

    dashboard_support = DashboardSupport(
        app=app,
        config_path=config_path,
        request_support=request_support,
        test_source_connection=lambda *args, **kwargs: test_source_connection(*args, **kwargs),
        test_ldap_connection=lambda *args, **kwargs: test_ldap_connection(*args, **kwargs),
    )
    load_config_summary = dashboard_support.load_config_summary
    build_preflight_snapshot = dashboard_support.build_preflight_snapshot
    build_dashboard_data = dashboard_support.build_dashboard_data

    sync_support = SyncSupport(
        app=app,
        logger=LOGGER,
        request_support=request_support,
        department_name_cache=department_name_cache,
        to_bool=_to_bool,
        validate_config_fn=validate_config,
        build_source_provider_fn=lambda *args, **kwargs: build_source_provider(*args, **kwargs),
        build_target_provider_fn=lambda *args, **kwargs: build_target_provider(*args, **kwargs),
        get_source_provider_display_name_fn=lambda provider_id: get_source_provider_display_name(provider_id),
        is_protected_ad_account_name_fn=is_protected_ad_account_name,
        recommend_conflict_resolution_fn=recommend_conflict_resolution,
        recommendation_requires_confirmation_fn=recommendation_requires_confirmation,
    )

    def validate_binding_target(request: Request, source_user_id: str, ad_username: str) -> Optional[str]:
        return sync_support.validate_binding_target(request, source_user_id, ad_username)

    def department_exists_in_source_provider(request: Request, department_id: str) -> tuple[bool, Optional[str]]:
        return sync_support.department_exists_in_source_provider(request, department_id)

    def load_department_name_map(request: Request) -> dict[str, str]:
        return sync_support.load_department_name_map(request)

    def search_source_users(request: Request, query: str, *, limit: int = 20) -> list[dict[str, Any]]:
        return sync_support.search_source_users(request, query, limit=limit)

    def list_source_user_departments(request: Request, source_user_id: str) -> list[dict[str, Any]]:
        return sync_support.list_source_user_departments(request, source_user_id)

    def search_target_users(request: Request, query: str, *, limit: int = 20) -> list[dict[str, Any]]:
        return sync_support.search_target_users(request, query, limit=limit)

    def source_user_exists_in_source_provider(request: Request, source_user_id: str) -> tuple[bool, Optional[str]]:
        return sync_support.source_user_exists_in_source_provider(request, source_user_id)

    def source_user_has_department(
        request: Request,
        source_user_id: str,
        department_id: str,
    ) -> tuple[bool, Optional[str]]:
        return sync_support.source_user_has_department(request, source_user_id, department_id)

    def parse_bulk_exception_rules(raw_text: str) -> tuple[list[dict[str, Any]], list[str]]:
        return sync_support.parse_bulk_exception_rules(raw_text)

    def normalize_optional_datetime_input(value: str) -> str:
        return sync_support.normalize_optional_datetime_input(value)

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
        return sync_support.enqueue_replay_request(
            app=app,
            request_type=request_type,
            requested_by=requested_by,
            org_id=org_id,
            target_scope=target_scope,
            target_id=target_id,
            trigger_reason=trigger_reason,
            payload=payload,
            execution_mode=execution_mode,
        )

    def build_conflicts_return_url(query: str, status: str, job_id: str) -> str:
        return sync_support.build_conflicts_return_url(query, status, job_id)

    def resolve_conflict_records_for_source(
        *,
        app: FastAPI,
        job_id: str,
        source_id: str,
        resolution_payload: dict[str, Any],
        actor_username: str,
    ) -> int:
        return sync_support.resolve_conflict_records_for_source(
            app=app,
            job_id=job_id,
            source_id=source_id,
            resolution_payload=resolution_payload,
            actor_username=actor_username,
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
        return sync_support.apply_conflict_manual_binding(
            app=app,
            conflict=conflict,
            ad_username=ad_username,
            actor_username=actor_username,
            org_id=org_id,
            notes=notes,
        )

    def apply_conflict_skip_user_sync(
        *,
        app: FastAPI,
        conflict: Any,
        actor_username: str,
        org_id: str,
        notes: str = "",
    ) -> tuple[bool, str, int]:
        return sync_support.apply_conflict_skip_user_sync(
            app=app,
            conflict=conflict,
            actor_username=actor_username,
            org_id=org_id,
            notes=notes,
        )

    def apply_conflict_recommendation(
        *,
        app: FastAPI,
        conflict: Any,
        actor_username: str,
        org_id: str,
        confirmation_reason: str = "",
    ) -> tuple[bool, str, int, Optional[dict[str, Any]]]:
        return sync_support.apply_conflict_recommendation(
            app=app,
            conflict=conflict,
            actor_username=actor_username,
            org_id=org_id,
            confirmation_reason=confirmation_reason,
        )

    register_public_routes(
        app,
        app_version=APP_VERSION,
        favicon_path=FAVICON_PATH,
        legacy_favicon_path=LEGACY_FAVICON_PATH,
        get_current_user=get_current_user,
    )

    register_auth_routes(
        app,
        flash=flash,
        flash_t=flash_t,
        get_client_ip=get_client_ip,
        get_current_user=get_current_user,
        hash_password=hash_password,
        normalize_role=normalize_role,
        reject_invalid_csrf=reject_invalid_csrf,
        render=render,
        rotate_csrf_token=rotate_csrf_token,
        validate_admin_password=validate_admin_password,
        verify_password=verify_password,
    )

    register_dashboard_routes(
        app,
        build_dashboard_data=build_dashboard_data,
        build_getting_started_view_state=build_getting_started_view_state,
        build_preflight_snapshot=build_preflight_snapshot,
        flash_t=flash_t,
        get_current_org=get_current_org,
        get_ui_mode=get_ui_mode,
        load_config_summary=load_config_summary,
        merge_saved_preflight_snapshot_data=merge_saved_preflight_snapshot_data,
        normalize_ui_mode=normalize_ui_mode,
        reject_invalid_csrf=reject_invalid_csrf,
        render=render,
        require_capability=require_capability,
        require_user=require_user,
        safe_redirect_target=_safe_redirect_target,
        source_provider_label=source_provider_label,
    )

    register_advanced_sync_routes(
        app,
        attribute_mapping_direction_labels=ATTRIBUTE_MAPPING_DIRECTION_LABELS,
        describe_connector_config_source=describe_connector_config_source,
        flash=flash,
        flash_t=flash_t,
        get_current_org=get_current_org,
        list_org_attribute_mapping_rules=list_org_attribute_mapping_rules,
        list_org_connector_records=list_org_connector_records,
        normalize_mapping_direction=normalize_mapping_direction,
        reject_invalid_csrf=reject_invalid_csrf,
        render=render,
        require_capability=require_capability,
        split_csv_values=_split_csv_values,
        to_bool=_to_bool,
    )

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

    register_metadata_routes(
        app,
        list_source_user_departments=list_source_user_departments,
        search_source_users=search_source_users,
        search_target_users=search_target_users,
        require_capability=require_capability,
        org_config_repo=app.state.org_config_repo,
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
        build_source_unit_catalog=build_source_unit_catalog,
        build_target_ou_catalog=build_target_ou_catalog,
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
        source_user_exists_in_source_provider=source_user_exists_in_source_provider,
        source_user_has_department=source_user_has_department,
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
