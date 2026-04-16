from __future__ import annotations

import logging
from functools import partial
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

from sync_app.core.common import APP_VERSION
from sync_app.core.config import test_ldap_connection, test_source_connection, validate_config
from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.core.conflict_recommendations import (
    recommend_conflict_resolution,
    recommendation_requires_confirmation,
)
from sync_app.core.sync_policies import normalize_mapping_direction
from sync_app.providers.source import (
    build_source_provider,
    get_source_provider_display_name,
)
from sync_app.providers.target import build_target_provider
from sync_app.services.config_bundle import export_organization_bundle, import_organization_bundle
from sync_app.web.authz import normalize_role
from sync_app.web.dashboard_state import (
    build_getting_started_data as build_getting_started_view_state,
    merge_saved_preflight_snapshot as merge_saved_preflight_snapshot_data,
)
from sync_app.web.helpers import parse_bulk_bindings
from sync_app.web.app_state import initialize_web_app_state
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
PUBLIC_AUTH_PATHS = {
    "/",
    "/favicon.ico",
    "/healthz",
    "/login",
    "/logout",
    "/readyz",
    "/setup",
}
PUBLIC_AUTH_PREFIXES = (
    "/static/",
)

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


def _is_public_auth_path(path: str) -> bool:
    normalized_path = str(path or "").strip() or "/"
    return normalized_path in PUBLIC_AUTH_PATHS or any(
        normalized_path.startswith(prefix)
        for prefix in PUBLIC_AUTH_PREFIXES
    )


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
    web_app_state = initialize_web_app_state(
        db_path=db_path,
        config_path=config_path,
        bind_host=bind_host,
        bind_port=bind_port,
        public_base_url=public_base_url,
        session_cookie_secure_mode=session_cookie_secure_mode,
        trust_proxy_headers=trust_proxy_headers,
        forwarded_allow_ips=forwarded_allow_ips,
    )
    repositories = web_app_state.repositories
    runtime_state = web_app_state.runtime

    app = FastAPI(title="AD Org Sync Web", version=APP_VERSION)
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    web_app_state.bind_to_app(app)
    app.router.on_startup.append(runtime_state.sync_runner.start)
    app.router.on_shutdown.append(runtime_state.sync_runner.stop)

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
    resolve_remembered_filters = partial(
        request_support.resolve_remembered_filters,
        to_text=_to_text,
        to_bool=_to_bool,
    )

    @app.middleware("http")
    async def require_login_middleware(request: Request, call_next):
        if request.method.upper() == "OPTIONS" or _is_public_auth_path(request.url.path):
            return await call_next(request)
        if not request.app.state.user_repo.has_any_user():
            return RedirectResponse(url="/setup", status_code=303)
        if not request_support.get_current_user(request):
            return RedirectResponse(url="/login", status_code=303)
        return await call_next(request)

    app.add_middleware(
        SessionMiddleware,
        secret_key=runtime_state.session_secret,
        same_site="strict",
        https_only=runtime_state.session_cookie_secure,
        max_age=runtime_state.session_minutes * 60,
    )

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

    dashboard_support = DashboardSupport(
        app=app,
        config_path=config_path,
        request_support=request_support,
        test_source_connection=lambda *args, **kwargs: test_source_connection(*args, **kwargs),
        test_ldap_connection=lambda *args, **kwargs: test_ldap_connection(*args, **kwargs),
    )

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
    sync_support_call = lambda method_name: (
        lambda *args, **kwargs: getattr(sync_support, method_name)(*args, **kwargs)
    )

    register_public_routes(
        app,
        app_version=APP_VERSION,
        favicon_path=FAVICON_PATH,
        legacy_favicon_path=LEGACY_FAVICON_PATH,
        get_current_user=request_support.get_current_user,
    )

    register_auth_routes(
        app,
        flash=request_support.flash,
        flash_t=request_support.flash_t,
        get_client_ip=request_support.get_client_ip,
        get_current_user=request_support.get_current_user,
        hash_password=hash_password,
        normalize_role=normalize_role,
        reject_invalid_csrf=request_support.reject_invalid_csrf,
        render=request_support.render,
        rotate_csrf_token=rotate_csrf_token,
        validate_admin_password=request_support.validate_admin_password,
        verify_password=verify_password,
    )

    register_dashboard_routes(
        app,
        build_dashboard_data=dashboard_support.build_dashboard_data,
        build_getting_started_view_state=build_getting_started_view_state,
        build_preflight_snapshot=dashboard_support.build_preflight_snapshot,
        flash_t=request_support.flash_t,
        get_current_org=request_support.get_current_org,
        get_ui_mode=request_support.get_ui_mode,
        load_config_summary=dashboard_support.load_config_summary,
        merge_saved_preflight_snapshot_data=merge_saved_preflight_snapshot_data,
        normalize_ui_mode=request_support.normalize_ui_mode,
        reject_invalid_csrf=request_support.reject_invalid_csrf,
        render=request_support.render,
        require_capability=request_support.require_capability,
        require_user=request_support.require_user,
        safe_redirect_target=_safe_redirect_target,
        source_provider_label=request_support.source_provider_label,
    )

    register_advanced_sync_routes(
        app,
        build_source_data_quality_snapshot=sync_support_call("build_source_data_quality_snapshot"),
        attribute_mapping_direction_labels=ATTRIBUTE_MAPPING_DIRECTION_LABELS,
        build_username_preview=sync_support_call("build_username_preview"),
        describe_connector_config_source=request_support.describe_connector_config_source,
        explain_identity_routing=sync_support_call("explain_identity_routing"),
        flash=request_support.flash,
        flash_t=request_support.flash_t,
        get_current_org=request_support.get_current_org,
        list_org_attribute_mapping_rules=request_support.list_org_attribute_mapping_rules,
        list_org_connector_records=request_support.list_org_connector_records,
        normalize_mapping_direction=normalize_mapping_direction,
        reject_invalid_csrf=request_support.reject_invalid_csrf,
        render=request_support.render,
        require_capability=request_support.require_capability,
        split_csv_values=_split_csv_values,
        to_bool=_to_bool,
    )

    register_job_routes(
        app,
        build_preflight_snapshot=dashboard_support.build_preflight_snapshot,
        enqueue_replay_request=sync_support_call("enqueue_replay_request"),
        fetch_page=fetch_page,
        flash=request_support.flash,
        flash_t=request_support.flash_t,
        get_current_org=request_support.get_current_org,
        get_ui_language=request_support.get_ui_language,
        merge_saved_preflight_snapshot_data=merge_saved_preflight_snapshot_data,
        parse_page_number=parse_page_number,
        reject_invalid_csrf=request_support.reject_invalid_csrf,
        render=request_support.render,
        require_capability=request_support.require_capability,
        translate_text=request_support.translate_text,
    )

    register_metadata_routes(
        app,
        list_source_user_departments=sync_support_call("list_source_user_departments"),
        search_source_users=sync_support_call("search_source_users"),
        search_target_users=sync_support_call("search_target_users"),
        require_capability=request_support.require_capability,
        org_config_repo=repositories.org_config_repo,
    )

    register_organization_routes(
        app,
        export_organization_bundle=export_organization_bundle,
        flash=request_support.flash,
        flash_t=request_support.flash_t,
        import_organization_bundle=import_organization_bundle,
        reject_invalid_csrf=request_support.reject_invalid_csrf,
        render=request_support.render,
        require_capability=request_support.require_capability,
        require_user=request_support.require_user,
        safe_redirect_target=_safe_redirect_target,
        to_bool=_to_bool,
    )

    register_config_routes(
        app,
        apply_config_submission=config_support.apply_config_submission,
        build_config_change_preview=config_support.build_config_change_preview,
        build_config_editable_override=config_support.build_config_editable_override,
        build_config_page_context=config_support.build_config_page_context,
        build_source_unit_catalog=config_support.build_source_unit_catalog,
        build_target_ou_catalog=config_support.build_target_ou_catalog,
        build_config_submission=config_support.build_config_submission,
        config_preview_session_key=CONFIG_PREVIEW_SESSION_KEY,
        flash=request_support.flash,
        reject_invalid_csrf=request_support.reject_invalid_csrf,
        render=request_support.render,
        require_capability=request_support.require_capability,
        resolve_web_runtime_settings=resolve_web_runtime_settings,
        web_runtime_requires_restart=web_runtime_requires_restart,
    )

    register_mapping_routes(
        app,
        department_exists_in_source_provider=sync_support_call("department_exists_in_source_provider"),
        fetch_page=fetch_page,
        flash=request_support.flash,
        flash_t=request_support.flash_t,
        get_current_org=request_support.get_current_org,
        iter_all_pages=iter_all_pages,
        load_department_name_map=sync_support_call("load_department_name_map"),
        parse_bulk_bindings=parse_bulk_bindings,
        parse_page_number=parse_page_number,
        reject_invalid_csrf=request_support.reject_invalid_csrf,
        render=request_support.render,
        require_capability=request_support.require_capability,
        resolve_remembered_filters=resolve_remembered_filters,
        source_user_exists_in_source_provider=sync_support_call("source_user_exists_in_source_provider"),
        source_user_has_department=sync_support_call("source_user_has_department"),
        stream_csv=stream_csv,
        to_bool=_to_bool,
        validate_binding_target=sync_support_call("validate_binding_target"),
    )

    register_exception_routes(
        app,
        department_exists_in_source_provider=sync_support_call("department_exists_in_source_provider"),
        enqueue_replay_request=sync_support_call("enqueue_replay_request"),
        fetch_page=fetch_page,
        flash=request_support.flash,
        flash_t=request_support.flash_t,
        get_current_org=request_support.get_current_org,
        iter_all_pages=iter_all_pages,
        load_department_name_map=sync_support_call("load_department_name_map"),
        normalize_optional_datetime_input=sync_support_call("normalize_optional_datetime_input"),
        parse_bulk_exception_rules=sync_support_call("parse_bulk_exception_rules"),
        parse_page_number=parse_page_number,
        reject_invalid_csrf=request_support.reject_invalid_csrf,
        render=request_support.render,
        require_capability=request_support.require_capability,
        resolve_remembered_filters=resolve_remembered_filters,
        stream_csv=stream_csv,
        to_bool=_to_bool,
    )

    register_conflict_routes(
        app,
        apply_conflict_manual_binding=sync_support_call("apply_conflict_manual_binding"),
        apply_conflict_recommendation=sync_support_call("apply_conflict_recommendation"),
        apply_conflict_skip_user_sync=sync_support_call("apply_conflict_skip_user_sync"),
        build_conflicts_return_url=sync_support_call("build_conflicts_return_url"),
        fetch_page=fetch_page,
        flash=request_support.flash,
        flash_t=request_support.flash_t,
        get_current_org=request_support.get_current_org,
        parse_page_number=parse_page_number,
        reject_invalid_csrf=request_support.reject_invalid_csrf,
        render=request_support.render,
        require_capability=request_support.require_capability,
        resolve_remembered_filters=resolve_remembered_filters,
        to_text=_to_text,
    )

    register_admin_routes(
        app,
        fetch_page=fetch_page,
        flash=request_support.flash,
        flash_t=request_support.flash_t,
        get_current_org=request_support.get_current_org,
        hash_password=hash_password,
        parse_page_number=parse_page_number,
        reject_invalid_csrf=request_support.reject_invalid_csrf,
        render=request_support.render,
        require_capability=request_support.require_capability,
        resolve_remembered_filters=resolve_remembered_filters,
        validate_admin_password=request_support.validate_admin_password,
        verify_password=verify_password,
    )

    return app
