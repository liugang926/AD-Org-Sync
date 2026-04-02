from __future__ import annotations

import csv
import io
import json
import logging
import secrets
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from urllib.parse import urlencode

from fastapi import FastAPI, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from sync_app.clients.wecom import WeComAPI
from sync_app.core.common import APP_VERSION
from sync_app.core.config import (
    load_sync_config,
    run_config_security_self_check,
    test_ldap_connection,
    test_wecom_connection,
    validate_config,
)
from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.core.conflict_recommendations import (
    recommend_conflict_resolution,
    recommendation_requires_confirmation,
)
from sync_app.core.exception_rules import (
    EXCEPTION_MATCH_TYPE_LABELS,
    EXCEPTION_RULE_DEFINITIONS,
    get_exception_rule_definition,
    normalize_exception_rule_type,
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
    normalize_source_provider,
)
from sync_app.services.runtime import run_sync_job
from sync_app.services.config_bundle import export_organization_bundle, import_organization_bundle
from sync_app.storage.local_db import (
    AttributeMappingRuleRepository,
    CustomManagedGroupBindingRepository,
    DatabaseManager,
    GroupExclusionRuleRepository,
    ManagedGroupBindingRepository,
    ObjectStateRepository,
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
    _normalize_org_config_values,
)
from sync_app.web.authz import WEB_ADMIN_ROLES, has_capability, normalize_role, role_capabilities
from sync_app.web.helpers import parse_bulk_bindings
from sync_app.web.i18n import (
    DEFAULT_UI_LANGUAGE,
    SUPPORTED_UI_LANGUAGES,
    detect_browser_ui_language,
    normalize_ui_language,
    translate,
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
FAVICON_PATH = APP_ROOT / "icon.ico"
PLACEMENT_STRATEGIES = {
    "wecom_primary_department": "Prefer WeCom primary department",
    "lowest_department_id": "Pick the lowest department ID",
    "shortest_path": "Pick the shortest department path",
    "first_non_excluded_department": "Pick the first valid department in source order",
}
LOCAL_WEB_BIND_HOSTS = {"127.0.0.1", "localhost", "::1"}
SECURE_COOKIE_MODES = {"auto", "always", "never"}
WEB_RUNTIME_RESTART_KEYS = (
    "bind_host",
    "bind_port",
    "public_base_url",
    "session_cookie_secure_mode",
    "session_cookie_secure",
    "trust_proxy_headers",
    "forwarded_allow_ips",
)
SUPPORTED_UI_MODES = {
    "basic": "Basic",
    "advanced": "Advanced",
}
SOURCE_PROVIDER_LABELS = {
    "wecom": "WeCom",
    "dingtalk": "DingTalk",
    "feishu": "Feishu",
}
CONFIG_SOURCE_PROVIDER_OPTIONS = (
    ("wecom", "WeCom"),
)
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
CONFIG_PREVIEW_GROUPS = (
    (
        "Connection Settings",
        (
            ("source_provider", "Source Provider", "source_provider"),
            ("corpid", "CorpID", "text"),
            ("agentid", "AgentID", "text"),
            ("corpsecret", "CorpSecret", "secret"),
            ("webhook_url", "WeCom Webhook", "secret"),
            ("ldap_server", "LDAP Server", "text"),
            ("ldap_domain", "LDAP Domain", "text"),
            ("ldap_username", "LDAP Username", "text"),
            ("ldap_password", "LDAP Password", "secret"),
            ("ldap_port", "LDAP Port", "number"),
            ("ldap_use_ssl", "Use SSL", "bool"),
        ),
    ),
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
        "Group Rules",
        (
            ("soft_excluded_groups", "Soft Excluded Groups", "multiline"),
        ),
    ),
)


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


def normalize_secure_cookie_mode(value: Optional[str]) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in SECURE_COOKIE_MODES else "auto"


def resolve_web_runtime_settings(
    settings_repo: SettingsRepository,
    *,
    bind_host: str | None = None,
    bind_port: int | None = None,
    public_base_url: str | None = None,
    session_cookie_secure_mode: str | None = None,
    trust_proxy_headers: bool | None = None,
    forwarded_allow_ips: str | None = None,
) -> dict[str, Any]:
    resolved_bind_host = _to_text(
        bind_host,
        settings_repo.get_value("web_bind_host", "127.0.0.1") or "127.0.0.1",
    ) or "127.0.0.1"
    resolved_bind_port = max(
        int(bind_port or settings_repo.get_int("web_bind_port", 8000) or 8000),
        1,
    )
    resolved_public_base_url = _clean_public_base_url(
        public_base_url if public_base_url is not None else settings_repo.get_value("web_public_base_url", "")
    )
    resolved_secure_mode = normalize_secure_cookie_mode(
        session_cookie_secure_mode
        if session_cookie_secure_mode is not None
        else settings_repo.get_value("web_session_cookie_secure_mode", "auto")
    )
    resolved_trust_proxy_headers = (
        settings_repo.get_bool("web_trust_proxy_headers", False)
        if trust_proxy_headers is None
        else bool(trust_proxy_headers)
    )
    resolved_forwarded_allow_ips = _to_text(
        forwarded_allow_ips,
        settings_repo.get_value("web_forwarded_allow_ips", "127.0.0.1") or "127.0.0.1",
    ) or "127.0.0.1"
    bind_is_local = resolved_bind_host.lower() in LOCAL_WEB_BIND_HOSTS
    public_url_is_https = resolved_public_base_url.lower().startswith("https://")
    session_cookie_secure = resolved_secure_mode == "always" or (
        resolved_secure_mode == "auto" and (public_url_is_https or not bind_is_local)
    )

    warnings: list[str] = []
    if resolved_secure_mode == "never":
        warnings.append("Secure session cookies are disabled.")
    if resolved_public_base_url and not public_url_is_https:
        warnings.append("Public base URL does not use HTTPS.")
    if resolved_trust_proxy_headers and resolved_forwarded_allow_ips in {"*", "0.0.0.0", "0.0.0.0/0", "::/0"}:
        warnings.append("Forwarded proxy headers are trusted from every IP address.")

    return {
        "bind_host": resolved_bind_host,
        "bind_port": resolved_bind_port,
        "public_base_url": resolved_public_base_url,
        "session_cookie_secure_mode": resolved_secure_mode,
        "session_cookie_secure": session_cookie_secure,
        "trust_proxy_headers": resolved_trust_proxy_headers,
        "forwarded_allow_ips": resolved_forwarded_allow_ips,
        "warnings": warnings,
    }


def web_runtime_requires_restart(
    current_runtime_settings: dict[str, Any],
    persisted_runtime_settings: dict[str, Any],
) -> bool:
    return any(
        current_runtime_settings.get(key) != persisted_runtime_settings.get(key)
        for key in WEB_RUNTIME_RESTART_KEYS
    )

class WebSyncRunner:
    def __init__(self, *, db_path: str, audit_repo: WebAuditLogRepository):
        self.db_path = db_path
        self.audit_repo = audit_repo
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self.last_error = ""

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def launch(
        self,
        *,
        mode: str,
        actor_username: str,
        org_id: str,
        config_path: str,
    ) -> tuple[bool, str]:
        with self._lock:
            if self.is_running():
                return False, "A synchronization job is already running in the background"
            self.last_error = ""
            self._thread = threading.Thread(
                target=self._run_job,
                kwargs={
                    "mode": mode,
                    "actor_username": actor_username,
                    "org_id": org_id,
                    "config_path": config_path,
                },
                daemon=True,
                name=f"web-sync-{org_id}-{mode}",
            )
            self._thread.start()
        return True, "Synchronization job started"

    def _run_job(self, *, mode: str, actor_username: str, org_id: str, config_path: str) -> None:
        try:
            result = run_sync_job(
                execution_mode=mode,
                trigger_type="web",
                db_path=self.db_path,
                config_path=config_path,
                org_id=org_id,
            )
            self.audit_repo.add_log(
                org_id=org_id,
                actor_username=actor_username,
                action_type="job.run",
                target_type="sync_job",
                target_id=result.get("job_id"),
                result="success",
                message=f"Started {mode} synchronization job",
                payload={
                    "job_id": result.get("job_id"),
                    "mode": mode,
                    "org_id": org_id,
                    "error_count": result.get("error_count"),
                },
            )
        except Exception as exc:
            self.last_error = str(exc)
            LOGGER.exception("web sync job failed")
            self.audit_repo.add_log(
                org_id=org_id,
                actor_username=actor_username,
                action_type="job.run",
                target_type="sync_job",
                target_id="",
                result="error",
                message=f"Failed to start synchronization job: {exc}",
                payload={"mode": mode, "org_id": org_id},
            )


class LoginRateLimiter:
    def __init__(self, *, max_attempts: int, window_seconds: int, lockout_seconds: int):
        self.max_attempts = max(int(max_attempts or 1), 1)
        self.window_seconds = max(int(window_seconds or 1), 1)
        self.lockout_seconds = max(int(lockout_seconds or 1), 1)
        self._lock = threading.Lock()
        self._failed_attempts: dict[str, list[float]] = {}
        self._locked_until: dict[str, float] = {}

    def _build_key(self, username: str, client_ip: str) -> str:
        normalized_username = str(username or "").strip().lower() or "-"
        normalized_ip = str(client_ip or "").strip().lower() or "unknown"
        return f"{normalized_ip}::{normalized_username}"

    def _prune(self, key: str, *, now: float) -> None:
        cutoff = now - self.window_seconds
        failures = [timestamp for timestamp in self._failed_attempts.get(key, []) if timestamp >= cutoff]
        if failures:
            self._failed_attempts[key] = failures
        else:
            self._failed_attempts.pop(key, None)

        locked_until = self._locked_until.get(key, 0.0)
        if locked_until and locked_until <= now:
            self._locked_until.pop(key, None)

    def check(self, username: str, client_ip: str) -> tuple[bool, int]:
        key = self._build_key(username, client_ip)
        now = time.time()
        with self._lock:
            self._prune(key, now=now)
            locked_until = self._locked_until.get(key, 0.0)
            if not locked_until or locked_until <= now:
                return False, 0
            return True, max(int(locked_until - now), 1)

    def record_failure(self, username: str, client_ip: str) -> tuple[bool, int]:
        key = self._build_key(username, client_ip)
        now = time.time()
        with self._lock:
            self._prune(key, now=now)
            failures = self._failed_attempts.setdefault(key, [])
            failures.append(now)
            if len(failures) < self.max_attempts:
                return False, 0
            self._locked_until[key] = now + self.lockout_seconds
            self._failed_attempts.pop(key, None)
            return True, self.lockout_seconds

    def clear(self, username: str, client_ip: str) -> None:
        key = self._build_key(username, client_ip)
        with self._lock:
            self._failed_attempts.pop(key, None)
            self._locked_until.pop(key, None)


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

    app = FastAPI(title="WeCom AD Sync Web", version=APP_VERSION)
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
        return SOURCE_PROVIDER_LABELS.get(normalized_value, normalized_value or "WeCom")

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
            "user_ou_placement_strategy": request.app.state.settings_repo.get_value(
                "user_ou_placement_strategy",
                "wecom_primary_department",
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
        user_ou_placement_strategy: str = "wecom_primary_department",
        soft_excluded_groups: str = "",
    ) -> dict[str, Any]:
        current_org = get_current_org(request)
        current_org_config_path = current_org.config_path or request.app.state.config_path
        current_org_values = request.app.state.org_config_repo.get_raw_config(
            current_org.org_id,
            config_path=current_org_config_path,
        )
        if user_ou_placement_strategy not in PLACEMENT_STRATEGIES:
            user_ou_placement_strategy = "wecom_primary_department"
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
        for group_title, fields in CONFIG_PREVIEW_GROUPS:
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
        source_provider_options = list(CONFIG_SOURCE_PROVIDER_OPTIONS)
        current_source_provider = normalize_source_provider(editable.get("source_provider"))
        if current_source_provider not in {option_value for option_value, _ in source_provider_options}:
            source_provider_options.append(
                (
                    current_source_provider,
                    f"{source_provider_label(current_source_provider)} (unsupported in this build)",
                )
            )
        protected_rules = request.app.state.exclusion_repo.list_rules(
            rule_type="protect",
            protection_level="hard",
            org_id=current_org.org_id,
        )
        return {
            "page": "config",
            "title": "Configuration",
            "editable": editable,
            "current_org": current_org,
            "source_provider_options": source_provider_options,
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
        min_length = request.app.state.settings_repo.get_int("web_admin_password_min_length", 12)
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

    def summarize_check_status(checks: list[dict[str, Any]]) -> str:
        if any(str(item.get("status") or "") == "error" for item in checks):
            return "error"
        if any(str(item.get("status") or "") == "warning" for item in checks):
            return "warning"
        return "success"

    def count_check_statuses(checks: list[dict[str, Any]]) -> dict[str, int]:
        counts = {"success": 0, "warning": 0, "error": 0}
        for item in checks:
            status = str(item.get("status") or "success")
            if status in counts:
                counts[status] += 1
        return counts

    def merge_saved_preflight_snapshot(
        request: Request,
        base_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        saved_snapshot = request.session.get("_preflight_snapshot")
        if not isinstance(saved_snapshot, dict):
            return base_snapshot
        if str(saved_snapshot.get("org_id") or "") != str(base_snapshot.get("org_id") or ""):
            return base_snapshot
        saved_checks = [
            item
            for item in list(saved_snapshot.get("checks") or [])
            if isinstance(item, dict) and str(item.get("key") or "").startswith("live_")
        ]
        if not saved_checks:
            return base_snapshot
        merged = dict(base_snapshot)
        merged_checks = list(base_snapshot.get("checks") or []) + saved_checks
        merged["checks"] = merged_checks
        merged["overall_status"] = summarize_check_status(merged_checks)
        merged["status_counts"] = count_check_statuses(merged_checks)
        merged["live_ran_at"] = str(saved_snapshot.get("generated_at") or "")
        merged["has_live_checks"] = True
        return merged

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
                    "detail": "Required source and LDAP settings are complete.",
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
                and normalize_source_provider(config.source_provider) == "wecom"
                and config.source_connector.corpid
                and config.source_connector.corpsecret
            ):
                wecom_ok, wecom_message = test_wecom_connection(
                    config.source_connector.corpid,
                    config.source_connector.corpsecret,
                    config.source_connector.agentid,
                )
                checks.append(
                    {
                        "key": "live_wecom",
                        "label": f"Live {source_provider_name} connection",
                        "status": "success" if wecom_ok else "error",
                        "detail": wecom_message,
                        "action_url": "/config",
                    }
                )
            else:
                if config and normalize_source_provider(config.source_provider) != "wecom":
                    live_source_detail = f"Skipped because {source_provider_name} is not implemented in this build."
                else:
                    live_source_detail = f"Skipped because {source_provider_name} credentials are incomplete or still invalid."
                checks.append(
                    {
                        "key": "live_wecom",
                        "label": f"Live {source_provider_name} connection",
                        "status": "warning",
                        "detail": live_source_detail,
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
            str(item.get("key") or "") in {"live_wecom", "live_ldap"} and str(item.get("status") or "") == "error"
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

    def build_getting_started_data(
        request: Request,
        *,
        preflight_snapshot: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        current_org = get_current_org(request)
        preflight = preflight_snapshot or merge_saved_preflight_snapshot(
            request,
            build_preflight_snapshot(request, include_live=False),
        )
        check_index = {
            str(item.get("key") or ""): item for item in list(preflight.get("checks") or []) if isinstance(item, dict)
        }
        config_ready = str(check_index.get("config", {}).get("status") or "") == "success"
        live_wecom_ok = str(check_index.get("live_wecom", {}).get("status") or "") == "success"
        live_ldap_ok = str(check_index.get("live_ldap", {}).get("status") or "") == "success"
        live_ready = live_wecom_ok and live_ldap_ok
        dry_run_ready = bool(preflight.get("dry_run_completed"))
        conflicts_ready = dry_run_ready and int(preflight.get("open_conflict_count") or 0) == 0
        apply_ready = bool(preflight.get("apply_completed"))
        current_ui_mode = get_ui_mode(request)

        steps = [
            {
                "title": "Configure organization settings",
                "detail": "Complete the source connector and LDAP values for the current organization.",
                "href": "/config",
                "action_label": "Open Config",
                "capability": "config.read",
                "done": config_ready,
            },
            {
                "title": "Run live connectivity preflight",
                "detail": (
                    "Verify both the source connector and LDAP from this server before the first synchronization run."
                    if not live_ready
                    else "Live source connector and LDAP connectivity checks both passed."
                ),
                "href": "/dashboard#preflight",
                "action_label": "Run Preflight",
                "capability": "dashboard.read",
                "done": live_ready,
            },
            {
                "title": "Review sync scope",
                "detail": (
                    "Basic mode keeps the default single-organization flow. Switch to Advanced mode only if you need routing, write-back, or lifecycle controls."
                    if current_ui_mode == "basic"
                    else "Review connectors, mappings, and lifecycle policies before the first rollout."
                ),
                "href": "/config" if current_ui_mode == "basic" else "/advanced-sync",
                "action_label": "Review Scope",
                "capability": "config.read",
                "done": config_ready,
            },
            {
                "title": "Run the first dry run",
                "detail": (
                    "A successful dry run is already recorded."
                    if dry_run_ready
                    else "Preview planned changes before applying them to AD."
                ),
                "href": "/jobs",
                "action_label": "Open Jobs",
                "capability": "jobs.read",
                "done": dry_run_ready,
            },
            {
                "title": "Clear blockers and run apply",
                "detail": (
                    "Apply is already successful for this organization."
                    if apply_ready
                    else (
                        "Resolve open conflicts before the first apply run."
                        if dry_run_ready and not conflicts_ready
                        else "Run the first apply after the dry run looks safe."
                    )
                ),
                "href": "/conflicts" if dry_run_ready and not conflicts_ready else "/jobs",
                "action_label": "Resolve Conflicts" if dry_run_ready and not conflicts_ready else "Run Apply",
                "capability": "conflicts.read" if dry_run_ready and not conflicts_ready else "jobs.read",
                "done": apply_ready,
            },
        ]

        current_assigned = False
        completed_steps = 0
        for step in steps:
            if step["done"]:
                step["status"] = "complete"
                completed_steps += 1
            elif not current_assigned:
                step["status"] = "current"
                current_assigned = True
            else:
                step["status"] = "upcoming"

        next_step = next((step for step in steps if step["status"] == "current"), steps[-1])
        return {
            "current_org_name": current_org.name,
            "steps": steps,
            "completed_steps": completed_steps,
            "total_steps": len(steps),
            "next_step": next_step,
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
        preflight_snapshot = merge_saved_preflight_snapshot(
            request,
            build_preflight_snapshot(request, include_live=False),
        )
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
            "user_count": request.app.state.user_repo.count_users(),
            "binding_count": len(bindings),
            "override_count": len(overrides),
            "preflight_summary": preflight_snapshot,
            "getting_started": build_getting_started_data(request, preflight_snapshot=preflight_snapshot),
            "placement_strategy": request.app.state.settings_repo.get_value(
                "user_ou_placement_strategy",
                "wecom_primary_department",
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
                api_factory=WeComAPI,
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
                api_factory=WeComAPI,
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
        app.state.user_binding_repo.upsert_binding(
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
            target_scope="wecom_user",
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
            target_scope="wecom_user",
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

    @app.get("/favicon.ico")
    def favicon(request: Request):
        if FAVICON_PATH.exists():
            return FileResponse(str(FAVICON_PATH), media_type="image/x-icon")
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
        return render(
            request,
            "dashboard.html",
            page="dashboard",
            title="Dashboard",
            **build_dashboard_data(request),
        )

    @app.get("/getting-started", response_class=HTMLResponse)
    def getting_started_page(request: Request):
        user = require_capability(request, "dashboard.read")
        if isinstance(user, RedirectResponse):
            return user
        preflight_snapshot = merge_saved_preflight_snapshot(
            request,
            build_preflight_snapshot(request, include_live=False),
        )
        return render(
            request,
            "getting_started.html",
            page="getting-started",
            title="Getting Started",
            preflight_summary=preflight_snapshot,
            getting_started=build_getting_started_data(request, preflight_snapshot=preflight_snapshot),
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

    @app.get("/organizations", response_class=HTMLResponse)
    def organizations_page(request: Request):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        return render(
            request,
            "organizations.html",
            page="organizations",
            title="Organizations",
        )

    @app.post("/organization-switch")
    def organization_switch(
        request: Request,
        csrf_token: str = Form(""),
        org_id: str = Form("default"),
        return_url: str = Form("/dashboard"),
    ):
        user = require_user(request)
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, _safe_redirect_target(return_url, "/dashboard"))
        if csrf_error:
            return csrf_error
        organization = request.app.state.organization_repo.get_organization_record(org_id)
        if not organization or not organization.is_enabled:
            flash(request, "error", "Organization not found or disabled")
            return RedirectResponse(url="/dashboard", status_code=303)
        request.session["selected_org_id"] = organization.org_id
        flash_t(request, "success", "Switched to organization {name}", name=organization.name)
        return RedirectResponse(url=_safe_redirect_target(return_url, "/dashboard"), status_code=303)

    @app.post("/organizations")
    def organization_submit(
        request: Request,
        csrf_token: str = Form(""),
        org_id: str = Form(""),
        name: str = Form(""),
        config_path_value: str = Form("", alias="config_path"),
        description: str = Form(""),
        is_enabled: Optional[str] = Form(None),
    ):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/organizations")
        if csrf_error:
            return csrf_error
        normalized_org_id = str(org_id or "").strip()
        if not normalized_org_id:
            flash(request, "error", "Organization ID is required")
            return RedirectResponse(url="/organizations", status_code=303)
        try:
            request.app.state.organization_repo.upsert_organization(
                org_id=normalized_org_id,
                name=name,
                config_path=config_path_value,
                description=description,
                is_enabled=_to_bool(is_enabled, True),
            )
        except Exception as exc:
            flash_t(request, "error", "Failed to save organization: {error}", error=str(exc))
            return RedirectResponse(url="/organizations", status_code=303)
        request.app.state.org_config_repo.ensure_loaded(normalized_org_id, config_path=config_path_value)
        organization = request.app.state.organization_repo.get_organization_record(normalized_org_id)
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="organization.upsert",
            target_type="organization",
            target_id=normalized_org_id.lower(),
            result="success",
            message="Saved organization definition",
            payload=organization.to_dict() if organization else {"org_id": org_id},
        )
        flash_t(request, "success", "Organization {org_id} saved", org_id=normalized_org_id.lower())
        return RedirectResponse(url="/organizations", status_code=303)

    @app.post("/organizations/{org_id}/select")
    def organization_select(
        request: Request,
        org_id: str,
        csrf_token: str = Form(""),
        return_url: str = Form("/dashboard"),
    ):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/organizations")
        if csrf_error:
            return csrf_error
        organization = request.app.state.organization_repo.get_organization_record(org_id)
        if not organization or not organization.is_enabled:
            flash(request, "error", "Organization not found or disabled")
            return RedirectResponse(url="/organizations", status_code=303)
        request.session["selected_org_id"] = organization.org_id
        flash_t(request, "success", "Switched to organization {name}", name=organization.name)
        return RedirectResponse(url=_safe_redirect_target(return_url, "/dashboard"), status_code=303)

    @app.get("/organizations/{org_id}/export")
    def organization_export(request: Request, org_id: str):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        organization = request.app.state.organization_repo.get_organization_record(org_id)
        if not organization:
            flash(request, "error", "Organization not found")
            return RedirectResponse(url="/organizations", status_code=303)
        try:
            bundle = export_organization_bundle(request.app.state.db_manager, organization.org_id)
        except Exception as exc:
            flash_t(request, "error", "Failed to export organization bundle: {error}", error=str(exc))
            return RedirectResponse(url="/organizations", status_code=303)
        request.app.state.audit_repo.add_log(
            org_id=organization.org_id,
            actor_username=user.username,
            action_type="organization.bundle_export",
            target_type="organization",
            target_id=organization.org_id,
            result="success",
            message="Exported configuration bundle",
            payload={"organization_name": organization.name},
        )
        filename = f"{organization.org_id}-config-bundle.json"
        return Response(
            content=json.dumps(bundle, ensure_ascii=False, indent=2).encode("utf-8"),
            media_type="application/json; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.post("/organizations/import")
    def organization_import(
        request: Request,
        csrf_token: str = Form(""),
        bundle_json: str = Form(""),
        target_org_id: str = Form(""),
        replace_existing: Optional[str] = Form(None),
    ):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/organizations")
        if csrf_error:
            return csrf_error
        bundle_payload = str(bundle_json or "").strip()
        if not bundle_payload:
            flash(request, "error", "Configuration bundle content is required")
            return RedirectResponse(url="/organizations", status_code=303)
        try:
            bundle = json.loads(bundle_payload)
        except json.JSONDecodeError as exc:
            flash_t(request, "error", "Invalid configuration bundle JSON: {error}", error=str(exc))
            return RedirectResponse(url="/organizations", status_code=303)
        try:
            summary = import_organization_bundle(
                request.app.state.db_manager,
                bundle,
                target_org_id=str(target_org_id or "").strip() or None,
                replace_existing=_to_bool(replace_existing, False),
            )
        except Exception as exc:
            flash_t(request, "error", "Failed to import organization bundle: {error}", error=str(exc))
            return RedirectResponse(url="/organizations", status_code=303)
        request.app.state.audit_repo.add_log(
            org_id=summary["org_id"],
            actor_username=user.username,
            action_type="organization.bundle_import",
            target_type="organization",
            target_id=summary["org_id"],
            result="success",
            message="Imported configuration bundle",
            payload=summary,
        )
        flash_t(
            request,
            "success",
            "Imported configuration bundle into {org_id} ({connectors} connectors, {mappings} mappings, {rules} group rules)",
            org_id=summary["org_id"],
            connectors=summary["imported_connectors"],
            mappings=summary["imported_mappings"],
            rules=summary["imported_group_rules"],
        )
        return RedirectResponse(url="/organizations", status_code=303)

    @app.post("/organizations/{org_id}/toggle")
    def organization_toggle(
        request: Request,
        org_id: str,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/organizations")
        if csrf_error:
            return csrf_error
        organization = request.app.state.organization_repo.get_organization_record(org_id)
        if not organization:
            flash(request, "error", "Organization not found")
            return RedirectResponse(url="/organizations", status_code=303)
        try:
            request.app.state.organization_repo.set_enabled(org_id, not organization.is_enabled)
        except Exception as exc:
            flash_t(request, "error", "Failed to update organization: {error}", error=str(exc))
            return RedirectResponse(url="/organizations", status_code=303)
        if request.session.get("selected_org_id") == organization.org_id and organization.is_enabled:
            request.session["selected_org_id"] = "default"
        flash_t(
            request,
            "success",
            "Organization {name} enabled" if not organization.is_enabled else "Organization {name} disabled",
            name=organization.name,
        )
        return RedirectResponse(url="/organizations", status_code=303)

    @app.post("/organizations/{org_id}/delete")
    def organization_delete(
        request: Request,
        org_id: str,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/organizations")
        if csrf_error:
            return csrf_error
        organization = request.app.state.organization_repo.get_organization_record(org_id)
        if not organization:
            flash(request, "error", "Organization not found")
            return RedirectResponse(url="/organizations", status_code=303)
        if request.app.state.job_repo.count_jobs(org_id=organization.org_id):
            flash(request, "error", "Organization has job history and cannot be deleted")
            return RedirectResponse(url="/organizations", status_code=303)
        request.app.state.connector_repo.delete_connectors_for_org(organization.org_id)
        request.app.state.exclusion_repo.delete_rules_for_org(organization.org_id)
        ManagedGroupBindingRepository(request.app.state.db_manager).delete_bindings_for_org(organization.org_id)
        ObjectStateRepository(request.app.state.db_manager).delete_states_for_org(organization.org_id)
        request.app.state.attribute_mapping_repo.delete_rules_for_org(organization.org_id)
        request.app.state.user_binding_repo.delete_bindings_for_org(organization.org_id)
        request.app.state.department_override_repo.delete_overrides_for_org(organization.org_id)
        request.app.state.exception_rule_repo.delete_rules_for_org(organization.org_id)
        request.app.state.offboarding_repo.delete_records_for_org(organization.org_id)
        request.app.state.lifecycle_repo.delete_records_for_org(organization.org_id)
        request.app.state.custom_group_binding_repo.delete_bindings_for_org(organization.org_id)
        request.app.state.replay_request_repo.delete_requests_for_org(organization.org_id)
        request.app.state.audit_repo.delete_logs_for_org(organization.org_id)
        request.app.state.org_config_repo.delete_config(organization.org_id)
        request.app.state.settings_repo.delete_org_scoped_values(organization.org_id)
        try:
            request.app.state.organization_repo.delete_organization(organization.org_id)
        except Exception as exc:
            flash_t(request, "error", "Failed to delete organization: {error}", error=str(exc))
            return RedirectResponse(url="/organizations", status_code=303)
        if request.session.get("selected_org_id") == organization.org_id:
            request.session["selected_org_id"] = "default"
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="organization.delete",
            target_type="organization",
            target_id=organization.org_id,
            result="success",
            message="Deleted organization definition",
            payload={"name": organization.name},
        )
        flash_t(request, "success", "Organization {name} deleted", name=organization.name)
        return RedirectResponse(url="/organizations", status_code=303)

    @app.get("/config", response_class=HTMLResponse)
    def config_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        request.session.pop(CONFIG_PREVIEW_SESSION_KEY, None)
        return render(
            request,
            "config.html",
            **build_config_page_context(request),
        )

    @app.post("/config/preview")
    def config_preview(
        request: Request,
        csrf_token: str = Form(""),
        source_provider: str = Form("wecom"),
        corpid: str = Form(""),
        agentid: str = Form(""),
        corpsecret: str = Form(""),
        webhook_url: str = Form(""),
        ldap_server: str = Form(""),
        ldap_domain: str = Form(""),
        ldap_username: str = Form(""),
        ldap_password: str = Form(""),
        ldap_port: int = Form(636),
        ldap_use_ssl: Optional[str] = Form(None),
        ldap_validate_cert: Optional[str] = Form(None),
        ldap_ca_cert_path: str = Form(""),
        default_password: str = Form(""),
        force_change_password: Optional[str] = Form(None),
        password_complexity: str = Form("strong"),
        schedule_time: str = Form("03:00"),
        retry_interval: int = Form(60),
        max_retries: int = Form(3),
        group_display_separator: str = Form("-"),
        group_recursive_enabled: Optional[str] = Form(None),
        managed_relation_cleanup_enabled: Optional[str] = Form(None),
        schedule_execution_mode: str = Form("apply"),
        web_bind_host: str = Form("127.0.0.1"),
        web_bind_port: int = Form(8000),
        web_public_base_url: str = Form(""),
        web_session_cookie_secure_mode: str = Form("auto"),
        web_trust_proxy_headers: Optional[str] = Form(None),
        web_forwarded_allow_ips: str = Form("127.0.0.1"),
        user_ou_placement_strategy: str = Form("wecom_primary_department"),
        soft_excluded_groups: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/config")
        if csrf_error:
            return csrf_error

        submission = build_config_submission(
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
            user_ou_placement_strategy=user_ou_placement_strategy,
            soft_excluded_groups=soft_excluded_groups,
        )
        preview = build_config_change_preview(request, submission)
        if preview["changed_count"] == 0:
            request.session.pop(CONFIG_PREVIEW_SESSION_KEY, None)
            flash(request, "warning", "No configuration changes were detected")
            return RedirectResponse(url="/config", status_code=303)

        preview_token = secrets.token_urlsafe(12)
        request.session[CONFIG_PREVIEW_SESSION_KEY] = {
            "token": preview_token,
            "submission": submission,
        }
        return render(
            request,
            "config.html",
            **build_config_page_context(
                request,
                editable_override=build_config_editable_override(request, submission),
                config_change_preview=preview,
                preview_token=preview_token,
            ),
        )

    @app.post("/config/confirm")
    def config_confirm(request: Request, csrf_token: str = Form(""), preview_token: str = Form("")):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/config")
        if csrf_error:
            return csrf_error

        preview_payload = request.session.get(CONFIG_PREVIEW_SESSION_KEY)
        if not isinstance(preview_payload, dict) or str(preview_payload.get("token") or "") != str(preview_token or ""):
            flash(request, "error", "The pending configuration preview has expired. Preview the changes again.")
            return RedirectResponse(url="/config", status_code=303)

        try:
            apply_config_submission(
                request,
                user=user,
                submission=dict(preview_payload.get("submission") or {}),
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/config", status_code=303)
        finally:
            request.session.pop(CONFIG_PREVIEW_SESSION_KEY, None)

        persisted_web_runtime_settings = resolve_web_runtime_settings(request.app.state.settings_repo)
        flash(
            request,
            "success",
            (
                "Configuration saved. Restart the web process to apply deployment security changes."
                if web_runtime_requires_restart(
                    request.app.state.web_runtime_settings,
                    persisted_web_runtime_settings,
                )
                else "Configuration saved"
            ),
        )
        return RedirectResponse(url="/config", status_code=303)

    @app.post("/config")
    def config_submit(
        request: Request,
        csrf_token: str = Form(""),
        source_provider: str = Form("wecom"),
        corpid: str = Form(""),
        agentid: str = Form(""),
        corpsecret: str = Form(""),
        webhook_url: str = Form(""),
        ldap_server: str = Form(""),
        ldap_domain: str = Form(""),
        ldap_username: str = Form(""),
        ldap_password: str = Form(""),
        ldap_port: int = Form(636),
        ldap_use_ssl: Optional[str] = Form(None),
        ldap_validate_cert: Optional[str] = Form(None),
        ldap_ca_cert_path: str = Form(""),
        default_password: str = Form(""),
        force_change_password: Optional[str] = Form(None),
        password_complexity: str = Form("strong"),
        schedule_time: str = Form("03:00"),
        retry_interval: int = Form(60),
        max_retries: int = Form(3),
        group_display_separator: str = Form("-"),
        group_recursive_enabled: Optional[str] = Form(None),
        managed_relation_cleanup_enabled: Optional[str] = Form(None),
        schedule_execution_mode: str = Form("apply"),
        web_bind_host: str = Form("127.0.0.1"),
        web_bind_port: int = Form(8000),
        web_public_base_url: str = Form(""),
        web_session_cookie_secure_mode: str = Form("auto"),
        web_trust_proxy_headers: Optional[str] = Form(None),
        web_forwarded_allow_ips: str = Form("127.0.0.1"),
        user_ou_placement_strategy: str = Form("wecom_primary_department"),
        soft_excluded_groups: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/config")
        if csrf_error:
            return csrf_error

        submission = build_config_submission(
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
            user_ou_placement_strategy=user_ou_placement_strategy,
            soft_excluded_groups=soft_excluded_groups,
        )
        apply_config_submission(request, user=user, submission=submission)
        request.session.pop(CONFIG_PREVIEW_SESSION_KEY, None)
        persisted_web_runtime_settings = resolve_web_runtime_settings(request.app.state.settings_repo)
        flash(
            request,
            "success",
            (
                "Configuration saved. Restart the web process to apply deployment security changes."
                if web_runtime_requires_restart(
                    request.app.state.web_runtime_settings,
                    persisted_web_runtime_settings,
                )
                else "Configuration saved"
            ),
        )
        return RedirectResponse(url="/config", status_code=303)

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

    @app.get("/mappings", response_class=HTMLResponse)
    def mappings_page(request: Request):
        user = require_capability(request, "mappings.read")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        remembered_filters = resolve_remembered_filters(
            request,
            page_name="mappings",
            defaults={"q": "", "status": "all"},
        )
        query = str(remembered_filters["q"])
        status = str(remembered_filters["status"] or "all").strip().lower()
        binding_page = parse_page_number(request.query_params.get("binding_page"), 1)
        override_page = parse_page_number(request.query_params.get("override_page"), 1)
        bindings, binding_page_data = fetch_page(
            lambda *, limit, offset: request.app.state.user_binding_repo.list_binding_records_page(
                limit=limit,
                offset=offset,
                query=query,
                status=status,
                org_id=current_org.org_id,
            ),
            page=binding_page,
            page_size=20,
        )
        overrides, override_page_data = fetch_page(
            lambda *, limit, offset: request.app.state.department_override_repo.list_override_records_page(
                limit=limit,
                offset=offset,
                query=query,
                org_id=current_org.org_id,
            ),
            page=override_page,
            page_size=20,
        )
        return render(
            request,
            "mappings.html",
            page="mappings",
            title="Mappings",
            bindings=bindings,
            overrides=overrides,
            mapping_query=query,
            mapping_status=status,
            binding_page_data=binding_page_data,
            override_page_data=override_page_data,
            department_name_map=load_department_name_map(request),
            filters_are_remembered=True,
        )

    @app.get("/mappings/export")
    def mappings_export(request: Request):
        user = require_capability(request, "mappings.read")
        if isinstance(user, RedirectResponse):
            return user

        query = (request.query_params.get("q") or "").strip()
        status = (request.query_params.get("status") or "all").strip().lower()
        current_org = get_current_org(request)

        def iter_rows():
            for item in iter_all_pages(
                lambda *, limit, offset: request.app.state.user_binding_repo.list_binding_records_page(
                    limit=limit,
                    offset=offset,
                    query=query,
                    status=status,
                    org_id=current_org.org_id,
                )
            ):
                yield [
                    "binding",
                    item.source_user_id,
                    item.ad_username,
                    "",
                    "true" if item.is_enabled else "false",
                    item.source,
                    item.notes,
                    item.updated_at,
                ]
            for item in iter_all_pages(
                lambda *, limit, offset: request.app.state.department_override_repo.list_override_records_page(
                    limit=limit,
                    offset=offset,
                    query=query,
                    org_id=current_org.org_id,
                )
            ):
                yield [
                    "override",
                    item.source_user_id,
                    "",
                    item.primary_department_id,
                    "",
                    "",
                    item.notes,
                    item.updated_at,
                ]

        return stream_csv(
            header=[
                "record_type",
                "source_user_id",
                "ad_username",
                "primary_department_id",
                "is_enabled",
                "source",
                "notes",
                "updated_at",
            ],
            row_iterable=iter_rows(),
            filename="mappings-export.csv",
        )

    @app.post("/mappings/bind")
    def mappings_bind_submit(
        request: Request,
        csrf_token: str = Form(""),
        source_user_id: str = Form(""),
        legacy_source_user_id: str = Form("", alias="wecom_userid"),
        ad_username: str = Form(...),
        notes: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/mappings")
        if csrf_error:
            return csrf_error

        source_user_id = (source_user_id or "").strip() or (legacy_source_user_id or "").strip()
        ad_username = ad_username.strip()
        if not source_user_id or not ad_username:
            flash(request, "error", "Source user ID and AD username are required")
            return RedirectResponse(url="/mappings", status_code=303)

        conflict_message = validate_binding_target(request, source_user_id, ad_username)
        if conflict_message:
            flash(request, "error", conflict_message)
            return RedirectResponse(url="/mappings", status_code=303)

        current_org = get_current_org(request)
        request.app.state.user_binding_repo.upsert_binding_for_source_user(
            source_user_id,
            ad_username,
            org_id=current_org.org_id,
            source="manual",
            notes=notes.strip(),
            preserve_manual=False,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="mapping.bind_upsert",
            target_type="user_identity_binding",
            target_id=source_user_id,
            result="success",
            message="Saved source to AD identity binding",
            payload={"source_user_id": source_user_id, "ad_username": ad_username},
        )
        flash(request, "success", "Identity binding saved")
        return RedirectResponse(url="/mappings", status_code=303)

    @app.post("/mappings/import")
    def mappings_import_submit(
        request: Request,
        csrf_token: str = Form(""),
        bulk_bindings: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/mappings")
        if csrf_error:
            return csrf_error

        rows, parse_errors = parse_bulk_bindings(bulk_bindings)
        if parse_errors:
            flash(request, "error", "; ".join(parse_errors[:5]))
            return RedirectResponse(url="/mappings", status_code=303)
        if not rows:
            flash(request, "error", "Bulk import content is empty")
            return RedirectResponse(url="/mappings", status_code=303)

        imported_count = 0
        conflicts: list[str] = []
        current_org = get_current_org(request)
        for row in rows:
            conflict_message = validate_binding_target(request, row["source_user_id"], row["ad_username"])
            if conflict_message:
                conflicts.append(conflict_message)
                continue
            request.app.state.user_binding_repo.upsert_binding_for_source_user(
                row["source_user_id"],
                row["ad_username"],
                org_id=current_org.org_id,
                source="manual",
                notes=row["notes"],
                preserve_manual=False,
            )
            imported_count += 1

        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="mapping.bind_import",
            target_type="user_identity_binding",
            target_id="bulk",
            result="success" if not conflicts else "warning",
            message="Imported identity bindings in bulk",
            payload={"imported_count": imported_count, "conflict_count": len(conflicts)},
        )
        if conflicts:
            flash(
                request,
                "error",
                f"Imported {imported_count} rows, skipped {len(conflicts)} conflict rows: "
                f"{'; '.join(conflicts[:3])}",
            )
        else:
            flash_t(request, "success", "Imported {imported_count} identity bindings", imported_count=imported_count)
        return RedirectResponse(url="/mappings", status_code=303)

    @app.post("/mappings/bind/{source_user_id}/toggle")
    def mappings_toggle_binding(
        request: Request,
        source_user_id: str,
        csrf_token: str = Form(""),
        enabled: str = Form(...),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/mappings")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        binding = request.app.state.user_binding_repo.get_binding_record_by_source_user_id(
            source_user_id,
            org_id=current_org.org_id,
        )
        if not binding:
            flash_t(request, "error", "Binding not found: {source_user_id}", source_user_id=source_user_id)
            return RedirectResponse(url="/mappings", status_code=303)

        new_state = _to_bool(enabled, binding.is_enabled)
        request.app.state.user_binding_repo.set_enabled_for_source_user(
            source_user_id,
            new_state,
            org_id=current_org.org_id,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="mapping.bind_toggle",
            target_type="user_identity_binding",
            target_id=source_user_id,
            result="success",
            message=f"{'Enabled' if new_state else 'Disabled'} identity binding",
            payload={"source_user_id": source_user_id, "ad_username": binding.ad_username},
        )
        flash_t(
            request,
            "success",
            "Binding {source_user_id} enabled" if new_state else "Binding {source_user_id} disabled",
            source_user_id=source_user_id,
        )
        return RedirectResponse(url="/mappings", status_code=303)

    @app.post("/mappings/override")
    def mappings_override_submit(
        request: Request,
        csrf_token: str = Form(""),
        source_user_id: str = Form(""),
        legacy_source_user_id: str = Form("", alias="wecom_userid"),
        primary_department_id: str = Form(...),
        notes: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/mappings")
        if csrf_error:
            return csrf_error

        source_user_id = (source_user_id or "").strip() or (legacy_source_user_id or "").strip()
        primary_department_id = primary_department_id.strip()
        if not source_user_id or not primary_department_id:
            flash(request, "error", "Source user ID and primary department ID are required")
            return RedirectResponse(url="/mappings", status_code=303)

        department_exists, department_error = department_exists_in_source_provider(request, primary_department_id)
        if not department_exists:
            flash(request, "error", department_error or "Primary department validation failed")
            return RedirectResponse(url="/mappings", status_code=303)

        current_org = get_current_org(request)
        request.app.state.department_override_repo.upsert_override_for_source_user(
            source_user_id,
            primary_department_id,
            org_id=current_org.org_id,
            notes=notes.strip(),
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="mapping.department_override_upsert",
            target_type="user_department_override",
            target_id=source_user_id,
            result="success",
            message="Saved primary department override",
            payload={"source_user_id": source_user_id, "primary_department_id": primary_department_id},
        )
        flash(request, "success", "Primary department override saved")
        return RedirectResponse(url="/mappings", status_code=303)

    @app.post("/mappings/override/{source_user_id}/delete")
    def mappings_override_delete(
        request: Request,
        source_user_id: str,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/mappings")
        if csrf_error:
            return csrf_error

        request.app.state.department_override_repo.delete_override_for_source_user(
            source_user_id,
            org_id=get_current_org(request).org_id,
        )
        request.app.state.audit_repo.add_log(
            org_id=get_current_org(request).org_id,
            actor_username=user.username,
            action_type="mapping.department_override_delete",
            target_type="user_department_override",
            target_id=source_user_id,
            result="success",
            message="Deleted primary department override",
        )
        flash_t(
            request,
            "success",
            "Deleted primary department override for {source_user_id}",
            source_user_id=source_user_id,
        )
        return RedirectResponse(url="/mappings", status_code=303)
    @app.get("/exceptions", response_class=HTMLResponse)
    def exceptions_page(request: Request):
        user = require_capability(request, "exceptions.read")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        remembered_filters = resolve_remembered_filters(
            request,
            page_name="exceptions",
            defaults={"q": "", "status": "all", "rule_type": "all"},
        )
        query = str(remembered_filters["q"])
        status = str(remembered_filters["status"] or "all").strip().lower()
        requested_rule_type = str(remembered_filters["rule_type"] or "all").strip().lower()
        normalized_rule_type = normalize_exception_rule_type(requested_rule_type)
        page_number = parse_page_number(request.query_params.get("page_number"), 1)
        rules, page_data = fetch_page(
            lambda *, limit, offset: request.app.state.exception_rule_repo.list_rule_records_page(
                limit=limit,
                offset=offset,
                query=query,
                rule_type="" if requested_rule_type == "all" else normalized_rule_type,
                status=status,
                org_id=current_org.org_id,
            ),
            page=page_number,
            page_size=25,
        )
        return render(
            request,
            "exceptions.html",
            page="exceptions",
            title="Exception Rules",
            exception_rules=rules,
            exception_page_data=page_data,
            exception_query=query,
            exception_status=status,
            exception_rule_type=normalized_rule_type if normalized_rule_type else "all",
            exception_rule_definitions=EXCEPTION_RULE_DEFINITIONS,
            exception_match_type_labels=EXCEPTION_MATCH_TYPE_LABELS,
            user_exception_rule_types=[
                rule_name
                for rule_name, definition in EXCEPTION_RULE_DEFINITIONS.items()
                if definition.get("match_type") in {"source_user_id", "wecom_userid"}
            ],
            department_exception_rule_types=[
                rule_name
                for rule_name, definition in EXCEPTION_RULE_DEFINITIONS.items()
                if definition.get("match_type") == "department_id"
            ],
            group_exception_rule_types=[
                rule_name
                for rule_name, definition in EXCEPTION_RULE_DEFINITIONS.items()
                if definition.get("match_type") == "group_sam"
            ],
            department_name_map=load_department_name_map(request),
            filters_are_remembered=True,
        )

    @app.post("/exceptions")
    def exceptions_submit(
        request: Request,
        csrf_token: str = Form(""),
        rule_type: str = Form(...),
        match_value: str = Form(...),
        notes: str = Form(""),
        expires_at: str = Form(""),
        is_once: Optional[str] = Form(None),
    ):
        user = require_capability(request, "exceptions.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/exceptions")
        if csrf_error:
            return csrf_error

        normalized_rule_type = normalize_exception_rule_type(rule_type)
        rule_definition = get_exception_rule_definition(normalized_rule_type)
        normalized_match_value = match_value.strip()
        if not rule_definition or not normalized_match_value:
            flash(request, "error", "Invalid exception rule input")
            return RedirectResponse(url="/exceptions", status_code=303)

        if rule_definition.get("match_type") == "department_id":
            department_exists, department_error = department_exists_in_source_provider(request, normalized_match_value)
            if not department_exists:
                flash(request, "error", department_error or "Invalid department id")
                return RedirectResponse(url="/exceptions", status_code=303)
        try:
            normalized_expires_at = normalize_optional_datetime_input(expires_at)
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/exceptions", status_code=303)

        try:
            current_org = get_current_org(request)
            request.app.state.exception_rule_repo.upsert_rule(
                rule_type=normalized_rule_type,
                match_value=normalized_match_value,
                org_id=current_org.org_id,
                notes=notes.strip(),
                expires_at=normalized_expires_at,
                is_once=_to_bool(is_once, False),
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/exceptions", status_code=303)
        enqueue_replay_request(
            app=request.app,
            request_type="exception_rule_changed",
            requested_by=user.username,
            org_id=current_org.org_id,
            target_scope="rule",
            target_id=f"{normalized_rule_type}:{normalized_match_value}",
            trigger_reason="exception_rule_saved",
            payload={
                "rule_type": normalized_rule_type,
                "match_value": normalized_match_value,
            },
        )

        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="exception_rule.upsert",
            target_type="sync_exception_rule",
            target_id=f"{normalized_rule_type}:{normalized_match_value}",
            result="success",
            message="Saved sync exception rule",
            payload={
                "rule_type": normalized_rule_type,
                "match_type": rule_definition.get("match_type"),
                "match_value": normalized_match_value,
                "expires_at": normalized_expires_at,
                "is_once": _to_bool(is_once, False),
            },
        )
        flash(request, "success", "Exception rule saved")
        return RedirectResponse(url="/exceptions", status_code=303)

    @app.post("/exceptions/import")
    def exceptions_import_submit(
        request: Request,
        csrf_token: str = Form(""),
        bulk_rules: str = Form(""),
    ):
        user = require_capability(request, "exceptions.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/exceptions")
        if csrf_error:
            return csrf_error

        rows, parse_errors = parse_bulk_exception_rules(bulk_rules)
        if parse_errors:
            flash(request, "error", "; ".join(parse_errors[:5]))
            return RedirectResponse(url="/exceptions", status_code=303)
        if not rows:
            flash(request, "error", "Bulk exception rule content is empty")
            return RedirectResponse(url="/exceptions", status_code=303)

        imported_count = 0
        import_errors: list[str] = []
        current_org = get_current_org(request)
        for row in rows:
            normalized_rule_type = normalize_exception_rule_type(row["rule_type"])
            rule_definition = get_exception_rule_definition(normalized_rule_type)
            if not rule_definition:
                import_errors.append(f"Line {row['line_number']}: unsupported rule_type {row['rule_type']}")
                continue
            if rule_definition.get("match_type") == "department_id":
                department_exists, department_error = department_exists_in_source_provider(request, str(row["match_value"]))
                if not department_exists:
                    import_errors.append(
                        f"Line {row['line_number']}: {department_error or 'invalid department id'}"
                    )
                    continue
            try:
                normalized_expires_at = normalize_optional_datetime_input(str(row["expires_at"]))
                request.app.state.exception_rule_repo.upsert_rule(
                    rule_type=normalized_rule_type,
                    match_value=str(row["match_value"]),
                    org_id=current_org.org_id,
                    notes=str(row["notes"]),
                    is_enabled=bool(row["is_enabled"]),
                    expires_at=normalized_expires_at,
                    is_once=bool(row["is_once"]),
                )
            except ValueError as exc:
                import_errors.append(f"Line {row['line_number']}: {exc}")
                continue
            imported_count += 1
        if imported_count:
            enqueue_replay_request(
                app=request.app,
                request_type="exception_rule_import",
                requested_by=user.username,
                org_id=current_org.org_id,
                target_scope="bulk",
                target_id="exceptions",
                trigger_reason="exception_rules_imported",
                payload={"imported_count": imported_count},
            )

        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="exception_rule.import",
            target_type="sync_exception_rule",
            target_id="bulk",
            result="success" if not import_errors else "warning",
            message="Imported sync exception rules",
            payload={"imported_count": imported_count, "error_count": len(import_errors)},
        )
        if import_errors:
            flash(
                request,
                "error",
                f"Imported {imported_count} rows, skipped {len(import_errors)} rows: "
                f"{'; '.join(import_errors[:3])}",
            )
        else:
            flash_t(request, "success", "Imported {imported_count} exception rules", imported_count=imported_count)
        return RedirectResponse(url="/exceptions", status_code=303)

    @app.get("/exceptions/export")
    def exceptions_export(request: Request):
        user = require_capability(request, "exceptions.read")
        if isinstance(user, RedirectResponse):
            return user

        query = (request.query_params.get("q") or "").strip()
        status = (request.query_params.get("status") or "all").strip().lower()
        requested_rule_type = (request.query_params.get("rule_type") or "all").strip().lower()
        current_org = get_current_org(request)

        def iter_rows():
            for item in iter_all_pages(
                lambda *, limit, offset: request.app.state.exception_rule_repo.list_rule_records_page(
                    limit=limit,
                    offset=offset,
                    query=query,
                    rule_type="" if requested_rule_type == "all" else normalize_exception_rule_type(requested_rule_type),
                    status=status,
                    org_id=current_org.org_id,
                )
            ):
                yield [
                    item.rule_type,
                    item.match_value,
                    item.notes or "",
                    "true" if item.is_enabled else "false",
                    item.expires_at or "",
                    "true" if item.is_once else "false",
                ]

        return stream_csv(
            header=["rule_type", "match_value", "notes", "is_enabled", "expires_at", "is_once"],
            row_iterable=iter_rows(),
            filename="exception-rules-export.csv",
        )

    @app.post("/exceptions/{rule_id}/toggle")
    def exceptions_toggle(
        request: Request,
        rule_id: int,
        csrf_token: str = Form(""),
        enabled: str = Form(...),
    ):
        user = require_capability(request, "exceptions.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/exceptions")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        rule_record = request.app.state.exception_rule_repo.get_rule_record(rule_id, org_id=current_org.org_id)
        if not rule_record:
            flash(request, "error", "Exception rule not found")
            return RedirectResponse(url="/exceptions", status_code=303)

        new_state = _to_bool(enabled, rule_record.is_enabled)
        request.app.state.exception_rule_repo.set_enabled(rule_id, new_state, org_id=current_org.org_id)
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="exception_rule.toggle",
            target_type="sync_exception_rule",
            target_id=str(rule_id),
            result="success",
            message=f"{'Enabled' if new_state else 'Disabled'} sync exception rule",
            payload={
                "rule_type": rule_record.rule_type,
                "match_type": rule_record.match_type,
                "match_value": rule_record.match_value,
            },
        )
        flash(
            request,
            "success",
            "Exception rule enabled" if new_state else "Exception rule disabled",
        )
        return RedirectResponse(url="/exceptions", status_code=303)

    @app.post("/exceptions/{rule_id}/delete")
    def exceptions_delete(
        request: Request,
        rule_id: int,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "exceptions.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/exceptions")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        rule_record = request.app.state.exception_rule_repo.get_rule_record(rule_id, org_id=current_org.org_id)
        if not rule_record:
            flash(request, "error", "Exception rule not found")
            return RedirectResponse(url="/exceptions", status_code=303)

        request.app.state.exception_rule_repo.delete_rule(rule_id, org_id=current_org.org_id)
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="exception_rule.delete",
            target_type="sync_exception_rule",
            target_id=str(rule_id),
            result="success",
            message="Deleted sync exception rule",
            payload={
                "rule_type": rule_record.rule_type,
                "match_type": rule_record.match_type,
                "match_value": rule_record.match_value,
            },
        )
        flash(request, "success", "Exception rule deleted")
        return RedirectResponse(url="/exceptions", status_code=303)

    @app.get("/conflicts", response_class=HTMLResponse)
    def conflicts_page(request: Request):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        remembered_filters = resolve_remembered_filters(
            request,
            page_name="conflicts",
            defaults={"q": "", "status": "open", "job_id": ""},
        )
        query = str(remembered_filters["q"])
        status = str(remembered_filters["status"] or "open").strip().lower()
        job_id = str(remembered_filters["job_id"])
        page_number = parse_page_number(request.query_params.get("page_number"), 1)

        status_filter = status if status in {"open", "resolved", "dismissed"} else None
        conflicts, page_data = fetch_page(
            lambda *, limit, offset: request.app.state.conflict_repo.list_conflict_records_page(
                limit=limit,
                offset=offset,
                job_id=job_id or None,
                status=status_filter,
                query=query,
                org_id=current_org.org_id,
            ),
            page=page_number,
            page_size=30,
        )
        conflict_recommendations = {
            item.id: recommend_conflict_resolution(item)
            for item in conflicts
        }
        return render(
            request,
            "conflicts.html",
            page="conflicts",
            title="Conflict Queue",
            conflicts=conflicts,
            conflict_recommendations=conflict_recommendations,
            conflict_page_data=page_data,
            conflict_query=query,
            conflict_status=status if status_filter else "all",
            conflict_job_id=job_id,
            current_org=current_org,
            filters_are_remembered=True,
        )

    @app.post("/conflicts/{conflict_id}/resolve-binding")
    def resolve_conflict_binding(
        request: Request,
        conflict_id: int,
        csrf_token: str = Form(""),
        ad_username: str = Form(...),
        return_query: str = Form(""),
        return_status: str = Form(""),
        return_job_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
        if not conflict:
            flash(request, "error", "Conflict record not found")
            return RedirectResponse(url="/conflicts", status_code=303)
        fallback_url = build_conflicts_return_url(_to_text(return_query), _to_text(return_status), _to_text(return_job_id) or conflict.job_id)
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        if conflict.status != "open":
            flash(request, "error", "Conflict is already processed")
            return RedirectResponse(url=fallback_url, status_code=303)

        ok, normalized_ad_username, resolved_count = apply_conflict_manual_binding(
            app=request.app,
            conflict=conflict,
            ad_username=ad_username,
            actor_username=user.username,
            org_id=current_org.org_id,
        )
        if not ok:
            flash(request, "error", normalized_ad_username)
            return RedirectResponse(url=fallback_url, status_code=303)
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="conflict.resolve_manual_binding",
            target_type="sync_conflict",
            target_id=str(conflict.id),
            result="success",
            message="Resolved conflict by creating manual binding",
            payload={
                "job_id": conflict.job_id,
                "source_user_id": conflict.source_id,
                "ad_username": normalized_ad_username,
                "resolved_count": resolved_count,
            },
        )
        flash_t(
            request,
            "success",
            "Resolved conflict with manual binding {source_id} -> {ad_username}",
            source_id=conflict.source_id,
            ad_username=normalized_ad_username,
        )
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/conflicts/{conflict_id}/skip-user")
    def resolve_conflict_with_skip_user(
        request: Request,
        conflict_id: int,
        csrf_token: str = Form(""),
        notes: str = Form(""),
        return_query: str = Form(""),
        return_status: str = Form(""),
        return_job_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
        if not conflict:
            flash(request, "error", "Conflict record not found")
            return RedirectResponse(url="/conflicts", status_code=303)
        fallback_url = build_conflicts_return_url(_to_text(return_query), _to_text(return_status), _to_text(return_job_id) or conflict.job_id)
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        if conflict.status != "open":
            flash(request, "error", "Conflict is already processed")
            return RedirectResponse(url=fallback_url, status_code=303)
        if not conflict.source_id:
            flash(request, "error", "Conflict does not have a source user to whitelist")
            return RedirectResponse(url=fallback_url, status_code=303)

        ok, rule_notes, resolved_count = apply_conflict_skip_user_sync(
            app=request.app,
            conflict=conflict,
            actor_username=user.username,
            org_id=current_org.org_id,
            notes=_to_text(notes),
        )
        if not ok:
            flash(request, "error", rule_notes)
            return RedirectResponse(url=fallback_url, status_code=303)
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="conflict.resolve_skip_user",
            target_type="sync_conflict",
            target_id=str(conflict.id),
            result="success",
            message="Resolved conflict by adding skip_user_sync exception",
            payload={
                "job_id": conflict.job_id,
                "source_user_id": conflict.source_id,
                "notes": rule_notes,
                "resolved_count": resolved_count,
            },
        )
        flash_t(request, "success", "Added skip_user_sync for {source_id}", source_id=conflict.source_id)
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/conflicts/{conflict_id}/apply-recommendation")
    def apply_conflict_recommendation_route(
        request: Request,
        conflict_id: int,
        csrf_token: str = Form(""),
        confirmation_reason: str = Form(""),
        return_query: str = Form(""),
        return_status: str = Form(""),
        return_job_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
        if not conflict:
            flash(request, "error", "Conflict record not found")
            return RedirectResponse(url="/conflicts", status_code=303)
        fallback_url = build_conflicts_return_url(_to_text(return_query), _to_text(return_status), _to_text(return_job_id) or conflict.job_id)
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        if conflict.status != "open":
            flash(request, "error", "Conflict is already processed")
            return RedirectResponse(url=fallback_url, status_code=303)

        ok, detail, resolved_count, recommendation = apply_conflict_recommendation(
            app=request.app,
            conflict=conflict,
            actor_username=user.username,
            org_id=current_org.org_id,
            confirmation_reason=_to_text(confirmation_reason),
        )
        if not ok:
            flash(request, "error", detail)
            return RedirectResponse(url=fallback_url, status_code=303)

        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="conflict.apply_recommendation",
            target_type="sync_conflict",
            target_id=str(conflict.id),
            result="success",
            message="Applied recommended conflict resolution",
            payload={
                "job_id": conflict.job_id,
                "source_user_id": conflict.source_id,
                "recommendation": recommendation,
                "detail": detail,
                "resolved_count": resolved_count,
            },
        )
        flash_t(
            request,
            "success",
            "Applied recommendation: {label}",
            label=str(recommendation.get("label") or "-"),
        )
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/conflicts/{conflict_id}/dismiss")
    def dismiss_conflict(
        request: Request,
        conflict_id: int,
        csrf_token: str = Form(""),
        notes: str = Form(""),
        return_query: str = Form(""),
        return_status: str = Form(""),
        return_job_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
        if not conflict:
            flash(request, "error", "Conflict record not found")
            return RedirectResponse(url="/conflicts", status_code=303)
        fallback_url = build_conflicts_return_url(_to_text(return_query), _to_text(return_status), _to_text(return_job_id) or conflict.job_id)
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error

        request.app.state.conflict_repo.update_conflict_status(
            conflict.id,
            status="dismissed",
            resolution_payload={
                "action": "dismissed",
                "notes": _to_text(notes),
                "actor_username": user.username,
            },
            resolved_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="conflict.dismiss",
            target_type="sync_conflict",
            target_id=str(conflict.id),
            result="success",
            message="Dismissed sync conflict",
            payload={"job_id": conflict.job_id, "notes": _to_text(notes)},
        )
        flash(request, "success", "Conflict dismissed")
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/conflicts/{conflict_id}/reopen")
    def reopen_conflict(
        request: Request,
        conflict_id: int,
        csrf_token: str = Form(""),
        return_query: str = Form(""),
        return_status: str = Form(""),
        return_job_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
        if not conflict:
            flash(request, "error", "Conflict record not found")
            return RedirectResponse(url="/conflicts", status_code=303)
        fallback_url = build_conflicts_return_url(_to_text(return_query), _to_text(return_status), _to_text(return_job_id) or conflict.job_id)
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        if conflict.status == "open":
            flash(request, "error", "Conflict is already open")
            return RedirectResponse(url=fallback_url, status_code=303)

        request.app.state.conflict_repo.update_conflict_status(
            conflict.id,
            status="open",
            resolution_payload=None,
            resolved_at=None,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="conflict.reopen",
            target_type="sync_conflict",
            target_id=str(conflict.id),
            result="success",
            message="Reopened sync conflict",
            payload={"job_id": conflict.job_id, "previous_status": conflict.status},
        )
        flash(request, "success", "Conflict reopened")
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/conflicts/bulk")
    def bulk_conflict_action(
        request: Request,
        csrf_token: str = Form(""),
        action: str = Form(...),
        conflict_ids: list[str] = Form([]),
        notes: str = Form(""),
        return_query: str = Form(""),
        return_status: str = Form(""),
        return_job_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user

        fallback_url = build_conflicts_return_url(_to_text(return_query), _to_text(return_status), _to_text(return_job_id))
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error

        normalized_action = _to_text(action).lower()
        current_org = get_current_org(request)
        raw_conflict_ids = [str(item or "").strip() for item in conflict_ids] if isinstance(conflict_ids, list) else []
        selected_conflict_ids = [int(item) for item in raw_conflict_ids if item.isdigit()]
        if normalized_action not in {"apply_recommendation", "skip_user_sync", "dismiss", "reopen"}:
            flash(request, "error", "Unsupported bulk conflict action")
            return RedirectResponse(url=fallback_url, status_code=303)
        if not selected_conflict_ids:
            flash(request, "error", "No conflicts selected")
            return RedirectResponse(url=fallback_url, status_code=303)
        if normalized_action == "apply_recommendation" and not _to_text(notes):
            for conflict_id in selected_conflict_ids:
                conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
                if not conflict or conflict.status != "open":
                    continue
                if recommendation_requires_confirmation(recommend_conflict_resolution(conflict)):
                    flash(request, "error", "Low-confidence recommendations require a confirmation reason for bulk apply")
                    return RedirectResponse(url=fallback_url, status_code=303)

        updated_count = 0
        skipped_count = 0
        for conflict_id in selected_conflict_ids:
            conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
            if not conflict:
                skipped_count += 1
                continue

            if normalized_action == "reopen":
                if conflict.status == "open":
                    skipped_count += 1
                    continue
                request.app.state.conflict_repo.update_conflict_status(
                    conflict.id,
                    status="open",
                    resolution_payload=None,
                    resolved_at=None,
                )
                updated_count += 1
                continue

            if conflict.status != "open":
                skipped_count += 1
                continue

            if normalized_action == "dismiss":
                request.app.state.conflict_repo.update_conflict_status(
                    conflict.id,
                    status="dismissed",
                    resolution_payload={
                        "action": "dismissed",
                        "notes": _to_text(notes),
                        "actor_username": user.username,
                        "bulk": True,
                    },
                    resolved_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                )
                updated_count += 1
                continue

            if normalized_action == "apply_recommendation":
                ok, _detail, resolved_count, _recommendation = apply_conflict_recommendation(
                    app=request.app,
                    conflict=conflict,
                    actor_username=user.username,
                    org_id=current_org.org_id,
                    confirmation_reason=_to_text(notes),
                )
                if ok and resolved_count:
                    updated_count += 1
                else:
                    skipped_count += 1
                continue

            if normalized_action == "skip_user_sync":
                ok, _rule_notes, resolved_count = apply_conflict_skip_user_sync(
                    app=request.app,
                    conflict=conflict,
                    actor_username=user.username,
                    org_id=current_org.org_id,
                    notes=_to_text(notes) or f"bulk resolved from conflict {conflict.id}",
                )
                if ok and resolved_count:
                    updated_count += 1
                else:
                    skipped_count += 1

        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="conflict.bulk_action",
            target_type="sync_conflict",
            target_id="bulk",
            result="success" if updated_count else "warning",
            message="Executed bulk conflict action",
            payload={
                "action": normalized_action,
                "selected_count": len(selected_conflict_ids),
                "updated_count": updated_count,
                "skipped_count": skipped_count,
            },
        )
        flash(
            request,
            "success" if updated_count else "warning",
            f"Bulk action {normalized_action} updated {updated_count} conflicts, skipped {skipped_count}",
        )
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.get("/jobs", response_class=HTMLResponse)
    def jobs_page(request: Request):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        return render(
            request,
            "jobs.html",
            page="jobs",
            title="Job Center",
            jobs=request.app.state.job_repo.list_recent_job_records(limit=30, org_id=current_org.org_id),
            active_job=request.app.state.job_repo.get_active_job_record(org_id=current_org.org_id),
            sync_runner_error=request.app.state.sync_runner.last_error,
            current_org=current_org,
        )

    @app.post("/jobs/{job_id}/approve")
    def approve_job_review(
        request: Request,
        job_id: str,
        csrf_token: str = Form(""),
        review_notes: str = Form(""),
    ):
        user = require_capability(request, "jobs.review")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, f"/jobs/{job_id}")
        if csrf_error:
            return csrf_error

        review_record = request.app.state.review_repo.get_review_record_by_job_id(job_id)
        if not review_record:
            flash(request, "error", "This job does not have a pending high-risk review")
            return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)
        current_org = get_current_org(request)
        job_record = request.app.state.job_repo.get_job_record(job_id)
        if not job_record or (job_record.org_id and job_record.org_id != current_org.org_id):
            flash(request, "error", "Job does not belong to the current organization")
            return RedirectResponse(url="/jobs", status_code=303)

        review_ttl_minutes = max(request.app.state.settings_repo.get_int("high_risk_review_ttl_minutes", 240), 1)
        expires_at = (time.time() + review_ttl_minutes * 60)
        expires_at_iso = datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat(timespec="seconds")
        request.app.state.review_repo.approve_review(
            job_id,
            reviewer_username=user.username,
            review_notes=review_notes.strip(),
            expires_at=expires_at_iso,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="job.review_approve",
            target_type="sync_job",
            target_id=job_id,
            result="success",
            message="Approved high-risk synchronization plan",
            payload={"expires_at": expires_at_iso},
        )
        enqueue_replay_request(
            app=request.app,
            request_type="plan_approval",
            requested_by=user.username,
            org_id=current_org.org_id,
            target_scope="job",
            target_id=job_id,
            trigger_reason="high_risk_plan_approved",
            payload={"expires_at": expires_at_iso},
        )
        flash(request, "success", "High-risk plan approved. You can rerun apply now.")
        return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)

    @app.post("/jobs/run")
    def run_job(
        request: Request,
        csrf_token: str = Form(""),
        mode: str = Form(...),
    ):
        user = require_capability(request, "jobs.run")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/jobs")
        if csrf_error:
            return csrf_error

        normalized_mode = "dry_run" if mode == "dry_run" else "apply"
        current_org = get_current_org(request)
        ok, message = request.app.state.sync_runner.launch(
            mode=normalized_mode,
            actor_username=user.username,
            org_id=current_org.org_id,
            config_path=current_org.config_path or request.app.state.config_path,
        )
        flash(request, "success" if ok else "error", message)
        return RedirectResponse(url="/jobs", status_code=303)

    @app.get("/jobs/{job_id}", response_class=HTMLResponse)
    def job_detail(request: Request, job_id: str):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user

        job: Optional[SyncJobRecord] = request.app.state.job_repo.get_job_record(job_id)
        if not job:
            flash_t(request, "error", "Job not found: {job_id}", job_id=job_id)
            return RedirectResponse(url="/jobs", status_code=303)
        current_org = get_current_org(request)
        if job.org_id and job.org_id != current_org.org_id:
            flash(request, "error", "Job does not belong to the current organization")
            return RedirectResponse(url="/jobs", status_code=303)
        return render(
            request,
            "job_detail.html",
            page="jobs",
            title=translate_text(get_ui_language(request), "Job Detail {job_id}", job_id=job_id),
            job=job,
            current_org=current_org,
            events=(events_result := fetch_page(
                lambda *, limit, offset: request.app.state.event_repo.list_events_for_job_page(
                    job_id,
                    limit=limit,
                    offset=offset,
                ),
                page=parse_page_number(request.query_params.get("events_page"), 1),
                page_size=25,
            ))[0],
            events_page_data=events_result[1],
            planned_operations=(planned_result := fetch_page(
                lambda *, limit, offset: request.app.state.planned_operation_repo.list_operations_for_job_page(
                    job_id,
                    limit=limit,
                    offset=offset,
                ),
                page=parse_page_number(request.query_params.get("planned_page"), 1),
                page_size=25,
            ))[0],
            planned_operations_page_data=planned_result[1],
            operation_records=(operations_result := fetch_page(
                lambda *, limit, offset: request.app.state.operation_log_repo.list_records_for_job_page(
                    job_id,
                    limit=limit,
                    offset=offset,
                ),
                page=parse_page_number(request.query_params.get("operations_page"), 1),
                page_size=25,
            ))[0],
            operation_records_page_data=operations_result[1],
            conflicts=(conflicts_result := fetch_page(
                lambda *, limit, offset: request.app.state.conflict_repo.list_conflicts_for_job_page(
                    job_id,
                    limit=limit,
                    offset=offset,
                ),
                page=parse_page_number(request.query_params.get("conflicts_page"), 1),
                page_size=25,
            ))[0],
            job_conflicts_page_data=conflicts_result[1],
            review_record=request.app.state.review_repo.get_review_record_by_job_id(job_id),
            summary_json=json.dumps(job.summary or {}, ensure_ascii=False, indent=2),
        )

    @app.get("/database", response_class=HTMLResponse)
    def database_page(request: Request):
        user = require_capability(request, "database.read")
        if isinstance(user, RedirectResponse):
            return user

        db_manager = request.app.state.db_manager
        integrity = db_manager.last_integrity_check or db_manager.run_integrity_check()
        return render(
            request,
            "database.html",
            page="database",
            title="Database Operations",
            db_info=db_manager.runtime_info(),
            integrity=integrity,
            retention_settings={
                "job_history_retention_days": request.app.state.settings_repo.get_int("job_history_retention_days", 30),
                "event_history_retention_days": request.app.state.settings_repo.get_int("event_history_retention_days", 30),
                "audit_log_retention_days": request.app.state.settings_repo.get_int("audit_log_retention_days", 90),
                "backup_retention_days": request.app.state.settings_repo.get_int("backup_retention_days", 30),
                "backup_retention_max_files": request.app.state.settings_repo.get_int("backup_retention_max_files", 30),
            },
        )

    @app.post("/database/check")
    def database_check(request: Request, csrf_token: str = Form("")):
        user = require_capability(request, "database.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/database")
        if csrf_error:
            return csrf_error

        result = request.app.state.db_manager.run_integrity_check()
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="database.check",
            target_type="sqlite",
            target_id=request.app.state.db_manager.db_path,
            result="success" if result.get("ok") else "error",
            message=f"Ran integrity check: {result.get('result')}",
            payload=result,
        )
        flash_t(
            request,
            "success" if result.get("ok") else "error",
            "Integrity check result: {result}",
            result=str(result.get("result") or "-"),
        )
        return RedirectResponse(url="/database", status_code=303)

    @app.post("/database/backup")
    def database_backup(request: Request, csrf_token: str = Form("")):
        user = require_capability(request, "database.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/database")
        if csrf_error:
            return csrf_error

        backup_path = request.app.state.db_manager.backup_database(label="web_manual")
        backup_cleanup = request.app.state.db_manager.cleanup_backups(
            retention_days=request.app.state.settings_repo.get_int("backup_retention_days", 30),
            max_files=request.app.state.settings_repo.get_int("backup_retention_max_files", 30),
        )
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="database.backup",
            target_type="sqlite",
            target_id=request.app.state.db_manager.db_path,
            result="success",
            message="Created database backup",
            payload={
                "backup_path": backup_path,
                "backup_cleanup": backup_cleanup,
            },
        )
        deleted_backups = int(backup_cleanup.get("deleted_backups", 0))
        if deleted_backups:
            flash_t(
                request,
                "success",
                "Backup created: {backup_path}. Pruned {deleted_backups} old backups.",
                backup_path=backup_path,
                deleted_backups=deleted_backups,
            )
        else:
            flash_t(request, "success", "Backup created: {backup_path}", backup_path=backup_path)
        return RedirectResponse(url="/database", status_code=303)

    @app.get("/account", response_class=HTMLResponse)
    def account_page(request: Request):
        user = require_capability(request, "account.manage")
        if isinstance(user, RedirectResponse):
            return user
        return render(request, "account.html", page="account", title="My Account")

    @app.post("/account/password")
    def change_password(
        request: Request,
        csrf_token: str = Form(""),
        current_password: str = Form(...),
        new_password: str = Form(...),
        confirm_password: str = Form(...),
    ):
        user = require_capability(request, "account.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/account")
        if csrf_error:
            return csrf_error

        if not verify_password(current_password, user.password_hash):
            flash(request, "error", "Current password is incorrect")
            return RedirectResponse(url="/account", status_code=303)
        if new_password != confirm_password:
            flash(request, "error", "New passwords do not match")
            return RedirectResponse(url="/account", status_code=303)
        password_error = validate_admin_password(request, new_password)
        if password_error:
            flash(request, "error", password_error)
            return RedirectResponse(url="/account", status_code=303)

        request.app.state.user_repo.set_password(user.username, hash_password(new_password))
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="account.password_change",
            target_type="web_admin_user",
            target_id=user.username,
            result="success",
            message="Changed account password",
        )
        flash(request, "success", "Password updated")
        return RedirectResponse(url="/account", status_code=303)

    @app.get("/users", response_class=HTMLResponse)
    def users_page(request: Request):
        user = require_capability(request, "users.manage")
        if isinstance(user, RedirectResponse):
            return user
        return render(
            request,
            "users.html",
            page="users",
            title="Admin Users",
            users=request.app.state.user_repo.list_user_records(),
        )

    @app.post("/users")
    def create_user(
        request: Request,
        csrf_token: str = Form(""),
        username: str = Form(...),
        password: str = Form(...),
        role: str = Form("operator"),
    ):
        user = require_capability(request, "users.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/users")
        if csrf_error:
            return csrf_error

        username = username.strip()
        role = normalize_role(role, default="operator")
        if role not in WEB_ADMIN_ROLES:
            role = "operator"
        if not username:
            flash(request, "error", "Username is required")
            return RedirectResponse(url="/users", status_code=303)
        password_error = validate_admin_password(request, password)
        if password_error:
            flash(request, "error", password_error)
            return RedirectResponse(url="/users", status_code=303)
        if request.app.state.user_repo.get_user_record_by_username(username):
            flash(request, "error", "Username already exists")
            return RedirectResponse(url="/users", status_code=303)

        request.app.state.user_repo.create_user(username, hash_password(password), role=role)
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="user.create",
            target_type="web_admin_user",
            target_id=username,
            result="success",
            message="Created local administrator account",
            payload={"role": role},
        )
        flash_t(request, "success", "User {username} created", username=username)
        return RedirectResponse(url="/users", status_code=303)

    @app.post("/users/{user_id}/toggle")
    def toggle_user(
        request: Request,
        user_id: int,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "users.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/users")
        if csrf_error:
            return csrf_error

        target = request.app.state.user_repo.get_user_record_by_id(user_id)
        if not target:
            flash(request, "error", "Target account was not found")
            return RedirectResponse(url="/users", status_code=303)
        if target.username == user.username and target.is_enabled:
            flash(request, "error", "You cannot disable the account currently signed in")
            return RedirectResponse(url="/users", status_code=303)

        new_state = not target.is_enabled
        request.app.state.user_repo.set_enabled(user_id, new_state)
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="user.toggle",
            target_type="web_admin_user",
            target_id=target.username,
            result="success",
            message=f"{'Enabled' if new_state else 'Disabled'} local administrator account",
        )
        flash_t(
            request,
            "success",
            "User {username} enabled" if new_state else "User {username} disabled",
            username=target.username,
        )
        return RedirectResponse(url="/users", status_code=303)

    @app.get("/audit", response_class=HTMLResponse)
    def audit_page(request: Request):
        user = require_capability(request, "audit.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        remembered_filters = resolve_remembered_filters(
            request,
            page_name="audit",
            defaults={"q": ""},
        )
        audit_query = str(remembered_filters["q"])
        return render(
            request,
            "audit.html",
            page="audit",
            title="Audit Logs",
            logs=(audit_result := fetch_page(
                lambda *, limit, offset: request.app.state.audit_repo.list_recent_logs_page(
                    limit=limit,
                    offset=offset,
                    query=audit_query,
                    org_id=current_org.org_id,
                    include_global=True,
                ),
                page=parse_page_number(request.query_params.get("page_number"), 1),
                page_size=50,
            ))[0],
            audit_query=audit_query,
            audit_page_data=audit_result[1],
            filters_are_remembered=True,
        )

    return app
