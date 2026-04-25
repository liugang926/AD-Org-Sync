from __future__ import annotations

import secrets
from dataclasses import dataclass, fields
from typing import Any, Optional

from fastapi import FastAPI, Request

from sync_app.modules.sspr import InMemorySSPRSessionStore, SSPRRateLimiter
from sync_app.services.typed_settings import WebSecuritySettings
from sync_app.storage.local_db import (
    AttributeMappingRuleRepository,
    ConfigReleaseSnapshotRepository,
    CustomManagedGroupBindingRepository,
    DataQualitySnapshotRepository,
    DatabaseManager,
    DepartmentOuMappingRepository,
    GroupExclusionRuleRepository,
    IntegrationWebhookOutboxRepository,
    OffboardingQueueRepository,
    OrganizationConfigRepository,
    OrganizationRepository,
    PlannedOperationRepository,
    SettingsRepository,
    SyncConnectorRepository,
    SyncConflictRepository,
    SyncEventRepository,
    IntegrationWebhookSubscriptionRepository,
    SyncExceptionRuleRepository,
    SyncJobRepository,
    SyncOperationLogRepository,
    SyncPlanReviewRepository,
    SyncReplayRequestRepository,
    UserDepartmentOverrideRepository,
    UserIdentityBindingRepository,
    UserLifecycleQueueRepository,
    WebAdminUserRepository,
    WebAuditLogRepository,
)
from sync_app.web.runtime import (
    IntegrationOutboxWorker,
    LoginRateLimiter,
    WebSyncRunner,
    resolve_web_runtime_settings,
)
from sync_app.web.services import WebServiceState, build_web_service_state


@dataclass(slots=True)
class WebRepositoryState:
    db_manager: DatabaseManager
    settings_repo: SettingsRepository
    organization_repo: OrganizationRepository
    org_config_repo: OrganizationConfigRepository
    exclusion_repo: GroupExclusionRuleRepository
    connector_repo: SyncConnectorRepository
    attribute_mapping_repo: AttributeMappingRuleRepository
    department_ou_mapping_repo: DepartmentOuMappingRepository
    custom_group_binding_repo: CustomManagedGroupBindingRepository
    offboarding_repo: OffboardingQueueRepository
    lifecycle_repo: UserLifecycleQueueRepository
    replay_request_repo: SyncReplayRequestRepository
    job_repo: SyncJobRepository
    event_repo: SyncEventRepository
    planned_operation_repo: PlannedOperationRepository
    operation_log_repo: SyncOperationLogRepository
    conflict_repo: SyncConflictRepository
    review_repo: SyncPlanReviewRepository
    exception_rule_repo: SyncExceptionRuleRepository
    user_repo: WebAdminUserRepository
    audit_repo: WebAuditLogRepository
    config_release_snapshot_repo: ConfigReleaseSnapshotRepository
    data_quality_snapshot_repo: DataQualitySnapshotRepository
    integration_webhook_outbox_repo: IntegrationWebhookOutboxRepository
    integration_webhook_subscription_repo: IntegrationWebhookSubscriptionRepository
    user_binding_repo: UserIdentityBindingRepository
    department_override_repo: UserDepartmentOverrideRepository


@dataclass(slots=True)
class WebRuntimeState:
    config_path: str
    session_secret: str
    session_minutes: int
    login_rate_limiter: LoginRateLimiter
    session_cookie_secure: bool
    web_runtime_settings: dict[str, Any]
    startup_persisted_web_runtime_settings: dict[str, Any]
    sync_runner: WebSyncRunner
    integration_outbox_worker: IntegrationOutboxWorker
    sspr_session_store: InMemorySSPRSessionStore
    sspr_rate_limiter: SSPRRateLimiter


@dataclass(slots=True)
class WebAppState:
    repositories: WebRepositoryState
    runtime: WebRuntimeState
    services: WebServiceState

    def bind_to_app(self, app: FastAPI) -> None:
        app.state.web_app_state = self
        for bundle in (self.repositories, self.runtime):
            for field_info in fields(bundle):
                if field_info.name in {"session_secret", "session_minutes"}:
                    continue
                setattr(app.state, field_info.name, getattr(bundle, field_info.name))


def _resolve_fastapi_app(target: FastAPI | Request) -> FastAPI:
    if isinstance(target, FastAPI):
        return target
    return target.app


def get_web_app_state(target: FastAPI | Request) -> WebAppState:
    app = _resolve_fastapi_app(target)
    state = getattr(app.state, "web_app_state", None)
    if state is None:
        raise RuntimeError("Web app state has not been initialized for this application.")
    return state


def get_web_repositories(target: FastAPI | Request) -> WebRepositoryState:
    return get_web_app_state(target).repositories


def get_web_runtime_state(target: FastAPI | Request) -> WebRuntimeState:
    return get_web_app_state(target).runtime


def get_web_services(target: FastAPI | Request) -> WebServiceState:
    return get_web_app_state(target).services


def initialize_web_app_state(
    *,
    db_path: str | None,
    config_path: str,
    bind_host: str | None,
    bind_port: int | None,
    public_base_url: str | None,
    session_cookie_secure_mode: str | None,
    trust_proxy_headers: bool | None,
    forwarded_allow_ips: str | None,
) -> WebAppState:
    db_manager = DatabaseManager(db_path=db_path)
    db_manager.initialize()

    repositories = WebRepositoryState(
        db_manager=db_manager,
        settings_repo=SettingsRepository(db_manager),
        organization_repo=OrganizationRepository(db_manager),
        org_config_repo=OrganizationConfigRepository(db_manager),
        exclusion_repo=GroupExclusionRuleRepository(db_manager),
        connector_repo=SyncConnectorRepository(db_manager),
        attribute_mapping_repo=AttributeMappingRuleRepository(db_manager),
        department_ou_mapping_repo=DepartmentOuMappingRepository(db_manager),
        custom_group_binding_repo=CustomManagedGroupBindingRepository(db_manager),
        offboarding_repo=OffboardingQueueRepository(db_manager),
        lifecycle_repo=UserLifecycleQueueRepository(db_manager),
        replay_request_repo=SyncReplayRequestRepository(db_manager),
        job_repo=SyncJobRepository(db_manager),
        event_repo=SyncEventRepository(db_manager),
        planned_operation_repo=PlannedOperationRepository(db_manager),
        operation_log_repo=SyncOperationLogRepository(db_manager),
        conflict_repo=SyncConflictRepository(db_manager),
        review_repo=SyncPlanReviewRepository(db_manager),
        exception_rule_repo=SyncExceptionRuleRepository(db_manager),
        user_repo=WebAdminUserRepository(db_manager),
        audit_repo=WebAuditLogRepository(db_manager),
        config_release_snapshot_repo=ConfigReleaseSnapshotRepository(db_manager),
        data_quality_snapshot_repo=DataQualitySnapshotRepository(db_manager),
        integration_webhook_outbox_repo=IntegrationWebhookOutboxRepository(db_manager),
        integration_webhook_subscription_repo=IntegrationWebhookSubscriptionRepository(db_manager),
        user_binding_repo=UserIdentityBindingRepository(db_manager),
        department_override_repo=UserDepartmentOverrideRepository(db_manager),
    )

    repositories.organization_repo.ensure_default(config_path=config_path)
    repositories.org_config_repo.ensure_loaded("default", config_path=config_path)

    session_secret = repositories.settings_repo.get_value("web_session_secret", "") or ""
    if not session_secret:
        session_secret = secrets.token_urlsafe(48)
        repositories.settings_repo.set_value("web_session_secret", session_secret, "string")

    security_settings = WebSecuritySettings.load(repositories.settings_repo)
    session_minutes = security_settings.session_idle_minutes
    startup_persisted_web_runtime_settings = resolve_web_runtime_settings(repositories.settings_repo)
    web_runtime_settings = resolve_web_runtime_settings(
        repositories.settings_repo,
        bind_host=bind_host,
        bind_port=bind_port,
        public_base_url=public_base_url,
        session_cookie_secure_mode=session_cookie_secure_mode,
        trust_proxy_headers=trust_proxy_headers,
        forwarded_allow_ips=forwarded_allow_ips,
    )

    runtime = WebRuntimeState(
        config_path=config_path,
        session_secret=session_secret,
        session_minutes=session_minutes,
        login_rate_limiter=LoginRateLimiter(
            max_attempts=security_settings.login_max_attempts,
            window_seconds=security_settings.login_window_seconds,
            lockout_seconds=security_settings.login_lockout_seconds,
        ),
        session_cookie_secure=bool(web_runtime_settings["session_cookie_secure"]),
        web_runtime_settings=web_runtime_settings,
        startup_persisted_web_runtime_settings=startup_persisted_web_runtime_settings,
        sync_runner=WebSyncRunner(
            db_path=db_manager.db_path,
            audit_repo=repositories.audit_repo,
        ),
        integration_outbox_worker=IntegrationOutboxWorker(
            db_path=db_manager.db_path,
        ),
        sspr_session_store=InMemorySSPRSessionStore(),
        sspr_rate_limiter=SSPRRateLimiter(),
    )

    services = build_web_service_state(
        db_manager=db_manager,
        settings_repo=repositories.settings_repo,
        config_release_snapshot_repo=repositories.config_release_snapshot_repo,
        subscription_repo=repositories.integration_webhook_subscription_repo,
        job_repo=repositories.job_repo,
        review_repo=repositories.review_repo,
        planned_operation_repo=repositories.planned_operation_repo,
        conflict_repo=repositories.conflict_repo,
        audit_repo=repositories.audit_repo,
    )

    return WebAppState(repositories=repositories, runtime=runtime, services=services)
