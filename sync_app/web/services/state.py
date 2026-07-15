from __future__ import annotations

from dataclasses import dataclass

from sync_app.services.external_integrations import build_approve_plan_use_case
from sync_app.storage.local_db import (
    ConfigReleaseSnapshotRepository,
    DatabaseManager,
    IntegrationWebhookSubscriptionRepository,
    PlannedOperationRepository,
    OrganizationConfigRepository,
    SettingsRepository,
    SyncConnectorRepository,
    SyncConflictRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
    WebAuditLogRepository,
)
from sync_app.web.services.config import WebConfigService
from sync_app.web.services.conflicts import WebConflictService
from sync_app.web.services.data_sources import WebDataSourceService
from sync_app.web.services.integrations import WebIntegrationService
from sync_app.web.services.jobs import WebJobService


@dataclass(slots=True)
class WebServiceState:
    jobs: WebJobService
    conflicts: WebConflictService
    config: WebConfigService
    data_sources: WebDataSourceService
    integrations: WebIntegrationService


def build_web_service_state(
    *,
    db_manager: DatabaseManager,
    settings_repo: SettingsRepository,
    org_config_repo: OrganizationConfigRepository,
    connector_repo: SyncConnectorRepository,
    config_release_snapshot_repo: ConfigReleaseSnapshotRepository,
    subscription_repo: IntegrationWebhookSubscriptionRepository,
    job_repo: SyncJobRepository,
    review_repo: SyncPlanReviewRepository,
    planned_operation_repo: PlannedOperationRepository,
    conflict_repo: SyncConflictRepository,
    audit_repo: WebAuditLogRepository,
) -> WebServiceState:
    approve_plan_use_case = build_approve_plan_use_case(db_manager)
    return WebServiceState(
        jobs=WebJobService(
            approve_plan_use_case=approve_plan_use_case,
            job_repo=job_repo,
            review_repo=review_repo,
            planned_operation_repo=planned_operation_repo,
            conflict_repo=conflict_repo,
        ),
        conflicts=WebConflictService(
            conflict_repo=conflict_repo,
            audit_repo=audit_repo,
        ),
        config=WebConfigService(
            db_manager=db_manager,
            settings_repo=settings_repo,
            config_release_snapshot_repo=config_release_snapshot_repo,
            audit_repo=audit_repo,
        ),
        data_sources=WebDataSourceService(
            org_config_repo=org_config_repo,
            connector_repo=connector_repo,
            audit_repo=audit_repo,
        ),
        integrations=WebIntegrationService(
            db_manager=db_manager,
            approve_plan_use_case=approve_plan_use_case,
            settings_repo=settings_repo,
            subscription_repo=subscription_repo,
            job_repo=job_repo,
            review_repo=review_repo,
            conflict_repo=conflict_repo,
            audit_repo=audit_repo,
        ),
    )
