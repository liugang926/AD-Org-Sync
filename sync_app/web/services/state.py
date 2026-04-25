from __future__ import annotations

from dataclasses import dataclass

from sync_app.storage.local_db import (
    ConfigReleaseSnapshotRepository,
    DatabaseManager,
    IntegrationWebhookSubscriptionRepository,
    PlannedOperationRepository,
    SettingsRepository,
    SyncConflictRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
    WebAuditLogRepository,
)
from sync_app.web.services.config import WebConfigService
from sync_app.web.services.conflicts import WebConflictService
from sync_app.web.services.integrations import WebIntegrationService
from sync_app.web.services.jobs import WebJobService


@dataclass(slots=True)
class WebServiceState:
    jobs: WebJobService
    conflicts: WebConflictService
    config: WebConfigService
    integrations: WebIntegrationService


def build_web_service_state(
    *,
    db_manager: DatabaseManager,
    settings_repo: SettingsRepository,
    config_release_snapshot_repo: ConfigReleaseSnapshotRepository,
    subscription_repo: IntegrationWebhookSubscriptionRepository,
    job_repo: SyncJobRepository,
    review_repo: SyncPlanReviewRepository,
    planned_operation_repo: PlannedOperationRepository,
    conflict_repo: SyncConflictRepository,
    audit_repo: WebAuditLogRepository,
) -> WebServiceState:
    return WebServiceState(
        jobs=WebJobService(
            db_manager=db_manager,
            job_repo=job_repo,
            review_repo=review_repo,
            planned_operation_repo=planned_operation_repo,
            conflict_repo=conflict_repo,
            audit_repo=audit_repo,
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
        integrations=WebIntegrationService(
            db_manager=db_manager,
            settings_repo=settings_repo,
            subscription_repo=subscription_repo,
            job_repo=job_repo,
            review_repo=review_repo,
            conflict_repo=conflict_repo,
            audit_repo=audit_repo,
        ),
    )
