from __future__ import annotations

from dataclasses import dataclass

from sync_app.services.external_integrations import build_approve_plan_use_case
from sync_app.services.rollout_readiness import RolloutReadinessService
from sync_app.storage.local_db import (
    AccountTakeoverRepository,
    ADDirectorySnapshotRepository,
    AttributeMappingRuleRepository,
    ConfigReleaseSnapshotRepository,
    DataQualityReviewRepository,
    DatabaseManager,
    DepartmentOuMappingRepository,
    FieldAuthorityRuleRepository,
    IntegrationWebhookSubscriptionRepository,
    PlannedOperationRepository,
    OrganizationConfigRepository,
    SettingsRepository,
    SourceConnectorRepository,
    IdentityMatchRuleRepository,
    IdentityMatchRunRepository,
    SyncConnectorRepository,
    SyncConflictRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
    SourceDirectoryRepository,
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
    readiness: RolloutReadinessService


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
    source_directory_repo: SourceDirectoryRepository,
    source_connector_repo: SourceConnectorRepository,
    ad_directory_snapshot_repo: ADDirectorySnapshotRepository,
    identity_match_rule_repo: IdentityMatchRuleRepository,
    identity_match_run_repo: IdentityMatchRunRepository,
    field_authority_rule_repo: FieldAuthorityRuleRepository,
    account_takeover_repo: AccountTakeoverRepository,
    attribute_mapping_repo: AttributeMappingRuleRepository,
    department_ou_mapping_repo: DepartmentOuMappingRepository,
    data_quality_review_repo: DataQualityReviewRepository,
) -> WebServiceState:
    approve_plan_use_case = build_approve_plan_use_case(db_manager)
    readiness_service = RolloutReadinessService(
        db_manager=db_manager,
        org_config_repo=org_config_repo,
        settings_repo=settings_repo,
        source_directory_repo=source_directory_repo,
        source_connector_repo=source_connector_repo,
        ad_directory_snapshot_repo=ad_directory_snapshot_repo,
        identity_match_rule_repo=identity_match_rule_repo,
        identity_match_run_repo=identity_match_run_repo,
        field_authority_rule_repo=field_authority_rule_repo,
        account_takeover_repo=account_takeover_repo,
        attribute_mapping_repo=attribute_mapping_repo,
        department_ou_mapping_repo=department_ou_mapping_repo,
        config_release_snapshot_repo=config_release_snapshot_repo,
        data_quality_review_repo=data_quality_review_repo,
        job_repo=job_repo,
        review_repo=review_repo,
        conflict_repo=conflict_repo,
    )
    return WebServiceState(
        jobs=WebJobService(
            approve_plan_use_case=approve_plan_use_case,
            job_repo=job_repo,
            review_repo=review_repo,
            planned_operation_repo=planned_operation_repo,
            conflict_repo=conflict_repo,
            source_directory_repo=source_directory_repo,
            settings_repo=settings_repo,
            ad_directory_snapshot_repo=ad_directory_snapshot_repo,
            identity_match_run_repo=identity_match_run_repo,
            identity_match_rule_repo=identity_match_rule_repo,
            config_release_snapshot_repo=config_release_snapshot_repo,
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
            readiness_service=readiness_service,
        ),
        data_sources=WebDataSourceService(
            org_config_repo=org_config_repo,
            connector_repo=connector_repo,
            audit_repo=audit_repo,
            settings_repo=settings_repo,
            source_connector_repo=source_connector_repo,
            source_directory_repo=source_directory_repo,
            ad_directory_snapshot_repo=ad_directory_snapshot_repo,
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
        readiness=readiness_service,
    )
