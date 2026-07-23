from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sync_app.storage.local_db import (
    ADDirectorySnapshotRepository,
    ADTargetAttributeRegistryRepository,
    AttributeMappingRuleRepository,
    DataQualityReviewRepository,
    DatabaseManager,
    DepartmentOuMappingRepository,
    FieldAuthorityRuleRepository,
    IdentityMatchRuleRepository,
    IdentityMatchRunRepository,
    OrganizationConfigRepository,
    OrganizationRepository,
    SettingsRepository,
    SourceConnectorRepository,
)
from sync_app.core.fingerprints import fingerprint_json
from sync_app.services.config_release import publish_current_config_release_snapshot
from sync_app.storage.repositories.conflicts import SyncPlanReviewRepository
from sync_app.storage.repositories.jobs import SyncJobRepository
from sync_app.storage.repositories.source_directory import SourceDirectoryRepository
from sync_app.services.runtime_bootstrap import resolve_runtime_config_fingerprint


def create_eligible_execution_plan(
    db_manager: DatabaseManager,
    *,
    job_id: str,
    org_id: str = "default",
    provider_id: str = "test-provider",
    environment_label: str = "Local environment",
    planned_operation_count: int = 1,
    high_risk_operation_count: int = 1,
    approved: bool = False,
) -> dict[str, Any]:
    """Create test-only snapshot, scope, Dry Run, review, and optional approval."""

    organization_repo = OrganizationRepository(db_manager)
    organization = organization_repo.get_organization_record(org_id)
    if organization is None:
        organization_repo.upsert_organization(
            org_id=org_id,
            name=(
                "Default Organization"
                if org_id == "default"
                else f"Test Organization {org_id}"
            ),
            config_path=(
                "config.ini"
                if org_id == "default"
                else f"config.{org_id}.ini"
            ),
        )
    config_repo = OrganizationConfigRepository(db_manager)
    if not config_repo.has_config(org_id):
        config_repo.save_config(
            org_id,
            {
                "source_provider": provider_id,
                "corpid": f"test-corp-{org_id}",
                "agentid": "10001",
                "corpsecret": "test-source-secret",
                "webhook_url": "",
                "ldap_server": "dc.test.example",
                "ldap_domain": "test.example",
                "ldap_username": "svc-test",
                "ldap_password": "test-directory-secret",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": False,
                "ldap_ca_cert_path": "",
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
            },
            config_path=(
                "config.ini"
                if org_id == "default"
                else f"config.{org_id}.ini"
            ),
        )
    effective_provider_id = (
        config_repo.get_app_config(
            org_id,
            config_path=(
                "config.ini"
                if org_id == "default"
                else f"config.{org_id}.ini"
            ),
        ).source_provider
        or provider_id
    )
    source_repo = SourceDirectoryRepository(db_manager)
    source_connector_repo = SourceConnectorRepository(db_manager)
    source_connector_id = f"{effective_provider_id}-default"
    source_connector_repo.upsert_connector(
        org_id=org_id,
        connector_id=source_connector_id,
        provider_id=effective_provider_id,
        name=f"{effective_provider_id} test connector",
        corpid=f"test-corp-{org_id}",
        agentid="10001",
        corpsecret="test-source-secret",
        granted_permissions=(),
        required_permissions=(),
    )
    source_connector_repo.update_connection_status(
        org_id=org_id,
        connector_id=source_connector_id,
        connection_status="connected",
    )
    settings_repo = SettingsRepository(db_manager)
    settings_repo.set_value(
        "ad_connection_status", "connected", "string", org_id=org_id
    )
    settings_repo.set_value(
        "ad_connection_tested_at",
        datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "string",
        org_id=org_id,
    )
    job_repo = SyncJobRepository(db_manager)
    review_repo = SyncPlanReviewRepository(db_manager)
    snapshot_fingerprint = f"sha256:v2:snapshot:{job_id}"
    plan_fingerprint = f"sha256:v2:plan:{job_id}"
    snapshot_id = source_repo.start_refresh(
        org_id=org_id,
        provider_id=effective_provider_id,
        created_by="test",
    )
    source_repo.replace_snapshot(
        snapshot_id,
        departments=[],
        users=[],
        fields=[
            {
                "name": "name",
                "canonical_field_key": "display_name",
                "data_type": "string",
                "coverage": 0,
            }
        ],
        fingerprint=snapshot_fingerprint,
        ttl_minutes=240,
    )
    DataQualityReviewRepository(db_manager).confirm_snapshot(
        org_id=org_id,
        source_snapshot_id=snapshot_id,
        source_snapshot_fingerprint=snapshot_fingerprint,
        reviewer_username="test-reviewer",
        review_notes="Test fixture reviewed this exact source snapshot.",
    )

    ad_snapshot_repo = ADDirectorySnapshotRepository(db_manager)
    ad_snapshot_id = ad_snapshot_repo.start_snapshot(
        org_id=org_id,
        connector_id="default",
        created_by="test",
    )
    ad_snapshot_fingerprint = f"sha256:v2:ad-snapshot:{job_id}"
    ad_snapshot_repo.complete_snapshot(
        ad_snapshot_id,
        org_id=org_id,
        user_count=0,
        ou_count=1,
        duplicate_employee_id_count=0,
        duplicate_employee_number_count=0,
        snapshot_fingerprint=ad_snapshot_fingerprint,
        expires_at=(datetime.now(timezone.utc) + timedelta(hours=4)).isoformat(
            timespec="seconds"
        ),
    )
    ADTargetAttributeRegistryRepository(db_manager).sync_snapshot_catalog(
        org_id=org_id,
        ad_connector_id="default",
        snapshot_id=ad_snapshot_id,
        capability_report={
            "schema_attributes": ["displayName"],
            "capabilities": {
                "update_user": {"status": "success", "verified": True}
            },
        },
    )

    match_rule_repo = IdentityMatchRuleRepository(db_manager)
    match_rule_repo.seed_defaults(org_id=org_id, created_by="test")
    match_rules = match_rule_repo.list_enabled_rules(org_id=org_id)
    rules_fingerprint = fingerprint_json(
        [rule.to_dict() for rule in match_rules],
        namespace="identity-match-rules",
    )
    match_run_repo = IdentityMatchRunRepository(db_manager)
    match_run_id = match_run_repo.create_run(
        org_id=org_id,
        source_snapshot_ids=[snapshot_id],
        ad_snapshot_id=ad_snapshot_id,
        rules_fingerprint=rules_fingerprint,
        created_by="test",
        run_id=f"match-{job_id}",
    )
    match_run_repo.replace_candidates(
        match_run_id,
        org_id=org_id,
        candidates=[],
        summary={"total": 0, "blocked": 0},
    )

    FieldAuthorityRuleRepository(db_manager).seed_defaults(
        org_id=org_id,
        created_by="test",
    )
    AttributeMappingRuleRepository(db_manager).upsert_rule(
        org_id=org_id,
        direction="source_to_ad",
        source_field="name",
        target_field="displayName",
        is_enabled=True,
        notes="Test fixture mapping",
    )
    DepartmentOuMappingRepository(db_manager).upsert_mapping(
        org_id=org_id,
        source_department_id="root",
        source_department_name="Test root",
        target_ou_path="Managed Users/Test",
        is_enabled=True,
    )
    for key, value, value_type in (
        ("default_directory_root_ou_path", "Managed Users/Test", "string"),
        ("default_disabled_users_ou_path", "Managed Users/Disabled", "string"),
        ("offboarding_lifecycle_enabled", True, "bool"),
        ("disable_circuit_breaker_enabled", True, "bool"),
        ("attribute_mapping_enabled", True, "bool"),
    ):
        settings_repo.set_value(key, value, value_type, org_id=org_id)
    selection = source_repo.save_scope_selection(
        org_id=org_id,
        provider_id=effective_provider_id,
        scope_type="full",
        snapshot_id=snapshot_id,
        requested_by="test",
    )
    release_result = publish_current_config_release_snapshot(
        db_manager,
        org_id,
        created_by="test",
        snapshot_name=f"Test release for {job_id}",
        trigger_action="unit_test",
        force=True,
    )
    release = release_result["snapshot"]
    if release is None:
        raise RuntimeError("test policy release was not created")
    config_fingerprint = resolve_runtime_config_fingerprint(
        db_manager=db_manager,
        org_id=org_id,
    )
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    job_repo.create_job(
        job_id,
        trigger_type="unit_test",
        execution_mode="dry_run",
        status="COMPLETED",
        org_id=org_id,
        config_snapshot_hash=config_fingerprint,
        started_at=generated_at,
    )
    summary = {
        "planned_operation_count": planned_operation_count,
        "high_risk_operation_count": high_risk_operation_count,
        "conflict_count": 0,
        "error_count": 0,
        "review_required": True,
        "plan_fingerprint": plan_fingerprint,
        "environment_label": environment_label,
        "plan_generated_at": generated_at,
        "source_snapshot_id": snapshot_id,
        "source_snapshot_fingerprint": snapshot_fingerprint,
        "selection_fingerprint": selection["selection_fingerprint"],
    }
    job_repo.update_job(
        job_id,
        planned_operation_count=planned_operation_count,
        summary=summary,
        ended=True,
    )
    source_repo.bind_job_scope(
        job_id=job_id,
        execution_mode="dry_run",
        config_fingerprint=config_fingerprint,
        selection=selection,
        requested_by="test",
        ad_snapshot_id=ad_snapshot_id,
        ad_snapshot_fingerprint=ad_snapshot_fingerprint,
        identity_match_run_id=match_run_id,
        identity_match_rules_fingerprint=rules_fingerprint,
        policy_release_id=release.id,
        policy_release_hash=release.bundle_hash,
    )
    review_repo.upsert_review_request(
        job_id=job_id,
        plan_fingerprint=plan_fingerprint,
        config_snapshot_hash=config_fingerprint,
        high_risk_operation_count=high_risk_operation_count,
    )
    if approved:
        review_repo.approve_review(
            job_id,
            reviewer_username="test-reviewer",
            expires_at=(
                datetime.now(timezone.utc) + timedelta(hours=2)
            ).isoformat(timespec="seconds"),
        )
    return {
        "job": job_repo.get_job_record(job_id),
        "review": review_repo.get_review_record_by_job_id(job_id),
        "selection": selection,
        "snapshot_id": snapshot_id,
        "ad_snapshot_id": ad_snapshot_id,
        "match_run_id": match_run_id,
        "policy_release_id": release.id,
        "summary": summary,
    }


__all__ = ["create_eligible_execution_plan"]
