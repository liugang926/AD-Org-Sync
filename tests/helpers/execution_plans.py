from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sync_app.storage.local_db import (
    DatabaseManager,
    OrganizationConfigRepository,
    OrganizationRepository,
)
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
    source_repo = SourceDirectoryRepository(db_manager)
    job_repo = SyncJobRepository(db_manager)
    review_repo = SyncPlanReviewRepository(db_manager)
    snapshot_fingerprint = f"sha256:v2:snapshot:{job_id}"
    config_fingerprint = resolve_runtime_config_fingerprint(
        db_manager=db_manager,
        org_id=org_id,
    )
    plan_fingerprint = f"sha256:v2:plan:{job_id}"
    snapshot_id = source_repo.start_refresh(
        org_id=org_id,
        provider_id=provider_id,
        created_by="test",
    )
    source_repo.replace_snapshot(
        snapshot_id,
        departments=[],
        users=[],
        fields=[],
        fingerprint=snapshot_fingerprint,
        ttl_minutes=240,
    )
    selection = source_repo.save_scope_selection(
        org_id=org_id,
        provider_id=provider_id,
        scope_type="full",
        snapshot_id=snapshot_id,
        requested_by="test",
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
        "summary": summary,
    }


__all__ = ["create_eligible_execution_plan"]
