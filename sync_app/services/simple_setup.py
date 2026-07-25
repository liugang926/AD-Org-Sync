from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from sync_app.services.config_release import publish_current_config_release_snapshot
from sync_app.services.directory_snapshot_ingestion import (
    PlatformAccountIngestionService,
)
from sync_app.services.enterprise_identity_matching import (
    EnterpriseIdentityMatchingService,
    identity_candidate_requires_decision,
)
from sync_app.services.runtime_bootstrap import build_runtime_config_fingerprint
from sync_app.services.sync_policy_center import USERNAME_STRATEGY_BY_SOURCE_FIELD


@dataclass(frozen=True, slots=True)
class SimpleSetupPreparationResult:
    source_snapshot_id: int
    ad_snapshot_id: int
    match_run_id: str
    blocking_candidate_count: int
    policy_release_id: int | None
    policy_release_created: bool

    @property
    def evidence_prepared(self) -> bool:
        return self.blocking_candidate_count == 0 and bool(self.policy_release_id)


def prepare_simple_setup_evidence(
    *,
    repositories: Any,
    organization: Any,
    config: Any,
    source_root_department_ids: Iterable[str],
    actor_username: str,
) -> SimpleSetupPreparationResult:
    """Create the internal evidence implied by the five-step basic setup.

    The administrator still makes every business choice: connector, snapshots,
    primary identity pair, field mappings, source root, and AD root.  This
    helper records the otherwise-technical scope, quality, match-run, field
    authority, and immutable release evidence so Basic mode does not send the
    administrator through hidden advanced policy pages.
    """

    org_id = str(getattr(organization, "org_id", "") or "").strip()
    provider_id = str(getattr(config, "source_provider", "") or "").strip().lower()
    root_ids = sorted(
        {
            str(value or "").strip()
            for value in source_root_department_ids
            if str(value or "").strip()
        }
    )
    if not org_id or not provider_id:
        raise ValueError("organization and source provider are required")
    if not root_ids:
        raise ValueError("one source root department is required")

    source_snapshot = repositories.source_directory_repo.get_latest_successful_snapshot(
        org_id=org_id,
        provider_id=provider_id,
    )
    ad_snapshot = repositories.ad_directory_snapshot_repo.get_latest_successful_snapshot(
        org_id=org_id,
        connector_id="default",
    )
    if not source_snapshot or not ad_snapshot:
        raise ValueError(
            "current source and AD snapshots are required before preparing Dry Run"
        )

    primary_rules = [
        rule
        for rule in repositories.identity_match_rule_repo.list_enabled_rules(
            org_id=org_id
        )
        if str(getattr(rule, "rule_name", "") or "") == "primary_identity"
    ]
    if len(primary_rules) != 1:
        raise ValueError("choose one primary identity field pair first")
    primary_rule = primary_rules[0]
    repositories.identity_match_rule_repo.enable_only_rule(
        org_id=org_id,
        rule_name="primary_identity",
    )

    source_field = str(primary_rule.source_field or "").strip()
    username_strategy = USERNAME_STRATEGY_BY_SOURCE_FIELD.get(
        source_field,
        "custom_template",
    )
    repositories.source_directory_repo.save_scope_selection(
        org_id=org_id,
        provider_id=provider_id,
        connector_id="default",
        scope_type="department",
        selected_department_ids=root_ids,
        username_strategy=username_strategy,
        username_template=(
            "" if username_strategy != "custom_template" else "{" + source_field + "}"
        ),
        source_field=source_field,
        snapshot_id=int(source_snapshot["id"]),
        requested_by=actor_username,
    )
    repositories.field_authority_rule_repo.seed_defaults(
        org_id=org_id,
        created_by=actor_username,
    )
    repositories.data_quality_review_repo.confirm_snapshot(
        org_id=org_id,
        source_snapshot_id=int(source_snapshot["id"]),
        source_snapshot_fingerprint=str(
            source_snapshot["snapshot_fingerprint"] or ""
        ),
        reviewer_username=actor_username,
        review_notes=(
            "Confirmed through Basic setup after the administrator previewed "
            "current fields and selected the primary identity pair."
        ),
    )

    PlatformAccountIngestionService(
        source_directory_repo=repositories.source_directory_repo,
        platform_account_repo=repositories.platform_account_repo,
    ).ingest_snapshot(
        org_id=org_id,
        provider_id=provider_id,
        connector_id="default",
        snapshot_id=int(source_snapshot["id"]),
    )
    config_fingerprint = build_runtime_config_fingerprint(
        config=config,
        organization=organization,
        settings_repo=repositories.settings_repo,
        exclusion_repo=repositories.exclusion_repo,
        exception_rule_repo=repositories.exception_rule_repo,
        mapping_rule_repo=repositories.attribute_mapping_repo,
        department_ou_mapping_repo=repositories.department_ou_mapping_repo,
        connector_repo=repositories.connector_repo,
        field_authority_rule_repo=repositories.field_authority_rule_repo,
        identity_match_rule_repo=repositories.identity_match_rule_repo,
    )
    match_result = EnterpriseIdentityMatchingService(
        platform_account_repo=repositories.platform_account_repo,
        ad_account_repo=repositories.ad_account_repo,
        identity_repo=repositories.enterprise_identity_repo,
        rule_repo=repositories.identity_match_rule_repo,
        run_repo=repositories.identity_match_run_repo,
    ).run(
        org_id=org_id,
        source_snapshot_ids=[int(source_snapshot["id"])],
        ad_snapshot_id=int(ad_snapshot["id"]),
        config_fingerprint=config_fingerprint,
        created_by=actor_username,
    )
    blocking_candidates = [
        candidate
        for candidate in list(match_result.get("candidates") or [])
        if identity_candidate_requires_decision(candidate)
    ]
    release_id: int | None = None
    release_created = False
    if not blocking_candidates:
        release_result = publish_current_config_release_snapshot(
            repositories.db_manager,
            org_id,
            created_by=actor_username,
            snapshot_name="Basic setup",
            trigger_action="simple_setup_auto_release",
            source_snapshot_id=int(source_snapshot["id"]),
        )
        release = release_result.get("snapshot")
        release_id = int(getattr(release, "id", 0) or 0) or None
        release_created = bool(release_result.get("created"))

    return SimpleSetupPreparationResult(
        source_snapshot_id=int(source_snapshot["id"]),
        ad_snapshot_id=int(ad_snapshot["id"]),
        match_run_id=str(match_result.get("run_id") or ""),
        blocking_candidate_count=len(blocking_candidates),
        policy_release_id=release_id,
        policy_release_created=release_created,
    )


__all__ = [
    "SimpleSetupPreparationResult",
    "prepare_simple_setup_evidence",
]
