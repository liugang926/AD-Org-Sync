from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from sync_app.core.fingerprints import fingerprint_json
from sync_app.services.config_release import build_config_release_center_data
from sync_app.services.runtime_bootstrap import resolve_runtime_config_fingerprint


READINESS_STATUSES = {
    "not_started",
    "ready",
    "action_required",
    "blocked",
    "stale",
    "complete",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return getattr(row, key, default)


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    try:
        decoded = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return decoded if isinstance(decoded, list) else []


def _is_expired(value: Any) -> bool:
    normalized = _text(value)
    if not normalized:
        return False
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return True
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed <= datetime.now(timezone.utc)


@dataclass(slots=True)
class RolloutReadinessStep:
    key: str
    status: str
    title: str
    summary: str
    blocker_reason: str = ""
    action_url: str = ""
    action_label: str = ""
    last_updated_at: str = ""
    related_snapshot_id: Optional[int] = None
    release_id: Optional[int] = None
    job_id: str = ""
    related_match_run_id: str = ""
    whether_required: bool = True
    phase: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in READINESS_STATUSES:
            raise ValueError(f"unsupported rollout readiness status: {self.status}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RolloutReadinessResult:
    org_id: str
    org_name: str
    generated_at: str
    steps: list[RolloutReadinessStep]

    @property
    def step_map(self) -> dict[str, RolloutReadinessStep]:
        return {step.key: step for step in self.steps}

    @property
    def completed_count(self) -> int:
        return sum(step.status == "complete" for step in self.steps if step.whether_required)

    @property
    def required_count(self) -> int:
        return sum(step.whether_required for step in self.steps)

    @property
    def blocker_count(self) -> int:
        return sum(step.status in {"blocked", "action_required", "stale"} for step in self.steps if step.whether_required)

    @property
    def next_step(self) -> RolloutReadinessStep:
        for step in self.steps:
            if step.whether_required and step.status in {
                "ready",
                "action_required",
                "stale",
            }:
                return step
        for step in self.steps:
            if step.whether_required and step.status == "blocked":
                return step
        return self.steps[-1]

    def to_dict(self) -> dict[str, Any]:
        next_step = self.next_step
        current_phase = next_step.phase or (self.steps[-1].phase if self.steps else "")
        return {
            "org_id": self.org_id,
            "org_name": self.org_name,
            "generated_at": self.generated_at,
            "steps": [step.to_dict() for step in self.steps],
            "step_map": {key: step.to_dict() for key, step in self.step_map.items()},
            "completed_count": self.completed_count,
            "required_count": self.required_count,
            "completion_percent": (
                round(self.completed_count * 100 / self.required_count)
                if self.required_count
                else 100
            ),
            "blocker_count": self.blocker_count,
            "current_phase": current_phase,
            "next_step": next_step.to_dict(),
            "apply_allowed": self.step_map["apply_allowed"].status in {"ready", "complete"},
        }


@dataclass(slots=True)
class RolloutReadinessService:
    db_manager: Any
    org_config_repo: Any
    settings_repo: Any
    source_directory_repo: Any
    source_connector_repo: Any
    ad_directory_snapshot_repo: Any
    identity_match_rule_repo: Any
    identity_match_run_repo: Any
    field_authority_rule_repo: Any
    account_takeover_repo: Any
    attribute_mapping_repo: Any
    department_ou_mapping_repo: Any
    config_release_snapshot_repo: Any
    data_quality_review_repo: Any
    job_repo: Any
    review_repo: Any
    conflict_repo: Any

    def evaluate_organization(
        self,
        *,
        organization: Any,
        config_path: str,
        source_connection_status_override: str = "",
        ad_connection_status_override: str = "",
    ) -> RolloutReadinessResult:
        resolved_config_path = _text(getattr(organization, "config_path", "")) or config_path
        config = self.org_config_repo.get_app_config(
            organization.org_id,
            config_path=resolved_config_path,
        )
        try:
            config_fingerprint = resolve_runtime_config_fingerprint(
                db_manager=self.db_manager,
                org_id=organization.org_id,
                config_path=resolved_config_path,
            )
        except Exception:
            # A brand-new organization must still render the rollout guide and
            # explain which connector settings are missing. An unavailable
            # runtime fingerprint keeps all evidence-dependent gates closed.
            config_fingerprint = ""
        return self.evaluate(
            org_id=organization.org_id,
            org_name=organization.name,
            source_provider=config.source_provider,
            config_fingerprint=config_fingerprint,
            source_connector_configured=bool(
                config.source_connector.corpid and config.source_connector.corpsecret
            ),
            ad_connector_configured=bool(
                config.ldap.server
                and config.ldap.domain
                and config.ldap.username
                and config.ldap.password
            ),
            source_connection_status_override=source_connection_status_override,
            ad_connection_status_override=ad_connection_status_override,
        )

    def _step(
        self,
        key: str,
        status: str,
        title: str,
        summary: str,
        *,
        blocker_reason: str = "",
        action_url: str,
        action_label: str,
        phase: str,
        last_updated_at: str = "",
        related_snapshot_id: Optional[int] = None,
        release_id: Optional[int] = None,
        job_id: str = "",
        related_match_run_id: str = "",
        whether_required: bool = True,
        metadata: Optional[dict[str, Any]] = None,
    ) -> RolloutReadinessStep:
        return RolloutReadinessStep(
            key=key,
            status=status,
            title=title,
            summary=summary,
            blocker_reason=blocker_reason,
            action_url=action_url,
            action_label=action_label,
            phase=phase,
            last_updated_at=last_updated_at,
            related_snapshot_id=related_snapshot_id,
            release_id=release_id,
            job_id=job_id,
            related_match_run_id=related_match_run_id,
            whether_required=whether_required,
            metadata=dict(metadata or {}),
        )

    def evaluate(
        self,
        *,
        org_id: str,
        org_name: str,
        source_provider: str,
        config_fingerprint: str,
        source_connector_configured: bool,
        ad_connector_configured: bool,
        source_connector_id: str = "",
        source_connection_status_override: str = "",
        ad_connection_status_override: str = "",
    ) -> RolloutReadinessResult:
        normalized_org_id = _text(org_id).lower() or "default"
        normalized_provider = _text(source_provider).lower() or "wecom"
        connector_id = _text(source_connector_id) or f"{normalized_provider}-default"
        source_connector = self.source_connector_repo.get_connector(
            connector_id,
            org_id=normalized_org_id,
        )
        source_connection_status = (
            _text(source_connection_status_override).lower()
            or _text(getattr(source_connector, "connection_status", "")).lower()
        )
        ad_connection_status = (
            _text(ad_connection_status_override).lower()
            or _text(
                self.settings_repo.get_value(
                    "ad_connection_status", "not_tested", org_id=normalized_org_id
                )
            ).lower()
        )

        source_connector_complete = bool(
            source_connector_configured and source_connection_status == "connected"
        )
        ad_connector_complete = bool(
            ad_connector_configured and ad_connection_status == "connected"
        )
        source_snapshot = self.source_directory_repo.get_latest_successful_snapshot(
            org_id=normalized_org_id,
            provider_id=normalized_provider,
        )
        ad_snapshot = self.ad_directory_snapshot_repo.get_latest_successful_snapshot(
            org_id=normalized_org_id,
            connector_id="default",
        )
        source_snapshot_id = int(_row_value(source_snapshot, "id", 0) or 0)
        source_snapshot_fingerprint = _text(
            _row_value(source_snapshot, "snapshot_fingerprint", "")
        )
        ad_snapshot_id = int(_row_value(ad_snapshot, "id", 0) or 0)
        ad_snapshot_fingerprint = _text(
            _row_value(ad_snapshot, "snapshot_fingerprint", "")
        )
        source_snapshot_expired = bool(
            source_snapshot and _is_expired(_row_value(source_snapshot, "expires_at", ""))
        )
        ad_snapshot_expired = bool(
            ad_snapshot and _is_expired(_row_value(ad_snapshot, "expires_at", ""))
        )

        review = (
            self.data_quality_review_repo.get_review_for_snapshot(
                org_id=normalized_org_id,
                source_snapshot_id=source_snapshot_id,
            )
            if source_snapshot_id
            else None
        )
        latest_quality_review = self.data_quality_review_repo.get_latest_review(
            org_id=normalized_org_id
        )
        quality_current = bool(
            review
            and review.status == "confirmed"
            and review.source_snapshot_fingerprint == source_snapshot_fingerprint
        )
        rules = self.identity_match_rule_repo.list_enabled_rules(
            org_id=normalized_org_id
        )
        rules_fingerprint = fingerprint_json(
            [rule.to_dict() for rule in rules],
            namespace="identity-match-rules",
        )
        match_run = self.identity_match_run_repo.get_latest_completed_run(
            org_id=normalized_org_id
        )
        match_source_ids = {
            int(value)
            for value in _json_list(_row_value(match_run, "source_snapshot_ids_json", "[]"))
            if str(value).isdigit()
        }
        match_run_current = bool(
            match_run
            and source_snapshot_id
            and not source_snapshot_expired
            and source_snapshot_id in match_source_ids
            and ad_snapshot_id
            and not ad_snapshot_expired
            and int(_row_value(match_run, "ad_snapshot_id", 0) or 0)
            == ad_snapshot_id
            and _text(_row_value(match_run, "rules_fingerprint", ""))
            == rules_fingerprint
        )
        match_run_id = _text(_row_value(match_run, "run_id", ""))
        match_candidates = (
            self.identity_match_run_repo.list_candidates(
                org_id=normalized_org_id,
                run_id=match_run_id,
                limit=5000,
            )
            if match_run_id
            else []
        )
        unresolved_match_candidates = [
            candidate
            for candidate in match_candidates
            if _text(getattr(candidate, "status", "")).lower() == "pending"
            and _text(getattr(candidate, "result_level", "")).lower()
            in {"blocked", "manual_confirmation", "suggested_link"}
        ]

        takeover_batches = self.account_takeover_repo.list_batches(
            org_id=normalized_org_id,
            limit=100,
        )
        unresolved_takeovers = [
            batch
            for batch in takeover_batches
            if _text(batch.get("status")).lower()
            in {"validating", "ready", "approved", "failed"}
        ]

        scope = self.source_directory_repo.get_scope_selection(
            org_id=normalized_org_id,
            provider_id=normalized_provider,
        )
        scope_current = bool(
            scope
            and source_snapshot_id
            and not source_snapshot_expired
            and int(scope.get("snapshot_id") or 0) == source_snapshot_id
            and _text(scope.get("source_snapshot_fingerprint"))
            == source_snapshot_fingerprint
        )
        naming_configured = bool(
            scope
            and _text(scope.get("username_strategy"))
            and (
                _text(scope.get("username_strategy")) != "custom_template"
                or _text(scope.get("username_template"))
            )
        )
        field_authority_rules = self.field_authority_rule_repo.list_enabled_rules(
            org_id=normalized_org_id
        )
        attribute_mappings = [
            item
            for item in self.attribute_mapping_repo.list_rule_records(
                org_id=normalized_org_id
            )
            if bool(getattr(item, "is_enabled", False))
        ]
        department_mappings = [
            item
            for item in self.department_ou_mapping_repo.list_mapping_records(
                org_id=normalized_org_id
            )
            if bool(getattr(item, "is_enabled", False))
        ]
        default_ou = _text(
            self.settings_repo.get_value(
                "default_directory_root_ou_path", "", org_id=normalized_org_id
            )
        )
        disabled_ou = _text(
            self.settings_repo.get_value(
                "default_disabled_users_ou_path", "", org_id=normalized_org_id
            )
        )
        lifecycle_safety_configured = bool(
            self.settings_repo.get_bool(
                "offboarding_lifecycle_enabled", False, org_id=normalized_org_id
            )
            and self.settings_repo.get_bool(
                "disable_circuit_breaker_enabled", False, org_id=normalized_org_id
            )
            and disabled_ou
        )

        release_data = build_config_release_center_data(
            self.db_manager,
            normalized_org_id,
        )
        release = release_data.get("latest_snapshot")
        release_current = bool(
            release
            and not release_data.get("has_unpublished_changes")
            and source_snapshot_id
            and int(getattr(release, "source_snapshot_id", 0) or 0)
            == source_snapshot_id
        )
        release_id = int(getattr(release, "id", 0) or 0)
        release_hash = _text(getattr(release, "bundle_hash", ""))

        successful_statuses = {"success", "completed"}
        recent_jobs = self.job_repo.list_recent_job_records(
            limit=200,
            org_id=normalized_org_id,
        )
        successful_dry_runs = [
            job
            for job in recent_jobs
            if _text(job.execution_mode).lower() == "dry_run"
            and _text(job.status).lower() in successful_statuses
        ]
        dry_run_job = successful_dry_runs[0] if successful_dry_runs else None
        dry_run_scope = (
            self.source_directory_repo.get_job_scope(
                dry_run_job.job_id,
                org_id=normalized_org_id,
            )
            if dry_run_job
            else None
        )
        dry_run_current = bool(
            dry_run_scope
            and source_connector_complete
            and ad_connector_complete
            and not source_snapshot_expired
            and not ad_snapshot_expired
            and quality_current
            and match_run_current
            and not unresolved_match_candidates
            and not unresolved_takeovers
            and naming_configured
            and bool(field_authority_rules)
            and bool(attribute_mappings)
            and bool(department_mappings or default_ou)
            and bool(disabled_ou)
            and lifecycle_safety_configured
            and scope_current
            and release_current
            and source_snapshot_id
            and int(dry_run_scope.get("snapshot_id") or 0) == source_snapshot_id
            and _text(dry_run_scope.get("source_snapshot_fingerprint"))
            == source_snapshot_fingerprint
            and ad_snapshot_id
            and int(dry_run_scope.get("ad_snapshot_id") or 0) == ad_snapshot_id
            and _text(dry_run_scope.get("ad_snapshot_fingerprint"))
            == ad_snapshot_fingerprint
            and match_run_id
            and _text(dry_run_scope.get("identity_match_run_id")) == match_run_id
            and _text(dry_run_scope.get("identity_match_rules_fingerprint"))
            == rules_fingerprint
            and release_id
            and int(dry_run_scope.get("policy_release_id") or 0) == release_id
            and _text(dry_run_scope.get("policy_release_hash")) == release_hash
            and _text(dry_run_scope.get("config_fingerprint"))
            == _text(config_fingerprint)
        )
        dry_run_job_id = _text(getattr(dry_run_job, "job_id", ""))
        open_plan_conflicts = (
            self.conflict_repo.list_conflict_records(
                job_id=dry_run_job_id,
                status="open",
                org_id=normalized_org_id,
            )
            if dry_run_job_id
            else []
        )
        plan_conflicts_resolved = bool(dry_run_current and not open_plan_conflicts)
        plan_review = (
            self.review_repo.get_review_record_by_job_id(dry_run_job_id)
            if dry_run_job_id
            else None
        )
        approval_current = bool(
            dry_run_current
            and plan_review
            and _text(getattr(plan_review, "status", "")).lower() == "approved"
            and not _is_expired(getattr(plan_review, "expires_at", ""))
            and _text(getattr(plan_review, "config_snapshot_hash", ""))
            == _text(getattr(dry_run_job, "config_snapshot_hash", ""))
        )
        current_apply = next(
            (
                job
                for job in recent_jobs
                if _text(job.execution_mode).lower() == "apply"
                and _text(job.status).lower() in successful_statuses
                and _text(job.plan_source_job_id) == dry_run_job_id
            ),
            None,
        )

        steps: list[RolloutReadinessStep] = []
        steps.append(self._step(
            "organization_scope_configured", "complete", "Organization and business scope",
            "The active organization is selected for this rollout.",
            action_url="/system-management/organizations", action_label="Review organization",
            phase="Organization scope", last_updated_at="",
        ))
        if not source_connector_configured:
            source_connector_status = "action_required"
            source_connector_reason = "Configure the source platform connection before reading identities."
        elif source_connection_status == "failed":
            source_connector_status = "blocked"
            source_connector_reason = "The latest source connection test failed."
        elif source_connection_status != "connected":
            source_connector_status = "action_required"
            source_connector_reason = "Run a successful source connection test."
        else:
            source_connector_status = "complete"
            source_connector_reason = ""
        steps.append(self._step(
            "source_connector_ready", source_connector_status, "Source platform connector",
            "Configure and verify the organization platform connector.",
            blocker_reason=source_connector_reason, action_url="/data-sources/connectors",
            action_label="Configure source connector", phase="Connectors",
            last_updated_at=_text(getattr(source_connector, "last_tested_at", "")),
        ))
        if not ad_connector_configured:
            ad_connector_status = "action_required"
            ad_connector_reason = "Configure the target AD domain controller."
        elif ad_connection_status == "failed":
            ad_connector_status = "blocked"
            ad_connector_reason = "The latest AD connection test failed."
        elif ad_connection_status != "connected":
            ad_connector_status = "action_required"
            ad_connector_reason = "Run a successful AD connection test."
        else:
            ad_connector_status = "complete"
            ad_connector_reason = ""
        steps.append(self._step(
            "ad_connector_ready", ad_connector_status, "Target AD connector",
            "Configure and verify the AD domain connection.", blocker_reason=ad_connector_reason,
            action_url="/data-sources/connectors", action_label="Configure AD connector",
            phase="Connectors",
            last_updated_at=_text(self.settings_repo.get_value("ad_connection_tested_at", "", org_id=normalized_org_id)),
        ))

        def snapshot_status(
            snapshot: Any,
            connector_complete: bool,
            snapshot_expired: bool,
        ) -> tuple[str, str]:
            if snapshot and snapshot_expired:
                return "stale", "The retained snapshot has expired and must be refreshed."
            if snapshot and connector_complete:
                return "complete", ""
            if snapshot:
                return "stale", "The retained snapshot cannot be treated as current until the connector is healthy."
            if connector_complete:
                return "ready", "Refresh the immutable directory snapshot."
            return "blocked", "Complete and verify the connector first."

        source_status, source_reason = snapshot_status(
            source_snapshot,
            source_connector_complete,
            source_snapshot_expired,
        )
        steps.append(self._step(
            "source_snapshot_current", source_status, "Source identity snapshot",
            "Use the latest immutable source directory snapshot.", blocker_reason=source_reason,
            action_url="/data-sources/source-directory", action_label="Refresh source snapshot",
            phase="Snapshots and quality", last_updated_at=_text(_row_value(source_snapshot, "completed_at", "")),
            related_snapshot_id=source_snapshot_id or None,
            metadata={
                "snapshot_fingerprint": source_snapshot_fingerprint,
                "user_count": int(_row_value(source_snapshot, "user_count", 0) or 0),
                "department_count": int(_row_value(source_snapshot, "department_count", 0) or 0),
                "expires_at": _text(_row_value(source_snapshot, "expires_at", "")),
            },
        ))
        ad_status, ad_reason = snapshot_status(
            ad_snapshot,
            ad_connector_complete,
            ad_snapshot_expired,
        )
        steps.append(self._step(
            "ad_snapshot_current", ad_status, "AD identity snapshot",
            "Use the latest immutable AD account and OU snapshot.", blocker_reason=ad_reason,
            action_url="/identity-governance/identity-matching", action_label="Refresh AD snapshot",
            phase="Snapshots and quality", last_updated_at=_text(_row_value(ad_snapshot, "completed_at", "")),
            related_snapshot_id=ad_snapshot_id or None,
            metadata={
                "snapshot_fingerprint": ad_snapshot_fingerprint,
                "user_count": int(_row_value(ad_snapshot, "user_count", 0) or 0),
                "ou_count": int(_row_value(ad_snapshot, "ou_count", 0) or 0),
                "expires_at": _text(_row_value(ad_snapshot, "expires_at", "")),
            },
        ))
        if quality_current:
            quality_status, quality_reason = "complete", ""
        elif latest_quality_review and source_snapshot:
            quality_status, quality_reason = "stale", "Data quality was reviewed for an older source snapshot."
        elif source_status == "complete":
            quality_status, quality_reason = "action_required", "Confirm the data quality review for this source snapshot."
        else:
            quality_status, quality_reason = "blocked", "A current source snapshot is required before review."
        steps.append(self._step(
            "data_quality_reviewed", quality_status, "Data quality review",
            "Review identity, naming and routing risks for the current source snapshot.",
            blocker_reason=quality_reason, action_url="/data-sources/data-quality",
            action_label="Review data quality", phase="Snapshots and quality",
            last_updated_at=_text(getattr(review, "reviewed_at", "")),
            related_snapshot_id=source_snapshot_id or None,
        ))

        rules_status = "complete" if rules else "action_required"
        steps.append(self._step(
            "identity_rules_configured", rules_status, "Enterprise identity match rules",
            "Configure deterministic identity matching rules and confidence levels.",
            blocker_reason="" if rules else "No enabled identity matching rule is configured.",
            action_url="/identity-governance/match-rules", action_label="Configure match rules",
            phase="Identity matching", last_updated_at=max((_text(rule.updated_at) for rule in rules), default=""),
            metadata={"rules_fingerprint": rules_fingerprint},
        ))
        if match_run_current:
            match_status, match_reason = "complete", ""
        elif match_run:
            match_status, match_reason = "stale", "The latest match run does not use the current source snapshot, AD snapshot or rule version."
        elif source_status == "complete" and ad_status == "complete" and rules:
            match_status, match_reason = "ready", "Run enterprise identity matching."
        else:
            match_status, match_reason = "blocked", "Current source and AD snapshots plus match rules are required."
        steps.append(self._step(
            "identity_match_run_current", match_status, "Current identity match run",
            "Run matching against the current source and AD snapshots.", blocker_reason=match_reason,
            action_url="/identity-governance/identity-matching", action_label="Run identity matching",
            phase="Identity matching", last_updated_at=_text(_row_value(match_run, "completed_at", "")),
            related_match_run_id=match_run_id,
            metadata={
                "rules_fingerprint": rules_fingerprint,
                "source_snapshot_id": source_snapshot_id or None,
                "ad_snapshot_id": ad_snapshot_id or None,
            },
        ))
        blockers_status = (
            "complete" if match_run_current and not unresolved_match_candidates
            else "action_required" if match_run_current
            else "blocked"
        )
        steps.append(self._step(
            "identity_blockers_resolved", blockers_status, "Identity candidates and conflicts",
            "Resolve ambiguous, suggested and blocked identity candidates.",
            blocker_reason=(
                f"{len(unresolved_match_candidates)} identity candidate(s) still require a decision."
                if unresolved_match_candidates else "A current match run is required."
                if not match_run_current else ""
            ), action_url="/identity-governance/identity-matching",
            action_label="Process identity candidates", phase="Identity matching",
            related_match_run_id=match_run_id,
        ))
        takeover_required = bool(unresolved_takeovers)
        steps.append(self._step(
            "account_takeover_resolved",
            "action_required" if unresolved_takeovers else "complete",
            "Existing AD account takeover",
            "Validate and approve explicit links to existing AD accounts without silent overwrite.",
            blocker_reason=(
                f"{len(unresolved_takeovers)} takeover batch(es) still require action."
                if unresolved_takeovers else ""
            ), action_url="/identity-governance/account-takeover",
            action_label="Review account takeover", phase="Identity matching",
            whether_required=takeover_required,
        ))

        config_steps = [
            ("account_naming_configured", naming_configured, "Account naming", "Configure account naming and collision handling.", "/sync-policies/account-naming", "Configure account naming"),
            ("field_authority_configured", bool(field_authority_rules), "Field authority", "Define the authoritative source and direction for managed identity fields.", "/sync-policies/field-authority", "Configure field authority"),
            ("attribute_mappings_configured", bool(attribute_mappings), "Attribute mappings", "Map authoritative enterprise identity fields to AD attributes.", "/sync-policies/attribute-mappings", "Configure attribute mappings"),
            ("department_ou_routing_configured", bool((department_mappings or default_ou) and disabled_ou), "Department and OU routing", "Configure stable department-to-OU routing and disabled-user placement.", "/sync-policies/department-ou-routing", "Configure OU routing"),
            ("lifecycle_safety_configured", lifecycle_safety_configured, "Lifecycle and security", "Configure joiner, mover, leaver, rehire and disable safety gates.", "/sync-policies/lifecycle", "Configure lifecycle safety"),
        ]
        for key, configured, title, summary, url, label in config_steps:
            steps.append(self._step(
                key, "complete" if configured else "action_required", title, summary,
                blocker_reason="" if configured else "This required business policy is not fully configured.",
                action_url=url, action_label=label, phase="Identity and directory policies",
            ))
        steps.append(self._step(
            "sync_scope_current", "complete" if scope_current else "stale" if scope else "action_required",
            "Synchronization scope", "Bind the rollout scope to the current source snapshot.",
            blocker_reason="" if scope_current else "The saved scope is missing or references an older source snapshot.",
            action_url="/sync-policies/scope", action_label="Review sync scope",
            phase="Identity and directory policies", last_updated_at=_text(scope.get("updated_at") if scope else ""),
            related_snapshot_id=source_snapshot_id or None,
        ))

        policy_prerequisite_keys = {
            "source_connector_ready", "ad_connector_ready",
            "source_snapshot_current", "ad_snapshot_current", "data_quality_reviewed",
            "identity_rules_configured", "identity_match_run_current", "identity_blockers_resolved",
            "account_takeover_resolved",
            "account_naming_configured", "field_authority_configured",
            "attribute_mappings_configured", "department_ou_routing_configured",
            "lifecycle_safety_configured", "sync_scope_current",
        }
        policy_prerequisites_complete = all(
            step.status == "complete"
            for step in steps
            if step.key in policy_prerequisite_keys and step.whether_required
        )
        if release_current and policy_prerequisites_complete:
            release_status, release_reason = "complete", ""
        elif release:
            release_status, release_reason = "stale", "The latest policy release does not represent the current reviewed configuration and source snapshot."
        elif policy_prerequisites_complete:
            release_status, release_reason = "ready", "Publish the reviewed policy configuration."
        else:
            release_status, release_reason = "blocked", "Complete all required identity and directory policies before publishing."
        steps.append(self._step(
            "policy_release_current", release_status, "Policy summary and release",
            "Publish one immutable policy version after all required checks pass.",
            blocker_reason=release_reason, action_url="/sync-policies/releases",
            action_label="Review and publish policy", phase="Release",
            last_updated_at=_text(getattr(release, "created_at", "")),
            release_id=release_id or None, related_snapshot_id=source_snapshot_id or None,
            metadata={
                "policy_release_hash": release_hash,
                "config_fingerprint": _text(config_fingerprint),
            },
        ))

        if dry_run_current:
            dry_status, dry_reason = "complete", ""
        elif dry_run_job:
            dry_status, dry_reason = "stale", "The latest successful Dry Run uses older snapshots, matching evidence or policy configuration."
        elif release_status == "complete":
            dry_status, dry_reason = "ready", "Run a Dry Run with the current evidence set."
        else:
            dry_status, dry_reason = "blocked", "A current policy release and all prerequisite evidence are required."
        steps.append(self._step(
            "dry_run_current", dry_status, "Current Dry Run",
            "Preview business changes using the current snapshots, match run and policy release.",
            blocker_reason=dry_reason, action_url="/execution-center/dry-run",
            action_label="Run Dry Run", phase="Dry Run and approval",
            last_updated_at=_text(getattr(dry_run_job, "ended_at", "")), job_id=dry_run_job_id,
            related_snapshot_id=source_snapshot_id or None, release_id=release_id or None,
            related_match_run_id=match_run_id,
            metadata={
                "source_snapshot_fingerprint": source_snapshot_fingerprint,
                "ad_snapshot_id": ad_snapshot_id or None,
                "ad_snapshot_fingerprint": ad_snapshot_fingerprint,
                "rules_fingerprint": rules_fingerprint,
                "policy_release_hash": release_hash,
                "config_fingerprint": _text(config_fingerprint),
            },
        ))
        steps.append(self._step(
            "plan_conflicts_resolved",
            "complete" if plan_conflicts_resolved else "action_required" if dry_run_current else "blocked",
            "Plan conflicts", "Resolve the conflicts raised by the current Dry Run plan.",
            blocker_reason=(
                f"{len(open_plan_conflicts)} plan conflict(s) remain open."
                if dry_run_current and open_plan_conflicts else "A current Dry Run is required."
                if not dry_run_current else ""
            ), action_url="/identity-governance/conflicts", action_label="Resolve plan conflicts",
            phase="Dry Run and approval", job_id=dry_run_job_id,
        ))
        if approval_current and plan_conflicts_resolved:
            approval_status, approval_reason = "complete", ""
        elif plan_review and not dry_run_current:
            approval_status, approval_reason = "stale", "The approval belongs to an older or stale Dry Run plan."
        elif plan_conflicts_resolved:
            approval_status, approval_reason = "action_required", "Approve the current Dry Run plan."
        else:
            approval_status, approval_reason = "blocked", "Resolve current plan conflicts before approval."
        steps.append(self._step(
            "approval_current", approval_status, "Current plan approval",
            "Record auditable approval for the exact current Dry Run plan.",
            blocker_reason=approval_reason, action_url="/execution-center/plan-review",
            action_label="Review and approve plan", phase="Dry Run and approval",
            last_updated_at=_text(getattr(plan_review, "reviewed_at", "")), job_id=dry_run_job_id,
        ))
        apply_gate_open = bool(approval_current and plan_conflicts_resolved and dry_run_current)
        steps.append(self._step(
            "apply_allowed", "complete" if current_apply else "ready" if apply_gate_open else "blocked",
            "Apply current plan", "Apply only the exact approved and current Dry Run plan.",
            blocker_reason="" if apply_gate_open else "Apply is blocked until the current plan, conflicts and approval all pass.",
            action_url="/execution-center/apply", action_label="Open Apply",
            phase="Apply and operate", last_updated_at=_text(getattr(current_apply, "ended_at", "")),
            job_id=_text(getattr(current_apply, "job_id", "")) or dry_run_job_id,
        ))

        return RolloutReadinessResult(
            org_id=normalized_org_id,
            org_name=_text(org_name) or normalized_org_id,
            generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            steps=steps,
        )


__all__ = [
    "READINESS_STATUSES",
    "RolloutReadinessResult",
    "RolloutReadinessService",
    "RolloutReadinessStep",
]
