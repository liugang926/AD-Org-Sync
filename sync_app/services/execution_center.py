from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sync_app.services.high_risk_operations import (
    HighRiskOperationContext,
    HighRiskOperationPolicy,
)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _normalized_status(value: Any) -> str:
    return _clean_text(value).upper()


def _parse_timestamp(value: Any) -> datetime | None:
    raw_value = _clean_text(value)
    if not raw_value:
        return None
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class ExecutionPlanEvaluation:
    allowed: bool
    reason_code: str
    next_action_code: str
    job: Any | None
    review: Any | None
    job_scope: dict[str, Any] | None
    current_scope: dict[str, Any] | None
    environment_label: str
    plan_generated_at: str
    max_age_hours: int
    context: HighRiskOperationContext

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reason_code": self.reason_code,
            "next_action_code": self.next_action_code,
            "job": self.job,
            "review": self.review,
            "job_scope": self.job_scope,
            "current_scope": self.current_scope,
            "environment_label": self.environment_label,
            "plan_generated_at": self.plan_generated_at,
            "max_age_hours": self.max_age_hours,
            "context": self.context.to_dict(),
        }


class ExecutionCenterService:
    """Resolve one immutable Dry Run plan for review and Apply gating."""

    def __init__(
        self,
        *,
        job_repo: Any,
        review_repo: Any,
        source_directory_repo: Any,
        settings_repo: Any,
    ) -> None:
        self.job_repo = job_repo
        self.review_repo = review_repo
        self.source_directory_repo = source_directory_repo
        self.settings_repo = settings_repo

    def _max_age_hours(self, org_id: str) -> int:
        return max(
            int(
                self.settings_repo.get_int(
                    "execution_plan_max_age_hours",
                    24,
                    org_id=org_id,
                )
                or 24
            ),
            1,
        )

    def _resolve_plan_job(self, *, org_id: str, plan_job_id: str = "") -> Any | None:
        requested_job_id = _clean_text(plan_job_id)
        if requested_job_id:
            job = self.job_repo.get_job_record(requested_job_id)
            if job is None or _clean_text(getattr(job, "org_id", "")).lower() != org_id:
                return None
            return job
        return next(
            (
                job
                for job in self.job_repo.list_recent_job_records(limit=100, org_id=org_id)
                if _clean_text(getattr(job, "execution_mode", "")).lower() == "dry_run"
                and _normalized_status(getattr(job, "status", "")) == "COMPLETED"
            ),
            None,
        )

    @staticmethod
    def _context_for(
        *,
        job: Any | None,
        org_id: str,
        organization_name: str,
        environment_label: str,
    ) -> HighRiskOperationContext:
        summary = dict(getattr(job, "summary", {}) or {}) if job is not None else {}
        snapshot_id = int(summary.get("source_snapshot_id") or 0)
        return HighRiskOperationContext.create(
            operation_code="sync.apply",
            organization_id=org_id,
            organization_name=organization_name,
            environment_label=environment_label,
            snapshot_version=f"#{snapshot_id}" if snapshot_id else "Not available",
            impact_count=int(
                summary.get("planned_operation_count")
                or getattr(job, "planned_operation_count", 0)
                or 0
            ),
            preview_id=_clean_text(getattr(job, "job_id", "")),
        )

    def evaluate_plan(
        self,
        *,
        org_id: str,
        organization_name: str,
        environment_label: str,
        plan_job_id: str = "",
        require_approval: bool = True,
        require_unused: bool = True,
        current_config_fingerprint: str | None = "",
        now: datetime | None = None,
    ) -> ExecutionPlanEvaluation:
        normalized_org_id = _clean_text(org_id).lower() or "default"
        current_time = now or datetime.now(timezone.utc)
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=timezone.utc)
        current_time = current_time.astimezone(timezone.utc)
        max_age_hours = self._max_age_hours(normalized_org_id)
        job = self._resolve_plan_job(
            org_id=normalized_org_id,
            plan_job_id=plan_job_id,
        )
        context = self._context_for(
            job=job,
            org_id=normalized_org_id,
            organization_name=organization_name,
            environment_label=environment_label,
        )

        def decision(
            allowed: bool,
            reason_code: str = "",
            next_action_code: str = "",
            *,
            review: Any | None = None,
            job_scope: dict[str, Any] | None = None,
            current_scope: dict[str, Any] | None = None,
            plan_generated_at: str = "",
        ) -> ExecutionPlanEvaluation:
            return ExecutionPlanEvaluation(
                allowed=allowed,
                reason_code=reason_code,
                next_action_code=next_action_code,
                job=job,
                review=review,
                job_scope=job_scope,
                current_scope=current_scope,
                environment_label=environment_label,
                plan_generated_at=plan_generated_at,
                max_age_hours=max_age_hours,
                context=context,
            )

        environment_gate = HighRiskOperationPolicy.evaluate(context)
        if not environment_gate.allowed:
            return decision(
                False,
                environment_gate.reason_code,
                environment_gate.next_action_code,
            )
        if job is None:
            return decision(
                False,
                "execution.blocker.no_dry_run",
                "execution.action.run_dry_run",
            )
        if _clean_text(getattr(job, "execution_mode", "")).lower() != "dry_run":
            return decision(
                False,
                "execution.blocker.not_dry_run",
                "execution.action.select_dry_run",
            )
        if _normalized_status(getattr(job, "status", "")) != "COMPLETED":
            return decision(
                False,
                "execution.blocker.plan_not_completed",
                "execution.action.run_dry_run",
            )

        summary = dict(getattr(job, "summary", {}) or {})
        plan_fingerprint = _clean_text(summary.get("plan_fingerprint"))
        plan_environment = _clean_text(summary.get("environment_label"))
        plan_generated_at = _clean_text(
            summary.get("plan_generated_at")
            or getattr(job, "ended_at", "")
            or getattr(job, "started_at", "")
        )
        if not plan_fingerprint or not _clean_text(getattr(job, "config_snapshot_hash", "")):
            return decision(
                False,
                "execution.blocker.plan_evidence_missing",
                "execution.action.run_dry_run",
                plan_generated_at=plan_generated_at,
            )
        if current_config_fingerprint is None:
            return decision(
                False,
                "execution.blocker.config_unavailable",
                "execution.action.run_dry_run",
                plan_generated_at=plan_generated_at,
            )
        normalized_current_config_fingerprint = _clean_text(current_config_fingerprint)
        if (
            normalized_current_config_fingerprint
            and normalized_current_config_fingerprint
            != _clean_text(getattr(job, "config_snapshot_hash", ""))
        ):
            return decision(
                False,
                "execution.blocker.config_changed",
                "execution.action.run_dry_run",
                plan_generated_at=plan_generated_at,
            )
        if not plan_environment:
            return decision(
                False,
                "execution.blocker.plan_environment_unknown",
                "execution.action.run_dry_run",
                plan_generated_at=plan_generated_at,
            )
        if plan_environment.casefold() != _clean_text(environment_label).casefold():
            return decision(
                False,
                "execution.blocker.environment_changed",
                "execution.action.run_dry_run",
                plan_generated_at=plan_generated_at,
            )
        generated_at = _parse_timestamp(plan_generated_at)
        if generated_at is None or generated_at < current_time - timedelta(hours=max_age_hours):
            return decision(
                False,
                "execution.blocker.plan_expired",
                "execution.action.run_dry_run",
                plan_generated_at=plan_generated_at,
            )

        job_scope = self.source_directory_repo.get_job_scope(
            _clean_text(getattr(job, "job_id", "")),
            org_id=normalized_org_id,
        )
        if job_scope is None:
            return decision(
                False,
                "execution.blocker.snapshot_evidence_missing",
                "execution.action.save_scope",
                plan_generated_at=plan_generated_at,
            )
        snapshot_id = int(job_scope.get("snapshot_id") or 0)
        snapshot_fingerprint = _clean_text(job_scope.get("source_snapshot_fingerprint"))
        if (
            snapshot_id <= 0
            or snapshot_id != int(summary.get("source_snapshot_id") or 0)
            or snapshot_fingerprint != _clean_text(summary.get("source_snapshot_fingerprint"))
            or _clean_text(job_scope.get("selection_fingerprint"))
            != _clean_text(summary.get("selection_fingerprint"))
            or _clean_text(job_scope.get("config_fingerprint"))
            != _clean_text(getattr(job, "config_snapshot_hash", ""))
        ):
            return decision(
                False,
                "execution.blocker.snapshot_changed",
                "execution.action.run_dry_run",
                job_scope=job_scope,
                plan_generated_at=plan_generated_at,
            )
        snapshot = self.source_directory_repo.get_snapshot(
            snapshot_id,
            org_id=normalized_org_id,
        )
        if (
            snapshot is None
            or _clean_text(snapshot["status"]).lower() != "succeeded"
            or _clean_text(snapshot["snapshot_fingerprint"]) != snapshot_fingerprint
        ):
            return decision(
                False,
                "execution.blocker.snapshot_unavailable",
                "execution.action.refresh_source",
                job_scope=job_scope,
                plan_generated_at=plan_generated_at,
            )
        latest_snapshot = self.source_directory_repo.get_latest_successful_snapshot(
            org_id=normalized_org_id,
            provider_id=_clean_text(job_scope.get("provider_id")),
            connector_id=_clean_text(job_scope.get("connector_id")) or "default",
        )
        if (
            latest_snapshot is None
            or int(latest_snapshot["id"] or 0) != snapshot_id
            or _clean_text(latest_snapshot["snapshot_fingerprint"])
            != snapshot_fingerprint
        ):
            return decision(
                False,
                "execution.blocker.snapshot_superseded",
                "execution.action.save_scope",
                job_scope=job_scope,
                plan_generated_at=plan_generated_at,
            )
        snapshot_expires_at = _parse_timestamp(snapshot["expires_at"])
        if snapshot_expires_at is None or snapshot_expires_at < current_time:
            return decision(
                False,
                "execution.blocker.snapshot_expired",
                "execution.action.refresh_source",
                job_scope=job_scope,
                plan_generated_at=plan_generated_at,
            )
        current_scope = self.source_directory_repo.get_scope_selection(
            org_id=normalized_org_id,
            provider_id=_clean_text(job_scope.get("provider_id")),
            connector_id=_clean_text(job_scope.get("connector_id")) or "default",
        )
        if (
            current_scope is None
            or int(current_scope.get("snapshot_id") or 0) != snapshot_id
            or _clean_text(current_scope.get("source_snapshot_fingerprint"))
            != snapshot_fingerprint
            or _clean_text(current_scope.get("selection_fingerprint"))
            != _clean_text(job_scope.get("selection_fingerprint"))
        ):
            return decision(
                False,
                "execution.blocker.scope_changed",
                "execution.action.run_dry_run",
                job_scope=job_scope,
                current_scope=current_scope,
                plan_generated_at=plan_generated_at,
            )

        review = self.review_repo.get_review_record_by_job_id(
            _clean_text(getattr(job, "job_id", ""))
        )
        if review is None:
            return decision(
                False,
                "execution.blocker.review_missing",
                "execution.action.run_dry_run",
                job_scope=job_scope,
                current_scope=current_scope,
                plan_generated_at=plan_generated_at,
            )
        if (
            _clean_text(getattr(review, "plan_fingerprint", "")) != plan_fingerprint
            or _clean_text(getattr(review, "config_snapshot_hash", ""))
            != _clean_text(getattr(job, "config_snapshot_hash", ""))
        ):
            return decision(
                False,
                "execution.blocker.review_mismatch",
                "execution.action.run_dry_run",
                review=review,
                job_scope=job_scope,
                current_scope=current_scope,
                plan_generated_at=plan_generated_at,
            )
        if require_approval:
            if _clean_text(getattr(review, "status", "")).lower() != "approved":
                return decision(
                    False,
                    "execution.blocker.review_pending",
                    "execution.action.review_plan",
                    review=review,
                    job_scope=job_scope,
                    current_scope=current_scope,
                    plan_generated_at=plan_generated_at,
                )
            expires_at = _parse_timestamp(getattr(review, "expires_at", ""))
            if expires_at is None or expires_at < current_time:
                return decision(
                    False,
                    "execution.blocker.review_expired",
                    "execution.action.run_dry_run",
                    review=review,
                    job_scope=job_scope,
                    current_scope=current_scope,
                    plan_generated_at=plan_generated_at,
                )
            if require_unused:
                existing_apply = self.job_repo.get_apply_job_for_plan_source(
                    _clean_text(getattr(job, "job_id", "")),
                    org_id=normalized_org_id,
                )
                if existing_apply is not None:
                    return decision(
                        False,
                        "execution.blocker.plan_already_applied",
                        "execution.action.run_dry_run",
                        review=review,
                        job_scope=job_scope,
                        current_scope=current_scope,
                        plan_generated_at=plan_generated_at,
                    )

        return decision(
            True,
            review=review,
            job_scope=job_scope,
            current_scope=current_scope,
            plan_generated_at=plan_generated_at,
        )

    def list_review_items(
        self,
        *,
        org_id: str,
        organization_name: str,
        environment_label: str,
        current_config_fingerprint: str | None = "",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for review in self.review_repo.list_review_records(
            org_id=org_id,
            limit=limit,
        ):
            evaluation = self.evaluate_plan(
                org_id=org_id,
                organization_name=organization_name,
                environment_label=environment_label,
                plan_job_id=review.job_id,
                require_approval=(
                    _clean_text(getattr(review, "status", "")).lower()
                    == "approved"
                ),
                current_config_fingerprint=current_config_fingerprint,
            )
            items.append(
                {
                    "job": evaluation.job,
                    "review": review,
                    "evaluation": evaluation,
                }
            )
        return items


__all__ = ["ExecutionCenterService", "ExecutionPlanEvaluation"]
