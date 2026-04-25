from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from sync_app.services.external_integrations import approve_job_review as approve_job_review_action
from sync_app.services.job_diff import build_job_comparison_summary
from sync_app.storage.local_db import (
    DatabaseManager,
    PlannedOperationRepository,
    SyncConflictRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
    WebAuditLogRepository,
)


@dataclass(slots=True)
class WebJobService:
    db_manager: DatabaseManager
    job_repo: SyncJobRepository
    review_repo: SyncPlanReviewRepository
    planned_operation_repo: PlannedOperationRepository
    conflict_repo: SyncConflictRepository
    audit_repo: WebAuditLogRepository

    @staticmethod
    def _normalize_job_status(value: str | None) -> str:
        return str(value or "").strip().upper()

    @classmethod
    def _is_successful_dry_run(cls, job: Any) -> bool:
        return (
            str(getattr(job, "execution_mode", "") or "").strip().lower() == "dry_run"
            and cls._normalize_job_status(getattr(job, "status", "")) == "COMPLETED"
        )

    @classmethod
    def _is_successful_apply(cls, job: Any) -> bool:
        return (
            str(getattr(job, "execution_mode", "") or "").strip().lower() == "apply"
            and cls._normalize_job_status(getattr(job, "status", "")) == "COMPLETED"
        )

    @staticmethod
    def _parse_job_started_at(job: Any) -> datetime | None:
        raw_value = str(getattr(job, "started_at", "") or "").strip()
        if not raw_value:
            return None
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @classmethod
    def _find_previous_job(
        cls,
        recent_jobs: list[Any],
        current_job: Any,
        matcher: Callable[[Any], bool],
    ) -> Any | None:
        current_started_at = cls._parse_job_started_at(current_job)
        for candidate in recent_jobs:
            if str(getattr(candidate, "job_id", "") or "") == str(getattr(current_job, "job_id", "") or ""):
                continue
            if not matcher(candidate):
                continue
            if current_started_at is not None:
                candidate_started_at = cls._parse_job_started_at(candidate)
                if candidate_started_at is None or candidate_started_at >= current_started_at:
                    continue
            return candidate
        return None

    def list_recent_jobs(self, *, org_id: str, limit: int = 30) -> list[Any]:
        return self.job_repo.list_recent_job_records(limit=limit, org_id=org_id)

    def get_active_job(self, *, org_id: str) -> Any | None:
        return self.job_repo.get_active_job_record(org_id=org_id)

    def get_job_record(self, job_id: str) -> Any | None:
        return self.job_repo.get_job_record(job_id)

    def get_review_record(self, job_id: str) -> Any | None:
        return self.review_repo.get_review_record_by_job_id(job_id)

    def approve_review(
        self,
        *,
        org_id: str,
        job_id: str,
        reviewer_username: str,
        review_notes: str,
    ) -> dict[str, Any]:
        result = approve_job_review_action(
            self.db_manager,
            org_id=org_id,
            job_id=job_id,
            reviewer_username=reviewer_username,
            review_notes=review_notes,
        )
        self.audit_repo.add_log(
            org_id=org_id,
            actor_username=reviewer_username,
            action_type="job.review_approve",
            target_type="sync_job",
            target_id=job_id,
            result="success",
            message="Approved high-risk synchronization plan",
            payload={
                "expires_at": result["expires_at_iso"],
                "replay_request_id": result["replay_request_id"],
                "fresh_approval": result["fresh_approval"],
            },
        )
        return result

    def build_job_comparison_sections(self, *, org_id: str, job: Any) -> list[dict[str, Any]]:
        recent_jobs = self.list_recent_jobs(org_id=org_id, limit=200)
        sections: list[dict[str, Any]] = []

        previous_successful_dry_run = self._find_previous_job(recent_jobs, job, self._is_successful_dry_run)
        if previous_successful_dry_run is not None:
            sections.append(
                {
                    "label": "Compared With Previous Successful Dry Run",
                    "comparison": build_job_comparison_summary(
                        current_job=job,
                        baseline_job=previous_successful_dry_run,
                        planned_operation_repo=self.planned_operation_repo,
                        conflict_repo=self.conflict_repo,
                    ),
                }
            )

        previous_successful_apply = self._find_previous_job(recent_jobs, job, self._is_successful_apply)
        if previous_successful_apply is not None:
            sections.append(
                {
                    "label": "Compared With Previous Apply",
                    "comparison": build_job_comparison_summary(
                        current_job=job,
                        baseline_job=previous_successful_apply,
                        planned_operation_repo=self.planned_operation_repo,
                        conflict_repo=self.conflict_repo,
                    ),
                }
            )

        return sections

    def build_job_center_summary(self, *, org_id: str, preflight_summary: dict[str, Any]) -> dict[str, Any]:
        recent_jobs = self.list_recent_jobs(org_id=org_id, limit=30)
        latest_dry_run = next(
            (
                job
                for job in recent_jobs
                if str(getattr(job, "execution_mode", "") or "").strip().lower() == "dry_run"
            ),
            None,
        )
        latest_successful_dry_run = next((job for job in recent_jobs if self._is_successful_dry_run(job)), None)
        latest_apply = next(
            (
                job
                for job in recent_jobs
                if str(getattr(job, "execution_mode", "") or "").strip().lower() == "apply"
            ),
            None,
        )

        review_record = None
        review_required = False
        if latest_successful_dry_run:
            summary = dict(getattr(latest_successful_dry_run, "summary", {}) or {})
            review_required = bool(summary.get("review_required") or False)
            if review_required:
                review_record = self.get_review_record(latest_successful_dry_run.job_id)

        blocked_reasons: list[str] = []
        if str(preflight_summary.get("overall_status") or "") == "error":
            blocked_reasons.append("Fix organization configuration or connectivity errors before running apply.")
        if latest_dry_run and not latest_successful_dry_run:
            blocked_reasons.append(
                "The most recent dry run did not complete successfully. Re-run dry run after fixing errors."
            )
        if not latest_successful_dry_run:
            blocked_reasons.append("No successful dry run has been recorded for this organization yet.")
        open_conflict_count = int(preflight_summary.get("open_conflict_count") or 0)
        if open_conflict_count > 0:
            blocked_reasons.append("Resolve the open conflict queue before running apply.")
        if review_required and (
            review_record is None or str(review_record.status or "").strip().lower() != "approved"
        ):
            blocked_reasons.append("Latest high-risk dry run still needs review approval before apply can continue.")

        if str(preflight_summary.get("overall_status") or "") == "error":
            overall_status = "error"
            overall_label = "Blocked"
        elif blocked_reasons:
            overall_status = "warning"
            overall_label = "Needs Attention"
        else:
            overall_status = "success"
            overall_label = "Ready"

        if str(preflight_summary.get("overall_status") or "") == "error":
            next_action_url = "/config"
            next_action_label = "Fix Configuration"
        elif latest_dry_run and not latest_successful_dry_run:
            next_action_url = f"/jobs/{latest_dry_run.job_id}"
            next_action_label = "Inspect Dry Run Errors"
        elif not latest_successful_dry_run:
            next_action_url = "/jobs"
            next_action_label = "Run Dry Run"
        elif open_conflict_count > 0:
            next_action_url = "/conflicts"
            next_action_label = "Review Conflicts"
        elif review_required and (
            review_record is None or str(review_record.status or "").strip().lower() != "approved"
        ):
            next_action_url = f"/jobs/{latest_successful_dry_run.job_id}"
            next_action_label = "Approve High-Risk Plan"
        elif not latest_apply:
            next_action_url = "/jobs"
            next_action_label = "Run Apply"
        else:
            next_action_url = "/jobs"
            next_action_label = "Review Latest Apply"

        impact_job = latest_successful_dry_run or latest_dry_run
        impact_summary = dict(getattr(impact_job, "summary", {}) or {}) if impact_job else {}
        return {
            "overall_status": overall_status,
            "overall_label": overall_label,
            "blocked_reasons": blocked_reasons,
            "next_action_url": next_action_url,
            "next_action_label": next_action_label,
            "preflight_summary": preflight_summary,
            "latest_dry_run": latest_dry_run,
            "latest_successful_dry_run": latest_successful_dry_run,
            "latest_apply": latest_apply,
            "review_record": review_record,
            "review_required": review_required,
            "impact_preview": {
                "job_id": getattr(impact_job, "job_id", ""),
                "planned_operation_count": int(
                    impact_summary.get("planned_operation_count")
                    or getattr(impact_job, "planned_operation_count", 0)
                    or 0
                ),
                "high_risk_operation_count": int(impact_summary.get("high_risk_operation_count") or 0),
                "conflict_count": int(impact_summary.get("conflict_count") or 0),
                "error_count": int(getattr(impact_job, "error_count", 0) or 0),
            },
        }
