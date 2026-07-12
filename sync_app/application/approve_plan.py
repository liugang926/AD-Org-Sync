from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from sync_app.application.context import TenantContext
from sync_app.storage.local_db import (
    DatabaseManager,
    SettingsRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
    SyncReplayRequestRepository,
    WebAuditLogRepository,
    normalize_org_id,
)


class ApprovalEventPublisher(Protocol):
    def publish_review_approved(
        self,
        *,
        tenant: TenantContext,
        job: Any,
        review: Any,
        replay_request_id: int | None,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class ApprovePlanResult:
    job: Any
    review: Any
    expires_at_iso: str
    replay_request_id: int | None
    fresh_approval: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "job": self.job,
            "review": self.review,
            "expires_at_iso": self.expires_at_iso,
            "replay_request_id": self.replay_request_id,
            "fresh_approval": self.fresh_approval,
        }


class ApproveSyncPlanUseCase:
    """Approve a high-risk sync plan consistently across all delivery channels."""

    def __init__(
        self,
        db_manager: DatabaseManager,
        *,
        event_publisher: ApprovalEventPublisher | None = None,
    ) -> None:
        self.db_manager = db_manager
        self.job_repo = SyncJobRepository(db_manager)
        self.review_repo = SyncPlanReviewRepository(db_manager)
        self.settings_repo = SettingsRepository(db_manager)
        self.replay_request_repo = SyncReplayRequestRepository(db_manager)
        self.audit_repo = WebAuditLogRepository(db_manager)
        self.event_publisher = event_publisher

    def execute(
        self,
        tenant: TenantContext,
        *,
        job_id: str,
        review_notes: str = "",
        ttl_minutes: int | None = None,
    ) -> ApprovePlanResult:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            raise ValueError("Job ID is required")

        job_record = self.job_repo.get_job_record(normalized_job_id)
        if job_record is None:
            raise ValueError("Job not found")
        job_org_id = normalize_org_id(getattr(job_record, "org_id", None), fallback="default") or "default"
        if job_org_id != tenant.org_id:
            raise ValueError("Job does not belong to the current organization")

        review_record = self.review_repo.get_review_record_by_job_id(normalized_job_id)
        if review_record is None:
            raise ValueError("This job does not have a pending high-risk review")

        already_approved = str(getattr(review_record, "status", "") or "").strip().lower() == "approved"
        replay_request_id: int | None = None
        if already_approved:
            proposed_expires_at = str(getattr(review_record, "expires_at", "") or "")
        else:
            resolved_ttl_minutes = self._resolve_ttl_minutes(ttl_minutes)
            proposed_expires_at = (
                datetime.now(timezone.utc) + timedelta(minutes=resolved_ttl_minutes)
            ).isoformat(timespec="seconds")

        with self.db_manager.transaction() as connection:
            fresh_approval = False
            if not already_approved:
                fresh_approval = self.review_repo.approve_review(
                    normalized_job_id,
                    reviewer_username=tenant.actor_username,
                    review_notes=str(review_notes or "").strip(),
                    expires_at=proposed_expires_at,
                    connection=connection,
                    only_if_pending=True,
                )
                if fresh_approval and self.settings_repo.get_bool(
                    "automatic_replay_enabled",
                    False,
                    org_id=tenant.org_id,
                ):
                    replay_request_id = self.replay_request_repo.enqueue_request(
                        request_type="plan_approval",
                        execution_mode="apply",
                        requested_by=tenant.actor_username,
                        org_id=tenant.org_id,
                        target_scope="job",
                        target_id=normalized_job_id,
                        trigger_reason="high_risk_plan_approved",
                        payload={"expires_at": proposed_expires_at},
                        connection=connection,
                    )

            persisted_review = connection.execute(
                "SELECT status, expires_at FROM sync_plan_reviews WHERE job_id = ? LIMIT 1",
                (normalized_job_id,),
            ).fetchone()
            if persisted_review is None or str(persisted_review["status"] or "").strip().lower() != "approved":
                raise ValueError("This high-risk review is not pending or approved")
            expires_at_iso = str(persisted_review["expires_at"] or "")
            self.audit_repo.add_log(
                org_id=tenant.org_id,
                actor_username=tenant.actor_username,
                action_type="plan_review.approve",
                target_type="sync_job",
                target_id=normalized_job_id,
                result="success",
                message="Approved high-risk synchronization plan",
                payload={
                    "channel": tenant.channel,
                    "expires_at": expires_at_iso,
                    "replay_request_id": replay_request_id,
                    "fresh_approval": fresh_approval,
                },
                connection=connection,
            )

        updated_review = self.review_repo.get_review_record_by_job_id(normalized_job_id)
        if updated_review is None:
            raise RuntimeError("Approved review could not be reloaded")
        if fresh_approval and self.event_publisher is not None:
            self.event_publisher.publish_review_approved(
                tenant=tenant,
                job=job_record,
                review=updated_review,
                replay_request_id=replay_request_id,
            )
        return ApprovePlanResult(
            job=job_record,
            review=updated_review,
            expires_at_iso=expires_at_iso,
            replay_request_id=replay_request_id,
            fresh_approval=fresh_approval,
        )

    def _resolve_ttl_minutes(self, ttl_minutes: int | None) -> int:
        if ttl_minutes is None:
            return max(self.settings_repo.get_int("high_risk_review_ttl_minutes", 240), 1)
        try:
            normalized_ttl = int(ttl_minutes)
        except (TypeError, ValueError) as exc:
            raise ValueError("Approval TTL must be an integer number of minutes") from exc
        if normalized_ttl <= 0:
            raise ValueError("Approval TTL must be greater than zero")
        return normalized_ttl
