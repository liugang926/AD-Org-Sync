from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sync_app.services.external_integrations import approve_job_review as approve_job_review_action
from sync_app.services.external_integrations import (
    build_integration_center_context,
    extract_bearer_token,
    generate_integration_api_token,
    is_valid_integration_api_token,
    organization_exists,
    retry_failed_outbox_deliveries,
    retry_outbox_delivery,
    serialize_conflict_record,
    serialize_job_record,
    serialize_job_records,
    validate_integration_subscription_payload,
)
from sync_app.storage.local_db import (
    DatabaseManager,
    IntegrationWebhookSubscriptionRepository,
    SettingsRepository,
    SyncConflictRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
    WebAuditLogRepository,
)


@dataclass(slots=True)
class WebIntegrationService:
    db_manager: DatabaseManager
    settings_repo: SettingsRepository
    subscription_repo: IntegrationWebhookSubscriptionRepository
    job_repo: SyncJobRepository
    review_repo: SyncPlanReviewRepository
    conflict_repo: SyncConflictRepository
    audit_repo: WebAuditLogRepository

    def build_center_context(self, *, org_id: str) -> dict[str, Any]:
        return build_integration_center_context(self.db_manager, org_id)

    def authorize_api_request(self, *, org_id: str, authorization_header: str | None) -> dict[str, Any]:
        from sync_app.storage.local_db import normalize_org_id

        normalized_org_id = normalize_org_id(org_id, fallback="default") or "default"
        if not organization_exists(self.db_manager, normalized_org_id):
            return {"ok": False, "error": "Organization not found", "status_code": 404}
        token = extract_bearer_token(authorization_header)
        if not is_valid_integration_api_token(
            self.settings_repo,
            org_id=normalized_org_id,
            token=token,
        ):
            return {"ok": False, "error": "Invalid or missing integration API token", "status_code": 401}
        return {"ok": True, "org_id": normalized_org_id}

    def rotate_api_token(self, *, org_id: str, actor_username: str) -> str:
        token = generate_integration_api_token()
        self.settings_repo.set_value(
            "integration_api_token",
            token,
            "string",
            org_id=org_id,
        )
        self.audit_repo.add_log(
            org_id=org_id,
            actor_username=actor_username,
            action_type="integration.token_rotate",
            target_type="integration_api",
            target_id=org_id,
            result="success",
            message="Rotated integration API token",
        )
        return token

    def clear_api_token(self, *, org_id: str, actor_username: str) -> None:
        self.settings_repo.set_value(
            "integration_api_token",
            "",
            "string",
            org_id=org_id,
        )
        self.audit_repo.add_log(
            org_id=org_id,
            actor_username=actor_username,
            action_type="integration.token_clear",
            target_type="integration_api",
            target_id=org_id,
            result="success",
            message="Cleared integration API token",
        )

    def save_subscription(
        self,
        *,
        org_id: str,
        actor_username: str,
        event_type: str,
        target_url: str,
        secret: str,
        description: str,
        is_enabled: bool,
    ) -> Any:
        normalized_event_type, normalized_target_url = validate_integration_subscription_payload(
            event_type=event_type,
            target_url=target_url,
        )
        record = self.subscription_repo.upsert_subscription(
            org_id=org_id,
            event_type=normalized_event_type,
            target_url=normalized_target_url,
            secret=secret,
            description=description,
            is_enabled=is_enabled,
        )
        self.audit_repo.add_log(
            org_id=org_id,
            actor_username=actor_username,
            action_type="integration.subscription_save",
            target_type="integration_webhook_subscription",
            target_id=str(record.id or ""),
            result="success",
            message="Saved integration webhook subscription",
            payload={
                "event_type": normalized_event_type,
                "target_url": normalized_target_url,
                "is_enabled": bool(record.is_enabled),
            },
        )
        return record

    def delete_subscription(self, *, org_id: str, actor_username: str, subscription_id: int) -> bool:
        subscription = self.subscription_repo.get_subscription_record(subscription_id, org_id=org_id)
        if subscription is None:
            return False
        self.subscription_repo.delete_subscription(subscription_id, org_id=org_id)
        self.audit_repo.add_log(
            org_id=org_id,
            actor_username=actor_username,
            action_type="integration.subscription_delete",
            target_type="integration_webhook_subscription",
            target_id=str(subscription_id),
            result="success",
            message="Deleted integration webhook subscription",
            payload={
                "event_type": subscription.event_type,
                "target_url": subscription.target_url,
            },
        )
        return True

    def retry_delivery(self, *, org_id: str, actor_username: str, delivery_id: int) -> Any:
        result = retry_outbox_delivery(
            self.db_manager,
            org_id=org_id,
            delivery_id=delivery_id,
        )
        delivery = result["delivery"]
        self.audit_repo.add_log(
            org_id=org_id,
            actor_username=actor_username,
            action_type="integration.delivery_retry",
            target_type="integration_webhook_delivery",
            target_id=str(delivery_id),
            result="success",
            message="Requeued failed integration delivery",
            payload={
                "event_type": delivery.event_type,
                "target_url": delivery.target_url,
            },
        )
        return delivery

    def retry_failed_deliveries(self, *, org_id: str, actor_username: str) -> int:
        result = retry_failed_outbox_deliveries(
            self.db_manager,
            org_id=org_id,
        )
        retried_count = int(result.get("retried_count") or 0)
        if retried_count > 0:
            self.audit_repo.add_log(
                org_id=org_id,
                actor_username=actor_username,
                action_type="integration.delivery_retry_bulk",
                target_type="integration_webhook_delivery",
                target_id=org_id,
                result="success",
                message="Requeued failed integration deliveries",
                payload={
                    "retried_count": retried_count,
                },
            )
        return retried_count

    def build_jobs_api_payload(self, *, org_id: str, limit: int, status_filter: str) -> dict[str, Any]:
        normalized_status = str(status_filter or "").strip().upper()
        jobs = self.job_repo.list_recent_job_records(limit=limit * 3, org_id=org_id)
        if normalized_status:
            jobs = [
                job
                for job in jobs
                if str(getattr(job, "status", "") or "").strip().upper() == normalized_status
            ]
        jobs = jobs[:limit]
        return {
            "ok": True,
            "org_id": org_id,
            "count": len(jobs),
            "items": serialize_job_records(jobs, self.review_repo),
        }

    def build_job_detail_api_payload(self, *, org_id: str, job_id: str) -> dict[str, Any] | None:
        job_record = self.job_repo.get_job_record(job_id)
        if job_record is None or (job_record.org_id or "default") != org_id:
            return None
        review_record = self.review_repo.get_review_record_by_job_id(job_id)
        return {
            "ok": True,
            "org_id": org_id,
            "item": serialize_job_record(job_record, review_record=review_record),
        }

    def build_conflicts_api_payload(
        self,
        *,
        org_id: str,
        limit: int,
        status_filter: str,
        job_id_filter: str | None,
    ) -> dict[str, Any]:
        conflicts = self.conflict_repo.list_conflict_records(
            limit=limit,
            job_id=job_id_filter,
            status=status_filter or None,
            org_id=org_id,
        )
        return {
            "ok": True,
            "org_id": org_id,
            "count": len(conflicts),
            "items": [serialize_conflict_record(conflict) for conflict in conflicts],
        }

    def approve_review_via_api(
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
            action_type="integration.review_approve",
            target_type="sync_job",
            target_id=job_id,
            result="success",
            message="Approved high-risk synchronization plan through integration API",
            payload={
                "expires_at": result["expires_at_iso"],
                "replay_request_id": result["replay_request_id"],
                "fresh_approval": result["fresh_approval"],
            },
        )
        return {
            "ok": True,
            "org_id": org_id,
            "job_id": job_id,
            "expires_at": result["expires_at_iso"],
            "replay_request_id": result["replay_request_id"],
            "fresh_approval": result["fresh_approval"],
            "review": serialize_job_record(result["job"], review_record=result["review"])["review"],
        }
