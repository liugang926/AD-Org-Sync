from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from sync_app.infra.requests_compat import ensure_requests_available, requests
from sync_app.storage.local_db import DatabaseManager, normalize_org_id
from sync_app.storage.repositories.conflicts import SyncConflictRepository, SyncPlanReviewRepository
from sync_app.storage.repositories.jobs import SyncJobRepository
from sync_app.storage.repositories.organizations import OrganizationRepository
from sync_app.storage.repositories.system import (
    IntegrationWebhookSubscriptionRepository,
    SettingsRepository,
    SyncReplayRequestRepository,
)

INTEGRATION_API_TOKEN_SETTING = "integration_api_token"
INTEGRATION_EVENT_TYPES = (
    "job.completed",
    "job.failed",
    "job.review_required",
    "review.approved",
)
INTEGRATION_EVENT_OPTIONS = (
    {
        "value": "job.completed",
        "label": "Job Completed",
        "description": "Emit when a dry run or apply job reaches a completed state.",
    },
    {
        "value": "job.failed",
        "label": "Job Failed",
        "description": "Emit when a sync job finishes in failed state.",
    },
    {
        "value": "job.review_required",
        "label": "Job Review Required",
        "description": "Emit when a job still requires a high-risk approval decision.",
    },
    {
        "value": "review.approved",
        "label": "Review Approved",
        "description": "Emit when a high-risk dry-run review is approved by UI or external callback.",
    },
)


def generate_integration_api_token() -> str:
    return secrets.token_urlsafe(36)


def mask_integration_api_token(token: str) -> str:
    normalized = str(token or "").strip()
    if not normalized:
        return ""
    if len(normalized) <= 12:
        return "***"
    return f"{normalized[:8]}***{normalized[-6:]}"


def mask_integration_secret(secret: str) -> str:
    normalized = str(secret or "").strip()
    if not normalized:
        return ""
    if len(normalized) <= 8:
        return "***"
    return f"{normalized[:4]}***{normalized[-4:]}"


def extract_bearer_token(authorization_header: str | None) -> str:
    raw_value = str(authorization_header or "").strip()
    if not raw_value:
        return ""
    scheme, _, token = raw_value.partition(" ")
    if scheme.lower() != "bearer":
        return ""
    return token.strip()


def is_valid_integration_api_token(
    settings_repo: SettingsRepository,
    *,
    org_id: str,
    token: str,
) -> bool:
    normalized_org_id = normalize_org_id(org_id, fallback="default") or "default"
    expected_token = settings_repo.get_value(
        INTEGRATION_API_TOKEN_SETTING,
        "",
        org_id=normalized_org_id,
        fallback_to_global=False,
    ) or ""
    normalized_token = str(token or "").strip()
    if not expected_token or not normalized_token:
        return False
    return hmac.compare_digest(expected_token, normalized_token)


def _normalize_event_type(value: str) -> str:
    return str(value or "").strip().lower()


def _normalize_job_status(value: str | None) -> str:
    return str(value or "").strip().upper()


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _serialize_review_record(review_record: Any | None) -> dict[str, Any] | None:
    if review_record is None:
        return None
    return {
        "job_id": str(getattr(review_record, "job_id", "") or ""),
        "status": str(getattr(review_record, "status", "") or ""),
        "high_risk_operation_count": int(getattr(review_record, "high_risk_operation_count", 0) or 0),
        "plan_fingerprint": str(getattr(review_record, "plan_fingerprint", "") or ""),
        "reviewer_username": str(getattr(review_record, "reviewer_username", "") or ""),
        "review_notes": str(getattr(review_record, "review_notes", "") or ""),
        "reviewed_at": str(getattr(review_record, "reviewed_at", "") or ""),
        "expires_at": str(getattr(review_record, "expires_at", "") or ""),
        "created_at": str(getattr(review_record, "created_at", "") or ""),
    }


def serialize_job_record(job_record: Any, *, review_record: Any | None = None) -> dict[str, Any]:
    summary = dict(getattr(job_record, "summary", {}) or {})
    return {
        "job_id": str(getattr(job_record, "job_id", "") or ""),
        "org_id": str(getattr(job_record, "org_id", "") or ""),
        "trigger_type": str(getattr(job_record, "trigger_type", "") or ""),
        "execution_mode": str(getattr(job_record, "execution_mode", "") or ""),
        "status": str(getattr(job_record, "status", "") or ""),
        "requested_by": str(getattr(job_record, "requested_by", "") or ""),
        "requested_config_path": str(getattr(job_record, "requested_config_path", "") or ""),
        "plan_source_job_id": str(getattr(job_record, "plan_source_job_id", "") or ""),
        "config_snapshot_hash": str(getattr(job_record, "config_snapshot_hash", "") or ""),
        "started_at": str(getattr(job_record, "started_at", "") or ""),
        "ended_at": str(getattr(job_record, "ended_at", "") or ""),
        "planned_operation_count": int(getattr(job_record, "planned_operation_count", 0) or 0),
        "executed_operation_count": int(getattr(job_record, "executed_operation_count", 0) or 0),
        "error_count": int(getattr(job_record, "error_count", 0) or 0),
        "summary": summary,
        "review_required": bool(summary.get("review_required") or False),
        "review": _serialize_review_record(review_record),
    }


def serialize_conflict_record(conflict_record: Any) -> dict[str, Any]:
    return {
        "id": int(getattr(conflict_record, "id", 0) or 0),
        "job_id": str(getattr(conflict_record, "job_id", "") or ""),
        "conflict_type": str(getattr(conflict_record, "conflict_type", "") or ""),
        "severity": str(getattr(conflict_record, "severity", "") or ""),
        "status": str(getattr(conflict_record, "status", "") or ""),
        "source_id": str(getattr(conflict_record, "source_id", "") or ""),
        "target_key": str(getattr(conflict_record, "target_key", "") or ""),
        "message": str(getattr(conflict_record, "message", "") or ""),
        "resolution_hint": str(getattr(conflict_record, "resolution_hint", "") or ""),
        "details": dict(getattr(conflict_record, "details", {}) or {}),
        "created_at": str(getattr(conflict_record, "created_at", "") or ""),
        "resolved_at": str(getattr(conflict_record, "resolved_at", "") or ""),
    }


def build_integration_center_context(db_manager: DatabaseManager, org_id: str) -> dict[str, Any]:
    normalized_org_id = normalize_org_id(org_id, fallback="default") or "default"
    settings_repo = SettingsRepository(db_manager)
    subscription_repo = IntegrationWebhookSubscriptionRepository(db_manager)
    job_repo = SyncJobRepository(db_manager)
    conflict_repo = SyncConflictRepository(db_manager)
    review_repo = SyncPlanReviewRepository(db_manager)

    raw_token = settings_repo.get_value(
        INTEGRATION_API_TOKEN_SETTING,
        "",
        org_id=normalized_org_id,
        fallback_to_global=False,
    ) or ""
    subscriptions = subscription_repo.list_subscription_records(org_id=normalized_org_id, limit=100)
    recent_jobs = job_repo.list_recent_job_records(limit=8, org_id=normalized_org_id)
    open_conflict_count = conflict_repo.list_conflict_records_page(
        limit=1,
        offset=0,
        status="open",
        org_id=normalized_org_id,
    )[1]
    pending_reviews = review_repo.list_review_records(status="pending", limit=10, org_id=normalized_org_id)
    subscription_view = [
        {
            "id": record.id,
            "event_type": record.event_type,
            "target_url": record.target_url,
            "description": record.description,
            "is_enabled": record.is_enabled,
            "secret_masked": mask_integration_secret(record.secret),
            "secret_configured": bool(record.secret),
            "last_attempt_at": record.last_attempt_at,
            "last_status": record.last_status,
            "last_error": record.last_error,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }
        for record in subscriptions
    ]
    return {
        "api_base_path": f"/api/integrations/orgs/{normalized_org_id}",
        "api_token_configured": bool(raw_token),
        "api_token_masked": mask_integration_api_token(raw_token),
        "subscription_records": subscription_view,
        "subscription_event_options": list(INTEGRATION_EVENT_OPTIONS),
        "recent_jobs": [
            serialize_job_record(
                job_record,
                review_record=review_repo.get_review_record_by_job_id(job_record.job_id),
            )
            for job_record in recent_jobs
        ],
        "open_conflict_count": int(open_conflict_count or 0),
        "pending_review_count": len(pending_reviews),
        "active_subscription_count": sum(1 for record in subscriptions if record.is_enabled),
    }


def validate_integration_subscription_payload(
    *,
    event_type: str,
    target_url: str,
) -> tuple[str, str]:
    normalized_event_type = _normalize_event_type(event_type)
    normalized_target_url = str(target_url or "").strip()
    if normalized_event_type not in INTEGRATION_EVENT_TYPES:
        raise ValueError("Unsupported event type")
    if not normalized_target_url.startswith(("http://", "https://")):
        raise ValueError("Webhook target URL must start with http:// or https://")
    return normalized_event_type, normalized_target_url


def _build_webhook_signature(secret: str, payload_bytes: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), payload_bytes, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def emit_integration_event(
    db_manager: DatabaseManager,
    *,
    org_id: str,
    event_type: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    normalized_org_id = normalize_org_id(org_id, fallback="default") or "default"
    normalized_event_type = _normalize_event_type(event_type)
    if normalized_event_type not in INTEGRATION_EVENT_TYPES:
        raise ValueError(f"unsupported integration event type: {event_type}")

    ensure_requests_available()
    subscription_repo = IntegrationWebhookSubscriptionRepository(db_manager)
    subscriptions = subscription_repo.list_subscription_records(
        org_id=normalized_org_id,
        event_type=normalized_event_type,
        enabled_only=True,
        limit=100,
    )
    if not subscriptions:
        return {"delivered_count": 0, "subscription_count": 0}

    delivery_id = secrets.token_hex(16)
    occurred_at = _utcnow_iso()
    envelope = {
        "event_type": normalized_event_type,
        "delivery_id": delivery_id,
        "occurred_at": occurred_at,
        "payload": payload,
    }
    body = json.dumps(envelope, ensure_ascii=False, sort_keys=True).encode("utf-8")
    delivered_count = 0

    for subscription in subscriptions:
        headers = {
            "Content-Type": "application/json",
            "X-AD-Org-Sync-Event": normalized_event_type,
            "X-AD-Org-Sync-Delivery": delivery_id,
        }
        if subscription.secret:
            headers["X-AD-Org-Sync-Signature"] = _build_webhook_signature(subscription.secret, body)
        try:
            response = requests.post(subscription.target_url, data=body, headers=headers, timeout=10)
            status_text = str(getattr(response, "status_code", "") or "").strip() or "unknown"
            reason_text = str(getattr(response, "reason", "") or "").strip()
            if reason_text:
                status_text = f"{status_text} {reason_text}".strip()
            error_text = ""
            if not getattr(response, "ok", False):
                error_text = str(getattr(response, "text", "") or "").strip()[:500]
            else:
                delivered_count += 1
            subscription_repo.record_delivery_result(
                int(subscription.id),
                last_status=status_text,
                last_error=error_text,
                attempted_at=occurred_at,
            )
        except Exception as exc:
            subscription_repo.record_delivery_result(
                int(subscription.id),
                last_status="request_failed",
                last_error=str(exc),
                attempted_at=occurred_at,
            )
    return {"delivered_count": delivered_count, "subscription_count": len(subscriptions)}


def emit_job_lifecycle_events(db_manager: DatabaseManager, *, job_id: str) -> dict[str, Any]:
    job_repo = SyncJobRepository(db_manager)
    review_repo = SyncPlanReviewRepository(db_manager)
    job_record = job_repo.get_job_record(job_id)
    if job_record is None:
        return {"emitted_events": []}

    review_record = review_repo.get_review_record_by_job_id(job_id)
    serialized_job = serialize_job_record(job_record, review_record=review_record)
    payload = {
        "organization": {
            "org_id": job_record.org_id or "default",
        },
        "job": serialized_job,
    }
    emitted_events: list[str] = []
    status = _normalize_job_status(job_record.status)
    if status in {"COMPLETED", "COMPLETED_WITH_ERRORS"}:
        emit_integration_event(
            db_manager,
            org_id=job_record.org_id,
            event_type="job.completed",
            payload=payload,
        )
        emitted_events.append("job.completed")
    elif status == "FAILED":
        emit_integration_event(
            db_manager,
            org_id=job_record.org_id,
            event_type="job.failed",
            payload=payload,
        )
        emitted_events.append("job.failed")

    if bool(serialized_job.get("review_required")):
        emit_integration_event(
            db_manager,
            org_id=job_record.org_id,
            event_type="job.review_required",
            payload=payload,
        )
        emitted_events.append("job.review_required")
    return {"emitted_events": emitted_events}


def approve_job_review(
    db_manager: DatabaseManager,
    *,
    org_id: str,
    job_id: str,
    reviewer_username: str,
    review_notes: str = "",
) -> dict[str, Any]:
    normalized_org_id = normalize_org_id(org_id, fallback="default") or "default"
    job_repo = SyncJobRepository(db_manager)
    review_repo = SyncPlanReviewRepository(db_manager)
    settings_repo = SettingsRepository(db_manager)
    replay_request_repo = SyncReplayRequestRepository(db_manager)

    job_record = job_repo.get_job_record(job_id)
    if job_record is None:
        raise ValueError("Job not found")
    if (job_record.org_id or "default") != normalized_org_id:
        raise ValueError("Job does not belong to the current organization")

    review_record = review_repo.get_review_record_by_job_id(job_id)
    if review_record is None:
        raise ValueError("This job does not have a pending high-risk review")

    review_was_already_approved = str(getattr(review_record, "status", "") or "").strip().lower() == "approved"
    replay_request_id: int | None = None
    if review_was_already_approved:
        updated_review = review_record
        expires_at_iso = str(getattr(review_record, "expires_at", "") or "")
    else:
        review_ttl_minutes = max(settings_repo.get_int("high_risk_review_ttl_minutes", 240), 1)
        expires_at = datetime.now(timezone.utc).timestamp() + review_ttl_minutes * 60
        expires_at_iso = datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat(timespec="seconds")
        review_repo.approve_review(
            job_id,
            reviewer_username=str(reviewer_username or "").strip(),
            review_notes=str(review_notes or "").strip(),
            expires_at=expires_at_iso,
        )
        updated_review = review_repo.get_review_record_by_job_id(job_id)
        if settings_repo.get_bool("automatic_replay_enabled", False, org_id=normalized_org_id):
            replay_request_id = replay_request_repo.enqueue_request(
                request_type="plan_approval",
                execution_mode="apply",
                requested_by=str(reviewer_username or "").strip(),
                org_id=normalized_org_id,
                target_scope="job",
                target_id=job_id,
                trigger_reason="high_risk_plan_approved",
                payload={"expires_at": expires_at_iso},
            )
        emit_integration_event(
            db_manager,
            org_id=normalized_org_id,
            event_type="review.approved",
            payload={
                "organization": {"org_id": normalized_org_id},
                "job": serialize_job_record(job_record, review_record=updated_review),
                "review": _serialize_review_record(updated_review),
                "replay_request_id": replay_request_id,
                "approved_by": str(reviewer_username or "").strip(),
            },
        )

    return {
        "job": job_record,
        "review": updated_review,
        "expires_at_iso": expires_at_iso,
        "replay_request_id": replay_request_id,
        "fresh_approval": not review_was_already_approved,
    }


def serialize_job_records(job_records: Iterable[Any], review_repo: SyncPlanReviewRepository) -> list[dict[str, Any]]:
    return [
        serialize_job_record(job_record, review_record=review_repo.get_review_record_by_job_id(job_record.job_id))
        for job_record in job_records
    ]


def organization_exists(db_manager: DatabaseManager, org_id: str) -> bool:
    organization_repo = OrganizationRepository(db_manager)
    organization = organization_repo.get_organization_record(org_id)
    return bool(organization and organization.is_enabled)
