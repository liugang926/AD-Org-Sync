from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from sync_app.core.conflict_recommendations import (
    recommend_conflict_resolution,
    recommendation_requires_confirmation,
)
from sync_app.services.config_release import (
    build_config_release_center_data,
    build_config_release_snapshot_title,
    publish_current_config_release_snapshot,
    rollback_config_release_snapshot,
)
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
from sync_app.services.job_diff import build_job_comparison_summary
from sync_app.storage.local_db import (
    ConfigReleaseSnapshotRepository,
    DatabaseManager,
    IntegrationWebhookSubscriptionRepository,
    PlannedOperationRepository,
    SettingsRepository,
    SyncConflictRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
    WebAuditLogRepository,
)
from sync_app.web.runtime import resolve_web_runtime_settings, web_runtime_requires_restart


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
            and cls._normalize_job_status(getattr(job, "status", "")) in {"COMPLETED", "COMPLETED_WITH_ERRORS"}
        )

    @classmethod
    def _is_successful_apply(cls, job: Any) -> bool:
        return (
            str(getattr(job, "execution_mode", "") or "").strip().lower() == "apply"
            and cls._normalize_job_status(getattr(job, "status", "")) in {"COMPLETED", "COMPLETED_WITH_ERRORS"}
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


@dataclass(slots=True)
class WebConflictService:
    conflict_repo: SyncConflictRepository
    audit_repo: WebAuditLogRepository
    recommend_conflict_resolution_fn: Callable[[Any], Optional[dict[str, Any]]] = recommend_conflict_resolution

    def list_conflicts_page(
        self,
        *,
        org_id: str,
        limit: int,
        offset: int,
        job_id: str | None = None,
        status: str | None = None,
        query: str = "",
    ) -> tuple[list[Any], dict[str, Any]]:
        return self.conflict_repo.list_conflict_records_page(
            limit=limit,
            offset=offset,
            job_id=job_id,
            status=status,
            query=query,
            org_id=org_id,
        )

    def get_conflict_record(self, conflict_id: int, *, org_id: str) -> Any | None:
        return self.conflict_repo.get_conflict_record(conflict_id, org_id=org_id)

    def build_recommendations(self, conflicts: list[Any]) -> dict[int, Optional[dict[str, Any]]]:
        return {
            item.id: self.recommend_conflict_resolution_fn(item)
            for item in conflicts
        }

    def bulk_apply_requires_confirmation(self, *, org_id: str, conflict_ids: list[int]) -> bool:
        for conflict_id in conflict_ids:
            conflict = self.get_conflict_record(conflict_id, org_id=org_id)
            if not conflict or conflict.status != "open":
                continue
            if recommendation_requires_confirmation(self.recommend_conflict_resolution_fn(conflict)):
                return True
        return False

    def resolve_manual_binding(
        self,
        *,
        app: Any,
        conflict: Any,
        org_id: str,
        actor_username: str,
        ad_username: str,
        apply_conflict_manual_binding: Callable[..., tuple[bool, str, int]],
        audit_action: bool = True,
    ) -> tuple[bool, str, int]:
        ok, normalized_ad_username, resolved_count = apply_conflict_manual_binding(
            app=app,
            conflict=conflict,
            ad_username=ad_username,
            actor_username=actor_username,
            org_id=org_id,
        )
        if ok and audit_action:
            self.audit_repo.add_log(
                org_id=org_id,
                actor_username=actor_username,
                action_type="conflict.resolve_manual_binding",
                target_type="sync_conflict",
                target_id=str(conflict.id),
                result="success",
                message="Resolved conflict by creating manual binding",
                payload={
                    "job_id": conflict.job_id,
                    "source_user_id": conflict.source_id,
                    "ad_username": normalized_ad_username,
                    "resolved_count": resolved_count,
                },
            )
        return ok, normalized_ad_username, resolved_count

    def resolve_skip_user_sync(
        self,
        *,
        app: Any,
        conflict: Any,
        org_id: str,
        actor_username: str,
        notes: str,
        apply_conflict_skip_user_sync: Callable[..., tuple[bool, str, int]],
        audit_action: bool = True,
    ) -> tuple[bool, str, int]:
        ok, rule_notes, resolved_count = apply_conflict_skip_user_sync(
            app=app,
            conflict=conflict,
            actor_username=actor_username,
            org_id=org_id,
            notes=notes,
        )
        if ok and audit_action:
            self.audit_repo.add_log(
                org_id=org_id,
                actor_username=actor_username,
                action_type="conflict.resolve_skip_user",
                target_type="sync_conflict",
                target_id=str(conflict.id),
                result="success",
                message="Resolved conflict by adding skip_user_sync exception",
                payload={
                    "job_id": conflict.job_id,
                    "source_user_id": conflict.source_id,
                    "notes": rule_notes,
                    "resolved_count": resolved_count,
                },
            )
        return ok, rule_notes, resolved_count

    def apply_recommendation(
        self,
        *,
        app: Any,
        conflict: Any,
        org_id: str,
        actor_username: str,
        confirmation_reason: str,
        apply_conflict_recommendation: Callable[..., tuple[bool, str, int, dict[str, Any] | None]],
        audit_action: bool = True,
    ) -> tuple[bool, str, int, dict[str, Any] | None]:
        ok, detail, resolved_count, recommendation = apply_conflict_recommendation(
            app=app,
            conflict=conflict,
            actor_username=actor_username,
            org_id=org_id,
            confirmation_reason=confirmation_reason,
        )
        if ok and audit_action:
            self.audit_repo.add_log(
                org_id=org_id,
                actor_username=actor_username,
                action_type="conflict.apply_recommendation",
                target_type="sync_conflict",
                target_id=str(conflict.id),
                result="success",
                message="Applied recommended conflict resolution",
                payload={
                    "job_id": conflict.job_id,
                    "source_user_id": conflict.source_id,
                    "recommendation": recommendation,
                    "detail": detail,
                    "resolved_count": resolved_count,
                },
            )
        return ok, detail, resolved_count, recommendation

    def dismiss_conflict(
        self,
        *,
        conflict: Any,
        org_id: str,
        actor_username: str,
        notes: str,
        bulk: bool = False,
        audit_action: bool = True,
    ) -> None:
        self.conflict_repo.update_conflict_status(
            conflict.id,
            status="dismissed",
            resolution_payload={
                "action": "dismissed",
                "notes": notes,
                "actor_username": actor_username,
                "bulk": bulk,
            },
            resolved_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )
        if audit_action:
            payload = {"job_id": conflict.job_id, "notes": notes}
            if bulk:
                payload["bulk"] = True
            self.audit_repo.add_log(
                org_id=org_id,
                actor_username=actor_username,
                action_type="conflict.dismiss",
                target_type="sync_conflict",
                target_id=str(conflict.id),
                result="success",
                message="Dismissed sync conflict",
                payload=payload,
            )

    def reopen_conflict(
        self,
        *,
        conflict: Any,
        org_id: str,
        actor_username: str,
        bulk: bool = False,
        audit_action: bool = True,
    ) -> None:
        self.conflict_repo.update_conflict_status(
            conflict.id,
            status="open",
            resolution_payload=None,
            resolved_at=None,
        )
        if audit_action:
            payload = {"job_id": conflict.job_id, "previous_status": conflict.status}
            if bulk:
                payload["bulk"] = True
            self.audit_repo.add_log(
                org_id=org_id,
                actor_username=actor_username,
                action_type="conflict.reopen",
                target_type="sync_conflict",
                target_id=str(conflict.id),
                result="success",
                message="Reopened sync conflict",
                payload=payload,
            )

    def execute_bulk_action(
        self,
        *,
        app: Any,
        org_id: str,
        actor_username: str,
        action: str,
        selected_conflict_ids: list[int],
        notes: str,
        apply_conflict_recommendation: Callable[..., tuple[bool, str, int, dict[str, Any] | None]],
        apply_conflict_skip_user_sync: Callable[..., tuple[bool, str, int]],
    ) -> tuple[int, int]:
        updated_count = 0
        skipped_count = 0
        for conflict_id in selected_conflict_ids:
            conflict = self.get_conflict_record(conflict_id, org_id=org_id)
            if not conflict:
                skipped_count += 1
                continue

            if action == "reopen":
                if conflict.status == "open":
                    skipped_count += 1
                    continue
                self.reopen_conflict(
                    conflict=conflict,
                    org_id=org_id,
                    actor_username=actor_username,
                    bulk=True,
                    audit_action=False,
                )
                updated_count += 1
                continue

            if conflict.status != "open":
                skipped_count += 1
                continue

            if action == "dismiss":
                self.dismiss_conflict(
                    conflict=conflict,
                    org_id=org_id,
                    actor_username=actor_username,
                    notes=notes,
                    bulk=True,
                    audit_action=False,
                )
                updated_count += 1
                continue

            if action == "apply_recommendation":
                ok, _detail, resolved_count, _recommendation = self.apply_recommendation(
                    app=app,
                    conflict=conflict,
                    org_id=org_id,
                    actor_username=actor_username,
                    confirmation_reason=notes,
                    apply_conflict_recommendation=apply_conflict_recommendation,
                    audit_action=False,
                )
                if ok and resolved_count:
                    updated_count += 1
                else:
                    skipped_count += 1
                continue

            if action == "skip_user_sync":
                ok, _rule_notes, resolved_count = self.resolve_skip_user_sync(
                    app=app,
                    conflict=conflict,
                    org_id=org_id,
                    actor_username=actor_username,
                    notes=notes or f"bulk resolved from conflict {conflict.id}",
                    apply_conflict_skip_user_sync=apply_conflict_skip_user_sync,
                    audit_action=False,
                )
                if ok and resolved_count:
                    updated_count += 1
                else:
                    skipped_count += 1

        self.audit_repo.add_log(
            org_id=org_id,
            actor_username=actor_username,
            action_type="conflict.bulk_action",
            target_type="sync_conflict",
            target_id="bulk",
            result="success" if updated_count else "warning",
            message="Executed bulk conflict action",
            payload={
                "action": action,
                "selected_count": len(selected_conflict_ids),
                "updated_count": updated_count,
                "skipped_count": skipped_count,
            },
        )
        return updated_count, skipped_count


@dataclass(slots=True)
class WebConfigService:
    db_manager: DatabaseManager
    settings_repo: SettingsRepository
    config_release_snapshot_repo: ConfigReleaseSnapshotRepository
    audit_repo: WebAuditLogRepository

    def build_saved_message(self, *, current_web_runtime_settings: dict[str, Any]) -> str:
        persisted_web_runtime_settings = resolve_web_runtime_settings(self.settings_repo)
        if web_runtime_requires_restart(current_web_runtime_settings, persisted_web_runtime_settings):
            return (
                "Configuration saved. Restart the web process to apply deployment security changes, "
                "then run the first dry run before apply."
            )
        return "Configuration saved. Run the first dry run before the first apply."

    def build_release_center_context(
        self,
        *,
        current_org: Any,
        current_snapshot_id: Optional[int] = None,
        baseline_snapshot_id: Optional[int] = None,
    ) -> dict[str, Any]:
        release_data = build_config_release_center_data(
            self.db_manager,
            current_org.org_id,
            current_snapshot_id=current_snapshot_id,
            baseline_snapshot_id=baseline_snapshot_id,
        )
        latest_snapshot = release_data.get("latest_snapshot")
        selected_current_snapshot = release_data.get("selected_current_snapshot")
        selected_baseline_snapshot = release_data.get("selected_baseline_snapshot")
        return {
            "page": "config",
            "title": "Config Release Center",
            "current_org": current_org,
            "latest_snapshot_title": (
                build_config_release_snapshot_title(latest_snapshot)
                if latest_snapshot is not None
                else ""
            ),
            "selected_current_snapshot_title": (
                build_config_release_snapshot_title(selected_current_snapshot)
                if selected_current_snapshot is not None
                else ""
            ),
            "selected_baseline_snapshot_title": (
                build_config_release_snapshot_title(selected_baseline_snapshot)
                if selected_baseline_snapshot is not None
                else ""
            ),
            **release_data,
        }

    def publish_release_snapshot(
        self,
        *,
        org_id: str,
        actor_username: str,
        snapshot_name: str = "",
    ) -> dict[str, Any]:
        result = publish_current_config_release_snapshot(
            self.db_manager,
            org_id,
            created_by=actor_username,
            snapshot_name=str(snapshot_name or "").strip(),
            trigger_action="manual_release",
            force=False,
        )
        snapshot = result.get("snapshot")
        if result.get("created") and snapshot is not None:
            self.audit_repo.add_log(
                org_id=getattr(snapshot, "org_id", ""),
                actor_username=actor_username,
                action_type="config.release_publish",
                target_type="config_release_snapshot",
                target_id=str(getattr(snapshot, "id", "") or ""),
                result="success",
                message="Published configuration snapshot",
                payload={
                    "snapshot_name": getattr(snapshot, "snapshot_name", ""),
                    "trigger_action": getattr(snapshot, "trigger_action", ""),
                    "bundle_hash": getattr(snapshot, "bundle_hash", ""),
                },
            )
        return result

    def rollback_release_snapshot(
        self,
        *,
        org_id: str,
        actor_username: str,
        snapshot_id: int,
    ) -> dict[str, Any]:
        result = rollback_config_release_snapshot(
            self.db_manager,
            snapshot_id,
            org_id=org_id,
            created_by=actor_username,
        )
        target_snapshot = result.get("target_snapshot")
        rollback_snapshot = result.get("rollback_snapshot")
        self.audit_repo.add_log(
            org_id=getattr(target_snapshot, "org_id", ""),
            actor_username=actor_username,
            action_type="config.release_rollback",
            target_type="config_release_snapshot",
            target_id=str(snapshot_id),
            result="success",
            message="Rolled back configuration snapshot",
            payload={
                "target_snapshot_id": snapshot_id,
                "rollback_snapshot_id": getattr(rollback_snapshot, "id", None),
                "safety_snapshot_id": getattr(result.get("safety_snapshot"), "id", None),
            },
        )
        return result

    def build_release_download(self, *, org_id: str, snapshot_id: int) -> dict[str, Any] | None:
        snapshot = self.config_release_snapshot_repo.get_snapshot_record(
            snapshot_id,
            org_id=org_id,
        )
        if not snapshot or not isinstance(snapshot.bundle, dict):
            return None
        return {
            "filename": f"{snapshot.org_id}-config-release-{snapshot.id}.json",
            "content": json.dumps(snapshot.bundle, ensure_ascii=False, indent=2).encode("utf-8"),
            "media_type": "application/json; charset=utf-8",
        }


@dataclass(slots=True)
class WebServiceState:
    jobs: WebJobService
    conflicts: WebConflictService
    config: WebConfigService
    integrations: "WebIntegrationService"


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


def build_web_service_state(
    *,
    db_manager: DatabaseManager,
    settings_repo: SettingsRepository,
    config_release_snapshot_repo: ConfigReleaseSnapshotRepository,
    subscription_repo: IntegrationWebhookSubscriptionRepository,
    job_repo: SyncJobRepository,
    review_repo: SyncPlanReviewRepository,
    planned_operation_repo: PlannedOperationRepository,
    conflict_repo: SyncConflictRepository,
    audit_repo: WebAuditLogRepository,
) -> WebServiceState:
    return WebServiceState(
        jobs=WebJobService(
            db_manager=db_manager,
            job_repo=job_repo,
            review_repo=review_repo,
            planned_operation_repo=planned_operation_repo,
            conflict_repo=conflict_repo,
            audit_repo=audit_repo,
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
        ),
        integrations=WebIntegrationService(
            db_manager=db_manager,
            settings_repo=settings_repo,
            subscription_repo=subscription_repo,
            job_repo=job_repo,
            review_repo=review_repo,
            conflict_repo=conflict_repo,
            audit_repo=audit_repo,
        ),
    )
