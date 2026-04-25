from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from sync_app.core.conflict_recommendations import (
    recommend_conflict_resolution,
    recommendation_requires_confirmation,
)
from sync_app.storage.local_db import SyncConflictRepository, WebAuditLogRepository


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
