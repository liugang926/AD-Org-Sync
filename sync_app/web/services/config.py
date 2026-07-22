from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

from sync_app.services.config_release import (
    build_config_release_center_data,
    build_config_release_snapshot_title,
    publish_current_config_release_snapshot,
    rollback_config_release_snapshot,
)
from sync_app.services.high_risk_operations import (
    HighRiskOperationContext,
    high_risk_audit_payload,
)
from sync_app.storage.local_db import (
    ConfigReleaseSnapshotRepository,
    DatabaseManager,
    SettingsRepository,
    WebAuditLogRepository,
)
from sync_app.web.runtime import resolve_web_runtime_settings, web_runtime_requires_restart


@dataclass(slots=True)
class WebConfigService:
    db_manager: DatabaseManager
    settings_repo: SettingsRepository
    config_release_snapshot_repo: ConfigReleaseSnapshotRepository
    audit_repo: WebAuditLogRepository
    readiness_service: Any | None = None

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
        current_org: Any | None = None,
        config_path: str = "config.ini",
    ) -> dict[str, Any]:
        if self.readiness_service is not None and current_org is not None:
            readiness = self.readiness_service.evaluate_organization(
                organization=current_org,
                config_path=config_path,
            )
            required_keys = {
                "source_connector_ready",
                "ad_connector_ready",
                "source_snapshot_current",
                "ad_snapshot_current",
                "data_quality_reviewed",
                "identity_rules_configured",
                "identity_match_run_current",
                "identity_blockers_resolved",
                "account_takeover_resolved",
                "account_naming_configured",
                "field_authority_configured",
                "attribute_mappings_configured",
                "department_ou_routing_configured",
                "lifecycle_safety_configured",
                "sync_scope_current",
            }
            blocking_steps = [
                step
                for step in readiness.steps
                if step.key in required_keys
                and step.whether_required
                and step.status != "complete"
            ]
            if blocking_steps:
                blocker = blocking_steps[0]
                raise ValueError(
                    "Policy release blocked: "
                    + (blocker.blocker_reason or blocker.summary)
                )
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

    def record_high_risk_rollback_audit(
        self,
        *,
        org_id: str,
        actor_username: str,
        snapshot_id: int,
        context: HighRiskOperationContext,
        result: str,
        reason_code: str = "",
    ) -> None:
        normalized_result = str(result or "blocked").strip().lower()
        self.audit_repo.add_log(
            org_id=org_id,
            actor_username=actor_username,
            action_type=(
                "high_risk.config_rollback.execute"
                if normalized_result == "success"
                else "high_risk.config_rollback.blocked"
            ),
            target_type="config_release_snapshot",
            target_id=str(snapshot_id),
            result=normalized_result,
            message=(
                "Configuration rollback passed high-risk validation"
                if normalized_result == "success"
                else "Configuration rollback was blocked by high-risk validation"
            ),
            payload=high_risk_audit_payload(
                context,
                **({"reason_code": reason_code} if reason_code else {}),
            ),
        )

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
