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
