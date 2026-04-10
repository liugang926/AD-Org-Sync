import json
import logging
import os
from datetime import datetime
from typing import Optional

from sync_app.core.common import hash_user_state
from sync_app.core.models import SourceDirectoryUser
from sync_app.core.sync_policies import extract_manager_userids
from sync_app.storage.local_db import DatabaseManager, ObjectStateRepository, SettingsRepository

CANONICAL_SOURCE_STATE_TYPE = "source"
LEGACY_SOURCE_STATE_TYPES = ("wecom",)


class SyncStateManager:
    """SQLite-backed sync state manager."""

    def __init__(
        self,
        state_file: str = "sync_state.json",
        db_manager: Optional[DatabaseManager] = None,
        *,
        org_id: str = "default",
    ):
        self.state_file = state_file
        self.org_id = str(org_id or "").strip().lower() or "default"
        self.logger = logging.getLogger(__name__)
        self.db_manager = db_manager or DatabaseManager()
        self.db_manager.initialize()
        self.settings_repo = SettingsRepository(self.db_manager)
        self.state_repo = ObjectStateRepository(self.db_manager, default_org_id=self.org_id)
        self._migrate_legacy_state()

    def _migrate_legacy_state(self) -> None:
        if self.org_id != "default":
            return
        if not os.path.exists(self.state_file):
            return
        if self._count_user_states() > 0:
            return

        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception as exc:
            self.logger.warning(f"failed to load legacy sync state: {exc}")
            return

        synced_users = state.get("synced_users", {})
        for userid, user_state in synced_users.items():
            self.state_repo.upsert_state(
                source_type=CANONICAL_SOURCE_STATE_TYPE,
                object_type="user",
                source_id=userid,
                source_hash=user_state.get("hash", ""),
                display_name=userid,
                last_action="legacy_migrate",
                last_status="success",
                extra={"legacy_timestamp": user_state.get("timestamp")},
            )

        if state.get("last_sync_time"):
            self.settings_repo.set_value("legacy_last_sync_time", state["last_sync_time"])
        self.settings_repo.set_value(
            "legacy_last_sync_success",
            str(bool(state.get("last_sync_success", False))).lower(),
            value_type="bool",
        )

    def _count_user_states(self) -> int:
        source_count = self.state_repo.count_by_type(CANONICAL_SOURCE_STATE_TYPE, "user")
        if source_count > 0:
            return source_count
        return sum(self.state_repo.count_by_type(source_type, "user") for source_type in LEGACY_SOURCE_STATE_TYPES)

    def get_user_state_record(self, userid: str):
        current_state = self.state_repo.get_state(CANONICAL_SOURCE_STATE_TYPE, "user", userid)
        if current_state:
            return current_state
        for source_type in LEGACY_SOURCE_STATE_TYPES:
            current_state = self.state_repo.get_state(source_type, "user", userid)
            if current_state:
                return current_state
        return None

    def _delete_missing_user_states(self, current_userids: set[str]) -> int:
        removed_count = self.state_repo.delete_missing(CANONICAL_SOURCE_STATE_TYPE, "user", current_userids)
        for source_type in LEGACY_SOURCE_STATE_TYPES:
            removed_count += self.state_repo.delete_missing(source_type, "user", current_userids)
        return removed_count

    def get_last_sync_time(self) -> Optional[str]:
        return self.settings_repo.get_value("last_sync_time", org_id=self.org_id) or self.settings_repo.get_value(
            "legacy_last_sync_time"
        )

    def set_sync_complete(self, success: bool = True) -> None:
        self.settings_repo.set_value("last_sync_time", datetime.now().isoformat(), org_id=self.org_id)
        self.settings_repo.set_value(
            "last_sync_success",
            str(bool(success)).lower(),
            value_type="bool",
            org_id=self.org_id,
        )

    def get_user_hash(self, user_data: dict) -> str:
        return hash_user_state(user_data)

    def is_user_changed(self, userid: str, user_data: dict) -> bool:
        current_hash = self.get_user_hash(user_data)
        current_state = self.get_user_state_record(userid)
        if not current_state:
            return True
        return current_state["source_hash"] != current_hash

    def update_user_state(
        self,
        userid: str,
        user_data: dict,
        job_id: Optional[str] = None,
        target_dn: Optional[str] = None,
    ) -> None:
        manager_userids = extract_manager_userids(
            SourceDirectoryUser(
                userid=str(user_data.get("userid") or userid),
                name=str(user_data.get("name") or userid),
                email=str(user_data.get("email") or ""),
                departments=[int(value) for value in user_data.get("department", []) if value is not None],
                raw_payload=dict(user_data),
            )
        )
        self.state_repo.upsert_state(
            source_type=CANONICAL_SOURCE_STATE_TYPE,
            object_type="user",
            source_id=userid,
            source_hash=self.get_user_hash(user_data),
            display_name=user_data.get("name", userid),
            target_dn=target_dn,
            last_job_id=job_id,
            last_action="sync_user",
            last_status="success",
            extra={
                "department": user_data.get("department", []),
                "email": user_data.get("email", ""),
                "manager_userids": manager_userids,
            },
        )

    def get_synced_user_count(self) -> int:
        return self._count_user_states()

    def cleanup_old_users(self, current_userids: set) -> None:
        removed_count = self._delete_missing_user_states(set(current_userids))
        if removed_count:
            self.logger.info(f"removed {removed_count} stale synced user records")
