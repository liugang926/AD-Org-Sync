import logging
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sync_app.core.models import AccountConfig, AppConfig, DepartmentNode, LDAPConfig, SourceDirectoryUser, WeComConfig
from sync_app.services import runtime
from sync_app.services.source_directory import SourceDirectoryService
from sync_app.storage.local_db import (
    DatabaseManager,
    SettingsRepository,
    SyncJobRepository,
    WebAuditLogRepository,
)
from sync_app.storage.repositories import SourceDirectoryRepository, SyncPlanReviewRepository
from tests.helpers.runtime_fakes import FakeADSyncApply, FakeWeComAPI


class _SnapshotProvider:
    def list_departments(self):
        return [DepartmentNode(1, "HQ", 0)]
    def list_department_users(self, _department_id):
        return [
            SourceDirectoryUser.from_source_payload({"userid": "alice", "name": "Alice", "employee_id": "E001", "email": "alice@example.com", "department": [1]}),
            SourceDirectoryUser.from_source_payload({"userid": "bob", "name": "Bob", "employee_id": "E002", "email": "bob@example.com", "department": [1]}),
        ]
    def get_user_detail(self, user_id):
        return {"userid": user_id, "department": [1]}


class _RecordingAD(FakeADSyncApply):
    disabled_users = []
    def get_all_enabled_users(self):
        return ["bob"]
    def disable_user(self, username):
        type(self).disabled_users.append(username)
        return True


class ScopedSnapshotRuntimeTests(unittest.TestCase):
    def test_read_only_directory_mode_blocks_apply_before_constructing_ad_client(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="svc",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )
        db_path = os.path.join("test_artifacts", "runtime_read_only_gate.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass
        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        SettingsRepository(manager).set_value(
            "ad_directory_mode", "read_only", "string", org_id="default"
        )

        with (
            patch.dict(os.environ, {"AD_ORG_SYNC_ENVIRONMENT_LABEL": "test"}),
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "ADSyncLDAPS") as ad_client,
            patch.object(
                runtime.sync_logging,
                "setup_logging",
                return_value=logging.getLogger("read-only-runtime"),
            ),
            patch.object(
                runtime.sync_logging,
                "log_filename",
                "read-only-runtime.log",
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "read-only mode"):
                runtime.run_sync_job(
                    execution_mode="apply",
                    trigger_type="unit_test",
                    db_path=db_path,
                    config_path="ignored.ini",
                    requested_by="executor1",
                )

        ad_client.assert_not_called()
        blocked_log = next(
            item
            for item in WebAuditLogRepository(manager).list_recent_logs(10)
            if item.action_type == "high_risk.apply.blocked"
        )
        self.assertEqual(
            blocked_log.payload["reason_code"],
            "ad_directory_mode_read_only",
        )
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

    def test_direct_apply_rechecks_environment_before_constructing_ad_client(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="svc",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )
        db_path = os.path.join("test_artifacts", "runtime_unlabeled_gate.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass
        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        SettingsRepository(manager).set_value("web_bind_host", "0.0.0.0", "string")

        with (
            patch.dict(os.environ, {"AD_ORG_SYNC_ENVIRONMENT_LABEL": ""}),
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "ADSyncLDAPS") as ad_client,
            patch.object(
                runtime.sync_logging,
                "setup_logging",
                return_value=logging.getLogger("unlabeled-runtime"),
            ),
            patch.object(runtime.sync_logging, "log_filename", "unlabeled-runtime.log"),
        ):
            with self.assertRaisesRegex(RuntimeError, "environment is unlabeled"):
                runtime.run_sync_job(
                    execution_mode="apply",
                    trigger_type="unit_test",
                    db_path=db_path,
                    config_path="ignored.ini",
                    requested_by="admin",
                )

        ad_client.assert_not_called()
        blocked_log = next(
            item
            for item in WebAuditLogRepository(manager).list_recent_logs(10)
            if item.action_type == "high_risk.apply.blocked"
        )
        self.assertEqual(blocked_log.result, "blocked")
        self.assertEqual(
            blocked_log.payload["reason_code"],
            "high_risk.blocker.environment_unlabeled",
        )
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

    def test_selected_users_requires_matching_dry_run_and_never_disables_unselected_users(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(server="ldap.example.com", domain="example.com", username="svc", password="password", use_ssl=True, port=636),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )
        db_path = os.path.join("test_artifacts", "runtime_scoped_snapshot.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass
        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        repo = SourceDirectoryRepository(manager)
        SourceDirectoryService(repo).refresh(org_id="default", provider_id="wecom", provider=_SnapshotProvider())
        repo.save_scope_selection(
            org_id="default", provider_id="wecom", scope_type="selected_users",
            selected_source_user_ids=["alice"], username_strategy="employee_id",
            source_field="employee_id", requested_by="admin",
        )
        _RecordingAD.disabled_users = []
        patches = (
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComAPI),
            patch.object(runtime, "ADSyncLDAPS", _RecordingAD),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("scoped-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "scoped-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip.csv"),
        )
        for item in patches:
            item.start()
        try:
            dry_run = runtime.run_sync_job(execution_mode="dry_run", trigger_type="unit_test", db_path=db_path, config_path="ignored.ini")
            self.assertEqual(dry_run["total_users"], 1)
            self.assertTrue(dry_run["summary"]["review_required"])
            review_repo = SyncPlanReviewRepository(manager)
            self.assertTrue(review_repo.approve_review(dry_run["job_id"], reviewer_username="admin"))
            apply_result = runtime.run_sync_job(execution_mode="apply", trigger_type="unit_test", db_path=db_path, config_path="ignored.ini")
            self.assertEqual(apply_result["total_users"], 1)
            self.assertEqual(_RecordingAD.disabled_users, [])
            self.assertEqual(apply_result["scope_type"], "selected_users")
            self.assertEqual(apply_result["plan_source_job_id"], dry_run["job_id"])
        finally:
            for item in reversed(patches):
                item.stop()
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.remove(db_path + suffix)
                except FileNotFoundError:
                    pass

    def test_selected_apply_rechecks_config_before_constructing_ad_client(self):
        config = AppConfig(
            wecom=WeComConfig(
                corpid="corp",
                corpsecret="secret",
                agentid="1001",
            ),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="svc",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )
        db_path = os.path.join(
            "test_artifacts",
            "runtime_config_fingerprint_gate.db",
        )
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass
        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        source_repo = SourceDirectoryRepository(manager)
        SourceDirectoryService(source_repo).refresh(
            org_id="default",
            provider_id="wecom",
            provider=_SnapshotProvider(),
        )
        source_repo.save_scope_selection(
            org_id="default",
            provider_id="wecom",
            scope_type="full",
            requested_by="admin",
        )
        dry_run_patches = (
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(
                runtime,
                "test_source_connection",
                return_value=(True, "ok"),
            ),
            patch.object(
                runtime,
                "test_ldap_connection",
                return_value=(True, "ok"),
            ),
            patch.object(
                runtime,
                "run_config_security_self_check",
                return_value=[],
            ),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComAPI),
            patch.object(runtime, "ADSyncLDAPS", _RecordingAD),
            patch.object(
                runtime.sync_logging,
                "setup_logging",
                return_value=logging.getLogger("config-gate-dry-run"),
            ),
            patch.object(
                runtime.sync_logging,
                "log_filename",
                "config-gate-dry-run.log",
            ),
            patch.object(
                runtime,
                "_generate_skip_detail_report",
                return_value="skip.csv",
            ),
        )
        for item in dry_run_patches:
            item.start()
        try:
            dry_run = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
                requested_by="admin",
            )
        finally:
            for item in reversed(dry_run_patches):
                item.stop()

        review_repo = SyncPlanReviewRepository(manager)
        self.assertTrue(
            review_repo.approve_review(
                dry_run["job_id"],
                reviewer_username="admin",
                expires_at=(
                    datetime.now(timezone.utc) + timedelta(hours=1)
                ).isoformat(timespec="seconds"),
            )
        )
        SettingsRepository(manager).set_value(
            "group_display_separator",
            "/",
            "string",
            org_id="default",
        )
        SyncJobRepository(manager).create_job(
            "apply-config-changed",
            trigger_type="unit_test",
            execution_mode="apply",
            status="QUEUED",
            org_id="default",
            plan_source_job_id=dry_run["job_id"],
        )

        with (
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "ADSyncLDAPS") as ad_client,
            patch.object(
                runtime.sync_logging,
                "setup_logging",
                return_value=logging.getLogger("config-gate-apply"),
            ),
            patch.object(
                runtime.sync_logging,
                "log_filename",
                "config-gate-apply.log",
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "execution.blocker.config_changed",
            ):
                runtime.run_sync_job(
                    execution_mode="apply",
                    trigger_type="unit_test",
                    db_path=db_path,
                    config_path="ignored.ini",
                    requested_by="admin",
                    job_id="apply-config-changed",
                    active_job_guard_id="apply-config-changed",
                )

        ad_client.assert_not_called()
        blocked_log = next(
            item
            for item in WebAuditLogRepository(manager).list_recent_logs(10)
            if item.action_type == "high_risk.apply.blocked"
        )
        self.assertEqual(
            blocked_log.payload["reason_code"],
            "execution.blocker.config_changed",
        )
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass


if __name__ == "__main__":
    unittest.main()
