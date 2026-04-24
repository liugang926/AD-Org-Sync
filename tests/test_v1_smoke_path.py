import logging
import unittest
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from sync_app.core.models import AccountConfig, AppConfig, LDAPConfig, WeComConfig
from sync_app.services import runtime
from sync_app.services.config_release import (
    publish_current_config_release_snapshot,
    rollback_config_release_snapshot,
)
from sync_app.services.external_integrations import approve_job_review
from sync_app.storage.local_db import (
    ConfigReleaseSnapshotRepository,
    DatabaseManager,
    OrganizationConfigRepository,
    OrganizationRepository,
    SettingsRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
    SyncReplayRequestRepository,
    UserIdentityBindingRepository,
)
from tests.helpers.runtime_fakes import FakeADSyncPolicy, FakeWeChatBot, FakeWeComProgrammableAPI


class V1SmokePathTests(unittest.TestCase):
    def _org_config_values(self, *, ldap_server: str = "dc01.example.local") -> dict:
        return {
            "corpid": "corp-001",
            "agentid": "10001",
            "corpsecret": "secret-001",
            "webhook_url": "https://example.invalid/webhook",
            "ldap_server": ldap_server,
            "ldap_domain": "example.com",
            "ldap_username": "EXAMPLE\\administrator",
            "ldap_password": "Password123!",
            "ldap_use_ssl": True,
            "ldap_port": 636,
            "ldap_validate_cert": True,
            "ldap_ca_cert_path": "",
            "default_password": "VeryStrong123!456",
            "force_change_password": True,
            "password_complexity": "strong",
        }

    def _build_db(self, temp_dir: str) -> tuple[DatabaseManager, str, str]:
        db_path = str((Path(temp_dir) / "v1_smoke.db").resolve())
        config_path = str((Path(temp_dir) / "default.ini").resolve())
        db_manager = DatabaseManager(db_path=db_path)
        db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        OrganizationRepository(db_manager).ensure_default(config_path=config_path)
        OrganizationConfigRepository(db_manager).save_config(
            "default",
            self._org_config_values(),
            config_path=config_path,
        )
        settings_repo = SettingsRepository(db_manager)
        settings_repo.set_value("group_display_separator", "-", "string", org_id="default")
        settings_repo.set_value("automatic_replay_enabled", "true", "bool", org_id="default")
        settings_repo.set_value("high_risk_review_ttl_minutes", "240", "int")
        return db_manager, db_path, config_path

    def _build_runtime_config(self, config_path: str) -> AppConfig:
        return AppConfig(
            wecom=WeComConfig(corpid="corp-001", corpsecret="secret-001", agentid="10001"),
            ldap=LDAPConfig(
                server="dc01.example.local",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="Password123!",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            webhook_url="https://example.invalid/webhook",
            config_path=config_path,
        )

    def _run_job(
        self,
        *,
        config: AppConfig,
        db_path: str,
        config_path: str,
        execution_mode: str,
    ) -> dict:
        patches = [
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime, "WebhookNotificationClient", FakeWeChatBot),
            patch.object(
                runtime.sync_logging,
                "setup_logging",
                return_value=logging.getLogger("test-v1-smoke"),
            ),
            patch.object(runtime.sync_logging, "log_filename", "test-v1-smoke.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]
        with ExitStack() as stack:
            for runtime_patch in patches:
                stack.enter_context(runtime_patch)
            return runtime.run_sync_job(
                execution_mode=execution_mode,
                trigger_type="unit_test_smoke",
                requested_by="tester",
                db_path=db_path,
                config_path=config_path,
                org_id="default",
            )

    def test_v1_smoke_path_covers_release_review_apply_replay_and_rollback(self):
        with TemporaryDirectory() as temp_dir:
            db_manager, db_path, config_path = self._build_db(temp_dir)
            config = self._build_runtime_config(config_path)
            org_config_repo = OrganizationConfigRepository(db_manager)
            review_repo = SyncPlanReviewRepository(db_manager)
            replay_repo = SyncReplayRequestRepository(db_manager)
            snapshot_repo = ConfigReleaseSnapshotRepository(db_manager)
            job_repo = SyncJobRepository(db_manager)

            baseline_publish = publish_current_config_release_snapshot(
                db_manager,
                "default",
                created_by="tester",
                snapshot_name="Smoke Baseline",
            )
            self.assertTrue(baseline_publish["created"])
            self.assertIsNotNone(baseline_publish["snapshot"])

            UserIdentityBindingRepository(db_manager).upsert_binding(
                "bob.wecom",
                "bob",
                source="manual",
                notes="managed disable candidate",
                preserve_manual=False,
            )

            FakeWeComProgrammableAPI.reset()
            FakeADSyncPolicy.reset()
            FakeWeChatBot.reset()
            FakeADSyncPolicy.enabled_users_by_domain = {"example.com": ["bob"]}

            dry_run_result = self._run_job(
                config=config,
                db_path=db_path,
                config_path=config_path,
                execution_mode="dry_run",
            )

            self.assertEqual(dry_run_result["error_count"], 0)
            self.assertTrue(dry_run_result["summary"]["review_required"])
            self.assertGreaterEqual(dry_run_result["high_risk_operation_count"], 1)

            review_record = review_repo.get_review_record_by_job_id(dry_run_result["job_id"])
            self.assertIsNotNone(review_record)
            self.assertEqual(review_record.status, "pending")

            blocked_apply_result = self._run_job(
                config=config,
                db_path=db_path,
                config_path=config_path,
                execution_mode="apply",
            )
            self.assertTrue(blocked_apply_result["summary"]["review_required"])

            approval_result = approve_job_review(
                db_manager,
                org_id="default",
                job_id=dry_run_result["job_id"],
                reviewer_username="tester",
                review_notes="approved in smoke path",
            )
            self.assertTrue(approval_result["fresh_approval"])
            self.assertIsNotNone(approval_result["replay_request_id"])

            pending_replay = replay_repo.get_request_record(int(approval_result["replay_request_id"]))
            self.assertIsNotNone(pending_replay)
            self.assertEqual(pending_replay.status, "pending")

            apply_result = self._run_job(
                config=config,
                db_path=db_path,
                config_path=config_path,
                execution_mode="apply",
            )

            self.assertEqual(apply_result["error_count"], 0)
            self.assertFalse(apply_result["summary"]["review_required"])
            self.assertGreater(apply_result["executed_operation_count"], 0)
            self.assertEqual(apply_result["summary"]["automatic_replay_request_count"], 1)
            self.assertEqual(
                FakeADSyncPolicy.disabled_users,
                [{"domain": "example.com", "username": "bob"}],
            )

            completed_replay = replay_repo.get_request_record(int(approval_result["replay_request_id"]))
            self.assertIsNotNone(completed_replay)
            self.assertEqual(completed_replay.status, "completed")
            self.assertEqual(completed_replay.last_job_id, apply_result["job_id"])

            latest_apply_job = job_repo.get_job_record(apply_result["job_id"])
            self.assertIsNotNone(latest_apply_job)
            self.assertEqual(latest_apply_job.status, "COMPLETED")

            org_config_repo.save_config(
                "default",
                self._org_config_values(ldap_server="dc02.example.local"),
                config_path=config_path,
            )
            changed_publish = publish_current_config_release_snapshot(
                db_manager,
                "default",
                created_by="tester",
                snapshot_name="Smoke Changed Config",
            )
            self.assertTrue(changed_publish["created"])
            self.assertNotEqual(
                changed_publish["snapshot"].id,
                baseline_publish["snapshot"].id,
            )

            rollback_result = rollback_config_release_snapshot(
                db_manager,
                baseline_publish["snapshot"].id,
                org_id="default",
                created_by="tester",
            )
            restored_config = org_config_repo.get_editable_config("default", config_path=config_path)
            trigger_actions = [
                record.trigger_action
                for record in snapshot_repo.list_snapshot_records(org_id="default", limit=10)
            ]

            self.assertEqual(restored_config["ldap_server"], "dc01.example.local")
            self.assertIsNotNone(rollback_result["safety_snapshot"])
            self.assertEqual(
                rollback_result["safety_snapshot"].id,
                changed_publish["snapshot"].id,
            )
            self.assertEqual(rollback_result["rollback_snapshot"].trigger_action, "rollback")
            self.assertIn("rollback", trigger_actions)


if __name__ == "__main__":
    unittest.main()
