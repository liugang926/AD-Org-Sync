import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from sync_app.services.config_release import (
    publish_current_config_release_snapshot,
    rollback_config_release_snapshot,
)
from sync_app.storage.local_db import (
    ConfigReleaseSnapshotRepository,
    DatabaseManager,
    OrganizationConfigRepository,
    OrganizationRepository,
    SettingsRepository,
)


class ConfigReleaseTests(unittest.TestCase):
    def _build_db(self, temp_dir: str) -> tuple[DatabaseManager, str]:
        db_path = Path(temp_dir) / "config_release.db"
        config_path = str((Path(temp_dir) / "config_release.ini").resolve())
        db_manager = DatabaseManager(db_path=str(db_path))
        db_manager.initialize()

        OrganizationRepository(db_manager).ensure_default(config_path=config_path)
        OrganizationConfigRepository(db_manager).save_config(
            "default",
            {
                "corpid": "corp-001",
                "agentid": "10001",
                "corpsecret": "secret-001",
                "webhook_url": "https://example.invalid/webhook",
                "ldap_server": "dc01.example.local",
                "ldap_domain": "example.local",
                "ldap_username": "administrator",
                "ldap_password": "Password123!",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": True,
                "ldap_ca_cert_path": "",
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
            },
            config_path=config_path,
        )
        SettingsRepository(db_manager).set_value(
            "group_display_separator",
            "-",
            "string",
            org_id="default",
        )
        return db_manager, config_path

    def test_publish_snapshot_dedupes_until_live_configuration_changes(self):
        with TemporaryDirectory() as temp_dir:
            db_manager, config_path = self._build_db(temp_dir)
            org_config_repo = OrganizationConfigRepository(db_manager)
            snapshot_repo = ConfigReleaseSnapshotRepository(db_manager)

            first_result = publish_current_config_release_snapshot(
                db_manager,
                "default",
                created_by="tester",
                snapshot_name="Initial Baseline",
            )
            duplicate_result = publish_current_config_release_snapshot(
                db_manager,
                "default",
                created_by="tester",
            )
            org_config_repo.save_config(
                "default",
                {
                    "corpid": "corp-001",
                    "agentid": "10001",
                    "corpsecret": "secret-001",
                    "webhook_url": "https://example.invalid/webhook",
                    "ldap_server": "dc02.example.local",
                    "ldap_domain": "example.local",
                    "ldap_username": "administrator",
                    "ldap_password": "Password123!",
                    "ldap_use_ssl": True,
                    "ldap_port": 636,
                    "ldap_validate_cert": True,
                    "ldap_ca_cert_path": "",
                    "default_password": "ChangeMe123!",
                    "force_change_password": True,
                    "password_complexity": "strong",
                },
                config_path=config_path,
            )
            changed_result = publish_current_config_release_snapshot(
                db_manager,
                "default",
                created_by="tester",
                snapshot_name="LDAP Rotation",
            )
            snapshot_count = len(snapshot_repo.list_snapshot_records(org_id="default"))

        self.assertTrue(first_result["created"])
        self.assertFalse(duplicate_result["created"])
        self.assertIsNotNone(first_result["snapshot"])
        self.assertEqual(duplicate_result["snapshot"].id, first_result["snapshot"].id)
        self.assertTrue(changed_result["created"])
        self.assertNotEqual(changed_result["snapshot"].id, first_result["snapshot"].id)
        self.assertTrue(changed_result["diff"]["changed"])
        self.assertGreater(changed_result["diff"]["changed_item_count"], 0)
        self.assertTrue(
            any(group["title"] == "Base Configuration" for group in changed_result["diff"]["groups"])
        )
        self.assertEqual(snapshot_count, 2)

    def test_rollback_restores_snapshot_and_records_safety_backup(self):
        with TemporaryDirectory() as temp_dir:
            db_manager, config_path = self._build_db(temp_dir)
            org_config_repo = OrganizationConfigRepository(db_manager)
            snapshot_repo = ConfigReleaseSnapshotRepository(db_manager)

            published_result = publish_current_config_release_snapshot(
                db_manager,
                "default",
                created_by="tester",
                snapshot_name="Known Good",
            )
            org_config_repo.save_config(
                "default",
                {
                    "corpid": "corp-rollback",
                    "agentid": "10001",
                    "corpsecret": "secret-002",
                    "webhook_url": "https://example.invalid/webhook/rollback",
                    "ldap_server": "dc99.example.local",
                    "ldap_domain": "example.local",
                    "ldap_username": "administrator",
                    "ldap_password": "Password123!",
                    "ldap_use_ssl": True,
                    "ldap_port": 636,
                    "ldap_validate_cert": True,
                    "ldap_ca_cert_path": "",
                    "default_password": "ChangeMe123!",
                    "force_change_password": True,
                    "password_complexity": "strong",
                },
                config_path=config_path,
            )

            rollback_result = rollback_config_release_snapshot(
                db_manager,
                published_result["snapshot"].id,
                org_id="default",
                created_by="tester",
            )
            restored_config = org_config_repo.get_editable_config("default", config_path=config_path)
            trigger_actions = [
                record.trigger_action
                for record in snapshot_repo.list_snapshot_records(org_id="default", limit=10)
            ]

        self.assertEqual(restored_config["corpid"], "corp-001")
        self.assertEqual(restored_config["ldap_server"], "dc01.example.local")
        self.assertIsNotNone(rollback_result["safety_snapshot"])
        self.assertEqual(rollback_result["rollback_snapshot"].trigger_action, "rollback")
        self.assertIn("rollback_safety", trigger_actions)
        self.assertIn("rollback", trigger_actions)


if __name__ == "__main__":
    unittest.main()
