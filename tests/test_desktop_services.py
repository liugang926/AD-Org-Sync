import configparser
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from sync_app.storage.local_db import DatabaseManager
from sync_app.ui.desktop_services import (
    DesktopConfigService,
    DesktopConfigValues,
    DesktopLocalStrategyService,
    DesktopLocalStrategyValues,
)


class DesktopConfigServiceTests(unittest.TestCase):
    def test_ensure_config_file_writes_default_once(self):
        with TemporaryDirectory() as temp_dir:
            service = DesktopConfigService(temp_dir)

            self.assertTrue(service.ensure_config_file())
            self.assertTrue(Path(service.config_path).exists())
            self.assertFalse(service.ensure_config_file())

    def test_save_preserves_hidden_values_and_load_round_trips_visible_form_data(self):
        with TemporaryDirectory() as temp_dir:
            service = DesktopConfigService(temp_dir)
            service.ensure_config_file()

            parser = configparser.ConfigParser()
            parser.read(service.config_path, encoding="utf-8")
            parser.set("SourceConnector", "AgentID", "agent-42")
            parser.set("Source", "Provider", "wecom")
            with open(service.config_path, "w", encoding="utf-8") as config_file:
                parser.write(config_file)

            service.save(
                DesktopConfigValues(
                    corp_id="corp-id",
                    corp_secret="corp-secret",
                    webhook_url="https://hooks.example.com",
                    ldap_server="dc01.example.local",
                    ldap_domain="example.local",
                    ldap_username="EXAMPLE\\sync",
                    ldap_password="secret",
                    ldap_use_ssl=False,
                    ldap_port=389,
                    schedule_time="08:30",
                    retry_interval=45,
                    max_retries=5,
                )
            )

            saved = configparser.ConfigParser()
            saved.read(service.config_path, encoding="utf-8")
            loaded = service.load()

            self.assertEqual(saved.get("SourceConnector", "AgentID"), "agent-42")
            self.assertEqual(saved.get("Source", "Provider"), "wecom")
            self.assertEqual(loaded.corp_id, "corp-id")
            self.assertEqual(loaded.ldap_domain, "example.local")
            self.assertFalse(loaded.ldap_use_ssl)
            self.assertEqual(loaded.ldap_port, 389)
            self.assertEqual(loaded.schedule_time, "08:30")
            self.assertEqual(loaded.retry_interval, 45)
            self.assertEqual(loaded.max_retries, 5)


class DesktopLocalStrategyServiceTests(unittest.TestCase):
    def test_initialize_save_and_runtime_operations_round_trip(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "desktop.db"
            service = DesktopLocalStrategyService(
                db_factory=lambda: DatabaseManager(db_path=str(db_path))
            )

            state = service.initialize()
            self.assertIsNone(state.error)

            service.save(
                state,
                DesktopLocalStrategyValues(
                    group_display_separator="_",
                    group_recursive_enabled=False,
                    managed_relation_cleanup_enabled=True,
                    schedule_execution_mode="dry_run",
                    soft_excluded_rules=[
                        {
                            "match_value": "CN=Ignore Me,OU=Groups,DC=example,DC=local",
                            "display_name": "Ignore Me",
                            "is_enabled": True,
                            "source": "user_ui",
                        }
                    ],
                ),
            )

            loaded = service.load(state)
            summary = service.build_summary(state)
            integrity_result = service.run_integrity_check(state)
            backup_path = service.create_backup(state)

            self.assertEqual(loaded.group_display_separator, "_")
            self.assertFalse(loaded.group_recursive_enabled)
            self.assertTrue(loaded.managed_relation_cleanup_enabled)
            self.assertEqual(loaded.schedule_execution_mode, "dry_run")
            self.assertTrue(
                any(rule["match_value"] == "CN=Ignore Me,OU=Groups,DC=example,DC=local" for rule in loaded.soft_excluded_rules)
            )
            self.assertIn("软排除组", summary)
            self.assertTrue(integrity_result["ok"])
            self.assertTrue(Path(backup_path).exists())


if __name__ == "__main__":
    unittest.main()
