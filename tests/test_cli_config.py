import io
import json
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from sync_app import cli
from sync_app.storage.local_db import DatabaseManager, OrganizationConfigRepository, OrganizationRepository


class CliConfigCommandTests(unittest.TestCase):
    def setUp(self):
        test_dir = Path(os.getcwd()) / "test_artifacts"
        test_dir.mkdir(exist_ok=True)
        self.db_path = test_dir / "cli_config_test.db"
        self.bundle_path = test_dir / "cli_bundle.json"
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(str(self.db_path) + suffix)
            except FileNotFoundError:
                pass
        if self.bundle_path.exists():
            self.bundle_path.unlink()
        self.db_manager = DatabaseManager(db_path=str(self.db_path))
        self.db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)

    def tearDown(self):
        if self.bundle_path.exists():
            self.bundle_path.unlink()

    def _run_cli(self, argv: list[str]) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = cli.main(argv)
        return exit_code, stdout.getvalue(), stderr.getvalue()

    def test_legacy_test_wecom_alias_is_hidden_from_help_but_still_parseable(self):
        parser = cli.build_parser()

        self.assertNotIn("test-wecom", parser.format_help())
        args = parser.parse_args(cli._normalize_legacy_command_aliases(["test-wecom"]))
        self.assertEqual(args.command, "test-source")

    def test_validate_config_uses_database_backed_org_config_by_default(self):
        OrganizationConfigRepository(self.db_manager).save_config(
            "default",
                {
                    "corpid": "corp-cli",
                    "agentid": "10001",
                    "corpsecret": "secret-cli",
                    "webhook_url": "https://example.invalid/webhook?key=cli-test",
                    "ldap_server": "dc01.cli.local",
                    "ldap_domain": "cli.local",
                    "ldap_username": "cli-admin",
                "ldap_password": "Password123!",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": True,
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
            },
            config_path="config.ini",
        )

        exit_code, stdout, stderr = self._run_cli(["validate-config", "--db-path", str(self.db_path)])

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("config valid: db:org:default", stdout)
        self.assertIn("organization: default", stdout)

    def test_validate_config_accepts_missing_optional_webhook(self):
        OrganizationConfigRepository(self.db_manager).save_config(
            "default",
            {
                "corpid": "corp-cli",
                "agentid": "10001",
                "corpsecret": "secret-cli",
                "webhook_url": "",
                "ldap_server": "dc01.cli.local",
                "ldap_domain": "cli.local",
                "ldap_username": "cli-admin",
                "ldap_password": "Password123!",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": True,
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
            },
            config_path="config.ini",
        )

        exit_code, stdout, stderr = self._run_cli(["validate-config", "--db-path", str(self.db_path)])

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("config valid: db:org:default", stdout)

    def test_test_source_uses_database_backed_org_config_by_default(self):
        OrganizationConfigRepository(self.db_manager).save_config(
            "default",
            {
                "corpid": "corp-cli",
                "agentid": "10001",
                "corpsecret": "secret-cli",
                "webhook_url": "https://example.invalid/webhook?key=cli-test",
                "ldap_server": "dc01.cli.local",
                "ldap_domain": "cli.local",
                "ldap_username": "cli-admin",
                "ldap_password": "Password123!",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": True,
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
                "source_provider": "wecom",
            },
            config_path="config.ini",
        )

        with patch.object(cli, "test_source_connection", return_value=(True, "WeCom connection succeeded (self-built app), departments: 1")) as mock_test:
            exit_code, stdout, stderr = self._run_cli(["test-source", "--db-path", str(self.db_path)])

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("WeCom connection succeeded", stdout)
        mock_test.assert_called_once()

    def test_sync_passes_org_id_and_resolved_legacy_path_to_runtime(self):
        asia_config_path = str((self.db_path.parent / "cli_asia.ini").resolve())
        OrganizationRepository(self.db_manager).upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path=asia_config_path,
            description="",
            is_enabled=True,
        )

        fake_result = {
            "job_id": "job-cli-sync-001",
            "org_id": "asia",
            "organization_config_path": "db:org:asia",
            "execution_mode": "dry_run",
            "error_count": 0,
            "planned_operation_count": 0,
            "executed_operation_count": 0,
            "high_risk_operation_count": 0,
            "conflict_count": 0,
        }
        with patch.object(cli, "run_sync", return_value=fake_result) as mock_run_sync:
            exit_code, stdout, stderr = self._run_cli(
                ["sync", "--mode", "dry-run", "--org-id", "asia", "--db-path", str(self.db_path), "--json"]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertEqual(payload["org_id"], "asia")
        self.assertEqual(payload["organization_config_path"], "db:org:asia")
        mock_run_sync.assert_called_once_with(
            execution_mode="dry_run",
            trigger_type="cli",
            db_path=str(self.db_path),
            config_path=asia_config_path,
            org_id="asia",
            requested_by=os.getenv("USERNAME") or os.getenv("USER") or "cli",
        )

    def test_config_export_prints_database_backed_organization_bundle(self):
        OrganizationRepository(self.db_manager).upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path="",
            description="regional tenant",
            is_enabled=True,
        )
        OrganizationConfigRepository(self.db_manager).save_config(
            "asia",
            {
                "corpid": "corp-asia",
                "agentid": "20002",
                "corpsecret": "secret-asia",
                "webhook_url": "https://example.invalid/webhook/asia",
                "ldap_server": "dc01.asia.local",
                "ldap_domain": "asia.local",
                "ldap_username": "asia-admin",
                "ldap_password": "Password123!",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": True,
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
            },
            config_path="",
        )

        exit_code, stdout, stderr = self._run_cli(
            ["config-export", "--db-path", str(self.db_path), "--org-id", "asia"]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        bundle = json.loads(stdout)
        self.assertEqual(bundle["organization"]["org_id"], "asia")
        self.assertEqual(bundle["organization_config"]["corpid"], "corp-asia")

    def test_config_import_loads_bundle_into_target_organization(self):
        bundle = {
            "bundle_type": "organization_config_bundle",
            "bundle_version": 1,
            "organization": {
                "org_id": "source-org",
                "name": "Source Organization",
                "description": "import source",
                "is_enabled": True,
                "config_path": "",
            },
            "organization_config": {
                "corpid": "corp-imported",
                "agentid": "30003",
                "corpsecret": "secret-imported",
                "webhook_url": "https://example.invalid/webhook/imported",
                "ldap_server": "dc01.imported.local",
                "ldap_domain": "imported.local",
                "ldap_username": "imported-admin",
                "ldap_password": "Password123!",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": True,
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
            },
            "org_settings": {
                "group_display_separator": "/",
            },
            "connectors": [],
            "attribute_mappings": [],
            "group_exclusion_rules": [],
        }
        self.bundle_path.write_text(json.dumps(bundle), encoding="utf-8")

        exit_code, stdout, stderr = self._run_cli(
            [
                "config-import",
                "--db-path",
                str(self.db_path),
                "--file",
                str(self.bundle_path),
                "--target-org-id",
                "europe",
            ]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("imported organization bundle into: europe", stdout)
        organization = OrganizationRepository(self.db_manager).get_organization_record("europe")
        self.assertIsNotNone(organization)
        imported_config = OrganizationConfigRepository(self.db_manager).get_editable_config("europe", config_path="")
        self.assertEqual(imported_config["corpid"], "corp-imported")


if __name__ == "__main__":
    unittest.main()
