import io
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from sync_app import cli
from sync_app.storage.local_db import DatabaseManager, OrganizationRepository, WebAdminUserRepository
from sync_app.web.security import verify_password


class CliDeployCommandTests(unittest.TestCase):
    def setUp(self):
        test_dir = Path(os.getcwd()) / "test_artifacts"
        test_dir.mkdir(exist_ok=True)
        self.db_path = test_dir / "cli_deploy_test.db"
        self.config_path = test_dir / "cli_deploy_test.ini"
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.db_path) + suffix)
            if candidate.exists():
                candidate.unlink()
        if self.config_path.exists():
            self.config_path.unlink()

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.db_path) + suffix)
            if candidate.exists():
                try:
                    candidate.unlink()
                except PermissionError:
                    pass
        if self.config_path.exists():
            self.config_path.unlink()

    def _run_cli(self, argv: list[str]) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = cli.main(argv)
        return exit_code, stdout.getvalue(), stderr.getvalue()

    def test_init_web_bootstraps_default_organization(self):
        exit_code, stdout, stderr = self._run_cli(
            ["init-web", "--db-path", str(self.db_path), "--config", str(self.config_path)]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("organization_id: default", stdout)

        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        organization = OrganizationRepository(manager).get_organization_record("default")
        self.assertIsNotNone(organization)
        self.assertEqual(organization.config_path, str(self.config_path.resolve()))

    def test_bootstrap_admin_creates_local_administrator(self):
        exit_code, stdout, stderr = self._run_cli(
            [
                "bootstrap-admin",
                "--db-path",
                str(self.db_path),
                "--username",
                "deployadmin",
                "--password",
                "simple88",
            ]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("administrator created: deployadmin", stdout)

        manager = DatabaseManager(db_path=str(self.db_path))
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        user = WebAdminUserRepository(manager).get_user_record_by_username("deployadmin")
        self.assertIsNotNone(user)
        self.assertTrue(verify_password("simple88", user.password_hash))

    def test_bootstrap_admin_requires_reset_for_existing_account(self):
        first_exit, _, first_stderr = self._run_cli(
            [
                "bootstrap-admin",
                "--db-path",
                str(self.db_path),
                "--username",
                "deployadmin",
                "--password",
                "simple88",
            ]
        )
        second_exit, _, second_stderr = self._run_cli(
            [
                "bootstrap-admin",
                "--db-path",
                str(self.db_path),
                "--username",
                "deployadmin",
                "--password",
                "simple88",
            ]
        )

        self.assertEqual(first_exit, 0)
        self.assertEqual(first_stderr, "")
        self.assertEqual(second_exit, 1)
        self.assertIn("Use --reset", second_stderr)

    def test_validate_config_accepts_generic_compatibility_sections(self):
        self.config_path.write_text(
            "\n".join(
                [
                    "[Source]",
                    "Provider = wecom",
                    "",
                    "[SourceConnector]",
                    "CorpID = corp-generic",
                    "CorpSecret = secret-generic",
                    "AgentID = 10001",
                    "",
                    "[Notification]",
                    "WebhookUrl = https://example.invalid/cgi-bin/webhook/send?key=test",
                    "",
                    "[LDAP]",
                    "Server = dc01.example.local",
                    "Domain = example.local",
                    "Username = EXAMPLE\\administrator",
                    "Password = Password123!",
                    "UseSSL = true",
                    "Port = 636",
                    "ValidateCert = false",
                    "",
                    "[Account]",
                    "DefaultPassword = ChangeMe123!",
                    "ForceChangePassword = true",
                    "PasswordComplexity = strong",
                    "",
                    "[ExcludeUsers]",
                    "SystemAccounts = admin,administrator,guest,krbtgt",
                    "CustomAccounts =",
                    "",
                    "[ExcludeDepartments]",
                    "Names =",
                    "",
                    "[Schedule]",
                    "Time = 03:00",
                    "RetryInterval = 60",
                    "MaxRetries = 3",
                ]
            ),
            encoding="utf-8",
        )

        exit_code, stdout, stderr = self._run_cli(["validate-config", "--config", str(self.config_path)])

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("config valid", stdout)


if __name__ == "__main__":
    unittest.main()
