import io
import os
import sys
import unittest
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace
from unittest.mock import Mock, patch

from sync_app import cli
from sync_app.storage.local_db import DatabaseManager, OrganizationRepository, SettingsRepository


class CliWebCommandTests(unittest.TestCase):
    def setUp(self):
        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        self.db_path = os.path.join(test_dir, "cli_web_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.db_path + suffix)
            except FileNotFoundError:
                pass

        db_manager = DatabaseManager(db_path=self.db_path)
        db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        self.settings_repo = SettingsRepository(db_manager)

    def test_web_handler_passes_proxy_settings_to_uvicorn(self):
        self.settings_repo.set_value("web_bind_host", "127.0.0.1", "string")
        self.settings_repo.set_value("web_bind_port", "8443", "int")
        self.settings_repo.set_value("web_public_base_url", "https://sync.example.com", "string")
        self.settings_repo.set_value("web_session_cookie_secure_mode", "always", "string")
        self.settings_repo.set_value("web_trust_proxy_headers", "true", "bool")
        self.settings_repo.set_value("web_forwarded_allow_ips", "10.0.0.1,10.0.0.2", "string")

        fake_uvicorn = SimpleNamespace(run=Mock())
        args = Namespace(
            host=None,
            port=None,
            public_base_url=None,
            secure_cookies=None,
            trust_proxy_headers=False,
            no_trust_proxy_headers=False,
            forwarded_allow_ips=None,
            config="config.ini",
            db_path=self.db_path,
        )
        stdout = io.StringIO()
        stderr = io.StringIO()

        with patch.dict(sys.modules, {"uvicorn": fake_uvicorn}), redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = cli._handle_web(args)

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertIn("web control plane bind address: http://127.0.0.1:8443", stdout.getvalue())
        self.assertIn("public base URL: https://sync.example.com", stdout.getvalue())
        self.assertIn("trusted proxy headers: enabled", stdout.getvalue())
        fake_uvicorn.run.assert_called_once()

        called_app = fake_uvicorn.run.call_args.args[0]
        called_kwargs = fake_uvicorn.run.call_args.kwargs
        self.assertTrue(called_app.state.session_cookie_secure)
        self.assertEqual(called_app.state.web_runtime_settings["public_base_url"], "https://sync.example.com")
        self.assertTrue(called_app.state.web_runtime_settings["trust_proxy_headers"])
        self.assertEqual(called_app.state.web_runtime_settings["forwarded_allow_ips"], "10.0.0.1,10.0.0.2")
        self.assertIn("/static", {getattr(route, "path", "") for route in called_app.routes})
        self.assertEqual(called_kwargs["host"], "127.0.0.1")
        self.assertEqual(called_kwargs["port"], 8443)
        self.assertTrue(called_kwargs["proxy_headers"])
        self.assertEqual(called_kwargs["forwarded_allow_ips"], "10.0.0.1,10.0.0.2")

    def test_web_handler_reuses_default_org_legacy_path_when_config_not_supplied(self):
        legacy_config_path = os.path.join(os.getcwd(), "test_artifacts", "cli_web_default_org.ini")
        OrganizationRepository(DatabaseManager(db_path=self.db_path)).ensure_default(config_path=legacy_config_path)

        fake_uvicorn = SimpleNamespace(run=Mock())
        args = Namespace(
            host=None,
            port=None,
            public_base_url=None,
            secure_cookies=None,
            trust_proxy_headers=False,
            no_trust_proxy_headers=False,
            forwarded_allow_ips=None,
            config=None,
            db_path=self.db_path,
        )

        with patch.dict(sys.modules, {"uvicorn": fake_uvicorn}):
            exit_code = cli._handle_web(args)

        self.assertEqual(exit_code, 0)
        called_app = fake_uvicorn.run.call_args.args[0]
        self.assertEqual(called_app.state.config_path, legacy_config_path)


if __name__ == "__main__":
    unittest.main()
