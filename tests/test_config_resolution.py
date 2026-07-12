from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sync_app.services.config_resolution import (
    CONFIG_SOURCE_PRECEDENCE,
    resolve_organization_config,
)
from sync_app.storage.local_db import DatabaseManager, OrganizationConfigRepository


CONFIG_TEMPLATE = """\
[Source]
Provider = wecom
[SourceConnector]
CorpID = file-corp
CorpSecret = file-secret
[LDAP]
Server = file-ad.example.com
Domain = example.com
Username = svc-sync
Password = file-password
UseSSL = true
[Account]
DefaultPassword = ChangeMe123!
"""


class ConfigurationResolutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.config_path = self.root / "tenant.ini"
        self.config_path.write_text(CONFIG_TEMPLATE, encoding="utf-8")
        self.db_manager = DatabaseManager(db_path=str(self.root / "app.db"))
        self.db_manager.initialize(create_startup_snapshot=False)
        self.repo = OrganizationConfigRepository(self.db_manager)

    def test_precedence_is_explicit_and_stable(self) -> None:
        self.assertEqual(
            CONFIG_SOURCE_PRECEDENCE,
            (
                "explicit_file_override",
                "organization_database",
                "registered_legacy_file_import",
                "fallback_loader",
            ),
        )

    def test_database_wins_unless_file_is_an_explicit_override(self) -> None:
        self.repo.save_config(
            "tenant-a",
            {
                "corpid": "database-corp",
                "corpsecret": "database-secret",
                "ldap_server": "database-ad.example.com",
                "ldap_domain": "example.com",
                "ldap_username": "svc-db",
                "ldap_password": "database-password",
                "default_password": "ChangeMe123!",
            },
            config_path=str(self.config_path),
        )

        database_result = resolve_organization_config(
            self.repo,
            org_id="tenant-a",
            config_path=str(self.config_path),
        )
        override_result = resolve_organization_config(
            None,
            org_id="tenant-a",
            config_path=str(self.config_path),
            explicit_file_override=True,
        )

        self.assertEqual(database_result.source_kind, "organization_database")
        self.assertEqual(database_result.config.ldap.server, "database-ad.example.com")
        self.assertEqual(override_result.source_kind, "explicit_file_override")
        self.assertEqual(override_result.config.ldap.server, "file-ad.example.com")

    def test_registered_legacy_file_is_imported_once_then_database_is_authoritative(self) -> None:
        first = resolve_organization_config(
            self.repo,
            org_id="tenant-b",
            config_path=str(self.config_path),
        )
        self.config_path.write_text(CONFIG_TEMPLATE.replace("file-ad.example.com", "changed.example.com"), encoding="utf-8")
        second = resolve_organization_config(
            self.repo,
            org_id="tenant-b",
            config_path=str(self.config_path),
        )

        self.assertEqual(first.source_kind, "registered_legacy_file_import")
        self.assertTrue(first.legacy_file_imported)
        self.assertEqual(second.source_kind, "organization_database")
        self.assertEqual(second.config.ldap.server, "file-ad.example.com")


if __name__ == "__main__":
    unittest.main()
