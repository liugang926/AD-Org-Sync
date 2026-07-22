import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from sync_app.services.config_release import (
    build_config_release_center_data,
    publish_current_config_release_snapshot,
    rollback_config_release_snapshot,
)
from sync_app.services.config_bundle import (
    export_organization_bundle,
    import_organization_bundle,
)
from sync_app.storage.local_db import (
    ConfigReleaseSnapshotRepository,
    DatabaseManager,
    FieldAuthorityRuleRepository,
    IdentityMatchRuleRepository,
    OrganizationConfigRepository,
    OrganizationRepository,
    SourceDirectoryRepository,
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

    def test_source_scope_is_published_diffed_and_restored(self):
        with TemporaryDirectory() as temp_dir:
            db_manager, _config_path = self._build_db(temp_dir)
            source_repo = SourceDirectoryRepository(db_manager)
            snapshot_id = source_repo.start_refresh(
                org_id="default",
                provider_id="wecom",
                created_by="tester",
            )
            source_repo.replace_snapshot(
                snapshot_id,
                departments=[
                    {
                        "source_department_id": "1",
                        "name": "HQ",
                        "parent_department_id": "0",
                        "path_ids": ["1"],
                        "path_names": ["HQ"],
                    }
                ],
                users=[
                    {
                        "source_user_id": "alice",
                        "display_name": "Alice",
                        "employee_id": "E001",
                        "department_ids": ["1"],
                        "department_names": ["HQ"],
                        "is_active": True,
                        "search_text": "Alice E001",
                    }
                ],
                fields=[],
                fingerprint="config-release-scope-v1",
            )
            source_repo.save_scope_selection(
                org_id="default",
                provider_id="wecom",
                scope_type="full",
                username_strategy="employee_id",
                source_field="employee_id",
                snapshot_id=snapshot_id,
                requested_by="tester",
            )
            baseline = publish_current_config_release_snapshot(
                db_manager,
                "default",
                created_by="tester",
                snapshot_name="Full Scope",
            )

            source_repo.save_scope_selection(
                org_id="default",
                provider_id="wecom",
                scope_type="selected_users",
                selected_source_user_ids=["alice"],
                username_strategy="employee_id",
                source_field="employee_id",
                snapshot_id=snapshot_id,
                requested_by="tester",
            )
            release_center = build_config_release_center_data(
                db_manager,
                "default",
            )
            rollback_config_release_snapshot(
                db_manager,
                baseline["snapshot"].id,
                org_id="default",
                created_by="tester",
            )
            restored = source_repo.get_scope_selection(
                org_id="default",
                provider_id="wecom",
            )

        self.assertTrue(release_center["has_unpublished_changes"])
        self.assertTrue(
            any(
                group["title"] == "Source Sync Scopes"
                for group in release_center["comparison_diff"]["groups"]
            )
        )
        self.assertEqual(restored["scope_type"], "full")
        self.assertEqual(restored["selected_source_user_ids"], [])
        self.assertEqual(restored["source_field"], "employee_id")

    def test_release_binds_the_configured_provider_scope_when_other_scopes_exist(self):
        with TemporaryDirectory() as temp_dir:
            db_manager, config_path = self._build_db(temp_dir)
            OrganizationConfigRepository(db_manager).save_config(
                "default",
                {"source_provider": "wecom"},
                config_path=config_path,
            )
            source_repo = SourceDirectoryRepository(db_manager)
            snapshot_ids: dict[str, int] = {}
            for provider_id in ("wecom", "dingtalk"):
                snapshot_id = source_repo.start_refresh(
                    org_id="default",
                    provider_id=provider_id,
                    created_by="tester",
                )
                source_repo.replace_snapshot(
                    snapshot_id,
                    departments=[],
                    users=[],
                    fields=[],
                    fingerprint=f"release-scope-{provider_id}",
                )
                source_repo.save_scope_selection(
                    org_id="default",
                    provider_id=provider_id,
                    scope_type="full",
                    snapshot_id=snapshot_id,
                    requested_by="tester",
                )
                snapshot_ids[provider_id] = snapshot_id

            result = publish_current_config_release_snapshot(
                db_manager,
                "default",
                created_by="tester",
                snapshot_name="Configured provider release",
            )

        self.assertEqual(
            result["snapshot"].source_snapshot_id,
            snapshot_ids["wecom"],
        )

    def test_identity_and_field_authority_rules_are_versioned_and_restored(self):
        with TemporaryDirectory() as temp_dir:
            db_manager, _config_path = self._build_db(temp_dir)
            match_repo = IdentityMatchRuleRepository(db_manager)
            authority_repo = FieldAuthorityRuleRepository(db_manager)
            match_repo.seed_defaults(org_id="default", created_by="tester")
            authority_repo.seed_defaults(org_id="default", created_by="tester")
            baseline = publish_current_config_release_snapshot(
                db_manager,
                "default",
                created_by="tester",
                snapshot_name="Identity policy baseline",
            )

            match_repo.upsert_rule(
                org_id="default",
                rule_order=5,
                rule_name="employee_id_to_employee_id",
                source_provider="*",
                source_field="employee_id",
                ad_field="employeeID",
                allow_auto_link=True,
                confidence_level="certain",
                confidence_score=99,
                created_by="tester",
            )
            authority_repo.upsert_rule(
                org_id="default",
                field_name="display_name",
                source_provider="wecom",
                source_priority=20,
                sync_direction="ad_to_source",
                sync_mode="fill_if_empty",
                created_by="tester",
            )
            release_center = build_config_release_center_data(
                db_manager,
                "default",
            )
            rollback_config_release_snapshot(
                db_manager,
                baseline["snapshot"].id,
                org_id="default",
                created_by="tester",
            )
            restored_match = next(
                rule
                for rule in match_repo.list_rules(org_id="default")
                if rule.rule_name == "employee_id_to_employee_id"
            )
            restored_authority = next(
                rule
                for rule in authority_repo.list_rules(org_id="default")
                if rule.field_name == "display_name"
                and rule.source_provider == "wecom"
            )

        self.assertTrue(release_center["has_unpublished_changes"])
        self.assertEqual(
            {group["title"] for group in release_center["comparison_diff"]["groups"]},
            {"Identity Match Rules", "Field Authority Rules"},
        )
        self.assertEqual(restored_match.confidence_score, 100)
        self.assertEqual(restored_authority.sync_direction, "source_to_ad")
        self.assertEqual(restored_authority.sync_mode, "replace")

    def test_legacy_bundle_without_identity_sections_preserves_current_rules(self):
        with TemporaryDirectory() as temp_dir:
            db_manager, _config_path = self._build_db(temp_dir)
            match_repo = IdentityMatchRuleRepository(db_manager)
            authority_repo = FieldAuthorityRuleRepository(db_manager)
            match_repo.seed_defaults(org_id="default", created_by="tester")
            authority_repo.seed_defaults(org_id="default", created_by="tester")
            legacy_bundle = export_organization_bundle(db_manager, "default")
            legacy_bundle.pop("identity_match_rules", None)
            legacy_bundle.pop("field_authority_rules", None)

            import_organization_bundle(
                db_manager,
                legacy_bundle,
                target_org_id="default",
                replace_existing=True,
            )
            match_rules_preserved = bool(match_repo.list_rules(org_id="default"))
            authority_rules_preserved = bool(
                authority_repo.list_rules(org_id="default")
            )

        self.assertTrue(match_rules_preserved)
        self.assertTrue(authority_rules_preserved)


if __name__ == "__main__":
    unittest.main()
