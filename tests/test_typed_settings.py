import tempfile
import unittest
from pathlib import Path

from sync_app.services.typed_settings import (
    AdvancedSyncPolicySettings,
    BrandingSettings,
    DirectoryUiSettings,
    NotificationAutomationPolicySettings,
    WebRuntimeSettings,
    WebSecuritySettings,
)
from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories.system import SettingsRepository


class TypedSettingsTests(unittest.TestCase):
    def _create_settings_repo(self) -> tuple[tempfile.TemporaryDirectory[str], SettingsRepository]:
        temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(temp_dir.name) / "typed_settings.db"
        db_manager = DatabaseManager(str(db_path))
        db_manager.initialize()
        return temp_dir, SettingsRepository(db_manager)

    def test_web_runtime_settings_persist_and_load_roundtrip(self):
        temp_dir, settings_repo = self._create_settings_repo()
        self.addCleanup(temp_dir.cleanup)

        runtime_settings = WebRuntimeSettings.from_mapping(
            {
                "web_bind_host": " 0.0.0.0 ",
                "web_bind_port": "8443",
                "web_public_base_url": "https://sync.example.com/ ",
                "web_session_cookie_secure_mode": "ALWAYS",
                "web_trust_proxy_headers": "true",
                "web_forwarded_allow_ips": "10.0.0.1,10.0.0.2",
            }
        )
        runtime_settings.persist(settings_repo)

        loaded_settings = WebRuntimeSettings.load(settings_repo)
        self.assertEqual(loaded_settings.bind_host, "0.0.0.0")
        self.assertEqual(loaded_settings.bind_port, 8443)
        self.assertEqual(loaded_settings.public_base_url, "https://sync.example.com")
        self.assertEqual(loaded_settings.session_cookie_secure_mode, "always")
        self.assertTrue(loaded_settings.trust_proxy_headers)
        self.assertEqual(loaded_settings.forwarded_allow_ips, "10.0.0.1,10.0.0.2")

    def test_notification_automation_policy_settings_persist_and_load_roundtrip(self):
        temp_dir, settings_repo = self._create_settings_repo()
        self.addCleanup(temp_dir.cleanup)

        policy_settings = NotificationAutomationPolicySettings.from_mapping(
            {
                "schedule_execution_mode": "dry_run",
                "notify_dry_run_failure_enabled": True,
                "notify_conflict_backlog_enabled": True,
                "notify_conflict_backlog_threshold": 0,
                "notify_review_pending_enabled": True,
                "notify_rule_governance_enabled": True,
                "scheduled_apply_gate_enabled": False,
                "scheduled_apply_max_dry_run_age_hours": 0,
                "scheduled_apply_requires_zero_conflicts": False,
                "scheduled_apply_requires_review_approval": False,
            }
        )
        policy_settings.persist(settings_repo, org_id="Default")

        loaded_settings = NotificationAutomationPolicySettings.load(settings_repo, org_id="default")
        self.assertEqual(loaded_settings.schedule_execution_mode, "dry_run")
        self.assertTrue(loaded_settings.notify_dry_run_failure_enabled)
        self.assertTrue(loaded_settings.notify_conflict_backlog_enabled)
        self.assertEqual(loaded_settings.notify_conflict_backlog_threshold, 1)
        self.assertTrue(loaded_settings.notify_review_pending_enabled)
        self.assertTrue(loaded_settings.notify_rule_governance_enabled)
        self.assertFalse(loaded_settings.scheduled_apply_gate_enabled)
        self.assertEqual(loaded_settings.scheduled_apply_max_dry_run_age_hours, 1)
        self.assertFalse(loaded_settings.scheduled_apply_requires_zero_conflicts)
        self.assertFalse(loaded_settings.scheduled_apply_requires_review_approval)

    def test_directory_ui_and_branding_settings_roundtrip(self):
        temp_dir, settings_repo = self._create_settings_repo()
        self.addCleanup(temp_dir.cleanup)

        directory_ui_settings = DirectoryUiSettings.from_mapping(
            {
                "group_display_separator": " ",
                "group_recursive_enabled": False,
                "managed_relation_cleanup_enabled": True,
                "schedule_execution_mode": "dry_run",
                "user_ou_placement_strategy": "wecom_primary_department",
                "source_root_unit_ids": "2,8",
                "source_root_unit_display_text": "中国区 / 华东区",
                "directory_root_ou_path": "Managed Users/China",
                "disabled_users_ou_path": "Disabled Users/China",
                "custom_group_ou_path": "Managed Groups/China",
            }
        )
        directory_ui_settings.persist(settings_repo, org_id="default")

        branding_settings = BrandingSettings.from_mapping(
            {
                "brand_display_name": "Directory Hub",
                "brand_mark_text": "DH",
                "brand_attribution": "Internal IT",
            },
            default_display_name="AD Org Sync",
            default_mark_text="AD",
            default_attribution="微信公众号：大刘讲IT",
        )
        branding_settings.persist(settings_repo)

        loaded_directory_ui_settings = DirectoryUiSettings.load(settings_repo, org_id="default")
        loaded_branding_settings = BrandingSettings.load(
            settings_repo,
            default_display_name="AD Org Sync",
            default_mark_text="AD",
            default_attribution="微信公众号：大刘讲IT",
        )
        self.assertEqual(loaded_directory_ui_settings.group_display_separator, " ")
        self.assertFalse(loaded_directory_ui_settings.group_recursive_enabled)
        self.assertTrue(loaded_directory_ui_settings.managed_relation_cleanup_enabled)
        self.assertEqual(loaded_directory_ui_settings.schedule_execution_mode, "dry_run")
        self.assertEqual(loaded_directory_ui_settings.user_ou_placement_strategy, "wecom_primary_department")
        self.assertEqual(loaded_directory_ui_settings.custom_group_ou_path, "Managed Groups/China")
        self.assertEqual(loaded_branding_settings.brand_display_name, "Directory Hub")
        self.assertEqual(loaded_branding_settings.brand_mark_text, "DH")
        self.assertEqual(loaded_branding_settings.brand_attribution, "Internal IT")

    def test_advanced_sync_policy_settings_roundtrip(self):
        temp_dir, settings_repo = self._create_settings_repo()
        self.addCleanup(temp_dir.cleanup)

        advanced_sync_settings = AdvancedSyncPolicySettings.from_mapping(
            {
                "offboarding_grace_days": 7,
                "offboarding_notify_managers": True,
                "advanced_connector_routing_enabled": True,
                "attribute_mapping_enabled": True,
                "write_back_enabled": True,
                "custom_group_sync_enabled": True,
                "offboarding_lifecycle_enabled": True,
                "rehire_restore_enabled": True,
                "automatic_replay_enabled": True,
                "future_onboarding_enabled": True,
                "future_onboarding_start_field": "start_date",
                "contractor_lifecycle_enabled": True,
                "lifecycle_employment_type_field": "worker_type",
                "contractor_end_field": "worker_end_date",
                "lifecycle_sponsor_field": "manager_userid",
                "contractor_type_values": "contractor,vendor",
                "disable_circuit_breaker_enabled": True,
                "disable_circuit_breaker_percent": 3.5,
                "disable_circuit_breaker_min_count": 12,
                "disable_circuit_breaker_requires_approval": False,
                "managed_group_type": "mail_enabled_security",
                "managed_group_mail_domain": "groups.example.com",
                "custom_group_ou_path": "Managed Groups/APAC",
            }
        )
        advanced_sync_settings.persist(settings_repo, org_id="default")

        loaded_settings = AdvancedSyncPolicySettings.load(settings_repo, org_id="default")
        self.assertEqual(loaded_settings.offboarding_grace_days, 7)
        self.assertTrue(loaded_settings.offboarding_notify_managers)
        self.assertTrue(loaded_settings.advanced_connector_routing_enabled)
        self.assertEqual(loaded_settings.future_onboarding_start_field, "start_date")
        self.assertEqual(loaded_settings.disable_circuit_breaker_percent, 3.5)
        self.assertEqual(loaded_settings.disable_circuit_breaker_min_count, 12)
        self.assertFalse(loaded_settings.disable_circuit_breaker_requires_approval)
        self.assertEqual(loaded_settings.managed_group_type, "mail_enabled_security")
        self.assertEqual(loaded_settings.custom_group_ou_path, "Managed Groups/APAC")

    def test_web_security_settings_normalize_minimum_values(self):
        temp_dir, settings_repo = self._create_settings_repo()
        self.addCleanup(temp_dir.cleanup)

        settings_repo.set_value("web_session_idle_minutes", "0", "int")
        settings_repo.set_value("web_login_max_attempts", "0", "int")
        settings_repo.set_value("web_login_window_seconds", "-1", "int")
        settings_repo.set_value("web_login_lockout_seconds", "0", "int")
        settings_repo.set_value("web_admin_password_min_length", "0", "int")

        security_settings = WebSecuritySettings.load(settings_repo)
        self.assertEqual(security_settings.session_idle_minutes, 1)
        self.assertEqual(security_settings.login_max_attempts, 1)
        self.assertEqual(security_settings.login_window_seconds, 1)
        self.assertEqual(security_settings.login_lockout_seconds, 1)
        self.assertEqual(security_settings.admin_password_min_length, 1)


if __name__ == "__main__":
    unittest.main()
