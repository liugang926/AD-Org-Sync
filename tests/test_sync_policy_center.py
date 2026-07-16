import unittest

from sync_app.core.models import SyncConnectorRecord
from sync_app.services.sync_policy_center import (
    build_connector_policy_upsert,
    update_policy_section,
)
from sync_app.services.typed_settings import AdvancedSyncPolicySettings


class SyncPolicyCenterTests(unittest.TestCase):
    def test_section_update_preserves_every_other_policy_field(self):
        current = AdvancedSyncPolicySettings(
            offboarding_grace_days=14,
            future_onboarding_enabled=True,
            attribute_mapping_enabled=False,
            write_back_enabled=False,
            disable_circuit_breaker_percent=7.5,
            managed_group_mail_domain="groups.example.com",
        )

        updated = update_policy_section(
            current,
            "attribute_mappings",
            {"attribute_mapping_enabled": True, "write_back_enabled": True},
        )

        self.assertTrue(updated.attribute_mapping_enabled)
        self.assertTrue(updated.write_back_enabled)
        self.assertEqual(updated.offboarding_grace_days, 14)
        self.assertTrue(updated.future_onboarding_enabled)
        self.assertEqual(updated.disable_circuit_breaker_percent, 7.5)
        self.assertEqual(updated.managed_group_mail_domain, "groups.example.com")

    def test_section_update_rejects_cross_section_field(self):
        with self.assertRaisesRegex(ValueError, "do not belong"):
            update_policy_section(
                AdvancedSyncPolicySettings(),
                "security",
                {"offboarding_grace_days": 99},
            )

    def test_connector_policy_update_preserves_connection_secrets_and_other_sections(self):
        connector = SyncConnectorRecord(
            connector_id="asia",
            org_id="default",
            name="Asia",
            config_path="config.asia.ini",
            ldap_server="dc01.asia.example.local",
            ldap_domain="asia.example.local",
            ldap_username="svc-sync",
            ldap_password="secret-value",
            default_password="bootstrap-secret",
            root_department_ids=[2, 8],
            username_strategy="userid",
            username_template="{userid}",
            group_mail_domain="groups.example.com",
            managed_tag_ids=["1001"],
            disabled_users_ou="Disabled/Asia",
        )

        payload = build_connector_policy_upsert(
            connector,
            "account_naming",
            {
                "username_strategy": "employee_id",
                "username_template": "{employee_id}",
                "username_collision_policy": "append_userid",
                "username_collision_template": "",
            },
        )

        self.assertEqual(payload["username_strategy"], "employee_id")
        self.assertEqual(payload["ldap_password"], "secret-value")
        self.assertEqual(payload["default_password"], "bootstrap-secret")
        self.assertEqual(payload["root_department_ids"], [2, 8])
        self.assertEqual(payload["managed_tag_ids"], ["1001"])
        self.assertEqual(payload["disabled_users_ou"], "Disabled/Asia")

    def test_connector_policy_update_rejects_cross_section_field(self):
        with self.assertRaisesRegex(ValueError, "do not belong"):
            build_connector_policy_upsert(
                SyncConnectorRecord(connector_id="asia", name="Asia"),
                "scope",
                {"username_strategy": "employee_id"},
            )


if __name__ == "__main__":
    unittest.main()
