import unittest
from tempfile import TemporaryDirectory

from sync_app.core.models import AccountConfig, AppConfig, LDAPConfig, WeComConfig
from sync_app.services.runtime_connectors import load_connector_specs
from sync_app.storage.local_db import DatabaseManager, SyncConnectorRepository


class RuntimeConnectorSpecTests(unittest.TestCase):
    def test_single_enabled_connector_becomes_effective_default_when_routing_is_disabled(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="default.ini",
        )

        with TemporaryDirectory() as temp_dir:
            db_path = f"{temp_dir}/runtime_connector_specs.db"
            manager = DatabaseManager(db_path=db_path)
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)
            repo = SyncConnectorRepository(manager)
            repo.upsert_connector(
                connector_id="cn",
                org_id="default",
                name="China Directory",
                config_path="",
                root_department_ids=[2],
                username_strategy="family_name_pinyin_given_initials",
                username_collision_policy="append_employee_id",
                username_collision_template="{base}{counter2}",
                username_template="{family_name_pinyin}{given_initials}",
                disabled_users_ou="Disabled Users/China",
                group_type="distribution",
                group_mail_domain="groups.example.com",
                custom_group_ou_path="Managed Groups/China",
                managed_tag_ids=["1001"],
                managed_external_chat_ids=["chat-01"],
                is_enabled=True,
            )

            specs = load_connector_specs(
                config,
                repo,
                connectors_enabled=False,
                org_id="default",
                default_root_department_ids=[1],
                default_disabled_users_ou="Disabled Users",
                default_custom_group_ou_path="Managed Groups",
                default_user_root_ou_path="Users/Employees",
            )

        self.assertEqual(len(specs), 1)
        effective_default = specs[0]
        self.assertEqual(effective_default["connector_id"], "default")
        self.assertEqual(effective_default["name"], "China Directory")
        self.assertEqual(effective_default["root_department_ids"], [2])
        self.assertEqual(effective_default["username_strategy"], "family_name_pinyin_given_initials")
        self.assertEqual(effective_default["username_collision_policy"], "append_employee_id")
        self.assertEqual(effective_default["username_collision_template"], "{base}{counter2}")
        self.assertEqual(effective_default["username_template"], "{family_name_pinyin}{given_initials}")
        self.assertEqual(effective_default["disabled_users_ou"], "Disabled Users/China")
        self.assertEqual(effective_default["group_type"], "distribution")
        self.assertEqual(effective_default["group_mail_domain"], "groups.example.com")
        self.assertEqual(effective_default["custom_group_ou_path"], "Managed Groups/China")
        self.assertEqual(effective_default["managed_tag_ids"], ["1001"])
        self.assertEqual(effective_default["managed_external_chat_ids"], ["chat-01"])
        self.assertEqual(effective_default["config"].domain, "example.com")
        self.assertEqual(effective_default["config"].ldap.domain, "example.com")
        self.assertEqual(effective_default["config"].config_path, "db:connector:default:cn")


if __name__ == "__main__":
    unittest.main()
