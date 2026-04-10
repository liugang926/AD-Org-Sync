import unittest
from unittest.mock import patch

import sync_app.providers as provider_exports
from sync_app.core.config import test_source_connection
from sync_app.core.models import AppConfig, DepartmentNode, LDAPConfig, SourceConfig, WeComConfig
from sync_app.providers.source import (
    build_source_provider,
    get_source_provider_display_name,
    get_source_provider_schema,
    list_source_provider_options,
    normalize_source_provider,
)


class FakeWeComClient:
    def __init__(self, corpid: str, corpsecret: str, agentid: str | None = None):
        self.corpid = corpid
        self.corpsecret = corpsecret
        self.agentid = agentid
        self.closed = False

    def get_department_list(self):
        return [{"id": 1, "name": "HQ", "parentid": 0}]

    def get_department_users(self, department_id: int):
        if department_id != 1:
            return []
        return [{"userid": "alice", "name": "Alice", "email": "alice@example.com", "department": [1]}]

    def get_user_detail(self, user_id: str):
        return {"userid": user_id, "name": "Alice Detail", "email": "alice.detail@example.com", "department": [1]}

    def update_user(self, user_id: str, updates: dict):
        return True

    def get_tag_list(self):
        return [{"tagid": "101", "tagname": "IT"}]

    def get_tag_users(self, tag_id: str | int):
        return {"tagid": str(tag_id), "userlist": [{"userid": "alice"}]}

    def get_external_group_chat(self, chat_id: str):
        return {"chat_id": chat_id, "name": "Partners"}

    def close(self):
        self.closed = True


class FakeDingTalkClient:
    def __init__(self, app_key: str, app_secret: str, agentid: str | None = None):
        self.app_key = app_key
        self.app_secret = app_secret
        self.agentid = agentid
        self.closed = False

    def get_department_list(self):
        return [{"id": 10, "name": "Ding HQ", "parentid": 1}]

    def get_department_users(self, department_id: int):
        if department_id != 10:
            return []
        return [
            {
                "userid": "alice.dd",
                "name": "Alice Ding",
                "email": "alice.ding@example.com",
                "department": [10],
                "main_department": 10,
            }
        ]

    def get_user_detail(self, user_id: str):
        return {
            "userid": user_id,
            "name": "Alice Ding Detail",
            "email": "alice.detail@example.com",
            "department": [10],
            "main_department": 10,
        }

    def update_user(self, user_id: str, updates: dict):
        return True

    def close(self):
        self.closed = True


class SourceProviderTests(unittest.TestCase):
    def test_top_level_provider_exports_include_multi_provider_registry(self):
        self.assertTrue(hasattr(provider_exports, "DingTalkSourceProvider"))
        self.assertTrue(hasattr(provider_exports, "WeComSourceProvider"))
        self.assertTrue(callable(provider_exports.list_source_provider_options))
        self.assertTrue(callable(provider_exports.get_source_provider_schema))

    def test_app_config_accepts_source_connector_and_preserves_wecom_alias(self):
        config = AppConfig(
            source_connector=SourceConfig(corpid="corp-id", corpsecret="secret", agentid="10001"),
            ldap=LDAPConfig(server="dc.example.com", domain="example.com", username="svc", password="secret"),
            domain="example.com",
            source_provider="wecom",
        )

        self.assertEqual(config.source_connector.corpid, "corp-id")
        self.assertEqual(config.wecom.corpid, "corp-id")
        self.assertEqual(config.to_public_dict()["source_connector"]["corpid"], "corp-id")
        self.assertEqual(config.to_public_dict()["wecom"]["corpid"], "corp-id")

    def test_normalize_source_provider_defaults_to_wecom(self):
        self.assertEqual(normalize_source_provider(None), "wecom")
        self.assertEqual(normalize_source_provider(""), "wecom")
        self.assertEqual(normalize_source_provider("WeCom"), "wecom")

    def test_department_node_from_source_payload_supports_generic_parent_keys(self):
        department = DepartmentNode.from_source_payload(
            {
                "dept_id": "10",
                "dept_name": "Engineering",
                "parent_id": "1",
            }
        )

        self.assertEqual(department.department_id, 10)
        self.assertEqual(department.name, "Engineering")
        self.assertEqual(department.parent_id, 1)

    def test_get_source_provider_display_name_uses_known_label(self):
        self.assertEqual(get_source_provider_display_name("wecom"), "WeCom")
        self.assertEqual(get_source_provider_display_name("WeCom"), "WeCom")
        self.assertEqual(get_source_provider_display_name("custom"), "custom")

    def test_provider_schema_registry_exposes_planned_options(self):
        options = dict(list_source_provider_options(include_unimplemented=True))
        self.assertIn("wecom", options)
        self.assertIn("dingtalk", options)
        self.assertIn("feishu", options)
        self.assertTrue(get_source_provider_schema("wecom").implemented)
        self.assertTrue(get_source_provider_schema("dingtalk").implemented)
        self.assertFalse(get_source_provider_schema("feishu").implemented)

    def test_build_source_provider_wraps_wecom_client_with_generic_interface(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp-id", corpsecret="secret", agentid="10001"),
            ldap=LDAPConfig(server="dc.example.com", domain="example.com", username="svc", password="secret"),
            domain="example.com",
            source_provider="wecom",
        )
        provider = build_source_provider(app_config=config, api_factory=FakeWeComClient)
        try:
            departments = provider.list_departments()
            self.assertEqual(len(departments), 1)
            self.assertEqual(departments[0].department_id, 1)
            self.assertEqual(departments[0].name, "HQ")

            users = provider.list_department_users(1)
            self.assertEqual(len(users), 1)
            self.assertEqual(users[0].userid, "alice")
            self.assertEqual(users[0].email, "alice@example.com")
            self.assertEqual(config.source_connector.corpid, "corp-id")

            detail = provider.get_user_detail("alice")
            self.assertEqual(detail["email"], "alice.detail@example.com")
            self.assertEqual(provider.list_tag_records()[0]["tagname"], "IT")
            self.assertEqual(provider.get_tag_users("101")["userlist"][0]["userid"], "alice")
            self.assertEqual(provider.get_external_group_chat("chat_01")["name"], "Partners")
        finally:
            provider.close()

    def test_build_source_provider_rejects_unknown_provider(self):
        with self.assertRaisesRegex(ValueError, "unsupported source provider"):
            build_source_provider(
                wecom_config=WeComConfig(corpid="corp-id", corpsecret="secret"),
                provider_type="custom",
                api_factory=FakeWeComClient,
            )

    def test_build_source_provider_rejects_not_implemented_provider(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp-id", corpsecret="secret"),
            ldap=LDAPConfig(server="dc.example.com", domain="example.com", username="svc", password="secret"),
            domain="example.com",
            source_provider="feishu",
        )
        with self.assertRaisesRegex(ValueError, "not implemented in this build"):
            build_source_provider(app_config=config, api_factory=FakeWeComClient)

    def test_build_source_provider_uses_app_config_provider_type(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="ding-app-key", corpsecret="ding-app-secret"),
            ldap=LDAPConfig(server="dc.example.com", domain="example.com", username="svc", password="secret"),
            domain="example.com",
            source_provider="dingtalk",
        )
        provider = build_source_provider(app_config=config, api_factory=FakeDingTalkClient)
        try:
            departments = provider.list_departments()
            self.assertEqual(departments[0].department_id, 10)
            users = provider.list_department_users(10)
            self.assertEqual(users[0].userid, "alice.dd")
            self.assertEqual(users[0].declared_primary_department_id(), 10)
        finally:
            provider.close()

    def test_test_source_connection_uses_provider_display_name(self):
        with patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComClient):
            success, message = test_source_connection("corp-id", "secret", "10001", source_provider="wecom")

        self.assertTrue(success)
        self.assertIn("WeCom connection succeeded", message)

    def test_test_source_connection_supports_dingtalk_provider(self):
        with patch("sync_app.providers.source.dingtalk.DingTalkAPI", FakeDingTalkClient):
            success, message = test_source_connection("app-key", "app-secret", source_provider="dingtalk")

        self.assertTrue(success)
        self.assertIn("DingTalk connection succeeded", message)


if __name__ == "__main__":
    unittest.main()
