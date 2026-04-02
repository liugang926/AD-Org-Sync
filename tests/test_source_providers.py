import unittest
from unittest.mock import patch

from sync_app.core.config import test_source_connection
from sync_app.core.models import AppConfig, LDAPConfig, WeComConfig
from sync_app.providers.source import build_source_provider, get_source_provider_display_name, normalize_source_provider


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


class SourceProviderTests(unittest.TestCase):
    def test_normalize_source_provider_defaults_to_wecom(self):
        self.assertEqual(normalize_source_provider(None), "wecom")
        self.assertEqual(normalize_source_provider(""), "wecom")
        self.assertEqual(normalize_source_provider("WeCom"), "wecom")

    def test_get_source_provider_display_name_uses_known_label(self):
        self.assertEqual(get_source_provider_display_name("wecom"), "WeCom")
        self.assertEqual(get_source_provider_display_name("WeCom"), "WeCom")
        self.assertEqual(get_source_provider_display_name("custom"), "custom")

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
                provider_type="dingtalk",
                api_factory=FakeWeComClient,
            )

    def test_build_source_provider_uses_app_config_provider_type(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp-id", corpsecret="secret"),
            ldap=LDAPConfig(server="dc.example.com", domain="example.com", username="svc", password="secret"),
            domain="example.com",
            source_provider="dingtalk",
        )
        with self.assertRaisesRegex(ValueError, "unsupported source provider"):
            build_source_provider(app_config=config, api_factory=FakeWeComClient)

    def test_test_source_connection_uses_provider_display_name(self):
        with patch("sync_app.core.config.WeComAPI", FakeWeComClient):
            success, message = test_source_connection("corp-id", "secret", "10001", source_provider="wecom")

        self.assertTrue(success)
        self.assertIn("WeCom connection succeeded", message)


if __name__ == "__main__":
    unittest.main()
