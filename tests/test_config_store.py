import os
import unittest
from pathlib import Path

from sync_app.core.config import load_sync_config
from sync_app.services.config_store import load_editable_config, save_editable_config


class ConfigStoreTests(unittest.TestCase):
    def test_save_and_load_roundtrip_masks_secrets_but_keeps_status(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        config_path = test_root / "config_store_roundtrip.ini"
        try:
            if config_path.exists():
                config_path.unlink()
            save_editable_config(
                {
                    "source_provider": "wecom",
                    "corpid": "corp-001",
                    "agentid": "10001",
                    "corpsecret": "secret-001",
                    "webhook_url": "https://example.invalid/cgi-bin/webhook/send?key=test",
                    "ldap_server": "dc01.example.local",
                    "ldap_domain": "example.local",
                    "ldap_username": "administrator",
                    "ldap_password": "Password123!",
                    "ldap_use_ssl": True,
                    "ldap_port": 636,
                    "ldap_validate_cert": True,
                    "ldap_ca_cert_path": "C:/certs/ad-ca.pem",
                    "default_password": "Stronger123!456",
                    "force_change_password": True,
                    "password_complexity": "strong",
                    "schedule_time": "03:30",
                    "retry_interval": 45,
                    "max_retries": 4,
                },
                config_path=str(config_path),
            )

            loaded = load_editable_config(str(config_path))
            self.assertEqual(loaded["source_provider"], "wecom")
            self.assertEqual(loaded["corpid"], "corp-001")
            self.assertEqual(loaded["agentid"], "10001")
            self.assertEqual(loaded["corpsecret"], "")
            self.assertTrue(loaded["corpsecret_configured"])
            self.assertEqual(loaded["webhook_url"], "")
            self.assertTrue(loaded["webhook_url_configured"])
            self.assertEqual(loaded["ldap_server"], "dc01.example.local")
            self.assertEqual(loaded["ldap_domain"], "example.local")
            self.assertEqual(loaded["ldap_password"], "")
            self.assertTrue(loaded["ldap_password_configured"])
            self.assertTrue(loaded["ldap_use_ssl"])
            self.assertTrue(loaded["ldap_validate_cert"])
            self.assertEqual(loaded["ldap_ca_cert_path"], "C:/certs/ad-ca.pem")
            self.assertEqual(loaded["default_password"], "")
            self.assertTrue(loaded["default_password_configured"])
            self.assertEqual(loaded["password_complexity"], "strong")
            self.assertEqual(loaded["schedule_time"], "03:30")
            self.assertEqual(loaded["retry_interval"], 45)
            self.assertEqual(loaded["max_retries"], 4)

            config = load_sync_config(str(config_path))
            self.assertEqual(config.source_provider, "wecom")
            self.assertEqual(config.wecom.agentid, "10001")
            self.assertEqual(config.source_connector.agentid, "10001")
            self.assertEqual(config.to_public_dict()["source_connector"]["corpid"], "corp-001")
            self.assertTrue(config.ldap.validate_cert)
            self.assertEqual(config.ldap.ca_cert_path, "C:/certs/ad-ca.pem")
            self.assertEqual(config.account.default_password, "Stronger123!456")
        finally:
            if config_path.exists():
                config_path.unlink()

    def test_save_preserves_existing_secrets_when_form_submits_blank_values(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        config_path = test_root / "config_store_preserve.ini"
        try:
            if config_path.exists():
                config_path.unlink()
            save_editable_config(
                {
                    "source_provider": "wecom",
                    "corpid": "corp-001",
                    "agentid": "10001",
                    "corpsecret": "secret-001",
                    "webhook_url": "https://example.invalid/cgi-bin/webhook/send?key=test",
                    "ldap_server": "dc01.example.local",
                    "ldap_domain": "example.local",
                    "ldap_username": "administrator",
                    "ldap_password": "Password123!",
                    "ldap_use_ssl": True,
                    "ldap_port": 636,
                    "ldap_validate_cert": False,
                    "ldap_ca_cert_path": "",
                    "default_password": "Stronger123!456",
                    "force_change_password": False,
                    "password_complexity": "medium",
                    "schedule_time": "03:30",
                    "retry_interval": 45,
                    "max_retries": 4,
                },
                config_path=str(config_path),
            )

            save_editable_config(
                {
                    "source_provider": "wecom",
                    "corpid": "corp-002",
                    "agentid": "10002",
                    "corpsecret": "",
                    "webhook_url": "",
                    "ldap_server": "dc02.example.local",
                    "ldap_domain": "example.local",
                    "ldap_username": "svc_sync",
                    "ldap_password": "",
                    "ldap_use_ssl": True,
                    "ldap_port": 636,
                    "ldap_validate_cert": True,
                    "ldap_ca_cert_path": "C:/certs/new-ca.pem",
                    "default_password": "",
                    "force_change_password": True,
                    "password_complexity": "strong",
                    "schedule_time": "04:00",
                    "retry_interval": 60,
                    "max_retries": 5,
                },
                config_path=str(config_path),
            )

            config = load_sync_config(str(config_path))
            self.assertEqual(config.source_provider, "wecom")
            self.assertEqual(config.wecom.corpid, "corp-002")
            self.assertEqual(config.source_connector.corpid, "corp-002")
            self.assertEqual(config.wecom.agentid, "10002")
            self.assertEqual(config.wecom.corpsecret, "secret-001")
            self.assertEqual(config.webhook_url, "https://example.invalid/cgi-bin/webhook/send?key=test")
            self.assertEqual(config.ldap.server, "dc02.example.local")
            self.assertEqual(config.ldap.username, "svc_sync")
            self.assertEqual(config.ldap.password, "Password123!")
            self.assertTrue(config.ldap.validate_cert)
            self.assertEqual(config.ldap.ca_cert_path, "C:/certs/new-ca.pem")
            self.assertEqual(config.account.default_password, "Stronger123!456")
            self.assertTrue(config.account.force_change_password)
            self.assertEqual(config.account.password_complexity, "strong")
        finally:
            if config_path.exists():
                config_path.unlink()

    def test_load_sync_config_accepts_generic_source_sections_without_legacy_wechat_sections(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        config_path = test_root / "config_store_generic_sections.ini"
        try:
            config_path.write_text(
                "\n".join(
                    [
                        "[Source]",
                        "Provider = dingtalk",
                        "",
                        "[SourceConnector]",
                        "CorpID = ding-app-key",
                        "AgentID = 9001",
                        "CorpSecret = ding-app-secret",
                        "",
                        "[Notification]",
                        "WebhookUrl = https://oapi.dingtalk.com/robot/send?access_token=test",
                        "",
                        "[LDAP]",
                        "Server = dc01.example.local",
                        "Domain = example.local",
                        "Username = EXAMPLE\\\\administrator",
                        "Password = Password123!",
                        "UseSSL = true",
                        "Port = 636",
                        "",
                        "[Account]",
                        "DefaultPassword = simple888",
                        "ForceChangePassword = true",
                        "PasswordComplexity = medium",
                    ]
                ),
                encoding="utf-8",
            )

            config = load_sync_config(str(config_path))
            self.assertEqual(config.source_provider, "dingtalk")
            self.assertEqual(config.source_connector.corpid, "ding-app-key")
            self.assertEqual(config.source_connector.agentid, "9001")
            self.assertEqual(config.source_connector.corpsecret, "ding-app-secret")
            self.assertEqual(config.webhook_url, "https://oapi.dingtalk.com/robot/send?access_token=test")
            self.assertEqual(config.ldap.server, "dc01.example.local")
            self.assertEqual(config.account.default_password, "simple888")
        finally:
            if config_path.exists():
                config_path.unlink()


if __name__ == "__main__":
    unittest.main()
