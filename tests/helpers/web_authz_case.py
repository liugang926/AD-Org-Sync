import os
import re
import unittest
from pathlib import Path
from urllib.parse import urlencode

from starlette.requests import Request

from sync_app.services.typed_settings import BrandingSettings, DirectoryUiSettings, SSPRSettings, WebRuntimeSettings
from sync_app.services.config_store import save_editable_config
from sync_app.web import create_app
from sync_app.web.security import hash_password


class WebAuthzBaseTestCase(unittest.TestCase):
    def setUp(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        self.db_path = test_root / "web_authz.db"
        self.config_path = test_root / "web_authz.ini"

        for path in (self.db_path, self.config_path):
            if path.exists():
                path.unlink()

        save_editable_config(
            {
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
                "default_password": "ChangeMe123!",
                "force_change_password": True,
                "password_complexity": "strong",
                "schedule_time": "03:00",
                "retry_interval": 60,
                "max_retries": 3,
            },
            config_path=str(self.config_path),
        )

        self.app = create_app(db_path=str(self.db_path), config_path=str(self.config_path))
        self.app.state.user_repo.create_user("superadmin", hash_password("Admin123!"), role="super_admin")
        self.app.state.user_repo.create_user("operator1", hash_password("Admin123!"), role="operator")
        self.app.state.user_repo.create_user("auditor1", hash_password("Admin123!"), role="auditor")
        self.session: dict[str, str] = {}

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_path) + suffix)
            if path.exists():
                try:
                    path.unlink()
                except PermissionError:
                    pass
        if self.config_path.exists():
            self.config_path.unlink()
        for config_file in self.db_path.parent.glob("web_authz*.ini"):
            if config_file == self.config_path:
                continue
            try:
                config_file.unlink()
            except PermissionError:
                pass
        backup_dir = self.db_path.parent / "backups"
        if backup_dir.exists():
            for item in backup_dir.glob("*"):
                try:
                    item.unlink()
                except PermissionError:
                    pass

    def _route(self, path: str, method: str):
        method = method.upper()
        for route in self.app.router.routes:
            if getattr(route, "path", None) == path and method in getattr(route, "methods", set()):
                return route.endpoint
        raise AssertionError(f"route not found: {method} {path}")

    def _request(
        self,
        path: str,
        method: str = "GET",
        *,
        query: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Request:
        query_string = urlencode(query or {}).encode("utf-8")
        scope = {
            "type": "http",
            "method": method.upper(),
            "path": path,
            "headers": [
                (str(key).lower().encode("utf-8"), str(value).encode("utf-8"))
                for key, value in (headers or {}).items()
            ],
            "query_string": query_string,
            "app": self.app,
            "session": self.session,
        }
        return Request(scope)

    @staticmethod
    def _response_body(response) -> bytes:
        render_for_test = getattr(response, "render_for_test", None)
        if callable(render_for_test):
            return render_for_test()
        body = getattr(response, "body", None)
        if body is not None:
            return body
        raise AssertionError(f"response does not expose body bytes: {response!r}")

    @classmethod
    def _text(cls, response) -> str:
        return cls._response_body(response).decode("utf-8")

    def _login(self, username: str):
        self.session = {}
        login_page = self._route("/login", "GET")(self._request("/login"))
        self.assertEqual(login_page.status_code, 200)
        match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(login_page))
        self.assertIsNotNone(match)
        response = self._route("/login", "POST")(
            self._request("/login", "POST"),
            csrf_token=match.group(1),
            username=username,
            password="Admin123!",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(self.session.get("username"), username)

    def _build_config_form_payload(self, **overrides):
        current_org = self.app.state.organization_repo.get_default_organization_record()
        self.assertIsNotNone(current_org)
        editable = self.app.state.org_config_repo.get_editable_config(
            current_org.org_id,
            config_path=str(self.config_path),
        )
        directory_ui_settings = DirectoryUiSettings.load(
            self.app.state.settings_repo,
            org_id=current_org.org_id,
        )
        web_runtime_settings = WebRuntimeSettings.load(self.app.state.settings_repo)
        sspr_settings = SSPRSettings.load(self.app.state.settings_repo, org_id=current_org.org_id)
        branding_settings = BrandingSettings.load(
            self.app.state.settings_repo,
            default_display_name="AD Org Sync",
            default_mark_text="AD",
            default_attribution="微信公众号：大刘讲IT",
        )
        payload = {
            "corpid": editable["corpid"],
            "agentid": editable["agentid"],
            "corpsecret": "secret-001",
            "webhook_url": "https://example.invalid/cgi-bin/webhook/send?key=test",
            "ldap_server": editable["ldap_server"],
            "ldap_domain": editable["ldap_domain"],
            "ldap_username": editable["ldap_username"],
            "ldap_password": "Password123!",
            "ldap_port": editable["ldap_port"],
            "ldap_use_ssl": "true" if editable["ldap_use_ssl"] else "false",
            "ldap_validate_cert": "true" if editable["ldap_validate_cert"] else "false",
            "ldap_ca_cert_path": editable["ldap_ca_cert_path"],
            "default_password": "ChangeMe123!",
            "force_change_password": "true" if editable["force_change_password"] else "false",
            "password_complexity": editable["password_complexity"],
            "schedule_time": editable["schedule_time"],
            "retry_interval": editable["retry_interval"],
            "max_retries": editable["max_retries"],
            "group_display_separator": directory_ui_settings.group_display_separator,
            "group_recursive_enabled": "true" if directory_ui_settings.group_recursive_enabled else "false",
            "managed_relation_cleanup_enabled": "true"
            if directory_ui_settings.managed_relation_cleanup_enabled
            else "false",
            "schedule_execution_mode": directory_ui_settings.schedule_execution_mode,
            "web_bind_host": web_runtime_settings.bind_host,
            "web_bind_port": web_runtime_settings.bind_port,
            "web_public_base_url": web_runtime_settings.public_base_url,
            "web_session_cookie_secure_mode": web_runtime_settings.session_cookie_secure_mode,
            "web_trust_proxy_headers": "true" if web_runtime_settings.trust_proxy_headers else "false",
            "web_forwarded_allow_ips": web_runtime_settings.forwarded_allow_ips,
            "sspr_enabled": "true" if sspr_settings.enabled else "false",
            "sspr_min_password_length": sspr_settings.min_password_length,
            "sspr_unlock_account_default": "true" if sspr_settings.unlock_account_default else "false",
            "sspr_verification_session_ttl_seconds": sspr_settings.verification_session_ttl_seconds,
            "brand_display_name": branding_settings.brand_display_name,
            "brand_mark_text": branding_settings.brand_mark_text,
            "brand_attribution": branding_settings.brand_attribution,
            "user_ou_placement_strategy": directory_ui_settings.user_ou_placement_strategy,
            "source_root_unit_ids": directory_ui_settings.source_root_unit_ids,
            "directory_root_ou_path": directory_ui_settings.directory_root_ou_path,
            "disabled_users_ou_path": directory_ui_settings.disabled_users_ou_path,
            "custom_group_ou_path": directory_ui_settings.custom_group_ou_path,
            "soft_excluded_groups": "\n".join(
                self.app.state.exclusion_repo.list_soft_excluded_group_names(
                    enabled_only=False,
                    org_id=current_org.org_id,
                )
            ),
        }
        payload.update(overrides)
        return payload
