import re
from http.cookies import SimpleCookie
from unittest.mock import patch

from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class WebOIDCRouteTests(WebAuthzBaseTestCase):
    def setUp(self):
        self.environment_patch = patch.dict(
            "os.environ",
            {
                "AD_ORG_SYNC_OIDC_ENABLED": "true",
                "AD_ORG_SYNC_OIDC_DISCOVERY_URL": "https://id.example/.well-known/openid-configuration",
                "AD_ORG_SYNC_OIDC_CLIENT_ID": "console-client",
                "AD_ORG_SYNC_OIDC_CLIENT_SECRET": "secret",
                "AD_ORG_SYNC_OIDC_DISPLAY_NAME": "Example Identity",
                "AD_ORG_SYNC_OIDC_MFA_REQUIRED": "true",
                "AD_ORG_SYNC_PASSWORD_RESET_URL": "https://id.example/reset",
                "AD_ORG_SYNC_ENVIRONMENT_LABEL": "Production / Shanghai",
            },
            clear=False,
        )
        self.environment_patch.start()
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.environment_patch.stop()

    def test_login_page_surfaces_sso_mfa_environment_and_recovery(self):
        response = self._route("/login", "GET")(self._request("/login"))
        text = self._text(response)

        self.assertIn("Production / Shanghai", text)
        self.assertIn("Sign in with Example Identity", text)
        self.assertIn('class="login-sso-primary"', text)
        self.assertIn('class="login-local-fallback"', text)
        self.assertIn("Use local administrator password", text)
        self.assertIn('href="/auth/oidc/start"', text)
        self.assertIn("Required for SSO", text)
        self.assertIn('href="https://id.example/reset"', text)
        self.assertIn('autocomplete="username"', text)
        self.assertIn('autocomplete="current-password"', text)

    def test_local_login_records_auth_method_and_signed_recent_browser_cookie(self):
        login_page = self._route("/login", "GET")(self._request("/login"))
        csrf_match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(login_page))
        self.assertIsNotNone(csrf_match)

        response = self._route("/login", "POST")(
            self._request("/login", "POST"),
            csrf_token=csrf_match.group(1),
            username="superadmin",
            password="Admin123!",
        )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(self.session.get("auth_method"), "local_password")
        self.assertFalse(self.session.get("mfa_satisfied"))
        cookie = SimpleCookie()
        cookie.load(response.headers["set-cookie"])
        recent_cookie = cookie["ad_org_sync_recent_login"].value
        self.assertTrue(recent_cookie)

        self.session = {}
        recent_page = self._route("/login", "GET")(
            self._request(
                "/login",
                headers={"Cookie": f"ad_org_sync_recent_login={recent_cookie}"},
            )
        )
        recent_text = self._text(recent_page)
        self.assertIn("Recent Login On This Browser", recent_text)
        self.assertIn("Local password", recent_text)


if __name__ == "__main__":
    import unittest

    unittest.main()
