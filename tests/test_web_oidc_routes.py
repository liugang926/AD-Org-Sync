import re
from http.cookies import SimpleCookie
from unittest.mock import patch

from sync_app.web.oidc import OIDCIdentity
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
        self.assertIn('method="post" action="/auth/oidc/start"', text)
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

    def test_oidc_get_handoffs_do_not_consume_session_or_write_audit(self):
        self.session["_oidc_transaction"] = {
            "state": "expected-state",
            "nonce": "nonce",
            "verifier": "verifier",
            "redirect_uri": "https://console.example/auth/oidc/callback",
        }
        before_logs = len(self.app.state.audit_repo.list_recent_logs(100))

        legacy_start = self._route("/auth/oidc/start", "GET")(
            self._request("/auth/oidc/start")
        )
        callback = self._route("/auth/oidc/callback", "GET")(
            self._request(
                "/auth/oidc/callback",
                query={"code": "returned-code", "state": "expected-state"},
            )
        )

        self.assertEqual(legacy_start.status_code, 303)
        self.assertEqual(legacy_start.headers["location"], "/login")
        callback_text = self._text(callback)
        self.assertIn('method="post" action="/auth/oidc/callback"', callback_text)
        self.assertIn('name="code" value="returned-code"', callback_text)
        self.assertIn('name="state" value="expected-state"', callback_text)
        self.assertEqual(callback.headers["cache-control"], "no-store, max-age=0")
        self.assertEqual(callback.headers["pragma"], "no-cache")
        self.assertEqual(callback.headers["referrer-policy"], "no-referrer")
        self.assertEqual(self.session["_oidc_transaction"]["state"], "expected-state")
        self.assertEqual(
            len(self.app.state.audit_repo.list_recent_logs(100)),
            before_logs,
        )

    def test_oidc_callback_writes_login_state_only_on_csrf_checked_post(self):
        login_page = self._route("/login", "GET")(self._request("/login"))
        csrf_match = re.search(r'name="csrf_token" value="([^"]+)"', self._text(login_page))
        self.assertIsNotNone(csrf_match)
        self.session["_oidc_transaction"] = {
            "state": "expected-state",
            "nonce": "nonce",
            "verifier": "verifier",
            "redirect_uri": "https://console.example/auth/oidc/callback",
        }

        with patch(
            "sync_app.web.routes_auth.OIDCService.finish",
            return_value=OIDCIdentity(
                username="superadmin",
                subject="subject-1",
                issuer="https://id.example",
                mfa_methods=("mfa",),
            ),
        ):
            response = self._route("/auth/oidc/callback", "POST")(
                self._request("/auth/oidc/callback", "POST"),
                csrf_token=csrf_match.group(1),
                code="returned-code",
                state="expected-state",
                error="",
                error_description="",
            )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")
        self.assertEqual(self.session["username"], "superadmin")
        self.assertEqual(self.session["auth_method"], "oidc")
        self.assertIn(
            "auth.login",
            [item.action_type for item in self.app.state.audit_repo.list_recent_logs(10)],
        )


if __name__ == "__main__":
    import unittest

    unittest.main()
