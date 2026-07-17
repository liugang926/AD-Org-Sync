from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from sync_app.modules.sspr import SSPRVerifiedIdentity
from sync_app.modules.sspr.repositories import hash_capability
from sync_app.services.typed_settings import SSPRSettings
from sync_app.web.app import create_app


VALID_PASSWORD = "N3w!SecurePass"


class FakeDingTalkProvider:
    def __init__(self):
        self.closed = False
        self.codes = []

    def verify_employee_identity(self, request):
        self.codes.append(request.verification_code)
        return SSPRVerifiedIdentity(
            org_id=request.org_id,
            provider_id="dingtalk",
            connector_id=request.connector_id,
            source_user_id="alice.dd",
            display_name="Alice Ding",
        )

    def close(self):
        self.closed = True


class FakeTargetProvider:
    def __init__(self):
        self.reset_calls = []
        self.unlock_calls = []
        self.state_calls = []

    def get_user_account_state(self, username):
        self.state_calls.append(username)
        return {
            "available": True,
            "exists": True,
            "enabled": True,
            "locked": True,
            "domain": "example.test",
        }

    def reset_user_password(self, username, new_password, *, force_change_at_next_login=False):
        self.reset_calls.append((username, new_password, force_change_at_next_login))
        return True

    def unlock_user(self, username):
        self.unlock_calls.append(username)
        return True

    def close(self):
        return None


class SSPRWebRouteTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        config_path = Path(self.temp_dir.name) / "config.ini"
        self.app = create_app(
            db_path=str(Path(self.temp_dir.name) / "app.db"),
            config_path=str(config_path),
            public_base_url="https://it-service.example.test:9443",
        )
        values = self.app.state.org_config_repo.get_raw_config(
            "default",
            config_path=str(config_path),
        )
        values.update(
            {
                "source_provider": "dingtalk",
                "corpid": "ding-app-key",
                "corpsecret": "test-app-secret",
                "agentid": "12345",
                "ldap_server": "dc.example.test",
                "ldap_domain": "example.test",
                "ldap_username": "svc-sync",
                "ldap_password": "test-directory-secret",
                "ldap_use_ssl": True,
                "ldap_port": 636,
                "ldap_validate_cert": True,
                "password_complexity": "strong",
            }
        )
        self.app.state.org_config_repo.save_config(
            "default",
            values,
            config_path=str(config_path),
        )
        SSPRSettings(
            enabled=True,
            dingtalk_corp_id="ding-corp-id",
            min_password_length=12,
            unlock_account_default=True,
            verification_session_ttl_seconds=600,
        ).persist(self.app.state.settings_repo, org_id="default")
        self.app.state.user_binding_repo.upsert_binding(
            "alice.dd",
            "alice.ad",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            source_display_name="Alice Ding",
            preserve_manual=False,
        )
        self.source = FakeDingTalkProvider()
        self.target = FakeTargetProvider()
        self.source_patch = patch(
            "sync_app.web.routes_sspr.build_source_provider",
            side_effect=lambda **_kwargs: self.source,
        )
        self.target_patch = patch(
            "sync_app.web.routes_sspr.build_target_provider",
            side_effect=lambda **_kwargs: self.target,
        )
        self.source_patch.start()
        self.target_patch.start()
        self.addCleanup(self.source_patch.stop)
        self.addCleanup(self.target_patch.stop)
        self.client = TestClient(self.app, base_url="https://testserver")
        self.addCleanup(self.client.close)

    def _cookie(self, name):
        matches = [cookie.value for cookie in self.client.cookies.jar if cookie.name == name]
        return matches[-1] if matches else ""

    def _start_oauth(self):
        entry = self.client.get("/sspr?corpid=ding-corp-id")
        self.assertEqual(entry.status_code, 200)
        csrf = self._cookie("ad_org_sync_sspr_start_csrf")
        self.assertTrue(csrf)
        start = self.client.post(
            "/sspr/oauth/start",
            data={
                "csrf_token": csrf,
                "corpid": "ding-corp-id",
            },
        )
        self.assertEqual(start.status_code, 200)
        return start

    def _authenticate(self):
        self._start_oauth()
        state = self._cookie("ad_org_sync_sspr_oauth")
        self.assertTrue(state)
        response = self.client.post(
            "/sspr/auth/dingtalk",
            json={"state": state, "authCode": "browser-one-time-code"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["nextUrl"], "/sspr/account")
        return response

    def test_sspr_bypasses_admin_login_and_starts_dingtalk_automatically(self):
        response = self.client.get("/sspr?corpid=ding-corp-id", follow_redirects=False)

        self.assertEqual(response.status_code, 200)
        self.assertIn("data-sspr-start", response.text)
        self.assertNotIn("/login", response.text)
        start = self._start_oauth()
        self.assertIn("data-sspr-auth", start.text)
        self.assertIn("requestAuthCode", self.client.get("/static/sspr.js").text)
        self.assertEqual(start.headers["cache-control"], "no-store, max-age=0")
        self.assertIn("g.alicdn.com", start.headers["content-security-policy"])

        unrelated = self.client.get("/sspr-not-a-portal-route", follow_redirects=False)
        self.assertEqual(unrelated.status_code, 303)
        self.assertIn(unrelated.headers["location"], {"/setup", "/login"})

    def test_unverified_browser_cannot_view_or_reset_an_account(self):
        account = self.client.get("/sspr/account", follow_redirects=False)
        reset = self.client.post(
            "/sspr/password/reset",
            data={"csrf_token": "forged", "new_password": VALID_PASSWORD, "confirm_password": VALID_PASSWORD},
            follow_redirects=False,
        )

        self.assertEqual(account.status_code, 303)
        self.assertEqual(account.headers["location"], "/sspr")
        self.assertEqual(reset.status_code, 303)
        self.assertEqual(self.target.reset_calls, [])

    def test_verified_employee_sees_only_server_bound_account(self):
        auth = self._authenticate()
        self.assertEqual(auth.cookies.get("ad_org_sync_sspr"), self._cookie("ad_org_sync_sspr"))

        account = self.client.get("/sspr/account?source_user_id=someone-else&ad_username=administrator")

        self.assertEqual(account.status_code, 200)
        self.assertIn("Alice Ding", account.text)
        self.assertIn("alice.ad", account.text)
        self.assertNotIn("administrator", account.text)
        self.assertIn("DingTalk Verified", account.text)
        session_cookie = next(
            cookie for cookie in self.client.cookies.jar if cookie.name == "ad_org_sync_sspr"
        )
        self.assertTrue(session_cookie.secure)
        self.assertTrue(session_cookie.has_nonstandard_attr("HttpOnly"))
        self.assertEqual(session_cookie.path, "/sspr")

    def test_reset_rejects_csrf_and_password_mismatch_without_calling_directory(self):
        self._authenticate()
        csrf = self._cookie("ad_org_sync_sspr_csrf")
        csrf_failure = self.client.post(
            "/sspr/password/reset",
            data={"csrf_token": "forged", "new_password": VALID_PASSWORD, "confirm_password": VALID_PASSWORD},
        )
        mismatch = self.client.post(
            "/sspr/password/reset",
            data={"csrf_token": csrf, "new_password": VALID_PASSWORD, "confirm_password": "Different1!Pass"},
            follow_redirects=False,
        )

        self.assertEqual(csrf_failure.status_code, 403)
        self.assertEqual(mismatch.status_code, 303)
        self.assertEqual(mismatch.headers["location"], "/sspr/account?error=password_mismatch")
        self.assertEqual(self.target.reset_calls, [])

    def test_success_uses_prg_consumes_session_and_prevents_duplicate_reset(self):
        self._authenticate()
        session = self._cookie("ad_org_sync_sspr")
        csrf = self._cookie("ad_org_sync_sspr_csrf")
        payload = {
            "csrf_token": csrf,
            "new_password": VALID_PASSWORD,
            "confirm_password": VALID_PASSWORD,
            "unlock_account": "true",
            "source_user_id": "victim",
            "ad_username": "administrator",
            "connector_id": "other",
            "org_id": "other",
        }

        reset = self.client.post("/sspr/password/reset", data=payload, follow_redirects=False)

        self.assertEqual(reset.status_code, 303)
        self.assertEqual(reset.headers["location"], "/sspr/result")
        correlation_id = reset.headers["x-correlation-id"]
        self.assertTrue(correlation_id)
        self.assertEqual(self.target.reset_calls, [("alice.ad", VALID_PASSWORD, False)])
        self.assertEqual(self.target.unlock_calls, ["alice.ad"])
        result = self.client.get("/sspr/result")
        self.assertIn("alice.ad", result.text)
        self.assertNotIn(VALID_PASSWORD, result.text)
        self.assertIsNone(
            self.app.state.sspr_session_store.validate_session(session, user_agent="testclient")
        )

        replay = self.client.post("/sspr/password/reset", data=payload, follow_redirects=False)
        self.assertEqual(replay.status_code, 303)
        self.assertEqual(len(self.target.reset_calls), 1)
        audit_payloads = [
            log.payload
            for log in self.app.state.audit_repo.list_recent_logs(20)
            if log.action_type == "sspr.password_reset"
        ]
        self.assertEqual(audit_payloads[0]["correlation_id"], correlation_id)
        self.assertNotIn(VALID_PASSWORD, str(audit_payloads))
        self.assertNotIn(session, str(audit_payloads))

    def test_oauth_state_is_one_time_and_org_configuration_change_is_rejected(self):
        self._start_oauth()
        state = self._cookie("ad_org_sync_sspr_oauth")
        SSPRSettings(
            enabled=True,
            dingtalk_corp_id="different-corp-id",
            min_password_length=12,
            verification_session_ttl_seconds=600,
        ).persist(self.app.state.settings_repo, org_id="default")

        changed = self.client.post(
            "/sspr/auth/dingtalk",
            json={"state": state, "authCode": "browser-one-time-code"},
        )
        replay = self.client.post(
            "/sspr/auth/dingtalk",
            json={"state": state, "authCode": "browser-one-time-code"},
        )

        self.assertEqual(changed.status_code, 403)
        self.assertEqual(changed.json()["status"], "organization_mismatch")
        self.assertEqual(replay.status_code, 403)
        self.assertEqual(self.source.codes, [])

    def test_duplicate_enabled_corp_id_mapping_fails_closed(self):
        config_path = Path(self.temp_dir.name) / "duplicate.ini"
        self.app.state.organization_repo.upsert_organization(
            org_id="duplicate",
            name="Duplicate",
            config_path=str(config_path),
            is_enabled=True,
        )
        values = self.app.state.org_config_repo.get_raw_config(
            "default",
            config_path=str(Path(self.temp_dir.name) / "config.ini"),
        )
        self.app.state.org_config_repo.save_config(
            "duplicate",
            values,
            config_path=str(config_path),
        )
        SSPRSettings(
            enabled=True,
            dingtalk_corp_id="ding-corp-id",
            min_password_length=12,
            verification_session_ttl_seconds=600,
        ).persist(self.app.state.settings_repo, org_id="duplicate")

        entry = self.client.get("/sspr?corpid=ding-corp-id")

        self.assertEqual(entry.status_code, 200)
        self.assertNotIn("data-sspr-start", entry.text)
        self.assertNotIn("data-sspr-auth", entry.text)
        self.assertEqual(self.source.codes, [])

    def test_unbound_disabled_and_protected_accounts_never_reach_directory_reset(self):
        self.app.state.user_binding_repo.set_enabled("alice.dd", False, org_id="default")
        self._authenticate()
        unbound = self.client.get("/sspr/account")
        self.assertIn("not bound", unbound.text)
        self.assertNotIn("alice.ad", unbound.text)
        self.assertEqual(self.target.reset_calls, [])

    def test_protected_account_is_blocked_before_target_directory_access(self):
        self.app.state.user_binding_repo.upsert_binding(
            "alice.dd",
            "administrator",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            preserve_manual=False,
        )
        self._authenticate()

        account = self.client.get("/sspr/account")

        self.assertEqual(account.status_code, 200)
        self.assertIn("cannot use self-service password reset", account.text)
        self.assertNotIn("administrator</dd>", account.text)
        self.assertEqual(self.target.state_calls, [])
        self.assertEqual(self.target.reset_calls, [])

    def test_expired_session_is_cleared_and_cannot_view_account(self):
        self._authenticate()
        session = self._cookie("ad_org_sync_sspr")
        with self.app.state.db_manager.transaction() as connection:
            connection.execute(
                "UPDATE sspr_verification_sessions SET expires_at = '2000-01-01T00:00:00+00:00' WHERE token_hash = ?",
                (hash_capability(session),),
            )

        account = self.client.get("/sspr/account", follow_redirects=False)

        self.assertEqual(account.status_code, 303)
        self.assertEqual(account.headers["location"], "/sspr?reason=session_expired")
        self.assertEqual(self.target.state_calls, [])
        self.assertNotIn(
            "sspr.session.expired",
            [item.action_type for item in self.app.state.audit_repo.list_recent_logs(10)],
        )

    def test_reset_attempts_are_rate_limited_with_retry_after(self):
        self._authenticate()
        csrf = self._cookie("ad_org_sync_sspr_csrf")
        responses = []
        for _ in range(5):
            responses.append(
                self.client.post(
                    "/sspr/password/reset",
                    data={
                        "csrf_token": csrf,
                        "new_password": VALID_PASSWORD,
                        "confirm_password": "Different1!Pass",
                    },
                    follow_redirects=False,
                )
            )

        self.assertEqual(responses[-1].status_code, 429)
        self.assertGreaterEqual(int(responses[-1].headers["retry-after"]), 1)
        self.assertEqual(self.target.reset_calls, [])

    def test_disabled_feature_does_not_start_dingtalk_verification(self):
        SSPRSettings(
            enabled=False,
            dingtalk_corp_id="ding-corp-id",
            min_password_length=12,
            verification_session_ttl_seconds=600,
        ).persist(self.app.state.settings_repo, org_id="default")

        entry = self.client.get("/sspr?corpid=ding-corp-id")
        start = self.client.get(
            "/sspr/oauth/start?corpid=ding-corp-id",
            follow_redirects=False,
        )

        self.assertEqual(entry.status_code, 200)
        self.assertEqual(start.status_code, 303)
        self.assertNotIn("data-sspr-auth", entry.text)
        self.assertEqual(self.source.codes, [])

    def test_existing_session_is_revoked_when_source_provider_changes(self):
        self._authenticate()
        csrf = self._cookie("ad_org_sync_sspr_csrf")
        organization = self.app.state.organization_repo.get_organization_record("default")
        values = self.app.state.org_config_repo.get_raw_config(
            "default",
            config_path=organization.config_path,
        )
        values["source_provider"] = "wecom"
        self.app.state.org_config_repo.save_config(
            "default",
            values,
            config_path=organization.config_path,
        )

        account = self.client.get("/sspr/account", follow_redirects=False)
        reset = self.client.post(
            "/sspr/password/reset",
            data={
                "csrf_token": csrf,
                "new_password": VALID_PASSWORD,
                "confirm_password": VALID_PASSWORD,
            },
            follow_redirects=False,
        )

        self.assertEqual(account.status_code, 303)
        self.assertEqual(account.headers["location"], "/sspr")
        self.assertEqual(reset.status_code, 303)
        self.assertEqual(reset.headers["location"], "/sspr")
        self.assertEqual(self.target.reset_calls, [])

    def test_callback_get_is_read_only_and_redirects_without_accepting_secrets(self):
        callback = self.client.get("/sspr/callback/dingtalk", follow_redirects=False)

        self.assertEqual(callback.status_code, 303)
        self.assertEqual(callback.headers["location"], "/sspr?lang=en")

    def test_oauth_start_requires_matching_double_submit_csrf(self):
        entry = self.client.get("/sspr?corpid=ding-corp-id")
        self.assertEqual(entry.status_code, 200)

        rejected = self.client.post(
            "/sspr/oauth/start",
            data={"csrf_token": "forged", "corpid": "ding-corp-id"},
        )

        self.assertEqual(rejected.status_code, 403)
        self.assertEqual(self._cookie("ad_org_sync_sspr_oauth"), "")


if __name__ == "__main__":
    unittest.main()
