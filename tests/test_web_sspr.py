import re
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class FakeSourceProvider:
    def __init__(self):
        self.closed = False

    def verify_employee_identity(self, request):
        if request.verification_code != "ok":
            return None
        return {
            "org_id": request.org_id,
            "source_user_id": request.source_user_id,
            "provider_id": request.provider_id,
            "display_name": "Alice",
        }

    def close(self):
        self.closed = True


class FakeTargetProvider:
    def __init__(self):
        self.reset_calls = []
        self.unlock_calls = []
        self.closed = False

    def reset_user_password(self, username, new_password, *, force_change_at_next_login=False):
        self.reset_calls.append((username, new_password, force_change_at_next_login))
        return True

    def unlock_user(self, username):
        self.unlock_calls.append(username)
        return True

    def close(self):
        self.closed = True


class WebSSPRTests(WebAuthzBaseTestCase):
    def test_sspr_page_is_public_without_admin_session(self):
        self.session = {}

        with TestClient(self.app) as client:
            response = client.get("/sspr", follow_redirects=False)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Employee Password Reset", response.text)
        self.assertNotIn('href="/dashboard"', response.text)

    def test_verified_employee_can_reset_bound_ad_password(self):
        self.app.state.user_binding_repo.upsert_binding(
            "alice",
            "alice.ad",
            org_id="default",
            source_display_name="Alice",
            source="manual",
        )
        self.session = {}
        page = self._route("/sspr", "GET")(self._request("/sspr"))
        self.assertEqual(page.status_code, 200)
        csrf_token = self._extract_field(self._text(page), "csrf_token")
        target_provider = FakeTargetProvider()
        source_provider = FakeSourceProvider()

        with (
            patch("sync_app.web.app.build_source_provider", return_value=source_provider),
            patch("sync_app.web.app.build_target_provider", return_value=target_provider),
        ):
            verify_response = self._route("/sspr/verify", "POST")(
                self._request("/sspr/verify", "POST"),
                csrf_token=csrf_token,
                org_id="default",
                source_user_id="alice",
                provider_id="wecom",
                verification_code="ok",
            )
            self.assertEqual(verify_response.status_code, 200)
            verify_body = self._text(verify_response)
            self.assertIn("Employee identity verified", verify_body)
            verification_session_id = self._extract_field(verify_body, "verification_session_id")
            self.assertTrue(verification_session_id)

            reset_response = self._route("/sspr/reset", "POST")(
                self._request("/sspr/reset", "POST"),
                csrf_token=csrf_token,
                org_id="default",
                source_user_id="alice",
                provider_id="wecom",
                verification_session_id=verification_session_id,
                new_password="NewSecret123!",
                confirm_password="NewSecret123!",
                unlock_account="1",
            )

        self.assertEqual(reset_response.status_code, 200)
        self.assertIn("Password reset completed", self._text(reset_response))
        self.assertEqual(target_provider.reset_calls, [("alice.ad", "NewSecret123!", False)])
        self.assertEqual(target_provider.unlock_calls, ["alice.ad"])
        self.assertTrue(source_provider.closed)
        self.assertTrue(target_provider.closed)

        logs = self.app.state.audit_repo.list_recent_logs()
        action_types = {record.action_type for record in logs}
        self.assertIn("sspr.verify", action_types)
        self.assertIn("sspr.password_reset", action_types)

    def test_oauth_callback_can_complete_employee_verification(self):
        self.session = {}
        source_provider = FakeSourceProvider()

        with patch("sync_app.web.app.build_source_provider", return_value=source_provider):
            callback_response = self._route("/sspr/callback/{provider_id}", "GET")(
                self._request(
                    "/sspr/callback/wecom",
                    query={
                        "code": "ok",
                        "state": "org_id=default&source_user_id=alice",
                    },
                ),
                provider_id="wecom",
                code="ok",
                state="org_id=default&source_user_id=alice",
            )

        self.assertEqual(callback_response.status_code, 200)
        body = self._text(callback_response)
        self.assertIn("Employee identity verified", body)
        self.assertTrue(self._extract_field(body, "verification_session_id"))
        self.assertTrue(source_provider.closed)

    @staticmethod
    def _extract_field(body: str, field_name: str) -> str:
        match = re.search(rf'name="{re.escape(field_name)}" value="([^"]*)"', body)
        if not match:
            raise AssertionError(f"field not found: {field_name}")
        return match.group(1)
