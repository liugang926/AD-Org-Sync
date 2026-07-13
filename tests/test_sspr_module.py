from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone

from sync_app.core.models import UserIdentityBindingRecord
from sync_app.modules.sspr import (
    InMemorySSPRSessionStore,
    SSPRPasswordResetRequest,
    SSPRRateLimiter,
    SSPRService,
    SSPRVerificationRequest,
    SSPRVerificationService,
    SSPRVerifiedIdentity,
)


VALID_PASSWORD = "N3w!SecurePass"


class FakeBindingRepository:
    def __init__(self, bindings=()):
        self.bindings = list(bindings)
        self.lookup_args = None

    def list_binding_records_for_source_identity(
        self,
        source_user_id,
        *,
        org_id,
        source_provider,
        connector_id=None,
        enabled_only=False,
    ):
        self.lookup_args = {
            "source_user_id": source_user_id,
            "org_id": org_id,
            "source_provider": source_provider,
            "connector_id": connector_id,
            "enabled_only": enabled_only,
        }
        return [
            binding
            for binding in self.bindings
            if binding.source_user_id == source_user_id
            and binding.org_id == org_id
            and binding.source_provider == source_provider
            and (not connector_id or binding.connector_id == connector_id)
            and (not enabled_only or binding.is_enabled)
        ]


class FakeAuditRepository:
    def __init__(self):
        self.logs = []

    def add_log(self, **kwargs):
        self.logs.append(kwargs)
        return len(self.logs)


class FakeTargetProvider:
    def __init__(self, *, reset_ok=True, exists=True, enabled=True, locked=False):
        self.reset_ok = reset_ok
        self.exists = exists
        self.enabled = enabled
        self.locked = locked
        self.reset_calls = []
        self.unlock_calls = []
        self.closed = False

    def get_user_account_state(self, username):
        return {
            "available": True,
            "exists": self.exists,
            "enabled": self.enabled,
            "locked": self.locked,
            "domain": "example.test",
        }

    def reset_user_password(self, username, new_password, *, force_change_at_next_login=False):
        self.reset_calls.append((username, new_password, force_change_at_next_login))
        return self.reset_ok

    def unlock_user(self, username):
        self.unlock_calls.append(username)
        return True

    def close(self):
        self.closed = True


class UnsupportedTargetProvider(FakeTargetProvider):
    reset_user_password = None


class FakeIdentityVerifier:
    def __init__(self, *, identity=None):
        self.identity = identity
        self.requests = []

    def verify(self, request):
        self.requests.append(request)
        return self.identity


class SSPRModuleTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 4, 25, 8, 0, tzinfo=timezone.utc)

    @staticmethod
    def _binding(*, provider="dingtalk", connector="ad-primary", enabled=True):
        return UserIdentityBindingRecord(
            org_id="default",
            source_user_id="alice",
            source_provider=provider,
            ad_username="alice.ad",
            connector_id=connector,
            is_enabled=enabled,
        )

    def _session_store(self, *, token="verified-session"):
        return InMemorySSPRSessionStore(
            now_factory=lambda: self.now,
            token_factory=lambda: token,
        )

    def _create_session(self, store, *, provider="dingtalk", connector="ad-primary"):
        return store.create_session(
            SSPRVerifiedIdentity(
                org_id="default",
                source_user_id="alice",
                provider_id=provider,
                connector_id=connector,
                display_name="Alice Employee",
            ),
            request_ip="127.0.0.1",
            user_agent="test-agent",
        )

    @staticmethod
    def _reset_request(**overrides):
        values = {
            "verification_session_id": "verified-session",
            "new_password": VALID_PASSWORD,
            "confirm_password": VALID_PASSWORD,
            "request_ip": "127.0.0.1",
            "user_agent": "test-agent",
            "unlock_account": True,
            "force_change_at_next_login": True,
            "min_password_length": 12,
            "password_complexity": "strong",
        }
        values.update(overrides)
        return SSPRPasswordResetRequest(**values)

    def test_password_reset_uses_verified_identity_exact_binding_and_never_forces_change(self):
        target = FakeTargetProvider(locked=True)
        audit_repo = FakeAuditRepository()
        binding_repo = FakeBindingRepository([self._binding()])
        session_store = self._session_store()
        self._create_session(session_store)
        service = SSPRService(
            binding_repo=binding_repo,
            audit_repo=audit_repo,
            target_provider_resolver=lambda binding: target,
            session_store=session_store,
        )

        result = service.reset_password(self._reset_request())

        self.assertTrue(result.ok)
        self.assertEqual(result.ad_username, "alice.ad")
        self.assertEqual(target.reset_calls, [("alice.ad", VALID_PASSWORD, False)])
        self.assertEqual(target.unlock_calls, ["alice.ad"])
        self.assertEqual(binding_repo.lookup_args["source_provider"], "dingtalk")
        self.assertEqual(binding_repo.lookup_args["connector_id"], "ad-primary")
        self.assertEqual(audit_repo.logs[-1]["result"], "success")
        serialized_audit = json.dumps(audit_repo.logs[-1], ensure_ascii=False)
        self.assertNotIn(VALID_PASSWORD, serialized_audit)
        self.assertNotIn("verified-session", serialized_audit)
        self.assertIsNone(
            session_store.validate_session(
                "verified-session",
                user_agent="test-agent",
            )
        )

    def test_password_reset_fails_closed_when_binding_missing(self):
        session_store = self._session_store()
        self._create_session(session_store)
        audit_repo = FakeAuditRepository()
        service = SSPRService(
            binding_repo=FakeBindingRepository(),
            audit_repo=audit_repo,
            target_provider_resolver=lambda _binding: FakeTargetProvider(),
            session_store=session_store,
        )

        result = service.reset_password(self._reset_request())

        self.assertEqual(result.status, "unbound")
        self.assertEqual(audit_repo.logs[-1]["target_type"], "sspr_session")

    def test_password_reset_reports_unsupported_target_capability(self):
        session_store = self._session_store()
        self._create_session(session_store)
        service = SSPRService(
            binding_repo=FakeBindingRepository([self._binding()]),
            audit_repo=FakeAuditRepository(),
            target_provider_resolver=lambda _binding: UnsupportedTargetProvider(),
            session_store=session_store,
        )

        result = service.reset_password(self._reset_request(unlock_account=False))

        self.assertEqual(result.status, "unsupported")
        self.assertIsNotNone(session_store.validate_session("verified-session", user_agent="test-agent"))

    def test_employee_verification_uses_provider_identity_without_client_user_id(self):
        audit_repo = FakeAuditRepository()
        session_store = self._session_store(token="sspr-session-token")
        binding_repo = FakeBindingRepository([self._binding()])
        service = SSPRVerificationService(
            identity_verifier=FakeIdentityVerifier(
                identity=SSPRVerifiedIdentity(
                    org_id="default",
                    source_user_id="alice",
                    provider_id="dingtalk",
                    display_name="Alice",
                )
            ),
            session_store=session_store,
            binding_repo=binding_repo,
            audit_repo=audit_repo,
            session_ttl_seconds=600,
        )

        result = service.verify_employee(
            SSPRVerificationRequest(
                org_id="default",
                provider_id="dingtalk",
                verification_code="one-time-code",
                request_ip="127.0.0.1",
                user_agent="test-agent",
            )
        )

        self.assertEqual(result.status, "verified")
        self.assertEqual(result.source_user_id, "alice")
        self.assertEqual(result.session.session_id, "sspr-session-token")
        self.assertEqual(result.session.connector_id, "ad-primary")
        self.assertEqual(result.session.expires_at, self.now + timedelta(seconds=600))
        self.assertIsNone(session_store.validate_session(result.session.session_id, user_agent=""))
        self.assertIsNone(
            session_store.validate_session(result.session.session_id, user_agent="different-agent")
        )
        self.assertNotIn("one-time-code", json.dumps(audit_repo.logs, ensure_ascii=False))

    def test_employee_verification_rate_limits_anonymous_code_failures(self):
        audit_repo = FakeAuditRepository()
        limiter = SSPRRateLimiter(max_attempts=2, window_seconds=60, lockout_seconds=120)
        service = SSPRVerificationService(
            identity_verifier=FakeIdentityVerifier(identity=None),
            session_store=self._session_store(token="unused"),
            audit_repo=audit_repo,
            rate_limiter=limiter,
        )
        request = SSPRVerificationRequest(
            org_id="default",
            verification_code="bad-code",
            request_ip="127.0.0.1",
        )

        first = service.verify_employee(request)
        second = service.verify_employee(request)
        third = service.verify_employee(request)

        self.assertEqual(first.status, "invalid_response")
        self.assertEqual(second.status, "rate_limited")
        self.assertEqual(third.status, "rate_limited")
        self.assertTrue(audit_repo.logs[-1]["payload"]["rate_limited"])

    def test_password_reset_requires_a_valid_employee_session(self):
        target = FakeTargetProvider()
        service = SSPRService(
            binding_repo=FakeBindingRepository([self._binding()]),
            audit_repo=FakeAuditRepository(),
            target_provider_resolver=lambda _binding: target,
            session_store=self._session_store(),
        )

        result = service.reset_password(self._reset_request())

        self.assertEqual(result.status, "invalid_session")
        self.assertEqual(target.reset_calls, [])

    def test_password_confirmation_and_minimum_length_are_server_validated(self):
        session_store = self._session_store()
        self._create_session(session_store)
        target = FakeTargetProvider()
        service = SSPRService(
            binding_repo=FakeBindingRepository([self._binding()]),
            audit_repo=FakeAuditRepository(),
            target_provider_resolver=lambda _binding: target,
            session_store=session_store,
        )

        mismatch = service.reset_password(self._reset_request(confirm_password="Different1!Pass"))
        short = service.reset_password(
            self._reset_request(new_password="Ab1!", confirm_password="Ab1!")
        )

        self.assertEqual(mismatch.status, "password_mismatch")
        self.assertEqual(short.status, "password_too_short")
        self.assertEqual(target.reset_calls, [])

    def test_same_source_user_id_does_not_cross_provider_boundary(self):
        session_store = self._session_store()
        self._create_session(session_store, provider="dingtalk")
        wecom = self._binding(provider="wecom")
        dingtalk = self._binding(provider="dingtalk")
        binding_repo = FakeBindingRepository([wecom, dingtalk])
        service = SSPRService(
            binding_repo=binding_repo,
            audit_repo=FakeAuditRepository(),
            target_provider_resolver=lambda _binding: FakeTargetProvider(),
            session_store=session_store,
        )

        account = service.get_account("verified-session", user_agent="test-agent")

        self.assertTrue(account.ok)
        self.assertEqual(binding_repo.lookup_args["source_provider"], "dingtalk")


if __name__ == "__main__":
    unittest.main()
