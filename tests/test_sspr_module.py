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


class FakeBindingRepository:
    def __init__(self, binding=None):
        self.binding = binding
        self.lookup_args = None

    def get_binding_record_by_source_user_id(self, source_user_id, *, org_id=None):
        self.lookup_args = (source_user_id, org_id)
        return self.binding


class FakeAuditRepository:
    def __init__(self):
        self.logs = []

    def add_log(self, **kwargs):
        self.logs.append(kwargs)
        return len(self.logs)


class FakeTargetProvider:
    def __init__(self):
        self.reset_calls = []
        self.unlock_calls = []

    def reset_user_password(self, username, new_password, *, force_change_at_next_login=False):
        self.reset_calls.append((username, new_password, force_change_at_next_login))
        return True

    def unlock_user(self, username):
        self.unlock_calls.append(username)
        return True


class UnsupportedTargetProvider:
    pass


class FakeIdentityVerifier:
    def __init__(self, *, identity=None):
        self.identity = identity
        self.requests = []

    def verify(self, request):
        self.requests.append(request)
        return self.identity


class SSPRModuleTests(unittest.TestCase):
    def _binding(self):
        return UserIdentityBindingRecord(
            org_id="default",
            source_user_id="alice",
            ad_username="alice.ad",
            connector_id="default",
            is_enabled=True,
        )

    def test_password_reset_uses_identity_binding_target_provider_and_audit(self):
        target = FakeTargetProvider()
        audit_repo = FakeAuditRepository()
        service = SSPRService(
            binding_repo=FakeBindingRepository(self._binding()),
            audit_repo=audit_repo,
            target_provider_resolver=lambda binding: target,
        )

        result = service.reset_password(
            SSPRPasswordResetRequest(
                org_id="default",
                source_user_id="alice",
                actor_username="alice",
                new_password="Secret123!",
                request_ip="127.0.0.1",
                unlock_account=True,
                force_change_at_next_login=True,
            )
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.ad_username, "alice.ad")
        self.assertEqual(target.reset_calls, [("alice.ad", "Secret123!", True)])
        self.assertEqual(target.unlock_calls, ["alice.ad"])
        self.assertEqual(audit_repo.logs[0]["action_type"], "sspr.password_reset")
        self.assertEqual(audit_repo.logs[0]["result"], "success")
        self.assertEqual(audit_repo.logs[0]["target_id"], "alice.ad")
        self.assertNotIn("Secret123!", json.dumps(audit_repo.logs[0]["payload"]))

    def test_password_reset_fails_when_binding_missing(self):
        audit_repo = FakeAuditRepository()
        service = SSPRService(
            binding_repo=FakeBindingRepository(None),
            audit_repo=audit_repo,
            target_provider_resolver=lambda _binding: FakeTargetProvider(),
        )

        result = service.reset_password(
            SSPRPasswordResetRequest(
                org_id="default",
                source_user_id="missing",
                actor_username="missing",
                new_password="Secret123!",
            )
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.status, "not_found")
        self.assertEqual(audit_repo.logs[0]["result"], "failure")
        self.assertEqual(audit_repo.logs[0]["target_type"], "source_user")

    def test_password_reset_reports_unsupported_target_capability(self):
        audit_repo = FakeAuditRepository()
        service = SSPRService(
            binding_repo=FakeBindingRepository(self._binding()),
            audit_repo=audit_repo,
            target_provider_resolver=lambda _binding: UnsupportedTargetProvider(),
        )

        result = service.reset_password(
            SSPRPasswordResetRequest(
                org_id="default",
                source_user_id="alice",
                actor_username="alice",
                new_password="Secret123!",
            )
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.status, "unsupported")
        self.assertEqual(audit_repo.logs[0]["result"], "failure")

    def test_employee_verification_issues_module_session_and_audits(self):
        now = datetime(2026, 4, 25, 8, 0, tzinfo=timezone.utc)
        audit_repo = FakeAuditRepository()
        session_store = InMemorySSPRSessionStore(
            now_factory=lambda: now,
            token_factory=lambda: "sspr-session-token",
        )
        service = SSPRVerificationService(
            identity_verifier=FakeIdentityVerifier(
                identity=SSPRVerifiedIdentity(
                    org_id="default",
                    source_user_id="alice",
                    provider_id="wecom",
                    display_name="Alice",
                    raw_claims={"userid": "alice"},
                )
            ),
            session_store=session_store,
            audit_repo=audit_repo,
            session_ttl_seconds=600,
        )

        result = service.verify_employee(
            SSPRVerificationRequest(
                org_id="default",
                source_user_id="alice",
                provider_id="wecom",
                verification_code="oauth-code",
                request_ip="127.0.0.1",
            )
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.session.session_id, "sspr-session-token")
        self.assertEqual(result.session.expires_at, now + timedelta(seconds=600))
        self.assertIsNotNone(
            session_store.validate_session(
                "sspr-session-token",
                org_id="default",
                source_user_id="alice",
                request_ip="127.0.0.1",
            )
        )
        self.assertEqual(audit_repo.logs[0]["action_type"], "sspr.verify")
        self.assertEqual(audit_repo.logs[0]["result"], "success")
        self.assertNotIn("oauth-code", json.dumps(audit_repo.logs[0]["payload"]))

    def test_employee_verification_rate_limits_by_source_user_and_ip(self):
        audit_repo = FakeAuditRepository()
        limiter = SSPRRateLimiter(max_attempts=2, window_seconds=60, lockout_seconds=120)
        service = SSPRVerificationService(
            identity_verifier=FakeIdentityVerifier(identity=None),
            session_store=InMemorySSPRSessionStore(token_factory=lambda: "unused"),
            audit_repo=audit_repo,
            rate_limiter=limiter,
        )
        request = SSPRVerificationRequest(
            org_id="default",
            source_user_id="alice",
            verification_code="bad-code",
            request_ip="127.0.0.1",
        )

        first = service.verify_employee(request)
        second = service.verify_employee(request)
        third = service.verify_employee(request)

        self.assertEqual(first.status, "failed")
        self.assertEqual(second.status, "rate_limited")
        self.assertEqual(third.status, "rate_limited")
        self.assertGreaterEqual(third.retry_after_seconds, 1)
        self.assertTrue(audit_repo.logs[-1]["payload"]["rate_limited"])

    def test_password_reset_can_require_verified_employee_session(self):
        now = datetime(2026, 4, 25, 8, 0, tzinfo=timezone.utc)
        target = FakeTargetProvider()
        audit_repo = FakeAuditRepository()
        session_store = InMemorySSPRSessionStore(
            now_factory=lambda: now,
            token_factory=lambda: "verified-session",
        )
        session_store.create_session(
            SSPRVerifiedIdentity(
                org_id="default",
                source_user_id="alice",
                provider_id="wecom",
            ),
            request_ip="127.0.0.1",
        )
        service = SSPRService(
            binding_repo=FakeBindingRepository(self._binding()),
            audit_repo=audit_repo,
            target_provider_resolver=lambda _binding: target,
            session_store=session_store,
            require_verified_session=True,
        )

        missing_session_result = service.reset_password(
            SSPRPasswordResetRequest(
                org_id="default",
                source_user_id="alice",
                actor_username="alice",
                new_password="Secret123!",
                request_ip="127.0.0.1",
            )
        )
        ok_result = service.reset_password(
            SSPRPasswordResetRequest(
                org_id="default",
                source_user_id="alice",
                actor_username="alice",
                new_password="Secret123!",
                request_ip="127.0.0.1",
                verification_session_id="verified-session",
            )
        )

        self.assertFalse(missing_session_result.ok)
        self.assertEqual(missing_session_result.status, "invalid_session")
        self.assertTrue(ok_result.ok)
        self.assertEqual(target.reset_calls, [("alice.ad", "Secret123!", False)])


if __name__ == "__main__":
    unittest.main()
