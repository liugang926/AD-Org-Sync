import json
import unittest

from sync_app.core.models import UserIdentityBindingRecord
from sync_app.modules.sspr import SSPRPasswordResetRequest, SSPRService


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


if __name__ == "__main__":
    unittest.main()
