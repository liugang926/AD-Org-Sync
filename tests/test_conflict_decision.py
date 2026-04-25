import unittest
from types import SimpleNamespace

from sync_app.core.conflict_recommendations import recommend_conflict_resolution
from sync_app.services.conflict_decision import build_binding_decision_summary


class ConflictDecisionTests(unittest.TestCase):
    def test_multiple_candidate_binding_resolves_to_existing_account(self):
        summary = build_binding_decision_summary(
            conflict_type="multiple_ad_candidates",
            source_user_id="alice",
            selected_target_username="alice",
            target_exists=True,
            target_enabled=True,
            current_binding_owner="",
            is_protected_account=False,
            shared_source_user_ids=[],
            rehire_restore_enabled=True,
        )

        self.assertEqual(summary["bind_now"]["action"], "update_user")
        self.assertFalse(summary["bind_now"]["will_create_new_account"])
        self.assertFalse(summary["bind_now"]["will_conflict_continue"])
        self.assertTrue(summary["without_binding"]["will_conflict_continue"])

    def test_shared_account_binding_stays_warning_when_target_is_still_shared(self):
        summary = build_binding_decision_summary(
            conflict_type="shared_ad_account",
            source_user_id="alice",
            selected_target_username="shared.account",
            target_exists=True,
            target_enabled=False,
            current_binding_owner="",
            is_protected_account=False,
            shared_source_user_ids=["alice", "bob"],
            rehire_restore_enabled=True,
        )

        self.assertEqual(summary["bind_now"]["action"], "reactivate_user")
        self.assertTrue(summary["bind_now"]["will_conflict_continue"])
        self.assertEqual(summary["bind_now"]["status"], "warning")
        self.assertIn("shared", summary["bind_now"]["notes"][0].lower())

    def test_existing_ad_identity_claim_review_recommends_manual_binding(self):
        recommendation = recommend_conflict_resolution(
            SimpleNamespace(
                conflict_type="existing_ad_identity_claim_review",
                source_id="alice",
                target_key="alice",
                details={
                    "candidate": {
                        "rule": "existing_ad_userid",
                        "username": "alice",
                        "explanation": "Source user ID maps directly to an existing AD username",
                    }
                },
            )
        )

        self.assertIsNotNone(recommendation)
        self.assertEqual(recommendation["action"], "manual_binding")
        self.assertEqual(recommendation["label"], "Approve existing AD account claim")
        self.assertEqual(recommendation["ad_username"], "alice")
        self.assertEqual(recommendation["confidence"], "high")
        self.assertFalse(recommendation["requires_confirmation"])

    def test_existing_ad_identity_claim_review_explains_bind_and_wait_paths(self):
        summary = build_binding_decision_summary(
            conflict_type="existing_ad_identity_claim_review",
            source_user_id="alice",
            selected_target_username="alice",
            target_exists=True,
            target_enabled=True,
            current_binding_owner="",
            is_protected_account=False,
            shared_source_user_ids=[],
            rehire_restore_enabled=True,
        )

        self.assertEqual(summary["bind_now"]["action"], "update_user")
        self.assertFalse(summary["bind_now"]["will_create_new_account"])
        self.assertFalse(summary["bind_now"]["will_conflict_continue"])
        self.assertIn("manual binding", summary["bind_now"]["notes"][0])
        self.assertTrue(summary["without_binding"]["will_conflict_continue"])
        self.assertIn("review-mode policy", summary["without_binding"]["summary"])


if __name__ == "__main__":
    unittest.main()
