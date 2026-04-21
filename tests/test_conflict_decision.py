import unittest

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


if __name__ == "__main__":
    unittest.main()
