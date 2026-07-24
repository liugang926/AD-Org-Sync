import unittest

from sync_app.web.dashboard_state import (
    build_getting_started_data,
    compact_preflight_snapshot_for_session,
    count_check_statuses,
    merge_saved_preflight_snapshot,
    summarize_check_status,
)


class WebDashboardStateTests(unittest.TestCase):
    def test_summarize_and_count_check_statuses(self):
        checks = [
            {"status": "success"},
            {"status": "warning"},
            {"status": "error"},
            {},
        ]

        self.assertEqual(summarize_check_status(checks), "error")
        self.assertEqual(
            count_check_statuses(checks),
            {"success": 2, "warning": 1, "error": 1},
        )

    def test_merge_saved_preflight_snapshot_only_merges_live_checks_for_same_org(self):
        base_snapshot = {
            "org_id": "org-1",
            "checks": [{"key": "config", "status": "success"}],
            "overall_status": "success",
            "status_counts": {"success": 1, "warning": 0, "error": 0},
            "has_live_checks": False,
        }
        saved_snapshot = {
            "org_id": "org-1",
            "generated_at": "2026-04-08T10:00:00+00:00",
            "checks": [
                {"key": "live_wecom", "status": "warning"},
                {"key": "config", "status": "error"},
            ],
        }

        merged = merge_saved_preflight_snapshot(saved_snapshot, base_snapshot)

        self.assertEqual(len(merged["checks"]), 2)
        self.assertEqual(merged["checks"][1]["key"], "live_wecom")
        self.assertEqual(merged["overall_status"], "warning")
        self.assertEqual(merged["status_counts"], {"success": 1, "warning": 1, "error": 0})
        self.assertTrue(merged["has_live_checks"])
        self.assertEqual(merged["live_ran_at"], "2026-04-08T10:00:00+00:00")

    def test_compact_preflight_snapshot_keeps_only_bounded_live_results(self):
        compact = compact_preflight_snapshot_for_session(
            {
                "org_id": "org-1",
                "generated_at": "2026-07-24T09:00:00+00:00",
                "checks": [
                    {"key": "config", "status": "error", "detail": "not saved"},
                    {
                        "key": "live_source",
                        "label": "Live source connection",
                        "status": "success",
                        "detail": "x" * 2000,
                        "detail_params": {"provider": "DingTalk"},
                    },
                ],
                "rollout_readiness": {"steps": [{"summary": "y" * 20000}]},
            }
        )

        self.assertEqual(compact["org_id"], "org-1")
        self.assertEqual([item["key"] for item in compact["checks"]], ["live_source"])
        self.assertEqual(compact["overall_status"], "success")
        self.assertEqual(compact["status_counts"]["success"], 1)
        self.assertLessEqual(len(compact["checks"][0]["detail"]), 512)
        self.assertNotIn("rollout_readiness", compact)

    def test_build_getting_started_data_marks_current_step(self):
        preflight_snapshot = {
            "checks": [
                {"key": "config", "status": "success"},
                {"key": "live_source", "status": "success"},
                {"key": "live_ldap", "status": "success"},
            ],
            "source_snapshot_ready": True,
            "scope_ready": True,
            "release_ready": True,
            "dry_run_completed": True,
            "review_ready": True,
            "apply_completed": False,
            "open_conflict_count": 0,
        }

        data = build_getting_started_data(
            current_org_name="HQ",
            preflight_snapshot=preflight_snapshot,
            source_provider_name="WeCom",
            ui_mode="advanced",
        )

        self.assertEqual(data["current_org_name"], "HQ")
        self.assertEqual(data["completed_steps"], 9)
        self.assertEqual(data["total_steps"], 10)
        self.assertEqual(data["next_step"]["title"], "Run Apply")
        self.assertEqual(data["next_step"]["status"], "current")
        self.assertEqual(data["steps"][2]["href"], "/data-sources/source-directory")
        self.assertEqual(data["steps"][7]["href"], "/identity-governance/conflicts")

    def test_build_getting_started_data_keeps_legacy_live_wecom_snapshot_compatible(self):
        preflight_snapshot = {
            "checks": [
                {"key": "config", "status": "success"},
                {"key": "live_wecom", "status": "success"},
                {"key": "live_ldap", "status": "success"},
            ],
            "dry_run_completed": False,
            "apply_completed": False,
            "open_conflict_count": 0,
        }

        data = build_getting_started_data(
            current_org_name="HQ",
            preflight_snapshot=preflight_snapshot,
            source_provider_name="WeCom",
            ui_mode="basic",
        )

        self.assertEqual(data["completed_steps"], 2)
        self.assertEqual(data["next_step"]["title"], "Refresh Source Directory")


if __name__ == "__main__":
    unittest.main()
