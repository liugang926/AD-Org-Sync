import io
import json
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout

from sync_app import cli
from sync_app.storage.local_db import (
    DatabaseManager,
    SyncConflictRepository,
    SyncExceptionRuleRepository,
    SyncJobRepository,
    UserIdentityBindingRepository,
)


class CliConflictCommandTests(unittest.TestCase):
    def setUp(self):
        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        self.db_path = os.path.join(test_dir, "cli_conflicts_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.db_path + suffix)
            except FileNotFoundError:
                pass

        self.db_manager = DatabaseManager(db_path=self.db_path)
        self.db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        self.job_repo = SyncJobRepository(self.db_manager)
        self.conflict_repo = SyncConflictRepository(self.db_manager)
        self.exception_rule_repo = SyncExceptionRuleRepository(self.db_manager)
        self.user_binding_repo = UserIdentityBindingRepository(self.db_manager)

    def _run_cli(self, argv: list[str]) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = cli.main(argv)
        return exit_code, stdout.getvalue(), stderr.getvalue()

    def _create_job(self, job_id: str) -> None:
        self.job_repo.create_job(
            job_id,
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
        )

    def test_conflicts_list_json_prints_conflicts(self):
        self._create_job("job-cli-001")
        self.conflict_repo.add_conflict(
            job_id="job-cli-001",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="alice matched multiple AD candidates",
            resolution_hint="create manual binding",
            details={
                "userid": "alice",
                "candidates": [
                    {"rule": "existing_ad_userid", "username": "alice"},
                    {"rule": "existing_ad_email_localpart", "username": "alice.alt"},
                ],
            },
        )

        exit_code, stdout, stderr = self._run_cli(
            ["conflicts", "list", "--db-path", self.db_path, "--status", "open", "--json"]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["conflict_type"], "multiple_ad_candidates")
        self.assertEqual(payload[0]["source_id"], "alice")
        self.assertEqual(payload[0]["recommendation"]["action"], "manual_binding")
        self.assertEqual(payload[0]["recommendation"]["ad_username"], "alice")
        self.assertFalse(payload[0]["recommendation"]["requires_confirmation"])

    def test_conflicts_resolve_binding_creates_manual_binding_and_resolves_related_conflicts(self):
        self._create_job("job-cli-002")
        conflict_id = self.conflict_repo.add_conflict(
            job_id="job-cli-002",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="alice matched multiple AD candidates",
            resolution_hint="create manual binding",
        )
        sibling_conflict_id = self.conflict_repo.add_conflict(
            job_id="job-cli-002",
            conflict_type="shared_ad_account",
            source_id="alice",
            target_key="shared.account",
            message="alice shares AD account",
            resolution_hint="resolve manually",
        )

        exit_code, stdout, stderr = self._run_cli(
            [
                "conflicts",
                "resolve-binding",
                str(conflict_id),
                "--ad-username",
                "alice.alt",
                "--db-path",
                self.db_path,
            ]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("resolved_conflicts: 2", stdout)

        binding = self.user_binding_repo.get_binding_record_by_source_user_id("alice")
        self.assertIsNotNone(binding)
        self.assertEqual(binding.ad_username, "alice.alt")

        first_conflict = self.conflict_repo.get_conflict_record(conflict_id)
        sibling_conflict = self.conflict_repo.get_conflict_record(sibling_conflict_id)
        self.assertEqual(first_conflict.status, "resolved")
        self.assertEqual(sibling_conflict.status, "resolved")
        self.assertEqual((first_conflict.resolution_payload or {}).get("action"), "manual_binding")

    def test_conflicts_resolve_binding_rejects_system_protected_ad_account(self):
        self._create_job("job-cli-002-protected")
        conflict_id = self.conflict_repo.add_conflict(
            job_id="job-cli-002-protected",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="alice matched multiple AD candidates",
            resolution_hint="create manual binding",
        )

        exit_code, stdout, stderr = self._run_cli(
            [
                "conflicts",
                "resolve-binding",
                str(conflict_id),
                "--ad-username",
                "administrator",
                "--db-path",
                self.db_path,
            ]
        )

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout, "")
        self.assertIn("system-protected", stderr)
        self.assertIsNone(self.user_binding_repo.get_binding_record_by_source_user_id("alice"))
        self.assertEqual(self.conflict_repo.get_conflict_record(conflict_id).status, "open")

    def test_conflicts_resolve_binding_resolves_all_matching_open_conflicts_without_limit(self):
        self._create_job("job-cli-002b")
        first_conflict_id = None
        for index in range(520):
            conflict_id = self.conflict_repo.add_conflict(
                job_id="job-cli-002b",
                conflict_type="multiple_ad_candidates",
                source_id="alice",
                target_key=f"identity_binding.{index}",
                message=f"alice matched multiple AD candidates #{index}",
                resolution_hint="create manual binding",
            )
            if first_conflict_id is None:
                first_conflict_id = conflict_id

        exit_code, stdout, stderr = self._run_cli(
            [
                "conflicts",
                "resolve-binding",
                str(first_conflict_id),
                "--ad-username",
                "alice.alt",
                "--db-path",
                self.db_path,
            ]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("resolved_conflicts: 520", stdout)
        remaining_open = self.conflict_repo.list_conflict_records(job_id="job-cli-002b", status="open", limit=10)
        resolved_records = self.conflict_repo.list_conflict_records(
            job_id="job-cli-002b",
            status="resolved",
            limit=600,
        )
        self.assertEqual(len(remaining_open), 0)
        self.assertEqual(len(resolved_records), 520)

    def test_conflicts_resolve_binding_scopes_binding_to_conflict_organization(self):
        self.job_repo.create_job(
            "job-cli-002c",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="asia",
        )
        self.user_binding_repo.upsert_binding(
            "alice",
            "legacy.default",
            org_id="default",
            source="manual",
            notes="default binding",
            preserve_manual=False,
        )
        conflict_id = self.conflict_repo.add_conflict(
            job_id="job-cli-002c",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="alice matched multiple AD candidates in asia",
            resolution_hint="create manual binding",
        )

        exit_code, stdout, stderr = self._run_cli(
            [
                "conflicts",
                "resolve-binding",
                str(conflict_id),
                "--ad-username",
                "alice.asia",
                "--db-path",
                self.db_path,
            ]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("resolved conflict", stdout)
        default_binding = self.user_binding_repo.get_binding_record_by_source_user_id("alice", org_id="default")
        asia_binding = self.user_binding_repo.get_binding_record_by_source_user_id("alice", org_id="asia")
        self.assertIsNotNone(default_binding)
        self.assertEqual(default_binding.ad_username, "legacy.default")
        self.assertIsNotNone(asia_binding)
        self.assertEqual(asia_binding.ad_username, "alice.asia")

    def test_conflicts_bulk_skip_user_sync_and_reopen(self):
        self._create_job("job-cli-003")
        conflict_id_1 = self.conflict_repo.add_conflict(
            job_id="job-cli-003",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="alice matched multiple AD candidates",
            resolution_hint="create manual binding",
        )
        conflict_id_2 = self.conflict_repo.add_conflict(
            job_id="job-cli-003",
            conflict_type="shared_ad_account",
            source_id="bob",
            target_key="shared.account",
            message="bob shares AD account",
            resolution_hint="resolve manually",
        )

        exit_code, stdout, stderr = self._run_cli(
            [
                "conflicts",
                "bulk",
                "--action",
                "skip-user-sync",
                "--notes",
                "bulk skip",
                "--db-path",
                self.db_path,
                str(conflict_id_1),
                str(conflict_id_2),
            ]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("updated: 2", stdout)
        self.assertTrue(
            any(
                item.rule_type == "skip_user_sync" and item.match_value == "alice"
                for item in self.exception_rule_repo.list_rule_records()
            )
        )
        self.assertTrue(
            any(
                item.rule_type == "skip_user_sync" and item.match_value == "bob"
                for item in self.exception_rule_repo.list_rule_records()
            )
        )
        self.assertEqual(self.conflict_repo.get_conflict_record(conflict_id_1).status, "resolved")
        self.assertEqual(self.conflict_repo.get_conflict_record(conflict_id_2).status, "resolved")

        exit_code, stdout, stderr = self._run_cli(
            ["conflicts", "reopen", str(conflict_id_1), "--db-path", self.db_path]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn(f"reopened conflict: {conflict_id_1}", stdout)
        reopened_conflict = self.conflict_repo.get_conflict_record(conflict_id_1)
        self.assertEqual(reopened_conflict.status, "open")
        self.assertIsNone(reopened_conflict.resolution_payload)
        self.assertEqual(reopened_conflict.resolved_at, "")

    def test_conflicts_apply_recommendation_uses_best_candidate(self):
        self._create_job("job-cli-004")
        conflict_id = self.conflict_repo.add_conflict(
            job_id="job-cli-004",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="alice matched multiple AD candidates",
            resolution_hint="create manual binding",
            details={
                "userid": "alice",
                "candidates": [
                    {"rule": "existing_ad_email_localpart", "username": "alice.alt"},
                    {"rule": "existing_ad_userid", "username": "alice"},
                ],
            },
        )

        exit_code, stdout, stderr = self._run_cli(
            ["conflicts", "apply-recommendation", str(conflict_id), "--db-path", self.db_path]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("recommended_action: manual_binding", stdout)
        self.assertIn("ad_username: alice", stdout)

        binding = self.user_binding_repo.get_binding_record_by_source_user_id("alice")
        self.assertIsNotNone(binding)
        self.assertEqual(binding.ad_username, "alice")

        conflict = self.conflict_repo.get_conflict_record(conflict_id)
        self.assertEqual(conflict.status, "resolved")
        self.assertEqual((conflict.resolution_payload or {}).get("action"), "manual_binding")

    def test_conflicts_apply_recommendation_requires_reason_for_medium_confidence(self):
        self._create_job("job-cli-005")
        conflict_id = self.conflict_repo.add_conflict(
            job_id="job-cli-005",
            conflict_type="multiple_ad_candidates",
            source_id="alice",
            target_key="identity_binding",
            message="alice matched multiple AD candidates",
            resolution_hint="create manual binding",
            details={
                "userid": "alice",
                "candidates": [
                    {"rule": "existing_ad_email_localpart", "username": "alice.alt"},
                ],
            },
        )

        exit_code, stdout, stderr = self._run_cli(
            ["conflicts", "apply-recommendation", str(conflict_id), "--db-path", self.db_path]
        )
        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout, "")
        self.assertIn("low-confidence recommendations require --reason", stderr)
        self.assertEqual(self.conflict_repo.get_conflict_record(conflict_id).status, "open")

        exit_code, stdout, stderr = self._run_cli(
            [
                "conflicts",
                "apply-recommendation",
                str(conflict_id),
                "--reason",
                "email local part verified manually",
                "--db-path",
                self.db_path,
            ]
        )
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("recommended_action: manual_binding", stdout)
        self.assertEqual(self.conflict_repo.get_conflict_record(conflict_id).status, "resolved")


if __name__ == "__main__":
    unittest.main()
