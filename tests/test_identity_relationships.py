import os
import unittest
from unittest.mock import patch

from sync_app.core.models import DirectoryUserRecord, SourceDirectoryUser
from sync_app.services.identity_relationships import (
    IdentityRelationshipPreviewService,
    build_runtime_identity_evidence,
)
from sync_app.storage.local_db import (
    DatabaseManager,
    PlannedOperationRepository,
    SyncJobRepository,
    SyncOperationLogRepository,
    UserIdentityBindingRepository,
)
from sync_app.storage.repositories import SourceDirectoryRepository


class IdentityRelationshipTests(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join("test_artifacts", "identity_relationships.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.path + suffix)
            except FileNotFoundError:
                pass
        self.manager = DatabaseManager(db_path=self.path)
        self.manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        self.bindings = UserIdentityBindingRepository(self.manager)
        self.source = SourceDirectoryRepository(self.manager)
        self.jobs = SyncJobRepository(self.manager)
        self.operations = SyncOperationLogRepository(self.manager)
        self.plans = PlannedOperationRepository(self.manager)
        self.service = IdentityRelationshipPreviewService(
            source_directory_repo=self.source,
            user_binding_repo=self.bindings,
            operation_log_repo=self.operations,
            planned_operation_repo=self.plans,
        )

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.path + suffix)
            except FileNotFoundError:
                pass

    def _snapshot(self):
        snapshot_id = self.source.start_refresh(
            org_id="default", provider_id="dingtalk", created_by="test"
        )
        self.source.replace_snapshot(
            snapshot_id,
            departments=[
                {
                    "source_department_id": "1",
                    "name": "HQ",
                    "parent_department_id": "0",
                    "path_ids": ["1"],
                    "path_names": ["HQ"],
                }
            ],
            users=[
                {
                    "source_user_id": "ding-001",
                    "display_name": "Alice Ding",
                    "employee_id": "TJ001",
                    "email": "alice@example.com",
                    "department_ids": ["1"],
                    "department_names": ["HQ"],
                    "is_active": True,
                    "raw_payload": {"userid": "ding-001", "employee_id": "TJ001"},
                    "search_text": "Alice Ding TJ001",
                },
                {
                    "source_user_id": "ding-002",
                    "display_name": "Bob Ding",
                    "employee_id": "TJ002",
                    "department_ids": ["1"],
                    "department_names": ["HQ"],
                    "is_active": True,
                    "raw_payload": {"userid": "ding-002", "employee_id": "TJ002"},
                    "search_text": "Bob Ding TJ002",
                },
            ],
            fields=[
                {
                    "name": "employee_id",
                    "label": "Employee ID",
                    "coverage": 2,
                    "samples": ["TJ001"],
                }
            ],
            fingerprint="snapshot-dingtalk-v1",
        )
        selection = self.source.save_scope_selection(
            org_id="default",
            provider_id="dingtalk",
            scope_type="full",
            username_strategy="employee_id",
            source_field="employee_id",
            snapshot_id=snapshot_id,
            requested_by="test",
        )
        return self.source.get_snapshot(snapshot_id, org_id="default"), selection

    def test_batch_binding_lookup_isolates_org_provider_connector_and_chunks(self):
        self.bindings.upsert_binding(
            "shared",
            "ding-default",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            source="manual",
        )
        self.bindings.upsert_binding(
            "shared",
            "ding-asia",
            org_id="default",
            source_provider="dingtalk",
            connector_id="asia",
            source="managed_generated",
            is_enabled=False,
        )
        self.bindings.upsert_binding(
            "shared",
            "wecom-default",
            org_id="default",
            source_provider="wecom",
            connector_id="default",
        )
        self.bindings.upsert_binding(
            "shared",
            "other-org",
            org_id="other",
            source_provider="dingtalk",
            connector_id="default",
        )
        for index in range(805):
            self.bindings.upsert_binding(
                f"user-{index}",
                f"ad-{index}",
                org_id="default",
                source_provider="dingtalk",
                connector_id="default",
            )
        source_ids = ["shared", *[f"user-{index}" for index in range(805)]]
        with patch.object(
            self.bindings,
            "_fetchall",
            wraps=self.bindings._fetchall,
        ) as fetchall:
            rows = self.bindings.list_binding_records_for_source_identities(
                source_ids,
                org_id="default",
                source_provider="dingtalk",
                chunk_size=400,
            )
        self.assertEqual(fetchall.call_count, 3)
        shared_rows = [item for item in rows if item.source_user_id == "shared"]
        self.assertEqual({item.connector_id for item in shared_rows}, {"default", "asia"})
        self.assertEqual({item.source_provider for item in rows}, {"dingtalk"})
        self.assertEqual({item.org_id for item in rows}, {"default"})
        self.assertFalse(next(item for item in shared_rows if item.connector_id == "asia").is_enabled)

        self.bindings.update_governance_metadata_for_source_user(
            "shared",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            effective_reason="scoped update",
        )
        scoped_rows = self.bindings.list_binding_records_for_source_identities(
            ["shared"],
            org_id="default",
            source_provider="dingtalk",
        )
        self.assertEqual(
            next(item for item in scoped_rows if item.connector_id == "default").effective_reason,
            "scoped update",
        )
        self.assertEqual(
            next(item for item in scoped_rows if item.connector_id == "asia").effective_reason,
            "",
        )
        wecom_row = self.bindings.list_binding_records_for_source_identities(
            ["shared"],
            org_id="default",
            source_provider="wecom",
        )[0]
        self.assertEqual(wecom_row.effective_reason, "")

        self.jobs.create_job(
            "batch-evidence",
            "test",
            "dry_run",
            "COMPLETED",
            org_id="default",
        )
        with patch.object(
            self.operations,
            "_fetchall",
            wraps=self.operations._fetchall,
        ) as fetchall:
            evidence = self.operations.list_latest_identity_resolution_evidence(
                source_ids,
                org_id="default",
                source_provider="dingtalk",
                execution_mode="dry_run",
            )
        self.assertEqual(evidence, [])
        self.assertEqual(fetchall.call_count, 3)

        with patch.object(
            self.operations,
            "_fetchall",
            wraps=self.operations._fetchall,
        ) as fetchall:
            apply_rows = self.operations.list_user_operation_evidence_for_jobs(
                ["batch-evidence"],
                org_id="default",
                source_user_ids=source_ids,
            )
        self.assertEqual(apply_rows, [])
        self.assertEqual(fetchall.call_count, 3)

    def test_manual_binding_overrides_candidate_and_ad_lookup_is_deduplicated(self):
        snapshot, selection = self._snapshot()
        self.bindings.upsert_binding(
            "ding-001",
            "alice.manual",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            source="manual",
        )
        users = self.source.list_users(
            int(snapshot["id"]),
            org_id="default",
            provider_id="dingtalk",
            limit=20,
        )["items"]
        specs = {
            "default": {
                "username_strategy": "employee_id",
                "username_template": "",
                "username_collision_policy": "append_employee_id",
                "username_collision_template": "",
            }
        }
        relationships = self.service.build_relationships(
            users,
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id=specs,
            connector_ids_by_source_user={"ding-001": "default", "ding-002": "default"},
        )
        alice = next(item for item in relationships if item.source_user_id == "ding-001")
        self.assertEqual(alice.candidate_mapping["ad_username"], "TJ001")
        self.assertEqual(alice.effective_ad_username, "alice.manual")
        self.assertEqual(alice.effective_resolution_source, "manual_binding")
        self.assertEqual(alice.difference["status"], "manual_binding_overrides_candidate")

        class Provider:
            calls = 0

            def get_users_batch(self, usernames):
                self.calls += 1
                return {
                    username: DirectoryUserRecord(
                        username=username,
                        dn="redacted",
                    )
                    for username in set(usernames)
                }

            def close(self):
                return None

        provider = Provider()
        states = self.service.load_ad_states(
            lambda _connector_id: provider,
            {"default": ["alice.manual", "alice.manual", "TJ001", "TJ002"]},
        )
        self.assertEqual(provider.calls, 1)
        self.assertEqual(len(states), 3)
        self.assertTrue(states[("default", "alice.manual")]["exists"])

        enabled_state = self.service._ad_state_from_record(
            DirectoryUserRecord(
                username="enabled.user",
                dn="redacted",
                raw_entry={
                    "attributes": {
                        "userAccountControl": 512,
                        "lockoutTime": 0,
                    }
                },
            )
        )
        locked_state = self.service._ad_state_from_record(
            DirectoryUserRecord(
                username="locked.user",
                dn="redacted",
                raw_entry={
                    "attributes": {
                        "userAccountControl": 514,
                        "lockoutTime": 1,
                    }
                },
            )
        )
        self.assertEqual(enabled_state["status"], "enabled")
        self.assertEqual(locked_state["status"], "locked")
        self.assertFalse(locked_state["enabled"])

    def test_account_creation_requires_verified_missing_candidate_without_binding_conflict(self):
        snapshot, selection = self._snapshot()
        users = self.source.list_users(
            int(snapshot["id"]),
            org_id="default",
            provider_id="dingtalk",
            limit=20,
        )["items"]
        specs = {
            "default": {
                "username_strategy": "employee_id",
                "username_template": "",
                "username_collision_policy": "append_employee_id",
                "username_collision_template": "",
            }
        }
        missing = {
            "status": "missing",
            "exists": False,
            "enabled": None,
            "locked": None,
            "protected": False,
            "verified_at": "2026-07-14T01:00:00+00:00",
        }
        exists = missing | {"status": "enabled", "exists": True, "enabled": True}

        verified = self.service.build_relationships(
            users,
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id=specs,
            connector_ids_by_source_user={
                "ding-001": "default",
                "ding-002": "default",
            },
            ad_states={
                ("default", "tj001"): missing,
                ("default", "tj002"): missing,
            },
        )
        bob = next(item for item in verified if item.source_user_id == "ding-002")
        self.assertEqual(bob.candidate_ad_state["status"], "missing")
        self.assertTrue(bob.creation_eligibility["eligible"])
        self.assertEqual(bob.creation_eligibility["status"], "eligible")

        existing = self.service.build_relationships(
            [next(item for item in users if item["source_user_id"] == "ding-002")],
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id=specs,
            connector_ids_by_source_user={"ding-002": "default"},
            ad_states={("default", "tj002"): exists},
        )[0]
        self.assertFalse(existing.creation_eligibility["eligible"])
        self.assertEqual(existing.creation_eligibility["status"], "candidate_exists")

        self.bindings.upsert_binding(
            "ding-001",
            "legacy.alice",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            source="manual",
        )
        mismatched_binding = self.service.build_relationships(
            [next(item for item in users if item["source_user_id"] == "ding-001")],
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id=specs,
            connector_ids_by_source_user={"ding-001": "default"},
            ad_states={
                ("default", "tj001"): missing,
                ("default", "legacy.alice"): missing,
            },
        )[0]
        self.assertFalse(mismatched_binding.creation_eligibility["eligible"])
        self.assertEqual(
            mismatched_binding.creation_eligibility["status"],
            "binding_review_required",
        )

    def test_connector_boundaries_never_reuse_another_connector_binding(self):
        snapshot, selection = self._snapshot()
        users = self.source.list_users(
            int(snapshot["id"]),
            org_id="default",
            provider_id="dingtalk",
            source_user_ids=["ding-001"],
            limit=1,
        )["items"]
        spec = {
            "username_strategy": "employee_id",
            "username_template": "",
            "username_collision_policy": "append_employee_id",
            "username_collision_template": "",
        }
        self.bindings.upsert_binding(
            "ding-001",
            "asia.alice",
            org_id="default",
            source_provider="dingtalk",
            connector_id="asia",
            source="managed_generated",
        )

        migration = self.service.build_relationships(
            users,
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id={"default": spec, "asia": spec},
            connector_ids_by_source_user={"ding-001": "default"},
        )[0]
        self.assertEqual(migration.before_state["bound_ad_username"], "")
        self.assertEqual(migration.effective_resolution_source, "conflict")
        self.assertEqual(
            migration.difference["status"], "connector_migration_required"
        )
        self.assertIn("connector_migration_required", migration.risks)

        self.bindings.upsert_binding(
            "ding-001",
            "default.alice",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            source="manual",
        )
        exact = self.service.build_relationships(
            users,
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id={"default": spec, "asia": spec},
            connector_ids_by_source_user={"ding-001": "default"},
        )[0]
        self.assertEqual(exact.before_state["bound_ad_username"], "default.alice")
        self.assertEqual(exact.effective_ad_username, "default.alice")
        self.assertEqual(exact.effective_resolution_source, "manual_binding")
        self.assertNotIn("connector_migration_required", exact.risks)

    def test_disabled_protected_and_candidate_collision_states_are_fail_closed(self):
        snapshot, selection = self._snapshot()
        users = self.source.list_users(
            int(snapshot["id"]),
            org_id="default",
            provider_id="dingtalk",
            limit=20,
        )["items"]
        spec = {
            "username_strategy": "employee_id",
            "username_template": "",
            "username_collision_policy": "append_employee_id",
            "username_collision_template": "",
        }
        self.bindings.upsert_binding(
            "ding-001",
            "protected.account",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            source="manual",
        )
        self.bindings.upsert_binding(
            "ding-002",
            "disabled.account",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            source="managed_generated",
            is_enabled=False,
        )
        relationships = self.service.build_relationships(
            users,
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id={"default": spec},
            connector_ids_by_source_user={
                "ding-001": "default",
                "ding-002": "default",
            },
            ad_states={
                ("default", "protected.account"): {
                    "status": "protected",
                    "exists": True,
                    "enabled": True,
                    "locked": False,
                    "protected": True,
                    "verified_at": "2026-07-14T01:00:00+00:00",
                }
            },
        )
        protected = next(
            item for item in relationships if item.source_user_id == "ding-001"
        )
        disabled = next(
            item for item in relationships if item.source_user_id == "ding-002"
        )
        self.assertEqual(protected.difference["status"], "protected_account")
        self.assertIn("protected_account", protected.risks)
        self.assertEqual(disabled.effective_ad_username, "")
        self.assertEqual(disabled.effective_resolution_source, "unresolved")
        self.assertIn("binding_disabled", disabled.risks)
        self.assertTrue(self.service.matches_filter(disabled, "disabled"))

        collision_rows = [dict(item) for item in users]
        collision_rows[0]["source_user_id"] = "collision-001"
        collision_rows[1]["source_user_id"] = "collision-002"
        collision_rows[1]["employee_id"] = collision_rows[0]["employee_id"]
        collision_rows[0]["raw_payload"] = {
            **dict(collision_rows[0].get("raw_payload") or {}),
            "userid": "collision-001",
        }
        collision_rows[1]["raw_payload"] = {
            **dict(collision_rows[1].get("raw_payload") or {}),
            "userid": "collision-002",
            "employee_id": collision_rows[0]["employee_id"],
        }
        collisions = self.service.build_relationships(
            collision_rows,
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id={"default": spec},
            connector_ids_by_source_user={
                "collision-001": "default",
                "collision-002": "default",
            },
        )
        self.assertTrue(
            all(
                item.difference["status"] == "multiple_candidate_conflict"
                and item.effective_ad_username == ""
                for item in collisions
            )
        )

    def test_structured_dry_run_and_apply_evidence_and_staleness(self):
        snapshot, selection = self._snapshot()
        user_row = self.source.list_users(
            int(snapshot["id"]),
            org_id="default",
            provider_id="dingtalk",
            source_user_ids=["ding-001"],
            limit=1,
        )["items"][0]
        user = SourceDirectoryUser.from_source_payload(
            {
                "userid": "ding-001",
                "name": "Alice Ding",
                "employee_id": "TJ001",
                "email": "alice@example.com",
                "department": [1],
            }
        )
        spec = {
            "username_strategy": "employee_id",
            "username_template": "",
            "username_collision_policy": "append_employee_id",
            "username_collision_template": "",
        }
        evidence = build_runtime_identity_evidence(
            user=user,
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            connector_spec=spec,
            source_scope=selection,
            config_fingerprint="config-1",
            before_ad_state={
                "status": "missing",
                "exists": False,
                "enabled": None,
                "locked": None,
                "protected": False,
            },
        )
        resolution = {
            **evidence,
            "source": "managed_username_primary",
            "ad_username": "TJ001",
            "rule_hits": ["managed_username_primary"],
            "explanation": "Generated from employee ID",
            "before_state": {
                "bound_ad_username": "",
                "binding_source": "",
                "binding_enabled": False,
                "connector_id": "default",
                "ad_account_state": {
                    "status": "missing",
                    "exists": False,
                    "enabled": None,
                    "locked": None,
                    "protected": False,
                },
            },
        }
        self.jobs.create_job(
            "dry-1",
            "test",
            "dry_run",
            "COMPLETED",
            org_id="default",
        )
        self.jobs.update_job(
            "dry-1",
            summary={"plan_fingerprint": "plan-1"},
            ended=True,
        )
        self.source.bind_job_scope(
            job_id="dry-1",
            execution_mode="dry_run",
            config_fingerprint="config-1",
            selection=selection,
        )
        self.operations.add_record(
            job_id="dry-1",
            stage_name="plan",
            object_type="user_binding",
            operation_type="resolve_identity_binding",
            status="selected",
            message="structured resolution",
            source_id="ding-001",
            target_id="TJ001",
            rule_source="managed_username_primary",
            reason_code="auto_resolution",
            details=resolution,
        )
        self.plans.add_operation(
            "dry-1",
            "user",
            "create_user",
            source_id="ding-001",
            desired_state={"ad_username": "TJ001"},
        )

        relationships = self.service.build_relationships(
            [user_row],
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id={"default": spec},
            connector_ids_by_source_user={"ding-001": "default"},
            config_fingerprint="config-1",
        )
        self.assertEqual(relationships[0].planned_after_state["ad_username"], "TJ001")
        self.assertFalse(relationships[0].planned_after_state["is_stale"])
        self.assertEqual(relationships[0].applied_after_state["result"], "not_applied")

        config_changed = self.service.build_relationships(
            [user_row],
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id={"default": spec},
            connector_ids_by_source_user={"ding-001": "default"},
            config_fingerprint="config-2",
        )[0]
        self.assertTrue(config_changed.planned_after_state["is_stale"])

        self.bindings.upsert_binding(
            "ding-001",
            "TJ001",
            org_id="default",
            source_provider="dingtalk",
            connector_id="default",
            source="managed_generated",
        )
        self.jobs.create_job(
            "apply-1",
            "test",
            "apply",
            "COMPLETED",
            org_id="default",
        )
        self.jobs.update_job("apply-1", summary={"plan_fingerprint": "plan-1"}, ended=True)
        self.source.bind_job_scope(
            job_id="apply-1",
            execution_mode="apply",
            config_fingerprint="config-1",
            selection=selection,
        )
        self.operations.add_record(
            job_id="apply-1",
            stage_name="plan",
            object_type="user_binding",
            operation_type="resolve_identity_binding",
            status="selected",
            message="structured resolution",
            source_id="ding-001",
            target_id="TJ001",
            rule_source="managed_username_primary",
            reason_code="auto_resolution",
            details=resolution,
        )
        self.operations.add_record(
            job_id="apply-1",
            stage_name="apply",
            object_type="user",
            operation_type="create_user",
            status="succeeded",
            message="created",
            source_id="ding-001",
            target_id="TJ001",
            details={
                "connector_id": "default",
                "binding_resolution": resolution,
                "post_apply_ad_account_state": {"status": "exists", "exists": True},
            },
        )
        applied = self.service.build_relationships(
            [user_row],
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id={"default": spec},
            connector_ids_by_source_user={"ding-001": "default"},
            config_fingerprint="config-1",
        )[0]
        self.assertEqual(applied.applied_after_state["result"], "succeeded")
        self.assertEqual(applied.applied_after_state["ad_username"], "TJ001")
        self.assertTrue(applied.planned_after_state["is_stale"])

        job_rows = self.service.build_job_identity_resolutions(
            "apply-1", org_id="default"
        )
        self.assertEqual(job_rows[0]["source_display_name"], "Alice Ding")
        self.assertEqual(job_rows[0]["candidate_mapping"]["ad_username"], "TJ001")
        self.assertEqual(job_rows[0]["apply_result"], "succeeded")

    def test_server_computed_existing_ad_matches_are_safe_and_ambiguous_matches_stop(self):
        snapshot, selection = self._snapshot()
        user_row = self.source.list_users(
            int(snapshot["id"]),
            org_id="default",
            provider_id="dingtalk",
            source_user_ids=["ding-001"],
            limit=1,
        )["items"][0]
        spec = {
            "username_strategy": "employee_id",
            "username_template": "",
            "username_collision_policy": "append_employee_id",
            "username_collision_template": "",
        }
        exists = {
            "status": "exists",
            "exists": True,
            "enabled": True,
            "locked": False,
            "protected": False,
            "verified_at": "2026-07-14T01:00:00+00:00",
        }
        missing = {
            "status": "missing",
            "exists": False,
            "enabled": None,
            "locked": None,
            "protected": False,
            "verified_at": "2026-07-14T01:00:00+00:00",
        }
        safe_match = self.service.build_relationships(
            [user_row],
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id={"default": spec},
            connector_ids_by_source_user={"ding-001": "default"},
            ad_states={
                ("default", "ding-001"): exists,
                ("default", "tj001"): missing,
                ("default", "alice"): missing,
            },
        )[0]
        self.assertEqual(safe_match.candidate_mapping["ad_username"], "TJ001")
        self.assertEqual(safe_match.effective_ad_username, "ding-001")
        self.assertEqual(safe_match.effective_resolution_source, "existing_ad_match")
        self.assertEqual(
            safe_match.before_state["checked_ad_username"], "ding-001"
        )
        self.assertFalse(safe_match.creation_eligibility["eligible"])
        self.assertEqual(
            safe_match.creation_eligibility["status"], "existing_identity_match"
        )

        ambiguous = self.service.build_relationships(
            [user_row],
            org_id="default",
            source_provider="dingtalk",
            snapshot=snapshot,
            scope=selection,
            connector_specs_by_id={"default": spec},
            connector_ids_by_source_user={"ding-001": "default"},
            ad_states={
                ("default", "ding-001"): exists,
                ("default", "tj001"): exists,
                ("default", "alice"): missing,
            },
        )[0]
        self.assertEqual(ambiguous.effective_ad_username, "")
        self.assertEqual(ambiguous.effective_resolution_source, "conflict")
        self.assertEqual(
            ambiguous.difference["status"], "multiple_ad_candidate_conflict"
        )
        self.assertIn("multiple_ad_candidates", ambiguous.risks)

    def test_apply_status_filter_is_strictly_provider_and_connector_scoped(self):
        _snapshot, selection = self._snapshot()
        for source_provider, connector_id, ad_username in (
            ("dingtalk", "default", "shared.ad"),
            ("dingtalk", "asia", "shared.asia"),
            ("wecom", "wecom", "shared.wecom"),
        ):
            self.bindings.upsert_binding(
                "shared-user",
                ad_username,
                org_id="default",
                source_provider=source_provider,
                connector_id=connector_id,
                source="managed_generated",
            )
        self.jobs.create_job(
            "strict-apply",
            "test",
            "apply",
            "COMPLETED",
            org_id="default",
        )
        self.source.bind_job_scope(
            job_id="strict-apply",
            execution_mode="apply",
            config_fingerprint="config-1",
            selection=selection,
        )
        self.operations.add_record(
            job_id="strict-apply",
            stage_name="apply",
            object_type="user",
            operation_type="update_user",
            status="succeeded",
            message="updated",
            source_id="shared-user",
            target_id="shared.ad",
            details={"connector_id": "default"},
        )

        rows, total = self.bindings.list_binding_records_page(
            org_id="default",
            apply_status="succeeded",
        )
        self.assertEqual(total, 1)
        self.assertEqual(rows[0].source_provider, "dingtalk")
        self.assertEqual(rows[0].connector_id, "default")

    def test_ad_unavailable_is_safe_and_does_not_expose_exception(self):
        class BrokenProvider:
            def get_users_batch(self, _usernames):
                raise RuntimeError("ldap://secret-host bind password=bad")

            def close(self):
                return None

        states = self.service.load_ad_states(
            lambda _connector_id: BrokenProvider(),
            {"default": ["safe-computed-name"]},
        )
        state = states[("default", "safe-computed-name")]
        self.assertEqual(state["status"], "unavailable")
        self.assertNotIn("secret-host", str(state))
        self.assertNotIn("password", str(state))

        class SoftFailureProvider:
            last_batch_lookup_failed = True

            def get_users_batch(self, _usernames):
                return {}

            def close(self):
                return None

        soft_states = self.service.load_ad_states(
            lambda _connector_id: SoftFailureProvider(),
            {"default": ["safe-computed-name"]},
        )
        self.assertEqual(
            soft_states[("default", "safe-computed-name")]["status"],
            "unavailable",
        )

        class CloseFailureProvider:
            def get_users_batch(self, _usernames):
                return {}

            def close(self):
                raise RuntimeError("ldap://secret-host close failed")

        close_failure_states = self.service.load_ad_states(
            lambda _connector_id: CloseFailureProvider(),
            {"default": ["safe-computed-name"]},
        )
        self.assertEqual(
            close_failure_states[("default", "safe-computed-name")]["status"],
            "missing",
        )

    def test_protected_accounts_are_not_sent_to_the_target_provider(self):
        class Provider:
            calls = 0

            def get_users_batch(self, _usernames):
                self.calls += 1
                return {}

            def close(self):
                return None

        provider = Provider()
        states = self.service.load_ad_states(
            lambda _connector_id: provider,
            {"default": ["Administrator"]},
            protected_accounts_by_connector={"default": ["Administrator"]},
        )
        self.assertEqual(provider.calls, 0)
        self.assertEqual(states[("default", "administrator")]["status"], "protected")
        self.assertIsNone(states[("default", "administrator")]["exists"])


if __name__ == "__main__":
    unittest.main()
