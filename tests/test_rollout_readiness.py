from __future__ import annotations

import unittest
import tempfile
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from sync_app.services.rollout_readiness import RolloutReadinessService
from sync_app.storage.local_db import (
    AccountTakeoverRepository,
    ADDirectorySnapshotRepository,
    AttributeMappingRuleRepository,
    ConfigReleaseSnapshotRepository,
    DataQualityReviewRepository,
    DatabaseManager,
    DepartmentOuMappingRepository,
    FieldAuthorityRuleRepository,
    IdentityMatchRuleRepository,
    IdentityMatchRunRepository,
    OrganizationConfigRepository,
    SettingsRepository,
    SourceConnectorRepository,
    SourceDirectoryRepository,
    SyncConflictRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
)
from tests.helpers.execution_plans import create_eligible_execution_plan


class _Settings:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get_value(self, key, default="", *, org_id=None):
        return self.values.get(key, default)

    def get_bool(self, key, default=False, *, org_id=None):
        return bool(self.values.get(key, default))


class _SourceDirectory:
    def __init__(self, *, snapshot=None, scope=None, job_scope=None):
        self.snapshot = snapshot
        self.scope = scope
        self.job_scope = job_scope

    def get_latest_successful_snapshot(self, **kwargs):
        return self.snapshot

    def get_scope_selection(self, **kwargs):
        return self.scope

    def get_job_scope(self, *args, **kwargs):
        return self.job_scope


class _ListRepository:
    def __init__(self, values=None):
        self.values = list(values or [])

    def list_enabled_rules(self, **kwargs):
        return self.values

    def list_rule_records(self, **kwargs):
        return self.values

    def list_mapping_records(self, **kwargs):
        return self.values

    def list_batches(self, **kwargs):
        return self.values


class _QualityReviews:
    def __init__(self, current=None, latest=None, fingerprint_review=None):
        self.current = current
        self.latest = latest
        self.fingerprint_review = fingerprint_review

    def get_review_for_snapshot(self, **kwargs):
        return self.current

    def get_latest_review(self, **kwargs):
        return self.latest

    def get_review_for_fingerprint(self, **kwargs):
        return self.fingerprint_review


class _MatchRuns:
    def __init__(self, run=None, candidates=None):
        self.run = run
        self.candidates = list(candidates or [])

    def get_latest_completed_run(self, **kwargs):
        return self.run

    def list_candidates(self, **kwargs):
        return self.candidates


class _Jobs:
    def __init__(self, jobs=None):
        self.jobs = list(jobs or [])

    def list_recent_job_records(self, **kwargs):
        return self.jobs


class _Reviews:
    def __init__(self, review=None):
        self.review = review

    def get_review_record_by_job_id(self, job_id):
        return self.review


class _Conflicts:
    def __init__(self, conflicts=None):
        self.conflicts = list(conflicts or [])

    def list_conflict_records(self, **kwargs):
        return self.conflicts


class RolloutReadinessTests(unittest.TestCase):
    def _service(
        self,
        *,
        source_snapshot=None,
        source_status="not_tested",
        ad_snapshot=None,
        review=None,
        latest_review=None,
        fingerprint_review=None,
        match_run=None,
        rules=None,
        scope=None,
        job_scope=None,
        jobs=None,
        plan_review=None,
        settings=None,
    ):
        rule_values = rules or []
        source_connector = SimpleNamespace(
            connection_status=source_status,
            last_tested_at="2026-07-22T09:00:00+00:00",
        )
        source_connectors = SimpleNamespace(
            get_connector=lambda *args, **kwargs: source_connector
        )
        ad_snapshots = SimpleNamespace(
            get_latest_successful_snapshot=lambda **kwargs: ad_snapshot
        )
        return RolloutReadinessService(
            db_manager=object(),
            org_config_repo=object(),
            settings_repo=_Settings(settings),
            source_directory_repo=_SourceDirectory(
                snapshot=source_snapshot,
                scope=scope,
                job_scope=job_scope,
            ),
            source_connector_repo=source_connectors,
            ad_directory_snapshot_repo=ad_snapshots,
            identity_match_rule_repo=_ListRepository(rule_values),
            identity_match_run_repo=_MatchRuns(match_run),
            field_authority_rule_repo=_ListRepository(),
            account_takeover_repo=_ListRepository(),
            attribute_mapping_repo=_ListRepository(),
            department_ou_mapping_repo=_ListRepository(),
            config_release_snapshot_repo=object(),
            data_quality_review_repo=_QualityReviews(
                review,
                latest_review,
                fingerprint_review,
            ),
            job_repo=_Jobs(jobs),
            review_repo=_Reviews(plan_review),
            conflict_repo=_Conflicts(),
        )

    @staticmethod
    def _rule():
        return SimpleNamespace(
            updated_at="2026-07-22T09:00:00+00:00",
            to_dict=lambda: {
                "rule_name": "employee-id",
                "rule_revision": 1,
            },
        )

    def _evaluate(self, service, **overrides):
        args = {
            "org_id": "org-1",
            "org_name": "Org 1",
            "source_provider": "wecom",
            "config_fingerprint": "cfg-1",
            "source_connector_configured": False,
            "ad_connector_configured": False,
        }
        args.update(overrides)
        with patch(
            "sync_app.services.rollout_readiness.build_config_release_center_data",
            return_value={
                "latest_snapshot": None,
                "has_unpublished_changes": True,
            },
        ):
            return service.evaluate(**args).to_dict()

    def test_new_organization_starts_at_source_connector_and_blocks_snapshots(self):
        result = self._evaluate(self._service())

        self.assertEqual(result["next_step"]["key"], "source_connector_ready")
        self.assertEqual(
            result["step_map"]["source_connector_ready"]["status"],
            "action_required",
        )
        self.assertEqual(
            result["step_map"]["source_snapshot_current"]["status"],
            "blocked",
        )

    def test_historical_snapshot_is_stale_when_source_connection_failed(self):
        source_snapshot = {
            "id": 11,
            "snapshot_fingerprint": "source-11",
            "completed_at": "2026-07-22T09:00:00+00:00",
        }
        result = self._evaluate(
            self._service(
                source_snapshot=source_snapshot,
                source_status="failed",
            ),
            source_connector_configured=True,
        )

        self.assertEqual(
            result["step_map"]["source_snapshot_current"]["status"],
            "stale",
        )
        self.assertEqual(
            result["step_map"]["identity_match_run_current"]["status"],
            "blocked",
        )

    def test_data_quality_review_is_valid_only_for_exact_source_snapshot(self):
        source_snapshot = {
            "id": 12,
            "snapshot_fingerprint": "source-12",
            "completed_at": "2026-07-22T10:00:00+00:00",
        }
        old_review = SimpleNamespace(
            status="confirmed",
            source_snapshot_fingerprint="source-11",
            reviewed_at="2026-07-22T09:30:00+00:00",
        )
        result = self._evaluate(
            self._service(
                source_snapshot=source_snapshot,
                source_status="connected",
                latest_review=old_review,
            ),
            source_connector_configured=True,
        )

        self.assertEqual(
            result["step_map"]["data_quality_reviewed"]["status"],
            "stale",
        )

    def test_data_quality_review_reuses_an_older_identical_fingerprint(self):
        source_snapshot = {
            "id": 13,
            "snapshot_fingerprint": "source-identical",
            "completed_at": "2026-07-22T10:00:00+00:00",
        }
        matching_review = SimpleNamespace(
            status="confirmed",
            source_snapshot_fingerprint="source-identical",
            reviewed_at="2026-07-21T09:30:00+00:00",
        )
        newer_different_review = SimpleNamespace(
            status="confirmed",
            source_snapshot_fingerprint="source-different",
            reviewed_at="2026-07-22T09:30:00+00:00",
        )

        result = self._evaluate(
            self._service(
                source_snapshot=source_snapshot,
                source_status="connected",
                latest_review=newer_different_review,
                fingerprint_review=matching_review,
            ),
            source_connector_configured=True,
        )

        self.assertEqual(
            result["step_map"]["data_quality_reviewed"]["status"],
            "complete",
        )

    def test_disabled_offboarding_needs_no_hidden_lifecycle_or_disabled_ou_setup(self):
        result = self._evaluate(
            self._service(
                settings={
                    "directory_root_ou_path": "Managed Users",
                    "source_root_unit_ids": "1",
                    "offboarding_lifecycle_enabled": False,
                }
            )
        )

        self.assertEqual(
            result["step_map"]["lifecycle_safety_configured"]["status"],
            "complete",
        )
        routing = result["step_map"]["department_ou_routing_configured"]
        self.assertEqual(routing["status"], "complete")
        self.assertTrue(routing["metadata"]["root_relationship_configured"])

    def test_new_ad_snapshot_makes_previous_match_run_stale(self):
        source_snapshot = {
            "id": 12,
            "snapshot_fingerprint": "source-12",
            "completed_at": "2026-07-22T10:00:00+00:00",
        }
        ad_snapshot = {
            "id": 22,
            "snapshot_fingerprint": "ad-22",
            "completed_at": "2026-07-22T10:05:00+00:00",
        }
        match_run = {
            "run_id": "match-1",
            "source_snapshot_ids_json": "[12]",
            "ad_snapshot_id": 21,
            "rules_fingerprint": "older",
            "completed_at": "2026-07-22T10:02:00+00:00",
        }
        result = self._evaluate(
            self._service(
                source_snapshot=source_snapshot,
                source_status="connected",
                ad_snapshot=ad_snapshot,
                match_run=match_run,
                rules=[self._rule()],
                settings={"ad_connection_status": "connected"},
            ),
            source_connector_configured=True,
            ad_connector_configured=True,
        )

        self.assertEqual(
            result["step_map"]["identity_match_run_current"]["status"],
            "stale",
        )


class RolloutReadinessIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_manager = DatabaseManager(
            db_path=f"{self.temp_dir.name}/rollout-readiness.db"
        )
        self.db_manager.initialize(create_startup_snapshot=False)
        self.settings_repo = SettingsRepository(self.db_manager)
        self.source_repo = SourceDirectoryRepository(self.db_manager)
        self.ad_snapshot_repo = ADDirectorySnapshotRepository(self.db_manager)
        self.match_rule_repo = IdentityMatchRuleRepository(self.db_manager)
        self.service = RolloutReadinessService(
            db_manager=self.db_manager,
            org_config_repo=OrganizationConfigRepository(self.db_manager),
            settings_repo=self.settings_repo,
            source_directory_repo=self.source_repo,
            source_connector_repo=SourceConnectorRepository(self.db_manager),
            ad_directory_snapshot_repo=self.ad_snapshot_repo,
            identity_match_rule_repo=self.match_rule_repo,
            identity_match_run_repo=IdentityMatchRunRepository(self.db_manager),
            field_authority_rule_repo=FieldAuthorityRuleRepository(self.db_manager),
            account_takeover_repo=AccountTakeoverRepository(self.db_manager),
            attribute_mapping_repo=AttributeMappingRuleRepository(self.db_manager),
            department_ou_mapping_repo=DepartmentOuMappingRepository(self.db_manager),
            config_release_snapshot_repo=ConfigReleaseSnapshotRepository(
                self.db_manager
            ),
            data_quality_review_repo=DataQualityReviewRepository(self.db_manager),
            job_repo=SyncJobRepository(self.db_manager),
            review_repo=SyncPlanReviewRepository(self.db_manager),
            conflict_repo=SyncConflictRepository(self.db_manager),
        )

    def _ready_result(self, job_id: str = "ready-rollout"):
        created = create_eligible_execution_plan(
            self.db_manager,
            job_id=job_id,
            approved=True,
        )
        config = OrganizationConfigRepository(self.db_manager).get_app_config(
            "default",
            config_path="config.ini",
        )
        result = self.service.evaluate(
            org_id="default",
            org_name="Default Organization",
            source_provider=config.source_provider,
            config_fingerprint=str(created["job"].config_snapshot_hash or ""),
            source_connector_configured=True,
            ad_connector_configured=True,
        ).to_dict()
        return created, result

    def test_complete_rollout_is_current_only_for_exact_evidence(self) -> None:
        _created, result = self._ready_result()

        self.assertEqual(result["step_map"]["dry_run_current"]["status"], "complete")
        self.assertEqual(result["step_map"]["approval_current"]["status"], "complete")
        self.assertEqual(result["step_map"]["apply_allowed"]["status"], "ready")

    def test_match_rule_change_stales_match_dry_run_and_approval(self) -> None:
        self._ready_result("rules-before-change")
        self.match_rule_repo.upsert_rule(
            org_id="default",
            rule_order=5,
            rule_name="Employee ID exact",
            source_provider="*",
            source_field="employee_id",
            ad_field="employeeID",
            allow_auto_link=True,
            confidence_level="certain",
            confidence_score=99,
            created_by="test",
        )
        config = OrganizationConfigRepository(self.db_manager).get_app_config(
            "default", config_path="config.ini"
        )
        result = self.service.evaluate(
            org_id="default",
            org_name="Default Organization",
            source_provider=config.source_provider,
            config_fingerprint="changed-by-rule-update",
            source_connector_configured=True,
            ad_connector_configured=True,
        ).to_dict()

        self.assertEqual(
            result["step_map"]["identity_match_run_current"]["status"], "stale"
        )
        self.assertEqual(result["step_map"]["dry_run_current"]["status"], "stale")
        self.assertEqual(result["step_map"]["approval_current"]["status"], "stale")

    def test_new_ad_snapshot_stales_match_and_historical_dry_run(self) -> None:
        created, _result = self._ready_result("ad-before-change")
        snapshot_id = self.ad_snapshot_repo.start_snapshot(
            org_id="default", connector_id="default", created_by="test"
        )
        self.ad_snapshot_repo.complete_snapshot(
            snapshot_id,
            org_id="default",
            user_count=0,
            ou_count=1,
            duplicate_employee_id_count=0,
            duplicate_employee_number_count=0,
            snapshot_fingerprint="sha256:v2:ad-snapshot:new-current",
            expires_at=(
                datetime.now(timezone.utc) + timedelta(hours=4)
            ).isoformat(timespec="seconds"),
        )
        config = OrganizationConfigRepository(self.db_manager).get_app_config(
            "default", config_path="config.ini"
        )
        result = self.service.evaluate(
            org_id="default",
            org_name="Default Organization",
            source_provider=config.source_provider,
            config_fingerprint=str(created["job"].config_snapshot_hash or ""),
            source_connector_configured=True,
            ad_connector_configured=True,
        ).to_dict()

        self.assertEqual(
            result["step_map"]["identity_match_run_current"]["status"], "stale"
        )
        self.assertEqual(result["step_map"]["dry_run_current"]["status"], "stale")

    def test_policy_change_stales_release_dry_run_and_approval(self) -> None:
        created, _result = self._ready_result("policy-before-change")
        FieldAuthorityRuleRepository(self.db_manager).upsert_rule(
            org_id="default",
            field_name="displayName",
            source_provider="*",
            source_priority=1,
            sync_direction="ad_to_source",
            sync_mode="fill_if_empty",
            is_enabled=True,
            created_by="test",
        )
        config = OrganizationConfigRepository(self.db_manager).get_app_config(
            "default", config_path="config.ini"
        )
        result = self.service.evaluate(
            org_id="default",
            org_name="Default Organization",
            source_provider=config.source_provider,
            config_fingerprint=str(created["job"].config_snapshot_hash or ""),
            source_connector_configured=True,
            ad_connector_configured=True,
        ).to_dict()

        self.assertEqual(
            result["step_map"]["policy_release_current"]["status"], "stale"
        )
        self.assertEqual(result["step_map"]["dry_run_current"]["status"], "stale")
        self.assertEqual(result["step_map"]["approval_current"]["status"], "stale")


if __name__ == "__main__":
    unittest.main()
