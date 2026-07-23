import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from sync_app.core.models import SyncRunStats
from sync_app.services.runtime_finalize import (
    finalize_failed_sync,
    finalize_interrupted_sync,
    finalize_successful_sync,
)


class RuntimeIdentityFinalizeTests(unittest.TestCase):
    def _context(self, *, error_count: int):
        stats = SyncRunStats()
        stats["error_count"] = error_count
        binding_repo = Mock()
        binding_repo.apply_successful_identity_bindings = Mock()
        marked_jobs = []
        hooks = SimpleNamespace(
            stats_callback=None,
            generate_skip_detail_report=lambda _stats: "skip.csv",
            generate_sync_operation_log=lambda _stats, _start, _config: "ops.csv",
            generate_sync_validation_report=lambda _stats, _current, _missing: "validation.txt",
            run_history_cleanup=lambda: {},
            mark_job=lambda status, **kwargs: marked_jobs.append((status, kwargs)),
            record_event=lambda *args, **kwargs: None,
            record_operation=lambda **kwargs: None,
        )
        context = SimpleNamespace(
            job_id="identity-finalize-job",
            execution_mode="apply",
            start_time=time.time(),
            sync_stats=stats,
            hooks=hooks,
            repositories=SimpleNamespace(
                user_binding_repo=binding_repo,
                replay_request_repo=SimpleNamespace(mark_finished=lambda *args, **kwargs: None),
            ),
            identity=SimpleNamespace(
                successful_apply_bindings=[
                    {
                        "source_user_id": "ding-001",
                        "connector_id": "default",
                        "ad_username": "TJ001",
                    }
                ]
            ),
            organization=SimpleNamespace(org_id="default", name="Default"),
            config=SimpleNamespace(source_provider="dingtalk"),
            environment=SimpleNamespace(bot=None, source_provider_name="DingTalk"),
            working=SimpleNamespace(
                current_source_ad_usernames_by_connector={},
                enabled_ad_users_by_connector={},
                managed_ad_identities=set(),
            ),
            plan=SimpleNamespace(
                approved_review=None,
                plan_fingerprint="plan-finalize",
                started_replay_requests=[],
            ),
            planned_count=1,
            executed_count=1,
            high_risk_operation_count=0,
            logger=Mock(),
        )
        return context, binding_repo, marked_jobs

    def test_completed_apply_persists_each_successful_binding(self):
        success_ctx, success_repo, success_jobs = self._context(error_count=0)
        finalize_successful_sync(success_ctx)
        success_repo.apply_successful_identity_bindings.assert_called_once()
        self.assertEqual(success_jobs[-1][0], "COMPLETED")

        partial_ctx, partial_repo, partial_jobs = self._context(error_count=1)
        finalize_successful_sync(partial_ctx)
        partial_repo.apply_successful_identity_bindings.assert_called_once_with(
            partial_ctx.identity.successful_apply_bindings,
            org_id="default",
            source_provider="dingtalk",
        )
        self.assertEqual(partial_jobs[-1][0], "COMPLETED_WITH_ERRORS")

    def test_failed_and_canceled_apply_never_confirm_bindings(self):
        canceled_ctx, canceled_repo, canceled_jobs = self._context(error_count=0)
        finalize_interrupted_sync(canceled_ctx, InterruptedError("operator canceled"))
        canceled_repo.apply_successful_identity_bindings.assert_not_called()
        self.assertEqual(canceled_jobs[-1][0], "CANCELED")

        failed_ctx, failed_repo, failed_jobs = self._context(error_count=0)
        finalize_failed_sync(failed_ctx, RuntimeError("apply failed"))
        failed_repo.apply_successful_identity_bindings.assert_not_called()
        self.assertEqual(failed_jobs[-1][0], "FAILED")

    def test_finalize_failure_before_completion_never_confirms_bindings(self):
        context, binding_repo, marked_jobs = self._context(error_count=0)
        context.hooks.generate_skip_detail_report = Mock(
            side_effect=RuntimeError("report generation failed")
        )

        with self.assertRaisesRegex(RuntimeError, "report generation failed"):
            finalize_successful_sync(context)

        binding_repo.apply_successful_identity_bindings.assert_not_called()
        self.assertEqual(marked_jobs, [])


if __name__ == "__main__":
    unittest.main()
