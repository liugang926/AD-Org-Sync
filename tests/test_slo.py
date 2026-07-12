from __future__ import annotations

import unittest

from sync_app.core.observability import MetricsRegistry
from sync_app.core.slo import RuntimeSLOPolicy, evaluate_runtime_slos


class RuntimeSLOTests(unittest.TestCase):
    def test_healthy_metrics_meet_all_objectives(self) -> None:
        metrics = MetricsRegistry()
        metrics.increment("ad_org_sync_runs_total", 100, labels={"status": "succeeded"})
        metrics.increment("ad_org_sync_runs_total", 1, labels={"status": "failed"})
        metrics.increment("ad_org_sync_outbox_delivered_total", 1000)
        metrics.observe("ad_org_sync_phase_duration_seconds", 12, labels={"phase": "plan"})
        metrics.observe("ad_org_sync_phase_duration_seconds", 80, labels={"phase": "apply"})

        result = evaluate_runtime_slos(metrics.snapshot())

        self.assertEqual(result["status"], "healthy")
        self.assertNotIn("breached", {item["status"] for item in result["objectives"]})

    def test_failures_dead_letters_and_slow_phase_breach_objectives(self) -> None:
        metrics = MetricsRegistry()
        metrics.increment("ad_org_sync_runs_total", 8, labels={"status": "succeeded"})
        metrics.increment("ad_org_sync_runs_total", 2, labels={"status": "completed_with_errors"})
        metrics.increment("ad_org_sync_outbox_delivered_total", 90)
        metrics.increment("ad_org_sync_outbox_dead_lettered_total", 10)
        metrics.observe("ad_org_sync_phase_duration_seconds", 31, labels={"phase": "plan"})

        result = evaluate_runtime_slos(
            metrics.snapshot(),
            policy=RuntimeSLOPolicy(
                minimum_sync_success_rate=0.95,
                minimum_outbox_delivery_rate=0.99,
                maximum_phase_duration_seconds={"plan": 30},
            ),
        )

        self.assertEqual(result["status"], "degraded")
        breached = {item["name"] for item in result["objectives"] if item["status"] == "breached"}
        self.assertEqual(
            breached,
            {"sync_success_rate", "outbox_delivery_rate", "plan_phase_max_duration"},
        )

    def test_empty_registry_reports_unknown_instead_of_false_health(self) -> None:
        result = evaluate_runtime_slos(MetricsRegistry().snapshot())

        self.assertEqual(result["status"], "unknown")
        self.assertEqual({item["status"] for item in result["objectives"]}, {"no_data"})


if __name__ == "__main__":
    unittest.main()
