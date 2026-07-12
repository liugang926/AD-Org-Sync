from __future__ import annotations

import logging
import unittest

from sync_app.core.observability import (
    MetricsRegistry,
    ObservabilityContextFilter,
    RedactingFormatter,
    bind_observability_context,
    normalize_correlation_id,
    redact_sensitive_text,
)


class ObservabilityTests(unittest.TestCase):
    def test_sensitive_values_and_direct_identifiers_are_redacted(self) -> None:
        raw = (
            'password="SuperSecret!" CorpSecret=corp-value '
            "Authorization: Bearer abc.def.ghi user=alice@example.com "
            "phone=13800138000 https://svc:plain-password@example.invalid/hook"
        )
        redacted = redact_sensitive_text(raw)

        for secret in ("SuperSecret!", "corp-value", "abc.def.ghi", "alice@example.com", "13800138000", "plain-password"):
            self.assertNotIn(secret, redacted)
        self.assertIn("[REDACTED]", redacted)
        self.assertIn("[REDACTED_EMAIL]", redacted)
        self.assertIn("[REDACTED_PHONE]", redacted)

    def test_log_formatter_adds_context_and_redacts_after_interpolation(self) -> None:
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="password=%s user=%s",
            args=("secret-value", "user@example.com"),
            exc_info=None,
        )
        with bind_observability_context(correlation_id="request-123", org_id="tenant-a", job_id="job-1"):
            self.assertTrue(ObservabilityContextFilter().filter(record))
            rendered = RedactingFormatter(
                "correlation_id=%(correlation_id)s org_id=%(org_id)s job_id=%(job_id)s %(message)s"
            ).format(record)

        self.assertIn("correlation_id=request-123", rendered)
        self.assertIn("org_id=tenant-a", rendered)
        self.assertIn("job_id=job-1", rendered)
        self.assertNotIn("secret-value", rendered)
        self.assertNotIn("user@example.com", rendered)

    def test_metrics_registry_exports_counters_and_duration_aggregates(self) -> None:
        registry = MetricsRegistry()
        registry.increment("sync_runs_total", labels={"status": "ok"})
        registry.increment("sync_runs_total", 2, labels={"status": "ok"})
        registry.observe("sync_duration_seconds", 1.25, labels={"phase": "plan"})
        registry.observe("sync_duration_seconds", 2.75, labels={"phase": "plan"})

        rendered = registry.render_prometheus()
        snapshot = registry.snapshot()
        self.assertIn('sync_runs_total{status="ok"} 3', rendered)
        self.assertIn('sync_duration_seconds_count{phase="plan"} 2', rendered)
        self.assertIn('sync_duration_seconds_sum{phase="plan"} 4', rendered)
        self.assertEqual(snapshot["observations"][0]["max"], 2.75)

    def test_correlation_id_rejects_header_injection_characters(self) -> None:
        self.assertEqual(normalize_correlation_id(" request\r\n-id/123 "), "request-id123")
        self.assertEqual(len(normalize_correlation_id("x" * 100)), 64)


if __name__ == "__main__":
    unittest.main()
