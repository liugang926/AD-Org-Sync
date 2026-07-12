import unittest
from types import SimpleNamespace

from sync_app.core.models import SyncRunStats
from sync_app.services.runtime_phases import run_runtime_phase


class RuntimePhaseTests(unittest.TestCase):
    def _context(self, *, canceled: bool = False):
        events = []
        phase_calls = []
        job_repo = SimpleNamespace(
            mark_phase_started=lambda job_id, phase: phase_calls.append(("started", job_id, phase)),
            mark_phase_completed=lambda job_id, phase: phase_calls.append(("completed", job_id, phase)),
            mark_phase_failed=lambda job_id, phase, hint: phase_calls.append(("failed", job_id, phase, hint)),
        )
        hooks = SimpleNamespace(
            is_cancelled=lambda: canceled,
            record_event=lambda level, event_type, message, stage_name=None, payload=None: events.append(
                {
                    "level": level,
                    "event_type": event_type,
                    "message": message,
                    "stage_name": stage_name,
                    "payload": payload or {},
                }
            ),
        )
        context = SimpleNamespace(
            hooks=hooks,
            sync_stats=SyncRunStats(),
            repositories=SimpleNamespace(job_repo=job_repo),
            job_id="phase-job",
            logger=None,
        )
        return context, events, phase_calls

    def test_successful_phase_records_boundaries_and_duration(self):
        ctx, events, phase_calls = self._context()

        result = run_runtime_phase(ctx, "plan", lambda: {"summary": {"ok": True}})

        self.assertEqual([event["event_type"] for event in events], ["phase_started", "phase_completed"])
        self.assertIn("plan", ctx.sync_stats.phase_durations_ms)
        self.assertEqual(result["phase_durations_ms"], ctx.sync_stats.phase_durations_ms)
        self.assertEqual(result["summary"]["phase_durations_ms"], ctx.sync_stats.phase_durations_ms)
        self.assertEqual(phase_calls, [("started", "phase-job", "plan"), ("completed", "phase-job", "plan")])
        self.assertEqual(ctx.sync_stats.phase_state["completed_phases"], ["plan"])

    def test_failed_phase_records_failure_and_preserves_exception(self):
        ctx, events, phase_calls = self._context()

        with self.assertRaisesRegex(RuntimeError, "boom"):
            run_runtime_phase(ctx, "apply", lambda: (_ for _ in ()).throw(RuntimeError("boom")))

        self.assertEqual([event["event_type"] for event in events], ["phase_started", "phase_failed"])
        self.assertEqual(events[-1]["payload"]["error_type"], "RuntimeError")
        self.assertIn("apply", ctx.sync_stats.phase_durations_ms)
        self.assertEqual(phase_calls[0], ("started", "phase-job", "apply"))
        self.assertEqual(phase_calls[1][:3], ("failed", "phase-job", "apply"))
        self.assertIn("inspect operation logs", phase_calls[1][3])
        self.assertEqual(ctx.sync_stats.phase_state["failed_phase"], "apply")
        self.assertTrue(ctx.sync_stats.phase_state["terminal"])

    def test_canceled_phase_does_not_run_operation(self):
        ctx, events, phase_calls = self._context(canceled=True)
        called = False

        def operation():
            nonlocal called
            called = True

        with self.assertRaisesRegex(InterruptedError, "before prepare phase"):
            run_runtime_phase(ctx, "prepare", operation)

        self.assertFalse(called)
        self.assertEqual([event["event_type"] for event in events], ["phase_canceled"])
        self.assertEqual(phase_calls, [])

    def test_out_of_order_transition_is_rejected_before_persistence(self):
        ctx, _events, phase_calls = self._context()
        run_runtime_phase(ctx, "prepare", lambda: None)

        with self.assertRaisesRegex(RuntimeError, "expected plan, got apply"):
            run_runtime_phase(ctx, "apply", lambda: None)

        self.assertEqual(
            phase_calls,
            [("started", "phase-job", "prepare"), ("completed", "phase-job", "prepare")],
        )


if __name__ == "__main__":
    unittest.main()
