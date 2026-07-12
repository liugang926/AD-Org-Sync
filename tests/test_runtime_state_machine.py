from __future__ import annotations

import itertools
import unittest

from sync_app.core.runtime_state_machine import (
    RUNTIME_PHASE_ORDER,
    RuntimePhaseStateMachine,
    RuntimePhaseTransitionError,
)


class RuntimePhaseStateMachineTests(unittest.TestCase):
    def test_canonical_phase_sequence_completes(self) -> None:
        machine = RuntimePhaseStateMachine()
        for phase in RUNTIME_PHASE_ORDER:
            machine.start(phase)
            machine.complete(phase)

        snapshot = machine.snapshot()
        self.assertEqual(snapshot["completed_phases"], list(RUNTIME_PHASE_ORDER))
        self.assertTrue(snapshot["terminal"])

    def test_every_noncanonical_full_permutation_is_rejected(self) -> None:
        accepted_sequences = []
        for sequence in itertools.permutations(RUNTIME_PHASE_ORDER):
            machine = RuntimePhaseStateMachine()
            try:
                for phase in sequence:
                    machine.start(phase)
                    machine.complete(phase)
            except RuntimePhaseTransitionError:
                continue
            accepted_sequences.append(sequence)

        self.assertEqual(accepted_sequences, [RUNTIME_PHASE_ORDER])

    def test_failure_is_terminal_and_duplicate_completion_is_rejected(self) -> None:
        failed = RuntimePhaseStateMachine()
        failed.start("plan")
        failed.fail("plan")
        with self.assertRaisesRegex(RuntimePhaseTransitionError, "already failed"):
            failed.start("apply")

        active = RuntimePhaseStateMachine()
        active.start("prepare")
        active.complete("prepare")
        with self.assertRaisesRegex(RuntimePhaseTransitionError, "active phase is <none>"):
            active.complete("prepare")

    def test_unknown_phase_is_rejected(self) -> None:
        with self.assertRaisesRegex(RuntimePhaseTransitionError, "unknown runtime phase"):
            RuntimePhaseStateMachine().start("publish")


if __name__ == "__main__":
    unittest.main()
