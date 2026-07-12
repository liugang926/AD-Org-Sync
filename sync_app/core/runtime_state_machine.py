from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RuntimePhase(str, Enum):
    REPLAY = "replay"
    PREPARE = "prepare"
    PLAN = "plan"
    APPLY = "apply"
    FINALIZE = "finalize"


RUNTIME_PHASE_ORDER = tuple(phase.value for phase in RuntimePhase)


class RuntimePhaseTransitionError(RuntimeError):
    pass


@dataclass(slots=True)
class RuntimePhaseStateMachine:
    """In-memory transition guard complementing persisted recovery metadata."""

    active_phase: str = ""
    completed_phases: list[str] = field(default_factory=list)
    failed_phase: str = ""

    @staticmethod
    def normalize(phase: str | RuntimePhase) -> str:
        normalized = str(getattr(phase, "value", phase) or "").strip().lower()
        if normalized not in RUNTIME_PHASE_ORDER:
            raise RuntimePhaseTransitionError(f"unknown runtime phase: {normalized or '<empty>'}")
        return normalized

    def start(self, phase: str | RuntimePhase) -> str:
        normalized = self.normalize(phase)
        if self.failed_phase:
            raise RuntimePhaseTransitionError(
                f"cannot start {normalized}; runtime already failed in {self.failed_phase}"
            )
        if self.active_phase:
            raise RuntimePhaseTransitionError(
                f"cannot start {normalized}; {self.active_phase} is still active"
            )
        if normalized in self.completed_phases:
            raise RuntimePhaseTransitionError(f"runtime phase already completed: {normalized}")
        if self.completed_phases:
            last_index = RUNTIME_PHASE_ORDER.index(self.completed_phases[-1])
            expected_index = last_index + 1
            if expected_index >= len(RUNTIME_PHASE_ORDER):
                raise RuntimePhaseTransitionError("runtime already completed all phases")
            expected = RUNTIME_PHASE_ORDER[expected_index]
            if normalized != expected:
                raise RuntimePhaseTransitionError(
                    f"invalid runtime phase transition: expected {expected}, got {normalized}"
                )
        self.active_phase = normalized
        return normalized

    def complete(self, phase: str | RuntimePhase) -> None:
        normalized = self.normalize(phase)
        if self.active_phase != normalized:
            raise RuntimePhaseTransitionError(
                f"cannot complete {normalized}; active phase is {self.active_phase or '<none>'}"
            )
        self.active_phase = ""
        self.completed_phases.append(normalized)

    def fail(self, phase: str | RuntimePhase) -> None:
        normalized = self.normalize(phase)
        if self.active_phase != normalized:
            raise RuntimePhaseTransitionError(
                f"cannot fail {normalized}; active phase is {self.active_phase or '<none>'}"
            )
        self.active_phase = ""
        self.failed_phase = normalized

    def snapshot(self) -> dict[str, Any]:
        return {
            "active_phase": self.active_phase,
            "completed_phases": list(self.completed_phases),
            "failed_phase": self.failed_phase,
            "terminal": bool(self.failed_phase or self.completed_phases[-1:] == [RuntimePhase.FINALIZE.value]),
        }


__all__ = [
    "RUNTIME_PHASE_ORDER",
    "RuntimePhase",
    "RuntimePhaseStateMachine",
    "RuntimePhaseTransitionError",
]
