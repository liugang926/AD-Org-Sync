from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, TypeVar

from sync_app.core.observability import METRICS
from sync_app.core.runtime_state_machine import RuntimePhaseStateMachine
from sync_app.services.runtime_context import SyncContext


T = TypeVar("T")


def _job_repo(ctx: SyncContext):
    repositories = getattr(ctx, "repositories", None)
    return getattr(repositories, "job_repo", None)


def _phase_recovery_hint(phase_name: str) -> str:
    if phase_name in {"replay", "prepare", "plan"}:
        return "enqueue a fresh run; no apply phase was entered"
    return "inspect operation logs and target state before running a new dry run"


def _phase_machine(ctx: SyncContext) -> RuntimePhaseStateMachine:
    machine = getattr(ctx, "_runtime_phase_state_machine", None)
    if isinstance(machine, RuntimePhaseStateMachine):
        return machine
    machine = RuntimePhaseStateMachine()
    setattr(ctx, "_runtime_phase_state_machine", machine)
    return machine


def _store_phase_state(ctx: SyncContext, machine: RuntimePhaseStateMachine) -> None:
    ctx.sync_stats.phase_state = machine.snapshot()


def _record_phase_failure_state(ctx: SyncContext, phase_name: str) -> None:
    job_repo = _job_repo(ctx)
    if not job_repo:
        return
    try:
        job_repo.mark_phase_failed(ctx.job_id, phase_name, _phase_recovery_hint(phase_name))
    except Exception as exc:
        logger = getattr(ctx, "logger", None)
        if logger:
            logger.warning("failed to persist runtime phase failure for %s: %s", phase_name, exc)


def _store_phase_duration(ctx: SyncContext, phase_name: str, started_at: float) -> int:
    duration_ms = max(int(round((time.perf_counter() - started_at) * 1000)), 0)
    ctx.sync_stats.phase_durations_ms[phase_name] = duration_ms
    METRICS.observe(
        "ad_org_sync_phase_duration_seconds",
        duration_ms / 1000,
        labels={"phase": phase_name},
    )
    return duration_ms


def run_runtime_phase(ctx: SyncContext, phase_name: str, operation: Callable[[], T]) -> T:
    """Execute one observable runtime phase without hiding its exceptions."""
    machine = _phase_machine(ctx)
    normalized_phase = machine.normalize(phase_name)
    if ctx.hooks.is_cancelled():
        ctx.hooks.record_event(
            "WARNING",
            "phase_canceled",
            f"runtime phase canceled before start: {normalized_phase}",
            stage_name=normalized_phase,
            payload={"phase": normalized_phase},
        )
        raise InterruptedError(f"sync canceled before {normalized_phase} phase")

    machine.start(normalized_phase)
    _store_phase_state(ctx, machine)
    started_at = time.perf_counter()
    METRICS.increment("ad_org_sync_phase_started_total", labels={"phase": normalized_phase})
    job_repo = _job_repo(ctx)
    if job_repo:
        job_repo.mark_phase_started(ctx.job_id, normalized_phase)
    ctx.hooks.record_event(
        "INFO",
        "phase_started",
        f"runtime phase started: {normalized_phase}",
        stage_name=normalized_phase,
        payload={"phase": normalized_phase},
    )
    try:
        result = operation()
    except InterruptedError as exc:
        machine.fail(normalized_phase)
        _store_phase_state(ctx, machine)
        METRICS.increment(
            "ad_org_sync_phase_completed_total",
            labels={"phase": normalized_phase, "status": "canceled"},
        )
        duration_ms = _store_phase_duration(ctx, normalized_phase, started_at)
        _record_phase_failure_state(ctx, normalized_phase)
        ctx.hooks.record_event(
            "WARNING",
            "phase_canceled",
            f"runtime phase canceled: {normalized_phase}",
            stage_name=normalized_phase,
            payload={"phase": normalized_phase, "duration_ms": duration_ms, "error": str(exc)},
        )
        raise
    except Exception as exc:
        machine.fail(normalized_phase)
        _store_phase_state(ctx, machine)
        METRICS.increment(
            "ad_org_sync_phase_completed_total",
            labels={"phase": normalized_phase, "status": "failed"},
        )
        duration_ms = _store_phase_duration(ctx, normalized_phase, started_at)
        _record_phase_failure_state(ctx, normalized_phase)
        ctx.hooks.record_event(
            "ERROR",
            "phase_failed",
            f"runtime phase failed: {normalized_phase}",
            stage_name=normalized_phase,
            payload={
                "phase": normalized_phase,
                "duration_ms": duration_ms,
                "error": str(exc),
                "error_type": type(exc).__name__,
            },
        )
        raise

    duration_ms = _store_phase_duration(ctx, normalized_phase, started_at)
    machine.complete(normalized_phase)
    _store_phase_state(ctx, machine)
    METRICS.increment(
        "ad_org_sync_phase_completed_total",
        labels={"phase": normalized_phase, "status": "succeeded"},
    )
    if job_repo:
        job_repo.mark_phase_completed(ctx.job_id, normalized_phase)
    ctx.hooks.record_event(
        "INFO",
        "phase_completed",
        f"runtime phase completed: {normalized_phase}",
        stage_name=normalized_phase,
        payload={"phase": normalized_phase, "duration_ms": duration_ms},
    )
    if isinstance(result, dict):
        result["phase_durations_ms"] = dict(ctx.sync_stats.phase_durations_ms)
        summary = result.get("summary")
        if isinstance(summary, dict):
            summary["phase_durations_ms"] = dict(ctx.sync_stats.phase_durations_ms)
    return result


__all__ = ["run_runtime_phase"]
