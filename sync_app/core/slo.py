from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class RuntimeSLOPolicy:
    minimum_sync_success_rate: float = 0.99
    minimum_outbox_delivery_rate: float = 0.995
    maximum_phase_duration_seconds: Mapping[str, float] = field(
        default_factory=lambda: {"plan": 300.0, "apply": 1800.0}
    )


def _counter_value(snapshot: Mapping[str, Any], name: str, **labels: str) -> float:
    for item in list(snapshot.get("counters") or []):
        if item.get("name") == name and dict(item.get("labels") or {}) == labels:
            return float(item.get("value") or 0)
    return 0.0


def _observation(snapshot: Mapping[str, Any], name: str, **labels: str) -> Mapping[str, Any] | None:
    for item in list(snapshot.get("observations") or []):
        if item.get("name") == name and dict(item.get("labels") or {}) == labels:
            return item
    return None


def _rate_objective(name: str, numerator: float, denominator: float, target: float) -> dict[str, Any]:
    if denominator <= 0:
        return {"name": name, "status": "no_data", "observed": None, "target": target}
    observed = numerator / denominator
    return {
        "name": name,
        "status": "met" if observed >= target else "breached",
        "observed": observed,
        "target": target,
        "sample_count": denominator,
    }


def evaluate_runtime_slos(
    metrics_snapshot: Mapping[str, Any],
    *,
    policy: RuntimeSLOPolicy | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or RuntimeSLOPolicy()
    succeeded = _counter_value(
        metrics_snapshot,
        "ad_org_sync_runs_total",
        status="succeeded",
    )
    failed = _counter_value(metrics_snapshot, "ad_org_sync_runs_total", status="failed")
    completed_with_errors = _counter_value(
        metrics_snapshot,
        "ad_org_sync_runs_total",
        status="completed_with_errors",
    )
    objectives = [
        _rate_objective(
            "sync_success_rate",
            succeeded,
            succeeded + failed + completed_with_errors,
            resolved_policy.minimum_sync_success_rate,
        )
    ]

    delivered = _counter_value(metrics_snapshot, "ad_org_sync_outbox_delivered_total")
    dead_lettered = _counter_value(metrics_snapshot, "ad_org_sync_outbox_dead_lettered_total")
    objectives.append(
        _rate_objective(
            "outbox_delivery_rate",
            delivered,
            delivered + dead_lettered,
            resolved_policy.minimum_outbox_delivery_rate,
        )
    )

    for phase, maximum_seconds in resolved_policy.maximum_phase_duration_seconds.items():
        observation = _observation(
            metrics_snapshot,
            "ad_org_sync_phase_duration_seconds",
            phase=phase,
        )
        if not observation or float(observation.get("count") or 0) <= 0:
            objectives.append(
                {
                    "name": f"{phase}_phase_max_duration",
                    "status": "no_data",
                    "observed": None,
                    "target": maximum_seconds,
                }
            )
            continue
        observed = float(observation.get("max") or 0)
        objectives.append(
            {
                "name": f"{phase}_phase_max_duration",
                "status": "met" if observed <= maximum_seconds else "breached",
                "observed": observed,
                "target": maximum_seconds,
                "sample_count": float(observation.get("count") or 0),
            }
        )

    statuses = {item["status"] for item in objectives}
    if "breached" in statuses:
        status = "degraded"
    elif statuses == {"no_data"}:
        status = "unknown"
    else:
        status = "healthy"
    return {
        "status": status,
        "objectives": objectives,
        "policy": {
            "minimum_sync_success_rate": resolved_policy.minimum_sync_success_rate,
            "minimum_outbox_delivery_rate": resolved_policy.minimum_outbox_delivery_rate,
            "maximum_phase_duration_seconds": dict(resolved_policy.maximum_phase_duration_seconds),
        },
    }


__all__ = ["RuntimeSLOPolicy", "evaluate_runtime_slos"]
