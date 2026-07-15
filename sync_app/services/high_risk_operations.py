from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping


UNLABELED_ENVIRONMENT_VALUES = frozenset(
    {
        "",
        "-",
        "unknown",
        "unknown environment",
        "unlabeled",
        "unlabelled",
        "unlabeled environment",
        "unlabelled environment",
    }
)
LOOPBACK_BIND_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
HIGH_RISK_WORKFLOW_STEPS = (
    ("scan", "Scan"),
    ("preview", "Preview"),
    ("confirm", "Confirm"),
    ("execute", "Execute"),
    ("audit", "Audit"),
)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def resolve_environment_label(
    *,
    explicit_label: str | None = None,
    bind_host: str | None = None,
    settings_repo: Any | None = None,
    org_id: str | None = None,
) -> str:
    label = _clean_text(
        explicit_label
        if explicit_label is not None
        else os.environ.get("AD_ORG_SYNC_ENVIRONMENT_LABEL", "")
    )
    if label:
        return label

    resolved_bind_host = _clean_text(bind_host).lower()
    if not resolved_bind_host and settings_repo is not None:
        resolved_bind_host = _clean_text(
            settings_repo.get_value(
                "web_bind_host",
                "127.0.0.1",
                org_id=org_id,
            )
        ).lower()
    if resolved_bind_host in LOOPBACK_BIND_HOSTS:
        return "Local environment"
    return "Unlabeled environment"


def is_environment_marked(environment_label: str | None) -> bool:
    return _clean_text(environment_label).casefold() not in UNLABELED_ENVIRONMENT_VALUES


@dataclass(frozen=True, slots=True)
class HighRiskOperationContext:
    operation_code: str
    organization_id: str
    organization_name: str
    environment_label: str
    snapshot_version: str
    impact_count: int
    preview_id: str = ""

    @classmethod
    def create(
        cls,
        *,
        operation_code: str,
        organization_id: str,
        organization_name: str,
        environment_label: str,
        snapshot_version: str | int | None,
        impact_count: int,
        preview_id: str = "",
    ) -> "HighRiskOperationContext":
        normalized_snapshot = _clean_text(snapshot_version) or "Not available"
        return cls(
            operation_code=_clean_text(operation_code),
            organization_id=_clean_text(organization_id).lower(),
            organization_name=_clean_text(organization_name) or _clean_text(organization_id),
            environment_label=_clean_text(environment_label) or "Unlabeled environment",
            snapshot_version=normalized_snapshot,
            impact_count=max(int(impact_count or 0), 0),
            preview_id=_clean_text(preview_id),
        )

    @property
    def environment_marked(self) -> bool:
        return is_environment_marked(self.environment_label)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["environment_marked"] = self.environment_marked
        return value


@dataclass(frozen=True, slots=True)
class HighRiskGateDecision:
    allowed: bool
    reason_code: str = ""
    next_action_code: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class HighRiskOperationPolicy:
    @staticmethod
    def evaluate(context: HighRiskOperationContext) -> HighRiskGateDecision:
        if not context.environment_marked:
            return HighRiskGateDecision(
                allowed=False,
                reason_code="high_risk.blocker.environment_unlabeled",
                next_action_code="high_risk.action.label_environment",
            )
        return HighRiskGateDecision(allowed=True)

    @staticmethod
    def validate_confirmation(
        context: HighRiskOperationContext,
        submitted: Mapping[str, Any],
    ) -> HighRiskGateDecision:
        expected = {
            "operation_code": context.operation_code,
            "organization_id": context.organization_id,
            "environment_label": context.environment_label,
            "snapshot_version": context.snapshot_version,
            "impact_count": str(context.impact_count),
            "preview_id": context.preview_id,
        }
        actual = {
            key: _clean_text(submitted.get(key))
            for key in expected
        }
        if any(actual[key] != str(value) for key, value in expected.items()):
            return HighRiskGateDecision(
                allowed=False,
                reason_code="high_risk.blocker.preview_changed",
                next_action_code="high_risk.action.scan_again",
            )
        return HighRiskOperationPolicy.evaluate(context)

    @staticmethod
    def target_fingerprint(targets: Iterable[Mapping[str, Any]]) -> str:
        normalized_targets = sorted(
            (
                {
                    "source_provider": _clean_text(target.get("source_provider")).lower(),
                    "connector_id": _clean_text(target.get("connector_id")) or "default",
                    "source_user_id": _clean_text(target.get("source_user_id")),
                    "ad_username": _clean_text(target.get("ad_username")).lower(),
                }
                for target in targets
            ),
            key=lambda value: (
                value["source_provider"],
                value["connector_id"],
                value["source_user_id"],
                value["ad_username"],
            ),
        )
        payload = json.dumps(
            normalized_targets,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def preview_expired(
        scanned_at: str | None,
        *,
        max_age_seconds: int = 900,
        now: datetime | None = None,
    ) -> bool:
        raw_value = _clean_text(scanned_at)
        if not raw_value:
            return True
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return True
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc) < current.astimezone(timezone.utc) - timedelta(
            seconds=max(int(max_age_seconds), 1)
        )

    @staticmethod
    def workflow(
        *,
        scan_state: str = "pending",
        preview_state: str = "pending",
        confirm_state: str = "pending",
        execute_state: str = "pending",
        audit_state: str = "pending",
    ) -> list[dict[str, str]]:
        states = {
            "scan": scan_state,
            "preview": preview_state,
            "confirm": confirm_state,
            "execute": execute_state,
            "audit": audit_state,
        }
        allowed_states = {"pending", "current", "complete", "blocked"}
        return [
            {
                "code": code,
                "label": label,
                "state": states[code] if states[code] in allowed_states else "pending",
            }
            for code, label in HIGH_RISK_WORKFLOW_STEPS
        ]


def high_risk_audit_payload(
    context: HighRiskOperationContext,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "operation_code": context.operation_code,
        "organization_id": context.organization_id,
        "organization_name": context.organization_name,
        "environment_label": context.environment_label,
        "environment_marked": context.environment_marked,
        "snapshot_version": context.snapshot_version,
        "impact_count": context.impact_count,
        "preview_id": context.preview_id,
        **extra,
    }


def build_apply_operation_context(
    *,
    settings_repo: Any,
    job_repo: Any,
    organization_id: str,
    organization_name: str,
    environment_label: str | None = None,
) -> HighRiskOperationContext:
    recent_jobs = job_repo.list_recent_job_records(
        limit=50,
        org_id=organization_id,
    )
    latest_dry_run = next(
        (
            job
            for job in recent_jobs
            if _clean_text(getattr(job, "execution_mode", "")).lower() == "dry_run"
            and _clean_text(getattr(job, "status", "")).upper() == "COMPLETED"
        ),
        None,
    )
    summary = dict(getattr(latest_dry_run, "summary", {}) or {}) if latest_dry_run else {}
    source_snapshot_id = int(summary.get("source_snapshot_id") or 0)
    return HighRiskOperationContext.create(
        operation_code="sync.apply",
        organization_id=organization_id,
        organization_name=organization_name,
        environment_label=resolve_environment_label(
            explicit_label=environment_label,
            settings_repo=settings_repo,
            org_id=organization_id,
        ),
        snapshot_version=f"#{source_snapshot_id}" if source_snapshot_id else "Not available",
        impact_count=int(
            summary.get("planned_operation_count")
            or getattr(latest_dry_run, "planned_operation_count", 0)
            or 0
        ),
        preview_id=_clean_text(getattr(latest_dry_run, "job_id", "")),
    )
