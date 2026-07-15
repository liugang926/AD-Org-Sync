from datetime import datetime, timedelta, timezone

from sync_app.services.high_risk_operations import (
    HIGH_RISK_WORKFLOW_STEPS,
    HighRiskOperationContext,
    HighRiskOperationPolicy,
    high_risk_audit_payload,
    is_environment_marked,
    resolve_environment_label,
)


def _context(**overrides):
    values = {
        "operation_code": "binding.cleanup",
        "organization_id": "Tenant-A",
        "organization_name": "Tenant A",
        "environment_label": "Staging",
        "snapshot_version": "#42",
        "impact_count": 2,
        "preview_id": "preview-42",
    }
    values.update(overrides)
    return HighRiskOperationContext.create(**values)


def test_environment_resolution_is_explicit_local_or_fail_closed(monkeypatch):
    monkeypatch.delenv("AD_ORG_SYNC_ENVIRONMENT_LABEL", raising=False)

    assert resolve_environment_label(explicit_label="Production", bind_host="0.0.0.0") == "Production"
    assert resolve_environment_label(bind_host="127.0.0.1") == "Local environment"
    assert resolve_environment_label(bind_host="0.0.0.0") == "Unlabeled environment"
    assert is_environment_marked("Production") is True
    assert is_environment_marked("Unlabeled environment") is False


def test_unlabeled_environment_blocks_high_risk_operation():
    decision = HighRiskOperationPolicy.evaluate(
        _context(environment_label="Unlabeled environment")
    )

    assert decision.allowed is False
    assert decision.reason_code == "high_risk.blocker.environment_unlabeled"
    assert decision.next_action_code == "high_risk.action.label_environment"


def test_confirmation_must_match_every_server_context_field():
    context = _context()
    submitted = {
        "operation_code": context.operation_code,
        "organization_id": context.organization_id,
        "environment_label": context.environment_label,
        "snapshot_version": context.snapshot_version,
        "impact_count": str(context.impact_count),
        "preview_id": context.preview_id,
    }

    assert HighRiskOperationPolicy.validate_confirmation(context, submitted).allowed is True
    submitted["impact_count"] = "999"
    changed = HighRiskOperationPolicy.validate_confirmation(context, submitted)
    assert changed.allowed is False
    assert changed.reason_code == "high_risk.blocker.preview_changed"


def test_target_fingerprint_is_order_independent_and_detects_change():
    first = {
        "source_provider": "wecom",
        "connector_id": "default",
        "source_user_id": "alice",
        "ad_username": "alice.old",
    }
    second = {
        "source_provider": "wecom",
        "connector_id": "default",
        "source_user_id": "bob",
        "ad_username": "bob.old",
    }

    baseline = HighRiskOperationPolicy.target_fingerprint([first, second])
    assert baseline == HighRiskOperationPolicy.target_fingerprint([second, first])
    assert baseline != HighRiskOperationPolicy.target_fingerprint(
        [first, {**second, "ad_username": "bob.changed"}]
    )


def test_preview_expiration_and_workflow_contract_are_stable():
    now = datetime.now(timezone.utc)

    assert HighRiskOperationPolicy.preview_expired(
        (now - timedelta(minutes=16)).isoformat(),
        max_age_seconds=900,
        now=now,
    )
    assert not HighRiskOperationPolicy.preview_expired(
        (now - timedelta(minutes=14)).isoformat(),
        max_age_seconds=900,
        now=now,
    )
    assert [item[0] for item in HIGH_RISK_WORKFLOW_STEPS] == [
        "scan",
        "preview",
        "confirm",
        "execute",
        "audit",
    ]
    workflow = HighRiskOperationPolicy.workflow(
        scan_state="complete",
        preview_state="current",
        confirm_state="invalid",
    )
    assert [step["state"] for step in workflow] == [
        "complete",
        "current",
        "pending",
        "pending",
        "pending",
    ]


def test_audit_payload_always_contains_required_high_risk_context():
    payload = high_risk_audit_payload(_context(), reason_code="unit-test")

    assert payload == {
        "operation_code": "binding.cleanup",
        "organization_id": "tenant-a",
        "organization_name": "Tenant A",
        "environment_label": "Staging",
        "environment_marked": True,
        "snapshot_version": "#42",
        "impact_count": 2,
        "preview_id": "preview-42",
        "reason_code": "unit-test",
    }
