from sync_app.services.identity_relationships import (
    IdentityRelationshipPreview,
    assess_identity_match,
)


def _preview(**overrides):
    values = {
        "org_id": "default",
        "source_provider": "wecom",
        "connector_id": "default",
        "source_user_id": "alice",
        "source_display_name": "Alice",
        "employee_id": "E001",
        "source_user": {},
        "mapping_input": {"method": "employee_id"},
        "candidate_mapping": {"ad_username": "E001"},
        "before_state": {"bound_ad_username": "", "binding_source": ""},
        "planned_after_state": {},
        "applied_after_state": {},
        "effective_ad_username": "E001",
        "effective_resolution_source": "employee_id",
        "resolution_reason": "Field mapping candidate only",
        "rule_hits": [],
        "difference": {"status": "not_dry_run", "changed": False},
    }
    values.update(overrides)
    return IdentityRelationshipPreview(**values)


def test_manual_binding_precedence_is_explicit_and_not_scored_as_generated_match():
    result = assess_identity_match(
        _preview(
            before_state={
                "bound_ad_username": "alice.reviewed",
                "binding_source": "manual",
            },
            effective_ad_username="alice.reviewed",
            effective_resolution_source="manual_binding",
            resolution_reason="Manual binding overrides the field-generated candidate",
            difference={"status": "manual_binding_overrides_candidate", "changed": True},
            risks=["normalized_username_collision"],
        )
    )

    assert result["status"] == "manual_override"
    assert result["confidence"] == "not_applicable"
    assert result["next_action"] == "Review Manual Override"


def test_conflict_and_missing_candidate_fail_closed_to_human_or_source_repair():
    conflict = assess_identity_match(
        _preview(
            effective_ad_username="",
            effective_resolution_source="conflict",
            risks=["multiple_bindings"],
            resolution_reason="Multiple persisted bindings exist",
        )
    )
    missing = assess_identity_match(
        _preview(
            candidate_mapping={"ad_username": ""},
            effective_ad_username="",
            effective_resolution_source="unresolved",
        )
    )

    assert conflict == {
        "status": "conflict",
        "confidence": "low",
        "level": "error",
        "reason": "Multiple persisted bindings exist",
        "next_action": "Open Conflict Queue",
    }
    assert missing["status"] == "blocked"
    assert missing["next_action"] == "Repair Source Data"


def test_unique_identifier_candidate_and_matching_binding_have_high_confidence():
    ready = assess_identity_match(_preview())
    confirmed = assess_identity_match(
        _preview(
            before_state={
                "bound_ad_username": "E001",
                "binding_source": "managed_generated",
            }
        )
    )

    assert ready["status"] == "ready"
    assert ready["confidence"] == "high"
    assert confirmed["status"] == "confirmed"
    assert confirmed["confidence"] == "high"


def test_disabled_binding_is_blocked_even_when_candidate_text_matches():
    result = assess_identity_match(
        _preview(
            before_state={
                "bound_ad_username": "E001",
                "binding_source": "managed_generated",
                "binding_enabled": False,
            }
        )
    )

    assert result["status"] == "blocked"
    assert result["next_action"] == "Review Binding"
