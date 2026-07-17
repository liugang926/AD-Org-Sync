from sync_app.services.identity_relationships import (
    IdentityRelationshipPreview,
    assess_identity_match,
    classify_identity_relationship,
    filter_identity_workbench_rows,
    summarize_identity_workbench_rows,
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


def test_workbench_business_conclusions_cover_binding_creation_and_ad_failures():
    bound = classify_identity_relationship(
        _preview(
            before_state={
                "bound_ad_username": "E001",
                "binding_source": "managed_generated",
                "binding_enabled": True,
                "ad_account_state": {"status": "enabled", "exists": True},
            }
        )
    )
    candidate_exists = classify_identity_relationship(
        _preview(
            candidate_ad_state={"status": "exists", "exists": True},
        )
    )
    creatable = classify_identity_relationship(
        _preview(
            candidate_ad_state={"status": "missing", "exists": False},
            creation_eligibility={"eligible": True},
        )
    )
    stale = classify_identity_relationship(
        _preview(
            before_state={
                "bound_ad_username": "legacy.alice",
                "binding_source": "manual",
                "binding_enabled": True,
                "ad_account_state": {"status": "missing", "exists": False},
            }
        )
    )
    mismatch = classify_identity_relationship(
        _preview(
            before_state={
                "bound_ad_username": "legacy.alice",
                "binding_source": "manual",
                "binding_enabled": True,
                "ad_account_state": {"status": "exists", "exists": True},
            }
        )
    )
    conflict = classify_identity_relationship(
        _preview(
            effective_ad_username="",
            effective_resolution_source="conflict",
            risks=["normalized_username_collision"],
        )
    )
    unknown = classify_identity_relationship(_preview())
    unavailable = classify_identity_relationship(
        _preview(
            candidate_ad_state={"status": "unavailable", "exists": None},
            before_state={
                "bound_ad_username": "",
                "binding_source": "",
                "ad_account_state": {"status": "unavailable", "exists": None},
            },
        )
    )

    assert bound["code"] == "bound_account_exists"
    assert candidate_exists["code"] == "unbound_candidate_exists"
    assert creatable["code"] == "creatable"
    assert stale["code"] == "saved_binding_expired"
    assert mismatch["code"] == "candidate_binding_mismatch"
    assert conflict["code"] == "multiple_user_candidate_conflict"
    assert unknown["code"] == "ad_status_unknown"
    assert unavailable["code"] == "connector_unavailable"


def test_workbench_summary_and_filters_use_the_same_queue_membership():
    rows = [
        {
            "relationship": _preview(source_user_id="bound"),
            "workbench": {
                "queues": ["all", "bound"],
                "identity_status": "bound_account_exists",
                "ad_status": "enabled",
            },
        },
        {
            "relationship": _preview(source_user_id="create"),
            "workbench": {
                "queues": ["all", "creatable", "pending", "unbound"],
                "identity_status": "creatable",
                "ad_status": "missing",
            },
        },
    ]

    counts = summarize_identity_workbench_rows(rows)
    filtered = filter_identity_workbench_rows(
        rows,
        queue="creatable",
        identity_status="creatable",
        ad_status="missing",
    )

    assert counts["all"] == 2
    assert counts["bound"] == 1
    assert counts["pending"] == 1
    assert counts["creatable"] == 1
    assert [row["relationship"].source_user_id for row in filtered] == ["create"]
