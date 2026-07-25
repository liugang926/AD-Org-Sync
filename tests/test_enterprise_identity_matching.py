from __future__ import annotations

import pytest

from sync_app.core.models import (
    ADAccountRecord,
    IdentityAccountLinkRecord,
    IdentityMatchRuleRecord,
    PlatformAccountRecord,
)
from sync_app.services.enterprise_identity_matching import (
    RESULT_AUTOMATIC,
    RESULT_BLOCKED,
    RESULT_MANUAL,
    RESULT_SUGGESTED,
    EnterpriseIdentityDecisionService,
    EnterpriseIdentityMatchingService,
    assess_identity_matches,
    identity_candidate_requires_decision,
)
from sync_app.services.directory_snapshot_ingestion import (
    ADDirectorySnapshotService,
    PlatformAccountIngestionService,
)
from sync_app.services.identity_canonicalization import IdentityCanonicalizationService
from sync_app.storage.local_db import (
    ADAccountRepository,
    ADDirectorySnapshotRepository,
    DatabaseManager,
    EnterpriseIdentityRepository,
    FieldAuthorityRuleRepository,
    IdentityMatchRuleRepository,
    IdentityMatchDecisionRepository,
    IdentityMatchRunRepository,
    PlatformAccountRepository,
    SourceDirectoryRepository,
    SourceConnectorRepository,
)


def _rules() -> list[IdentityMatchRuleRecord]:
    return [
        IdentityMatchRuleRecord(
            id=1,
            rule_order=10,
            rule_name="employee_id_to_employee_id",
            source_field="employee_id",
            ad_field="employee_id",
            allow_auto_link=True,
            confidence_level="certain",
            confidence_score=100,
        ),
        IdentityMatchRuleRecord(
            id=2,
            rule_order=20,
            rule_name="email_to_mail",
            source_field="email",
            ad_field="mail",
            lowercase_email=True,
            allow_auto_link=False,
            confidence_level="high",
            confidence_score=80,
        ),
        IdentityMatchRuleRecord(
            id=3,
            rule_order=30,
            rule_name="mobile_to_mobile",
            source_field="mobile",
            ad_field="mobile",
            strip_phone_country_code=True,
            allow_auto_link=False,
            confidence_level="medium",
            confidence_score=75,
        ),
    ]


def _source(
    account_id: int,
    stable_id: str,
    *,
    provider: str = "dingtalk",
    employee_id: str = "E001",
    email: str = "",
    mobile: str = "",
    display_name: str = "Alice",
    account_status: str = "active",
    account_type: str = "person",
    is_excluded: bool = False,
) -> PlatformAccountRecord:
    return PlatformAccountRecord(
        id=account_id,
        org_id="acme",
        provider_id=provider,
        connector_id="ad-main",
        platform_account_id=stable_id,
        display_name=display_name,
        employee_id=employee_id,
        email=email,
        mobile=mobile,
        account_status=account_status,
        account_type=account_type,
        is_excluded=is_excluded,
    )


def _ad(
    account_id: int,
    username: str,
    *,
    employee_id: str = "E001",
    mail: str = "",
    mobile: str = "",
    enabled: bool = True,
    account_type: str = "person",
    protected: bool = False,
) -> ADAccountRecord:
    return ADAccountRecord(
        id=account_id,
        org_id="acme",
        connector_id="ad-main",
        object_guid=f"guid-{account_id}",
        sam_account_name=username,
        employee_id=employee_id,
        mail=mail,
        mobile=mobile,
        display_name="Alice",
        account_enabled=enabled,
        account_type=account_type,
        is_protected=protected,
    )


def test_unique_employee_id_is_the_only_default_automatic_match() -> None:
    result = assess_identity_matches(
        [_source(1, "ding-1")],
        [_ad(10, "alice")],
        _rules(),
    )

    assert result[0].result_level == RESULT_AUTOMATIC
    assert result[0].ad_account_id == 10
    assert result[0].confidence == 100
    assert result[0].recommended_action == "establish_permanent_link"


def test_duplicate_employee_id_on_source_blocks_automatic_link() -> None:
    result = assess_identity_matches(
        [_source(1, "ding-1"), _source(2, "ding-2")],
        [_ad(10, "alice")],
        _rules(),
    )

    assert {item.result_level for item in result} == {RESULT_BLOCKED}
    assert all("employee_id" in item.conflict_fields for item in result)
    assert all(
        item.conflict_fields["employee_id"]["reason"] == "duplicate_source_value"
        for item in result
    )


def test_duplicate_ad_employee_id_blocks_automatic_link() -> None:
    result = assess_identity_matches(
        [_source(1, "ding-1")],
        [_ad(10, "alice"), _ad(11, "alice2")],
        _rules(),
    )

    assert result[0].result_level == RESULT_BLOCKED
    assert result[0].conflict_fields["employee_id"]["reason"] == "duplicate_ad_value"


def test_same_employee_on_dingtalk_and_feishu_can_resolve_to_same_ad_account() -> None:
    sources = [
        _source(1, "ding-1", provider="dingtalk"),
        _source(2, "fei-1", provider="feishu"),
    ]

    result = assess_identity_matches(sources, [_ad(10, "alice")], _rules())

    assert [item.result_level for item in result] == [RESULT_AUTOMATIC, RESULT_AUTOMATIC]
    assert {item.ad_account_id for item in result} == {10}


def test_multi_platform_field_conflict_requires_manual_confirmation() -> None:
    sources = [
        _source(1, "ding-1", provider="dingtalk", email="alice@acme.example"),
        _source(2, "fei-1", provider="feishu", email="alice.other@acme.example"),
    ]

    result = assess_identity_matches(sources, [_ad(10, "alice")], _rules())

    assert {item.result_level for item in result} == {RESULT_MANUAL}
    assert all(
        item.conflict_fields["email"]["reason"] == "multi_platform_field_conflict"
        for item in result
    )


def test_ad_account_already_linked_to_another_identity_is_blocked() -> None:
    links = [
        IdentityAccountLinkRecord(
            id=1,
            org_id="acme",
            identity_id="eid-other",
            account_kind="ad",
            ad_account_id=10,
            account_role="primary_ad",
            association_type="manual",
            status="active",
        )
    ]

    result = assess_identity_matches(
        [_source(1, "ding-1")], [_ad(10, "alice")], _rules(), links
    )

    assert result[0].result_level == RESULT_BLOCKED
    assert result[0].conflict_fields["ad_ownership"]["identity_id"] == "eid-other"


def test_second_platform_account_can_merge_into_corroborated_existing_identity() -> None:
    links = [
        IdentityAccountLinkRecord(
            id=1,
            org_id="acme",
            identity_id="eid-alice",
            account_kind="platform",
            platform_account_id=2,
            association_type="manual",
            status="active",
        ),
        IdentityAccountLinkRecord(
            id=2,
            org_id="acme",
            identity_id="eid-alice",
            account_kind="ad",
            ad_account_id=10,
            account_role="primary_ad",
            association_type="manual",
            status="active",
        ),
    ]
    sources = [
        _source(1, "ding-1", provider="dingtalk"),
        _source(2, "fei-1", provider="feishu"),
    ]

    result = assess_identity_matches(sources, [_ad(10, "alice")], _rules(), links)

    new_account_result = result[0]
    assert new_account_result.result_level == RESULT_AUTOMATIC
    assert new_account_result.proposed_identity_id == "eid-alice"
    assert (
        new_account_result.recommended_action
        == "merge_platform_account_into_existing_identity"
    )


def test_name_only_never_creates_a_match() -> None:
    result = assess_identity_matches(
        [_source(1, "ding-1", employee_id="", display_name="Same Name")],
        [_ad(10, "someone", employee_id="")],
        _rules(),
    )

    assert result[0].result_level == RESULT_MANUAL
    assert result[0].ad_account_id is None
    assert result[0].recommended_action == "create_new_ad_account"
    assert not identity_candidate_requires_decision(
        result[0].to_candidate(run_id="run-1", org_id="acme")
    )


def test_ambiguous_or_suggested_identity_candidates_still_block_rollout() -> None:
    assert identity_candidate_requires_decision(
        {"status": "pending", "result_level": RESULT_BLOCKED}
    )
    assert identity_candidate_requires_decision(
        {"status": "pending", "result_level": RESULT_SUGGESTED}
    )
    assert identity_candidate_requires_decision(
        {
            "status": "pending",
            "result_level": RESULT_MANUAL,
            "recommended_action": "confirm_target_manually",
        }
    )


def test_unique_email_is_suggested_and_never_default_auto_linked() -> None:
    result = assess_identity_matches(
        [_source(1, "ding-1", employee_id="", email="Alice@Acme.Example")],
        [_ad(10, "alice", employee_id="", mail="alice@acme.example")],
        _rules(),
    )

    assert result[0].result_level == RESULT_SUGGESTED
    assert result[0].ad_account_id == 10
    assert result[0].confidence == 80


def test_permanent_manual_link_has_precedence_over_generated_rules() -> None:
    links = [
        IdentityAccountLinkRecord(
            id=1,
            org_id="acme",
            identity_id="eid-alice",
            account_kind="platform",
            platform_account_id=1,
            association_type="manual",
            source="admin-confirmed",
            status="active",
        ),
        IdentityAccountLinkRecord(
            id=2,
            org_id="acme",
            identity_id="eid-alice",
            account_kind="ad",
            ad_account_id=11,
            account_role="primary_ad",
            association_type="manual",
            source="admin-confirmed",
            status="active",
        ),
    ]

    result = assess_identity_matches(
        [_source(1, "ding-1")],
        [_ad(10, "rule-match"), _ad(11, "manual-target", employee_id="OTHER")],
        _rules(),
        links,
    )

    assert result[0].result_level == RESULT_AUTOMATIC
    assert result[0].ad_account_id == 11
    assert "permanent_link" in result[0].matched_fields


@pytest.mark.parametrize("account_type", ["service", "shared", "test"])
def test_non_person_accounts_require_explicit_maintenance(account_type: str) -> None:
    result = assess_identity_matches(
        [_source(1, "special-1", account_type=account_type)],
        [_ad(10, "alice")],
        _rules(),
    )

    assert result[0].result_level == RESULT_BLOCKED
    assert result[0].recommended_action == "maintain_explicitly_or_exclude"


def test_repository_enforces_stable_ids_and_exclusive_account_ownership(tmp_path) -> None:
    db = DatabaseManager(db_path=str(tmp_path / "identity.db"))
    db.initialize(create_startup_snapshot=False)
    platforms = PlatformAccountRepository(db)
    directory = ADAccountRepository(db)
    identities = EnterpriseIdentityRepository(db)

    source = platforms.upsert_account(
        org_id="acme",
        provider_id="dingtalk",
        connector_id="ad-main",
        platform_account_id="immutable-ding-id",
        display_name="Alice",
        employee_id="E001",
    )
    source_again = platforms.upsert_account(
        org_id="acme",
        provider_id="dingtalk",
        connector_id="ad-main",
        platform_account_id="immutable-ding-id",
        display_name="Alice Updated",
        employee_id="E001",
    )
    target = directory.upsert_account(
        org_id="acme",
        connector_id="ad-main",
        object_guid="guid-alice",
        object_sid="sid-alice",
        sam_account_name="alice",
        employee_id="E001",
    )
    identity = identities.create_identity(
        org_id="acme",
        display_name="Alice",
        canonical_employee_id="E001",
        created_by="reviewer",
    )
    other_identity = identities.create_identity(
        org_id="acme",
        display_name="Other",
        canonical_employee_id="E002",
        created_by="reviewer",
    )

    assert source.id == source_again.id
    assert source_again.display_name == "Alice Updated"
    platform_link = identities.link_account(
        org_id="acme",
        identity_id=identity.identity_id,
        account_kind="platform",
        platform_account_id=source.id,
        association_type="manual",
        source="reviewer",
        evidence={"employee_id": "E001"},
        confidence=100,
        created_by="reviewer",
    )
    ad_link = identities.link_account(
        org_id="acme",
        identity_id=identity.identity_id,
        account_kind="ad",
        ad_account_id=target.id,
        account_role="primary_ad",
        association_type="manual",
        source="reviewer",
        evidence={"object_guid": "guid-alice"},
        confidence=100,
        created_by="reviewer",
    )

    assert platform_link.identity_id == identity.identity_id
    assert ad_link.identity_id == identity.identity_id
    with pytest.raises(ValueError, match="already linked"):
        identities.link_account(
            org_id="acme",
            identity_id=other_identity.identity_id,
            account_kind="ad",
            ad_account_id=target.id,
            account_role="primary_ad",
            association_type="manual",
        )


def test_persisted_match_run_keeps_explainable_candidate_evidence(tmp_path) -> None:
    db = DatabaseManager(db_path=str(tmp_path / "match-run.db"))
    db.initialize(create_startup_snapshot=False)
    platforms = PlatformAccountRepository(db)
    directory = ADAccountRepository(db)
    identities = EnterpriseIdentityRepository(db)
    rules = IdentityMatchRuleRepository(db)
    runs = IdentityMatchRunRepository(db)
    platforms.upsert_account(
        org_id="acme",
        provider_id="feishu",
        connector_id="ad-main",
        platform_account_id="fei-alice",
        display_name="Alice",
        employee_id="E001",
    )
    directory.upsert_account(
        org_id="acme",
        connector_id="ad-main",
        object_guid="guid-alice",
        sam_account_name="alice",
        employee_id="E001",
        account_enabled=True,
    )
    service = EnterpriseIdentityMatchingService(
        platform_account_repo=platforms,
        ad_account_repo=directory,
        identity_repo=identities,
        rule_repo=rules,
        run_repo=runs,
    )

    result = service.run(org_id="acme", created_by="reviewer")

    assert result["summary"]["automatic_link"] == 1
    assert result["summary"]["blocked"] == 0
    assert result["candidates"][0]["result_level"] == RESULT_AUTOMATIC
    assert result["candidates"][0]["candidate_fingerprint"]
    assert result["candidates"][0]["evidence"]["stable_target_id"] == "guid-alice"


def _decision_services(tmp_path, *, with_ad: bool = True):
    db = DatabaseManager(db_path=str(tmp_path / "decisions.db"))
    db.initialize(create_startup_snapshot=False)
    platforms = PlatformAccountRepository(db)
    directory = ADAccountRepository(db)
    identities = EnterpriseIdentityRepository(db)
    rules = IdentityMatchRuleRepository(db)
    runs = IdentityMatchRunRepository(db)
    decisions = IdentityMatchDecisionRepository(db)
    platforms.upsert_account(
        org_id="acme",
        provider_id="dingtalk",
        connector_id="ad-main",
        platform_account_id="ding-alice",
        display_name="Alice",
        employee_id="E001",
        email="alice@acme.example",
    )
    if with_ad:
        directory.upsert_account(
            org_id="acme",
            connector_id="ad-main",
            object_guid="guid-alice",
            sam_account_name="alice",
            employee_id="E001",
            account_enabled=True,
        )
    matching = EnterpriseIdentityMatchingService(
        platform_account_repo=platforms,
        ad_account_repo=directory,
        identity_repo=identities,
        rule_repo=rules,
        run_repo=runs,
    )
    decision_service = EnterpriseIdentityDecisionService(
        platform_account_repo=platforms,
        ad_account_repo=directory,
        identity_repo=identities,
        rule_repo=rules,
        decision_repo=decisions,
    )
    return db, platforms, directory, identities, decisions, matching, decision_service


def test_permanent_decision_is_atomic_and_request_id_is_idempotent(tmp_path) -> None:
    _, _, _, identities, decisions, matching, decision_service = _decision_services(
        tmp_path
    )
    candidate = matching.run(org_id="acme", created_by="reviewer")["candidates"][0]

    first = decision_service.decide(
        org_id="acme",
        candidate_id=candidate["id"],
        candidate_fingerprint=candidate["candidate_fingerprint"],
        decision="link_permanently",
        request_id="decision-request-1",
        decided_by="reviewer",
        reason="Employee ID verified",
    )
    replay = decision_service.decide(
        org_id="acme",
        candidate_id=candidate["id"],
        candidate_fingerprint=candidate["candidate_fingerprint"],
        decision="link_permanently",
        request_id="decision-request-1",
        decided_by="reviewer",
        reason="Employee ID verified",
    )

    assert first["id"] == replay["id"]
    assert first["resulting_identity_id"]
    assert len(first["resulting_link_ids"]) == 2
    assert len(
        identities.list_links(
            org_id="acme", identity_id=first["resulting_identity_id"]
        )
    ) == 2
    assert len(decisions.list_decisions(org_id="acme")) == 1


def test_decision_rejects_stale_ad_evidence(tmp_path) -> None:
    _, _, directory, _, _, matching, decision_service = _decision_services(tmp_path)
    candidate = matching.run(org_id="acme", created_by="reviewer")["candidates"][0]
    directory.upsert_account(
        org_id="acme",
        connector_id="ad-main",
        object_guid="guid-alice",
        sam_account_name="alice",
        employee_id="E999",
        account_enabled=False,
    )

    with pytest.raises(ValueError, match="stale"):
        decision_service.decide(
            org_id="acme",
            candidate_id=candidate["id"],
            candidate_fingerprint=candidate["candidate_fingerprint"],
            decision="link_permanently",
            request_id="decision-request-stale",
            decided_by="reviewer",
        )


def test_create_new_account_decision_creates_identity_without_fake_ad_link(tmp_path) -> None:
    _, _, _, identities, _, matching, decision_service = _decision_services(
        tmp_path, with_ad=False
    )
    candidate = matching.run(org_id="acme", created_by="reviewer")["candidates"][0]

    result = decision_service.decide(
        org_id="acme",
        candidate_id=candidate["id"],
        candidate_fingerprint=candidate["candidate_fingerprint"],
        decision="create_new_ad_account",
        request_id="decision-request-create",
        decided_by="reviewer",
    )

    links = identities.list_links(
        org_id="acme", identity_id=result["resulting_identity_id"]
    )
    assert [link.account_kind for link in links] == ["platform"]
    assert decisions_status(identities.db, candidate["id"]) == "create_requested"


def decisions_status(db, candidate_id: int) -> str:
    with db.connection() as conn:
        row = conn.execute(
            "SELECT status FROM identity_match_candidates WHERE id = ?", (candidate_id,)
        ).fetchone()
    return str(row["status"])


def test_link_once_records_run_scoped_decision_without_permanent_links(tmp_path) -> None:
    _, _, _, identities, _, matching, decision_service = _decision_services(tmp_path)
    candidate = matching.run(org_id="acme", created_by="reviewer")["candidates"][0]

    result = decision_service.decide(
        org_id="acme",
        candidate_id=candidate["id"],
        candidate_fingerprint=candidate["candidate_fingerprint"],
        decision="link_once",
        request_id="decision-request-once",
        decided_by="reviewer",
    )

    assert result["resulting_link_ids"] == []
    assert identities.list_links(org_id="acme") == []


def test_exclude_decision_marks_stable_platform_account_excluded(tmp_path) -> None:
    _, platforms, _, _, _, matching, decision_service = _decision_services(tmp_path)
    candidate = matching.run(org_id="acme", created_by="reviewer")["candidates"][0]

    decision_service.decide(
        org_id="acme",
        candidate_id=candidate["id"],
        candidate_fingerprint=candidate["candidate_fingerprint"],
        decision="exclude",
        request_id="decision-request-exclude",
        decided_by="reviewer",
        reason="Shared mailbox represented as a person upstream",
    )

    account = platforms.get_account(candidate["platform_account_id"], org_id="acme")
    assert account is not None
    assert account.is_excluded is True


def test_source_snapshot_ingestion_preserves_platform_stable_id_and_account_type(
    tmp_path,
) -> None:
    db = DatabaseManager(db_path=str(tmp_path / "source-ingestion.db"))
    db.initialize(create_startup_snapshot=False)
    source_repo = SourceDirectoryRepository(db)
    platform_repo = PlatformAccountRepository(db)
    snapshot_id = source_repo.start_refresh(
        org_id="acme",
        provider_id="dingtalk",
        connector_id="ding-main",
        created_by="connector-admin",
    )
    source_repo.replace_snapshot(
        snapshot_id,
        departments=[
            {
                "source_department_id": "D1",
                "name": "Engineering",
                "parent_department_id": "",
                "path_names": ["Engineering"],
                "path_ids": ["D1"],
            }
        ],
        users=[
            {
                "source_user_id": "immutable-user-id",
                "display_name": "Build Bot",
                "employee_id": "",
                "email": "bot@acme.example",
                "department_ids": ["D1"],
                "department_names": ["Engineering"],
                "primary_department_id": "D1",
                "account_status": "active",
                "is_active": True,
                "raw_payload": {
                    "mobile": "+8613800000000",
                    "account_type": "service",
                },
            }
        ],
        fields=[],
        fingerprint="source-fingerprint",
    )
    service = PlatformAccountIngestionService(
        source_directory_repo=source_repo,
        platform_account_repo=platform_repo,
    )

    summary = service.ingest_snapshot(
        org_id="acme",
        provider_id="dingtalk",
        connector_id="ding-main",
        snapshot_id=snapshot_id,
    )
    account = platform_repo.get_account_by_stable_id(
        org_id="acme",
        provider_id="dingtalk",
        connector_id="ding-main",
        platform_account_id="immutable-user-id",
    )

    assert summary["account_count"] == 1
    assert summary["missing_employee_id_count"] == 1
    assert account is not None
    assert account.platform_account_id == "immutable-user-id"
    assert account.account_type == "service"
    assert account.mobile == "+8613800000000"
    assert account.primary_department_id == "D1"


class _SnapshotTargetProvider:
    def list_directory_users(self, *, search_base: str = "", page_size: int = 500):
        assert page_size == 500
        return [
            {
                "object_guid": "guid-alice",
                "object_sid": "sid-alice",
                "distinguished_name": "CN=Alice,OU=Engineering,DC=acme,DC=example",
                "sam_account_name": "alice",
                "user_principal_name": "alice@acme.example",
                "employee_id": "E001",
                "employee_number": "1001",
                "mail": "alice@acme.example",
                "telephone_number": "1001",
                "mobile": "13800000001",
                "display_name": "Alice",
                "account_enabled": True,
                "manager_dn": "CN=Manager,OU=Engineering,DC=acme,DC=example",
                "group_membership": ["CN=Employees,OU=Groups,DC=acme,DC=example"],
                "ou_path": "Engineering",
                "extension_attributes": {"extensionAttribute1": "CN"},
                "when_created": "2026-01-01T00:00:00Z",
                "when_changed": "2026-07-21T00:00:00Z",
                "account_type": "person",
                "is_protected": False,
            },
            {
                "object_guid": "guid-alice-duplicate",
                "object_sid": "sid-alice-duplicate",
                "distinguished_name": "CN=Alice2,OU=Engineering,DC=acme,DC=example",
                "sam_account_name": "alice2",
                "employee_id": "E001",
                "employee_number": "1002",
                "display_name": "Alice Duplicate",
                "account_enabled": True,
                "group_membership": [],
                "extension_attributes": {},
            },
        ]

    def list_organizational_units(self):
        return [
            {
                "guid": "ou-guid-engineering",
                "dn": "OU=Engineering,DC=acme,DC=example",
                "name": "Engineering",
                "parent_dn": "DC=acme,DC=example",
                "path": ["Engineering"],
                "when_created": "2025-01-01T00:00:00Z",
                "when_changed": "2026-07-01T00:00:00Z",
            }
        ]


def test_ad_snapshot_persists_stable_attributes_and_duplicate_quality_counts(
    tmp_path,
) -> None:
    db = DatabaseManager(db_path=str(tmp_path / "ad-snapshot.db"))
    db.initialize(create_startup_snapshot=False)
    snapshots = ADDirectorySnapshotRepository(db)
    directory = ADAccountRepository(db)
    service = ADDirectorySnapshotService(
        snapshot_repo=snapshots,
        ad_account_repo=directory,
    )

    result = service.refresh(
        org_id="acme",
        connector_id="ad-main",
        provider=_SnapshotTargetProvider(),
        created_by="ad-admin",
        capability_report={"read_users": {"status": "success"}},
    )
    accounts = directory.list_accounts(org_id="acme", connector_id="ad-main")

    assert result["status"] == "success"
    assert result["user_count"] == 2
    assert result["ou_count"] == 1
    assert result["duplicate_employee_id_count"] == 2
    assert result["duplicate_employee_number_count"] == 0
    assert result["snapshot_fingerprint"]
    assert accounts[0].object_guid == "guid-alice"
    assert accounts[0].object_sid == "sid-alice"
    assert accounts[0].extension_attributes["extensionAttribute1"] == "CN"
    with db.connection() as connection:
        ou_count = connection.execute(
            "SELECT COUNT(*) FROM ad_ou_snapshots WHERE snapshot_id = ?",
            (result["id"],),
        ).fetchone()[0]
    assert ou_count == 1


def test_ad_snapshot_failure_is_recorded_without_exposing_provider_secret(tmp_path) -> None:
    class FailingProvider:
        def list_directory_users(self, *, search_base: str = "", page_size: int = 500):
            raise RuntimeError("bind failed with password SuperSecret!")

    db = DatabaseManager(db_path=str(tmp_path / "ad-snapshot-failure.db"))
    db.initialize(create_startup_snapshot=False)
    snapshots = ADDirectorySnapshotRepository(db)
    service = ADDirectorySnapshotService(
        snapshot_repo=snapshots,
        ad_account_repo=ADAccountRepository(db),
    )

    with pytest.raises(RuntimeError, match="SuperSecret"):
        service.refresh(
            org_id="acme",
            connector_id="ad-main",
            provider=FailingProvider(),
            created_by="ad-admin",
        )

    with db.connection() as connection:
        failed = connection.execute(
            "SELECT status, error_summary FROM ad_directory_snapshots"
        ).fetchone()
    assert failed["status"] == "failed"
    assert "SuperSecret" not in failed["error_summary"]


def test_field_authority_preview_exposes_conflicts_and_requires_fresh_confirmation(
    tmp_path,
) -> None:
    db = DatabaseManager(db_path=str(tmp_path / "field-authority.db"))
    db.initialize(create_startup_snapshot=False)
    platforms = PlatformAccountRepository(db)
    identities = EnterpriseIdentityRepository(db)
    authority = FieldAuthorityRuleRepository(db)
    dingtalk = platforms.upsert_account(
        org_id="acme",
        provider_id="dingtalk",
        connector_id="ad-main",
        platform_account_id="ding-alice",
        display_name="Alice Ding",
        employee_id="E001",
        email="alice.ding@acme.example",
        mobile="13800000001",
        primary_department_id="D-DING",
    )
    wecom = platforms.upsert_account(
        org_id="acme",
        provider_id="wecom",
        connector_id="ad-main",
        platform_account_id="wecom-alice",
        display_name="Alice WeCom",
        employee_id="E001",
        email="alice.wecom@acme.example",
        mobile="13800000002",
        primary_department_id="D-WECOM",
    )
    identity = identities.create_identity(
        org_id="acme",
        display_name="Alice",
        canonical_employee_id="E001",
        created_by="reviewer",
    )
    for account in (dingtalk, wecom):
        identities.link_account(
            org_id="acme",
            identity_id=identity.identity_id,
            account_kind="platform",
            platform_account_id=account.id,
            association_type="manual",
            source="reviewer",
            confidence=100,
        )
    service = IdentityCanonicalizationService(
        identity_repo=identities,
        platform_account_repo=platforms,
        field_authority_repo=authority,
    )

    preview = service.preview(org_id="acme", identity_id=identity.identity_id)

    assert preview.requires_confirmation
    assert preview.selected_fields["employee_id"] == "E001"
    assert preview.selected_fields["display_name"] == "Alice Ding"
    assert preview.selected_fields["mobile"] == "13800000002"
    assert preview.field_sources["mobile"]["provider_id"] == "wecom"
    assert preview.conflicts["email"]["selected_provider"] == "dingtalk"
    assert preview.conflicts["primary_department_id"]["selected_provider"] == "dingtalk"
    with pytest.raises(ValueError, match="require confirmation"):
        service.apply_preview(
            org_id="acme",
            identity_id=identity.identity_id,
            expected_preview_fingerprint=preview.preview_fingerprint,
        )
    updated = service.apply_preview(
        org_id="acme",
        identity_id=identity.identity_id,
        expected_preview_fingerprint=preview.preview_fingerprint,
        confirm_conflicts=True,
    )
    assert updated.identity_revision == 2
    assert updated.field_sources["mobile"]["provider_id"] == "wecom"
    with pytest.raises(ValueError, match="stale"):
        service.apply_preview(
            org_id="acme",
            identity_id=identity.identity_id,
            expected_preview_fingerprint=preview.preview_fingerprint,
            confirm_conflicts=True,
        )


def test_multiple_source_connector_records_encrypt_secrets_and_keep_status_metadata(
    tmp_path,
) -> None:
    db = DatabaseManager(db_path=str(tmp_path / "source-connectors.db"))
    db.initialize(create_startup_snapshot=False)
    connectors = SourceConnectorRepository(db)
    connectors.upsert_connector(
        org_id="acme",
        connector_id="dingtalk-main",
        provider_id="dingtalk",
        name="DingTalk",
        corpid="ding-app-key",
        agentid="agent-1",
        corpsecret="TopSecretDing",
    )
    connectors.upsert_connector(
        org_id="acme",
        connector_id="feishu-main",
        provider_id="feishu",
        name="Feishu",
        corpid="fei-app-id",
        corpsecret="TopSecretFeishu",
    )
    connectors.update_connection_status(
        org_id="acme",
        connector_id="dingtalk-main",
        connection_status="connected",
        granted_permissions=["contact.departments.read", "contact.users.read"],
        authorization_scope={"root_department_ids": [1]},
    )
    connectors.update_snapshot_stats(
        org_id="acme",
        connector_id="dingtalk-main",
        department_count=5,
        account_count=42,
        quality_issue_count=2,
    )

    records = connectors.list_connectors(org_id="acme")
    revealed = connectors.get_connector(
        "dingtalk-main",
        org_id="acme",
        reveal_secret=True,
    )
    with db.connection() as connection:
        raw_secret = connection.execute(
            "SELECT corpsecret FROM source_connectors WHERE org_id = 'acme' AND connector_id = 'dingtalk-main'"
        ).fetchone()[0]

    assert {record.provider_id for record in records} == {"dingtalk", "feishu"}
    assert all(record.corpsecret == "" for record in records)
    assert raw_secret != "TopSecretDing"
    assert revealed is not None
    assert revealed.corpsecret == "TopSecretDing"
    assert revealed.connection_status == "connected"
    assert revealed.authorization_scope == {"root_department_ids": [1]}
    assert revealed.department_count == 5
    assert revealed.account_count == 42
    assert revealed.quality_issue_count == 2
