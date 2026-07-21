from __future__ import annotations

import pytest

from sync_app.services.account_takeover import AccountTakeoverService
from sync_app.storage.local_db import (
    ADAccountRepository,
    AccountTakeoverRepository,
    DatabaseManager,
    EnterpriseIdentityRepository,
    PlatformAccountRepository,
    UserIdentityBindingRepository,
)


def _context(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "takeover.db"))
    db.initialize(create_startup_snapshot=False)
    platforms = PlatformAccountRepository(db)
    directory = ADAccountRepository(db)
    identities = EnterpriseIdentityRepository(db)
    takeover = AccountTakeoverRepository(db)
    bindings = UserIdentityBindingRepository(db)
    service = AccountTakeoverService(
        takeover_repo=takeover,
        platform_account_repo=platforms,
        ad_account_repo=directory,
        identity_repo=identities,
    )
    return db, platforms, directory, identities, takeover, bindings, service


def _seed_accounts(platforms, directory, *, source_id="ding-alice", ad_name="alice"):
    source = platforms.upsert_account(
        org_id="acme",
        provider_id="dingtalk",
        connector_id="ad-main",
        platform_account_id=source_id,
        display_name="Alice",
        employee_id="E001",
    )
    target = directory.upsert_account(
        org_id="acme",
        connector_id="ad-main",
        object_guid=f"guid-{ad_name}",
        object_sid=f"sid-{ad_name}",
        distinguished_name=f"CN={ad_name},OU=People,DC=acme,DC=example",
        sam_account_name=ad_name,
        employee_id="E001",
        account_enabled=True,
    )
    return source, target


def test_takeover_preview_approval_and_apply_create_permanent_graph(tmp_path) -> None:
    _, platforms, directory, identities, takeover, bindings, service = _context(
        tmp_path
    )
    _seed_accounts(platforms, directory)
    preview = service.preview(
        org_id="acme",
        csv_text=(
            "provider_id,connector_id,platform_account_id,ad_account_key\n"
            "dingtalk,ad-main,ding-alice,guid-alice\n"
        ),
        original_filename="reviewed.csv",
        created_by="submitter",
    )
    batch = preview["batch"]

    assert batch["status"] == "ready"
    assert preview["rows"][0]["proposed_action"] == "create_permanent_link"
    service.approve(
        org_id="acme",
        batch_id=batch["batch_id"],
        preview_fingerprint=batch["preview_fingerprint"],
        approved_by="reviewer",
    )
    applied = service.apply(
        org_id="acme",
        batch_id=batch["batch_id"],
        preview_fingerprint=batch["preview_fingerprint"],
        applied_by="executor",
    )
    replay = service.apply(
        org_id="acme",
        batch_id=batch["batch_id"],
        preview_fingerprint=batch["preview_fingerprint"],
        applied_by="executor",
    )

    assert applied["status"] == replay["status"] == "applied"
    assert len(identities.list_identities(org_id="acme")) == 1
    assert len(identities.list_links(org_id="acme")) == 2
    binding = bindings.get_binding_record_by_source_user_id(
        "ding-alice",
        org_id="acme",
        source_provider="dingtalk",
        connector_id="ad-main",
    )
    assert binding is not None
    assert binding.ad_username == "alice"
    assert binding.target_object_guid == "guid-alice"
    assert takeover.list_rows(batch["batch_id"], org_id="acme")[0]["result_json"] != "{}"


def test_takeover_import_never_silently_overwrites_active_legacy_binding(tmp_path) -> None:
    _, platforms, directory, identities, _, bindings, service = _context(tmp_path)
    _seed_accounts(platforms, directory)
    other_identity = identities.create_identity(
        org_id="acme", display_name="Other", created_by="reviewer"
    )
    other_ad = directory.upsert_account(
        org_id="acme",
        connector_id="ad-main",
        object_guid="guid-other",
        sam_account_name="other",
        employee_id="E999",
    )
    identities.link_account(
        org_id="acme",
        identity_id=other_identity.identity_id,
        account_kind="ad",
        ad_account_id=other_ad.id,
        account_role="primary_ad",
    )
    bindings.upsert_binding(
        "ding-alice",
        "other",
        org_id="acme",
        source_provider="dingtalk",
        connector_id="ad-main",
        source="manual",
    )

    preview = service.preview(
        org_id="acme",
        csv_text=(
            "source_provider,connector_id,source_user_id,ad_username\n"
            "dingtalk,ad-main,ding-alice,alice\n"
        ),
        created_by="submitter",
    )

    assert preview["batch"]["status"] == "conflicts"
    assert preview["batch"]["overwrite_count"] == 1
    assert "existing_legacy_binding_would_be_overwritten" in preview["rows"][0][
        "conflict_codes"
    ]
    with pytest.raises(ValueError, match="conflicts"):
        service.approve(
            org_id="acme",
            batch_id=preview["batch"]["batch_id"],
            preview_fingerprint=preview["batch"]["preview_fingerprint"],
            approved_by="reviewer",
        )
    binding = bindings.get_binding_record_by_source_user_id(
        "ding-alice",
        org_id="acme",
        source_provider="dingtalk",
        connector_id="ad-main",
    )
    assert binding is not None
    assert binding.ad_username == "other"


def test_takeover_detects_one_ad_account_assigned_to_multiple_people(tmp_path) -> None:
    _, platforms, directory, _, _, _, service = _context(tmp_path)
    _seed_accounts(platforms, directory)
    platforms.upsert_account(
        org_id="acme",
        provider_id="dingtalk",
        connector_id="ad-main",
        platform_account_id="ding-bob",
        display_name="Bob",
        employee_id="E002",
    )

    preview = service.preview(
        org_id="acme",
        csv_text=(
            "provider_id,connector_id,platform_account_id,ad_account_key\n"
            "dingtalk,ad-main,ding-alice,guid-alice\n"
            "dingtalk,ad-main,ding-bob,guid-alice\n"
        ),
        created_by="submitter",
    )

    assert preview["batch"]["conflict_count"] == 2
    assert all(
        "one_ad_account_multiple_people" in row["conflict_codes"]
        for row in preview["rows"]
    )


def test_takeover_requires_submitter_approver_executor_separation(tmp_path) -> None:
    _, platforms, directory, _, _, _, service = _context(tmp_path)
    _seed_accounts(platforms, directory)
    preview = service.preview(
        org_id="acme",
        csv_text=(
            "provider_id,connector_id,platform_account_id,ad_account_key\n"
            "dingtalk,ad-main,ding-alice,guid-alice\n"
        ),
        created_by="alice",
    )
    batch = preview["batch"]

    with pytest.raises(ValueError, match="different"):
        service.approve(
            org_id="acme",
            batch_id=batch["batch_id"],
            preview_fingerprint=batch["preview_fingerprint"],
            approved_by="alice",
        )
    service.approve(
        org_id="acme",
        batch_id=batch["batch_id"],
        preview_fingerprint=batch["preview_fingerprint"],
        approved_by="reviewer",
    )
    with pytest.raises(ValueError, match="different"):
        service.apply(
            org_id="acme",
            batch_id=batch["batch_id"],
            preview_fingerprint=batch["preview_fingerprint"],
            applied_by="reviewer",
        )
