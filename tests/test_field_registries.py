from __future__ import annotations

from sync_app.core.models import CanonicalUserDTO, SourceDirectoryUser
from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories import (
    ADTargetAttributeRegistryRepository,
    CanonicalFieldRegistryRepository,
    SourceDirectoryRepository,
    SourceFieldRegistryRepository,
)


def test_canonical_user_dto_normalizes_provider_specific_identity_fields() -> None:
    user = CanonicalUserDTO.from_source_payload(
        {
            "user_id": "feishu-user-1",
            "open_id": "ou-1",
            "union_id": "on-1",
            "employee_no": "E001",
            "enterprise_email": "person@example.com",
            "job_title": "Engineer",
            "leader_user_id": "manager-1",
            "work_station": "Shanghai",
        }
    )

    assert isinstance(user, SourceDirectoryUser)
    assert user.source_user_id == "feishu-user-1"
    assert user.platform_open_id == "ou-1"
    assert user.platform_union_id == "on-1"
    assert user.employee_id == "E001"
    assert user.enterprise_email == "person@example.com"
    assert user.job_title == "Engineer"
    assert user.manager_account_id == "manager-1"
    assert user.work_station == "Shanghai"


def test_source_registry_is_snapshot_bound_dynamic_and_secret_safe(tmp_path) -> None:
    db = DatabaseManager(db_path=str(tmp_path / "registry.db"))
    db.initialize(create_startup_snapshot=False)
    source_repo = SourceDirectoryRepository(db)
    snapshot_id = source_repo.start_refresh(
        org_id="org-1",
        provider_id="wecom",
        connector_id="wecom-main",
    )
    source_repo.replace_snapshot(
        snapshot_id,
        departments=[],
        users=[{"source_user_id": "u1", "employee_id": "E1"}],
        fields=[
            {
                "name": "extattr.cost_center",
                "label": "Cost center",
                "data_type": "string",
                "coverage": 1,
                "samples": ["CC-100"],
                "is_custom": True,
            },
            {
                "name": "department_ids",
                "data_type": "array",
                "is_multi_value": True,
                "coverage": 1,
                "samples": ["10"],
            },
            {
                "name": "mobile",
                "data_type": "string",
                "coverage": 1,
                "samples": ["13800138000"],
            },
            {
                "name": "access_token",
                "data_type": "string",
                "coverage": 1,
                "samples": ["must-not-be-stored"],
            },
        ],
        fingerprint="snapshot-1",
    )

    registry = SourceFieldRegistryRepository(db)
    fields = registry.list_fields(
        org_id="org-1",
        provider_id="wecom",
        source_connector_id="wecom-main",
        current_snapshot_id=snapshot_id,
    )
    by_path = {item.raw_field_path: item for item in fields}
    assert set(by_path) == {"department_ids", "extattr.cost_center", "mobile"}
    assert by_path["department_ids"].is_multi_value is True
    assert by_path["department_ids"].canonical_field_key == "department_ids"
    assert by_path["mobile"].is_sensitive is True
    assert by_path["mobile"].masked_sample_values == ["13***00"]
    assert "13800138000" not in str(by_path["mobile"].to_dict())
    assert by_path["extattr.cost_center"].coverage_rate == 1.0

    with db.connection() as conn:
        legacy_names = {
            row[0]
            for row in conn.execute(
                "SELECT field_name FROM source_field_catalogs WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchall()
        }
    assert "access_token" not in legacy_names


def test_source_registry_tracks_not_returned_and_type_conflict(tmp_path) -> None:
    db = DatabaseManager(db_path=str(tmp_path / "registry-state.db"))
    db.initialize(create_startup_snapshot=False)
    registry = SourceFieldRegistryRepository(db)
    registry.sync_snapshot_catalog(
        org_id="org-1",
        provider_id="dingtalk",
        source_connector_id="main",
        snapshot_id=1,
        user_count=2,
        fields=[{"name": "custom.grade", "data_type": "string", "coverage": 2}],
    )
    registry.sync_snapshot_catalog(
        org_id="org-1",
        provider_id="dingtalk",
        source_connector_id="main",
        snapshot_id=2,
        user_count=2,
        fields=[{"name": "custom.grade", "data_type": "array", "coverage": 1}],
    )
    grade = registry.list_fields(org_id="org-1")[0]
    assert grade.availability_status == "type_conflict"
    assert grade.schema_version == 2
    assert grade.latest_snapshot_id == 2

    registry.sync_snapshot_catalog(
        org_id="org-1",
        provider_id="dingtalk",
        source_connector_id="main",
        snapshot_id=3,
        user_count=2,
        fields=[],
    )
    grade = registry.list_fields(org_id="org-1")[0]
    assert grade.availability_status == "not_returned"
    assert grade.latest_snapshot_id == 2


def test_canonical_registry_seeds_extensible_fields_and_governs_custom_namespace(tmp_path) -> None:
    db = DatabaseManager(db_path=str(tmp_path / "canonical.db"))
    db.initialize(create_startup_snapshot=False)
    registry = CanonicalFieldRegistryRepository(db)
    registry.seed_defaults()
    builtins = {item.canonical_field_key: item for item in registry.list_fields(org_id="org-1")}
    assert "employee_id" in builtins
    assert "manager_source_user_id" in builtins
    assert builtins["manager_source_user_id"].allowed_mapping_roles == [
        "RELATIONSHIP",
        "READ_ONLY_REFERENCE",
    ]

    custom = registry.register_custom_field(
        org_id="org-1",
        canonical_field_key="custom.hr.cost_center",
        display_label="Cost center",
    )
    assert custom.is_custom is True
    assert custom.category == "custom"

    try:
        registry.register_custom_field(
            org_id="org-1",
            canonical_field_key="cost_center",
            display_label="Invalid",
        )
    except ValueError as exc:
        assert "custom.<namespace>.<field_key>" in str(exc)
    else:
        raise AssertionError("ungoverned custom key was accepted")


def test_ad_target_registry_fails_closed_and_marks_special_handlers(tmp_path) -> None:
    db = DatabaseManager(db_path=str(tmp_path / "ad-targets.db"))
    db.initialize(create_startup_snapshot=False)
    registry = ADTargetAttributeRegistryRepository(db)
    registry.sync_snapshot_catalog(
        org_id="org-1",
        ad_connector_id="ad-main",
        snapshot_id=10,
        capability_report={
            "schema_attributes": [
                "objectGUID",
                "sAMAccountName",
                "displayName",
                "manager",
                "proxyAddresses",
                "extensionAttribute1",
            ],
            "capabilities": {
                "update_user": {"status": "not_tested", "verified": False}
            },
        },
    )
    blocked = {
        item.ldap_attribute: item
        for item in registry.list_attributes(
            org_id="org-1", ad_connector_id="ad-main", current_snapshot_id=10
        )
    }
    assert blocked["objectGUID"].is_read_only is True
    assert blocked["objectGUID"].is_writable is False
    assert blocked["displayName"].capability_status == "unavailable_by_permission"
    assert blocked["extensionAttribute2"].capability_status == "not_detected"
    assert blocked["manager"].special_handler_type == "manager_dn"
    assert blocked["proxyAddresses"].is_multi_value is True
    assert blocked["proxyAddresses"].special_handler_type == "proxy_addresses"

    registry.sync_snapshot_catalog(
        org_id="org-1",
        ad_connector_id="ad-main",
        snapshot_id=11,
        capability_report={
            "schema_attributes": ["displayName", "manager", "proxyAddresses"],
            "capabilities": {
                "update_user": {"status": "success", "verified": True}
            },
        },
    )
    writable = {
        item.ldap_attribute: item
        for item in registry.list_attributes(
            org_id="org-1", ad_connector_id="ad-main", current_snapshot_id=11
        )
    }
    assert writable["displayName"].is_writable is True
    assert writable["objectGUID"].is_writable is False
