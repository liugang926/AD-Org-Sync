from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from sync_app.core.models import SourceDirectoryUser
from sync_app.core.sync_policies import (
    MappingEvaluationError,
    apply_transform_pipeline,
    build_proxy_address_values,
    build_source_to_ad_mapping_payload,
    merge_proxy_addresses,
)
from sync_app.infra.ldap_compat import MODIFY_REPLACE
from sync_app.services.ad_sync import ADSyncLDAPS
from sync_app.storage.local_db import AttributeMappingRuleRepository, DatabaseManager


def _capability(
    name: str,
    *,
    writable: bool = True,
    multi: bool = False,
    special_handler_type: str = "",
) -> SimpleNamespace:
    return SimpleNamespace(
        ldap_attribute=name,
        schema_detected=True,
        is_writable=writable,
        is_read_only=not writable,
        is_multi_value=multi,
        requires_special_handler=bool(special_handler_type),
        special_handler_type=special_handler_type,
    )


def _rule(source: str, target: str, **overrides: object) -> SimpleNamespace:
    values = {
        "connector_id": "default",
        "ad_connector_id": "default",
        "provider_scope": "*",
        "source_field": source,
        "canonical_source_field": source,
        "raw_source_field_path": source,
        "target_field": target,
        "mapping_role": "ATTRIBUTE_SYNC",
        "transform_template": "",
        "transform_pipeline": [],
        "null_policy": "PRESERVE_TARGET",
        "conflict_policy": "REJECT_ON_CONFLICT",
        "write_policy": "REPLACE",
        "sync_mode": "replace",
        "version": 1,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _user() -> SourceDirectoryUser:
    return SourceDirectoryUser.from_source_payload(
        {
            "provider_id": "dingtalk",
            "userid": "alice",
            "name": " Alice Zhang ",
            "email": " Alice@Example.COM ",
            "mobile": "+86 138-0000-0000",
            "title": "Engineer",
            "cost_center": "CC-42",
            "aliases": "alice@example.com,a.zhang@example.com",
        }
    )


def test_governed_mapping_repository_versions_and_rejects_scripts(tmp_path) -> None:
    db = DatabaseManager(db_path=str(tmp_path / "mapping.db"))
    db.initialize(create_startup_snapshot=False)
    repo = AttributeMappingRuleRepository(db)
    repo.upsert_rule(
        org_id="acme",
        direction="source_to_ad",
        source_field="display_name",
        target_field="displayName",
        provider_scope="dingtalk",
        mapping_role="ATTRIBUTE_SYNC",
        transform_pipeline=[{"op": "trim"}],
        null_policy="PRESERVE_TARGET",
        conflict_policy="REJECT_ON_CONFLICT",
        write_policy="REPLACE",
        created_by="admin",
    )
    first = repo.list_rule_records(org_id="acme")[0]
    assert first.version == 1
    assert first.provider_scope == "dingtalk"
    assert first.transform_pipeline == [{"op": "trim"}]

    repo.upsert_rule(
        org_id="acme",
        direction="source_to_ad",
        source_field="display_name",
        target_field="displayName",
        transform_pipeline=[{"op": "normalize_whitespace"}],
    )
    assert repo.list_rule_records(org_id="acme")[0].version == 2

    with pytest.raises(ValueError, match="unsafe transform"):
        repo.upsert_rule(
            org_id="acme",
            direction="source_to_ad",
            source_field="display_name",
            target_field="displayName",
            transform_pipeline=[{"op": "python", "script": "import os"}],
        )


def test_common_and_custom_attributes_use_capability_checked_pipeline() -> None:
    user = _user()
    capabilities = {
        name: _capability(name)
        for name in ("displayName", "mail", "mobile", "title", "extensionAttribute1")
    }
    rules = [
        _rule("display_name", "displayName", transform_pipeline=[{"op": "trim"}]),
        _rule("email", "mail", transform_pipeline=[{"op": "normalize_email"}]),
        _rule("mobile", "mobile", transform_pipeline=[{"op": "normalize_mobile"}]),
        _rule("job_title", "title"),
        _rule("cost_center", "extensionAttribute1"),
    ]
    mapped = build_source_to_ad_mapping_payload(
        user,
        connector_id="default",
        ad_username="alice",
        email=user.email,
        target_department=None,
        rules=rules,
        attribute_capabilities=capabilities,
        strict_capabilities=True,
    )
    assert mapped["displayName"]["value"] == "Alice Zhang"
    assert mapped["mail"]["value"] == "alice@example.com"
    assert mapped["mobile"]["value"] == "13800000000"
    assert mapped["title"]["value"] == "Engineer"
    assert mapped["extensionAttribute1"]["value"] == "CC-42"


def test_null_cardinality_and_forbidden_target_policies_fail_closed() -> None:
    user = _user()
    writable_scalar = {"description": _capability("description")}
    assert build_source_to_ad_mapping_payload(
        user,
        connector_id="default",
        ad_username="alice",
        email=user.email,
        target_department=None,
        rules=[_rule("missing", "description")],
        attribute_capabilities=writable_scalar,
        strict_capabilities=True,
    ) == {}

    cleared = build_source_to_ad_mapping_payload(
        user,
        connector_id="default",
        ad_username="alice",
        email=user.email,
        target_department=None,
        rules=[_rule("missing", "description", null_policy="CLEAR")],
        attribute_capabilities=writable_scalar,
        strict_capabilities=True,
    )
    assert cleared["description"]["clear"] is True

    with pytest.raises(MappingEvaluationError, match="empty source value"):
        build_source_to_ad_mapping_payload(
            user,
            connector_id="default",
            ad_username="alice",
            email=user.email,
            target_department=None,
            rules=[_rule("missing", "description", null_policy="BLOCK")],
            attribute_capabilities=writable_scalar,
            strict_capabilities=True,
        )

    with pytest.raises(MappingEvaluationError, match="dedicated handler"):
        build_source_to_ad_mapping_payload(
            user,
            connector_id="default",
            ad_username="alice",
            email=user.email,
            target_department=None,
            rules=[_rule("manager_account_id", "manager")],
        )

    proxy_capability = {"proxyAddresses": _capability("proxyAddresses", multi=True)}
    with pytest.raises(MappingEvaluationError, match="dedicated handler"):
        build_source_to_ad_mapping_payload(
            user,
            connector_id="default",
            ad_username="alice",
            email=user.email,
            target_department=None,
            rules=[_rule("aliases", "proxyAddresses")],
            attribute_capabilities=proxy_capability,
            strict_capabilities=True,
        )

    multi_capability = {"otherTelephone": _capability("otherTelephone", multi=True)}
    with pytest.raises(MappingEvaluationError, match="explicit split"):
        build_source_to_ad_mapping_payload(
            user,
            connector_id="default",
            ad_username="alice",
            email=user.email,
            target_department=None,
            rules=[_rule("email", "otherTelephone")],
            attribute_capabilities=multi_capability,
            strict_capabilities=True,
        )


def test_declarative_transform_types_and_ad_clear_change() -> None:
    assert apply_transform_pipeline(
        " A, B ",
        [
            {"op": "split", "separator": ",", "output_type": "array"},
            {"op": "join", "separator": ";", "input_type": "array"},
        ],
    ) == "A;B"
    with pytest.raises(MappingEvaluationError, match="array input"):
        apply_transform_pipeline("not-an-array", [{"op": "join"}])

    ad_sync = object.__new__(ADSyncLDAPS)
    ad_sync.domain = "example.com"
    ad_sync.get_user_attribute_values = lambda _username, _attributes: {
        "description": "old"
    }
    changes = ad_sync._build_user_attribute_changes(
        "alice",
        display_name="Alice",
        email="alice@example.com",
        extra_attributes={
            "description": {"value": None, "mode": "clear", "clear": True}
        },
    )
    assert "description" in changes


def test_proxy_addresses_use_dedicated_primary_and_alias_merge_handler() -> None:
    user = _user()
    capability = {
        "proxyAddresses": _capability(
            "proxyAddresses",
            multi=True,
            special_handler_type="proxy_addresses",
        )
    }
    values = build_proxy_address_values(
        user,
        connector_id="default",
        primary_email=user.email,
        rules=[
            _rule(
                "aliases",
                "proxyAddresses",
                mapping_role="RELATIONSHIP",
                transform_pipeline=[{"op": "split", "separator": ","}],
            )
        ],
        attribute_capabilities=capability,
        strict_capabilities=True,
    )
    assert values == ("alice@example.com", ["a.zhang@example.com"])

    merged = merge_proxy_addresses(
        [
            "SMTP:old-primary@example.com",
            "smtp:legacy@example.com",
            "X500:/o=Example/ou=Legacy/cn=Alice",
        ],
        primary_address=values[0],
        aliases=values[1],
    )
    assert merged == [
        "SMTP:alice@example.com",
        "smtp:old-primary@example.com",
        "smtp:legacy@example.com",
        "X500:/o=Example/ou=Legacy/cn=Alice",
        "smtp:a.zhang@example.com",
    ]

    client = object.__new__(ADSyncLDAPS)
    client.logger = Mock()
    client.connection = SimpleNamespace(modify=Mock(return_value=True), result={})
    client.get_user = Mock(
        return_value={"dn": "CN=Alice,OU=People,DC=example,DC=com"}
    )
    client.get_user_attribute_values = Mock(
        return_value={"proxyAddresses": ["SMTP:old-primary@example.com"]}
    )
    client._is_protected_account = Mock(return_value=False)
    assert client.update_proxy_addresses("alice", values[0], values[1])
    client.connection.modify.assert_called_once_with(
        "CN=Alice,OU=People,DC=example,DC=com",
        {
            "proxyAddresses": [
                (
                    MODIFY_REPLACE,
                    [
                        "SMTP:alice@example.com",
                        "smtp:old-primary@example.com",
                        "smtp:a.zhang@example.com",
                    ],
                )
            ]
        },
    )
