from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from sync_app.infra.ldap_compat import SIMPLE
from sync_app.services.ad_sync import ADSyncLDAPS


def _client(*, use_ssl: bool, validate_cert: bool) -> ADSyncLDAPS:
    client = ADSyncLDAPS.__new__(ADSyncLDAPS)
    client.server = object()
    client.server_address = "dc.example.test"
    client.port = 636 if use_ssl else 389
    client.domain = "example.test"
    client.username = "EXAMPLE\\administrator"
    client.password = "test-only"
    client.use_ssl = use_ssl
    client.validate_cert = validate_cert
    client.logger = logging.getLogger(__name__)
    return client


def test_strict_ldaps_runtime_falls_back_to_simple_after_any_ntlm_failure() -> None:
    client = _client(use_ssl=True, validate_cert=True)
    simple_connection = Mock()

    with patch(
        "sync_app.services.ad_sync.Connection",
        side_effect=[RuntimeError("session terminated by server"), simple_connection],
    ) as connection:
        client._connect()

    assert client.connection is simple_connection
    assert connection.call_count == 2
    assert connection.call_args_list[1].kwargs["authentication"] == SIMPLE
    assert connection.call_args_list[1].kwargs["user"] == "administrator@example.test"


def test_non_tls_runtime_does_not_expand_simple_fallback_for_unknown_ntlm_failure() -> None:
    client = _client(use_ssl=False, validate_cert=True)

    with patch(
        "sync_app.services.ad_sync.Connection",
        side_effect=RuntimeError("session terminated by server"),
    ) as connection:
        with pytest.raises(RuntimeError, match="session terminated"):
            client._connect()

    assert connection.call_count == 1


def test_unverified_tls_does_not_expand_simple_fallback_for_unknown_ntlm_failure() -> None:
    client = _client(use_ssl=True, validate_cert=False)

    with patch(
        "sync_app.services.ad_sync.Connection",
        side_effect=RuntimeError("session terminated by server"),
    ) as connection:
        with pytest.raises(RuntimeError, match="session terminated"):
            client._connect()

    assert connection.call_count == 1


def test_snapshot_attributes_skip_optional_fields_missing_from_ad_schema() -> None:
    client = ADSyncLDAPS.__new__(ADSyncLDAPS)
    client.server = SimpleNamespace(
        schema=SimpleNamespace(
            attribute_types={
                "objectGUID": object(),
                "distinguishedName": object(),
                "sAMAccountName": object(),
                "employeeID": object(),
            }
        )
    )

    attributes = client._snapshot_search_attributes()

    assert attributes == [
        "objectGUID",
        "distinguishedName",
        "sAMAccountName",
        "employeeID",
    ]
    assert "extensionAttribute1" not in attributes


def test_snapshot_attributes_keep_full_list_when_schema_is_unavailable() -> None:
    client = ADSyncLDAPS.__new__(ADSyncLDAPS)
    client.server = SimpleNamespace(schema=None)

    attributes = client._snapshot_search_attributes()

    assert "objectGUID" in attributes
    assert "extensionAttribute15" in attributes
