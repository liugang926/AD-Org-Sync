from __future__ import annotations

import unittest

from sync_app.core.models import DepartmentNode, SourceConnectorConfig, SourceDirectoryUser
from sync_app.core.observability import METRICS
from sync_app.providers.source import (
    SourceDirectoryProvider,
    build_source_provider,
    register_source_provider,
    unregister_source_provider,
)
from sync_app.providers.target import (
    ADLDAPSTargetProvider,
    build_target_provider,
    register_target_provider,
    unregister_target_provider,
)


class _ContractSourceProvider(SourceDirectoryProvider):
    provider_id = "contract_source"
    display_name = "Contract Source"

    def __init__(self, *_args, **_kwargs) -> None:
        self.closed = False

    def list_departments(self) -> list[DepartmentNode]:
        return [
            DepartmentNode(department_id=1, name="Healthy", parent_id=0),
            DepartmentNode(department_id=2, name="Unavailable", parent_id=0),
        ]

    def list_department_users(self, department_id: int) -> list[SourceDirectoryUser]:
        if department_id == 2:
            raise TimeoutError("injected provider timeout")
        return [
            SourceDirectoryUser(
                userid="alice",
                name="Alice",
                email="alice@example.com",
                departments=[1],
            )
        ]

    def get_user_detail(self, user_id: str) -> dict[str, object]:
        return {"userid": user_id}

    def close(self) -> None:
        self.closed = True


class _ContractTargetProvider(ADLDAPSTargetProvider):
    provider_id = "contract_target"


class _TargetClient:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs


class ProviderContractTests(unittest.TestCase):
    def tearDown(self) -> None:
        unregister_source_provider("contract_source")
        unregister_target_provider("contract_target")

    def test_runtime_source_registry_accepts_contract_implementations(self) -> None:
        register_source_provider("contract_source", _ContractSourceProvider)
        provider = build_source_provider(
            provider_type="contract_source",
            source_connector_config=SourceConnectorConfig(corpid="id", corpsecret="secret"),
        )

        self.assertIsInstance(provider, _ContractSourceProvider)
        self.assertEqual(provider.get_user_detail("alice"), {"userid": "alice"})

    def test_source_search_isolates_department_fault_and_records_metric(self) -> None:
        METRICS.reset()
        provider = _ContractSourceProvider()
        users = provider.search_users("alice")

        self.assertEqual([user.userid for user in users], ["alice"])
        counters = METRICS.snapshot()["counters"]
        failure_metric = next(
            item
            for item in counters
            if item["name"] == "ad_org_sync_source_provider_department_failures_total"
        )
        self.assertEqual(failure_metric["labels"], {"provider": "contract_source"})
        self.assertEqual(failure_metric["value"], 1)

    def test_internal_factory_type_error_is_not_misclassified_as_legacy_signature(self) -> None:
        calls = 0

        def failing_factory(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            raise TypeError("injected credential decoding failure")

        with self.assertRaisesRegex(TypeError, "credential decoding failure"):
            build_source_provider(
                provider_type="wecom",
                source_connector_config=SourceConnectorConfig(corpid="id", corpsecret="secret"),
                api_factory=failing_factory,
            )
        self.assertEqual(calls, 1)

    def test_source_registry_rejects_non_contract_factory_result(self) -> None:
        register_source_provider("contract_source", lambda *_args, **_kwargs: object())
        with self.assertRaisesRegex(TypeError, "SourceDirectoryProvider"):
            build_source_provider(
                provider_type="contract_source",
                source_connector_config=SourceConnectorConfig(corpid="id", corpsecret="secret"),
            )

    def test_runtime_target_registry_accepts_contract_implementations(self) -> None:
        register_target_provider("contract_target", _ContractTargetProvider)
        provider = build_target_provider(
            provider_type="contract_target",
            client_factory=_TargetClient,
            server="directory.example.com",
        )

        self.assertIsInstance(provider, _ContractTargetProvider)
        self.assertEqual(provider.client.kwargs["server"], "directory.example.com")


if __name__ == "__main__":
    unittest.main()
