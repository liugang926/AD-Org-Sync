from __future__ import annotations

from typing import Any, Callable

from sync_app.providers.target.ad_ldaps import ADLDAPSTargetProvider
from sync_app.providers.target.base import (
    TargetDirectoryProvider,
    normalize_target_provider,
    validate_target_provider_contract,
)
from sync_app.services.ad_sync import ADSyncLDAPS


TARGET_PROVIDER_FACTORIES: dict[str, Callable[[Any], TargetDirectoryProvider]] = {
    ADLDAPSTargetProvider.provider_id: ADLDAPSTargetProvider,
}
_BUILTIN_TARGET_PROVIDERS = frozenset(TARGET_PROVIDER_FACTORIES)


def register_target_provider(
    provider_id: str,
    factory: Callable[[Any], TargetDirectoryProvider],
    *,
    replace: bool = False,
) -> None:
    normalized_provider = normalize_target_provider(provider_id, default="")
    if not normalized_provider:
        raise ValueError("target provider ID is required")
    if not callable(factory):
        raise TypeError("target provider factory must be callable")
    if normalized_provider in TARGET_PROVIDER_FACTORIES and not replace:
        raise ValueError(f"target provider already registered: {normalized_provider}")
    TARGET_PROVIDER_FACTORIES[normalized_provider] = factory


def unregister_target_provider(provider_id: str) -> None:
    normalized_provider = normalize_target_provider(provider_id, default="")
    if normalized_provider in _BUILTIN_TARGET_PROVIDERS:
        raise ValueError(f"built-in target provider cannot be unregistered: {normalized_provider}")
    TARGET_PROVIDER_FACTORIES.pop(normalized_provider, None)


def build_target_provider(
    *,
    provider_type: str | None = None,
    client_factory: Callable[..., ADSyncLDAPS] | None = None,
    **kwargs: Any,
) -> TargetDirectoryProvider:
    normalized_provider = normalize_target_provider(provider_type)
    provider_factory = TARGET_PROVIDER_FACTORIES.get(normalized_provider)
    if provider_factory is None:
        raise ValueError(f"unsupported target provider: {normalized_provider}")
    factory = client_factory or ADSyncLDAPS
    return validate_target_provider_contract(provider_factory(factory(**kwargs)))
