from __future__ import annotations

from typing import Any, Callable

from sync_app.providers.target.ad_ldaps import ADLDAPSTargetProvider
from sync_app.providers.target.base import TargetDirectoryProvider, normalize_target_provider
from sync_app.services.ad_sync import ADSyncLDAPS


def build_target_provider(
    *,
    provider_type: str | None = None,
    client_factory: Callable[..., ADSyncLDAPS] | None = None,
    **kwargs: Any,
) -> TargetDirectoryProvider:
    normalized_provider = normalize_target_provider(provider_type)
    if normalized_provider != ADLDAPSTargetProvider.provider_id:
        raise ValueError(f"unsupported target provider: {normalized_provider}")
    factory = client_factory or ADSyncLDAPS
    return ADLDAPSTargetProvider(factory(**kwargs))
