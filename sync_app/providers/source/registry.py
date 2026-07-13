from __future__ import annotations

from typing import Any, Callable

from sync_app.core.models import AppConfig, SourceConnectorConfig
from sync_app.providers.source.base import (
    SourceDirectoryProvider,
    get_source_provider_schema,
    normalize_source_provider,
    validate_source_provider_contract,
)
from sync_app.providers.source.dingtalk import DingTalkSourceProvider
from sync_app.providers.source.feishu import FeishuSourceProvider
from sync_app.providers.source.wecom import WeComSourceProvider


SOURCE_PROVIDER_FACTORIES: dict[str, Callable[..., SourceDirectoryProvider]] = {
    "wecom": WeComSourceProvider,
    "dingtalk": DingTalkSourceProvider,
    "feishu": FeishuSourceProvider,
}
_BUILTIN_SOURCE_PROVIDERS = frozenset(SOURCE_PROVIDER_FACTORIES)


def register_source_provider(
    provider_id: str,
    factory: Callable[..., SourceDirectoryProvider],
    *,
    replace: bool = False,
) -> None:
    normalized_provider = normalize_source_provider(provider_id, default="")
    if not normalized_provider:
        raise ValueError("source provider ID is required")
    if not callable(factory):
        raise TypeError("source provider factory must be callable")
    if normalized_provider in SOURCE_PROVIDER_FACTORIES and not replace:
        raise ValueError(f"source provider already registered: {normalized_provider}")
    SOURCE_PROVIDER_FACTORIES[normalized_provider] = factory


def unregister_source_provider(provider_id: str) -> None:
    normalized_provider = normalize_source_provider(provider_id, default="")
    if normalized_provider in _BUILTIN_SOURCE_PROVIDERS:
        raise ValueError(f"built-in source provider cannot be unregistered: {normalized_provider}")
    SOURCE_PROVIDER_FACTORIES.pop(normalized_provider, None)


def build_source_provider(
    *,
    app_config: AppConfig | None = None,
    source_connector_config: SourceConnectorConfig | None = None,
    wecom_config: SourceConnectorConfig | None = None,
    provider_type: str | None = None,
    logger=None,
    api_factory: Callable[..., Any] | None = None,
) -> SourceDirectoryProvider:
    resolved_provider = provider_type
    if resolved_provider is None and app_config is not None:
        resolved_provider = getattr(app_config, "source_provider", None)
    normalized_provider = normalize_source_provider(resolved_provider)
    config = source_connector_config or wecom_config or (app_config.source_connector if app_config else None)
    if config is None:
        raise ValueError("source_connector_config, wecom_config, or app_config is required to build a source provider")

    provider_factory = SOURCE_PROVIDER_FACTORIES.get(normalized_provider)
    provider_schema = get_source_provider_schema(normalized_provider)
    if provider_factory is None and provider_schema.provider_id == normalized_provider and not provider_schema.implemented:
        raise ValueError(
            provider_schema.implementation_status
            or f"source provider '{provider_schema.display_name}' is not implemented in this build"
        )
    if provider_factory is None:
        raise ValueError(f"unsupported source provider: {normalized_provider}")
    provider = provider_factory(
        config.corpid,
        config.corpsecret,
        config.agentid,
        logger=logger,
        api_factory=api_factory,
    )
    return validate_source_provider_contract(provider)

