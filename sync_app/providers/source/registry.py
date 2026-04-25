from __future__ import annotations

from typing import Any, Callable

from sync_app.core.models import AppConfig, SourceConnectorConfig
from sync_app.providers.source.base import (
    SourceDirectoryProvider,
    get_source_provider_schema,
    normalize_source_provider,
)
from sync_app.providers.source.dingtalk import DingTalkSourceProvider
from sync_app.providers.source.wecom import WeComSourceProvider


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

    provider_schema = get_source_provider_schema(normalized_provider)
    if not provider_schema.implemented:
        raise ValueError(
            provider_schema.implementation_status
            or f"source provider '{provider_schema.display_name}' is not implemented in this build"
        )
    if normalized_provider == "wecom":
        return WeComSourceProvider(
            config.corpid,
            config.corpsecret,
            config.agentid,
            logger=logger,
            api_factory=api_factory,
        )
    if normalized_provider == "dingtalk":
        return DingTalkSourceProvider(
            config.corpid,
            config.corpsecret,
            config.agentid,
            logger=logger,
            api_factory=api_factory,
        )
    raise ValueError(f"unsupported source provider: {normalized_provider}")

