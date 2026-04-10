from __future__ import annotations

from typing import Any, Callable

from sync_app.clients.wecom import WeComAPI
from sync_app.core.models import AppConfig, DepartmentNode, SourceConnectorConfig, SourceDirectoryUser
from sync_app.providers.source.dingtalk import DingTalkSourceProvider
from sync_app.providers.source.base import (
    SourceDirectoryProvider,
    get_source_provider_schema,
    normalize_source_provider,
)


class WeComSourceProvider(SourceDirectoryProvider):
    provider_id = "wecom"
    display_name = "WeCom"

    def __init__(
        self,
        corpid: str,
        corpsecret: str,
        agentid: str | None = None,
        *,
        logger=None,
        api_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.corpid = corpid
        self.corpsecret = corpsecret
        self.agentid = agentid
        self._api_factory = api_factory or WeComAPI
        try:
            self._api = self._api_factory(corpid, corpsecret, agentid, logger=logger)
        except TypeError:
            self._api = self._api_factory(corpid, corpsecret, agentid)

    def list_departments(self) -> list[DepartmentNode]:
        return [DepartmentNode.from_source_payload(item) for item in self._api.get_department_list()]

    def list_department_users(self, department_id: int) -> list[SourceDirectoryUser]:
        return [
            SourceDirectoryUser.from_source_payload(item)
            for item in self._api.get_department_users(int(department_id))
        ]

    def get_user_detail(self, user_id: str) -> dict[str, Any]:
        return dict(self._api.get_user_detail(user_id) or {})

    def update_user(self, user_id: str, updates: dict[str, Any]) -> bool:
        return bool(self._api.update_user(user_id, updates))

    def list_tag_records(self) -> list[dict[str, Any]]:
        return [dict(item) for item in self._api.get_tag_list()]

    def get_tag_users(self, tag_id: str | int) -> dict[str, Any]:
        return dict(self._api.get_tag_users(tag_id) or {})

    def list_external_group_chats(self, *, status_filter: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        return [
            dict(item)
            for item in self._api.list_external_group_chats(status_filter=status_filter, limit=limit)
        ]

    def get_external_group_chat(self, chat_id: str) -> dict[str, Any]:
        return dict(self._api.get_external_group_chat(chat_id) or {})

    def close(self) -> None:
        close_method = getattr(self._api, "close", None)
        if callable(close_method):
            close_method()


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
