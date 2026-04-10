from __future__ import annotations

from typing import Any, Callable

from sync_app.clients.dingtalk import DingTalkAPI
from sync_app.core.models import DepartmentNode, SourceDirectoryUser
from sync_app.providers.source.base import SourceDirectoryProvider


class DingTalkSourceProvider(SourceDirectoryProvider):
    provider_id = "dingtalk"
    display_name = "DingTalk"

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        agentid: str | None = None,
        *,
        logger=None,
        api_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.app_key = app_key
        self.app_secret = app_secret
        self.agentid = agentid
        self._api_factory = api_factory or DingTalkAPI
        try:
            self._api = self._api_factory(app_key, app_secret, agentid, logger=logger)
        except TypeError:
            self._api = self._api_factory(app_key, app_secret, agentid)

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

    def close(self) -> None:
        close_method = getattr(self._api, "close", None)
        if callable(close_method):
            close_method()
