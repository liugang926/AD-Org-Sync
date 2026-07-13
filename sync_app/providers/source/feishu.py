from __future__ import annotations

from typing import Any, Callable

from sync_app.clients.feishu import FeishuAPI
from sync_app.core.models import DepartmentNode, SourceDirectoryUser
from sync_app.providers.source.base import SourceDirectoryProvider, instantiate_source_api_client


class FeishuSourceProvider(SourceDirectoryProvider):
    provider_id = "feishu"
    display_name = "Feishu"

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        tenant_key: str | None = None,
        *,
        logger=None,
        api_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.app_id = app_id
        self.app_secret = app_secret
        self.tenant_key = tenant_key
        self._api = instantiate_source_api_client(
            api_factory or FeishuAPI,
            app_id,
            app_secret,
            tenant_key,
            logger=logger,
        )

    def list_departments(self) -> list[DepartmentNode]:
        return [DepartmentNode.from_source_payload(item) for item in self._api.get_department_list()]

    def list_department_users(self, department_id: int) -> list[SourceDirectoryUser]:
        return [self.normalize_user(item) for item in self._api.get_department_users(department_id)]

    def get_user_detail(self, user_id: str) -> dict[str, Any]:
        payload = dict(self._api.get_user_detail(user_id) or {})
        return self.normalize_user(payload).to_state_payload() if payload else {}

    def close(self) -> None:
        close_method = getattr(self._api, "close", None)
        if callable(close_method):
            close_method()
