from __future__ import annotations

from typing import Any, Callable

from sync_app.clients.dingtalk import DingTalkAPI
from sync_app.core.models import DepartmentNode, SourceDirectoryUser
from sync_app.providers.source.base import SourceDirectoryProvider, instantiate_source_api_client


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
        self._api = instantiate_source_api_client(
            self._api_factory,
            app_key,
            app_secret,
            agentid,
            logger=logger,
        )

    def list_departments(self) -> list[DepartmentNode]:
        return [DepartmentNode.from_source_payload(item) for item in self._api.get_department_list()]

    def list_department_users(self, department_id: int) -> list[SourceDirectoryUser]:
        return [
            self.normalize_user(item)
            for item in self._api.get_department_users(int(department_id))
        ]

    def get_user_detail(self, user_id: str) -> dict[str, Any]:
        payload = dict(self._api.get_user_detail(user_id) or {})
        if payload:
            payload = self.normalize_user(payload).to_state_payload()
        return payload

    def update_user(self, user_id: str, updates: dict[str, Any]) -> bool:
        return bool(self._api.update_user(user_id, updates))

    def verify_employee_identity(self, request: Any) -> dict[str, Any]:
        """Resolve identity exclusively from DingTalk's one-time authorization code."""

        payload = dict(self._api.exchange_employee_auth_code(request.verification_code) or {})
        return {
            "org_id": str(request.org_id or "").strip().lower() or "default",
            "provider_id": self.provider_id,
            "connector_id": str(request.connector_id or "").strip(),
            "source_user_id": str(payload.get("userid") or "").strip(),
            "display_name": str(payload.get("name") or "").strip(),
            "sys_level": int(payload.get("sys_level") or 0),
        }

    def close(self) -> None:
        close_method = getattr(self._api, "close", None)
        if callable(close_method):
            close_method()
