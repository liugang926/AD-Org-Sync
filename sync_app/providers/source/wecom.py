from __future__ import annotations

from typing import Any, Callable

from sync_app.clients.wecom import WeComAPI
from sync_app.core.models import DepartmentNode, SourceDirectoryUser
from sync_app.providers.source.base import SourceDirectoryProvider


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

    def verify_employee_identity(self, request: Any) -> dict[str, Any] | None:
        oauth_code = str(getattr(request, "verification_code", "") or "").strip()
        if not oauth_code:
            return None
        get_oauth_user_info = getattr(self._api, "get_oauth_user_info", None)
        if not callable(get_oauth_user_info):
            raise NotImplementedError("WeCom API client does not support SSPR employee verification")
        payload = dict(get_oauth_user_info(oauth_code) or {})
        source_user_id = _extract_wecom_user_id(payload)
        if not source_user_id:
            return None
        return {
            "org_id": str(getattr(request, "org_id", "") or "default"),
            "source_user_id": source_user_id,
            "provider_id": self.provider_id,
            "display_name": str(payload.get("name") or payload.get("Name") or source_user_id),
            "raw_claims": payload,
        }

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


def _extract_wecom_user_id(payload: dict[str, Any]) -> str:
    for key in ("UserId", "userid", "userId", "user_id", "open_userid", "OpenUserId"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""
