from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from sync_app.core.models import DepartmentNode, SourceDirectoryUser

DEFAULT_SOURCE_PROVIDER = "wecom"
SOURCE_PROVIDER_DISPLAY_NAMES = {
    "wecom": "WeCom",
}


def normalize_source_provider(value: str | None, *, default: str = DEFAULT_SOURCE_PROVIDER) -> str:
    normalized = str(value or "").strip().lower()
    return normalized or default


def get_source_provider_display_name(value: str | None) -> str:
    normalized = normalize_source_provider(value)
    return SOURCE_PROVIDER_DISPLAY_NAMES.get(normalized, normalized or DEFAULT_SOURCE_PROVIDER)


class SourceDirectoryProvider(ABC):
    provider_id = DEFAULT_SOURCE_PROVIDER
    display_name = "WeCom"

    @abstractmethod
    def list_departments(self) -> list[DepartmentNode]:
        raise NotImplementedError

    @abstractmethod
    def list_department_users(self, department_id: int) -> list[SourceDirectoryUser]:
        raise NotImplementedError

    @abstractmethod
    def get_user_detail(self, user_id: str) -> dict[str, Any]:
        raise NotImplementedError

    def update_user(self, user_id: str, updates: dict[str, Any]) -> bool:
        raise NotImplementedError(f"{self.provider_id} does not support user updates")

    def list_tag_records(self) -> list[dict[str, Any]]:
        return []

    def get_tag_users(self, tag_id: str | int) -> dict[str, Any]:
        return {"userlist": []}

    def list_external_group_chats(self, *, status_filter: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        return []

    def get_external_group_chat(self, chat_id: str) -> dict[str, Any]:
        return {}

    def close(self) -> None:
        return None

    def __enter__(self) -> "SourceDirectoryProvider":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
