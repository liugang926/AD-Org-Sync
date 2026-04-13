from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

DEFAULT_TARGET_PROVIDER = "ad_ldaps"


def normalize_target_provider(value: str | None, *, default: str = DEFAULT_TARGET_PROVIDER) -> str:
    normalized = str(value or "").strip().lower()
    return normalized or default


class TargetDirectoryProvider(ABC):
    provider_id = DEFAULT_TARGET_PROVIDER
    display_name = "Target Directory Provider"

    @property
    @abstractmethod
    def base_dn(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_ou_dn(self, ou_path: list[str]) -> str:
        raise NotImplementedError

    @abstractmethod
    def list_organizational_units(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def ou_exists(self, ou_dn: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_users_batch(self, usernames: list[str]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_all_enabled_users(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_user_details(self, username: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def search_users(self, query: str, *, limit: int = 20) -> list[Any]:
        raise NotImplementedError

    @abstractmethod
    def find_parent_groups_for_member(self, member_dn: str) -> list[Any]:
        raise NotImplementedError

    @abstractmethod
    def inspect_department_group(
        self,
        *,
        department_id: int,
        ou_name: str,
        ou_dn: str,
        full_path: list[str],
        display_separator: str = "-",
    ) -> Any:
        raise NotImplementedError

    @abstractmethod
    def ensure_ou(self, ou_name: str, parent_dn: str) -> tuple[bool, str, bool]:
        raise NotImplementedError

    @abstractmethod
    def ensure_department_group(
        self,
        *,
        department_id: int,
        parent_department_id: int | None = None,
        ou_name: str,
        ou_dn: str,
        full_path: list[str],
        display_separator: str = "-",
        binding_repo: Any = None,
    ) -> Any:
        raise NotImplementedError

    @abstractmethod
    def ensure_custom_group(
        self,
        *,
        source_type: str,
        source_key: str,
        display_name: str,
        connector_id: str = "default",
    ) -> Any:
        raise NotImplementedError

    @abstractmethod
    def create_user(
        self,
        username: str,
        display_name: str,
        email: str,
        ou_dn: str,
        *,
        extra_attributes: dict[str, Any] | None = None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def update_user(
        self,
        username: str,
        display_name: str,
        email: str,
        ou_dn: str,
        *,
        extra_attributes: dict[str, Any] | None = None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def reactivate_user(
        self,
        username: str,
        display_name: str,
        email: str,
        ou_dn: str,
        *,
        extra_attributes: dict[str, Any] | None = None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def add_group_to_group(self, child_group_dn: str, parent_group_dn: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def add_user_to_group(self, username: str, group_name: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def remove_group_from_group(self, child_group_dn: str, parent_group_dn: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def disable_user(self, username: str) -> bool:
        raise NotImplementedError

    def close(self) -> None:
        return None
