from __future__ import annotations

from typing import Any

from sync_app.providers.target.base import TargetDirectoryProvider
from sync_app.services.ad_sync import ADSyncLDAPS


class ADLDAPSTargetProvider(TargetDirectoryProvider):
    provider_id = "ad_ldaps"
    display_name = "AD / LDAPS"

    def __init__(self, client: ADSyncLDAPS) -> None:
        self.client = client

    @property
    def base_dn(self) -> str:
        return self.client.base_dn

    def __getattr__(self, item: str):
        return getattr(self.client, item)

    def get_ou_dn(self, ou_path: list[str]) -> str:
        return self.client.get_ou_dn(ou_path)

    def list_organizational_units(self) -> list[dict[str, Any]]:
        return self.client.list_organizational_units()

    def ou_exists(self, ou_dn: str) -> bool:
        return bool(self.client.ou_exists(ou_dn))

    def get_users_batch(self, usernames: list[str]):
        return self.client.get_users_batch(usernames)

    def get_all_enabled_users(self) -> list[str]:
        return self.client.get_all_enabled_users()

    def get_user_details(self, username: str) -> dict[str, Any]:
        return self.client.get_user_details(username)

    def search_users(self, query: str, *, limit: int = 20):
        return self.client.search_users(query, limit=limit)

    def find_parent_groups_for_member(self, member_dn: str):
        return self.client.find_parent_groups_for_member(member_dn)

    def inspect_department_group(
        self,
        *,
        department_id: int,
        ou_name: str,
        ou_dn: str,
        full_path: list[str],
        display_separator: str = "-",
    ):
        return self.client.inspect_department_group(
            department_id=department_id,
            ou_name=ou_name,
            ou_dn=ou_dn,
            full_path=full_path,
            display_separator=display_separator,
        )

    def ensure_ou(self, ou_name: str, parent_dn: str) -> tuple[bool, str, bool]:
        return self.client.ensure_ou(ou_name, parent_dn)

    def ensure_department_group(
        self,
        *,
        department_id: int,
        parent_department_id: int | None = None,
        ou_name: str,
        ou_dn: str,
        full_path: list[str],
        display_separator: str = "-",
        binding_repo=None,
    ):
        return self.client.ensure_department_group(
            department_id=department_id,
            parent_department_id=parent_department_id,
            ou_name=ou_name,
            ou_dn=ou_dn,
            full_path=full_path,
            display_separator=display_separator,
            binding_repo=binding_repo,
        )

    def ensure_custom_group(
        self,
        *,
        source_type: str,
        source_key: str,
        display_name: str,
        ou_path: list[str] | None = None,
        connector_id: str = "default",
    ):
        return self.client.ensure_custom_group(
            source_type=source_type,
            source_key=source_key,
            display_name=display_name,
            ou_path=ou_path,
        )

    def create_user(
        self,
        username: str,
        display_name: str,
        email: str,
        ou_dn: str,
        *,
        extra_attributes: dict[str, Any] | None = None,
    ) -> bool:
        return bool(
            self.client.create_user(
                username,
                display_name,
                email,
                ou_dn,
                extra_attributes=extra_attributes,
            )
        )

    def update_user(
        self,
        username: str,
        display_name: str,
        email: str,
        ou_dn: str,
        *,
        extra_attributes: dict[str, Any] | None = None,
    ) -> bool:
        return bool(
            self.client.update_user(
                username,
                display_name,
                email,
                ou_dn,
                extra_attributes=extra_attributes,
            )
        )

    def reactivate_user(
        self,
        username: str,
        display_name: str,
        email: str,
        ou_dn: str,
        *,
        extra_attributes: dict[str, Any] | None = None,
    ) -> bool:
        return bool(
            self.client.reactivate_user(
                username,
                display_name,
                email,
                ou_dn,
                extra_attributes=extra_attributes,
            )
        )

    def add_group_to_group(self, child_group_dn: str, parent_group_dn: str) -> bool:
        return bool(self.client.add_group_to_group(child_group_dn, parent_group_dn))

    def add_user_to_group(self, username: str, group_name: str) -> bool:
        return bool(self.client.add_user_to_group(username, group_name))

    def remove_group_from_group(self, child_group_dn: str, parent_group_dn: str) -> bool:
        return bool(self.client.remove_group_from_group(child_group_dn, parent_group_dn))

    def disable_user(self, username: str) -> bool:
        return bool(self.client.disable_user(username))

    def reset_user_password(
        self,
        username: str,
        new_password: str,
        *,
        force_change_at_next_login: bool = False,
    ) -> bool:
        reset_fn = getattr(self.client, "reset_user_password", None)
        if not callable(reset_fn):
            raise NotImplementedError("AD LDAPS client does not support password reset")
        return bool(
            reset_fn(
                username,
                new_password,
                force_change_at_next_login=force_change_at_next_login,
            )
        )

    def unlock_user(self, username: str) -> bool:
        unlock_fn = getattr(self.client, "unlock_user", None)
        if not callable(unlock_fn):
            raise NotImplementedError("AD LDAPS client does not support account unlock")
        return bool(unlock_fn(username))

    def close(self) -> None:
        close_fn = getattr(self.client, "close", None)
        if callable(close_fn):
            close_fn()
