from sync_app.core.models import (
    DepartmentGroupInfo,
    DirectoryGroupRecord,
    DirectoryUserRecord,
)
from sync_app.services import runtime


class FakeWeComAPI:
    def __init__(self, corpid: str, corpsecret: str, agentid: str | None = None):
        self.corpid = corpid
        self.corpsecret = corpsecret
        self.agentid = agentid

    def get_department_list(self):
        return [{"id": 1, "name": "HQ", "parentid": 0}]

    def get_department_users(self, department_id: int):
        if department_id != 1:
            return []
        return [{"userid": "alice", "name": "Alice"}]

    def get_user_detail(self, username: str):
        return {
            "userid": username,
            "name": "Alice",
            "email": "alice@example.com",
            "department": [1],
        }


class FakeADSyncLDAPS:
    last_init_kwargs = None

    def __init__(self, *args, **kwargs):
        type(self).last_init_kwargs = dict(kwargs)
        self.base_dn = "DC=example,DC=com"
        self.user_root_ou_path = str(kwargs.get("user_root_ou_path", "") or "").strip()

    def get_ou_dn(self, path):
        effective_path = [segment for segment in self.user_root_ou_path.replace("\\", "/").split("/") if segment]
        effective_path.extend(path or [])
        if not effective_path:
            return self.base_dn
        return ",".join([f"OU={segment}" for segment in reversed(effective_path)] + [self.base_dn])

    def ou_exists(self, _ou_dn: str) -> bool:
        return False

    def inspect_department_group(self, department_id, ou_name, ou_dn, full_path, display_separator="-"):
        return DepartmentGroupInfo(
            exists=False,
            group_sam=f"WECOM_D{department_id}",
            group_cn=f"{ou_name}__D{department_id}",
            group_dn=f"CN={ou_name}__D{department_id},{ou_dn}",
            display_name=f"{display_separator.join(full_path)} [D{department_id}]",
            description=f"source=wecom; dept_id={department_id}; path={'/'.join(full_path)}",
            binding_source="new",
            created=False,
        )

    def get_users_batch(self, usernames):
        return {}

    def get_all_enabled_users(self):
        return []

    def find_parent_groups_for_member(self, member_dn):
        return []


class FakeWeComConflictAPI(FakeWeComAPI):
    def get_user_detail(self, username: str):
        return {
            "userid": username,
            "name": "Alice",
            "email": "alice.alt@example.com",
            "department": [1],
        }


class FakeADSyncConflict(FakeADSyncLDAPS):
    def get_users_batch(self, usernames):
        result = {}
        for username in usernames:
            if username in {"alice", "alice.alt"}:
                result[username] = DirectoryUserRecord(
                    username=username,
                    dn=f"CN={username},OU=HQ,DC=example,DC=com",
                    display_name=username,
                    email=f"{username}@example.com",
                )
        return result


class FakeADSyncApply(FakeADSyncLDAPS):
    def ensure_ou(self, ou_name: str, parent_dn: str):
        return True, f"OU={ou_name},{parent_dn}", True

    def ensure_department_group(self, department_id, parent_department_id, ou_name, ou_dn, full_path, display_separator="-", binding_repo=None):
        group_info = DepartmentGroupInfo(
            exists=True,
            group_sam=f"WECOM_D{department_id}",
            group_cn=f"{ou_name}__D{department_id}",
            group_dn=f"CN={ou_name}__D{department_id},{ou_dn}",
            display_name=f"{display_separator.join(full_path)} [D{department_id}]",
            description=f"source=wecom; dept_id={department_id}; path={'/'.join(full_path)}",
            binding_source="managed",
            created=True,
        )
        if binding_repo:
            binding_repo.upsert_binding(
                department_id=str(department_id),
                parent_department_id=str(parent_department_id) if parent_department_id else None,
                group_sam=group_info.group_sam,
                group_dn=group_info.group_dn,
                group_cn=group_info.group_cn,
                display_name=group_info.display_name,
                path_text="/".join(full_path),
                status="active",
            )
        return group_info

    def create_user(self, username: str, display_name: str, email: str, ou_dn: str, *, extra_attributes=None) -> bool:
        return True

    def update_user(self, username: str, display_name: str, email: str, ou_dn: str, *, extra_attributes=None) -> bool:
        return True

    def add_user_to_group(self, username: str, group_name: str) -> bool:
        return True

    def disable_user(self, username: str) -> bool:
        return True

    def get_user_details(self, username: str):
        return {
            "SamAccountName": username,
            "DisplayName": username,
            "Mail": f"{username}@example.com",
            "Created": "",
            "Modified": "",
            "LastLogonDate": "",
            "Description": "",
        }

    def get_all_enabled_users(self):
        return ["bob"]


class FakeADSyncProtectedDisable(FakeADSyncLDAPS):
    def get_all_enabled_users(self):
        return ["administrator"]


class FakeADSyncCleanup(FakeADSyncLDAPS):
    def find_parent_groups_for_member(self, member_dn):
        if member_dn and "HQ__D1" in member_dn:
            return [
                DirectoryGroupRecord(
                    dn="CN=LegacyParent,OU=Managed,DC=example,DC=com",
                    cn="LegacyParent",
                    group_sam="legacy_parent",
                    display_name="Legacy Parent",
                )
            ]
        return []


class FakeWeComProgrammableAPI(FakeWeComAPI):
    department_list = [{"id": 1, "name": "HQ", "parentid": 0}]
    department_users = {1: [{"userid": "alice", "name": "Alice"}]}
    user_details = {
        "alice": {
            "userid": "alice",
            "name": "Alice",
            "email": "alice@example.com",
            "department": [1],
        }
    }
    updated_users = []
    tag_list = []
    tag_users = {}
    external_group_chats = {}

    @classmethod
    def reset(cls):
        cls.department_list = [{"id": 1, "name": "HQ", "parentid": 0}]
        cls.department_users = {1: [{"userid": "alice", "name": "Alice"}]}
        cls.user_details = {
            "alice": {
                "userid": "alice",
                "name": "Alice",
                "email": "alice@example.com",
                "department": [1],
            }
        }
        cls.updated_users = []
        cls.tag_list = []
        cls.tag_users = {}
        cls.external_group_chats = {}

    def get_department_list(self):
        return [dict(item) for item in type(self).department_list]

    def get_department_users(self, department_id: int):
        return [dict(item) for item in type(self).department_users.get(department_id, [])]

    def get_user_detail(self, username: str):
        return dict(type(self).user_details.get(username, {}))

    def update_user(self, userid: str, updates: dict):
        type(self).updated_users.append({"userid": userid, "updates": dict(updates or {})})
        return True

    def get_tag_list(self):
        return [dict(item) for item in type(self).tag_list]

    def get_tag_users(self, tag_id):
        return dict(type(self).tag_users.get(str(tag_id), {"userlist": []}))

    def get_external_group_chat(self, chat_id: str):
        return dict(type(self).external_group_chats.get(str(chat_id), {"member_list": []}))


class FakeWeChatBot:
    messages = []

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    @classmethod
    def reset(cls):
        cls.messages = []

    def send_message(self, message: str):
        type(self).messages.append(message)
        return True


class FakeADSyncPolicy(FakeADSyncLDAPS):
    init_kwargs = []
    created_users = []
    updated_users = []
    disabled_users = []
    user_group_memberships = []
    custom_groups = []
    enabled_users_by_domain = {}
    existing_users_by_domain = {}
    user_details_by_username = {}

    @classmethod
    def reset(cls):
        cls.init_kwargs = []
        cls.created_users = []
        cls.updated_users = []
        cls.disabled_users = []
        cls.user_group_memberships = []
        cls.custom_groups = []
        cls.enabled_users_by_domain = {}
        cls.existing_users_by_domain = {}
        cls.user_details_by_username = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain = kwargs.get("domain", "example.com")
        self.managed_group_type = kwargs.get("managed_group_type", "security")
        self.managed_group_mail_domain = kwargs.get("managed_group_mail_domain", "")
        self.custom_group_ou_path = kwargs.get("custom_group_ou_path", "Managed Groups")
        type(self).init_kwargs.append(dict(kwargs))

    def ensure_ou(self, ou_name: str, parent_dn: str):
        return True, f"OU={ou_name},{parent_dn}", True

    def ensure_department_group(self, department_id, parent_department_id, ou_name, ou_dn, full_path, display_separator="-", binding_repo=None):
        group_info = DepartmentGroupInfo(
            exists=True,
            group_sam=f"WECOM_D{department_id}",
            group_cn=f"{ou_name}__D{department_id}",
            group_dn=f"CN={ou_name}__D{department_id},{ou_dn}",
            display_name=f"{display_separator.join(full_path)} [D{department_id}]",
            description=f"source=wecom; dept_id={department_id}; path={'/'.join(full_path)}",
            binding_source="managed",
            created=True,
        )
        if binding_repo:
            binding_repo.upsert_binding(
                department_id=str(department_id),
                parent_department_id=str(parent_department_id) if parent_department_id else None,
                group_sam=group_info.group_sam,
                group_dn=group_info.group_dn,
                group_cn=group_info.group_cn,
                display_name=group_info.display_name,
                path_text="/".join(full_path),
                status="active",
            )
        return group_info

    def get_users_batch(self, usernames):
        existing = type(self).existing_users_by_domain.get(self.domain, {})
        return {username: existing[username] for username in usernames if username in existing}

    def get_all_enabled_users(self):
        return list(type(self).enabled_users_by_domain.get(self.domain, []))

    def create_user(self, username: str, display_name: str, email: str, ou_dn: str, *, extra_attributes=None) -> bool:
        type(self).created_users.append(
            {
                "domain": self.domain,
                "username": username,
                "display_name": display_name,
                "email": email,
                "ou_dn": ou_dn,
                "extra_attributes": dict(extra_attributes or {}),
            }
        )
        return True

    def update_user(self, username: str, display_name: str, email: str, ou_dn: str, *, extra_attributes=None) -> bool:
        type(self).updated_users.append(
            {
                "domain": self.domain,
                "username": username,
                "display_name": display_name,
                "email": email,
                "ou_dn": ou_dn,
                "extra_attributes": dict(extra_attributes or {}),
            }
        )
        return True

    def add_user_to_group(self, username: str, group_name: str) -> bool:
        type(self).user_group_memberships.append(
            {
                "domain": self.domain,
                "username": username,
                "group_name": group_name,
            }
        )
        return True

    def add_group_to_group(self, child_group_dn: str, parent_group_dn: str) -> bool:
        return True

    def remove_group_from_group(self, child_group_dn: str, parent_group_dn: str) -> bool:
        return True

    def ensure_custom_group(self, source_type: str, source_key: str, display_name: str, ou_path=None):
        group_sam = runtime.build_custom_group_sam(source_type, source_key)
        group_cn = f"{display_name}__{source_type}_{source_key}"
        group_dn = f"CN={group_cn},{self.get_ou_dn(['Managed Groups'])}"
        type(self).custom_groups.append(
            {
                "domain": self.domain,
                "source_type": source_type,
                "source_key": source_key,
                "display_name": display_name,
                "group_sam": group_sam,
                "group_dn": group_dn,
                "group_type": self.managed_group_type,
                "group_mail_domain": self.managed_group_mail_domain,
            }
        )
        return DepartmentGroupInfo(
            exists=True,
            group_sam=group_sam,
            group_cn=group_cn,
            group_dn=group_dn,
            display_name=display_name,
            description=f"source={source_type}; key={source_key}",
            binding_source="managed",
            created=True,
        )

    def disable_user(self, username: str) -> bool:
        type(self).disabled_users.append({"domain": self.domain, "username": username})
        return True

    def get_user_details(self, username: str):
        return dict(
            type(self).user_details_by_username.get(
                username,
                {
                    "SamAccountName": username,
                    "DisplayName": username,
                    "Mail": f"{username}@example.com",
                },
            )
        )

    def find_parent_groups_for_member(self, member_dn):
        return []
