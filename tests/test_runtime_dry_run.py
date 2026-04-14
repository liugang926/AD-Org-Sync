import logging
import os
import unittest
from unittest.mock import patch

from sync_app.core.models import (
    AccountConfig,
    AppConfig,
    DepartmentGroupInfo,
    DepartmentNode,
    DirectoryGroupRecord,
    DirectoryUserRecord,
    LDAPConfig,
    SyncRunStats,
    UserDepartmentBundle,
    WeComConfig,
    WeComUser,
)
from sync_app.services import runtime
from sync_app.storage.local_db import (
    AttributeMappingRuleRepository,
    CustomManagedGroupBindingRepository,
    DatabaseManager,
    DepartmentOuMappingRepository,
    ManagedGroupBindingRepository,
    OffboardingQueueRepository,
    OrganizationRepository,
    PlannedOperationRepository,
    SettingsRepository,
    SyncConnectorRepository,
    SyncConflictRepository,
    SyncEventRepository,
    SyncExceptionRuleRepository,
    SyncJobRepository,
    SyncOperationLogRepository,
    SyncPlanReviewRepository,
    SyncReplayRequestRepository,
    UserLifecycleQueueRepository,
    UserIdentityBindingRepository,
)


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


class SyncRunStatsModelTests(unittest.TestCase):
    def test_from_mapping_coerces_nested_structures(self):
        stats = SyncRunStats.from_mapping(
            {
                "execution_mode": "dry_run",
                "total_users": 3,
                "processed_users": 2,
                "disabled_users": ["u1"],
                "error_count": 1,
                "skipped_operations": {
                    "total": 2,
                    "by_action": {"group_membership": 2},
                    "samples": [{"action_type": "group_membership"}],
                },
                "errors": {
                    "department_errors": [{"department": "HQ", "error": "boom"}],
                },
                "operations": {
                    "users_created": 5,
                    "groups_assigned": 4,
                },
            }
        )

        self.assertEqual(stats.execution_mode, "dry_run")
        self.assertEqual(stats.total_users, 3)
        self.assertEqual(stats.processed_users, 2)
        self.assertEqual(stats.disabled_users, ["u1"])
        self.assertEqual(stats.skipped_operations.total, 2)
        self.assertEqual(stats.skipped_operations.by_action["group_membership"], 2)
        self.assertEqual(len(stats.errors.department_errors), 1)
        self.assertEqual(stats.operations.users_created, 5)
        self.assertEqual(stats.operations.groups_assigned, 4)


class DepartmentPlacementStrategyTests(unittest.TestCase):
    def test_resolve_target_department_prefers_manual_override(self):
        user = WeComUser(userid="alice", name="Alice", departments=[2, 1], raw_payload={"main_department": 1})
        bundle = UserDepartmentBundle(
            user=user,
            departments=[
                DepartmentNode(department_id=2, name="Sales", parent_id=0, path=["Root", "Sales"], path_ids=[10, 2]),
                DepartmentNode(department_id=1, name="HQ", parent_id=0, path=["Root", "HQ"], path_ids=[10, 1]),
            ],
        )

        target, reason = runtime._resolve_target_department(
            bundle,
            placement_strategy="wecom_primary_department",
            is_department_excluded=lambda _: False,
            override_department_id=2,
        )

        self.assertIsNotNone(target)
        self.assertEqual(target.department_id, 2)
        self.assertEqual(reason, "manual_override")

    def test_resolve_target_department_accepts_source_and_legacy_primary_department_aliases(self):
        user = WeComUser(userid="alice", name="Alice", departments=[30, 5], raw_payload={"main_department": 30})
        bundle = UserDepartmentBundle(
            user=user,
            departments=[
                DepartmentNode(department_id=30, name="Branch", parent_id=0, path=["Root", "Branch"], path_ids=[10, 30]),
                DepartmentNode(department_id=5, name="HQ", parent_id=0, path=["Root", "HQ"], path_ids=[10, 5]),
            ],
        )

        target, reason = runtime._resolve_target_department(
            bundle,
            placement_strategy="source_primary_department",
            is_department_excluded=lambda _: False,
        )
        legacy_target, legacy_reason = runtime._resolve_target_department(
            bundle,
            placement_strategy="wecom_primary_department",
            is_department_excluded=lambda _: False,
        )

        self.assertIsNotNone(target)
        self.assertEqual(target.department_id, 30)
        self.assertEqual(reason, "source_primary_department")
        self.assertIsNotNone(legacy_target)
        self.assertEqual(legacy_target.department_id, 30)
        self.assertEqual(legacy_reason, "source_primary_department")

    def test_resolve_target_department_uses_configured_strategy(self):
        user = WeComUser(userid="alice", name="Alice", departments=[30, 5])
        bundle = UserDepartmentBundle(
            user=user,
            departments=[
                DepartmentNode(department_id=30, name="Branch", parent_id=0, path=["Root", "Branch"], path_ids=[10, 30]),
                DepartmentNode(department_id=5, name="HQ", parent_id=0, path=["Root", "HQ"], path_ids=[10, 5]),
            ],
        )

        target, reason = runtime._resolve_target_department(
            bundle,
            placement_strategy="lowest_department_id",
            is_department_excluded=lambda _: False,
        )

        self.assertIsNotNone(target)
        self.assertEqual(target.department_id, 5)
        self.assertEqual(reason, "lowest_department_id")


class RunSyncDryRunTests(unittest.TestCase):
    def test_run_sync_job_dry_run_returns_compatible_summary_dict(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
                validate_cert=True,
                ca_cert_path="C:/certs/ad.pem",
            ),
            domain="example.com",
            account=AccountConfig(
                default_password="VeryStrong123!456",
                force_change_password=True,
                password_complexity="strong",
            ),
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_dry_run_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        FakeADSyncLDAPS.last_init_kwargs = None

        with patch.object(runtime, "load_sync_config", return_value=config), \
            patch.object(runtime, "validate_config", return_value=(True, [])), \
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")), \
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")), \
            patch.object(runtime, "run_config_security_self_check", return_value=[]), \
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComAPI), \
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncLDAPS), \
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")), \
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"), \
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"):
            result = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertIsInstance(result, dict)
        self.assertEqual(result["execution_mode"], "dry_run")
        self.assertEqual(result["total_users"], 1)
        self.assertEqual(result["error_count"], 0)
        self.assertGreaterEqual(result["planned_operation_count"], 3)
        self.assertEqual(result["executed_operation_count"], 0)
        self.assertIn("job_summary", result)
        self.assertEqual(result["job_summary"]["mode"], "dry_run")
        self.assertEqual(result["job_summary"]["planned_operation_count"], result["planned_operation_count"])

        stats = SyncRunStats.from_mapping(result)
        self.assertEqual(stats.total_users, 1)
        self.assertEqual(stats.execution_mode, "dry_run")
        self.assertEqual(stats.error_count, 0)
        self.assertGreaterEqual(stats.planned_operation_count, 3)

        self.assertIsNotNone(FakeADSyncLDAPS.last_init_kwargs)
        self.assertEqual(FakeADSyncLDAPS.last_init_kwargs["default_password"], "VeryStrong123!456")
        self.assertTrue(FakeADSyncLDAPS.last_init_kwargs["force_change_password"])
        self.assertEqual(FakeADSyncLDAPS.last_init_kwargs["password_complexity"], "strong")
        self.assertTrue(FakeADSyncLDAPS.last_init_kwargs["validate_cert"])
        self.assertEqual(FakeADSyncLDAPS.last_init_kwargs["ca_cert_path"], "C:/certs/ad.pem")

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        binding = UserIdentityBindingRepository(manager).get_binding_record_by_source_user_id("alice")
        self.assertIsNotNone(binding)
        self.assertEqual(binding.ad_username, "alice")

    def test_run_sync_job_applies_basic_scope_and_directory_root_ou_settings(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(
                default_password="VeryStrong123!456",
                force_change_password=True,
                password_complexity="strong",
            ),
            config_path="ignored.ini",
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_list = [
            {"id": 1, "name": "HQ", "parentid": 0},
            {"id": 2, "name": "China", "parentid": 1},
            {"id": 3, "name": "East", "parentid": 2},
        ]
        FakeWeComProgrammableAPI.department_users = {
            3: [{"userid": "alice", "name": "Alice"}],
        }
        FakeWeComProgrammableAPI.user_details = {
            "alice": {
                "userid": "alice",
                "name": "Alice",
                "email": "alice@example.com",
                "department": [3],
            }
        }

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_scope_root_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        OrganizationRepository(manager).ensure_default(config_path="ignored.ini")
        settings_repo = SettingsRepository(manager)
        settings_repo.set_value("source_root_unit_ids", "2", "string", org_id="default")
        settings_repo.set_value("directory_root_ou_path", "Managed Users", "string", org_id="default")
        settings_repo.set_value("disabled_users_ou_path", "Managed Users/Disabled Users", "string", org_id="default")

        FakeADSyncLDAPS.last_init_kwargs = None

        with patch.object(runtime, "load_sync_config", return_value=config), \
            patch.object(runtime, "validate_config", return_value=(True, [])), \
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")), \
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")), \
            patch.object(runtime, "run_config_security_self_check", return_value=[]), \
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI), \
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncLDAPS), \
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")), \
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"), \
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"):
            result = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(result["error_count"], 0)
        self.assertIsNotNone(FakeADSyncLDAPS.last_init_kwargs)
        self.assertEqual(FakeADSyncLDAPS.last_init_kwargs["user_root_ou_path"], "Managed Users")
        self.assertEqual(FakeADSyncLDAPS.last_init_kwargs["disabled_users_ou_name"], "Managed Users/Disabled Users")

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        planned_operations = PlannedOperationRepository(manager).list_operations_for_job(result["job_id"])
        create_user_ops = [item for item in planned_operations if item["operation_type"] == "create_user"]
        self.assertEqual(len(create_user_ops), 1)
        self.assertIn("OU=East,OU=China,OU=Managed Users,DC=example,DC=com", create_user_ops[0]["target_dn"])
        self.assertNotIn("OU=HQ", create_user_ops[0]["target_dn"])

    def test_run_sync_job_accepts_directory_root_ou_dn_input(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(
                default_password="VeryStrong123!456",
                force_change_password=True,
                password_complexity="strong",
            ),
            config_path="ignored.ini",
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_list = [
            {"id": 10, "name": "Root", "parentid": 0},
            {"id": 2, "name": "China", "parentid": 10},
            {"id": 20, "name": "East", "parentid": 2},
        ]
        FakeWeComProgrammableAPI.department_users = {
            20: [
                {
                    "userid": "alice",
                    "name": "Alice",
                    "email": "alice@example.com",
                    "department": [20],
                }
            ]
        }
        FakeWeComProgrammableAPI.user_detail = {
            "alice": {"userid": "alice", "name": "Alice", "email": "alice@example.com", "department": [20]}
        }

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_scope_root_dn_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        OrganizationRepository(manager).ensure_default(config_path="ignored.ini")
        settings_repo = SettingsRepository(manager)
        settings_repo.set_value("source_root_unit_ids", "2", "string", org_id="default")
        settings_repo.set_value(
            "directory_root_ou_path",
            "OU=China,OU=Managed Users,DC=example,DC=com",
            "string",
            org_id="default",
        )

        FakeADSyncLDAPS.last_init_kwargs = None

        with patch.object(runtime, "load_sync_config", return_value=config), \
            patch.object(runtime, "validate_config", return_value=(True, [])), \
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")), \
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")), \
            patch.object(runtime, "run_config_security_self_check", return_value=[]), \
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI), \
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncLDAPS), \
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")), \
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"), \
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"):
            result = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(result["error_count"], 0)
        self.assertEqual(FakeADSyncLDAPS.last_init_kwargs["user_root_ou_path"], "Managed Users/China")

    def test_run_sync_job_skips_protected_default_account_create(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(
                default_password="VeryStrong123!456",
                force_change_password=True,
                password_complexity="strong",
            ),
            config_path="ignored.ini",
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_users = {1: [{"userid": "administrator", "name": "Administrator"}]}
        FakeWeComProgrammableAPI.user_details = {
            "administrator": {
                "userid": "administrator",
                "name": "Administrator",
                "email": "administrator@example.com",
                "department": [1],
            }
        }

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_protected_create_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        with patch.object(runtime, "load_sync_config", return_value=config), \
            patch.object(runtime, "validate_config", return_value=(True, [])), \
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")), \
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")), \
            patch.object(runtime, "run_config_security_self_check", return_value=[]), \
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI), \
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncLDAPS), \
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")), \
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"), \
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"):
            result = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        self.assertIsNone(UserIdentityBindingRepository(manager).get_binding_record_by_source_user_id("administrator"))
        planned_operations = PlannedOperationRepository(manager).list_operations_for_job(result["job_id"], limit=50)
        self.assertFalse(
            any(
                item["operation_type"] == "create_user" and item["source_id"] == "administrator"
                for item in planned_operations
            )
        )
        operation_records = SyncOperationLogRepository(manager).list_records_for_job(result["job_id"], limit=100)
        self.assertTrue(
            any(
                item.reason_code == "protected_ad_account"
                and item.target_id == "administrator"
                for item in operation_records
            )
        )

    def test_run_sync_job_skips_protected_account_disable_even_when_bound(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(
                default_password="VeryStrong123!456",
                force_change_password=True,
                password_complexity="strong",
            ),
            config_path="ignored.ini",
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_users = {1: []}
        FakeWeComProgrammableAPI.user_details = {}

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_protected_disable_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        UserIdentityBindingRepository(manager).upsert_binding(
            "former-admin",
            "administrator",
            org_id="default",
            source="manual",
            notes="protected account binding",
            preserve_manual=False,
        )

        with patch.object(runtime, "load_sync_config", return_value=config), \
            patch.object(runtime, "validate_config", return_value=(True, [])), \
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")), \
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")), \
            patch.object(runtime, "run_config_security_self_check", return_value=[]), \
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI), \
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncProtectedDisable), \
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")), \
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"), \
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"):
            result = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        planned_operations = PlannedOperationRepository(manager).list_operations_for_job(result["job_id"], limit=50)
        self.assertFalse(
            any(
                item["operation_type"] == "disable_user"
                and (item["desired_state"] or {}).get("ad_username") == "administrator"
                for item in planned_operations
            )
        )
        operation_records = SyncOperationLogRepository(manager).list_records_for_job(result["job_id"], limit=100)
        self.assertTrue(
            any(
                item.reason_code == "protected_ad_account"
                and item.operation_type == "disable_user"
                and item.target_id == "administrator"
                for item in operation_records
            )
        )

    def test_run_sync_job_records_conflict_when_multiple_ad_candidates_exist(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_conflict_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        with patch.object(runtime, "load_sync_config", return_value=config), \
            patch.object(runtime, "validate_config", return_value=(True, [])), \
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")), \
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")), \
            patch.object(runtime, "run_config_security_self_check", return_value=[]), \
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComConflictAPI), \
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncConflict), \
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")), \
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"), \
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"):
            result = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(result["conflict_count"], 1)

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        conflicts = SyncConflictRepository(manager).list_conflicts_for_job(result["job_id"])
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0].conflict_type, "multiple_ad_candidates")

        binding = UserIdentityBindingRepository(manager).get_binding_record_by_source_user_id("alice")
        self.assertIsNone(binding)

    def test_run_sync_job_respects_skip_user_group_membership_exception(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_exception_membership_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        SyncExceptionRuleRepository(manager).upsert_rule(
            rule_type="skip_user_group_membership",
            match_value="alice",
            notes="test exception",
        )

        with patch.object(runtime, "load_sync_config", return_value=config), \
            patch.object(runtime, "validate_config", return_value=(True, [])), \
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")), \
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")), \
            patch.object(runtime, "run_config_security_self_check", return_value=[]), \
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComAPI), \
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncLDAPS), \
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")), \
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"), \
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"):
            result = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(result["skipped_operations"]["by_action"]["add_user_to_group"], 1)

        planned_operations = PlannedOperationRepository(manager).list_operations_for_job(result["job_id"])
        self.assertTrue(
            any(item["object_type"] == "user" and item["operation_type"] in {"create_user", "update_user"} for item in planned_operations)
        )
        self.assertFalse(any(item["object_type"] == "group_membership" for item in planned_operations))

    def test_run_sync_job_respects_skip_department_placement_exception(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_exception_placement_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        SyncExceptionRuleRepository(manager).upsert_rule(
            rule_type="skip_department_placement",
            match_value="1",
            notes="test exception",
        )

        with patch.object(runtime, "load_sync_config", return_value=config), \
            patch.object(runtime, "validate_config", return_value=(True, [])), \
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")), \
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")), \
            patch.object(runtime, "run_config_security_self_check", return_value=[]), \
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComAPI), \
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncLDAPS), \
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")), \
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"), \
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"):
            result = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(result["skipped_operations"]["by_action"]["resolve_target_department"], 1)

        planned_operations = PlannedOperationRepository(manager).list_operations_for_job(result["job_id"])
        self.assertFalse(any(item["object_type"] == "user" and item["operation_type"] in {"create_user", "update_user"} for item in planned_operations))

    def test_run_sync_job_respects_skip_group_relation_cleanup_exception(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_exception_cleanup_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        SettingsRepository(manager).set_value("managed_relation_cleanup_enabled", "true", "bool")
        ManagedGroupBindingRepository(manager).upsert_binding(
            department_id="1",
            parent_department_id=None,
            group_sam="wecom_d1",
            group_dn="CN=HQ__D1,OU=HQ,DC=example,DC=com",
            group_cn="HQ__D1",
            display_name="HQ [D1]",
            path_text="HQ",
            status="active",
        )
        ManagedGroupBindingRepository(manager).upsert_binding(
            department_id="99",
            parent_department_id=None,
            group_sam="legacy_parent",
            group_dn="CN=LegacyParent,OU=Managed,DC=example,DC=com",
            group_cn="LegacyParent",
            display_name="Legacy Parent",
            path_text="Legacy",
            status="active",
        )
        SyncExceptionRuleRepository(manager).upsert_rule(
            rule_type="skip_group_relation_cleanup",
            match_value="legacy_parent",
            notes="test exception",
        )

        with patch.object(runtime, "load_sync_config", return_value=config), \
            patch.object(runtime, "validate_config", return_value=(True, [])), \
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")), \
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")), \
            patch.object(runtime, "run_config_security_self_check", return_value=[]), \
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComAPI), \
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncCleanup), \
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")), \
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"), \
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"):
            result = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(result["skipped_operations"]["by_action"]["remove_group_from_group"], 1)

        planned_operations = PlannedOperationRepository(manager).list_operations_for_job(result["job_id"])
        self.assertFalse(any(item["operation_type"] == "remove_group_from_group" for item in planned_operations))

    def test_run_sync_job_respects_skip_user_disable_exception(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_exception_disable_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        UserIdentityBindingRepository(manager).upsert_binding(
            "bob.wecom",
            "bob",
            source="manual",
            notes="managed disable candidate",
            preserve_manual=False,
        )
        SyncExceptionRuleRepository(manager).upsert_rule(
            rule_type="skip_user_disable",
            match_value="bob.wecom",
            notes="test exception",
        )

        with patch.object(runtime, "load_sync_config", return_value=config), \
            patch.object(runtime, "validate_config", return_value=(True, [])), \
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")), \
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")), \
            patch.object(runtime, "run_config_security_self_check", return_value=[]), \
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComAPI), \
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncApply), \
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")), \
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"), \
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"):
            result = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(result["skipped_operations"]["by_action"]["disable_user"], 1)
        self.assertEqual(result["high_risk_operation_count"], 0)
        self.assertFalse(result["summary"]["review_required"])

        planned_operations = PlannedOperationRepository(manager).list_operations_for_job(result["job_id"])
        self.assertFalse(any(item["operation_type"] == "disable_user" for item in planned_operations))

    def test_run_sync_job_applies_attribute_mapping_and_write_back(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_mapping_writeback_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        settings_repo = SettingsRepository(manager)
        settings_repo.set_value("attribute_mapping_enabled", "true", "bool")
        settings_repo.set_value("write_back_enabled", "true", "bool")
        AttributeMappingRuleRepository(manager).upsert_rule(
            connector_id="",
            direction="source_to_ad",
            source_field="position",
            target_field="title",
            sync_mode="replace",
            notes="map position to title",
        )
        AttributeMappingRuleRepository(manager).upsert_rule(
            connector_id="",
            direction="ad_to_source",
            source_field="mail",
            target_field="email",
            sync_mode="replace",
            notes="write exchange mail back to WeCom",
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.user_details = {
            "alice": {
                "userid": "alice",
                "name": "Alice",
                "email": "alice@stale.example.com",
                "department": [1],
                "position": "Platform Engineer",
            }
        }
        FakeADSyncPolicy.reset()
        FakeADSyncPolicy.user_details_by_username = {
            "alice": {
                "mail": "alice@exchange.example.com",
                "displayName": "Alice",
            }
        }

        patches = [
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(result["error_count"], 0)
        self.assertEqual(len(FakeADSyncPolicy.created_users), 1)
        self.assertEqual(
            FakeADSyncPolicy.created_users[0]["extra_attributes"]["title"]["value"],
            "Platform Engineer",
        )
        self.assertEqual(
            FakeWeComProgrammableAPI.updated_users,
            [{"userid": "alice", "updates": {"email": "alice@exchange.example.com"}}],
        )

    def test_run_sync_job_routes_users_to_connector_and_uses_connector_username_template(self):
        default_config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.default.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="default.ini",
        )
        asia_config = AppConfig(
            wecom=default_config.wecom,
            ldap=LDAPConfig(
                server="ldap.asia.example.com",
                domain="asia.example.com",
                username="ASIA\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="asia.example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="asia.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_connector_template_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        SettingsRepository(manager).set_value("advanced_connector_routing_enabled", "true", "bool")
        SyncConnectorRepository(manager).upsert_connector(
            connector_id="asia",
            name="Asia Domain",
            config_path="asia.ini",
            root_department_ids=[2],
            username_template="{pinyin_initials}{employee_id}",
            group_type="distribution",
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_list = [
            {"id": 1, "name": "HQ", "parentid": 0},
            {"id": 2, "name": "Asia", "parentid": 0},
        ]
        FakeWeComProgrammableAPI.department_users = {
            1: [],
            2: [{"userid": "alice.wecom", "name": "Alice Zhang"}],
        }
        FakeWeComProgrammableAPI.user_details = {
            "alice.wecom": {
                "userid": "alice.wecom",
                "name": "Alice Zhang",
                "email": "",
                "department": [2],
                "employee_id": "1001",
            }
        }
        FakeADSyncPolicy.reset()

        def fake_load_config(path):
            return asia_config if os.path.basename(path) == "asia.ini" else default_config

        patches = [
            patch.object(runtime, "load_sync_config", side_effect=fake_load_config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="default.ini",
            )

        self.assertEqual(result["error_count"], 0)
        self.assertTrue(
            any(item["domain"] == "asia.example.com" and item["username"] == "az1001" for item in FakeADSyncPolicy.created_users)
        )
        self.assertTrue(
            any(
                kwargs.get("domain") == "asia.example.com" and kwargs.get("managed_group_type") == "distribution"
                for kwargs in FakeADSyncPolicy.init_kwargs
            )
        )
        binding = UserIdentityBindingRepository(manager).get_binding_record_by_ad_username("az1001", connector_id="asia")
        self.assertIsNotNone(binding)
        self.assertEqual(binding.source_user_id, "alice.wecom")

    def test_run_sync_job_handles_same_pinyin_collisions_and_reuses_persisted_bindings(self):
        default_config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.default.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="default.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_same_pinyin_binding_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        SettingsRepository(manager).set_value("advanced_connector_routing_enabled", "true", "bool")
        SyncConnectorRepository(manager).upsert_connector(
            connector_id="cn",
            name="China Domain",
            config_path="default.ini",
            root_department_ids=[2],
            username_strategy="family_name_pinyin_given_initials",
            username_collision_policy="append_employee_id",
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_list = [
            {"id": 2, "name": "China", "parentid": 0},
        ]
        FakeWeComProgrammableAPI.department_users = {
            2: [
                {"userid": "zhangsan.wecom", "name": "张三"},
                {"userid": "zhangsen.wecom", "name": "张森"},
            ],
        }
        FakeWeComProgrammableAPI.user_details = {
            "zhangsan.wecom": {
                "userid": "zhangsan.wecom",
                "name": "张三",
                "department": [2],
                "employee_id": "2001",
            },
            "zhangsen.wecom": {
                "userid": "zhangsen.wecom",
                "name": "张森",
                "department": [2],
                "employee_id": "2002",
            },
        }
        FakeADSyncPolicy.reset()

        patches = [
            patch.object(runtime, "load_sync_config", return_value=default_config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            first_result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="default.ini",
            )

        self.assertEqual(first_result["error_count"], 0)
        created_usernames = {
            item["username"]
            for item in FakeADSyncPolicy.created_users
            if item["domain"] == "example.com"
        }
        self.assertEqual(created_usernames, {"zhangs2001", "zhangs2002"})

        binding_repo = UserIdentityBindingRepository(manager)
        first_binding = binding_repo.get_binding_record_by_source_user_id("zhangsan.wecom")
        second_binding = binding_repo.get_binding_record_by_source_user_id("zhangsen.wecom")
        self.assertEqual(first_binding.ad_username, "zhangs2001")
        self.assertEqual(second_binding.ad_username, "zhangs2002")
        self.assertEqual(first_binding.source, "managed_generated")
        self.assertEqual(second_binding.source, "managed_generated")

        FakeADSyncPolicy.created_users = []
        FakeADSyncPolicy.updated_users = []
        FakeADSyncPolicy.existing_users_by_domain = {
                "example.com": {
                "zhangs2001": DirectoryUserRecord(
                    username="zhangs2001",
                    dn="CN=zhangs2001,OU=China,DC=example,DC=com",
                    display_name="张三",
                    email="zhangs2001@example.com",
                ),
                "zhangs2002": DirectoryUserRecord(
                    username="zhangs2002",
                    dn="CN=zhangs2002,OU=China,DC=example,DC=com",
                    display_name="张森",
                    email="zhangs2002@example.com",
                ),
            }
        }
        FakeADSyncPolicy.enabled_users_by_domain = {"example.com": ["zhangs2001", "zhangs2002"]}

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            second_result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test_repeat",
                db_path=db_path,
                config_path="default.ini",
            )

        self.assertEqual(second_result["error_count"], 0)
        self.assertEqual(FakeADSyncPolicy.created_users, [])
        updated_usernames = {
            item["username"]
            for item in FakeADSyncPolicy.updated_users
            if item["domain"] == "example.com"
        }
        self.assertEqual(updated_usernames, {"zhangs2001", "zhangs2002"})

    def test_run_sync_job_applies_department_to_ou_mapping_for_subtree(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="default.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_department_ou_mapping_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        DepartmentOuMappingRepository(manager).upsert_mapping(
            source_department_id="2",
            source_department_name="China",
            target_ou_path="Managed Users/China",
            apply_mode="subtree",
            org_id="default",
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_list = [
            {"id": 1, "name": "HQ", "parentid": 0},
            {"id": 2, "name": "China", "parentid": 1},
            {"id": 3, "name": "Shanghai", "parentid": 2},
        ]
        FakeWeComProgrammableAPI.department_users = {
            1: [],
            2: [],
            3: [{"userid": "alice", "name": "Alice"}],
        }
        FakeWeComProgrammableAPI.user_details = {
            "alice": {
                "userid": "alice",
                "name": "Alice",
                "department": [3],
                "email": "alice@example.com",
            }
        }
        FakeADSyncPolicy.reset()

        patches = [
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="default.ini",
            )

        self.assertEqual(result["error_count"], 0)
        self.assertTrue(FakeADSyncPolicy.created_users)
        self.assertEqual(
            FakeADSyncPolicy.created_users[0]["ou_dn"],
            "OU=Shanghai,OU=China,OU=Managed Users,DC=example,DC=com",
        )

    def test_run_sync_job_persists_failure_diagnostics_to_job_events_and_operation_logs(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="default.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_failure_logging_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        patches = [
            patch.object(runtime, "generate_job_id", return_value="job-failed-001"),
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_prepare_sync_environment", side_effect=RuntimeError("boom failure")),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            with self.assertRaisesRegex(RuntimeError, "boom failure"):
                runtime.run_sync_job(
                    execution_mode="dry_run",
                    trigger_type="unit_test_failure",
                    db_path=db_path,
                    config_path="default.ini",
                )

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        job_record = SyncJobRepository(manager).get_job_record("job-failed-001")
        self.assertIsNotNone(job_record)
        self.assertEqual(job_record.status, "FAILED")
        self.assertEqual(job_record.error_count, 1)
        self.assertEqual(job_record.summary["error"], "boom failure")
        self.assertEqual(job_record.summary["error_type"], "RuntimeError")
        self.assertEqual(job_record.summary["error_category"], "unknown")
        self.assertEqual(job_record.summary["error_category_label"], "Unknown")
        self.assertTrue(job_record.summary["diagnostic_summary"])
        self.assertGreaterEqual(len(job_record.summary["diagnostic_actions"]), 1)
        self.assertEqual(job_record.summary["log_file"], "test-runtime.log")
        self.assertIn("RuntimeError: boom failure", job_record.summary["error_traceback"])

        events = SyncEventRepository(manager).list_events_for_job("job-failed-001", limit=20)
        self.assertTrue(any(item["event_type"] == "sync_failed" for item in events))
        failed_event = next(item for item in events if item["event_type"] == "sync_failed")
        self.assertEqual(failed_event["level"], "ERROR")
        self.assertEqual(failed_event["payload"]["error"], "boom failure")
        self.assertEqual(failed_event["payload"]["error_category"], "unknown")
        self.assertEqual(failed_event["payload"]["log_file"], "test-runtime.log")

        operation_logs = SyncOperationLogRepository(manager).list_records_for_job("job-failed-001", limit=20)
        failure_logs = [item for item in operation_logs if item.operation_type == "sync_job" and item.status == "error"]
        self.assertEqual(len(failure_logs), 1)
        self.assertEqual(failure_logs[0].details["error"], "boom failure")
        self.assertEqual(failure_logs[0].details["error_category"], "unknown")
        self.assertEqual(failure_logs[0].details["log_file"], "test-runtime.log")

    def test_run_sync_job_applies_custom_collision_template_and_persists_binding_anchor(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="default.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_custom_collision_template_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        SettingsRepository(manager).set_value("advanced_connector_routing_enabled", "true", "bool")
        SyncConnectorRepository(manager).upsert_connector(
            connector_id="na",
            name="North America",
            config_path="default.ini",
            root_department_ids=[2],
            username_strategy="family_name_pinyin_given_initials",
            username_collision_policy="custom_template",
            username_collision_template="{base}{counter2}",
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_list = [
            {"id": 2, "name": "North America", "parentid": 0},
        ]
        FakeWeComProgrammableAPI.department_users = {
            2: [
                {"userid": "alice.zhang", "name": "Alice Zhang"},
                {"userid": "alan.zhang", "name": "Alan Zhang"},
            ],
        }
        FakeWeComProgrammableAPI.user_details = {
            "alice.zhang": {
                "userid": "alice.zhang",
                "name": "Alice Zhang",
                "department": [2],
                "email": "alice.zhang@example.com",
            },
            "alan.zhang": {
                "userid": "alan.zhang",
                "name": "Alan Zhang",
                "department": [2],
                "email": "alan.zhang@example.com",
            },
        }
        FakeADSyncPolicy.reset()
        FakeADSyncPolicy.user_details_by_username = {
            "zhanga01": {
                "SamAccountName": "zhanga01",
                "DisplayName": "Alice Zhang",
                "Mail": "zhanga01@example.com",
                "DistinguishedName": "CN=Alice Zhang,OU=North America,DC=example,DC=com",
                "ObjectGUID": "11111111-1111-1111-1111-111111111111",
            },
            "zhanga02": {
                "SamAccountName": "zhanga02",
                "DisplayName": "Alan Zhang",
                "Mail": "zhanga02@example.com",
                "DistinguishedName": "CN=Alan Zhang,OU=North America,DC=example,DC=com",
                "ObjectGUID": "22222222-2222-2222-2222-222222222222",
            },
        }

        patches = [
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="default.ini",
            )

        self.assertEqual(result["error_count"], 0)
        created_usernames = {
            item["username"]
            for item in FakeADSyncPolicy.created_users
            if item["domain"] == "example.com"
        }
        self.assertEqual(created_usernames, {"zhanga01", "zhanga02"})

        binding_repo = UserIdentityBindingRepository(manager)
        first_binding = binding_repo.get_binding_record_by_source_user_id("alice.zhang")
        second_binding = binding_repo.get_binding_record_by_source_user_id("alan.zhang")
        self.assertEqual(first_binding.managed_username_base, "zhanga")
        self.assertEqual(second_binding.managed_username_base, "zhanga")
        expected_anchor_by_username = {
            "zhanga01": {
                "guid": "11111111-1111-1111-1111-111111111111",
                "dn_fragment": "CN=Alice Zhang",
            },
            "zhanga02": {
                "guid": "22222222-2222-2222-2222-222222222222",
                "dn_fragment": "CN=Alan Zhang",
            },
        }
        for binding in (first_binding, second_binding):
            expected_anchor = expected_anchor_by_username[binding.ad_username]
            self.assertEqual(binding.target_object_guid, expected_anchor["guid"])
            self.assertIn(expected_anchor["dn_fragment"], binding.target_object_dn)

    def test_run_sync_job_queues_offboarding_and_uses_last_synced_manager_state_for_notification(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            webhook_url="https://example.invalid/webhook",
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_offboarding_queue_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        settings_repo = SettingsRepository(manager)
        settings_repo.set_value("offboarding_lifecycle_enabled", "true", "bool")
        settings_repo.set_value("offboarding_grace_days", "7", "int")
        settings_repo.set_value("offboarding_notify_managers", "true", "bool")
        UserIdentityBindingRepository(manager).upsert_binding(
            "bob.wecom",
            "bob",
            source="manual",
            notes="managed user",
            preserve_manual=False,
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_users = {
            1: [{"userid": "bob.wecom", "name": "Bob"}],
        }
        FakeWeComProgrammableAPI.user_details = {
            "bob.wecom": {
                "userid": "bob.wecom",
                "name": "Bob",
                "email": "bob@example.com",
                "department": [1],
                "leader_in_dept": [{"leader_userid": "manager1"}],
            }
        }
        FakeADSyncPolicy.reset()
        FakeWeChatBot.reset()

        patches = [
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime, "WebhookNotificationClient", FakeWeChatBot),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11], patches[12]:
            first_result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(first_result["error_count"], 0)

        FakeWeComProgrammableAPI.department_users = {1: []}
        FakeWeComProgrammableAPI.user_details = {}
        FakeADSyncPolicy.enabled_users_by_domain = {"example.com": ["bob"]}
        FakeADSyncPolicy.disabled_users = []
        FakeWeChatBot.reset()

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11], patches[12]:
            second_result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(second_result["error_count"], 0)
        self.assertEqual(FakeADSyncPolicy.disabled_users, [])
        offboarding_records = OffboardingQueueRepository(manager).list_pending_records()
        self.assertEqual(len(offboarding_records), 1)
        self.assertEqual(offboarding_records[0].ad_username, "bob")
        self.assertEqual(offboarding_records[0].manager_userids, ["manager1"])
        self.assertTrue(any("manager1" in message for message in FakeWeChatBot.messages))

    def test_run_sync_job_queues_future_onboarding_until_start_date(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_future_onboarding_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        settings_repo = SettingsRepository(manager)
        settings_repo.set_value("future_onboarding_enabled", "true", "bool")
        settings_repo.set_value("future_onboarding_start_field", "hire_date", "string")

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.user_details = {
            "alice": {
                "userid": "alice",
                "name": "Alice",
                "email": "alice@example.com",
                "department": [1],
                "hire_date": "2999-01-01T00:00:00+00:00",
            }
        }
        FakeADSyncPolicy.reset()

        patches = [
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(result["error_count"], 0)
        self.assertEqual(FakeADSyncPolicy.created_users, [])
        lifecycle_record = UserLifecycleQueueRepository(manager).get_record_for_source_user(
            lifecycle_type="future_onboarding",
            connector_id="default",
            source_user_id="alice",
        )
        self.assertIsNotNone(lifecycle_record)
        self.assertEqual(lifecycle_record.status, "pending")
        planned_operations = PlannedOperationRepository(manager).list_operations_for_job(result["job_id"])
        self.assertTrue(any(item["operation_type"] == "queue_future_onboarding" for item in planned_operations))

    def test_run_sync_job_disables_expired_contractor_and_marks_lifecycle_complete(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_contractor_expiry_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        settings_repo = SettingsRepository(manager)
        settings_repo.set_value("contractor_lifecycle_enabled", "true", "bool")
        settings_repo.set_value("lifecycle_employment_type_field", "employment_type", "string")
        settings_repo.set_value("contractor_end_field", "contract_end_date", "string")
        settings_repo.set_value("lifecycle_sponsor_field", "sponsor_userid", "string")
        settings_repo.set_value("high_risk_apply_requires_review", "false", "bool")

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_users = {1: [{"userid": "vendor1", "name": "Vendor One"}]}
        FakeWeComProgrammableAPI.user_details = {
            "vendor1": {
                "userid": "vendor1",
                "name": "Vendor One",
                "email": "vendor1@example.com",
                "department": [1],
                "employment_type": "contractor",
                "contract_end_date": "2000-01-01T00:00:00+00:00",
                "sponsor_userid": "manager1",
            }
        }
        FakeADSyncPolicy.reset()
        FakeADSyncPolicy.enabled_users_by_domain = {"example.com": ["vendor1"]}

        patches = [
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(result["error_count"], 0)
        self.assertEqual(FakeADSyncPolicy.disabled_users, [{"domain": "example.com", "username": "vendor1"}])
        lifecycle_record = UserLifecycleQueueRepository(manager).get_record_for_source_user(
            lifecycle_type="contractor_expiry",
            connector_id="default",
            source_user_id="vendor1",
        )
        self.assertIsNotNone(lifecycle_record)
        self.assertEqual(lifecycle_record.status, "completed")
        operation_records = SyncOperationLogRepository(manager).list_records_for_job(result["job_id"])
        self.assertTrue(any(item["reason_code"] == "contractor_expired" for item in operation_records))

    def test_run_sync_job_processes_pending_replay_requests(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_replay_request_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        settings_repo = SettingsRepository(manager)
        settings_repo.set_value("automatic_replay_enabled", "true", "bool")
        replay_repo = SyncReplayRequestRepository(manager)
        request_id = replay_repo.enqueue_request(
            request_type="conflict_resolution",
            execution_mode="apply",
            requested_by="superadmin",
            target_scope="source_user",
            target_id="alice",
            trigger_reason="unit_test",
        )

        FakeWeComProgrammableAPI.reset()
        FakeADSyncPolicy.reset()

        patches = [
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertEqual(result["error_count"], 0)
        replay_record = replay_repo.get_request_record(request_id)
        self.assertIsNotNone(replay_record)
        self.assertEqual(replay_record.status, "completed")
        self.assertEqual(replay_record.last_job_id, result["job_id"])
        self.assertEqual(result["summary"]["automatic_replay_request_count"], 1)

    def test_run_sync_job_blocks_apply_when_disable_circuit_breaker_triggers(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            webhook_url="https://example.invalid/webhook",
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_breaker_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        settings_repo = SettingsRepository(manager)
        settings_repo.set_value("offboarding_grace_days", "0", "int")
        settings_repo.set_value("disable_circuit_breaker_enabled", "true", "bool")
        settings_repo.set_value("disable_circuit_breaker_percent", "5", "float")
        settings_repo.set_value("disable_circuit_breaker_min_count", "1", "int")
        settings_repo.set_value("disable_circuit_breaker_requires_approval", "true", "bool")
        UserIdentityBindingRepository(manager).upsert_binding(
            "bob.wecom",
            "bob",
            source="manual",
            notes="managed disable candidate",
            preserve_manual=False,
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_users = {1: []}
        FakeWeComProgrammableAPI.user_details = {}
        FakeADSyncPolicy.reset()
        FakeADSyncPolicy.enabled_users_by_domain = {"example.com": ["bob"]}
        FakeWeChatBot.reset()

        patches = [
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime, "WebhookNotificationClient", FakeWeChatBot),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11], patches[12]:
            result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertTrue(result["summary"]["review_required"])
        self.assertEqual(result["summary"]["reason"], "disable_circuit_breaker")
        self.assertEqual(FakeADSyncPolicy.disabled_users, [])
        self.assertTrue(any("circuit breaker" in message.lower() for message in FakeWeChatBot.messages))

    def test_run_sync_job_materializes_custom_groups_for_tags_and_external_chats(self):
        default_config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="default.ini",
        )
        collab_config = AppConfig(
            wecom=default_config.wecom,
            ldap=default_config.ldap,
            domain="example.com",
            account=default_config.account,
            config_path="collab.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_custom_group_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        settings_repo = SettingsRepository(manager)
        settings_repo.set_value("advanced_connector_routing_enabled", "true", "bool")
        settings_repo.set_value("custom_group_sync_enabled", "true", "bool")
        SyncConnectorRepository(manager).upsert_connector(
            connector_id="collab",
            name="Collaboration",
            config_path="collab.ini",
            root_department_ids=[1],
            group_type="mail_enabled_security",
            managed_tag_ids=["1001"],
            managed_external_chat_ids=["chat_01"],
        )
        UserIdentityBindingRepository(manager).upsert_binding(
            "alice",
            "alice",
            connector_id="collab",
            source="manual",
            notes="connector scoped binding",
            preserve_manual=False,
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.user_details = {
            "alice": {
                "userid": "alice",
                "name": "Alice",
                "email": "alice@example.com",
                "department": [1],
            }
        }
        FakeWeComProgrammableAPI.tag_list = [{"tagid": "1001", "tagname": "IT Admins"}]
        FakeWeComProgrammableAPI.tag_users = {
            "1001": {"tagname": "IT Admins", "userlist": [{"userid": "alice"}]}
        }
        FakeWeComProgrammableAPI.external_group_chats = {
            "chat_01": {
                "chat_id": "chat_01",
                "name": "Partners",
                "member_list": [{"userid": "alice"}],
            }
        }
        FakeADSyncPolicy.reset()

        def fake_load_config(path):
            return collab_config if path == "collab.ini" else default_config

        patches = [
            patch.object(runtime, "load_sync_config", side_effect=fake_load_config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="default.ini",
            )

        self.assertEqual(result["error_count"], 0)
        bindings = CustomManagedGroupBindingRepository(manager).list_active_records(connector_id="collab")
        self.assertEqual({item.source_type for item in bindings}, {"tag", "external_chat"})
        self.assertTrue(
            any(item["source_type"] == "tag" and item["group_type"] == "mail_enabled_security" for item in FakeADSyncPolicy.custom_groups)
        )
        self.assertTrue(
            any(item["username"] == "alice" and item["group_name"].startswith("WECOM_TAG_") for item in FakeADSyncPolicy.user_group_memberships)
        )
        self.assertTrue(
            any(item["username"] == "alice" and item["group_name"].startswith("WECOM_EXTERNAL") for item in FakeADSyncPolicy.user_group_memberships)
        )

    def test_apply_high_risk_plan_requires_approved_dry_run_review(self):
        config = AppConfig(
            wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.example.com",
                domain="example.com",
                username="EXAMPLE\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="ignored.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_review_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        UserIdentityBindingRepository(manager).upsert_binding(
            "bob.wecom",
            "bob",
            source="manual",
            notes="managed disable candidate",
            preserve_manual=False,
        )

        patches = [
            patch.object(runtime, "load_sync_config", return_value=config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncApply),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10]:
            dry_run_result = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertTrue(dry_run_result["summary"]["review_required"])
        self.assertGreaterEqual(dry_run_result["high_risk_operation_count"], 1)

        review_repo = SyncPlanReviewRepository(manager)
        review_record = review_repo.get_review_record_by_job_id(dry_run_result["job_id"])
        self.assertIsNotNone(review_record)
        self.assertEqual(review_record.status, "pending")

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10]:
            blocked_apply_result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertTrue(blocked_apply_result["summary"]["review_required"])

        review_repo.approve_review(
            dry_run_result["job_id"],
            reviewer_username="tester",
            review_notes="approved in test",
            expires_at="2999-01-01T00:00:00+00:00",
        )

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10]:
            approved_apply_result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="ignored.ini",
            )

        self.assertFalse(approved_apply_result["summary"]["review_required"])
        self.assertGreater(approved_apply_result["executed_operation_count"], 0)

        latest_job = SyncJobRepository(manager).get_job_record(approved_apply_result["job_id"])
        self.assertIsNotNone(latest_job)
        self.assertEqual(latest_job.status, "COMPLETED")

    def test_run_sync_job_uses_selected_organization_config_and_connector_scope(self):
        default_config = AppConfig(
            wecom=WeComConfig(corpid="corp-default", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.default.example.com",
                domain="default.example.com",
                username="DEFAULT\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="default.example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="default.ini",
        )
        asia_config = AppConfig(
            wecom=WeComConfig(corpid="corp-asia", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.asia-root.example.com",
                domain="asia-root.example.com",
                username="ASIA\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="asia-root.example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="asia.ini",
        )
        asia_connector_config = AppConfig(
            wecom=asia_config.wecom,
            ldap=LDAPConfig(
                server="ldap.asia-connector.example.com",
                domain="asia-connector.example.com",
                username="ASIA\\connector-admin",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="asia-connector.example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="asia-connector.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_organization_scope_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        OrganizationRepository(manager).upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path="asia.ini",
            description="",
            is_enabled=True,
        )
        settings_repo = SettingsRepository(manager)
        settings_repo.set_value("advanced_connector_routing_enabled", "true", "bool")
        connector_repo = SyncConnectorRepository(manager)
        connector_repo.upsert_connector(
            connector_id="asia-connector",
            org_id="asia",
            name="Asia Connector",
            config_path="asia-connector.ini",
            ldap_server="ldap.asia-connector.example.com",
            ldap_domain="asia-connector.example.com",
            ldap_username="ASIA\\connector-admin",
            ldap_password="password",
            ldap_use_ssl=True,
            ldap_port=636,
            root_department_ids=[2],
            username_template="{pinyin_initials}{employee_id}",
        )
        connector_repo.upsert_connector(
            connector_id="other-org-connector",
            org_id="default",
            name="Default Connector",
            config_path="default-org-connector.ini",
            root_department_ids=[3],
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_list = [
            {"id": 1, "name": "HQ", "parentid": 0},
            {"id": 2, "name": "Asia", "parentid": 0},
        ]
        FakeWeComProgrammableAPI.department_users = {
            1: [],
            2: [{"userid": "alice.wecom", "name": "Alice Zhang"}],
        }
        FakeWeComProgrammableAPI.user_details = {
            "alice.wecom": {
                "userid": "alice.wecom",
                "name": "Alice Zhang",
                "email": "",
                "department": [2],
                "employee_id": "2001",
            }
        }
        FakeADSyncPolicy.reset()

        def fake_load_config(path):
            filename = os.path.basename(path)
            if filename == "asia.ini":
                return asia_config
            if filename == "asia-connector.ini":
                raise AssertionError("connector config should be resolved from database overrides")
            if filename == "default.ini":
                return default_config
            if filename == "default-org-connector.ini":
                raise AssertionError("connector from another organization should not be loaded")
            raise AssertionError(f"unexpected config path: {path}")

        patches = [
            patch.object(runtime, "load_sync_config", side_effect=fake_load_config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="default.ini",
                org_id="asia",
            )

        self.assertEqual(result["error_count"], 0)
        self.assertEqual(result["org_id"], "asia")
        latest_job = SyncJobRepository(manager).get_job_record(result["job_id"])
        self.assertIsNotNone(latest_job)
        self.assertEqual(latest_job.org_id, "asia")
        self.assertTrue(
            any(kwargs.get("domain") == "asia-connector.example.com" for kwargs in FakeADSyncPolicy.init_kwargs)
        )

    def test_run_sync_job_isolates_bindings_and_exception_rules_by_organization(self):
        default_config = AppConfig(
            wecom=WeComConfig(corpid="corp-default", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.default.example.com",
                domain="default.example.com",
                username="DEFAULT\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="default.example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="default.ini",
        )
        asia_config = AppConfig(
            wecom=WeComConfig(corpid="corp-asia", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.asia-root.example.com",
                domain="asia-root.example.com",
                username="ASIA\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="asia-root.example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="asia.ini",
        )
        asia_connector_config = AppConfig(
            wecom=asia_config.wecom,
            ldap=LDAPConfig(
                server="ldap.asia-connector.example.com",
                domain="asia-connector.example.com",
                username="ASIA\\connector-admin",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="asia-connector.example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="asia-connector.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_org_isolation_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        OrganizationRepository(manager).upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path="asia.ini",
            description="",
            is_enabled=True,
        )
        settings_repo = SettingsRepository(manager)
        settings_repo.set_value("advanced_connector_routing_enabled", "true", "bool")
        SyncConnectorRepository(manager).upsert_connector(
            connector_id="asia-connector",
            org_id="asia",
            name="Asia Connector",
            config_path="asia-connector.ini",
            root_department_ids=[2],
            username_template="{employee_id}",
        )
        UserIdentityBindingRepository(manager).upsert_binding(
            "alice.wecom",
            "legacy.default",
            org_id="default",
            source="manual",
            notes="default org binding",
            preserve_manual=False,
        )
        SyncExceptionRuleRepository(manager).upsert_rule(
            rule_type="skip_user_sync",
            match_value="alice.wecom",
            org_id="default",
            notes="default org skip",
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.department_list = [
            {"id": 1, "name": "HQ", "parentid": 0},
            {"id": 2, "name": "Asia", "parentid": 0},
        ]
        FakeWeComProgrammableAPI.department_users = {
            1: [],
            2: [{"userid": "alice.wecom", "name": "Alice Zhang"}],
        }
        FakeWeComProgrammableAPI.user_details = {
            "alice.wecom": {
                "userid": "alice.wecom",
                "name": "Alice Zhang",
                "email": "",
                "department": [2],
                "employee_id": "2001",
            }
        }
        FakeADSyncPolicy.reset()

        def fake_load_config(path):
            filename = os.path.basename(path)
            if filename == "asia.ini":
                return asia_config
            if filename == "asia-connector.ini":
                return asia_connector_config
            if filename == "default.ini":
                return default_config
            raise AssertionError(f"unexpected config path: {path}")

        patches = [
            patch.object(runtime, "load_sync_config", side_effect=fake_load_config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            result = runtime.run_sync_job(
                execution_mode="dry_run",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="default.ini",
                org_id="asia",
            )

        self.assertEqual(result["error_count"], 0)
        asia_binding = UserIdentityBindingRepository(manager).get_binding_record_by_source_user_id(
            "alice.wecom",
            org_id="asia",
        )
        default_binding = UserIdentityBindingRepository(manager).get_binding_record_by_source_user_id(
            "alice.wecom",
            org_id="default",
        )
        self.assertIsNotNone(asia_binding)
        self.assertEqual(asia_binding.ad_username, "2001")
        self.assertIsNotNone(default_binding)
        self.assertEqual(default_binding.ad_username, "legacy.default")

    def test_run_sync_job_uses_org_scoped_advanced_settings_and_mapping_rules(self):
        default_config = AppConfig(
            wecom=WeComConfig(corpid="corp-default", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.default.example.com",
                domain="default.example.com",
                username="DEFAULT\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="default.example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="default.ini",
        )
        asia_config = AppConfig(
            wecom=WeComConfig(corpid="corp-asia", corpsecret="secret", agentid="1001"),
            ldap=LDAPConfig(
                server="ldap.asia.example.com",
                domain="asia.example.com",
                username="ASIA\\administrator",
                password="password",
                use_ssl=True,
                port=636,
            ),
            domain="asia.example.com",
            account=AccountConfig(default_password="VeryStrong123!456"),
            config_path="asia.ini",
        )

        test_dir = os.path.join(os.getcwd(), "test_artifacts")
        os.makedirs(test_dir, exist_ok=True)
        db_path = os.path.join(test_dir, "runtime_org_scoped_settings_test.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        OrganizationRepository(manager).upsert_organization(
            org_id="asia",
            name="Asia Region",
            config_path="asia.ini",
            description="",
            is_enabled=True,
        )
        settings_repo = SettingsRepository(manager)
        settings_repo.set_value("attribute_mapping_enabled", "true", "bool")
        settings_repo.set_value("attribute_mapping_enabled", "false", "bool", org_id="asia")
        AttributeMappingRuleRepository(manager).upsert_rule(
            connector_id="",
            direction="source_to_ad",
            source_field="position",
            target_field="title",
            sync_mode="replace",
            notes="default mapping",
            org_id="default",
        )

        FakeWeComProgrammableAPI.reset()
        FakeWeComProgrammableAPI.user_details = {
            "alice": {
                "userid": "alice",
                "name": "Alice",
                "email": "alice@example.com",
                "department": [1],
                "position": "Platform Engineer",
            }
        }
        FakeADSyncPolicy.reset()

        def fake_load_config(path):
            filename = os.path.basename(path)
            if filename == "asia.ini":
                return asia_config
            if filename == "default.ini":
                return default_config
            raise AssertionError(f"unexpected config path: {path}")

        patches = [
            patch.object(runtime, "load_sync_config", side_effect=fake_load_config),
            patch.object(runtime, "validate_config", return_value=(True, [])),
            patch.object(runtime, "test_source_connection", return_value=(True, "ok")),
            patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")),
            patch.object(runtime, "run_config_security_self_check", return_value=[]),
            patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI),
            patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy),
            patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("test-runtime")),
            patch.object(runtime.sync_logging, "log_filename", "test-runtime.log"),
            patch.object(runtime, "_generate_skip_detail_report", return_value="skip-details.csv"),
            patch.object(runtime, "_generate_sync_operation_log", return_value="ops.csv"),
            patch.object(runtime, "_generate_sync_validation_report", return_value="validation.txt"),
        ]

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9], patches[10], patches[11]:
            result = runtime.run_sync_job(
                execution_mode="apply",
                trigger_type="unit_test",
                db_path=db_path,
                config_path="default.ini",
                org_id="asia",
            )

        self.assertEqual(result["error_count"], 0)
        self.assertEqual(len(FakeADSyncPolicy.created_users), 1)
        self.assertNotIn("title", FakeADSyncPolicy.created_users[0]["extra_attributes"])


if __name__ == "__main__":
    unittest.main()
