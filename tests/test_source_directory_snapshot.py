import os
import unittest
from types import SimpleNamespace

from sync_app.core.models import DepartmentNode, SourceDirectoryUser
from sync_app.services.source_directory import SourceDirectoryService
from sync_app.services.runtime_source_phase import collect_source_user_departments
from sync_app.providers.source.wecom import WeComSourceProvider
from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories import SourceDirectoryRepository


class _Provider:
    provider_id = "wecom"
    def __init__(self, *, fail=False):
        self.fail = fail
    def list_departments(self):
        if self.fail:
            raise RuntimeError("credential rejected")
        return [DepartmentNode(1, "Root", 0), DepartmentNode(2, "R&D", 1)]
    def list_department_users(self, department_id):
        if department_id != 2:
            return []
        return [
            SourceDirectoryUser.from_source_payload({"userid": "alice", "name": "Alice", "job_number": "E001", "email": "alice@example.com", "mobile": "13800138000", "position": "Engineer", "department": [2]}),
            SourceDirectoryUser.from_source_payload({"userid": "bob", "name": "Bob", "staff_no": "E001", "department": [2]}),
            SourceDirectoryUser.from_source_payload({"userid": "carol", "name": "Carol", "department": [2]}),
        ]
    def get_user_detail(self, user_id):
        return {"userid": user_id, "custom_badge": f"badge-{user_id}", "department": [2]}


class SourceDirectorySnapshotTests(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join("test_artifacts", "source_directory_snapshot.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.path + suffix)
            except FileNotFoundError:
                pass
        self.manager = DatabaseManager(db_path=self.path)
        self.manager.initialize(create_startup_snapshot=False, verify_integrity=True)
        self.repo = SourceDirectoryRepository(self.manager)
        self.service = SourceDirectoryService(self.repo)

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.path + suffix)
            except FileNotFoundError:
                pass

    def test_refresh_pagination_quality_masking_and_org_isolation(self):
        snapshot = self.service.refresh(org_id="default", provider_id="wecom", provider=_Provider())
        self.assertEqual(snapshot["user_count"], 3)
        self.assertEqual(snapshot["missing_employee_id_count"], 1)
        self.assertEqual(snapshot["duplicate_employee_id_count"], 2)
        page = self.repo.list_users(snapshot["id"], org_id="default", provider_id="wecom", limit=2)
        self.assertEqual(page["total"], 3)
        self.assertEqual(len(page["items"]), 2)
        alice = next(item for item in page["items"] if item["source_user_id"] == "alice")
        self.assertEqual(alice["employee_id"], "E001")
        self.assertEqual(alice["mobile_masked"], "138****8000")
        self.assertNotIn("mobile", alice["raw_payload"])
        other_org = self.repo.list_users(snapshot["id"], org_id="other", provider_id="wecom")
        self.assertEqual(other_org["total"], 0)
        fields = self.repo.list_field_catalog(snapshot["id"], org_id="default")
        fields_by_name = {item["field_name"]: item for item in fields}
        self.assertIn("custom_badge", fields_by_name)
        self.assertEqual(fields_by_name["job_number"]["field_label"], "Employee ID")
        self.assertEqual(fields_by_name["staff_no"]["field_label"], "Employee ID")
        self.assertEqual(fields_by_name["userid"]["field_label"], "Platform User ID")
        self.assertEqual(fields_by_name["custom_badge"]["field_label"], "Custom Badge")

    def test_failed_refresh_retains_last_successful_snapshot(self):
        first = self.service.refresh(org_id="default", provider_id="wecom", provider=_Provider())
        with self.assertRaisesRegex(RuntimeError, "credential rejected"):
            self.service.refresh(org_id="default", provider_id="wecom", provider=_Provider(fail=True))
        latest_success = self.repo.get_latest_successful_snapshot(org_id="default", provider_id="wecom")
        latest_attempt = self.repo.get_latest_refresh(org_id="default", provider_id="wecom")
        self.assertEqual(latest_success["id"], first["id"])
        self.assertEqual(latest_attempt["status"], "failed")

    def test_snapshot_history_is_paginated_filtered_and_organization_scoped(self):
        first = self.service.refresh(
            org_id="default", provider_id="wecom", provider=_Provider()
        )
        second = self.repo.start_refresh(
            org_id="default", provider_id="wecom", created_by="tester"
        )
        self.repo.fail_refresh(second, "simulated failure")
        other = self.repo.start_refresh(
            org_id="other", provider_id="wecom", created_by="tester"
        )
        self.repo.fail_refresh(other, "other organization failure")

        page = self.repo.list_snapshots(org_id="default", limit=1)
        failed = self.repo.list_snapshots(
            org_id="default", provider_id="wecom", status="failed"
        )

        self.assertEqual(page["total"], 2)
        self.assertEqual(len(page["items"]), 1)
        self.assertEqual(page["items"][0]["id"], second)
        self.assertEqual(failed["total"], 1)
        self.assertEqual(failed["items"][0]["id"], second)
        self.assertEqual(
            self.repo.get_snapshot(first["id"], org_id="other"),
            None,
        )

    def test_scope_fingerprint_and_custom_field_mapping(self):
        snapshot = self.service.refresh(org_id="default", provider_id="wecom", provider=_Provider())
        selection = self.repo.save_scope_selection(
            org_id="default", provider_id="wecom", scope_type="selected_users",
            selected_source_user_ids=["alice"], source_field="employee_id",
            username_strategy="employee_id", requested_by="admin",
        )
        self.assertEqual(selection["snapshot_id"], snapshot["id"])
        row = self.repo.list_users(snapshot["id"], org_id="default", provider_id="wecom", source_user_ids=["alice"])["items"][0]
        employee_preview = self.service.preview_username(row, username_strategy="employee_id", source_field="employee_id")
        custom_preview = self.service.preview_username(row, username_strategy="custom_template", source_field="custom_badge")
        self.assertEqual(employee_preview["username"], "E001")
        self.assertEqual(custom_preview["username"], "badge-alice")
        quality = self.service.build_mapping_quality_report(
            snapshot_id=snapshot["id"], org_id="default", provider_id="wecom",
            username_strategy="employee_id", source_field="employee_id",
        )
        self.assertEqual(quality["duplicate_employee_id_count"], 2)
        self.assertEqual(quality["normalized_username_collision_count"], 2)
        self.assertIn("duplicate_employee_id", quality["issues_by_user"]["alice"])
        first_selection_fingerprint = selection["selection_fingerprint"]
        second_snapshot = self.service.refresh(
            org_id="default", provider_id="wecom", provider=_Provider(), created_by="admin"
        )
        rebound = self.repo.get_scope_selection(org_id="default", provider_id="wecom")
        self.assertEqual(rebound["snapshot_id"], second_snapshot["id"])
        self.assertNotEqual(rebound["selection_fingerprint"], first_selection_fingerprint)

    def test_mapping_preview_reports_illegal_character_removal_and_truncation(self):
        row = {
            "source_user_id": "alice", "display_name": "Alice", "employee_id": "EMP/INVALID-12345678901234567890",
            "email": "", "position": "", "department_ids": ["2"], "raw_payload": {},
        }
        preview = self.service.preview_username(row, username_strategy="employee_id", source_field="employee_id")
        self.assertLessEqual(len(preview["username"]), 20)
        self.assertIn("illegal_characters_removed", preview["risks"])
        self.assertIn("username_truncated", preview["risks"])

    def test_mapping_preview_uses_unified_strategies_and_stable_collision_candidates(self):
        row = {
            "source_user_id": "ding/user-001",
            "display_name": "张三",
            "employee_id": "E001",
            "email": "Alice.Team@example.com",
            "position": "Engineer",
            "department_ids": ["2"],
            "raw_payload": {"custom_badge": "Badge-77"},
        }
        expectations = {
            "source_user_id": "dinguser-001",
            "employee_id": "E001",
            "email_localpart": "Alice.Team",
            "pinyin_initials_employee_id": "zsE001",
            "pinyin_full_employee_id": "zhangsanE001",
            "family_name_pinyin_given_initials": "zhangs",
            "family_name_pinyin_given_name_pinyin": "zhangsan",
        }
        for source_field, expected in expectations.items():
            with self.subTest(source_field=source_field):
                preview = self.service.preview_username(
                    row,
                    username_strategy=source_field,
                    source_field=source_field,
                )
                self.assertEqual(preview["username"], expected)
                self.assertEqual(preview["candidate_mapping"]["ad_username"], expected)
                self.assertEqual(
                    preview["mapping_input"]["method"],
                    "userid" if source_field == "source_user_id" else source_field,
                )
                self.assertTrue(preview["mapping_input"]["value"])

        custom_field = self.service.preview_username(
            row,
            username_strategy="custom_template",
            source_field="custom_badge",
        )
        custom_template = self.service.preview_username(
            row,
            username_strategy="custom_template",
            username_template="{family_name_pinyin}.{given_initials}-{employee_id}",
            source_field="custom_template",
        )
        self.assertEqual(custom_field["username"], "Badge-77")
        self.assertEqual(custom_field["mapping_input"]["value"], "Badge-77")
        self.assertEqual(custom_template["username"], "zhang.s-E001")

        sensitive_custom_field = self.service.preview_username(
            {
                **row,
                "raw_payload": {**row["raw_payload"], "custom_secret": "private-value"},
            },
            username_strategy="custom_template",
            source_field="custom_secret",
        )
        self.assertNotEqual(
            sensitive_custom_field["mapping_input"]["value"], "private-value"
        )

        first_hash = self.service.preview_username(
            row,
            username_strategy="employee_id",
            source_field="employee_id",
            username_collision_policy="append_hash",
        )
        second_hash = self.service.preview_username(
            row,
            username_strategy="employee_id",
            source_field="employee_id",
            username_collision_policy="append_hash",
        )
        self.assertEqual(first_hash["candidates"], second_hash["candidates"])
        self.assertTrue(
            any(
                item["rule"] == "managed_username_hash_suffix"
                for item in first_hash["candidates"]
            )
        )

        missing = self.service.preview_username(
            {**row, "employee_id": ""},
            username_strategy="employee_id",
            source_field="employee_id",
        )
        self.assertEqual(missing["username"], "")
        self.assertIn("mapping_field_missing", missing["risks"])

    def test_wecom_extended_employee_id_auto_detection_and_configured_name(self):
        class FakeAPI:
            def __init__(self, *_args, **_kwargs):
                pass
            def get_department_list(self):
                return []
            def get_department_users(self, _department_id):
                return []
            def get_user_detail(self, _user_id):
                return {}
        provider = WeComSourceProvider("corp", "secret", api_factory=FakeAPI)
        automatic = provider.normalize_user({"userid": "a", "name": "A", "extattr": {"attrs": [{"name": "工号", "value": "E100"}]}})
        provider.employee_id_attribute = "Badge Number"
        configured = provider.normalize_user({"userid": "b", "name": "B", "extattr": {"attrs": [{"name": "Badge Number", "value": "E200"}]}})
        self.assertEqual(automatic.employee_id, "E100")
        self.assertEqual(configured.employee_id, "E200")

    def test_department_scope_includes_descendants_and_excludes_other_departments(self):
        class Provider:
            provider_id = "wecom"
            def list_departments(self):
                return [DepartmentNode(1, "Root", 0), DepartmentNode(2, "R&D", 1), DepartmentNode(4, "Platform", 2), DepartmentNode(3, "Sales", 1)]
            def list_department_users(self, department_id):
                rows = {
                    4: [{"userid": "alice", "name": "Alice", "department": [4]}],
                    3: [{"userid": "bob", "name": "Bob", "department": [3]}],
                }
                return [SourceDirectoryUser.from_source_payload(row) for row in rows.get(department_id, [])]
            def get_user_detail(self, user_id):
                return {"userid": user_id, "department": [4 if user_id == "alice" else 3]}
            def normalize_user(self, payload):
                return SourceDirectoryUser.from_source_payload(payload)
        snapshot = self.service.refresh(org_id="default", provider_id="wecom", provider=Provider())
        scope = self.repo.save_scope_selection(
            org_id="default", provider_id="wecom", scope_type="department",
            selected_department_ids=["2"], snapshot_id=snapshot["id"],
        )
        departments = Provider().list_departments()
        dept_tree = {item.department_id: item for item in departments}
        ctx = SimpleNamespace(
            environment=SimpleNamespace(source_scope=scope, dept_tree=dept_tree, source_provider=Provider()),
            identity=SimpleNamespace(user_departments={}),
            working=SimpleNamespace(source_user_ids=set()),
            repositories=SimpleNamespace(source_directory_repo=self.repo),
            organization=SimpleNamespace(org_id="default"),
            hooks=SimpleNamespace(is_cancelled=lambda: False, stats_callback=None),
            sync_stats={},
        )
        users = collect_source_user_departments(ctx)
        self.assertEqual(set(users), {"alice"})


if __name__ == "__main__":
    unittest.main()
