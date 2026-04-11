import logging
import unittest
from types import SimpleNamespace

from sync_app.core.models import (
    DepartmentAction,
    DepartmentGroupInfo,
    GroupCleanupAction,
    GroupHierarchyAction,
    GroupMembershipAction,
    GroupPolicyEvaluation,
    ManagedGroupTarget,
)
from sync_app.services.runtime_apply_phase import (
    apply_custom_group_actions,
    apply_department_actions,
    apply_final_state_updates,
    apply_group_cleanup_actions,
    apply_group_hierarchy_actions,
    apply_group_membership_actions,
)


class DummyBindingRepo:
    def __init__(self):
        self.by_department_id = {}
        self.by_group_sam = {}

    def get_binding_record_by_department_id(self, department_id):
        return self.by_department_id.get(str(department_id))

    def get_binding_record_by_group_sam(self, group_sam):
        return self.by_group_sam.get(group_sam)


class DummyStateRepo:
    def __init__(self):
        self.calls = []

    def upsert_state(self, **kwargs):
        self.calls.append(kwargs)


class DummyCustomGroupBindingRepo:
    def __init__(self):
        self.records_by_connector = {}

    def upsert_binding(self, **kwargs):
        connector_id = kwargs["connector_id"]
        source_type = kwargs["source_type"]
        source_key = kwargs["source_key"]
        record = SimpleNamespace(**kwargs)
        current_records = self.records_by_connector.setdefault(connector_id, [])
        for index, existing in enumerate(current_records):
            if existing.source_type == source_type and existing.source_key == source_key:
                current_records[index] = record
                break
        else:
            current_records.append(record)
        return record

    def list_active_records(self, connector_id):
        return [
            record
            for record in self.records_by_connector.get(connector_id, [])
            if getattr(record, "status", "") == "active"
        ]


class DummyStateManager:
    def __init__(self):
        self.cleaned_user_ids = None
        self.sync_complete = None

    def cleanup_old_users(self, source_user_ids):
        self.cleaned_user_ids = set(source_user_ids)

    def set_sync_complete(self, is_complete):
        self.sync_complete = bool(is_complete)


class FakeDirectoryProvider:
    def __init__(self):
        self.ensured_ous = []
        self.ensured_department_groups = []
        self.added_group_links = []
        self.removed_group_links = []
        self.custom_groups = []
        self.user_group_memberships = []

    def ensure_ou(self, ou_name, parent_dn):
        self.ensured_ous.append((ou_name, parent_dn))
        return True, f"OU={ou_name},DC=example,DC=com", True

    def ensure_department_group(
        self,
        *,
        department_id,
        parent_department_id,
        ou_name,
        ou_dn,
        full_path,
        display_separator,
        binding_repo,
    ):
        self.ensured_department_groups.append((department_id, parent_department_id, ou_dn))
        return DepartmentGroupInfo(
            exists=True,
            group_sam=f"wecom_d{department_id}",
            group_cn=f"{ou_name}__D{department_id}",
            group_dn=f"CN={ou_name}__D{department_id},{ou_dn}",
            display_name=display_separator.join(full_path) + f" [D{department_id}]",
            description="managed department group",
            binding_source="managed",
            created=True,
        )

    def add_group_to_group(self, child_group_dn, parent_group_dn):
        self.added_group_links.append((child_group_dn, parent_group_dn))
        return True

    def remove_group_from_group(self, child_group_dn, parent_group_dn):
        self.removed_group_links.append((child_group_dn, parent_group_dn))
        return True

    def ensure_custom_group(self, source_type, source_key, display_name):
        group_sam = f"WECOM_{source_type.upper()}_{source_key}"
        group_dn = f"CN={display_name},OU=Managed Groups,DC=example,DC=com"
        self.custom_groups.append((source_type, source_key, display_name, group_sam, group_dn))
        return DepartmentGroupInfo(
            exists=True,
            group_sam=group_sam,
            group_cn=display_name,
            group_dn=group_dn,
            display_name=display_name,
            description="managed custom group",
            binding_source="managed",
            created=True,
        )

    def add_user_to_group(self, username, group_name):
        self.user_group_memberships.append((username, group_name))
        return True


def build_ctx(*, binding_repo=None, custom_group_binding_repo=None, state_repo=None, state_manager=None):
    operations = []
    events = []
    stats_events = []

    hooks = SimpleNamespace(
        record_operation=lambda **kwargs: operations.append(kwargs),
        record_event=lambda level, event_type, message, stage_name=None, payload=None: events.append(
            {
                "level": level,
                "event_type": event_type,
                "message": message,
                "stage_name": stage_name,
                "payload": payload,
            }
        ),
        evaluate_group_policy=lambda **kwargs: GroupPolicyEvaluation(),
        has_exception_rule=lambda *args, **kwargs: False,
        stats_callback=lambda name, value: stats_events.append((name, value)),
        is_cancelled=lambda: False,
    )
    sync_stats = {
        "operations": {
            "departments_created": 0,
            "departments_existed": 0,
            "groups_nested": 0,
            "groups_assigned": 0,
            "group_relations_removed": 0,
        },
        "errors": {
            "department_errors": [],
            "group_hierarchy_errors": [],
            "group_add_errors": [],
            "group_relation_cleanup_errors": [],
        },
        "error_count": 0,
        "executed_operation_count": 0,
    }
    ctx = SimpleNamespace(
        actions=SimpleNamespace(
            department_actions=[],
            custom_group_actions=[],
            membership_actions=[],
            group_hierarchy_actions=[],
            group_cleanup_actions=[],
        ),
        environment=SimpleNamespace(
            department_group_targets={},
        ),
        repositories=SimpleNamespace(
            binding_repo=binding_repo or DummyBindingRepo(),
            custom_group_binding_repo=custom_group_binding_repo or DummyCustomGroupBindingRepo(),
            state_repo=state_repo or DummyStateRepo(),
            state_manager=state_manager or DummyStateManager(),
        ),
        hooks=hooks,
        sync_stats=sync_stats,
        executed_count=0,
        job_id="job-123",
        working=SimpleNamespace(source_user_ids={"alice"}),
        policy_settings=SimpleNamespace(
            group_recursive_enabled=True,
            managed_relation_cleanup_enabled=True,
        ),
        logger=logging.getLogger("runtime-apply-phase-test"),
    )
    return ctx, operations, events, stats_events


class RuntimeApplyPhaseTests(unittest.TestCase):
    def test_apply_department_actions_updates_department_state_and_progress(self):
        provider = FakeDirectoryProvider()
        state_repo = DummyStateRepo()
        ctx, operations, _, stats_events = build_ctx(state_repo=state_repo)
        group_target = ManagedGroupTarget(
            exists=False,
            group_sam="wecom_d1",
            group_cn="HQ__D1",
            group_dn="CN=HQ__D1,OU=HQ,DC=example,DC=com",
            display_name="HQ [D1]",
            description="planned",
            binding_source="inspect",
            created=False,
            binding_exists=False,
            department_id=1,
            parent_department_id=None,
            ou_name="HQ",
            ou_dn="OU=HQ,DC=example,DC=com",
            full_path=["HQ"],
            policy=GroupPolicyEvaluation(),
        )
        ctx.actions.department_actions.append(
            DepartmentAction(
                connector_id="default",
                department_id=1,
                parent_department_id=None,
                ou_name="HQ",
                parent_dn="DC=example,DC=com",
                ou_dn="OU=HQ,DC=example,DC=com",
                full_path=["HQ"],
                group_target=group_target,
                should_manage_group=True,
            )
        )

        apply_department_actions(
            ctx,
            get_ad_sync=lambda connector_id: provider,
            display_separator=" / ",
            record_group_policy_skip=lambda *args, **kwargs: None,
        )

        self.assertEqual(provider.ensured_ous, [("HQ", "DC=example,DC=com")])
        self.assertEqual(provider.ensured_department_groups, [(1, None, "OU=HQ,DC=example,DC=com")])
        self.assertEqual(len(state_repo.calls), 1)
        self.assertEqual(ctx.environment.department_group_targets[1].group_sam, "wecom_d1")
        self.assertEqual(ctx.sync_stats["operations"]["departments_created"], 1)
        self.assertEqual(ctx.executed_count, 1)
        self.assertEqual(len(operations), 1)
        self.assertIn(("department_sync_done", True), stats_events)

    def test_apply_group_hierarchy_and_cleanup_actions_manage_relations(self):
        provider = FakeDirectoryProvider()
        binding_repo = DummyBindingRepo()
        binding_repo.by_department_id["1"] = SimpleNamespace(
            status="active",
            group_sam="child_new",
            group_dn="CN=ChildNew,OU=Managed,DC=example,DC=com",
            display_name="Child New",
        )
        binding_repo.by_department_id["10"] = SimpleNamespace(
            status="active",
            group_sam="parent_new",
            group_dn="CN=ParentNew,OU=Managed,DC=example,DC=com",
            display_name="Parent New",
        )
        binding_repo.by_group_sam["stale_parent"] = SimpleNamespace(
            status="active",
            group_sam="stale_parent",
            group_dn="CN=StaleParent,OU=Managed,DC=example,DC=com",
            display_name="Stale Parent",
        )
        ctx, _, _, _ = build_ctx(binding_repo=binding_repo)
        ctx.actions.group_hierarchy_actions.append(
            GroupHierarchyAction(
                connector_id="default",
                child_department_id=1,
                parent_department_id=10,
                child_group_sam="child_old",
                child_group_dn="CN=ChildOld,OU=Managed,DC=example,DC=com",
                child_display_name="Child Old",
                parent_group_sam="parent_old",
                parent_group_dn="CN=ParentOld,OU=Managed,DC=example,DC=com",
                parent_display_name="Parent Old",
            )
        )
        ctx.actions.group_cleanup_actions.append(
            GroupCleanupAction(
                connector_id="default",
                child_department_id=1,
                child_group_sam="child_old",
                child_group_dn="CN=ChildOld,OU=Managed,DC=example,DC=com",
                parent_group_sam="stale_parent",
                parent_group_dn="CN=StaleParent,OU=Managed,DC=example,DC=com",
                expected_parent_group_sam="parent_new",
            )
        )

        successful_pairs = apply_group_hierarchy_actions(
            ctx,
            get_ad_sync=lambda connector_id: provider,
            record_group_policy_skip=lambda *args, **kwargs: None,
        )
        apply_group_cleanup_actions(
            ctx,
            get_ad_sync=lambda connector_id: provider,
            planned_hierarchy_pairs={("default", "child_new", "parent_new")},
            successful_hierarchy_pairs=successful_pairs,
            record_exception_skip=lambda **kwargs: None,
            record_group_policy_skip=lambda *args, **kwargs: None,
            record_skip_detail=lambda **kwargs: None,
        )

        self.assertEqual(
            provider.added_group_links,
            [("CN=ChildNew,OU=Managed,DC=example,DC=com", "CN=ParentNew,OU=Managed,DC=example,DC=com")],
        )
        self.assertEqual(
            provider.removed_group_links,
            [("CN=ChildNew,OU=Managed,DC=example,DC=com", "CN=StaleParent,OU=Managed,DC=example,DC=com")],
        )
        self.assertEqual(ctx.sync_stats["operations"]["groups_nested"], 1)
        self.assertEqual(ctx.sync_stats["operations"]["group_relations_removed"], 1)
        self.assertEqual(ctx.executed_count, 2)

    def test_apply_custom_group_and_membership_actions_resolve_binding_dn(self):
        provider = FakeDirectoryProvider()
        custom_group_binding_repo = DummyCustomGroupBindingRepo()
        ctx, operations, _, _ = build_ctx(custom_group_binding_repo=custom_group_binding_repo)
        ctx.actions.custom_group_actions.append(
            {
                "connector_id": "default",
                "source_type": "tag",
                "source_key": "1001",
                "display_name": "IT Admins",
            }
        )
        membership_action = GroupMembershipAction(
            connector_id="default",
            source_user_id="alice",
            username="alice",
            group_sam="WECOM_TAG_1001",
            group_dn="",
            group_display_name="IT Admins",
            department_id=1,
        )
        ctx.actions.membership_actions.append(membership_action)

        apply_custom_group_actions(
            ctx,
            get_ad_sync=lambda connector_id: provider,
        )
        apply_group_membership_actions(
            ctx,
            get_ad_sync=lambda connector_id: provider,
            record_exception_skip=lambda **kwargs: None,
            record_group_policy_skip=lambda *args, **kwargs: None,
        )

        self.assertEqual(provider.custom_groups[0][:3], ("tag", "1001", "IT Admins"))
        self.assertEqual(membership_action.group_dn, "CN=IT Admins,OU=Managed Groups,DC=example,DC=com")
        self.assertEqual(provider.user_group_memberships, [("alice", "WECOM_TAG_1001")])
        self.assertEqual(ctx.sync_stats["operations"]["groups_assigned"], 1)
        self.assertEqual(ctx.executed_count, 1)
        self.assertEqual(len(operations), 2)

    def test_apply_final_state_updates_marks_sync_completion(self):
        state_manager = DummyStateManager()
        ctx, _, _, _ = build_ctx(state_manager=state_manager)
        ctx.sync_stats["error_count"] = 1

        apply_final_state_updates(ctx)

        self.assertEqual(state_manager.cleaned_user_ids, {"alice"})
        self.assertFalse(state_manager.sync_complete)


if __name__ == "__main__":
    unittest.main()
