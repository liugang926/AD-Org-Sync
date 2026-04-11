from __future__ import annotations

from typing import Any, Callable, Optional

from sync_app.core.models import (
    DepartmentAction,
    DepartmentGroupInfo,
    DepartmentNode,
    GroupCleanupAction,
    GroupHierarchyAction,
    GroupMembershipAction,
    ManagedGroupTarget,
)
from sync_app.services.ad_sync import build_custom_group_sam, build_group_cn, build_group_display_name
from sync_app.services.runtime_context import SyncContext


def get_department_group_target(
    ctx: SyncContext,
    dept_info: DepartmentNode,
    *,
    get_connector_id_for_department: Callable[[Optional[DepartmentNode]], str],
    get_ad_sync: Callable[[str], Any],
    display_separator: str,
) -> ManagedGroupTarget:
    dept_id = dept_info.department_id
    department_group_targets = ctx.environment.department_group_targets
    if dept_id in department_group_targets:
        return department_group_targets[dept_id]

    connector_id = get_connector_id_for_department(dept_info)
    connector_ad_sync = get_ad_sync(connector_id)
    ou_dn = connector_ad_sync.get_ou_dn(dept_info.path)
    binding = ctx.repositories.binding_repo.get_binding_record_by_department_id(str(dept_id))
    if binding and binding.status != "active":
        binding = None

    if binding and binding.get("group_sam"):
        group_cn = binding.get("group_cn") or build_group_cn(dept_info.name, dept_id)
        group_dn = binding.get("group_dn") or f"CN={group_cn},{ou_dn}"
        display_name = binding.get("display_name") or build_group_display_name(
            dept_info.path,
            dept_id,
            display_separator,
        )
        group_info = DepartmentGroupInfo(
            exists=True,
            group_sam=binding.group_sam,
            group_cn=group_cn,
            group_dn=group_dn,
            display_name=display_name,
            description=(
                f"source={getattr(ctx.config, 'source_provider', 'wecom')}; "
                f"dept_id={dept_id}; path={'/'.join(dept_info.path)}"
            ),
            binding_source="binding",
            created=False,
        )
        binding_exists = True
    else:
        group_info = connector_ad_sync.inspect_department_group(
            department_id=dept_id,
            ou_name=dept_info.name,
            ou_dn=ou_dn,
            full_path=dept_info.path,
            display_separator=display_separator,
        )
        binding_exists = False

    target = ManagedGroupTarget(
        exists=bool(group_info.exists),
        group_sam=group_info.group_sam,
        group_cn=group_info.group_cn,
        group_dn=group_info.group_dn,
        display_name=group_info.display_name,
        description=group_info.description,
        binding_source=group_info.binding_source,
        created=bool(group_info.created),
        binding_exists=binding_exists,
        department_id=dept_id,
        parent_department_id=dept_info.parent_id if dept_info.parent_id in ctx.environment.dept_tree else None,
        ou_name=dept_info.name,
        ou_dn=ou_dn,
        full_path=list(dept_info.path),
        policy=ctx.hooks.evaluate_group_policy(
            group_sam=group_info.group_sam,
            group_dn=group_info.group_dn,
            display_name=group_info.display_name,
        ),
    )
    department_group_targets[dept_id] = target
    return target


def get_effective_parent_department_id(
    ctx: SyncContext,
    dept_info: DepartmentNode,
    *,
    is_department_excluded: Callable[[Optional[DepartmentNode]], bool],
) -> Optional[int]:
    dept_id = dept_info.department_id
    effective_parent_cache = ctx.environment.effective_parent_cache
    if dept_id in effective_parent_cache:
        return effective_parent_cache[dept_id]

    parent_id = dept_info.parent_id
    while parent_id and parent_id in ctx.environment.dept_tree:
        parent_dept = ctx.environment.dept_tree[parent_id]
        if not is_department_excluded(parent_dept):
            effective_parent_cache[dept_id] = parent_id
            return parent_id
        parent_id = parent_dept.parent_id

    effective_parent_cache[dept_id] = None
    return None


def get_current_parent_groups(
    ctx: SyncContext,
    member_dn: Optional[str],
    *,
    connector_id: str,
    get_ad_sync: Callable[[str], Any],
) -> list[Any]:
    if not member_dn:
        return []
    cache_key = f"{connector_id}:{member_dn}"
    current_parent_groups_cache = ctx.environment.current_parent_groups_cache
    if cache_key not in current_parent_groups_cache:
        current_parent_groups_cache[cache_key] = get_ad_sync(connector_id).find_parent_groups_for_member(member_dn)
    return current_parent_groups_cache[cache_key]


def _add_custom_group_memberships(
    ctx: SyncContext,
    *,
    connector_id: str,
    group_sam: str,
    display_name: str,
    group_source_type: str,
    members: list[dict[str, Any]],
    planned_memberships: set[tuple[str, str, str]],
) -> None:
    for member in members or []:
        userid = str(member.get("userid") or "").strip()
        if (
            not userid
            or userid in ctx.identity.exception_skipped_userids
            or userid in ctx.identity.disabled_bound_userids
        ):
            continue
        if ctx.identity.user_connector_id_by_userid.get(userid, "default") != connector_id:
            continue
        username = ctx.identity.active_user_bindings.get(userid)
        if not username:
            continue

        membership_key = (connector_id, username, group_sam)
        if membership_key in planned_memberships:
            continue

        planned_memberships.add(membership_key)
        ctx.actions.membership_actions.append(
            GroupMembershipAction(
                connector_id=connector_id,
                source_user_id=userid,
                username=username,
                group_sam=group_sam,
                group_dn="",
                group_display_name=display_name,
                department_id=0,
            )
        )
        ctx.hooks.add_planned_operation(
            object_type="group_membership",
            operation_type="add_user_to_group",
            source_id=userid,
            target_dn="",
            desired_state={
                "connector_id": connector_id,
                "ad_username": username,
                "group_sam": group_sam,
                "display_name": display_name,
                "group_source_type": group_source_type,
            },
        )


def plan_directory_and_custom_groups(
    ctx: SyncContext,
    *,
    is_department_excluded: Callable[[Optional[DepartmentNode]], bool],
    get_connector_id_for_department: Callable[[Optional[DepartmentNode]], str],
    get_ad_sync: Callable[[str], Any],
    record_group_policy_skip: Callable[[str, str, ManagedGroupTarget, str], None],
    display_separator: str,
) -> set[tuple[str, str, str]]:
    processed_department_nodes: set[int] = set()
    planned_memberships: set[tuple[str, str, str]] = set()
    dept_tree = ctx.environment.dept_tree
    department_actions = ctx.actions.department_actions
    custom_group_actions = ctx.actions.custom_group_actions
    source_provider = ctx.environment.source_provider
    source_provider_name = ctx.environment.source_provider_name

    for dept_id, dept_info in dept_tree.items():
        if ctx.hooks.is_cancelled():
            raise InterruptedError("sync cancelled by user")
        if is_department_excluded(dept_info):
            continue

        for idx, ancestor_id in enumerate(dept_info.path_ids):
            if ancestor_id in processed_department_nodes:
                continue

            processed_department_nodes.add(ancestor_id)
            ancestor = dept_tree.get(ancestor_id)
            if not ancestor:
                continue

            current_path = dept_info.path[: idx + 1]
            connector_id = get_connector_id_for_department(ancestor)
            connector_ad_sync = get_ad_sync(connector_id)
            parent_dn = connector_ad_sync.get_ou_dn([]) if idx == 0 else connector_ad_sync.get_ou_dn(current_path[:-1])
            ou_dn = connector_ad_sync.get_ou_dn(current_path)
            ou_exists = connector_ad_sync.ou_exists(ou_dn)
            group_target = get_department_group_target(
                ctx,
                ancestor,
                get_connector_id_for_department=get_connector_id_for_department,
                get_ad_sync=get_ad_sync,
                display_separator=display_separator,
            )
            should_manage_group = not group_target.policy.is_excluded

            if group_target.policy.is_excluded:
                record_group_policy_skip(
                    "plan",
                    "department_group_management",
                    group_target,
                    f"skip managed group for department {ancestor.name}",
                )

            if (not ou_exists) or (
                should_manage_group and ((not group_target.exists) or (not group_target.binding_exists))
            ):
                department_actions.append(
                    DepartmentAction(
                        connector_id=connector_id,
                        department_id=ancestor_id,
                        parent_department_id=ancestor.parent_id if ancestor.parent_id in dept_tree else None,
                        ou_name=ancestor.name,
                        parent_dn=parent_dn,
                        ou_dn=ou_dn,
                        full_path=list(current_path),
                        group_target=group_target,
                        should_manage_group=should_manage_group,
                    )
                )
                ctx.hooks.add_planned_operation(
                    object_type="department",
                    operation_type="ensure_department_node",
                    source_id=str(ancestor_id),
                    department_id=str(ancestor_id),
                    target_dn=ou_dn,
                    desired_state={
                        "path": current_path,
                        "group_sam": group_target.group_sam,
                        "group_dn": group_target.group_dn,
                        "group_management": "managed" if should_manage_group else "skipped_by_policy",
                    },
                )

    if not ctx.policy_settings.custom_group_sync_enabled:
        return planned_memberships

    for connector_spec in ctx.environment.connector_specs:
        connector_id = connector_spec["connector_id"]
        managed_tag_ids = connector_spec.get("managed_tag_ids") or []
        managed_external_chat_ids = connector_spec.get("managed_external_chat_ids") or []
        tag_index: dict[str, dict[str, Any]] = {}
        if managed_tag_ids:
            try:
                tag_index = {
                    str(item.get("tagid") or item.get("id") or ""): item
                    for item in source_provider.list_tag_records()
                    if str(item.get("tagid") or item.get("id") or "").strip()
                }
            except Exception as tag_error:
                ctx.hooks.record_event(
                    "WARNING",
                    "tag_group_fetch_failed",
                    f"failed to load {source_provider_name} tag definitions for connector {connector_id}: {tag_error}",
                    stage_name="plan",
                )

        for tag_id in managed_tag_ids:
            tag_id_text = str(tag_id or "").strip()
            if not tag_id_text:
                continue
            try:
                tag_membership = source_provider.get_tag_users(tag_id_text)
            except Exception as tag_error:
                ctx.hooks.record_event(
                    "WARNING",
                    "tag_group_fetch_failed",
                    f"failed to load {source_provider_name} tag {tag_id_text}: {tag_error}",
                    stage_name="plan",
                )
                continue
            display_name = (
                str(tag_index.get(tag_id_text, {}).get("tagname") or "").strip()
                or str(tag_membership.get("tagname") or "").strip()
                or f"{source_provider_name} Tag {tag_id_text}"
            )
            group_sam = build_custom_group_sam("tag", tag_id_text)
            group_policy = ctx.hooks.evaluate_group_policy(group_sam=group_sam, display_name=display_name)
            if group_policy.is_excluded:
                continue
            custom_group_actions.append(
                {
                    "connector_id": connector_id,
                    "source_type": "tag",
                    "source_key": tag_id_text,
                    "display_name": display_name,
                }
            )
            ctx.hooks.add_planned_operation(
                object_type="custom_group",
                operation_type="ensure_custom_group",
                source_id=f"tag:{tag_id_text}",
                target_dn="",
                desired_state={
                    "connector_id": connector_id,
                    "source_type": "tag",
                    "source_key": tag_id_text,
                    "display_name": display_name,
                },
            )
            _add_custom_group_memberships(
                ctx,
                connector_id=connector_id,
                group_sam=group_sam,
                display_name=display_name,
                group_source_type="tag",
                members=tag_membership.get("userlist", []) or [],
                planned_memberships=planned_memberships,
            )

        for chat_id in managed_external_chat_ids:
            chat_id_text = str(chat_id or "").strip()
            if not chat_id_text:
                continue
            try:
                chat_info = source_provider.get_external_group_chat(chat_id_text)
            except Exception as chat_error:
                ctx.hooks.record_event(
                    "WARNING",
                    "external_group_fetch_failed",
                    f"failed to load {source_provider_name} external chat {chat_id_text}: {chat_error}",
                    stage_name="plan",
                )
                continue
            display_name = (
                str(chat_info.get("name") or "").strip()
                or f"{source_provider_name} External Chat {chat_id_text}"
            )
            group_sam = build_custom_group_sam("external_chat", chat_id_text)
            group_policy = ctx.hooks.evaluate_group_policy(group_sam=group_sam, display_name=display_name)
            if group_policy.is_excluded:
                continue
            custom_group_actions.append(
                {
                    "connector_id": connector_id,
                    "source_type": "external_chat",
                    "source_key": chat_id_text,
                    "display_name": display_name,
                }
            )
            ctx.hooks.add_planned_operation(
                object_type="custom_group",
                operation_type="ensure_custom_group",
                source_id=f"external_chat:{chat_id_text}",
                target_dn="",
                desired_state={
                    "connector_id": connector_id,
                    "source_type": "external_chat",
                    "source_key": chat_id_text,
                    "display_name": display_name,
                },
            )
            _add_custom_group_memberships(
                ctx,
                connector_id=connector_id,
                group_sam=group_sam,
                display_name=display_name,
                group_source_type="external_chat",
                members=chat_info.get("member_list", []) or [],
                planned_memberships=planned_memberships,
            )

    return planned_memberships


def plan_group_relationship_cleanup(
    ctx: SyncContext,
    *,
    is_department_excluded: Callable[[Optional[DepartmentNode]], bool],
    get_connector_id_for_department: Callable[[Optional[DepartmentNode]], str],
    get_ad_sync: Callable[[str], Any],
    record_group_policy_skip: Callable[[str, str, ManagedGroupTarget, str], None],
    record_skip_detail: Callable[..., None],
    record_exception_skip: Callable[..., None],
    display_separator: str,
) -> set[tuple[str, str, str]]:
    if not ctx.policy_settings.group_recursive_enabled:
        return set()

    dept_tree = ctx.environment.dept_tree
    planned_hierarchy_pairs: set[tuple[str, str, str]] = set()

    for dept_id, dept_info in dept_tree.items():
        if ctx.hooks.is_cancelled():
            raise InterruptedError("sync cancelled by user")
        if is_department_excluded(dept_info):
            continue

        parent_department_id = get_effective_parent_department_id(
            ctx,
            dept_info,
            is_department_excluded=is_department_excluded,
        )
        if not parent_department_id:
            continue

        child_target = get_department_group_target(
            ctx,
            dept_info,
            get_connector_id_for_department=get_connector_id_for_department,
            get_ad_sync=get_ad_sync,
            display_separator=display_separator,
        )
        parent_target = get_department_group_target(
            ctx,
            dept_tree[parent_department_id],
            get_connector_id_for_department=get_connector_id_for_department,
            get_ad_sync=get_ad_sync,
            display_separator=display_separator,
        )
        if child_target.policy.is_excluded:
            record_group_policy_skip(
                "plan",
                "group_hierarchy_child",
                child_target,
                f"skip recursive child group {child_target.group_sam}",
            )
            continue
        if parent_target.policy.is_excluded:
            record_group_policy_skip(
                "plan",
                "group_hierarchy_parent",
                parent_target,
                f"skip recursive parent group {parent_target.group_sam}",
            )
            continue

        child_group_sam = child_target.group_sam
        parent_group_sam = parent_target.group_sam
        if not child_group_sam or not parent_group_sam or child_group_sam == parent_group_sam:
            continue
        connector_id = get_connector_id_for_department(dept_info)
        if connector_id != get_connector_id_for_department(dept_tree[parent_department_id]):
            continue

        current_parent_sams = {
            entry.group_sam
            for entry in get_current_parent_groups(
                ctx,
                child_target.group_dn,
                connector_id=connector_id,
                get_ad_sync=get_ad_sync,
            )
            if entry.group_sam
        }
        if parent_group_sam in current_parent_sams:
            continue

        hierarchy_key = (connector_id, child_group_sam, parent_group_sam)
        if hierarchy_key in planned_hierarchy_pairs:
            continue

        planned_hierarchy_pairs.add(hierarchy_key)
        ctx.actions.group_hierarchy_actions.append(
            GroupHierarchyAction(
                connector_id=connector_id,
                child_department_id=dept_id,
                parent_department_id=parent_department_id,
                child_group_sam=child_group_sam,
                child_group_dn=child_target.group_dn,
                child_display_name=child_target.display_name,
                parent_group_sam=parent_group_sam,
                parent_group_dn=parent_target.group_dn,
                parent_display_name=parent_target.display_name,
            )
        )
        ctx.hooks.add_planned_operation(
            object_type="group_hierarchy",
            operation_type="add_group_to_group",
            source_id=child_group_sam,
            department_id=str(dept_id),
            target_dn=parent_target.group_dn,
            desired_state={
                "connector_id": connector_id,
                "child_group_dn": child_target.group_dn,
                "parent_group_dn": parent_target.group_dn,
                "parent_department_id": parent_department_id,
                "parent_group_sam": parent_group_sam,
            },
        )

    if not ctx.policy_settings.managed_relation_cleanup_enabled:
        return planned_hierarchy_pairs

    active_bindings = ctx.repositories.binding_repo.list_active_binding_records()
    active_bindings_by_sam = {
        binding.group_sam: binding for binding in active_bindings if binding.group_sam
    }
    planned_cleanup_pairs: set[tuple[str, str, str]] = set()

    for binding in active_bindings:
        if ctx.hooks.is_cancelled():
            raise InterruptedError("sync cancelled by user")
        if not binding.group_sam or not binding.group_dn:
            continue

        try:
            binding_department_id = int(binding.department_id)
        except (TypeError, ValueError):
            continue

        dept_info = dept_tree.get(binding_department_id)
        if not dept_info or is_department_excluded(dept_info):
            continue

        child_target = get_department_group_target(
            ctx,
            dept_info,
            get_connector_id_for_department=get_connector_id_for_department,
            get_ad_sync=get_ad_sync,
            display_separator=display_separator,
        )
        if child_target.policy.is_excluded:
            record_group_policy_skip(
                "plan",
                "group_relation_cleanup_child",
                child_target,
                f"skip relation cleanup for child group {binding.group_sam}",
            )
            continue
        if ctx.hooks.has_exception_rule("skip_group_relation_cleanup", binding.group_sam):
            record_exception_skip(
                stage_name="plan",
                object_type="group_hierarchy",
                operation_type="remove_group_from_group",
                exception_rule_type="skip_group_relation_cleanup",
                match_value=binding.group_sam,
                reason=f"skip cleanup for child group {binding.group_sam}: matched exception rule skip_group_relation_cleanup",
                source_id=binding.group_sam,
                department_id=str(binding_department_id),
                target_dn=binding.group_dn,
                risk_level="high",
                details={"child_group_sam": binding.group_sam},
            )
            continue

        expected_parent_department_id = get_effective_parent_department_id(
            ctx,
            dept_info,
            is_department_excluded=is_department_excluded,
        )
        expected_parent_target = None
        if expected_parent_department_id:
            candidate_parent = get_department_group_target(
                ctx,
                dept_tree[expected_parent_department_id],
                get_connector_id_for_department=get_connector_id_for_department,
                get_ad_sync=get_ad_sync,
                display_separator=display_separator,
            )
            if not candidate_parent.policy.is_excluded:
                expected_parent_target = candidate_parent

        expected_parent_sam = expected_parent_target.group_sam if expected_parent_target else None
        connector_id = get_connector_id_for_department(dept_info)
        for current_parent in get_current_parent_groups(
            ctx,
            binding.group_dn,
            connector_id=connector_id,
            get_ad_sync=get_ad_sync,
        ):
            current_parent_sam = current_parent.group_sam
            if not current_parent_sam:
                continue

            managed_parent_binding = active_bindings_by_sam.get(current_parent_sam)
            if not managed_parent_binding:
                continue
            if ctx.hooks.has_exception_rule("skip_group_relation_cleanup", current_parent_sam):
                record_exception_skip(
                    stage_name="plan",
                    object_type="group_hierarchy",
                    operation_type="remove_group_from_group",
                    exception_rule_type="skip_group_relation_cleanup",
                    match_value=current_parent_sam,
                    reason=f"skip cleanup against parent group {current_parent_sam}: matched exception rule skip_group_relation_cleanup",
                    source_id=binding.group_sam,
                    department_id=str(binding_department_id),
                    target_id=current_parent_sam,
                    target_dn=managed_parent_binding.get("group_dn") or current_parent.dn,
                    risk_level="high",
                    details={
                        "child_group_sam": binding.group_sam,
                        "parent_group_sam": current_parent_sam,
                    },
                )
                continue
            if current_parent_sam == expected_parent_sam:
                continue

            parent_policy = ctx.hooks.evaluate_group_policy(
                group_sam=managed_parent_binding.get("group_sam"),
                group_dn=managed_parent_binding.get("group_dn") or current_parent.dn,
                display_name=managed_parent_binding.get("display_name") or current_parent.display_name,
            )
            if parent_policy.is_excluded:
                record_skip_detail(
                    stage_name="plan",
                    action_type="group_relation_cleanup_parent",
                    group_sam=current_parent_sam,
                    group_dn=managed_parent_binding.get("group_dn") or current_parent.dn,
                    reason=f"skip cleanup against excluded parent group {current_parent_sam}",
                    matched_rules=parent_policy.matched_rule_labels(),
                )
                ctx.hooks.record_event(
                    "INFO",
                    "group_relation_cleanup_skipped",
                    f"skip cleanup against excluded parent group {current_parent_sam}",
                    stage_name="plan",
                    payload={
                        "child_group_sam": binding.group_sam,
                        "parent_group_sam": current_parent_sam,
                    },
                )
                continue

            cleanup_key = (connector_id, binding.group_sam, current_parent_sam)
            if cleanup_key in planned_cleanup_pairs:
                continue

            planned_cleanup_pairs.add(cleanup_key)
            ctx.actions.group_cleanup_actions.append(
                GroupCleanupAction(
                    connector_id=connector_id,
                    child_department_id=binding_department_id,
                    child_group_sam=binding.group_sam,
                    child_group_dn=binding.group_dn,
                    parent_group_sam=current_parent_sam,
                    parent_group_dn=managed_parent_binding.get("group_dn") or current_parent.dn,
                    expected_parent_group_sam=expected_parent_sam,
                )
            )
            ctx.hooks.add_planned_operation(
                object_type="group_hierarchy",
                operation_type="remove_group_from_group",
                source_id=binding.group_sam,
                department_id=str(binding_department_id),
                target_dn=managed_parent_binding.get("group_dn") or current_parent.dn,
                desired_state={
                    "connector_id": connector_id,
                    "parent_group_sam": current_parent_sam,
                    "expected_parent_group_sam": expected_parent_sam,
                },
                risk_level="high",
            )
    return planned_hierarchy_pairs
