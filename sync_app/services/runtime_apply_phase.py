from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import Any, Callable, Optional

from sync_app.core.sync_policies import (
    build_ad_to_source_mapping_payload,
    build_source_to_ad_mapping_payload,
)
from sync_app.core.common import hash_department_state
from sync_app.core.models import DepartmentGroupInfo, ManagedGroupTarget
from sync_app.services.runtime_connectors import (
    sanitize_source_writeback_payload,
    select_mapping_rules,
)
from sync_app.services.runtime_context import SyncContext
from sync_app.services.runtime_lifecycle import serialize_lifecycle_profile


def apply_user_actions(
    ctx: SyncContext,
    *,
    get_ad_sync: Callable[[str], Any],
    field_ownership_policy: dict[str, Any],
) -> None:
    user_actions = ctx.actions.user_actions
    binding_resolution_details = ctx.identity.binding_resolution_details
    dept_tree = ctx.environment.dept_tree
    source_provider = ctx.environment.source_provider
    sync_stats = ctx.sync_stats
    stats_callback = ctx.hooks.stats_callback
    state_manager = ctx.repositories.state_manager
    lifecycle_repo = ctx.repositories.lifecycle_repo

    processed_users: set[str] = set()
    for index, action in enumerate(user_actions, start=1):
        if index % 10 == 0 and ctx.hooks.is_cancelled():
            raise InterruptedError("sync cancelled by user")

        if action.operation_type == "create_user":
            bucket = "user_create_errors"
        else:
            bucket = "user_update_errors"
        try:
            connector_ad_sync = get_ad_sync(action.connector_id)
            connector_source_to_ad_rules = (
                select_mapping_rules(
                    ctx.policy_settings.enabled_mapping_rules,
                    direction="source_to_ad",
                    connector_id=action.connector_id,
                )
                if ctx.policy_settings.attribute_mapping_enabled
                else []
            )
            extra_attributes = build_source_to_ad_mapping_payload(
                action.user,
                connector_id=action.connector_id,
                ad_username=action.username,
                email=action.email,
                target_department=dept_tree.get(action.target_department_id),
                rules=connector_source_to_ad_rules,
            )
            if action.operation_type == "update_user":
                success = connector_ad_sync.update_user(
                    action.username,
                    action.display_name,
                    action.email,
                    action.ou_dn,
                    extra_attributes=extra_attributes,
                )
                if success:
                    sync_stats["operations"]["users_updated"] += 1
            elif action.operation_type == "reactivate_user":
                success = connector_ad_sync.reactivate_user(
                    action.username,
                    action.display_name,
                    action.email,
                    action.ou_dn,
                    extra_attributes=extra_attributes,
                )
                if success:
                    sync_stats["operations"]["users_updated"] += 1
            else:
                success = connector_ad_sync.create_user(
                    action.username,
                    action.display_name,
                    action.email,
                    action.ou_dn,
                    extra_attributes=extra_attributes,
                )
                if success:
                    sync_stats["operations"]["users_created"] += 1

            if not success:
                raise Exception("LDAP operation returned failure")

            state_payload = action.user.to_state_payload()
            state_payload.update(
                {
                    "connector_id": action.connector_id,
                    "ad_username": action.username,
                    "target_department_id": action.target_department_id,
                    "placement_reason": action.placement_reason,
                    "lifecycle_profile": serialize_lifecycle_profile(action.lifecycle_profile),
                }
            )
            state_manager.update_user_state(
                action.user.userid,
                state_payload,
                job_id=ctx.job_id,
                target_dn=f"CN={action.display_name},{action.ou_dn}",
            )
            ctx.hooks.record_operation(
                stage_name="apply",
                object_type="user",
                operation_type=action.operation_type,
                status="succeeded",
                message=f"{action.operation_type} succeeded for {action.username}",
                source_id=action.user.userid,
                department_id=str(action.target_department_id),
                target_id=action.username,
                target_dn=f"CN={action.display_name},{action.ou_dn}",
                rule_source=binding_resolution_details.get(action.user.userid, {}).get("source"),
                reason_code=action.placement_reason,
                details={
                    "connector_id": action.connector_id,
                    "binding_resolution": binding_resolution_details.get(action.user.userid, {}),
                    "ou_path": action.ou_path,
                    "email": action.email,
                    "mapped_attributes": sorted(extra_attributes.keys()),
                    "field_ownership_policy": dict(field_ownership_policy),
                },
            )
            connector_writeback_rules = (
                select_mapping_rules(
                    ctx.policy_settings.enabled_mapping_rules,
                    direction="ad_to_source",
                    connector_id=action.connector_id,
                )
                if ctx.policy_settings.write_back_enabled
                else []
            )
            if connector_writeback_rules:
                ad_attributes = connector_ad_sync.get_user_details(action.username)
                writeback_payload = sanitize_source_writeback_payload(
                    build_ad_to_source_mapping_payload(
                        ad_attributes,
                        action.user.to_state_payload(),
                        connector_id=action.connector_id,
                        rules=connector_writeback_rules,
                    )
                )
                if writeback_payload:
                    source_provider.update_user(action.user.userid, writeback_payload)
                    ctx.hooks.record_operation(
                        stage_name="apply",
                        object_type="user",
                        operation_type="write_back_user",
                        status="succeeded",
                        message=f"wrote AD attributes back to source provider for {action.user.userid}",
                        source_id=action.user.userid,
                        target_id=action.username,
                        risk_level="normal",
                        details={
                            "connector_id": action.connector_id,
                            "fields": sorted(writeback_payload.keys()),
                        },
                    )
            if action.lifecycle_profile.get("start_at"):
                lifecycle_repo.mark_completed_for_source_user(
                    lifecycle_type="future_onboarding",
                    connector_id=action.connector_id,
                    source_user_id=action.user.userid,
                    last_job_id=ctx.job_id,
                )
            ctx.executed_count += 1
            sync_stats["executed_operation_count"] = ctx.executed_count
            processed_users.add(action.user.userid)
            if stats_callback and (len(user_actions) < 100 or index % 5 == 0 or index == 1):
                stats_callback("user_processed", index)
        except Exception as user_error:
            sync_stats["errors"][bucket].append(
                {
                    "userid": action.user.userid,
                    "username": action.username,
                    "display_name": action.display_name,
                    "email": action.email,
                    "department": action.ou_path[-1] if action.ou_path else "",
                    "placement_reason": action.placement_reason,
                    "error": str(user_error),
                }
            )
            sync_stats["error_count"] += 1
            ctx.hooks.record_operation(
                stage_name="apply",
                object_type="user",
                operation_type=action.operation_type,
                status="error",
                message=f"{action.operation_type} failed for {action.username}: {user_error}",
                source_id=action.user.userid,
                department_id=str(action.target_department_id),
                target_id=action.username,
                target_dn=f"CN={action.display_name},{action.ou_dn}",
                rule_source=binding_resolution_details.get(action.user.userid, {}).get("source"),
                reason_code=action.placement_reason,
                details={"error": str(user_error)},
            )

    sync_stats["processed_users"] = len(processed_users)


def apply_disable_actions(
    ctx: SyncContext,
    *,
    get_ad_sync: Callable[[str], Any],
    record_exception_skip: Callable[..., None],
) -> None:
    disable_actions = ctx.actions.disable_actions
    sync_stats = ctx.sync_stats
    stats_callback = ctx.hooks.stats_callback
    offboarding_repo = ctx.repositories.offboarding_repo
    lifecycle_repo = ctx.repositories.lifecycle_repo

    if stats_callback:
        stats_callback("disable_stage_start", True)
        stats_callback("users_to_disable", len(disable_actions))

    if not disable_actions:
        return

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    disable_log_filename = os.path.join(
        log_dir,
        f"disabled_accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    )
    with open(disable_log_filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "SamAccountName",
                "DisplayName",
                "Mail",
                "Created",
                "Modified",
                "LastLogonDate",
                "Description",
                "ConnectorID",
                "DisableTime",
            ],
        )
        writer.writeheader()
        for index, action in enumerate(disable_actions, start=1):
            if index % 5 == 0 and ctx.hooks.is_cancelled():
                raise InterruptedError("sync cancelled by user")
            try:
                if action.source_user_id and ctx.hooks.has_exception_rule("skip_user_sync", action.source_user_id):
                    record_exception_skip(
                        stage_name="apply",
                        object_type="user",
                        operation_type="disable_user",
                        exception_rule_type="skip_user_sync",
                        match_value=action.source_user_id,
                        reason=f"skip disable for AD user {action.username}: matched exception rule skip_user_sync",
                        source_id=action.source_user_id,
                        target_id=action.username,
                        risk_level="high",
                        details={"source_user_id": action.source_user_id, "ad_username": action.username},
                    )
                    continue
                if action.source_user_id and ctx.hooks.has_exception_rule("skip_user_disable", action.source_user_id):
                    record_exception_skip(
                        stage_name="apply",
                        object_type="user",
                        operation_type="disable_user",
                        exception_rule_type="skip_user_disable",
                        match_value=action.source_user_id,
                        reason=f"skip disable for AD user {action.username}: matched exception rule skip_user_disable",
                        source_id=action.source_user_id,
                        target_id=action.username,
                        risk_level="high",
                        details={"source_user_id": action.source_user_id, "ad_username": action.username},
                    )
                    continue
                connector_ad_sync = get_ad_sync(action.connector_id)
                user_details = connector_ad_sync.get_user_details(action.username)
                if user_details:
                    user_details["DisableTime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    user_details["ConnectorID"] = action.connector_id
                    writer.writerow(user_details)

                if not connector_ad_sync.disable_user(action.username):
                    raise Exception("failed to disable user")

                offboarding_repo.mark_disabled(
                    connector_id=action.connector_id,
                    ad_username=action.username,
                    last_job_id=ctx.job_id,
                )
                sync_stats["operations"]["users_disabled"] += 1
                ctx.hooks.record_operation(
                    stage_name="apply",
                    object_type="user",
                    operation_type="disable_user",
                    status="succeeded",
                    message=f"disabled AD user {action.username}",
                    source_id=action.source_user_id or action.username,
                    target_id=action.username,
                    risk_level="high",
                    reason_code=action.reason or "missing_from_source",
                    details={
                        "source_user_id": action.source_user_id,
                        "connector_id": action.connector_id,
                        "employment_type": action.employment_type,
                        "sponsor_userid": action.sponsor_userid,
                        "effective_at": action.effective_at,
                    },
                )
                if action.reason == "contractor_expired" and action.source_user_id:
                    lifecycle_repo.mark_completed_for_source_user(
                        lifecycle_type="contractor_expiry",
                        connector_id=action.connector_id,
                        source_user_id=action.source_user_id,
                        last_job_id=ctx.job_id,
                    )
                ctx.executed_count += 1
                sync_stats["executed_operation_count"] = ctx.executed_count
                if stats_callback and (len(disable_actions) < 50 or index % 5 == 0 or index == 1):
                    stats_callback("user_disable_progress", index / max(len(disable_actions), 1))
            except Exception as disable_error:
                sync_stats["errors"]["user_disable_errors"].append(
                    {
                        "username": action.username,
                        "userid": action.source_user_id,
                        "error": str(disable_error),
                    }
                )
                sync_stats["error_count"] += 1
                ctx.hooks.record_operation(
                    stage_name="apply",
                    object_type="user",
                    operation_type="disable_user",
                    status="error",
                    message=f"failed to disable AD user {action.username}: {disable_error}",
                    source_id=action.source_user_id or action.username,
                    target_id=action.username,
                    risk_level="high",
                    reason_code=action.reason or "missing_from_source",
                    details={
                        "error": str(disable_error),
                        "source_user_id": action.source_user_id,
                        "employment_type": action.employment_type,
                        "sponsor_userid": action.sponsor_userid,
                        "effective_at": action.effective_at,
                    },
                )

    sync_stats["disabled_users"] = [f"{action.connector_id}:{action.username}" for action in disable_actions]


def _get_active_binding(record: Any) -> Any | None:
    if record and getattr(record, "status", None) != "active":
        return None
    return record


def _build_runtime_group_target(
    *,
    group_sam: str,
    group_dn: str,
    display_name: str,
    department_id: int,
    policy: Any,
    parent_department_id: Optional[int] = None,
) -> ManagedGroupTarget:
    return ManagedGroupTarget(
        exists=True,
        group_sam=group_sam,
        group_cn=group_sam,
        group_dn=group_dn,
        display_name=display_name or "",
        description="",
        binding_source="runtime",
        created=False,
        binding_exists=True,
        department_id=department_id,
        parent_department_id=parent_department_id,
        ou_name="",
        ou_dn="",
        full_path=[],
        policy=policy,
    )


def apply_department_actions(
    ctx: SyncContext,
    *,
    get_ad_sync: Callable[[str], Any],
    display_separator: str,
    record_group_policy_skip: Callable[..., None],
) -> None:
    department_actions = ctx.actions.department_actions
    sync_stats = ctx.sync_stats
    stats_callback = ctx.hooks.stats_callback
    binding_repo = ctx.repositories.binding_repo
    state_repo = ctx.repositories.state_repo
    department_group_targets = ctx.environment.department_group_targets

    for idx, action in enumerate(
        sorted(department_actions, key=lambda item: (len(item.full_path), item.department_id)),
        start=1,
    ):
        if ctx.hooks.is_cancelled():
            raise InterruptedError("sync cancelled by user")

        try:
            connector_ad_sync = get_ad_sync(action.connector_id)
            success, ensured_ou_dn, ou_created = connector_ad_sync.ensure_ou(action.ou_name, action.parent_dn)
            if not success:
                raise Exception(f"failed to ensure OU: {action.ou_name}")

            group_target = action.group_target
            group_info = DepartmentGroupInfo(
                exists=group_target.exists,
                group_sam=group_target.group_sam,
                group_cn=group_target.group_cn,
                group_dn=group_target.group_dn,
                display_name=group_target.display_name,
                description=group_target.description,
                binding_source=group_target.binding_source,
                created=group_target.created,
            )
            if ensured_ou_dn:
                if action.should_manage_group:
                    group_info = connector_ad_sync.ensure_department_group(
                        department_id=action.department_id,
                        parent_department_id=action.parent_department_id,
                        ou_name=action.ou_name,
                        ou_dn=ensured_ou_dn,
                        full_path=action.full_path,
                        display_separator=display_separator,
                        binding_repo=binding_repo,
                    )
                    group_target.apply_mapping(group_info)
                    group_target.binding_exists = True
                    group_target.policy = ctx.hooks.evaluate_group_policy(
                        group_sam=group_info.group_sam,
                        group_dn=group_info.group_dn,
                        display_name=group_info.display_name,
                    )
                    department_group_targets[action.department_id] = group_target
                else:
                    record_group_policy_skip(
                        "apply",
                        "department_group_management",
                        group_target,
                        f"skip managed group for department {action.ou_name}",
                    )

                state_repo.upsert_state(
                    source_type="source",
                    object_type="department",
                    source_id=str(action.department_id),
                    source_hash=hash_department_state(
                        {
                            "id": action.department_id,
                            "name": action.ou_name,
                            "parentid": action.parent_department_id or 0,
                        }
                    ),
                    display_name=action.ou_name,
                    target_dn=ensured_ou_dn,
                    last_job_id=ctx.job_id,
                    last_action="sync_department",
                    last_status="success",
                    extra={
                        "path": action.full_path,
                        "group_sam": group_info.group_sam,
                        "group_management": "managed" if action.should_manage_group else "skipped_by_policy",
                    },
                )
                if ou_created:
                    sync_stats["operations"]["departments_created"] += 1
                else:
                    sync_stats["operations"]["departments_existed"] += 1

            ctx.hooks.record_operation(
                stage_name="apply",
                object_type="department",
                operation_type="ensure_department_node",
                status="succeeded",
                message=f"ensured department node for {action.ou_name}",
                source_id=str(action.department_id),
                department_id=str(action.department_id),
                target_dn=ensured_ou_dn or action.ou_dn,
                target_id=group_info.group_sam if ensured_ou_dn and action.should_manage_group else "",
                details={
                    "ou_created": ou_created,
                    "group_management": "managed" if action.should_manage_group else "skipped_by_policy",
                    "group_sam": group_info.group_sam,
                    "group_dn": group_info.group_dn,
                },
            )
            ctx.executed_count += 1
            sync_stats["executed_operation_count"] = ctx.executed_count
            if stats_callback:
                stats_callback("department_progress", idx / max(len(department_actions), 1))
        except Exception as department_error:
            ctx.logger.error("failed to sync department %s: %s", action.ou_name, department_error)
            sync_stats["errors"]["department_errors"].append(
                {
                    "department": action.ou_name,
                    "path": " > ".join(action.full_path),
                    "error": str(department_error),
                }
            )
            sync_stats["error_count"] += 1
            ctx.hooks.record_operation(
                stage_name="apply",
                object_type="department",
                operation_type="ensure_department_node",
                status="error",
                message=f"failed to sync department {action.ou_name}: {department_error}",
                source_id=str(action.department_id),
                department_id=str(action.department_id),
                target_dn=action.ou_dn,
                details={
                    "path": action.full_path,
                    "error": str(department_error),
                },
            )

    if stats_callback:
        stats_callback("department_sync_done", True)


def apply_group_hierarchy_actions(
    ctx: SyncContext,
    *,
    get_ad_sync: Callable[[str], Any],
    record_group_policy_skip: Callable[..., None],
) -> set[tuple[str, str, str]]:
    sync_stats = ctx.sync_stats
    binding_repo = ctx.repositories.binding_repo
    successful_hierarchy_pairs: set[tuple[str, str, str]] = set()

    for action in ctx.actions.group_hierarchy_actions:
        if ctx.hooks.is_cancelled():
            raise InterruptedError("sync cancelled by user")

        try:
            child_binding = _get_active_binding(
                binding_repo.get_binding_record_by_department_id(str(action.child_department_id))
            )
            parent_binding = _get_active_binding(
                binding_repo.get_binding_record_by_department_id(str(action.parent_department_id))
            )

            child_group_sam = child_binding.group_sam if child_binding else action.child_group_sam
            child_group_dn = child_binding.group_dn if child_binding and child_binding.group_dn else action.child_group_dn
            child_display_name = child_binding.display_name if child_binding else action.child_display_name
            parent_group_sam = parent_binding.group_sam if parent_binding else action.parent_group_sam
            parent_group_dn = (
                parent_binding.group_dn if parent_binding and parent_binding.group_dn else action.parent_group_dn
            )
            parent_display_name = parent_binding.display_name if parent_binding else action.parent_display_name

            child_policy = ctx.hooks.evaluate_group_policy(
                group_sam=child_group_sam,
                group_dn=child_group_dn,
                display_name=child_display_name,
            )
            parent_policy = ctx.hooks.evaluate_group_policy(
                group_sam=parent_group_sam,
                group_dn=parent_group_dn,
                display_name=parent_display_name,
            )
            if child_policy.is_excluded:
                record_group_policy_skip(
                    "apply",
                    "group_hierarchy_child",
                    _build_runtime_group_target(
                        group_sam=child_group_sam,
                        group_dn=child_group_dn,
                        display_name=child_display_name,
                        department_id=action.child_department_id,
                        parent_department_id=action.parent_department_id,
                        policy=child_policy,
                    ),
                    f"skip recursive child group {child_group_sam}",
                )
                continue
            if parent_policy.is_excluded:
                record_group_policy_skip(
                    "apply",
                    "group_hierarchy_parent",
                    _build_runtime_group_target(
                        group_sam=parent_group_sam,
                        group_dn=parent_group_dn,
                        display_name=parent_display_name,
                        department_id=action.parent_department_id,
                        policy=parent_policy,
                    ),
                    f"skip recursive parent group {parent_group_sam}",
                )
                continue

            if not child_group_dn or not parent_group_dn:
                raise Exception("group DN missing for recursive relation")
            if not get_ad_sync(action.connector_id).add_group_to_group(child_group_dn, parent_group_dn):
                raise Exception(f"failed to add group relation {child_group_sam} -> {parent_group_sam}")

            successful_hierarchy_pairs.add((action.connector_id, child_group_sam, parent_group_sam))
            sync_stats["operations"]["groups_nested"] += 1
            ctx.hooks.record_operation(
                stage_name="apply",
                object_type="group_hierarchy",
                operation_type="add_group_to_group",
                status="succeeded",
                message=f"added group relation {child_group_sam} -> {parent_group_sam}",
                source_id=child_group_sam,
                department_id=str(action.child_department_id),
                target_id=parent_group_sam,
                target_dn=parent_group_dn,
                details={
                    "child_group_dn": child_group_dn,
                    "parent_group_dn": parent_group_dn,
                },
            )
            ctx.executed_count += 1
            sync_stats["executed_operation_count"] = ctx.executed_count
        except Exception as hierarchy_error:
            sync_stats["errors"]["group_hierarchy_errors"].append(
                {
                    "child_group_sam": action.child_group_sam,
                    "parent_group_sam": action.parent_group_sam,
                    "error": str(hierarchy_error),
                }
            )
            sync_stats["error_count"] += 1
            ctx.hooks.record_operation(
                stage_name="apply",
                object_type="group_hierarchy",
                operation_type="add_group_to_group",
                status="error",
                message=(
                    f"failed to add group relation {action.child_group_sam} -> "
                    f"{action.parent_group_sam}: {hierarchy_error}"
                ),
                source_id=action.child_group_sam,
                department_id=str(action.child_department_id),
                target_id=action.parent_group_sam,
                target_dn=action.parent_group_dn,
                details={"error": str(hierarchy_error)},
            )

    return successful_hierarchy_pairs


def apply_custom_group_actions(
    ctx: SyncContext,
    *,
    get_ad_sync: Callable[[str], Any],
) -> None:
    sync_stats = ctx.sync_stats
    custom_group_binding_repo = ctx.repositories.custom_group_binding_repo

    for action in ctx.actions.custom_group_actions:
        try:
            connector_ad_sync = get_ad_sync(action["connector_id"])
            group_info = connector_ad_sync.ensure_custom_group(
                source_type=action["source_type"],
                source_key=action["source_key"],
                display_name=action["display_name"],
            )
            custom_group_binding_repo.upsert_binding(
                connector_id=action["connector_id"],
                source_type=action["source_type"],
                source_key=action["source_key"],
                group_sam=group_info.group_sam,
                group_dn=group_info.group_dn,
                group_cn=group_info.group_cn,
                display_name=group_info.display_name,
                status="active",
            )
            ctx.hooks.record_operation(
                stage_name="apply",
                object_type="custom_group",
                operation_type="ensure_custom_group",
                status="succeeded",
                message=f"ensured custom group {action['source_type']}:{action['source_key']}",
                source_id=f"{action['source_type']}:{action['source_key']}",
                target_id=group_info.group_sam,
                target_dn=group_info.group_dn,
                details={
                    "connector_id": action["connector_id"],
                    "display_name": group_info.display_name,
                },
            )
        except Exception as custom_group_error:
            sync_stats["error_count"] += 1
            ctx.hooks.record_operation(
                stage_name="apply",
                object_type="custom_group",
                operation_type="ensure_custom_group",
                status="error",
                message=(
                    f"failed to ensure custom group {action['source_type']}:{action['source_key']}: "
                    f"{custom_group_error}"
                ),
                source_id=f"{action['source_type']}:{action['source_key']}",
                details={"connector_id": action["connector_id"], "error": str(custom_group_error)},
            )


def apply_group_membership_actions(
    ctx: SyncContext,
    *,
    get_ad_sync: Callable[[str], Any],
    record_exception_skip: Callable[..., None],
    record_group_policy_skip: Callable[..., None],
) -> None:
    sync_stats = ctx.sync_stats
    custom_group_binding_repo = ctx.repositories.custom_group_binding_repo
    custom_group_bindings_by_connector: dict[str, dict[str, Any]] = {}

    for action in ctx.actions.membership_actions:
        try:
            if not action.group_dn and action.group_sam.startswith("WECOM_"):
                if action.connector_id not in custom_group_bindings_by_connector:
                    custom_group_bindings_by_connector[action.connector_id] = {
                        binding.group_sam: binding
                        for binding in custom_group_binding_repo.list_active_records(connector_id=action.connector_id)
                        if binding.group_sam
                    }
                binding = custom_group_bindings_by_connector[action.connector_id].get(action.group_sam)
                if binding:
                    action.group_dn = binding.group_dn
                    if binding.display_name:
                        action.group_display_name = binding.display_name
            if action.source_user_id and ctx.hooks.has_exception_rule("skip_user_group_membership", action.source_user_id):
                record_exception_skip(
                    stage_name="apply",
                    object_type="group_membership",
                    operation_type="add_user_to_group",
                    exception_rule_type="skip_user_group_membership",
                    match_value=action.source_user_id,
                    reason=(
                        "skip managed group memberships for user "
                        f"{action.source_user_id}: matched exception rule skip_user_group_membership"
                    ),
                    source_id=action.source_user_id,
                    department_id=str(action.department_id),
                    target_id=action.group_sam,
                    target_dn=action.group_dn,
                    details={
                        "source_user_id": action.source_user_id,
                        "ad_username": action.username,
                        "group_sam": action.group_sam,
                    },
                )
                continue
            membership_policy = ctx.hooks.evaluate_group_policy(
                group_sam=action.group_sam,
                group_dn=action.group_dn,
                display_name=action.group_display_name,
            )
            if membership_policy.is_excluded:
                record_group_policy_skip(
                    "apply",
                    "group_membership",
                    _build_runtime_group_target(
                        group_sam=action.group_sam,
                        group_dn=action.group_dn,
                        display_name=action.group_display_name,
                        department_id=action.department_id,
                        policy=membership_policy,
                    ),
                    f"skip user membership management for group {action.group_sam}",
                )
                continue

            if not get_ad_sync(action.connector_id).add_user_to_group(action.username, action.group_sam):
                raise Exception(f"failed to add {action.username} to {action.group_sam}")

            sync_stats["operations"]["groups_assigned"] += 1
            ctx.hooks.record_operation(
                stage_name="apply",
                object_type="group_membership",
                operation_type="add_user_to_group",
                status="succeeded",
                message=f"added {action.username} to {action.group_sam}",
                source_id=action.username,
                department_id=str(action.department_id),
                target_id=action.group_sam,
                target_dn=action.group_dn,
                details={"group_display_name": action.group_display_name},
            )
            ctx.executed_count += 1
            sync_stats["executed_operation_count"] = ctx.executed_count
        except Exception as membership_error:
            sync_stats["errors"]["group_add_errors"].append(
                {
                    "username": action.username,
                    "groups": action.group_sam,
                    "error": str(membership_error),
                }
            )
            sync_stats["error_count"] += 1
            ctx.hooks.record_operation(
                stage_name="apply",
                object_type="group_membership",
                operation_type="add_user_to_group",
                status="error",
                message=f"failed to add {action.username} to {action.group_sam}: {membership_error}",
                source_id=action.username,
                department_id=str(action.department_id),
                target_id=action.group_sam,
                target_dn=action.group_dn,
                details={"error": str(membership_error)},
            )


def apply_group_cleanup_actions(
    ctx: SyncContext,
    *,
    get_ad_sync: Callable[[str], Any],
    planned_hierarchy_pairs: set[tuple[str, str, str]],
    successful_hierarchy_pairs: set[tuple[str, str, str]],
    record_exception_skip: Callable[..., None],
    record_group_policy_skip: Callable[..., None],
    record_skip_detail: Callable[..., None],
) -> None:
    if not (ctx.policy_settings.group_recursive_enabled and ctx.policy_settings.managed_relation_cleanup_enabled):
        return

    sync_stats = ctx.sync_stats
    binding_repo = ctx.repositories.binding_repo

    for action in ctx.actions.group_cleanup_actions:
        if ctx.hooks.is_cancelled():
            raise InterruptedError("sync cancelled by user")

        try:
            if ctx.hooks.has_exception_rule("skip_group_relation_cleanup", action.child_group_sam):
                record_exception_skip(
                    stage_name="apply",
                    object_type="group_hierarchy",
                    operation_type="remove_group_from_group",
                    exception_rule_type="skip_group_relation_cleanup",
                    match_value=action.child_group_sam,
                    reason=(
                        f"skip cleanup for child group {action.child_group_sam}: "
                        "matched exception rule skip_group_relation_cleanup"
                    ),
                    source_id=action.child_group_sam,
                    department_id=str(action.child_department_id),
                    target_dn=action.child_group_dn,
                    risk_level="high",
                    details={"child_group_sam": action.child_group_sam},
                )
                continue
            if ctx.hooks.has_exception_rule("skip_group_relation_cleanup", action.parent_group_sam):
                record_exception_skip(
                    stage_name="apply",
                    object_type="group_hierarchy",
                    operation_type="remove_group_from_group",
                    exception_rule_type="skip_group_relation_cleanup",
                    match_value=action.parent_group_sam,
                    reason=(
                        f"skip cleanup against parent group {action.parent_group_sam}: "
                        "matched exception rule skip_group_relation_cleanup"
                    ),
                    source_id=action.child_group_sam,
                    department_id=str(action.child_department_id),
                    target_id=action.parent_group_sam,
                    target_dn=action.parent_group_dn,
                    risk_level="high",
                    details={
                        "child_group_sam": action.child_group_sam,
                        "parent_group_sam": action.parent_group_sam,
                    },
                )
                continue
            child_binding = _get_active_binding(
                binding_repo.get_binding_record_by_department_id(str(action.child_department_id))
            )
            parent_binding = _get_active_binding(binding_repo.get_binding_record_by_group_sam(action.parent_group_sam))

            child_group_sam = child_binding.group_sam if child_binding else action.child_group_sam
            child_group_dn = child_binding.group_dn if child_binding and child_binding.group_dn else action.child_group_dn
            parent_group_sam = parent_binding.group_sam if parent_binding else action.parent_group_sam
            parent_group_dn = (
                parent_binding.group_dn if parent_binding and parent_binding.group_dn else action.parent_group_dn
            )
            expected_parent_group_sam = action.expected_parent_group_sam

            if expected_parent_group_sam:
                expected_pair = (action.connector_id, child_group_sam, expected_parent_group_sam)
                if expected_pair in planned_hierarchy_pairs and expected_pair not in successful_hierarchy_pairs:
                    record_skip_detail(
                        stage_name="apply",
                        action_type="group_relation_cleanup_deferred",
                        group_sam=child_group_sam,
                        group_dn=child_group_dn,
                        reason=(
                            f"skip cleanup for {child_group_sam} because expected parent relation was not ensured"
                        ),
                        matched_rules=[],
                    )
                    ctx.hooks.record_event(
                        "WARNING",
                        "group_relation_cleanup_deferred",
                        f"skip cleanup for {child_group_sam} because expected parent relation was not ensured",
                        stage_name="apply",
                        payload={
                            "child_group_sam": child_group_sam,
                            "expected_parent_group_sam": expected_parent_group_sam,
                            "stale_parent_group_sam": parent_group_sam,
                        },
                    )
                    continue

            child_policy = ctx.hooks.evaluate_group_policy(group_sam=child_group_sam, group_dn=child_group_dn)
            parent_policy = ctx.hooks.evaluate_group_policy(group_sam=parent_group_sam, group_dn=parent_group_dn)
            if child_policy.is_excluded:
                record_group_policy_skip(
                    "apply",
                    "group_relation_cleanup_child",
                    _build_runtime_group_target(
                        group_sam=child_group_sam,
                        group_dn=child_group_dn,
                        display_name=child_binding.display_name if child_binding else "",
                        department_id=action.child_department_id,
                        policy=child_policy,
                    ),
                    f"skip relation cleanup for child group {child_group_sam}",
                )
                continue
            if parent_policy.is_excluded:
                record_group_policy_skip(
                    "apply",
                    "group_relation_cleanup_parent",
                    _build_runtime_group_target(
                        group_sam=parent_group_sam,
                        group_dn=parent_group_dn,
                        display_name=parent_binding.display_name if parent_binding else "",
                        department_id=action.child_department_id,
                        policy=parent_policy,
                    ),
                    f"skip relation cleanup for parent group {parent_group_sam}",
                )
                continue

            if not child_group_dn or not parent_group_dn:
                raise Exception("group DN missing for cleanup relation")
            if not get_ad_sync(action.connector_id).remove_group_from_group(child_group_dn, parent_group_dn):
                raise Exception(f"failed to remove stale group relation {child_group_sam} -> {parent_group_sam}")

            sync_stats["operations"]["group_relations_removed"] += 1
            ctx.hooks.record_operation(
                stage_name="apply",
                object_type="group_hierarchy",
                operation_type="remove_group_from_group",
                status="succeeded",
                message=f"removed stale group relation {child_group_sam} -> {parent_group_sam}",
                source_id=child_group_sam,
                department_id=str(action.child_department_id),
                target_id=parent_group_sam,
                target_dn=parent_group_dn,
                risk_level="high",
                details={"expected_parent_group_sam": expected_parent_group_sam},
            )
            ctx.executed_count += 1
            sync_stats["executed_operation_count"] = ctx.executed_count
        except Exception as cleanup_error:
            sync_stats["errors"]["group_relation_cleanup_errors"].append(
                {
                    "child_group_sam": action.child_group_sam,
                    "parent_group_sam": action.parent_group_sam,
                    "error": str(cleanup_error),
                }
            )
            sync_stats["error_count"] += 1
            ctx.hooks.record_operation(
                stage_name="apply",
                object_type="group_hierarchy",
                operation_type="remove_group_from_group",
                status="error",
                message=(
                    f"failed to remove stale group relation {action.child_group_sam} -> "
                    f"{action.parent_group_sam}: {cleanup_error}"
                ),
                source_id=action.child_group_sam,
                department_id=str(action.child_department_id),
                target_id=action.parent_group_sam,
                target_dn=action.parent_group_dn,
                risk_level="high",
                details={"error": str(cleanup_error)},
            )


def apply_final_state_updates(ctx: SyncContext) -> None:
    state_manager = ctx.repositories.state_manager
    state_manager.cleanup_old_users(ctx.working.source_user_ids)
    state_manager.set_sync_complete(ctx.sync_stats["error_count"] == 0)
