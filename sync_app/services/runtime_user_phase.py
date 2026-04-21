from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from sync_app.core.models import DepartmentNode, DisableUserAction, GroupMembershipAction, ManagedGroupTarget, UserAction
from sync_app.core.sync_policies import extract_manager_userids
from sync_app.services.runtime_context import SyncContext
from sync_app.services.runtime_identity import resolve_target_department
from sync_app.services.runtime_lifecycle import build_user_lifecycle_profile
from sync_app.services.runtime_connectors import select_mapping_rules


def _get_source_user_detail_cached(ctx: SyncContext, userid: str, *, user: Optional[Any] = None) -> dict[str, Any]:
    source_user_detail_cache = ctx.identity.source_user_detail_cache
    if userid not in source_user_detail_cache:
        try:
            source_user_detail_cache[userid] = ctx.environment.source_provider.get_user_detail(userid) or {}
        except Exception as detail_error:
            ctx.logger.warning(
                "failed to load %s user detail for %s: %s",
                ctx.environment.source_provider_name,
                userid,
                detail_error,
            )
            source_user_detail_cache[userid] = {}
    detail_payload = source_user_detail_cache[userid]
    if user and detail_payload:
        user.merge_payload(detail_payload)
    return detail_payload


def _get_offboarding_manager_userids(ctx: SyncContext, source_user_id: str) -> list[str]:
    if source_user_id and source_user_id in ctx.identity.user_departments:
        return extract_manager_userids(ctx.identity.user_departments[source_user_id].user)
    if not source_user_id:
        return []
    state_row = ctx.repositories.state_manager.get_user_state_record(source_user_id)
    if not state_row:
        return []
    try:
        extra_payload = json.loads(state_row["extra_json"]) if state_row["extra_json"] else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        extra_payload = {}
    manager_userids = extra_payload.get("manager_userids")
    if not isinstance(manager_userids, list):
        return []
    return [str(value).strip() for value in manager_userids if str(value).strip()]


def plan_user_actions(
    ctx: SyncContext,
    *,
    planned_memberships: set[tuple[str, str, str]],
    is_department_excluded: Callable[[Optional[DepartmentNode]], bool],
    is_department_blocked_for_placement: Callable[[Optional[DepartmentNode]], bool],
    get_connector_id_for_department: Callable[[Optional[DepartmentNode]], str],
    get_connector_spec: Callable[[str], dict[str, Any]],
    get_ad_sync: Callable[[str], Any],
    get_effective_ou_path: Callable[[DepartmentNode, str], list[str]],
    get_department_group_target: Callable[[DepartmentNode], ManagedGroupTarget],
    is_protected_ad_account: Callable[[str, str], bool],
    record_exception_skip: Callable[..., None],
    record_protected_account_skip: Callable[..., None],
    record_group_policy_skip: Callable[[str, str, ManagedGroupTarget, str], None],
    field_ownership_policy: dict[str, Any],
) -> None:
    user_departments = ctx.identity.user_departments
    active_user_bindings = ctx.identity.active_user_bindings
    binding_resolution_details = ctx.identity.binding_resolution_details
    user_connector_id_by_userid = ctx.identity.user_connector_id_by_userid
    disabled_bound_userids = ctx.identity.disabled_bound_userids
    exception_skipped_userids = ctx.identity.exception_skipped_userids
    existing_users_map_by_connector = ctx.identity.existing_users_map_by_connector
    enabled_ad_users_by_connector = ctx.working.enabled_ad_users_by_connector

    for userid, info in user_departments.items():
        user = info.user
        departments_for_user = info.departments
        if userid in exception_skipped_userids or userid in disabled_bound_userids:
            continue

        username = active_user_bindings.get(userid)
        if not username:
            ctx.hooks.record_event(
                "WARNING",
                "user_skipped",
                f"skip user {userid}: no enabled identity binding is available",
                stage_name="plan",
            )
            ctx.hooks.record_operation(
                stage_name="plan",
                object_type="user",
                operation_type="resolve_identity_binding",
                status="skipped",
                message=f"skip user {userid}: no enabled identity binding is available",
                source_id=userid,
                reason_code="missing_binding",
                details={"userid": userid},
            )
            continue

        connector_id = binding_resolution_details.get(userid, {}).get(
            "connector_id",
            user_connector_id_by_userid.get(userid, "default"),
        )
        connector_spec = get_connector_spec(connector_id)
        connector_ad_sync = get_ad_sync(connector_id)
        connector_domain = connector_spec["config"].domain
        display_name = user.name
        override_record = ctx.repositories.department_override_repo.get_override_record_by_source_user_id(
            userid,
            org_id=ctx.organization.org_id,
        )
        override_department_id = None
        if override_record and override_record.primary_department_id:
            try:
                override_department_id = int(override_record.primary_department_id)
            except (TypeError, ValueError):
                override_department_id = None
        if override_department_id is not None:
            ctx.repositories.department_override_repo.record_rule_hit_for_source_user(
                userid,
                org_id=ctx.organization.org_id,
            )

        target_dept, placement_reason = resolve_target_department(
            info,
            placement_strategy=ctx.policy_settings.user_ou_placement_strategy,
            is_department_excluded=is_department_blocked_for_placement,
            override_department_id=override_department_id,
        )
        if not target_dept:
            blocked_department_ids = [
                dept.department_id
                for dept in departments_for_user
                if dept.path and dept.department_id in ctx.environment.placement_blocked_department_ids
            ]
            if blocked_department_ids:
                record_exception_skip(
                    stage_name="plan",
                    object_type="user",
                    operation_type="resolve_target_department",
                    exception_rule_type="skip_department_placement",
                    match_value=str(blocked_department_ids[0]),
                    reason=f"skip user {userid}: all eligible placement departments are blocked by skip_department_placement",
                    source_id=userid,
                    target_id=username,
                    details={
                        "userid": userid,
                        "ad_username": username,
                        "blocked_department_ids": blocked_department_ids,
                        "placement_reason": placement_reason,
                    },
                )
                continue
            ctx.hooks.record_event(
                "WARNING",
                "user_skipped",
                f"skip user {userid}: no eligible department for OU placement",
                stage_name="plan",
                payload={
                    "userid": userid,
                    "ad_username": username,
                    "placement_reason": placement_reason,
                },
            )
            ctx.hooks.record_operation(
                stage_name="plan",
                object_type="user",
                operation_type="resolve_target_department",
                status="skipped",
                message=f"skip user {userid}: no eligible department for OU placement",
                source_id=userid,
                target_id=username,
                reason_code=placement_reason,
                details={
                    "userid": userid,
                    "ad_username": username,
                    "placement_reason": placement_reason,
                },
            )
            continue

        user_detail = _get_source_user_detail_cached(ctx, userid, user=user)
        email = str(user_detail.get("email") or "").strip()
        connector_existing_users = existing_users_map_by_connector.get(connector_id, {})
        connector_enabled_usernames = set(enabled_ad_users_by_connector.get(connector_id, []))
        user_lifecycle_profile = build_user_lifecycle_profile(
            user,
            future_onboarding_start_field=ctx.policy_settings.future_onboarding_start_field,
            contractor_end_field=ctx.policy_settings.contractor_end_field,
            lifecycle_employment_type_field=ctx.policy_settings.lifecycle_employment_type_field,
            lifecycle_sponsor_field=ctx.policy_settings.lifecycle_sponsor_field,
            contractor_type_values=ctx.policy_settings.contractor_type_values,
        )
        lifecycle_now = datetime.now(timezone.utc)
        lifecycle_manager_userids = extract_manager_userids(user)
        lifecycle_payload = {
            "connector_id": connector_id,
            "ad_username": username,
            "employment_type": user_lifecycle_profile["employment_type"],
            "start_value": user_lifecycle_profile["start_value"],
            "end_value": user_lifecycle_profile["end_value"],
            "sponsor_userid": user_lifecycle_profile["sponsor_userid"],
            "manager_userids": lifecycle_manager_userids,
        }
        if (
            ctx.policy_settings.future_onboarding_enabled
            and user_lifecycle_profile["start_at"]
            and user_lifecycle_profile["start_at"] > lifecycle_now
        ):
            ctx.repositories.lifecycle_repo.upsert_pending_for_source_user(
                lifecycle_type="future_onboarding",
                connector_id=connector_id,
                source_user_id=userid,
                ad_username=username,
                effective_at=user_lifecycle_profile["start_at"].isoformat(timespec="seconds"),
                reason="future_start_date",
                employment_type=user_lifecycle_profile["employment_type"],
                sponsor_userid=user_lifecycle_profile["sponsor_userid"],
                manager_userids=lifecycle_manager_userids,
                payload=lifecycle_payload,
                last_job_id=ctx.job_id,
            )
            ctx.hooks.add_planned_operation(
                object_type="user",
                operation_type="queue_future_onboarding",
                source_id=userid,
                risk_level="normal",
                desired_state={
                    **lifecycle_payload,
                    "effective_at": user_lifecycle_profile["start_at"].isoformat(timespec="seconds"),
                    "reason": "future_start_date",
                },
            )
            ctx.hooks.record_event(
                "INFO",
                "future_onboarding_queued",
                f"queued future onboarding for user {userid} until {user_lifecycle_profile['start_at'].isoformat(timespec='seconds')}",
                stage_name="plan",
                payload=lifecycle_payload,
            )
            continue
        if (
            not ctx.policy_settings.future_onboarding_enabled
            or not user_lifecycle_profile["start_at"]
            or user_lifecycle_profile["start_at"] <= lifecycle_now
        ):
            ctx.repositories.lifecycle_repo.clear_pending_for_source_user(
                lifecycle_type="future_onboarding",
                connector_id=connector_id,
                source_user_id=userid,
            )

        if ctx.policy_settings.contractor_lifecycle_enabled and user_lifecycle_profile["is_contractor"]:
            if user_lifecycle_profile["end_at"] and user_lifecycle_profile["end_at"] > lifecycle_now:
                ctx.repositories.lifecycle_repo.upsert_pending_for_source_user(
                    lifecycle_type="contractor_expiry",
                    connector_id=connector_id,
                    source_user_id=userid,
                    ad_username=username,
                    effective_at=user_lifecycle_profile["end_at"].isoformat(timespec="seconds"),
                    reason="contractor_end_date",
                    employment_type=user_lifecycle_profile["employment_type"],
                    sponsor_userid=user_lifecycle_profile["sponsor_userid"],
                    manager_userids=lifecycle_manager_userids,
                    payload=lifecycle_payload,
                    last_job_id=ctx.job_id,
                )
            elif user_lifecycle_profile["end_at"] and user_lifecycle_profile["end_at"] <= lifecycle_now:
                ctx.repositories.lifecycle_repo.upsert_pending_for_source_user(
                    lifecycle_type="contractor_expiry",
                    connector_id=connector_id,
                    source_user_id=userid,
                    ad_username=username,
                    effective_at=user_lifecycle_profile["end_at"].isoformat(timespec="seconds"),
                    reason="contractor_expired",
                    employment_type=user_lifecycle_profile["employment_type"],
                    sponsor_userid=user_lifecycle_profile["sponsor_userid"],
                    manager_userids=lifecycle_manager_userids,
                    payload=lifecycle_payload,
                    last_job_id=ctx.job_id,
                )
                ctx.hooks.record_event(
                    "WARNING",
                    "contractor_expired",
                    f"detected expired contractor user {userid}; disable workflow will be applied",
                    stage_name="plan",
                    payload=lifecycle_payload,
                )
                if connector_existing_users.get(username) or username in connector_enabled_usernames:
                    if is_protected_ad_account(username, connector_id):
                        record_protected_account_skip(
                            stage_name="plan",
                            object_type="user",
                            operation_type="disable_user",
                            connector_id=connector_id,
                            ad_username=username,
                            source_id=userid,
                            risk_level="high",
                            details={
                                "userid": userid,
                                "reason": "contractor_expired",
                            },
                        )
                        continue
                    ctx.actions.disable_actions.append(
                        DisableUserAction(
                            connector_id=connector_id,
                            username=username,
                            source_user_id=userid,
                            reason="contractor_expired",
                            employment_type=user_lifecycle_profile["employment_type"],
                            sponsor_userid=user_lifecycle_profile["sponsor_userid"],
                            effective_at=user_lifecycle_profile["end_at"].isoformat(timespec="seconds"),
                        )
                    )
                    ctx.hooks.add_planned_operation(
                        object_type="user",
                        operation_type="disable_user",
                        source_id=userid,
                        risk_level="high",
                        desired_state={
                            **lifecycle_payload,
                            "effective_at": user_lifecycle_profile["end_at"].isoformat(timespec="seconds"),
                            "reason": "contractor_expired",
                        },
                    )
                else:
                    ctx.hooks.add_planned_operation(
                        object_type="user",
                        operation_type="skip_expired_user_without_ad_identity",
                        source_id=userid,
                        risk_level="normal",
                        desired_state={
                            **lifecycle_payload,
                            "effective_at": user_lifecycle_profile["end_at"].isoformat(timespec="seconds"),
                            "reason": "contractor_expired_without_existing_ad_identity",
                        },
                    )
                continue
            else:
                ctx.repositories.lifecycle_repo.clear_pending_for_source_user(
                    lifecycle_type="contractor_expiry",
                    connector_id=connector_id,
                    source_user_id=userid,
                )
        else:
            ctx.repositories.lifecycle_repo.clear_pending_for_source_user(
                lifecycle_type="contractor_expiry",
                connector_id=connector_id,
                source_user_id=userid,
            )

        if not email:
            email = f"{username}@{connector_domain}"

        effective_ou_path = get_effective_ou_path(target_dept, connector_id)
        ou_dn = connector_ad_sync.get_ou_dn(effective_ou_path)
        user.email = email
        user.departments = [dept.department_id for dept in departments_for_user]
        if connector_existing_users.get(username):
            operation_type = (
                "reactivate_user"
                if ctx.policy_settings.rehire_restore_enabled and username not in connector_enabled_usernames
                else "update_user"
            )
        else:
            operation_type = "create_user"
        if is_protected_ad_account(username, connector_id):
            record_protected_account_skip(
                stage_name="plan",
                object_type="user",
                operation_type=operation_type,
                connector_id=connector_id,
                ad_username=username,
                source_id=userid,
                details={
                    "userid": userid,
                    "placement_reason": placement_reason,
                },
            )
            continue
        ctx.actions.user_actions.append(
            UserAction(
                connector_id=connector_id,
                operation_type=operation_type,
                username=username,
                display_name=display_name,
                email=email,
                ou_dn=ou_dn,
                ou_path=list(effective_ou_path),
                target_department_id=target_dept.department_id,
                placement_reason=placement_reason,
                user=user,
                lifecycle_profile=user_lifecycle_profile,
            )
        )
        ctx.hooks.add_planned_operation(
            object_type="user",
            operation_type=operation_type,
            source_id=userid,
            department_id=str(target_dept.department_id),
            target_dn=f"CN={display_name},{ou_dn}",
            desired_state={
                "userid": userid,
                "connector_id": connector_id,
                "ad_username": username,
                "display_name": display_name,
                "email": email,
                "ou_path": effective_ou_path,
                "placement_reason": placement_reason,
                "binding_resolution": binding_resolution_details.get(userid, {}),
                "field_ownership_policy": dict(field_ownership_policy),
                "lifecycle_profile": {
                    "employment_type": user_lifecycle_profile["employment_type"],
                    "start_value": user_lifecycle_profile["start_value"],
                    "end_value": user_lifecycle_profile["end_value"],
                    "sponsor_userid": user_lifecycle_profile["sponsor_userid"],
                },
            },
        )
        connector_writeback_rules = (
            select_mapping_rules(
                ctx.policy_settings.enabled_mapping_rules,
                direction="ad_to_source",
                connector_id=connector_id,
            )
            if ctx.policy_settings.write_back_enabled
            else []
        )
        if connector_writeback_rules:
            ctx.hooks.add_planned_operation(
                object_type="user",
                operation_type="write_back_user",
                source_id=userid,
                department_id=str(target_dept.department_id),
                target_dn=f"CN={display_name},{ou_dn}",
                desired_state={
                    "connector_id": connector_id,
                    "ad_username": username,
                    "fields": [rule.target_field for rule in connector_writeback_rules],
                },
            )

        seen_group_sams: set[str] = set()
        if ctx.hooks.has_exception_rule("skip_user_group_membership", userid):
            record_exception_skip(
                stage_name="plan",
                object_type="group_membership",
                operation_type="add_user_to_group",
                exception_rule_type="skip_user_group_membership",
                match_value=userid,
                reason=f"skip managed group memberships for user {userid}: matched exception rule skip_user_group_membership",
                source_id=userid,
                target_id=username,
                details={"userid": userid, "ad_username": username},
            )
            continue
        for dept in departments_for_user:
            if not dept.path or is_department_excluded(dept):
                continue

            group_target = get_department_group_target(dept)
            if group_target.policy.is_excluded:
                record_group_policy_skip(
                    "plan",
                    "group_membership",
                    group_target,
                    f"skip user membership management for group {group_target.group_sam}",
                )
                continue

            group_sam = group_target.group_sam
            group_dn = group_target.group_dn
            if not group_sam or group_sam in seen_group_sams:
                continue

            group_connector_id = get_connector_id_for_department(dept)
            membership_key = (group_connector_id, username, group_sam)
            if membership_key in planned_memberships:
                continue

            seen_group_sams.add(group_sam)
            planned_memberships.add(membership_key)
            ctx.actions.membership_actions.append(
                GroupMembershipAction(
                    connector_id=group_connector_id,
                    source_user_id=userid,
                    username=username,
                    group_sam=group_sam,
                    group_dn=group_dn,
                    group_display_name=group_target.display_name,
                    department_id=dept.department_id,
                )
            )
            ctx.hooks.add_planned_operation(
                object_type="group_membership",
                operation_type="add_user_to_group",
                source_id=userid,
                department_id=str(dept.department_id),
                target_dn=group_dn,
                desired_state={
                    "connector_id": group_connector_id,
                    "ad_username": username,
                    "group_sam": group_sam,
                    "display_name": group_target.display_name,
                    "binding_resolution": binding_resolution_details.get(userid, {}),
                },
            )


def plan_disable_actions(
    ctx: SyncContext,
    *,
    is_protected_ad_account: Callable[[str, str], bool],
    record_exception_skip: Callable[..., None],
    record_protected_account_skip: Callable[..., None],
) -> None:
    all_enabled_binding_records = ctx.repositories.user_binding_repo.list_enabled_binding_records()
    all_enabled_binding_source_user_id_by_identity = {
        (record.connector_id or "default", record.ad_username): record.source_user_id
        for record in all_enabled_binding_records
        if record.ad_username and record.source_user_id
    }
    skip_sync_ad_identities = {
        (record.connector_id or "default", record.ad_username)
        for record in all_enabled_binding_records
        if record.ad_username
        and record.source_user_id
        and ctx.hooks.has_exception_rule("skip_user_sync", record.source_user_id)
    }
    managed_source_user_id_by_identity = {
        (record.connector_id or "default", record.ad_username): record.source_user_id
        for record in all_enabled_binding_records
        if record.ad_username
        and record.source_user_id
        and (record.connector_id or "default", record.ad_username) not in skip_sync_ad_identities
    }
    ctx.working.managed_ad_identities.clear()
    ctx.working.managed_ad_identities.update(set(managed_source_user_id_by_identity.keys()))

    for userid, resolution in ctx.identity.binding_resolution_details.items():
        if not resolution.get("ad_username"):
            continue
        ctx.repositories.offboarding_repo.clear_pending(
            connector_id=resolution.get("connector_id") or "default",
            ad_username=resolution["ad_username"],
        )

    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat(timespec="seconds")
    for connector_id, connector_enabled_users in ctx.working.enabled_ad_users_by_connector.items():
        connector_current_usernames = ctx.working.current_source_ad_usernames_by_connector.get(connector_id, set())
        for username in sorted(set(connector_enabled_users)):
            identity_key = (connector_id, username)
            skipped_source_user_id = all_enabled_binding_source_user_id_by_identity.get(identity_key, "")
            if identity_key in skip_sync_ad_identities:
                if not skipped_source_user_id or skipped_source_user_id in ctx.working.source_user_ids:
                    continue
                record_exception_skip(
                    stage_name="plan",
                    object_type="user",
                    operation_type="disable_user",
                    exception_rule_type="skip_user_sync",
                    match_value=skipped_source_user_id,
                    reason=f"skip disable for AD user {username}: matched exception rule skip_user_sync",
                    source_id=skipped_source_user_id,
                    target_id=username,
                    risk_level="high",
                    details={
                        "source_user_id": skipped_source_user_id,
                        "ad_username": username,
                        "connector_id": connector_id,
                    },
                )
                continue
            if identity_key not in ctx.working.managed_ad_identities or username in connector_current_usernames:
                continue

            managed_source_user_id = managed_source_user_id_by_identity.get(identity_key, "")
            if managed_source_user_id and ctx.hooks.has_exception_rule("skip_user_disable", managed_source_user_id):
                record_exception_skip(
                    stage_name="plan",
                    object_type="user",
                    operation_type="disable_user",
                    exception_rule_type="skip_user_disable",
                    match_value=managed_source_user_id,
                    reason=f"skip disable for AD user {username}: matched exception rule skip_user_disable",
                    source_id=managed_source_user_id,
                    target_id=username,
                    risk_level="high",
                    details={
                        "source_user_id": managed_source_user_id,
                        "ad_username": username,
                        "connector_id": connector_id,
                    },
                )
                continue

            if is_protected_ad_account(username, connector_id):
                record_protected_account_skip(
                    stage_name="plan",
                    object_type="user",
                    operation_type="disable_user",
                    connector_id=connector_id,
                    ad_username=username,
                    source_id=managed_source_user_id or username,
                    risk_level="high",
                    details={
                        "source_user_id": managed_source_user_id,
                        "reason": "missing_from_source",
                    },
                )
                continue

            pending_offboarding = ctx.repositories.offboarding_repo.get_record(
                connector_id=connector_id,
                ad_username=username,
            )
            if ctx.policy_settings.offboarding_grace_days > 0:
                due_at = pending_offboarding.due_at if pending_offboarding and pending_offboarding.status == "pending" else (
                    now_dt + timedelta(days=ctx.policy_settings.offboarding_grace_days)
                ).isoformat(timespec="seconds")
                manager_userids = _get_offboarding_manager_userids(ctx, managed_source_user_id)
                if not pending_offboarding or pending_offboarding.status != "pending":
                    ctx.repositories.offboarding_repo.upsert_pending_for_source_user(
                        connector_id=connector_id,
                        source_user_id=managed_source_user_id,
                        ad_username=username,
                        due_at=due_at,
                        reason="missing_from_source",
                        manager_userids=manager_userids,
                        last_job_id=ctx.job_id,
                    )
                if (
                    ctx.policy_settings.offboarding_notify_managers
                    and ctx.environment.bot
                    and (not pending_offboarding or not pending_offboarding.notified_at)
                ):
                    ctx.environment.bot.send_message(
                        f"## {ctx.environment.source_provider_name}-AD offboarding pending\n\n"
                        f"> Connector: {connector_id}\n"
                        f"> AD user: {username}\n"
                        f"> Source user: {managed_source_user_id or 'unknown'}\n"
                        f"> Grace period ends: {due_at}\n"
                        f"> Managers: {', '.join(manager_userids) if manager_userids else 'n/a'}"
                    )
                    ctx.repositories.offboarding_repo.mark_notified(connector_id=connector_id, ad_username=username)
                if str(due_at) > now_iso:
                    ctx.hooks.add_planned_operation(
                        object_type="user",
                        operation_type="queue_user_disable",
                        source_id=managed_source_user_id or username,
                        risk_level="normal",
                        desired_state={
                            "connector_id": connector_id,
                            "ad_username": username,
                            "reason": "pending_offboarding_grace",
                            "due_at": due_at,
                        },
                    )
                    continue

            ctx.actions.disable_actions.append(
                DisableUserAction(
                    connector_id=connector_id,
                    username=username,
                    source_user_id=managed_source_user_id,
                    reason="missing_from_source",
                )
            )
            ctx.hooks.add_planned_operation(
                object_type="user",
                operation_type="disable_user",
                source_id=managed_source_user_id or username,
                risk_level="high",
                desired_state={
                    "connector_id": connector_id,
                    "ad_username": username,
                    "reason": "missing_from_source",
                },
            )


def evaluate_disable_circuit_breaker(ctx: SyncContext) -> None:
    ctx.plan.disable_breaker_triggered = False
    ctx.plan.disable_breaker_threshold = 0
    ctx.plan.managed_user_baseline = max(
        int(ctx.sync_stats["total_users"] or 0),
        len(ctx.working.managed_ad_identities),
        len(ctx.working.enabled_ad_users_flat),
    )
    if not ctx.policy_settings.disable_breaker_enabled or ctx.plan.managed_user_baseline <= 0:
        return

    percent_threshold = math.ceil(
        ctx.plan.managed_user_baseline * (ctx.policy_settings.disable_breaker_percent / 100.0)
    )
    ctx.plan.disable_breaker_threshold = max(ctx.policy_settings.disable_breaker_min_count, percent_threshold)
    ctx.plan.disable_breaker_triggered = (
        len(ctx.actions.disable_actions) >= ctx.plan.disable_breaker_threshold > 0
    )
    if ctx.plan.disable_breaker_triggered:
        ctx.hooks.record_event(
            "WARNING",
            "disable_circuit_breaker",
            (
                f"disable circuit breaker triggered: {len(ctx.actions.disable_actions)} pending disables "
                f"exceeds threshold {ctx.plan.disable_breaker_threshold}"
            ),
            stage_name="plan",
            payload={
                "pending_disable_count": len(ctx.actions.disable_actions),
                "threshold_count": ctx.plan.disable_breaker_threshold,
                "total_users": ctx.sync_stats["total_users"],
                "managed_user_baseline": ctx.plan.managed_user_baseline,
                "percent_threshold": ctx.policy_settings.disable_breaker_percent,
            },
        )
