from __future__ import annotations

from datetime import datetime
from typing import Any

from sync_app.services.runtime_apply_phase import (
    apply_custom_group_actions,
    apply_department_actions,
    apply_disable_actions,
    apply_final_state_updates,
    apply_group_cleanup_actions,
    apply_group_hierarchy_actions,
    apply_group_membership_actions,
    apply_user_actions,
)
from sync_app.services.runtime_context import SyncContext, SyncExecutionServices
from sync_app.services.runtime_group_phase import (
    plan_directory_and_custom_groups,
    plan_group_relationship_cleanup,
)
from sync_app.services.runtime_plan import complete_plan_phase
from sync_app.services.runtime_source_phase import (
    collect_source_user_departments,
    resolve_identity_bindings_phase,
)
from sync_app.services.runtime_user_phase import (
    evaluate_disable_circuit_breaker,
    plan_disable_actions,
    plan_user_actions,
)


def run_planning_phase(
    ctx: SyncContext,
    *,
    services: SyncExecutionServices,
    field_ownership_policy: dict[str, Any],
    display_separator: str,
) -> tuple[Any, set[tuple[str, str, str]]]:
    collect_source_user_departments(ctx)
    resolve_identity_bindings_phase(
        ctx,
        get_connector_id_for_department=services.get_connector_id_for_department,
        get_connector_spec=services.get_connector_spec,
        get_ad_sync=services.get_ad_sync,
        is_protected_ad_account=services.is_protected_ad_account,
        record_exception_skip=services.record_exception_skip,
        record_protected_account_skip=services.record_protected_account_skip,
    )

    ctx.actions.department_actions.clear()
    ctx.actions.custom_group_actions.clear()
    ctx.actions.user_actions.clear()
    ctx.actions.membership_actions.clear()
    ctx.actions.group_hierarchy_actions.clear()
    ctx.actions.group_cleanup_actions.clear()
    ctx.actions.disable_actions.clear()

    planned_memberships = plan_directory_and_custom_groups(
        ctx,
        is_department_excluded=services.is_department_excluded,
        get_connector_id_for_department=services.get_connector_id_for_department,
        get_ad_sync=services.get_ad_sync,
        record_group_policy_skip=services.record_group_policy_skip,
        display_separator=display_separator,
    )
    plan_user_actions(
        ctx,
        planned_memberships=planned_memberships,
        is_department_excluded=services.is_department_excluded,
        is_department_blocked_for_placement=services.is_department_blocked_for_placement,
        get_connector_id_for_department=services.get_connector_id_for_department,
        get_connector_spec=services.get_connector_spec,
        get_ad_sync=services.get_ad_sync,
        get_department_group_target=services.get_department_group_target,
        is_protected_ad_account=services.is_protected_ad_account,
        record_exception_skip=services.record_exception_skip,
        record_protected_account_skip=services.record_protected_account_skip,
        record_group_policy_skip=services.record_group_policy_skip,
        field_ownership_policy=field_ownership_policy,
    )
    planned_hierarchy_pairs = plan_group_relationship_cleanup(
        ctx,
        is_department_excluded=services.is_department_excluded,
        get_connector_id_for_department=services.get_connector_id_for_department,
        get_ad_sync=services.get_ad_sync,
        record_group_policy_skip=services.record_group_policy_skip,
        record_skip_detail=services.record_skip_detail,
        record_exception_skip=services.record_exception_skip,
        display_separator=display_separator,
    )
    plan_disable_actions(
        ctx,
        is_protected_ad_account=services.is_protected_ad_account,
        record_exception_skip=services.record_exception_skip,
        record_protected_account_skip=services.record_protected_account_skip,
    )
    evaluate_disable_circuit_breaker(ctx)
    return complete_plan_phase(ctx), planned_hierarchy_pairs


def run_apply_phase(
    ctx: SyncContext,
    *,
    services: SyncExecutionServices,
    field_ownership_policy: dict[str, Any],
    display_separator: str,
    planned_hierarchy_pairs: set[tuple[str, str, str]],
) -> None:
    if ctx.environment.bot:
        ctx.environment.bot.send_message(
            f"## {ctx.environment.source_provider_name}-AD sync started (LDAPS)\n\n"
            f"> Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"> Domain: {ctx.config.domain}\n"
            f"> LDAP server: {ctx.config.ldap.server}\n"
            f"> SSL: {'yes' if ctx.config.ldap.use_ssl else 'no'}"
        )

    apply_department_actions(
        ctx,
        get_ad_sync=services.get_ad_sync,
        display_separator=display_separator,
        record_group_policy_skip=services.record_group_policy_skip,
    )

    successful_hierarchy_pairs = apply_group_hierarchy_actions(
        ctx,
        get_ad_sync=services.get_ad_sync,
        record_group_policy_skip=services.record_group_policy_skip,
    )

    apply_user_actions(
        ctx,
        get_ad_sync=services.get_ad_sync,
        field_ownership_policy=field_ownership_policy,
    )

    apply_custom_group_actions(
        ctx,
        get_ad_sync=services.get_ad_sync,
    )

    apply_group_membership_actions(
        ctx,
        get_ad_sync=services.get_ad_sync,
        record_exception_skip=services.record_exception_skip,
        record_group_policy_skip=services.record_group_policy_skip,
    )

    apply_group_cleanup_actions(
        ctx,
        get_ad_sync=services.get_ad_sync,
        planned_hierarchy_pairs=planned_hierarchy_pairs,
        successful_hierarchy_pairs=successful_hierarchy_pairs,
        record_exception_skip=services.record_exception_skip,
        record_group_policy_skip=services.record_group_policy_skip,
        record_skip_detail=services.record_skip_detail,
    )

    apply_disable_actions(
        ctx,
        get_ad_sync=services.get_ad_sync,
        record_exception_skip=services.record_exception_skip,
    )

    apply_final_state_updates(ctx)
