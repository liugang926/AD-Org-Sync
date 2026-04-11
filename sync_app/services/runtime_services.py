from __future__ import annotations

from typing import Any, Dict, List, Optional

from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.core.exception_rules import get_exception_rule_match_type, normalize_exception_match_value
from sync_app.core.models import DepartmentNode, GroupPolicyEvaluation, ManagedGroupTarget
from sync_app.services.runtime_context import SyncContext, SyncExecutionServices
from sync_app.services.runtime_group_phase import (
    get_department_group_target as resolve_department_group_target,
    get_effective_parent_department_id as resolve_effective_parent_department_id,
)
from sync_app.services.runtime_connectors import is_department_in_connector_scope


def build_execution_services(
    ctx: SyncContext,
    *,
    enabled_group_rules: list[Any],
    exception_match_values_by_rule_type: dict[str, set[str]],
    display_separator: str,
) -> SyncExecutionServices:
    sync_stats = ctx.sync_stats
    dept_tree = ctx.environment.dept_tree
    ad_sync_clients = ctx.environment.ad_sync_clients
    default_ad_sync = ad_sync_clients["default"]
    connector_specs_by_id = ctx.environment.connector_specs_by_id
    protected_ad_accounts_by_connector = ctx.environment.protected_ad_accounts_by_connector
    department_connector_map = ctx.environment.department_connector_map
    department_scope_root_map = ctx.environment.department_scope_root_map
    excluded_department_names = ctx.environment.excluded_department_names
    placement_blocked_department_ids = ctx.environment.placement_blocked_department_ids
    policy_skip_markers = ctx.environment.policy_skip_markers
    exception_rule_repo = ctx.repositories.exception_rule_repo

    def is_department_excluded(dept_info: Optional[DepartmentNode]) -> bool:
        return (
            not dept_info
            or dept_info.name in excluded_department_names
            or not is_department_in_connector_scope(
                dept_info,
                connector_specs_by_id=connector_specs_by_id,
                department_connector_map=department_connector_map,
                department_scope_root_map=department_scope_root_map,
            )
        )

    def get_connector_id_for_department(dept_info: Optional[DepartmentNode]) -> str:
        if not dept_info:
            return "default"
        return department_connector_map.get(dept_info.department_id, "default")

    def get_connector_spec(connector_id: str) -> dict[str, Any]:
        return connector_specs_by_id.get(connector_id, connector_specs_by_id["default"])

    def get_ad_sync(connector_id: str) -> Any:
        return ad_sync_clients.get(connector_id, default_ad_sync)

    def is_protected_ad_account(username: str, connector_id: str) -> bool:
        return is_protected_ad_account_name(
            username,
            protected_ad_accounts_by_connector.get(connector_id, set()),
        )

    def is_department_blocked_for_placement(dept_info: Optional[DepartmentNode]) -> bool:
        return is_department_excluded(dept_info) or (
            bool(dept_info) and dept_info.department_id in placement_blocked_department_ids
        )

    def record_skip_detail(
        *,
        stage_name: str,
        action_type: str,
        group_sam: Optional[str],
        group_dn: Optional[str],
        reason: str,
        matched_rules: Optional[List[str]] = None,
    ) -> None:
        skipped_summary = sync_stats["skipped_operations"]
        skipped_summary["total"] += 1
        skipped_summary["by_action"][action_type] = skipped_summary["by_action"].get(action_type, 0) + 1

        detail = {
            "stage": stage_name,
            "action_type": action_type,
            "group_sam": group_sam,
            "group_dn": group_dn,
            "reason": reason,
            "matched_rules": matched_rules or [],
        }
        if len(skipped_summary["samples"]) < 20:
            skipped_summary["samples"].append(detail)
        if len(skipped_summary["details"]) < 1000:
            skipped_summary["details"].append(detail)
        ctx.hooks.record_operation(
            stage_name=stage_name,
            object_type="group",
            operation_type=action_type,
            status="skipped",
            message=reason,
            source_id=group_sam,
            target_dn=group_dn,
            risk_level="normal",
            reason_code="policy_skip",
            details=detail,
        )

    def record_group_policy_skip(stage_name: str, action_type: str, group_target: ManagedGroupTarget, reason: str) -> None:
        marker = (
            stage_name,
            action_type,
            group_target.group_sam,
            group_target.group_dn,
        )
        if marker in policy_skip_markers:
            return

        policy_skip_markers.add(marker)
        matched_rules = group_target.policy.matched_rule_labels()
        record_skip_detail(
            stage_name=stage_name,
            action_type=action_type,
            group_sam=group_target.group_sam,
            group_dn=group_target.group_dn,
            reason=reason,
            matched_rules=matched_rules,
        )
        ctx.hooks.record_event(
            "WARNING" if group_target.policy.is_hard_protected else "INFO",
            f"{action_type}_skipped",
            reason,
            stage_name=stage_name,
            payload={
                "group_sam": group_target.group_sam,
                "group_dn": group_target.group_dn,
                "display_name": group_target.display_name,
                "matched_rules": matched_rules,
            },
        )

    def record_protected_account_skip(
        *,
        stage_name: str,
        object_type: str,
        operation_type: str,
        connector_id: str,
        ad_username: str,
        source_id: Optional[str] = None,
        target_id: Optional[str] = None,
        risk_level: str = "normal",
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        message = f"skip {operation_type} for protected AD account {ad_username}"
        payload = {
            "connector_id": connector_id,
            "ad_username": ad_username,
            "protected_accounts": sorted(protected_ad_accounts_by_connector.get(connector_id, set())),
        }
        if details:
            payload.update(details)
        ctx.hooks.record_event(
            "WARNING",
            "protected_ad_account_skip",
            message,
            stage_name=stage_name,
            payload=payload,
        )
        ctx.hooks.record_operation(
            stage_name=stage_name,
            object_type=object_type,
            operation_type=operation_type,
            status="skipped",
            message=message,
            source_id=source_id,
            target_id=target_id or ad_username,
            risk_level=risk_level,
            rule_source="system_protected_account",
            reason_code="protected_ad_account",
            details=payload,
        )

    def record_exception_skip(
        *,
        stage_name: str,
        object_type: str,
        operation_type: str,
        exception_rule_type: str,
        match_value: str,
        reason: str,
        source_id: Optional[str] = None,
        department_id: Optional[str] = None,
        target_id: Optional[str] = None,
        target_dn: Optional[str] = None,
        risk_level: str = "normal",
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        skipped_summary = sync_stats["skipped_operations"]
        skipped_summary["total"] += 1
        skipped_summary["by_action"][operation_type] = skipped_summary["by_action"].get(operation_type, 0) + 1

        detail = {
            "stage": stage_name,
            "action_type": operation_type,
            "object_type": object_type,
            "source_id": source_id,
            "department_id": department_id,
            "target_id": target_id,
            "target_dn": target_dn,
            "reason": reason,
            "exception_rule_type": exception_rule_type,
            "match_value": match_value,
        }
        if details:
            detail.update(details)
        if len(skipped_summary["samples"]) < 20:
            skipped_summary["samples"].append(detail)
        if len(skipped_summary["details"]) < 1000:
            skipped_summary["details"].append(detail)
        exception_rule_repo.consume_rule(
            rule_type=exception_rule_type,
            match_value=match_value,
        )

        ctx.hooks.record_operation(
            stage_name=stage_name,
            object_type=object_type,
            operation_type=operation_type,
            status="skipped",
            message=reason,
            source_id=source_id,
            department_id=department_id,
            target_id=target_id,
            target_dn=target_dn,
            risk_level=risk_level,
            rule_source=exception_rule_type,
            reason_code="exception_rule",
            details=detail,
        )
        ctx.hooks.record_event(
            "INFO",
            "exception_rule_skip",
            reason,
            stage_name=stage_name,
            payload=detail,
        )

    def get_department_group_target(dept_info: DepartmentNode) -> ManagedGroupTarget:
        return resolve_department_group_target(
            ctx,
            dept_info,
            get_connector_id_for_department=get_connector_id_for_department,
            get_ad_sync=get_ad_sync,
            display_separator=display_separator,
        )

    def get_effective_parent_department_id(dept_info: DepartmentNode) -> Optional[int]:
        return resolve_effective_parent_department_id(
            ctx,
            dept_info,
            is_department_excluded=is_department_excluded,
        )

    return SyncExecutionServices(
        is_department_excluded=is_department_excluded,
        get_connector_id_for_department=get_connector_id_for_department,
        get_connector_spec=get_connector_spec,
        get_ad_sync=get_ad_sync,
        is_protected_ad_account=is_protected_ad_account,
        is_department_blocked_for_placement=is_department_blocked_for_placement,
        record_group_policy_skip=record_group_policy_skip,
        record_skip_detail=record_skip_detail,
        record_protected_account_skip=record_protected_account_skip,
        record_exception_skip=record_exception_skip,
        get_department_group_target=get_department_group_target,
        get_effective_parent_department_id=get_effective_parent_department_id,
    )


def evaluate_group_policy(
    *,
    enabled_group_rules: list[Any],
    group_sam: Optional[str] = None,
    group_dn: Optional[str] = None,
    display_name: Optional[str] = None,
) -> GroupPolicyEvaluation:
    matched_rules: List[Dict[str, Any]] = []
    for rule in enabled_group_rules:
        match_type = (rule.get("match_type") or "").strip().lower()
        match_value = (rule.get("match_value") or "").strip()
        is_match = False

        if match_type == "samaccountname" and group_sam:
            is_match = group_sam.lower() == match_value.lower()
        elif match_type == "dn" and group_dn:
            is_match = group_dn.lower() == match_value.lower()
        elif match_type == "display_name" and display_name:
            is_match = display_name.lower() == match_value.lower()

        if is_match:
            matched_rules.append(rule.to_dict())

    is_hard_protected = any(
        rule.get("rule_type") == "protect" and rule.get("protection_level") == "hard"
        for rule in matched_rules
    )
    is_excluded = is_hard_protected or any(rule.get("rule_type") == "exclude" for rule in matched_rules)
    return GroupPolicyEvaluation(
        is_hard_protected=is_hard_protected,
        is_excluded=is_excluded,
        matched_rules=matched_rules,
    )


def has_exception_rule(
    *,
    exception_match_values_by_rule_type: dict[str, set[str]],
    rule_type: str,
    match_value: Optional[str],
) -> bool:
    normalized_rule_type = str(rule_type or "").strip().lower()
    normalized_match_type = get_exception_rule_match_type(normalized_rule_type)
    normalized_match_value = normalize_exception_match_value(normalized_match_type, match_value)
    if not normalized_rule_type or not normalized_match_type or not normalized_match_value:
        return False
    return normalized_match_value in exception_match_values_by_rule_type.get(normalized_rule_type, set())
