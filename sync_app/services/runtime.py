import csv
import hashlib
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sync_app.clients.wechat_bot import WeChatBot
from sync_app.clients.wecom import WeComAPI
from sync_app.core import logging_utils as sync_logging
from sync_app.core.common import APP_VERSION, format_time_duration, generate_job_id, hash_department_state
from sync_app.core.config import (
    load_sync_config,
    run_config_security_self_check,
    test_ldap_connection,
    test_wecom_connection,
    validate_config,
)
from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.core.exception_rules import (
    get_exception_rule_match_type,
    normalize_exception_match_value,
)
from sync_app.core.sync_policies import (
    build_ad_to_wecom_mapping_payload,
    build_identity_candidates as build_identity_candidates_from_policy,
    build_wecom_to_ad_mapping_payload,
    extract_manager_userids,
    normalize_mapping_direction,
    normalize_group_type,
    render_template,
)
from sync_app.core.models import (
    AttributeMappingRuleRecord,
    CustomManagedGroupBindingRecord,
    DepartmentGroupInfo,
    DepartmentNode,
    DepartmentAction,
    DirectoryGroupRecord,
    DisableUserAction,
    GroupCleanupAction,
    GroupHierarchyAction,
    GroupMembershipAction,
    GroupPolicyEvaluation,
    ManagedGroupTarget,
    SourceDirectoryUser,
    SyncRunStats,
    SyncJobSummary,
    UserAction,
    UserDepartmentBundle,
)
from sync_app.providers.source import build_source_provider, get_source_provider_display_name
from sync_app.services.ad_sync import (
    ADSyncLDAPS,
    build_custom_group_sam,
    build_group_cn,
    build_group_display_name,
)
from sync_app.services.reports import (
    _generate_skip_detail_report,
    _generate_sync_operation_log,
    _generate_sync_validation_report,
)
from sync_app.services.state import SyncStateManager
from sync_app.storage.local_db import (
    DatabaseManager,
    GroupExclusionRuleRepository,
    ManagedGroupBindingRepository,
    ObjectStateRepository,
    OrganizationConfigRepository,
    OrganizationRepository,
    PlannedOperationRepository,
    SettingsRepository,
    SyncConnectorRepository,
    SyncConflictRepository,
    SyncExceptionRuleRepository,
    SyncEventRepository,
    SyncJobRepository,
    SyncOperationLogRepository,
    AttributeMappingRuleRepository,
    CustomManagedGroupBindingRepository,
    OffboardingQueueRepository,
    SyncReplayRequestRepository,
    SyncPlanReviewRepository,
    UserDepartmentOverrideRepository,
    UserIdentityBindingRepository,
    UserLifecycleQueueRepository,
)


FIELD_OWNERSHIP_POLICY = {
    'display_name': 'wecom_authoritative',
    'email': 'initialize_if_missing_then_preserve_ad',
    'user_principal_name': 'sync_managed',
    'account_status': 'managed_scope_follows_wecom_membership',
    'ou_placement': 'department_override_then_wecom_strategy',
    'group_membership': 'managed_department_groups_only',
    'custom_attribute_mapping': 'connector_specific_wecom_to_ad_rules',
    'write_back': 'configured_ad_to_wecom_rules_after_apply',
    'connector_routing': 'top_level_department_connector_assignment',
    'offboarding': 'wecom_departure_enters_grace_period_before_disable',
    'group_type': 'connector_specific_security_distribution_policy',
    'disable_circuit_breaker': 'block_apply_when_disable_threshold_exceeds_policy',
}

IDENTITY_RULE_PRIORITY = (
    'manual_binding',
    'existing_binding',
    'existing_ad_userid',
    'existing_ad_email_localpart',
    'derived_default_userid',
)

HIGH_RISK_OPERATION_TYPES = {
    'disable_user',
    'remove_group_from_group',
}


def _compute_plan_fingerprint(items: list[dict[str, Any]]) -> str:
    normalized = []
    for item in items:
        normalized.append(
            {
                'object_type': str(item.get('object_type') or ''),
                'operation_type': str(item.get('operation_type') or ''),
                'source_id': str(item.get('source_id') or ''),
                'department_id': str(item.get('department_id') or ''),
                'target_dn': str(item.get('target_dn') or ''),
                'risk_level': str(item.get('risk_level') or 'normal'),
            }
        )
    normalized = sorted(
        normalized,
        key=lambda item: (
            item['object_type'],
            item['operation_type'],
            item['source_id'],
            item['department_id'],
            item['target_dn'],
            item['risk_level'],
        ),
    )
    payload = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _build_identity_candidates(user: SourceDirectoryUser, *, username_template: str = "") -> list[dict[str, str]]:
    return build_identity_candidates_from_policy(user, username_template=username_template)


def _resolve_target_department(
    bundle: UserDepartmentBundle,
    *,
    placement_strategy: str,
    is_department_excluded,
    override_department_id: Optional[int] = None,
) -> tuple[Optional[DepartmentNode], str]:
    valid_departments = [
        department
        for department in bundle.departments
        if department.path and not is_department_excluded(department)
    ]
    if not valid_departments:
        return None, 'all_departments_excluded'

    departments_by_id = {
        department.department_id: department for department in valid_departments
    }

    if override_department_id is not None and override_department_id in departments_by_id:
        return departments_by_id[override_department_id], 'manual_override'

    strategy = (placement_strategy or 'wecom_primary_department').strip().lower()
    if strategy == 'wecom_primary_department':
        declared_primary_id = bundle.user.declared_primary_department_id()
        if declared_primary_id is not None and declared_primary_id in departments_by_id:
            return departments_by_id[declared_primary_id], 'wecom_primary_department'

    if strategy == 'lowest_department_id':
        department = min(valid_departments, key=lambda item: (item.department_id, len(item.path_ids), item.name))
        return department, 'lowest_department_id'

    if strategy == 'shortest_path':
        department = min(valid_departments, key=lambda item: (len(item.path_ids), item.department_id, item.name))
        return department, 'shortest_path'

    department = sorted(valid_departments, key=lambda item: (item.department_id, len(item.path_ids), item.name))[0]
    return department, 'first_non_excluded_department'


def _load_connector_specs(
    config,
    connector_repo: SyncConnectorRepository,
    *,
    connectors_enabled: bool = False,
    org_id: str = 'default',
) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = [
        {
            'connector_id': 'default',
            'org_id': org_id,
            'name': 'Default Connector',
            'config_path': config.config_path,
            'root_department_ids': [],
            'username_template': '',
            'disabled_users_ou': 'Disabled Users',
            'group_type': 'security',
            'group_mail_domain': '',
            'custom_group_ou_path': 'Managed Groups',
            'managed_tag_ids': [],
            'managed_external_chat_ids': [],
            'config': config,
        }
    ]
    if not connectors_enabled:
        return specs
    for record in connector_repo.list_connector_records(enabled_only=True, org_id=org_id):
        connector_config = connector_repo.get_connector_app_config(
            record.connector_id,
            base_config=config,
            org_id=org_id,
        )
        if connector_config is None:
            connector_config = load_sync_config(record.config_path)
        specs.append(
            {
                'connector_id': record.connector_id,
                'org_id': record.org_id,
                'name': record.name,
                'config_path': record.config_path,
                'root_department_ids': list(record.root_department_ids),
                'username_template': record.username_template,
                'disabled_users_ou': record.disabled_users_ou or 'Disabled Users',
                'group_type': normalize_group_type(record.group_type),
                'group_mail_domain': record.group_mail_domain,
                'custom_group_ou_path': record.custom_group_ou_path,
                'managed_tag_ids': list(record.managed_tag_ids),
                'managed_external_chat_ids': list(record.managed_external_chat_ids),
                'config': connector_config,
            }
        )
    return specs


def _build_department_connector_map(
    dept_tree: dict[int, DepartmentNode],
    connector_specs: list[dict[str, Any]],
) -> dict[int, str]:
    mapping: dict[int, str] = {}
    explicit_root_departments = {
        int(root_id): spec['connector_id']
        for spec in connector_specs
        for root_id in spec.get('root_department_ids') or []
        if str(root_id).strip()
    }
    for department_id, department in dept_tree.items():
        selected_connector_id = 'default'
        for ancestor_id in department.path_ids:
            if ancestor_id in explicit_root_departments:
                selected_connector_id = explicit_root_departments[ancestor_id]
                break
        mapping[department_id] = selected_connector_id
    return mapping


def _select_mapping_rules(
    rules: list[AttributeMappingRuleRecord],
    *,
    direction: str,
    connector_id: str,
) -> list[AttributeMappingRuleRecord]:
    normalized_direction = normalize_mapping_direction(direction)
    return [
        rule
        for rule in rules
        if rule.is_enabled
        and normalize_mapping_direction(rule.direction) == normalized_direction
        and (not rule.connector_id or rule.connector_id == connector_id)
    ]


def _sanitize_wecom_writeback_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in dict(payload or {}).items():
        if value in (None, ''):
            continue
        if key == 'department':
            continue
        sanitized[key] = value
    return sanitized


def run_sync_job(
    stats_callback=None,
    cancel_flag=None,
    execution_mode: str = 'apply',
    trigger_type: str = 'manual',
    db_path: Optional[str] = None,
    config_path: str = 'config.ini',
    org_id: str = 'default',
):
    start_time = time.time()
    execution_mode = (execution_mode or 'apply').strip().lower()
    if execution_mode not in {'apply', 'dry_run'}:
        raise ValueError(f"unsupported execution_mode: {execution_mode}")
    org_id = str(org_id or '').strip().lower() or 'default'

    def is_cancelled():
        return bool(cancel_flag and getattr(cancel_flag, 'is_cancelled', False))

    sync_stats = SyncRunStats(execution_mode=execution_mode)
    sync_stats['field_ownership_policy'] = dict(FIELD_OWNERSHIP_POLICY)

    logger = sync_logging.setup_logging()
    sync_stats['log_file'] = sync_logging.log_filename

    db_manager = DatabaseManager(db_path=db_path)
    db_init_result = db_manager.initialize()
    sync_stats['db_path'] = db_manager.db_path
    sync_stats['db_backup_dir'] = db_manager.backup_dir
    sync_stats['db_startup_snapshot_path'] = db_init_result.get('startup_snapshot_path') or ''
    sync_stats['db_migration_source_path'] = db_init_result.get('migration_source_path') or ''
    sync_stats['db_integrity_check'] = db_init_result.get('integrity_check') or {}

    settings_repo = SettingsRepository(db_manager)
    organization_repo = OrganizationRepository(db_manager)
    organization_config_repo = OrganizationConfigRepository(db_manager)
    exclusion_repo = GroupExclusionRuleRepository(db_manager)
    connector_repo = SyncConnectorRepository(db_manager)
    mapping_rule_repo = AttributeMappingRuleRepository(db_manager)
    job_repo = SyncJobRepository(db_manager)
    event_repo = SyncEventRepository(db_manager)
    plan_repo = PlannedOperationRepository(db_manager)
    operation_log_repo = SyncOperationLogRepository(db_manager)
    conflict_repo = SyncConflictRepository(db_manager)
    review_repo = SyncPlanReviewRepository(db_manager)
    organization_repo.ensure_default(config_path=config_path)
    organization = organization_repo.get_organization_record(org_id)
    if not organization:
        raise ValueError(f"organization not found: {org_id}")
    if not organization.is_enabled:
        raise ValueError(f"organization is disabled: {org_id}")
    exclusion_repo = GroupExclusionRuleRepository(db_manager, default_org_id=organization.org_id)
    binding_repo = ManagedGroupBindingRepository(db_manager, default_org_id=organization.org_id)
    user_binding_repo = UserIdentityBindingRepository(db_manager, default_org_id=organization.org_id)
    department_override_repo = UserDepartmentOverrideRepository(db_manager, default_org_id=organization.org_id)
    custom_group_binding_repo = CustomManagedGroupBindingRepository(db_manager, default_org_id=organization.org_id)
    offboarding_repo = OffboardingQueueRepository(db_manager, default_org_id=organization.org_id)
    lifecycle_repo = UserLifecycleQueueRepository(db_manager, default_org_id=organization.org_id)
    replay_request_repo = SyncReplayRequestRepository(db_manager, default_org_id=organization.org_id)
    state_repo = ObjectStateRepository(db_manager, default_org_id=organization.org_id)
    exception_rule_repo = SyncExceptionRuleRepository(db_manager, default_org_id=organization.org_id)
    state_manager = SyncStateManager(db_manager=db_manager, org_id=organization.org_id)
    resolved_config_path = organization.config_path or config_path
    sync_stats['org_id'] = organization.org_id
    sync_stats['organization_name'] = organization.name
    sync_stats['organization_config_path'] = f"db:org:{organization.org_id}"

    def get_org_setting_value(key: str, default: Optional[str] = None) -> Optional[str]:
        return settings_repo.get_value(key, default, org_id=organization.org_id)

    def get_org_setting_bool(key: str, default: bool = False) -> bool:
        return settings_repo.get_bool(key, default, org_id=organization.org_id)

    def get_org_setting_int(key: str, default: int = 0) -> int:
        return settings_repo.get_int(key, default, org_id=organization.org_id)

    def get_org_setting_float(key: str, default: float = 0.0) -> float:
        return settings_repo.get_float(key, default, org_id=organization.org_id)

    enabled_group_rules = exclusion_repo.list_enabled_rule_records()
    enabled_exception_rules = exception_rule_repo.list_enabled_rule_records()
    connector_routing_enabled = get_org_setting_bool('advanced_connector_routing_enabled', False)
    attribute_mapping_enabled = get_org_setting_bool('attribute_mapping_enabled', False)
    write_back_enabled = get_org_setting_bool('write_back_enabled', False)
    custom_group_sync_enabled = get_org_setting_bool('custom_group_sync_enabled', False)
    offboarding_lifecycle_enabled = get_org_setting_bool('offboarding_lifecycle_enabled', False)
    field_conflict_queue_enabled = get_org_setting_bool('field_conflict_queue_enabled', False)
    rehire_restore_enabled = get_org_setting_bool('rehire_restore_enabled', False)
    custom_group_archive_enabled = get_org_setting_bool('custom_group_archive_enabled', False)
    scheduled_review_execution_enabled = get_org_setting_bool('scheduled_review_execution_enabled', False)
    automatic_replay_enabled = get_org_setting_bool('automatic_replay_enabled', False)
    future_onboarding_enabled = get_org_setting_bool('future_onboarding_enabled', False)
    future_onboarding_start_field = get_org_setting_value('future_onboarding_start_field', 'hire_date') or 'hire_date'
    contractor_lifecycle_enabled = get_org_setting_bool('contractor_lifecycle_enabled', False)
    lifecycle_employment_type_field = (
        get_org_setting_value('lifecycle_employment_type_field', 'employment_type') or 'employment_type'
    )
    contractor_end_field = get_org_setting_value('contractor_end_field', 'contract_end_date') or 'contract_end_date'
    lifecycle_sponsor_field = get_org_setting_value('lifecycle_sponsor_field', 'sponsor_userid') or 'sponsor_userid'
    contractor_type_values = {
        str(value).strip().lower()
        for value in str(get_org_setting_value('contractor_type_values', 'contractor,intern,vendor,temp') or '')
        .split(',')
        if str(value).strip()
    }
    enabled_mapping_rules = mapping_rule_repo.list_rule_records(enabled_only=True, org_id=organization.org_id)
    group_recursive_enabled = get_org_setting_bool('group_recursive_enabled', True)
    managed_relation_cleanup_enabled = get_org_setting_bool('managed_relation_cleanup_enabled', False)
    user_ou_placement_strategy = get_org_setting_value(
        'user_ou_placement_strategy',
        'wecom_primary_department',
    ) or 'wecom_primary_department'
    offboarding_grace_days = max(get_org_setting_int('offboarding_grace_days', 0), 0)
    offboarding_notify_managers = get_org_setting_bool('offboarding_notify_managers', False)
    if not offboarding_lifecycle_enabled:
        offboarding_grace_days = 0
        offboarding_notify_managers = False
    disable_breaker_enabled = get_org_setting_bool('disable_circuit_breaker_enabled', False)
    disable_breaker_percent = max(get_org_setting_float('disable_circuit_breaker_percent', 5.0), 0.0)
    disable_breaker_min_count = max(get_org_setting_int('disable_circuit_breaker_min_count', 10), 0)
    disable_breaker_requires_approval = get_org_setting_bool(
        'disable_circuit_breaker_requires_approval',
        True,
    )
    global_group_type = normalize_group_type(get_org_setting_value('managed_group_type', 'security'))
    global_group_mail_domain = get_org_setting_value('managed_group_mail_domain', '') or ''
    global_custom_group_ou_path = get_org_setting_value('custom_group_ou_path', 'Managed Groups') or 'Managed Groups'
    exception_match_values_by_rule_type: Dict[str, set[str]] = {}
    for rule in enabled_exception_rules:
        exception_match_values_by_rule_type.setdefault(rule.rule_type, set()).add(rule.match_value)

    active_job = job_repo.get_active_job_record()
    if active_job:
        raise RuntimeError(f"active sync job already exists: {active_job.job_id}")

    if organization_config_repo.has_config(organization.org_id) or os.path.exists(resolved_config_path):
        config = organization_config_repo.get_app_config(organization.org_id, config_path=resolved_config_path)
    else:
        config = load_sync_config(resolved_config_path)
    sync_stats['organization_config_path'] = config.config_path
    config_snapshot_payload = {
        'organization': organization.to_dict(),
        'primary_config': config.to_hash_payload(),
        'connectors': [
            record.to_dict()
            for record in connector_repo.list_connector_records(enabled_only=True, org_id=organization.org_id)
        ],
        'attribute_mappings': [record.to_dict() for record in enabled_mapping_rules],
        'settings': {
            'advanced_connector_routing_enabled': connector_routing_enabled,
            'attribute_mapping_enabled': attribute_mapping_enabled,
            'write_back_enabled': write_back_enabled,
            'custom_group_sync_enabled': custom_group_sync_enabled,
            'offboarding_lifecycle_enabled': offboarding_lifecycle_enabled,
            'field_conflict_queue_enabled': field_conflict_queue_enabled,
            'rehire_restore_enabled': rehire_restore_enabled,
            'custom_group_archive_enabled': custom_group_archive_enabled,
            'scheduled_review_execution_enabled': scheduled_review_execution_enabled,
            'automatic_replay_enabled': automatic_replay_enabled,
            'future_onboarding_enabled': future_onboarding_enabled,
            'future_onboarding_start_field': future_onboarding_start_field,
            'contractor_lifecycle_enabled': contractor_lifecycle_enabled,
            'lifecycle_employment_type_field': lifecycle_employment_type_field,
            'contractor_end_field': contractor_end_field,
            'lifecycle_sponsor_field': lifecycle_sponsor_field,
            'contractor_type_values': sorted(contractor_type_values),
            'group_recursive_enabled': group_recursive_enabled,
            'managed_relation_cleanup_enabled': managed_relation_cleanup_enabled,
            'user_ou_placement_strategy': user_ou_placement_strategy,
            'offboarding_grace_days': offboarding_grace_days,
            'disable_circuit_breaker_percent': disable_breaker_percent,
            'disable_circuit_breaker_min_count': disable_breaker_min_count,
            'managed_group_type': global_group_type,
            'managed_group_mail_domain': global_group_mail_domain,
            'custom_group_ou_path': global_custom_group_ou_path,
        },
    }
    config_hash = hashlib.md5(
        json.dumps(config_snapshot_payload, ensure_ascii=False, sort_keys=True).encode('utf-8')
    ).hexdigest()
    display_separator = get_org_setting_value('group_display_separator', '-') or '-'
    job_id = generate_job_id()
    sync_stats['job_id'] = job_id
    planned_count = 0
    executed_count = 0
    high_risk_operation_count = 0
    plan_fingerprint_items: list[dict[str, Any]] = []
    started_replay_requests: list[Any] = []

    job_repo.create_job(
        job_id=job_id,
        org_id=organization.org_id,
        trigger_type=trigger_type,
        execution_mode=execution_mode,
        status='CREATED',
        app_version=APP_VERSION,
        config_snapshot_hash=config_hash,
    )

    def record_event(
        level: str,
        event_type: str,
        message: str,
        stage_name: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ):
        try:
            event_repo.add_event(
                job_id=job_id,
                level=level.upper(),
                event_type=event_type,
                message=message,
                stage_name=stage_name,
                payload=payload,
            )
        except Exception as event_error:
            logger.warning(f"failed to persist sync event: {event_error}")

    def record_operation(
        *,
        stage_name: str,
        object_type: str,
        operation_type: str,
        status: str,
        message: str,
        source_id: Optional[str] = None,
        department_id: Optional[str] = None,
        target_id: Optional[str] = None,
        target_dn: Optional[str] = None,
        risk_level: str = 'normal',
        rule_source: Optional[str] = None,
        reason_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            operation_log_repo.add_record(
                job_id=job_id,
                stage_name=stage_name,
                object_type=object_type,
                operation_type=operation_type,
                status=status,
                message=message,
                source_id=source_id,
                department_id=department_id,
                target_id=target_id,
                target_dn=target_dn,
                risk_level=risk_level,
                rule_source=rule_source,
                reason_code=reason_code,
                details=details,
            )
        except Exception as operation_error:
            logger.warning("failed to persist sync operation log: %s", operation_error)

    def record_conflict(
        *,
        conflict_type: str,
        source_id: str,
        message: str,
        target_key: Optional[str] = None,
        severity: str = 'warning',
        resolution_hint: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            conflict_repo.add_conflict(
                job_id=job_id,
                conflict_type=conflict_type,
                source_id=source_id,
                message=message,
                target_key=target_key,
                severity=severity,
                resolution_hint=resolution_hint,
                details=details,
            )
            sync_stats['conflict_count'] = int(sync_stats.get('conflict_count') or 0) + 1
        except Exception as conflict_error:
            logger.warning("failed to persist sync conflict: %s", conflict_error)

    def add_planned_operation(
        object_type: str,
        operation_type: str,
        *,
        source_id: Optional[str] = None,
        department_id: Optional[str] = None,
        target_dn: Optional[str] = None,
        desired_state: Optional[Dict[str, Any]] = None,
        risk_level: str = 'normal',
    ):
        nonlocal planned_count, high_risk_operation_count
        plan_repo.add_operation(
            job_id=job_id,
            object_type=object_type,
            operation_type=operation_type,
            source_id=source_id,
            department_id=department_id,
            target_dn=target_dn,
            desired_state=desired_state,
            risk_level=risk_level,
        )
        planned_count += 1
        sync_stats['planned_operation_count'] = planned_count
        if risk_level == 'high' or operation_type in HIGH_RISK_OPERATION_TYPES:
            high_risk_operation_count += 1
            sync_stats['high_risk_operation_count'] = high_risk_operation_count
        plan_fingerprint_items.append(
            {
                'object_type': object_type,
                'operation_type': operation_type,
                'source_id': source_id,
                'department_id': department_id,
                'target_dn': target_dn,
                'desired_state': desired_state or {},
                'risk_level': risk_level,
            }
        )
        record_operation(
            stage_name='plan',
            object_type=object_type,
            operation_type=operation_type,
            status='planned',
            message=f"planned {operation_type}",
            source_id=source_id,
            department_id=department_id,
            target_dn=target_dn,
            risk_level=risk_level,
            details=desired_state or {},
        )

    def mark_job(status: str, *, ended: bool = False, summary: Optional[Dict[str, Any]] = None):
        job_repo.update_job(
            job_id,
            status=status,
            planned_operation_count=planned_count,
            executed_operation_count=executed_count,
            error_count=sync_stats['error_count'],
            summary=summary,
            ended=ended,
        )

    def run_history_cleanup() -> dict[str, Any]:
        job_retention_days = settings_repo.get_int('job_history_retention_days', 30)
        event_retention_days = settings_repo.get_int('event_history_retention_days', 30)
        audit_log_retention_days = settings_repo.get_int('audit_log_retention_days', 90)
        backup_retention_days = settings_repo.get_int('backup_retention_days', 30)
        backup_retention_max_files = settings_repo.get_int('backup_retention_max_files', 30)
        history_cleanup_result = db_manager.cleanup_history(
            job_retention_days=job_retention_days,
            event_retention_days=event_retention_days,
            audit_log_retention_days=audit_log_retention_days,
        )
        backup_cleanup_result = db_manager.cleanup_backups(
            retention_days=backup_retention_days,
            max_files=backup_retention_max_files,
        )
        deleted_total = (
            history_cleanup_result.get('deleted_jobs', 0)
            + history_cleanup_result.get('deleted_events', 0)
            + history_cleanup_result.get('deleted_planned_operations', 0)
            + history_cleanup_result.get('deleted_operation_logs', 0)
            + history_cleanup_result.get('deleted_conflicts', 0)
            + history_cleanup_result.get('deleted_review_requests', 0)
            + history_cleanup_result.get('deleted_audit_logs', 0)
            + backup_cleanup_result.get('deleted_backups', 0)
        )
        cleanup_result = {
            'history': history_cleanup_result,
            'backups': backup_cleanup_result,
        }
        if deleted_total:
            record_event(
                'INFO',
                'history_cleanup',
                "pruned old history and backup records",
                stage_name='finalize',
                payload=cleanup_result,
            )
            logger.info("history cleanup completed: %s", cleanup_result)
        return cleanup_result

    def evaluate_group_policy(
        *,
        group_sam: Optional[str] = None,
        group_dn: Optional[str] = None,
        display_name: Optional[str] = None,
    ) -> GroupPolicyEvaluation:
        matched_rules: List[Dict[str, Any]] = []
        for rule in enabled_group_rules:
            match_type = (rule.get('match_type') or '').strip().lower()
            match_value = (rule.get('match_value') or '').strip()
            is_match = False

            if match_type == 'samaccountname' and group_sam:
                is_match = group_sam.lower() == match_value.lower()
            elif match_type == 'dn' and group_dn:
                is_match = group_dn.lower() == match_value.lower()
            elif match_type == 'display_name' and display_name:
                is_match = display_name.lower() == match_value.lower()

            if is_match:
                matched_rules.append(rule.to_dict())

        is_hard_protected = any(
            rule.get('rule_type') == 'protect' and rule.get('protection_level') == 'hard'
            for rule in matched_rules
        )
        is_excluded = is_hard_protected or any(rule.get('rule_type') == 'exclude' for rule in matched_rules)
        return GroupPolicyEvaluation(
            is_hard_protected=is_hard_protected,
            is_excluded=is_excluded,
            matched_rules=matched_rules,
        )

    def normalize_lifecycle_field_name(value: Optional[str]) -> str:
        normalized = "".join(
            char.lower() if char.isalnum() else "_"
            for char in str(value or "").strip()
        )
        return normalized.strip("_")

    def get_payload_field_value(payload: Dict[str, Any], field_name: str) -> str:
        normalized_field_name = normalize_lifecycle_field_name(field_name)
        if not normalized_field_name:
            return ""
        for key, value in (payload or {}).items():
            if normalize_lifecycle_field_name(str(key)) != normalized_field_name:
                continue
            if value in (None, ""):
                return ""
            if isinstance(value, (list, tuple)):
                return ",".join(str(item).strip() for item in value if str(item).strip())
            return str(value).strip()
        return ""

    def parse_lifecycle_datetime(value: str) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        normalized_text = text.replace("Z", "+00:00")
        for candidate in (
            normalized_text,
            normalized_text.replace("/", "-"),
        ):
            try:
                parsed = datetime.fromisoformat(candidate)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except ValueError:
                pass
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    def get_user_lifecycle_profile(user: SourceDirectoryUser) -> Dict[str, Any]:
        payload = user.to_state_payload()
        start_value = get_payload_field_value(payload, future_onboarding_start_field)
        end_value = get_payload_field_value(payload, contractor_end_field)
        employment_type = get_payload_field_value(payload, lifecycle_employment_type_field)
        sponsor_userid = get_payload_field_value(payload, lifecycle_sponsor_field)
        start_at = parse_lifecycle_datetime(start_value)
        end_at = parse_lifecycle_datetime(end_value)
        normalized_employment_type = str(employment_type or "").strip().lower()
        return {
            'start_field': future_onboarding_start_field,
            'start_value': start_value,
            'start_at': start_at,
            'end_field': contractor_end_field,
            'end_value': end_value,
            'end_at': end_at,
            'employment_type_field': lifecycle_employment_type_field,
            'employment_type': str(employment_type or "").strip(),
            'normalized_employment_type': normalized_employment_type,
            'is_contractor': bool(normalized_employment_type and normalized_employment_type in contractor_type_values),
            'sponsor_field': lifecycle_sponsor_field,
            'sponsor_userid': sponsor_userid,
        }

    def serialize_lifecycle_profile(profile: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        serialized: Dict[str, Any] = {}
        for key, value in dict(profile or {}).items():
            if isinstance(value, datetime):
                serialized[key] = value.astimezone(timezone.utc).isoformat(timespec='seconds')
            else:
                serialized[key] = value
        return serialized

    def has_exception_rule(rule_type: str, match_value: Optional[str]) -> bool:
        normalized_rule_type = str(rule_type or '').strip().lower()
        normalized_match_type = get_exception_rule_match_type(normalized_rule_type)
        normalized_match_value = normalize_exception_match_value(normalized_match_type, match_value)
        if not normalized_rule_type or not normalized_match_type or not normalized_match_value:
            return False
        return normalized_match_value in exception_match_values_by_rule_type.get(normalized_rule_type, set())

    record_event(
        'INFO',
        'job_created',
        f"sync job created, mode={execution_mode}",
        payload={
            'db_path': db_manager.db_path,
            'org_id': organization.org_id,
            'organization_name': organization.name,
            'config_path': resolved_config_path,
            'exclusion_rule_count': len(enabled_group_rules),
            'exception_rule_count': len(enabled_exception_rules),
            'group_recursive_enabled': group_recursive_enabled,
            'managed_relation_cleanup_enabled': managed_relation_cleanup_enabled,
            'user_ou_placement_strategy': user_ou_placement_strategy,
        },
    )
    if automatic_replay_enabled:
        for replay_request in replay_request_repo.list_request_records(status='pending', limit=100):
            request_execution_mode = str(replay_request.execution_mode or '').strip().lower()
            if request_execution_mode and request_execution_mode != execution_mode:
                continue
            replay_request_repo.mark_started(int(replay_request.id))
            started_replay_requests.append(replay_request)
        if started_replay_requests:
            record_event(
                'INFO',
                'automatic_replay_started',
                f"picked up {len(started_replay_requests)} pending replay requests",
                stage_name='plan',
                payload={
                    'request_ids': [int(request.id) for request in started_replay_requests if request.id is not None],
                    'execution_mode': execution_mode,
                },
            )
    sync_stats['automatic_replay_request_count'] = len(started_replay_requests)
    sync_stats['automatic_replay_request_ids'] = [
        int(request.id) for request in started_replay_requests if request.id is not None
    ]

    bot = None
    enabled_ad_users: List[str] = []
    wecom_users = set()
    source_provider = None
    source_provider_name = get_source_provider_display_name(getattr(config, 'source_provider', 'wecom'))

    try:
        mark_job('PLANNING')

        is_valid, validation_errors = validate_config(config)
        if not is_valid:
            raise ValueError("config validation failed:\n" + "\n".join([f"  - {err}" for err in validation_errors]))

        wecom_success, wecom_msg = test_wecom_connection(
            config.source_connector.corpid,
            config.source_connector.corpsecret,
            config.source_connector.agentid,
        )
        if not wecom_success:
            raise ConnectionError(f"{source_provider_name} connection test failed: {wecom_msg}")

        connector_specs = _load_connector_specs(
            config,
            connector_repo,
            connectors_enabled=connector_routing_enabled,
            org_id=organization.org_id,
        )
        for connector_spec in connector_specs:
            connector_config = connector_spec['config']
            ldap_success, ldap_msg = test_ldap_connection(
                connector_config.ldap.server,
                connector_config.ldap.domain,
                connector_config.ldap.username,
                connector_config.ldap.password,
                connector_config.ldap.use_ssl,
                connector_config.ldap.port,
                connector_config.ldap.validate_cert,
                connector_config.ldap.ca_cert_path,
            )
            if not ldap_success:
                raise ConnectionError(
                    f"LDAP connection test failed for connector {connector_spec['connector_id']}: {ldap_msg}"
                )

        security_warnings = run_config_security_self_check(config)
        if security_warnings:
            for warning in security_warnings:
                record_event("WARNING", "security_self_check", warning, stage_name="config")
                logger.warning("security self-check warning: %s", warning)

        if execution_mode == 'apply' and config.webhook_url:
            bot = WeChatBot(config.webhook_url)

        source_provider = build_source_provider(
            app_config=config,
            logger=logger,
            api_factory=WeComAPI,
        )
        ad_sync_clients: Dict[str, ADSyncLDAPS] = {}
        for connector_spec in connector_specs:
            connector_config = connector_spec['config']
            ad_sync_clients[connector_spec['connector_id']] = ADSyncLDAPS(
                server=connector_config.ldap.server,
                domain=connector_config.ldap.domain,
                username=connector_config.ldap.username,
                password=connector_config.ldap.password,
                use_ssl=connector_config.ldap.use_ssl,
                port=connector_config.ldap.port,
                exclude_departments=connector_config.exclude_departments,
                exclude_accounts=connector_config.exclude_accounts,
                default_password=connector_config.account.default_password,
                force_change_password=connector_config.account.force_change_password,
                password_complexity=connector_config.account.password_complexity,
                validate_cert=connector_config.ldap.validate_cert,
                ca_cert_path=connector_config.ldap.ca_cert_path,
                disabled_users_ou_name=connector_spec.get('disabled_users_ou') or 'Disabled Users',
                managed_group_type=connector_spec.get('group_type') or global_group_type,
                managed_group_mail_domain=connector_spec.get('group_mail_domain') or global_group_mail_domain,
                custom_group_ou_path=connector_spec.get('custom_group_ou_path') or global_custom_group_ou_path,
            )
        protected_ad_accounts_by_connector = {
            connector_spec['connector_id']: {
                str(account).strip().lower()
                for account in connector_spec['config'].exclude_accounts
                if str(account).strip()
            }
            for connector_spec in connector_specs
        }
        default_ad_sync = ad_sync_clients['default']
        ad_sync = default_ad_sync

        departments = source_provider.list_departments()
        dept_tree: Dict[int, DepartmentNode] = {
            dept.department_id: dept for dept in departments if dept.department_id
        }

        for dept_id in dept_tree:
            path_names: List[str] = []
            path_ids: List[int] = []
            current_id = dept_id
            while current_id != 0:
                if current_id not in dept_tree:
                    break
                path_names.insert(0, dept_tree[current_id].name)
                path_ids.insert(0, current_id)
                current_id = dept_tree[current_id].parent_id
            dept_tree[dept_id].set_hierarchy(path_names, path_ids)

        department_connector_map = _build_department_connector_map(dept_tree, connector_specs)
        connector_specs_by_id = {spec['connector_id']: spec for spec in connector_specs}
        excluded_department_names = set(config.exclude_departments)
        department_group_targets: Dict[int, ManagedGroupTarget] = {}
        current_parent_groups_cache: Dict[str, List[DirectoryGroupRecord]] = {}
        effective_parent_cache: Dict[int, Optional[int]] = {}
        policy_skip_markers = set()
        placement_blocked_department_ids = {
            int(rule.match_value)
            for rule in enabled_exception_rules
            if rule.rule_type == 'skip_department_placement'
            and str(rule.match_value).strip().isdigit()
        }
        exception_skipped_userids = set()

        def is_department_excluded(dept_info: Optional[DepartmentNode]) -> bool:
            return not dept_info or dept_info.name in excluded_department_names

        def get_connector_id_for_department(dept_info: Optional[DepartmentNode]) -> str:
            if not dept_info:
                return 'default'
            return department_connector_map.get(dept_info.department_id, 'default')

        def get_connector_spec(connector_id: str) -> dict[str, Any]:
            return connector_specs_by_id.get(connector_id, connector_specs_by_id['default'])

        def get_ad_sync(connector_id: str) -> ADSyncLDAPS:
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

        def get_current_parent_groups(member_dn: Optional[str], *, connector_id: str) -> List[DirectoryGroupRecord]:
            if not member_dn:
                return []
            cache_key = f"{connector_id}:{member_dn}"
            if cache_key not in current_parent_groups_cache:
                current_parent_groups_cache[cache_key] = get_ad_sync(connector_id).find_parent_groups_for_member(member_dn)
            return current_parent_groups_cache[cache_key]

        def record_group_policy_skip(stage_name: str, action_type: str, group_target: ManagedGroupTarget, reason: str):
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
            record_event(
                'WARNING' if group_target.policy.is_hard_protected else 'INFO',
                f'{action_type}_skipped',
                reason,
                stage_name=stage_name,
                payload={
                    'group_sam': group_target.group_sam,
                    'group_dn': group_target.group_dn,
                    'display_name': group_target.display_name,
                    'matched_rules': matched_rules,
                },
            )

        def record_skip_detail(
            *,
            stage_name: str,
            action_type: str,
            group_sam: Optional[str],
            group_dn: Optional[str],
            reason: str,
            matched_rules: Optional[List[str]] = None,
        ):
            skipped_summary = sync_stats['skipped_operations']
            skipped_summary['total'] += 1
            skipped_summary['by_action'][action_type] = skipped_summary['by_action'].get(action_type, 0) + 1

            detail = {
                'stage': stage_name,
                'action_type': action_type,
                'group_sam': group_sam,
                'group_dn': group_dn,
                'reason': reason,
                'matched_rules': matched_rules or [],
            }
            if len(skipped_summary['samples']) < 20:
                skipped_summary['samples'].append(detail)
            if len(skipped_summary['details']) < 1000:
                skipped_summary['details'].append(detail)
            record_operation(
                stage_name=stage_name,
                object_type='group',
                operation_type=action_type,
                status='skipped',
                message=reason,
                source_id=group_sam,
                target_dn=group_dn,
                risk_level='normal',
                reason_code='policy_skip',
                details=detail,
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
            risk_level: str = 'normal',
            details: Optional[Dict[str, Any]] = None,
        ) -> None:
            message = f"skip {operation_type} for protected AD account {ad_username}"
            payload = {
                'connector_id': connector_id,
                'ad_username': ad_username,
                'protected_accounts': sorted(protected_ad_accounts_by_connector.get(connector_id, set())),
            }
            if details:
                payload.update(details)
            record_event(
                'WARNING',
                'protected_ad_account_skip',
                message,
                stage_name=stage_name,
                payload=payload,
            )
            record_operation(
                stage_name=stage_name,
                object_type=object_type,
                operation_type=operation_type,
                status='skipped',
                message=message,
                source_id=source_id,
                target_id=target_id or ad_username,
                risk_level=risk_level,
                rule_source='system_protected_account',
                reason_code='protected_ad_account',
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
            risk_level: str = 'normal',
            details: Optional[Dict[str, Any]] = None,
        ) -> None:
            skipped_summary = sync_stats['skipped_operations']
            skipped_summary['total'] += 1
            skipped_summary['by_action'][operation_type] = skipped_summary['by_action'].get(operation_type, 0) + 1

            detail = {
                'stage': stage_name,
                'action_type': operation_type,
                'object_type': object_type,
                'source_id': source_id,
                'department_id': department_id,
                'target_id': target_id,
                'target_dn': target_dn,
                'reason': reason,
                'exception_rule_type': exception_rule_type,
                'match_value': match_value,
            }
            if details:
                detail.update(details)
            if len(skipped_summary['samples']) < 20:
                skipped_summary['samples'].append(detail)
            if len(skipped_summary['details']) < 1000:
                skipped_summary['details'].append(detail)
            exception_rule_repo.consume_rule(
                rule_type=exception_rule_type,
                match_value=match_value,
            )

            record_operation(
                stage_name=stage_name,
                object_type=object_type,
                operation_type=operation_type,
                status='skipped',
                message=reason,
                source_id=source_id,
                department_id=department_id,
                target_id=target_id,
                target_dn=target_dn,
                risk_level=risk_level,
                rule_source=exception_rule_type,
                reason_code='exception_rule',
                details=detail,
            )
            record_event(
                'INFO',
                'exception_rule_skip',
                reason,
                stage_name=stage_name,
                payload=detail,
            )

        def get_department_group_target(dept_info: DepartmentNode) -> ManagedGroupTarget:
            dept_id = dept_info.department_id
            if dept_id in department_group_targets:
                return department_group_targets[dept_id]

            connector_id = get_connector_id_for_department(dept_info)
            connector_ad_sync = get_ad_sync(connector_id)
            ou_dn = connector_ad_sync.get_ou_dn(dept_info.path)
            binding = binding_repo.get_binding_record_by_department_id(str(dept_id))
            if binding and binding.status != 'active':
                binding = None

            if binding and binding.get('group_sam'):
                group_cn = binding.get('group_cn') or build_group_cn(dept_info.name, dept_id)
                group_dn = binding.get('group_dn') or f"CN={group_cn},{ou_dn}"
                display_name = binding.get('display_name') or build_group_display_name(
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
                    description=f"source=wecom; dept_id={dept_id}; path={'/'.join(dept_info.path)}",
                    binding_source='binding',
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
                parent_department_id=dept_info.parent_id if dept_info.parent_id in dept_tree else None,
                ou_name=dept_info.name,
                ou_dn=ou_dn,
                full_path=list(dept_info.path),
                policy=evaluate_group_policy(
                    group_sam=group_info.group_sam,
                    group_dn=group_info.group_dn,
                    display_name=group_info.display_name,
                ),
            )
            department_group_targets[dept_id] = target
            return target

        def get_effective_parent_department_id(dept_info: DepartmentNode) -> Optional[int]:
            dept_id = dept_info.department_id
            if dept_id in effective_parent_cache:
                return effective_parent_cache[dept_id]

            parent_id = dept_info.parent_id
            while parent_id and parent_id in dept_tree:
                parent_dept = dept_tree[parent_id]
                if not is_department_excluded(parent_dept):
                    effective_parent_cache[dept_id] = parent_id
                    return parent_id
                parent_id = parent_dept.parent_id

            effective_parent_cache[dept_id] = None
            return None

        user_departments: Dict[str, UserDepartmentBundle] = {}
        for dept_id, dept_info in dept_tree.items():
            if is_cancelled():
                raise InterruptedError('sync cancelled by user')
            try:
                users = source_provider.list_department_users(dept_id)
                dept_info.users = users
                for user in users:
                    userid = user.userid
                    wecom_users.add(userid)
                    if userid not in user_departments:
                        user_departments[userid] = UserDepartmentBundle(user=user)
                    else:
                        user_departments[userid].user.merge_payload(user.to_state_payload())
                    user_departments[userid].add_department(dept_info)
            except Exception as fetch_error:
                logger.error(f"failed to load users from department {dept_info.name}: {fetch_error}")

        sync_stats['total_users'] = len(wecom_users)
        if stats_callback:
            stats_callback('total_users', len(wecom_users))

        active_user_bindings: Dict[str, str] = {}
        binding_resolution_details: Dict[str, Dict[str, Any]] = {}
        user_connector_id_by_userid: Dict[str, str] = {}
        disabled_bound_userids = set()
        current_wecom_ad_usernames_by_connector: Dict[str, set[str]] = {}
        wecom_user_detail_cache: Dict[str, Dict[str, Any]] = {}

        def get_wecom_user_detail_cached(userid: str, user: Optional[SourceDirectoryUser] = None) -> Dict[str, Any]:
            if userid not in wecom_user_detail_cache:
                try:
                    wecom_user_detail_cache[userid] = source_provider.get_user_detail(userid) or {}
                except Exception as detail_error:
                    logger.warning("failed to load WeCom detail for %s: %s", userid, detail_error)
                    wecom_user_detail_cache[userid] = {}
            detail_payload = wecom_user_detail_cache[userid]
            if user and detail_payload:
                user.merge_payload(detail_payload)
            return detail_payload

        identity_candidates_by_userid: Dict[str, list[dict[str, str]]] = {}
        identity_candidate_usernames_by_connector: Dict[str, set[str]] = {}
        for userid, bundle in user_departments.items():
            get_wecom_user_detail_cached(userid, bundle.user)
            connector_candidates = {
                get_connector_id_for_department(department)
                for department in bundle.departments
                if department and department.department_id
            }
            connector_candidates.discard('')
            if not connector_candidates:
                connector_candidates = {'default'}
            if len(connector_candidates) > 1:
                record_conflict(
                    conflict_type='multiple_connector_candidates',
                    source_id=userid,
                    target_key='connector_assignment',
                    message=(
                        f"Source user {userid} spans multiple connector roots: "
                        + ", ".join(sorted(connector_candidates))
                    ),
                    resolution_hint='Narrow the department connector roots or move the user into a single managed root',
                    details={'userid': userid, 'connector_ids': sorted(connector_candidates)},
                )
                continue
            connector_id = next(iter(connector_candidates))
            user_connector_id_by_userid[userid] = connector_id
            connector_spec = get_connector_spec(connector_id)
            candidates = _build_identity_candidates(
                bundle.user,
                username_template=connector_spec.get('username_template') or '',
            )
            identity_candidates_by_userid[userid] = candidates
            for candidate in candidates:
                identity_candidate_usernames_by_connector.setdefault(connector_id, set()).add(candidate['username'])

        existing_candidate_users_map: Dict[str, Dict[str, DirectoryUserRecord]] = {}
        for connector_id, usernames in identity_candidate_usernames_by_connector.items():
            existing_candidate_users_map[connector_id] = get_ad_sync(connector_id).get_users_batch(sorted(usernames))
        pending_auto_bindings: Dict[str, Dict[str, Any]] = {}

        for userid in sorted(user_departments.keys()):
            if has_exception_rule('skip_user_sync', userid):
                exception_skipped_userids.add(userid)
                record_exception_skip(
                    stage_name='plan',
                    object_type='user',
                    operation_type='user_sync',
                    exception_rule_type='skip_user_sync',
                    match_value=userid,
                    reason=f"skip user {userid}: matched exception rule skip_user_sync",
                    source_id=userid,
                    details={'userid': userid},
                )
                continue

            connector_id = user_connector_id_by_userid.get(userid, 'default')
            binding_record = user_binding_repo.get_binding_record_by_source_user_id(userid)
            if binding_record:
                binding_connector_id = binding_record.connector_id or connector_id
                if is_protected_ad_account(binding_record.ad_username, binding_connector_id):
                    record_protected_account_skip(
                        stage_name='plan',
                        object_type='user_binding',
                        operation_type='resolve_identity_binding',
                        connector_id=binding_connector_id,
                        ad_username=binding_record.ad_username,
                        source_id=userid,
                        details={
                            'userid': userid,
                            'binding_source': binding_record.source,
                        },
                    )
                    continue
                if not binding_record.is_enabled:
                    disabled_bound_userids.add(userid)
                    record_event(
                        'INFO',
                        'user_binding_disabled',
                        f"skip user {userid}: user identity binding is disabled",
                        stage_name='plan',
                    )
                    record_operation(
                        stage_name='plan',
                        object_type='user_binding',
                        operation_type='resolve_identity_binding',
                        status='skipped',
                        message=f"skip user {userid}: user identity binding is disabled",
                        source_id=userid,
                        target_id=binding_record.ad_username,
                        rule_source='disabled_binding',
                        reason_code='binding_disabled',
                        details={'userid': userid, 'ad_username': binding_record.ad_username},
                    )
                    continue

                if (
                    connector_routing_enabled
                    and binding_record.connector_id
                    and binding_record.connector_id != connector_id
                ):
                    conflict_message = (
                        f"Source user {userid} moved from connector {binding_record.connector_id} "
                        f"to {connector_id} and requires migration review"
                    )
                    record_conflict(
                        conflict_type='connector_migration_required',
                        source_id=userid,
                        target_key=f"{binding_record.connector_id}->{connector_id}",
                        message=conflict_message,
                        resolution_hint='Review cross-domain migration, then update the manual binding connector or rebind the user',
                        details={
                            'userid': userid,
                            'existing_connector_id': binding_record.connector_id,
                            'target_connector_id': connector_id,
                            'ad_username': binding_record.ad_username,
                        },
                    )
                    record_operation(
                        stage_name='plan',
                        object_type='user_binding',
                        operation_type='resolve_identity_binding',
                        status='conflict',
                        message=conflict_message,
                        source_id=userid,
                        target_id=binding_record.ad_username,
                        rule_source='connector_routing',
                        reason_code='connector_migration_required',
                        details={
                            'existing_connector_id': binding_record.connector_id,
                            'target_connector_id': connector_id,
                            'ad_username': binding_record.ad_username,
                        },
                    )
                    continue

                binding_source = 'manual_binding' if binding_record.source == 'manual' else 'existing_binding'
                active_user_bindings[userid] = binding_record.ad_username
                binding_resolution_details[userid] = {
                    'source': binding_source,
                    'ad_username': binding_record.ad_username,
                    'connector_id': binding_connector_id,
                    'rule_hits': [binding_source],
                    'explanation': 'Using the persisted identity binding',
                    'binding_record_source': binding_record.source,
                    'is_manual': binding_record.source == 'manual',
                }
                current_wecom_ad_usernames_by_connector.setdefault(
                    binding_connector_id,
                    set(),
                ).add(binding_record.ad_username)
                record_operation(
                    stage_name='plan',
                    object_type='user_binding',
                    operation_type='resolve_identity_binding',
                    status='selected',
                    message=f"resolved {userid} -> {binding_record.ad_username}",
                    source_id=userid,
                    target_id=binding_record.ad_username,
                    rule_source=binding_source,
                    reason_code='persisted_binding',
                    details=binding_resolution_details[userid],
                )
                continue

            candidates = identity_candidates_by_userid.get(userid) or _build_identity_candidates(
                user_departments[userid].user,
                username_template=get_connector_spec(connector_id).get('username_template') or '',
            )
            connector_existing_users = existing_candidate_users_map.get(connector_id, {})
            existing_candidates = [
                candidate
                for candidate in candidates
                if candidate['rule'] != 'derived_default_userid'
                and candidate['username'] in connector_existing_users
            ]
            protected_existing_candidates = [
                candidate
                for candidate in existing_candidates
                if is_protected_ad_account(candidate['username'], connector_id)
            ]
            for candidate in protected_existing_candidates:
                record_protected_account_skip(
                    stage_name='plan',
                    object_type='user_binding',
                    operation_type='resolve_identity_binding',
                    connector_id=connector_id,
                    ad_username=candidate['username'],
                    source_id=userid,
                    details={
                        'userid': userid,
                        'candidate_rule': candidate['rule'],
                    },
                )
            existing_candidates = [
                candidate
                for candidate in existing_candidates
                if not is_protected_ad_account(candidate['username'], connector_id)
            ]
            unique_existing_usernames = {candidate['username'].lower(): candidate for candidate in existing_candidates}
            if len(unique_existing_usernames) > 1:
                conflict_message = (
                    f"Source user {userid} matched multiple AD candidates: "
                    + " / ".join(sorted(candidate['username'] for candidate in unique_existing_usernames.values()))
                )
                record_conflict(
                    conflict_type='multiple_ad_candidates',
                    source_id=userid,
                    target_key='identity_binding',
                    message=conflict_message,
                    resolution_hint='Create a manual identity binding before rerunning synchronization',
                    details={
                        'userid': userid,
                        'candidates': list(unique_existing_usernames.values()),
                    },
                )
                record_event(
                    'WARNING',
                    'user_binding_conflict',
                    conflict_message,
                    stage_name='plan',
                    payload={'userid': userid, 'candidates': list(unique_existing_usernames.values())},
                )
                record_operation(
                    stage_name='plan',
                    object_type='user_binding',
                    operation_type='resolve_identity_binding',
                    status='conflict',
                    message=conflict_message,
                    source_id=userid,
                    rule_source='auto_candidate_resolution',
                    reason_code='multiple_ad_candidates',
                    details={
                        'userid': userid,
                        'candidates': list(unique_existing_usernames.values()),
                    },
                )
                continue

            if existing_candidates:
                selected_candidate = next(iter(unique_existing_usernames.values()))
                resolution = {
                    'source': selected_candidate['rule'],
                    'ad_username': selected_candidate['username'],
                    'connector_id': connector_id,
                    'rule_hits': [selected_candidate['rule']],
                    'explanation': selected_candidate['explanation'],
                    'binding_record_source': selected_candidate['rule'],
                    'is_manual': False,
                }
            else:
                default_candidate = next(
                    (candidate for candidate in candidates if candidate['rule'] == 'derived_default_userid'),
                    candidates[0]
                    if candidates
                    else {
                        'rule': 'derived_default_userid',
                        'username': userid,
                        'explanation': 'Defaulting to userid because no existing AD user matched',
                    },
                )
                resolution = {
                    'source': 'derived_default_userid',
                    'ad_username': default_candidate['username'],
                    'connector_id': connector_id,
                    'rule_hits': [default_candidate['rule']],
                    'explanation': default_candidate['explanation'],
                    'binding_record_source': 'derived_default',
                    'is_manual': False,
                }

            if is_protected_ad_account(resolution['ad_username'], connector_id):
                record_protected_account_skip(
                    stage_name='plan',
                    object_type='user_binding',
                    operation_type='resolve_identity_binding',
                    connector_id=connector_id,
                    ad_username=resolution['ad_username'],
                    source_id=userid,
                    details={
                        'userid': userid,
                        'binding_source': resolution['source'],
                    },
                )
                continue
            pending_auto_bindings[userid] = resolution

        username_to_userids: Dict[str, list[str]] = {}
        for userid, resolution in {**binding_resolution_details, **pending_auto_bindings}.items():
            ad_username = str(resolution.get('ad_username') or '').strip().lower()
            if not ad_username:
                continue
            username_to_userids.setdefault(ad_username, []).append(userid)

        conflicted_userids = set()
        for ad_username, userids in username_to_userids.items():
            if len(userids) <= 1:
                continue

            authoritative_userids = [
                userid for userid in userids if binding_resolution_details.get(userid, {}).get('source') in {'manual_binding', 'existing_binding'}
            ]
            if len(authoritative_userids) == 1:
                losing_userids = [userid for userid in userids if userid != authoritative_userids[0]]
            else:
                losing_userids = list(userids)

            for userid in losing_userids:
                conflicted_userids.add(userid)
                conflict_message = (
                    f"AD account {ad_username} matched multiple source users: {', '.join(sorted(userids))}"
                )
                record_conflict(
                    conflict_type='shared_ad_account',
                    source_id=userid,
                    target_key=ad_username,
                    message=conflict_message,
                    resolution_hint='Create unique manual identity bindings for the affected users before rerunning synchronization',
                    details={'ad_username': ad_username, 'source_user_ids': sorted(userids)},
                )
                record_operation(
                    stage_name='plan',
                    object_type='user_binding',
                    operation_type='resolve_identity_binding',
                    status='conflict',
                    message=conflict_message,
                    source_id=userid,
                    target_id=ad_username,
                    rule_source='duplicate_binding_detection',
                    reason_code='shared_ad_account',
                    details={'ad_username': ad_username, 'source_user_ids': sorted(userids)},
                )

        for userid, resolution in pending_auto_bindings.items():
            if userid in conflicted_userids:
                continue
            resolved_username = resolution['ad_username']
            resolved_connector_id = resolution.get('connector_id') or user_connector_id_by_userid.get(userid, 'default')
            user_binding_repo.upsert_binding_for_source_user(
                userid,
                resolved_username,
                connector_id=resolved_connector_id,
                source=resolution['binding_record_source'],
                notes=resolution['explanation'],
                preserve_manual=True,
            )
            active_user_bindings[userid] = resolved_username
            binding_resolution_details[userid] = resolution
            current_wecom_ad_usernames_by_connector.setdefault(resolved_connector_id, set()).add(resolved_username)
            record_operation(
                stage_name='plan',
                object_type='user_binding',
                operation_type='resolve_identity_binding',
                status='selected',
                message=f"resolved {userid} -> {resolved_username}",
                source_id=userid,
                target_id=resolved_username,
                rule_source=resolution['source'],
                reason_code='auto_resolution',
                details=resolution,
            )

        existing_users_map_by_connector: Dict[str, Dict[str, Any]] = {}
        enabled_ad_users_by_connector: Dict[str, List[str]] = {}
        enabled_ad_users = []
        for connector_id, connector_usernames in current_wecom_ad_usernames_by_connector.items():
            existing_users_map_by_connector[connector_id] = get_ad_sync(connector_id).get_users_batch(
                sorted(connector_usernames)
            )
        for connector_id in connector_specs_by_id.keys():
            connector_enabled_users = get_ad_sync(connector_id).get_all_enabled_users()
            enabled_ad_users_by_connector[connector_id] = connector_enabled_users
            enabled_ad_users.extend([f"{connector_id}:{username}" for username in connector_enabled_users])

        department_actions: List[DepartmentAction] = []
        custom_group_actions: List[Dict[str, Any]] = []
        user_actions: List[UserAction] = []
        membership_actions: List[GroupMembershipAction] = []
        group_hierarchy_actions: List[GroupHierarchyAction] = []
        group_cleanup_actions: List[GroupCleanupAction] = []
        disable_actions: List[DisableUserAction] = []
        processed_department_nodes = set()
        planned_memberships = set()
        planned_hierarchy_pairs = set()
        planned_cleanup_pairs = set()

        for dept_id, dept_info in dept_tree.items():
            if is_cancelled():
                raise InterruptedError('sync cancelled by user')
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
                parent_dn = connector_ad_sync.base_dn if idx == 0 else connector_ad_sync.get_ou_dn(current_path[:-1])
                ou_dn = connector_ad_sync.get_ou_dn(current_path)
                ou_exists = connector_ad_sync.ou_exists(ou_dn)
                group_target = get_department_group_target(ancestor)
                should_manage_group = not group_target.policy.is_excluded

                if group_target.policy.is_excluded:
                    record_group_policy_skip(
                        'plan',
                        'department_group_management',
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
                    add_planned_operation(
                        object_type='department',
                        operation_type='ensure_department_node',
                        source_id=str(ancestor_id),
                        department_id=str(ancestor_id),
                        target_dn=ou_dn,
                        desired_state={
                            'path': current_path,
                            'group_sam': group_target.group_sam,
                            'group_dn': group_target.group_dn,
                            'group_management': 'managed' if should_manage_group else 'skipped_by_policy',
                        },
                    )

        if custom_group_sync_enabled:
            for connector_spec in connector_specs:
                connector_id = connector_spec['connector_id']
                managed_tag_ids = connector_spec.get('managed_tag_ids') or []
                managed_external_chat_ids = connector_spec.get('managed_external_chat_ids') or []
                tag_index: Dict[str, Dict[str, Any]] = {}
                if managed_tag_ids:
                    try:
                        tag_index = {
                            str(item.get('tagid') or item.get('id') or ''): item
                            for item in source_provider.list_tag_records()
                            if str(item.get('tagid') or item.get('id') or '').strip()
                        }
                    except Exception as tag_error:
                        record_event(
                            'WARNING',
                            'tag_group_fetch_failed',
                            f"failed to load WeCom tag definitions for connector {connector_id}: {tag_error}",
                            stage_name='plan',
                        )

                for tag_id in managed_tag_ids:
                    tag_id_text = str(tag_id or '').strip()
                    if not tag_id_text:
                        continue
                    try:
                        tag_membership = source_provider.get_tag_users(tag_id_text)
                    except Exception as tag_error:
                        record_event(
                            'WARNING',
                            'tag_group_fetch_failed',
                            f"failed to load WeCom tag {tag_id_text}: {tag_error}",
                            stage_name='plan',
                        )
                        continue
                    display_name = (
                        str(tag_index.get(tag_id_text, {}).get('tagname') or '').strip()
                        or str(tag_membership.get('tagname') or '').strip()
                        or f"WeCom Tag {tag_id_text}"
                    )
                    group_sam = build_custom_group_sam('tag', tag_id_text)
                    group_policy = evaluate_group_policy(group_sam=group_sam, display_name=display_name)
                    if group_policy.is_excluded:
                        continue
                    custom_group_actions.append(
                        {
                            'connector_id': connector_id,
                            'source_type': 'tag',
                            'source_key': tag_id_text,
                            'display_name': display_name,
                        }
                    )
                    add_planned_operation(
                        object_type='custom_group',
                        operation_type='ensure_custom_group',
                        source_id=f"tag:{tag_id_text}",
                        target_dn='',
                        desired_state={
                            'connector_id': connector_id,
                            'source_type': 'tag',
                            'source_key': tag_id_text,
                            'display_name': display_name,
                        },
                    )
                    for member in tag_membership.get('userlist', []) or []:
                        userid = str(member.get('userid') or '').strip()
                        if not userid or userid in exception_skipped_userids or userid in disabled_bound_userids:
                            continue
                        if user_connector_id_by_userid.get(userid, 'default') != connector_id:
                            continue
                        username = active_user_bindings.get(userid)
                        if not username:
                            continue
                        membership_key = (connector_id, username, group_sam)
                        if membership_key in planned_memberships:
                            continue
                        planned_memberships.add(membership_key)
                        membership_actions.append(
                            GroupMembershipAction(
                                connector_id=connector_id,
                                source_user_id=userid,
                                username=username,
                                group_sam=group_sam,
                                group_dn='',
                                group_display_name=display_name,
                                department_id=0,
                            )
                        )
                        add_planned_operation(
                            object_type='group_membership',
                            operation_type='add_user_to_group',
                            source_id=userid,
                            target_dn='',
                            desired_state={
                                'connector_id': connector_id,
                                'ad_username': username,
                                'group_sam': group_sam,
                                'display_name': display_name,
                                'group_source_type': 'tag',
                            },
                        )

                for chat_id in managed_external_chat_ids:
                    chat_id_text = str(chat_id or '').strip()
                    if not chat_id_text:
                        continue
                    try:
                        chat_info = source_provider.get_external_group_chat(chat_id_text)
                    except Exception as chat_error:
                        record_event(
                            'WARNING',
                            'external_group_fetch_failed',
                            f"failed to load WeCom external chat {chat_id_text}: {chat_error}",
                            stage_name='plan',
                        )
                        continue
                    display_name = str(chat_info.get('name') or '').strip() or f"WeCom External Chat {chat_id_text}"
                    group_sam = build_custom_group_sam('external_chat', chat_id_text)
                    group_policy = evaluate_group_policy(group_sam=group_sam, display_name=display_name)
                    if group_policy.is_excluded:
                        continue
                    custom_group_actions.append(
                        {
                            'connector_id': connector_id,
                            'source_type': 'external_chat',
                            'source_key': chat_id_text,
                            'display_name': display_name,
                        }
                    )
                    add_planned_operation(
                        object_type='custom_group',
                        operation_type='ensure_custom_group',
                        source_id=f"external_chat:{chat_id_text}",
                        target_dn='',
                        desired_state={
                            'connector_id': connector_id,
                            'source_type': 'external_chat',
                            'source_key': chat_id_text,
                            'display_name': display_name,
                        },
                    )
                    for member in chat_info.get('member_list', []) or []:
                        userid = str(member.get('userid') or '').strip()
                        if not userid or userid in exception_skipped_userids or userid in disabled_bound_userids:
                            continue
                        if user_connector_id_by_userid.get(userid, 'default') != connector_id:
                            continue
                        username = active_user_bindings.get(userid)
                        if not username:
                            continue
                        membership_key = (connector_id, username, group_sam)
                        if membership_key in planned_memberships:
                            continue
                        planned_memberships.add(membership_key)
                        membership_actions.append(
                            GroupMembershipAction(
                                connector_id=connector_id,
                                source_user_id=userid,
                                username=username,
                                group_sam=group_sam,
                                group_dn='',
                                group_display_name=display_name,
                                department_id=0,
                            )
                        )
                        add_planned_operation(
                            object_type='group_membership',
                            operation_type='add_user_to_group',
                            source_id=userid,
                            target_dn='',
                            desired_state={
                                'connector_id': connector_id,
                                'ad_username': username,
                                'group_sam': group_sam,
                                'display_name': display_name,
                                'group_source_type': 'external_chat',
                            },
                        )

        for userid, info in user_departments.items():
            user = info.user
            departments_for_user = info.departments
            if userid in exception_skipped_userids:
                continue
            if userid in disabled_bound_userids:
                continue

            username = active_user_bindings.get(userid)
            if not username:
                record_event(
                    'WARNING',
                    'user_skipped',
                    f"skip user {userid}: no enabled identity binding is available",
                    stage_name='plan',
                )
                record_operation(
                    stage_name='plan',
                    object_type='user',
                    operation_type='resolve_identity_binding',
                    status='skipped',
                    message=f"skip user {userid}: no enabled identity binding is available",
                    source_id=userid,
                    reason_code='missing_binding',
                    details={'userid': userid},
                )
                continue

            connector_id = binding_resolution_details.get(userid, {}).get(
                'connector_id',
                user_connector_id_by_userid.get(userid, 'default'),
            )
            connector_spec = get_connector_spec(connector_id)
            connector_ad_sync = get_ad_sync(connector_id)
            connector_domain = connector_spec['config'].domain
            display_name = user.name
            override_record = department_override_repo.get_override_record_by_source_user_id(userid)
            override_department_id = None
            if override_record and override_record.primary_department_id:
                try:
                    override_department_id = int(override_record.primary_department_id)
                except (TypeError, ValueError):
                    override_department_id = None

            target_dept, placement_reason = _resolve_target_department(
                info,
                placement_strategy=user_ou_placement_strategy,
                is_department_excluded=is_department_blocked_for_placement,
                override_department_id=override_department_id,
            )
            if not target_dept:
                blocked_department_ids = [
                    dept.department_id
                    for dept in departments_for_user
                    if dept.path and dept.department_id in placement_blocked_department_ids
                ]
                if blocked_department_ids:
                    record_exception_skip(
                        stage_name='plan',
                        object_type='user',
                        operation_type='resolve_target_department',
                        exception_rule_type='skip_department_placement',
                        match_value=str(blocked_department_ids[0]),
                        reason=f"skip user {userid}: all eligible placement departments are blocked by skip_department_placement",
                        source_id=userid,
                        target_id=username,
                        details={
                            'userid': userid,
                            'ad_username': username,
                            'blocked_department_ids': blocked_department_ids,
                            'placement_reason': placement_reason,
                        },
                    )
                    continue
                record_event(
                    'WARNING',
                    'user_skipped',
                    f"skip user {userid}: no eligible department for OU placement",
                    stage_name='plan',
                    payload={
                        'userid': userid,
                        'ad_username': username,
                        'placement_reason': placement_reason,
                    },
                )
                record_operation(
                    stage_name='plan',
                    object_type='user',
                    operation_type='resolve_target_department',
                    status='skipped',
                    message=f"skip user {userid}: no eligible department for OU placement",
                    source_id=userid,
                    target_id=username,
                    reason_code=placement_reason,
                    details={
                        'userid': userid,
                        'ad_username': username,
                        'placement_reason': placement_reason,
                    },
                )
                continue

            try:
                user_detail = source_provider.get_user_detail(userid)
                user.merge_payload(user_detail)
                email = user_detail.get('email', '')
            except Exception:
                email = ''
            connector_existing_users = existing_users_map_by_connector.get(connector_id, {})
            connector_enabled_usernames = set(enabled_ad_users_by_connector.get(connector_id, []))
            user_lifecycle_profile = get_user_lifecycle_profile(user)
            lifecycle_now = datetime.now(timezone.utc)
            lifecycle_manager_userids = extract_manager_userids(user)
            lifecycle_payload = {
                'connector_id': connector_id,
                'ad_username': username,
                'employment_type': user_lifecycle_profile['employment_type'],
                'start_value': user_lifecycle_profile['start_value'],
                'end_value': user_lifecycle_profile['end_value'],
                'sponsor_userid': user_lifecycle_profile['sponsor_userid'],
                'manager_userids': lifecycle_manager_userids,
            }
            if (
                future_onboarding_enabled
                and user_lifecycle_profile['start_at']
                and user_lifecycle_profile['start_at'] > lifecycle_now
            ):
                lifecycle_repo.upsert_pending_for_source_user(
                    lifecycle_type='future_onboarding',
                    connector_id=connector_id,
                    source_user_id=userid,
                    ad_username=username,
                    effective_at=user_lifecycle_profile['start_at'].isoformat(timespec='seconds'),
                    reason='future_start_date',
                    employment_type=user_lifecycle_profile['employment_type'],
                    sponsor_userid=user_lifecycle_profile['sponsor_userid'],
                    manager_userids=lifecycle_manager_userids,
                    payload=lifecycle_payload,
                    last_job_id=job_id,
                )
                add_planned_operation(
                    object_type='user',
                    operation_type='queue_future_onboarding',
                    source_id=userid,
                    risk_level='normal',
                    desired_state={
                        **lifecycle_payload,
                        'effective_at': user_lifecycle_profile['start_at'].isoformat(timespec='seconds'),
                        'reason': 'future_start_date',
                    },
                )
                record_event(
                    'INFO',
                    'future_onboarding_queued',
                    f"queued future onboarding for user {userid} until {user_lifecycle_profile['start_at'].isoformat(timespec='seconds')}",
                    stage_name='plan',
                    payload=lifecycle_payload,
                )
                continue
            if not future_onboarding_enabled or not user_lifecycle_profile['start_at'] or user_lifecycle_profile['start_at'] <= lifecycle_now:
                lifecycle_repo.clear_pending_for_source_user(
                    lifecycle_type='future_onboarding',
                    connector_id=connector_id,
                    source_user_id=userid,
                )
            if contractor_lifecycle_enabled and user_lifecycle_profile['is_contractor']:
                if user_lifecycle_profile['end_at'] and user_lifecycle_profile['end_at'] > lifecycle_now:
                    lifecycle_repo.upsert_pending_for_source_user(
                        lifecycle_type='contractor_expiry',
                        connector_id=connector_id,
                        source_user_id=userid,
                        ad_username=username,
                        effective_at=user_lifecycle_profile['end_at'].isoformat(timespec='seconds'),
                        reason='contractor_end_date',
                        employment_type=user_lifecycle_profile['employment_type'],
                        sponsor_userid=user_lifecycle_profile['sponsor_userid'],
                        manager_userids=lifecycle_manager_userids,
                        payload=lifecycle_payload,
                        last_job_id=job_id,
                    )
                elif user_lifecycle_profile['end_at'] and user_lifecycle_profile['end_at'] <= lifecycle_now:
                    lifecycle_repo.upsert_pending_for_source_user(
                        lifecycle_type='contractor_expiry',
                        connector_id=connector_id,
                        source_user_id=userid,
                        ad_username=username,
                        effective_at=user_lifecycle_profile['end_at'].isoformat(timespec='seconds'),
                        reason='contractor_expired',
                        employment_type=user_lifecycle_profile['employment_type'],
                        sponsor_userid=user_lifecycle_profile['sponsor_userid'],
                        manager_userids=lifecycle_manager_userids,
                        payload=lifecycle_payload,
                        last_job_id=job_id,
                    )
                    record_event(
                        'WARNING',
                        'contractor_expired',
                        f"detected expired contractor user {userid}; disable workflow will be applied",
                        stage_name='plan',
                        payload=lifecycle_payload,
                    )
                    if connector_existing_users.get(username) or username in connector_enabled_usernames:
                        if is_protected_ad_account(username, connector_id):
                            record_protected_account_skip(
                                stage_name='plan',
                                object_type='user',
                                operation_type='disable_user',
                                connector_id=connector_id,
                                ad_username=username,
                                source_id=userid,
                                risk_level='high',
                                details={
                                    'userid': userid,
                                    'reason': 'contractor_expired',
                                },
                            )
                            continue
                        disable_actions.append(
                            DisableUserAction(
                                connector_id=connector_id,
                                username=username,
                                source_user_id=userid,
                                reason='contractor_expired',
                                employment_type=user_lifecycle_profile['employment_type'],
                                sponsor_userid=user_lifecycle_profile['sponsor_userid'],
                                effective_at=user_lifecycle_profile['end_at'].isoformat(timespec='seconds'),
                            )
                        )
                        add_planned_operation(
                            object_type='user',
                            operation_type='disable_user',
                            source_id=userid,
                            risk_level='high',
                            desired_state={
                                **lifecycle_payload,
                                'effective_at': user_lifecycle_profile['end_at'].isoformat(timespec='seconds'),
                                'reason': 'contractor_expired',
                            },
                        )
                    else:
                        add_planned_operation(
                            object_type='user',
                            operation_type='skip_expired_user_without_ad_identity',
                            source_id=userid,
                            risk_level='normal',
                            desired_state={
                                **lifecycle_payload,
                                'effective_at': user_lifecycle_profile['end_at'].isoformat(timespec='seconds'),
                                'reason': 'contractor_expired_without_existing_ad_identity',
                            },
                        )
                    continue
                else:
                    lifecycle_repo.clear_pending_for_source_user(
                        lifecycle_type='contractor_expiry',
                        connector_id=connector_id,
                        source_user_id=userid,
                    )
            else:
                lifecycle_repo.clear_pending_for_source_user(
                    lifecycle_type='contractor_expiry',
                    connector_id=connector_id,
                    source_user_id=userid,
                )
            if not email:
                email = f"{username}@{connector_domain}"

            ou_dn = connector_ad_sync.get_ou_dn(target_dept.path)
            user.email = email
            user.departments = [dept.department_id for dept in departments_for_user]
            if connector_existing_users.get(username):
                operation_type = (
                    'reactivate_user'
                    if rehire_restore_enabled and username not in connector_enabled_usernames
                    else 'update_user'
                )
            else:
                operation_type = 'create_user'
            if is_protected_ad_account(username, connector_id):
                record_protected_account_skip(
                    stage_name='plan',
                    object_type='user',
                    operation_type=operation_type,
                    connector_id=connector_id,
                    ad_username=username,
                    source_id=userid,
                    details={
                        'userid': userid,
                        'placement_reason': placement_reason,
                    },
                )
                continue
            user_actions.append(
                UserAction(
                    connector_id=connector_id,
                    operation_type=operation_type,
                    username=username,
                    display_name=display_name,
                    email=email,
                    ou_dn=ou_dn,
                    ou_path=list(target_dept.path),
                    target_department_id=target_dept.department_id,
                    placement_reason=placement_reason,
                    user=user,
                    lifecycle_profile=user_lifecycle_profile,
                )
            )
            add_planned_operation(
                object_type='user',
                operation_type=operation_type,
                source_id=userid,
                department_id=str(target_dept.department_id),
                target_dn=f"CN={display_name},{ou_dn}",
                desired_state={
                    'userid': userid,
                    'connector_id': connector_id,
                    'ad_username': username,
                    'display_name': display_name,
                    'email': email,
                    'ou_path': target_dept.path,
                    'placement_reason': placement_reason,
                    'binding_resolution': binding_resolution_details.get(userid, {}),
                    'field_ownership_policy': dict(FIELD_OWNERSHIP_POLICY),
                    'lifecycle_profile': {
                        'employment_type': user_lifecycle_profile['employment_type'],
                        'start_value': user_lifecycle_profile['start_value'],
                        'end_value': user_lifecycle_profile['end_value'],
                        'sponsor_userid': user_lifecycle_profile['sponsor_userid'],
                    },
                },
            )
            connector_writeback_rules = (
                _select_mapping_rules(
                    enabled_mapping_rules,
                    direction='ad_to_source',
                    connector_id=connector_id,
                )
                if write_back_enabled
                else []
            )
            if connector_writeback_rules:
                add_planned_operation(
                    object_type='user',
                    operation_type='write_back_user',
                    source_id=userid,
                    department_id=str(target_dept.department_id),
                    target_dn=f"CN={display_name},{ou_dn}",
                    desired_state={
                        'connector_id': connector_id,
                        'ad_username': username,
                        'fields': [rule.target_field for rule in connector_writeback_rules],
                    },
                )

            seen_group_sams = set()
            if has_exception_rule('skip_user_group_membership', userid):
                record_exception_skip(
                    stage_name='plan',
                    object_type='group_membership',
                    operation_type='add_user_to_group',
                    exception_rule_type='skip_user_group_membership',
                    match_value=userid,
                    reason=f"skip managed group memberships for user {userid}: matched exception rule skip_user_group_membership",
                    source_id=userid,
                    target_id=username,
                    details={'userid': userid, 'ad_username': username},
                )
                continue
            for dept in departments_for_user:
                if not dept.path or is_department_excluded(dept):
                    continue

                group_target = get_department_group_target(dept)
                if group_target.policy.is_excluded:
                    record_group_policy_skip(
                        'plan',
                        'group_membership',
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
                membership_actions.append(
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
                add_planned_operation(
                    object_type='group_membership',
                    operation_type='add_user_to_group',
                    source_id=userid,
                    department_id=str(dept.department_id),
                    target_dn=group_dn,
                    desired_state={
                        'connector_id': group_connector_id,
                        'ad_username': username,
                        'group_sam': group_sam,
                        'display_name': group_target.display_name,
                        'binding_resolution': binding_resolution_details.get(userid, {}),
                    },
                )

        if group_recursive_enabled:
            for dept_id, dept_info in dept_tree.items():
                if is_cancelled():
                    raise InterruptedError('sync cancelled by user')
                if is_department_excluded(dept_info):
                    continue

                parent_department_id = get_effective_parent_department_id(dept_info)
                if not parent_department_id:
                    continue

                child_target = get_department_group_target(dept_info)
                parent_target = get_department_group_target(dept_tree[parent_department_id])
                if child_target.policy.is_excluded:
                    record_group_policy_skip(
                        'plan',
                        'group_hierarchy_child',
                        child_target,
                        f"skip recursive child group {child_target.group_sam}",
                    )
                    continue
                if parent_target.policy.is_excluded:
                    record_group_policy_skip(
                        'plan',
                        'group_hierarchy_parent',
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
                    for entry in get_current_parent_groups(child_target.group_dn, connector_id=connector_id)
                    if entry.group_sam
                }
                if parent_group_sam in current_parent_sams:
                    continue

                hierarchy_key = (connector_id, child_group_sam, parent_group_sam)
                if hierarchy_key in planned_hierarchy_pairs:
                    continue

                planned_hierarchy_pairs.add(hierarchy_key)
                group_hierarchy_actions.append(
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
                add_planned_operation(
                    object_type='group_hierarchy',
                    operation_type='add_group_to_group',
                    source_id=child_group_sam,
                    department_id=str(dept_id),
                    target_dn=parent_target.group_dn,
                    desired_state={
                        'connector_id': connector_id,
                        'child_group_dn': child_target.group_dn,
                        'parent_group_dn': parent_target.group_dn,
                        'parent_department_id': parent_department_id,
                        'parent_group_sam': parent_group_sam,
                    },
                )

        if group_recursive_enabled and managed_relation_cleanup_enabled:
            active_bindings = binding_repo.list_active_binding_records()
            active_bindings_by_sam = {
                binding.group_sam: binding for binding in active_bindings if binding.group_sam
            }

            for binding in active_bindings:
                if is_cancelled():
                    raise InterruptedError('sync cancelled by user')
                if not binding.group_sam or not binding.group_dn:
                    continue

                try:
                    binding_department_id = int(binding.department_id)
                except (TypeError, ValueError):
                    continue

                dept_info = dept_tree.get(binding_department_id)
                if not dept_info or is_department_excluded(dept_info):
                    continue

                child_target = get_department_group_target(dept_info)
                if child_target.policy.is_excluded:
                    record_group_policy_skip(
                        'plan',
                        'group_relation_cleanup_child',
                        child_target,
                        f"skip relation cleanup for child group {binding.group_sam}",
                    )
                    continue
                if has_exception_rule('skip_group_relation_cleanup', binding.group_sam):
                    record_exception_skip(
                        stage_name='plan',
                        object_type='group_hierarchy',
                        operation_type='remove_group_from_group',
                        exception_rule_type='skip_group_relation_cleanup',
                        match_value=binding.group_sam,
                        reason=f"skip cleanup for child group {binding.group_sam}: matched exception rule skip_group_relation_cleanup",
                        source_id=binding.group_sam,
                        department_id=str(binding_department_id),
                        target_dn=binding.group_dn,
                        risk_level='high',
                        details={'child_group_sam': binding.group_sam},
                    )
                    continue

                expected_parent_department_id = get_effective_parent_department_id(dept_info)
                expected_parent_target = None
                if expected_parent_department_id:
                    candidate_parent = get_department_group_target(dept_tree[expected_parent_department_id])
                    if not candidate_parent.policy.is_excluded:
                        expected_parent_target = candidate_parent

                expected_parent_sam = expected_parent_target.group_sam if expected_parent_target else None
                connector_id = get_connector_id_for_department(dept_info)
                for current_parent in get_current_parent_groups(binding.group_dn, connector_id=connector_id):
                    current_parent_sam = current_parent.group_sam
                    if not current_parent_sam:
                        continue

                    managed_parent_binding = active_bindings_by_sam.get(current_parent_sam)
                    if not managed_parent_binding:
                        continue
                    if has_exception_rule('skip_group_relation_cleanup', current_parent_sam):
                        record_exception_skip(
                            stage_name='plan',
                            object_type='group_hierarchy',
                            operation_type='remove_group_from_group',
                            exception_rule_type='skip_group_relation_cleanup',
                            match_value=current_parent_sam,
                            reason=f"skip cleanup against parent group {current_parent_sam}: matched exception rule skip_group_relation_cleanup",
                            source_id=binding.group_sam,
                            department_id=str(binding_department_id),
                            target_id=current_parent_sam,
                            target_dn=managed_parent_binding.get('group_dn') or current_parent.dn,
                            risk_level='high',
                            details={
                                'child_group_sam': binding.group_sam,
                                'parent_group_sam': current_parent_sam,
                            },
                        )
                        continue
                    if current_parent_sam == expected_parent_sam:
                        continue

                    parent_policy = evaluate_group_policy(
                        group_sam=managed_parent_binding.get('group_sam'),
                        group_dn=managed_parent_binding.get('group_dn') or current_parent.dn,
                        display_name=managed_parent_binding.get('display_name') or current_parent.display_name,
                    )
                    if parent_policy.is_excluded:
                        record_skip_detail(
                            stage_name='plan',
                            action_type='group_relation_cleanup_parent',
                            group_sam=current_parent_sam,
                            group_dn=managed_parent_binding.get('group_dn') or current_parent.dn,
                            reason=f"skip cleanup against excluded parent group {current_parent_sam}",
                            matched_rules=parent_policy.matched_rule_labels(),
                        )
                        record_event(
                            'INFO',
                            'group_relation_cleanup_skipped',
                            f"skip cleanup against excluded parent group {current_parent_sam}",
                            stage_name='plan',
                            payload={
                                'child_group_sam': binding.group_sam,
                                'parent_group_sam': current_parent_sam,
                            },
                        )
                        continue

                    cleanup_key = (connector_id, binding.group_sam, current_parent_sam)
                    if cleanup_key in planned_cleanup_pairs:
                        continue

                    planned_cleanup_pairs.add(cleanup_key)
                    group_cleanup_actions.append(
                        GroupCleanupAction(
                            connector_id=connector_id,
                            child_department_id=binding_department_id,
                            child_group_sam=binding.group_sam,
                            child_group_dn=binding.group_dn,
                            parent_group_sam=current_parent_sam,
                            parent_group_dn=managed_parent_binding.get('group_dn') or current_parent.dn,
                            expected_parent_group_sam=expected_parent_sam,
                        )
                    )
                    add_planned_operation(
                        object_type='group_hierarchy',
                        operation_type='remove_group_from_group',
                        source_id=binding.group_sam,
                        department_id=str(binding_department_id),
                        target_dn=managed_parent_binding.get('group_dn') or current_parent.dn,
                        desired_state={
                            'connector_id': connector_id,
                            'parent_group_sam': current_parent_sam,
                            'expected_parent_group_sam': expected_parent_sam,
                        },
                        risk_level='high',
                    )

        all_enabled_binding_records = user_binding_repo.list_enabled_binding_records()
        all_enabled_binding_source_user_id_by_identity = {
            (record.connector_id or 'default', record.ad_username): record.source_user_id
            for record in all_enabled_binding_records
            if record.ad_username and record.source_user_id
        }
        skip_sync_ad_identities = {
            (record.connector_id or 'default', record.ad_username)
            for record in all_enabled_binding_records
            if record.ad_username
            and record.source_user_id
            and has_exception_rule('skip_user_sync', record.source_user_id)
        }
        managed_source_user_id_by_identity = {
            (record.connector_id or 'default', record.ad_username): record.source_user_id
            for record in all_enabled_binding_records
            if record.ad_username and record.source_user_id
            and (record.connector_id or 'default', record.ad_username) not in skip_sync_ad_identities
        }
        managed_ad_identities = set(managed_source_user_id_by_identity.keys())

        for userid, resolution in binding_resolution_details.items():
            if not resolution.get('ad_username'):
                continue
            offboarding_repo.clear_pending(
                connector_id=resolution.get('connector_id') or 'default',
                ad_username=resolution['ad_username'],
            )

        def get_offboarding_manager_userids(source_user_id: str) -> list[str]:
            if source_user_id and source_user_id in user_departments:
                return extract_manager_userids(user_departments[source_user_id].user)
            if not source_user_id:
                return []
            state_row = state_repo.get_state('wecom', 'user', source_user_id)
            if not state_row:
                return []
            try:
                extra_payload = json.loads(state_row['extra_json']) if state_row['extra_json'] else {}
            except (TypeError, ValueError, json.JSONDecodeError):
                extra_payload = {}
            manager_userids = extra_payload.get('manager_userids')
            if not isinstance(manager_userids, list):
                return []
            return [str(value).strip() for value in manager_userids if str(value).strip()]

        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.isoformat(timespec='seconds')
        for connector_id, connector_enabled_users in enabled_ad_users_by_connector.items():
            connector_current_usernames = current_wecom_ad_usernames_by_connector.get(connector_id, set())
            connector_enabled_set = set(connector_enabled_users)

            for username in sorted(connector_enabled_set):
                identity_key = (connector_id, username)
                skipped_source_user_id = all_enabled_binding_source_user_id_by_identity.get(identity_key, '')
                if identity_key in skip_sync_ad_identities:
                    if not skipped_source_user_id or skipped_source_user_id in wecom_users:
                        continue
                    record_exception_skip(
                        stage_name='plan',
                        object_type='user',
                        operation_type='disable_user',
                        exception_rule_type='skip_user_sync',
                        match_value=skipped_source_user_id,
                        reason=f"skip disable for AD user {username}: matched exception rule skip_user_sync",
                        source_id=skipped_source_user_id,
                        target_id=username,
                        risk_level='high',
                        details={'source_user_id': skipped_source_user_id, 'ad_username': username, 'connector_id': connector_id},
                    )
                    continue
                if identity_key not in managed_ad_identities or username in connector_current_usernames:
                    continue

                managed_source_user_id = managed_source_user_id_by_identity.get(identity_key, '')
                if managed_source_user_id and has_exception_rule('skip_user_disable', managed_source_user_id):
                    record_exception_skip(
                        stage_name='plan',
                        object_type='user',
                        operation_type='disable_user',
                        exception_rule_type='skip_user_disable',
                        match_value=managed_source_user_id,
                        reason=f"skip disable for AD user {username}: matched exception rule skip_user_disable",
                        source_id=managed_source_user_id,
                        target_id=username,
                        risk_level='high',
                        details={'source_user_id': managed_source_user_id, 'ad_username': username, 'connector_id': connector_id},
                    )
                    continue

                if is_protected_ad_account(username, connector_id):
                    record_protected_account_skip(
                        stage_name='plan',
                        object_type='user',
                        operation_type='disable_user',
                        connector_id=connector_id,
                        ad_username=username,
                        source_id=managed_source_user_id or username,
                        risk_level='high',
                        details={
                            'source_user_id': managed_source_user_id,
                            'reason': 'missing_from_wecom',
                        },
                    )
                    continue

                pending_offboarding = offboarding_repo.get_record(
                    connector_id=connector_id,
                    ad_username=username,
                )
                if offboarding_grace_days > 0:
                    due_at = pending_offboarding.due_at if pending_offboarding and pending_offboarding.status == 'pending' else (
                        now_dt + timedelta(days=offboarding_grace_days)
                    ).isoformat(timespec='seconds')
                    manager_userids = get_offboarding_manager_userids(managed_source_user_id)
                    if not pending_offboarding or pending_offboarding.status != 'pending':
                        offboarding_repo.upsert_pending_for_source_user(
                            connector_id=connector_id,
                            source_user_id=managed_source_user_id,
                            ad_username=username,
                            due_at=due_at,
                            reason='missing_from_wecom',
                            manager_userids=manager_userids,
                            last_job_id=job_id,
                        )
                    if offboarding_notify_managers and bot and (not pending_offboarding or not pending_offboarding.notified_at):
                        bot.send_message(
                            f"## {source_provider_name}-AD offboarding pending\n\n"
                            f"> Connector: {connector_id}\n"
                            f"> AD user: {username}\n"
                            f"> Source user: {managed_source_user_id or 'unknown'}\n"
                            f"> Grace period ends: {due_at}\n"
                            f"> Managers: {', '.join(manager_userids) if manager_userids else 'n/a'}"
                        )
                        offboarding_repo.mark_notified(connector_id=connector_id, ad_username=username)
                    if str(due_at) > now_iso:
                        add_planned_operation(
                            object_type='user',
                            operation_type='queue_user_disable',
                            source_id=managed_source_user_id or username,
                            risk_level='normal',
                            desired_state={
                                'connector_id': connector_id,
                                'ad_username': username,
                                'reason': 'pending_offboarding_grace',
                                'due_at': due_at,
                            },
                        )
                        continue

                disable_actions.append(
                    DisableUserAction(
                        connector_id=connector_id,
                        username=username,
                        source_user_id=managed_source_user_id,
                        reason='missing_from_wecom',
                    )
                )
                add_planned_operation(
                    object_type='user',
                    operation_type='disable_user',
                    source_id=managed_source_user_id or username,
                    risk_level='high',
                    desired_state={
                        'connector_id': connector_id,
                        'ad_username': username,
                        'reason': 'missing_from_wecom',
                    },
                )

        disable_breaker_triggered = False
        disable_breaker_threshold = 0
        managed_user_baseline = max(
            int(sync_stats['total_users'] or 0),
            len(managed_ad_identities),
            len(enabled_ad_users),
        )
        if disable_breaker_enabled and managed_user_baseline > 0:
            percent_threshold = math.ceil(managed_user_baseline * (disable_breaker_percent / 100.0))
            disable_breaker_threshold = max(disable_breaker_min_count, percent_threshold)
            disable_breaker_triggered = len(disable_actions) >= disable_breaker_threshold > 0
            if disable_breaker_triggered:
                record_event(
                    'WARNING',
                    'disable_circuit_breaker',
                    (
                        f"disable circuit breaker triggered: {len(disable_actions)} pending disables "
                        f"exceeds threshold {disable_breaker_threshold}"
                    ),
                    stage_name='plan',
                    payload={
                        'pending_disable_count': len(disable_actions),
                        'threshold_count': disable_breaker_threshold,
                        'total_users': sync_stats['total_users'],
                        'managed_user_baseline': managed_user_baseline,
                        'percent_threshold': disable_breaker_percent,
                    },
                )
        plan_fingerprint = _compute_plan_fingerprint(plan_fingerprint_items)
        review_required_for_high_risk = settings_repo.get_bool('high_risk_apply_requires_review', True)
        review_ttl_minutes = max(settings_repo.get_int('high_risk_review_ttl_minutes', 240), 1)
        approved_review = None
        if disable_breaker_triggered and disable_breaker_requires_approval:
            sync_stats['review_required'] = True
            breaker_summary = {
                'org_id': organization.org_id,
                'organization_name': organization.name,
                'mode': execution_mode,
                'pending_disable_count': len(disable_actions),
                'threshold_count': disable_breaker_threshold,
                'percent_threshold': disable_breaker_percent,
                'managed_user_baseline': managed_user_baseline,
                'review_required': True,
                'plan_fingerprint': plan_fingerprint,
                'reason': 'disable_circuit_breaker',
            }
            if execution_mode == 'dry_run':
                review_repo.upsert_review_request(
                    job_id=job_id,
                    plan_fingerprint=plan_fingerprint,
                    config_snapshot_hash=config_hash,
                    high_risk_operation_count=high_risk_operation_count,
                )
            else:
                sync_stats['summary'] = breaker_summary
                sync_stats['job_summary'] = SyncJobSummary.from_sync_stats(sync_stats).to_dict()
                mark_job('REVIEW_REQUIRED', ended=True, summary=breaker_summary)
                record_operation(
                    stage_name='plan',
                    object_type='review',
                    operation_type='disable_circuit_breaker',
                    status='review_required',
                    message='apply blocked by disable circuit breaker policy',
                    source_id=job_id,
                    risk_level='high',
                    reason_code='disable_circuit_breaker',
                    details=breaker_summary,
                )
                if bot:
                    bot.send_message(
                        f"## {source_provider_name}-AD sync blocked by circuit breaker\n\n"
                        f"> Pending disables: {len(disable_actions)}\n"
                        f"> Threshold: {disable_breaker_threshold}\n"
                        f"> Managed user baseline: {managed_user_baseline}"
                    )
                return sync_stats.to_dict()
        if high_risk_operation_count and review_required_for_high_risk:
            sync_stats['review_required'] = True
            if execution_mode == 'dry_run':
                review_repo.upsert_review_request(
                    job_id=job_id,
                    plan_fingerprint=plan_fingerprint,
                    config_snapshot_hash=config_hash,
                    high_risk_operation_count=high_risk_operation_count,
                )
                record_event(
                    'WARNING',
                    'high_risk_review_pending',
                    f"dry-run generated {high_risk_operation_count} high-risk operations and requires approval before apply",
                    stage_name='plan',
                    payload={
                        'plan_fingerprint': plan_fingerprint,
                        'high_risk_operation_count': high_risk_operation_count,
                    },
                )
                record_operation(
                    stage_name='plan',
                    object_type='review',
                    operation_type='require_high_risk_review',
                    status='pending',
                    message='dry-run generated high-risk operations and created a pending review request',
                    source_id=job_id,
                    risk_level='high',
                    reason_code='high_risk_review_required',
                    details={
                        'plan_fingerprint': plan_fingerprint,
                        'high_risk_operation_count': high_risk_operation_count,
                    },
                )
            else:
                approved_review = review_repo.find_matching_approved_review(
                    plan_fingerprint=plan_fingerprint,
                    config_snapshot_hash=config_hash,
                    now_iso=datetime.now(timezone.utc).isoformat(timespec='seconds'),
                )
                if not approved_review:
                    summary = {
                        'org_id': organization.org_id,
                        'organization_name': organization.name,
                        'mode': execution_mode,
                        'planned_operation_count': planned_count,
                        'executed_operation_count': 0,
                        'error_count': sync_stats['error_count'],
                        'conflict_count': sync_stats['conflict_count'],
                        'high_risk_operation_count': high_risk_operation_count,
                        'review_required': True,
                        'plan_fingerprint': plan_fingerprint,
                        'review_hint': 'Approve the matching dry-run review before rerunning apply',
                    }
                    sync_stats['summary'] = summary
                    sync_stats['job_summary'] = SyncJobSummary.from_sync_stats(sync_stats).to_dict()
                    mark_job('REVIEW_REQUIRED', ended=True, summary=summary)
                    record_event(
                        'WARNING',
                        'review_required',
                        f"apply blocked: {high_risk_operation_count} high-risk operations require approved dry-run review",
                        stage_name='plan',
                        payload={
                            'plan_fingerprint': plan_fingerprint,
                            'high_risk_operation_count': high_risk_operation_count,
                        },
                    )
                    record_operation(
                        stage_name='plan',
                        object_type='review',
                        operation_type='require_high_risk_review',
                        status='review_required',
                        message='apply blocked until a matching dry-run plan is approved',
                        source_id=job_id,
                        risk_level='high',
                        reason_code='high_risk_review_required',
                        details=summary,
                    )
                    if bot:
                        bot.send_message(
                            f'## {source_provider_name}-AD sync review required\n\n'
                            f"> Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                            f"> High-risk operations: {high_risk_operation_count}\n"
                            '> Result: blocked pending dry-run approval'
                        )
                    return sync_stats.to_dict()

        mark_job('READY')
        record_event(
            'INFO',
            'plan_ready',
            'sync plan generated',
            stage_name='plan',
            payload={
                'department_actions': len(department_actions),
                'user_actions': len(user_actions),
                'membership_actions': len(membership_actions),
                'group_hierarchy_actions': len(group_hierarchy_actions),
                'group_cleanup_actions': len(group_cleanup_actions),
                'disable_actions': len(disable_actions),
                'conflict_count': sync_stats['conflict_count'],
                'high_risk_operation_count': high_risk_operation_count,
                'plan_fingerprint': plan_fingerprint,
            },
        )

        if execution_mode == 'dry_run':
            sync_stats['skip_detail_report'] = _generate_skip_detail_report(sync_stats)
            sync_stats['review_required'] = bool(high_risk_operation_count and review_required_for_high_risk)
            summary = {
                'org_id': organization.org_id,
                'organization_name': organization.name,
                'mode': execution_mode,
                'planned_operation_count': planned_count,
                'executed_operation_count': 0,
                'department_actions': len(department_actions),
                'user_actions': len(user_actions),
                'membership_actions': len(membership_actions),
                'group_hierarchy_actions': len(group_hierarchy_actions),
                'group_cleanup_actions': len(group_cleanup_actions),
                'disable_actions': len(disable_actions),
                'conflict_count': sync_stats['conflict_count'],
                'high_risk_operation_count': high_risk_operation_count,
                'review_required': sync_stats['review_required'],
                'plan_fingerprint': plan_fingerprint,
                'field_ownership_policy': dict(FIELD_OWNERSHIP_POLICY),
                'skipped_operation_count': sync_stats['skipped_operations']['total'],
                'skipped_by_action': dict(sync_stats['skipped_operations']['by_action']),
            }
            sync_stats['summary'] = summary
            sync_stats['job_summary'] = SyncJobSummary.from_sync_stats(sync_stats).to_dict()
            sync_stats['disabled_users'] = [f"{action.connector_id}:{action.username}" for action in disable_actions]
            mark_job('COMPLETED', ended=True, summary=summary)
            return sync_stats.to_dict()

        mark_job('RUNNING')
        if bot:
            bot.send_message(
                f"## {source_provider_name}-AD sync started (LDAPS)\n\n"
                f"> Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"> Domain: {config.domain}\n"
                f"> LDAP server: {config.ldap.server}\n"
                f"> SSL: {'yes' if config.ldap.use_ssl else 'no'}"
            )

        for idx, action in enumerate(
            sorted(department_actions, key=lambda item: (len(item.full_path), item.department_id)),
            start=1,
        ):
            if is_cancelled():
                raise InterruptedError('sync cancelled by user')

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
                        group_target.policy = evaluate_group_policy(
                            group_sam=group_info.group_sam,
                            group_dn=group_info.group_dn,
                            display_name=group_info.display_name,
                        )
                        department_group_targets[action.department_id] = group_target
                    else:
                        record_group_policy_skip(
                            'apply',
                            'department_group_management',
                            group_target,
                            f"skip managed group for department {action.ou_name}",
                        )

                    state_repo.upsert_state(
                        source_type='wecom',
                        object_type='department',
                        source_id=str(action.department_id),
                        source_hash=hash_department_state(
                            {
                                'id': action.department_id,
                                'name': action.ou_name,
                                'parentid': action.parent_department_id or 0,
                            }
                        ),
                        display_name=action.ou_name,
                        target_dn=ensured_ou_dn,
                        last_job_id=job_id,
                        last_action='sync_department',
                        last_status='success',
                        extra={
                            'path': action.full_path,
                            'group_sam': group_info.group_sam,
                            'group_management': 'managed' if action.should_manage_group else 'skipped_by_policy',
                        },
                    )
                    if ou_created:
                        sync_stats['operations']['departments_created'] += 1
                    else:
                        sync_stats['operations']['departments_existed'] += 1

                record_operation(
                    stage_name='apply',
                    object_type='department',
                    operation_type='ensure_department_node',
                    status='succeeded',
                    message=f"ensured department node for {action.ou_name}",
                    source_id=str(action.department_id),
                    department_id=str(action.department_id),
                    target_dn=ensured_ou_dn or action.ou_dn,
                    target_id=group_info.group_sam if ensured_ou_dn and action.should_manage_group else '',
                    details={
                        'ou_created': ou_created,
                        'group_management': 'managed' if action.should_manage_group else 'skipped_by_policy',
                        'group_sam': group_info.group_sam,
                        'group_dn': group_info.group_dn,
                    },
                )

                executed_count += 1
                sync_stats['executed_operation_count'] = executed_count
                if stats_callback:
                    stats_callback('department_progress', idx / max(len(department_actions), 1))
            except Exception as department_error:
                logger.error(f"failed to sync department {action.ou_name}: {department_error}")
                sync_stats['errors']['department_errors'].append(
                    {
                        'department': action.ou_name,
                        'path': ' > '.join(action.full_path),
                        'error': str(department_error),
                    }
                )
                sync_stats['error_count'] += 1
                record_operation(
                    stage_name='apply',
                    object_type='department',
                    operation_type='ensure_department_node',
                    status='error',
                    message=f"failed to sync department {action.ou_name}: {department_error}",
                    source_id=str(action.department_id),
                    department_id=str(action.department_id),
                    target_dn=action.ou_dn,
                    details={
                        'path': action.full_path,
                        'error': str(department_error),
                    },
                )

        if stats_callback:
            stats_callback('department_sync_done', True)

        successful_hierarchy_pairs = set()
        for action in group_hierarchy_actions:
            if is_cancelled():
                raise InterruptedError('sync cancelled by user')

            try:
                child_binding = binding_repo.get_binding_record_by_department_id(str(action.child_department_id))
                if child_binding and child_binding.status != 'active':
                    child_binding = None
                parent_binding = binding_repo.get_binding_record_by_department_id(str(action.parent_department_id))
                if parent_binding and parent_binding.status != 'active':
                    parent_binding = None

                child_group_sam = child_binding.group_sam if child_binding else action.child_group_sam
                child_group_dn = child_binding.group_dn if child_binding and child_binding.group_dn else action.child_group_dn
                child_display_name = child_binding.display_name if child_binding else action.child_display_name
                parent_group_sam = parent_binding.group_sam if parent_binding else action.parent_group_sam
                parent_group_dn = parent_binding.group_dn if parent_binding and parent_binding.group_dn else action.parent_group_dn
                parent_display_name = parent_binding.display_name if parent_binding else action.parent_display_name

                child_policy = evaluate_group_policy(
                    group_sam=child_group_sam,
                    group_dn=child_group_dn,
                    display_name=child_display_name,
                )
                parent_policy = evaluate_group_policy(
                    group_sam=parent_group_sam,
                    group_dn=parent_group_dn,
                    display_name=parent_display_name,
                )
                if child_policy.is_excluded:
                    record_group_policy_skip(
                        'apply',
                        'group_hierarchy_child',
                        ManagedGroupTarget(
                            exists=True,
                            group_sam=child_group_sam,
                            group_cn=child_group_sam,
                            group_dn=child_group_dn,
                            display_name=child_display_name or "",
                            description="",
                            binding_source="runtime",
                            created=False,
                            binding_exists=True,
                            department_id=action.child_department_id,
                            parent_department_id=action.parent_department_id,
                            ou_name="",
                            ou_dn="",
                            full_path=[],
                            policy=child_policy,
                        ),
                        f"skip recursive child group {child_group_sam}",
                    )
                    continue
                if parent_policy.is_excluded:
                    record_group_policy_skip(
                        'apply',
                        'group_hierarchy_parent',
                        ManagedGroupTarget(
                            exists=True,
                            group_sam=parent_group_sam,
                            group_cn=parent_group_sam,
                            group_dn=parent_group_dn,
                            display_name=parent_display_name or "",
                            description="",
                            binding_source="runtime",
                            created=False,
                            binding_exists=True,
                            department_id=action.parent_department_id,
                            parent_department_id=None,
                            ou_name="",
                            ou_dn="",
                            full_path=[],
                            policy=parent_policy,
                        ),
                        f"skip recursive parent group {parent_group_sam}",
                    )
                    continue

                if not child_group_dn or not parent_group_dn:
                    raise Exception('group DN missing for recursive relation')
                if not get_ad_sync(action.connector_id).add_group_to_group(child_group_dn, parent_group_dn):
                    raise Exception(f"failed to add group relation {child_group_sam} -> {parent_group_sam}")

                successful_hierarchy_pairs.add((action.connector_id, child_group_sam, parent_group_sam))
                sync_stats['operations']['groups_nested'] += 1
                record_operation(
                    stage_name='apply',
                    object_type='group_hierarchy',
                    operation_type='add_group_to_group',
                    status='succeeded',
                    message=f"added group relation {child_group_sam} -> {parent_group_sam}",
                    source_id=child_group_sam,
                    department_id=str(action.child_department_id),
                    target_id=parent_group_sam,
                    target_dn=parent_group_dn,
                    details={
                        'child_group_dn': child_group_dn,
                        'parent_group_dn': parent_group_dn,
                    },
                )
                executed_count += 1
                sync_stats['executed_operation_count'] = executed_count
            except Exception as hierarchy_error:
                sync_stats['errors']['group_hierarchy_errors'].append(
                    {
                        'child_group_sam': action.child_group_sam,
                        'parent_group_sam': action.parent_group_sam,
                        'error': str(hierarchy_error),
                    }
                )
                sync_stats['error_count'] += 1
                record_operation(
                    stage_name='apply',
                    object_type='group_hierarchy',
                    operation_type='add_group_to_group',
                    status='error',
                    message=f"failed to add group relation {action.child_group_sam} -> {action.parent_group_sam}: {hierarchy_error}",
                    source_id=action.child_group_sam,
                    department_id=str(action.child_department_id),
                    target_id=action.parent_group_sam,
                    target_dn=action.parent_group_dn,
                    details={'error': str(hierarchy_error)},
                )

        processed_users = set()
        for index, action in enumerate(user_actions, start=1):
            if index % 10 == 0 and is_cancelled():
                raise InterruptedError('sync cancelled by user')

            if action.operation_type == 'create_user':
                bucket = 'user_create_errors'
            else:
                bucket = 'user_update_errors'
            try:
                connector_ad_sync = get_ad_sync(action.connector_id)
                connector_wecom_to_ad_rules = (
                    _select_mapping_rules(
                        enabled_mapping_rules,
                        direction='source_to_ad',
                        connector_id=action.connector_id,
                    )
                    if attribute_mapping_enabled
                    else []
                )
                extra_attributes = build_wecom_to_ad_mapping_payload(
                    action.user,
                    connector_id=action.connector_id,
                    ad_username=action.username,
                    email=action.email,
                    target_department=dept_tree.get(action.target_department_id),
                    rules=connector_wecom_to_ad_rules,
                )
                if action.operation_type == 'update_user':
                    success = connector_ad_sync.update_user(
                        action.username,
                        action.display_name,
                        action.email,
                        action.ou_dn,
                        extra_attributes=extra_attributes,
                    )
                    if success:
                        sync_stats['operations']['users_updated'] += 1
                elif action.operation_type == 'reactivate_user':
                    success = connector_ad_sync.reactivate_user(
                        action.username,
                        action.display_name,
                        action.email,
                        action.ou_dn,
                        extra_attributes=extra_attributes,
                    )
                    if success:
                        sync_stats['operations']['users_updated'] += 1
                else:
                    success = connector_ad_sync.create_user(
                        action.username,
                        action.display_name,
                        action.email,
                        action.ou_dn,
                        extra_attributes=extra_attributes,
                    )
                    if success:
                        sync_stats['operations']['users_created'] += 1

                if not success:
                    raise Exception('LDAP operation returned failure')

                state_payload = action.user.to_state_payload()
                state_payload.update(
                    {
                        'connector_id': action.connector_id,
                        'ad_username': action.username,
                        'target_department_id': action.target_department_id,
                        'placement_reason': action.placement_reason,
                        'lifecycle_profile': serialize_lifecycle_profile(action.lifecycle_profile),
                    }
                )
                state_manager.update_user_state(
                    action.user.userid,
                    state_payload,
                    job_id=job_id,
                    target_dn=f"CN={action.display_name},{action.ou_dn}",
                )
                record_operation(
                    stage_name='apply',
                    object_type='user',
                    operation_type=action.operation_type,
                    status='succeeded',
                    message=f"{action.operation_type} succeeded for {action.username}",
                    source_id=action.user.userid,
                    department_id=str(action.target_department_id),
                    target_id=action.username,
                    target_dn=f"CN={action.display_name},{action.ou_dn}",
                    rule_source=binding_resolution_details.get(action.user.userid, {}).get('source'),
                    reason_code=action.placement_reason,
                    details={
                        'connector_id': action.connector_id,
                        'binding_resolution': binding_resolution_details.get(action.user.userid, {}),
                        'ou_path': action.ou_path,
                        'email': action.email,
                        'mapped_attributes': sorted(extra_attributes.keys()),
                        'field_ownership_policy': dict(FIELD_OWNERSHIP_POLICY),
                    },
                )
                connector_writeback_rules = (
                    _select_mapping_rules(
                        enabled_mapping_rules,
                        direction='ad_to_source',
                        connector_id=action.connector_id,
                    )
                    if write_back_enabled
                    else []
                )
                if connector_writeback_rules:
                    ad_attributes = connector_ad_sync.get_user_details(action.username)
                    writeback_payload = _sanitize_wecom_writeback_payload(
                        build_ad_to_wecom_mapping_payload(
                            ad_attributes,
                            action.user.to_state_payload(),
                            connector_id=action.connector_id,
                            rules=connector_writeback_rules,
                        )
                    )
                    if writeback_payload:
                        source_provider.update_user(action.user.userid, writeback_payload)
                        record_operation(
                            stage_name='apply',
                            object_type='user',
                            operation_type='write_back_user',
                            status='succeeded',
                            message=f"wrote AD attributes back to source provider for {action.user.userid}",
                            source_id=action.user.userid,
                            target_id=action.username,
                            risk_level='normal',
                            details={
                                'connector_id': action.connector_id,
                                'fields': sorted(writeback_payload.keys()),
                            },
                        )
                if action.lifecycle_profile.get('start_at'):
                    lifecycle_repo.mark_completed_for_source_user(
                        lifecycle_type='future_onboarding',
                        connector_id=action.connector_id,
                        source_user_id=action.user.userid,
                        last_job_id=job_id,
                    )
                executed_count += 1
                sync_stats['executed_operation_count'] = executed_count
                processed_users.add(action.user.userid)
                if stats_callback and (len(user_actions) < 100 or index % 5 == 0 or index == 1):
                    stats_callback('user_processed', index)
            except Exception as user_error:
                sync_stats['errors'][bucket].append(
                    {
                        'userid': action.user.userid,
                        'username': action.username,
                        'display_name': action.display_name,
                        'email': action.email,
                        'department': action.ou_path[-1] if action.ou_path else '',
                        'placement_reason': action.placement_reason,
                        'error': str(user_error),
                    }
                )
                sync_stats['error_count'] += 1
                record_operation(
                    stage_name='apply',
                    object_type='user',
                    operation_type=action.operation_type,
                    status='error',
                    message=f"{action.operation_type} failed for {action.username}: {user_error}",
                    source_id=action.user.userid,
                    department_id=str(action.target_department_id),
                    target_id=action.username,
                    target_dn=f"CN={action.display_name},{action.ou_dn}",
                    rule_source=binding_resolution_details.get(action.user.userid, {}).get('source'),
                    reason_code=action.placement_reason,
                    details={'error': str(user_error)},
                )

        sync_stats['processed_users'] = len(processed_users)

        for action in custom_group_actions:
            try:
                connector_ad_sync = get_ad_sync(action['connector_id'])
                group_info = connector_ad_sync.ensure_custom_group(
                    source_type=action['source_type'],
                    source_key=action['source_key'],
                    display_name=action['display_name'],
                )
                custom_group_binding_repo.upsert_binding(
                    connector_id=action['connector_id'],
                    source_type=action['source_type'],
                    source_key=action['source_key'],
                    group_sam=group_info.group_sam,
                    group_dn=group_info.group_dn,
                    group_cn=group_info.group_cn,
                    display_name=group_info.display_name,
                    status='active',
                )
                record_operation(
                    stage_name='apply',
                    object_type='custom_group',
                    operation_type='ensure_custom_group',
                    status='succeeded',
                    message=f"ensured custom group {action['source_type']}:{action['source_key']}",
                    source_id=f"{action['source_type']}:{action['source_key']}",
                    target_id=group_info.group_sam,
                    target_dn=group_info.group_dn,
                    details={
                        'connector_id': action['connector_id'],
                        'display_name': group_info.display_name,
                    },
                )
            except Exception as custom_group_error:
                sync_stats['error_count'] += 1
                record_operation(
                    stage_name='apply',
                    object_type='custom_group',
                    operation_type='ensure_custom_group',
                    status='error',
                    message=(
                        f"failed to ensure custom group {action['source_type']}:{action['source_key']}: "
                        f"{custom_group_error}"
                    ),
                    source_id=f"{action['source_type']}:{action['source_key']}",
                    details={'connector_id': action['connector_id'], 'error': str(custom_group_error)},
                )

        for action in membership_actions:
            try:
                if not action.group_dn and action.group_sam.startswith('WECOM_'):
                    for binding in custom_group_binding_repo.list_active_records(connector_id=action.connector_id):
                        if binding.group_sam == action.group_sam:
                            action.group_dn = binding.group_dn
                            if binding.display_name:
                                action.group_display_name = binding.display_name
                            break
                if action.source_user_id and has_exception_rule('skip_user_group_membership', action.source_user_id):
                    record_exception_skip(
                        stage_name='apply',
                        object_type='group_membership',
                        operation_type='add_user_to_group',
                        exception_rule_type='skip_user_group_membership',
                        match_value=action.source_user_id,
                        reason=f"skip managed group memberships for user {action.source_user_id}: matched exception rule skip_user_group_membership",
                        source_id=action.source_user_id,
                        department_id=str(action.department_id),
                        target_id=action.group_sam,
                        target_dn=action.group_dn,
                        details={'source_user_id': action.source_user_id, 'ad_username': action.username, 'group_sam': action.group_sam},
                    )
                    continue
                membership_policy = evaluate_group_policy(
                    group_sam=action.group_sam,
                    group_dn=action.group_dn,
                    display_name=action.group_display_name,
                )
                if membership_policy.is_excluded:
                    record_group_policy_skip(
                        'apply',
                        'group_membership',
                        ManagedGroupTarget(
                            exists=True,
                            group_sam=action.group_sam,
                            group_cn=action.group_sam,
                            group_dn=action.group_dn,
                            display_name=action.group_display_name,
                            description="",
                            binding_source="runtime",
                            created=False,
                            binding_exists=True,
                            department_id=action.department_id,
                            parent_department_id=None,
                            ou_name="",
                            ou_dn="",
                            full_path=[],
                            policy=membership_policy,
                        ),
                        f"skip user membership management for group {action.group_sam}",
                    )
                    continue

                if not get_ad_sync(action.connector_id).add_user_to_group(action.username, action.group_sam):
                    raise Exception(f"failed to add {action.username} to {action.group_sam}")

                sync_stats['operations']['groups_assigned'] += 1
                record_operation(
                    stage_name='apply',
                    object_type='group_membership',
                    operation_type='add_user_to_group',
                    status='succeeded',
                    message=f"added {action.username} to {action.group_sam}",
                    source_id=action.username,
                    department_id=str(action.department_id),
                    target_id=action.group_sam,
                    target_dn=action.group_dn,
                    details={'group_display_name': action.group_display_name},
                )
                executed_count += 1
                sync_stats['executed_operation_count'] = executed_count
            except Exception as membership_error:
                sync_stats['errors']['group_add_errors'].append(
                    {
                        'username': action.username,
                        'groups': action.group_sam,
                        'error': str(membership_error),
                    }
                )
                sync_stats['error_count'] += 1
                record_operation(
                    stage_name='apply',
                    object_type='group_membership',
                    operation_type='add_user_to_group',
                    status='error',
                    message=f"failed to add {action.username} to {action.group_sam}: {membership_error}",
                    source_id=action.username,
                    department_id=str(action.department_id),
                    target_id=action.group_sam,
                    target_dn=action.group_dn,
                    details={'error': str(membership_error)},
                )

        if group_recursive_enabled and managed_relation_cleanup_enabled:
            for action in group_cleanup_actions:
                if is_cancelled():
                    raise InterruptedError('sync cancelled by user')

                try:
                    if has_exception_rule('skip_group_relation_cleanup', action.child_group_sam):
                        record_exception_skip(
                            stage_name='apply',
                            object_type='group_hierarchy',
                            operation_type='remove_group_from_group',
                            exception_rule_type='skip_group_relation_cleanup',
                            match_value=action.child_group_sam,
                            reason=f"skip cleanup for child group {action.child_group_sam}: matched exception rule skip_group_relation_cleanup",
                            source_id=action.child_group_sam,
                            department_id=str(action.child_department_id),
                            target_dn=action.child_group_dn,
                            risk_level='high',
                            details={'child_group_sam': action.child_group_sam},
                        )
                        continue
                    if has_exception_rule('skip_group_relation_cleanup', action.parent_group_sam):
                        record_exception_skip(
                            stage_name='apply',
                            object_type='group_hierarchy',
                            operation_type='remove_group_from_group',
                            exception_rule_type='skip_group_relation_cleanup',
                            match_value=action.parent_group_sam,
                            reason=f"skip cleanup against parent group {action.parent_group_sam}: matched exception rule skip_group_relation_cleanup",
                            source_id=action.child_group_sam,
                            department_id=str(action.child_department_id),
                            target_id=action.parent_group_sam,
                            target_dn=action.parent_group_dn,
                            risk_level='high',
                            details={
                                'child_group_sam': action.child_group_sam,
                                'parent_group_sam': action.parent_group_sam,
                            },
                        )
                        continue
                    child_binding = binding_repo.get_binding_record_by_department_id(str(action.child_department_id))
                    if child_binding and child_binding.status != 'active':
                        child_binding = None
                    parent_binding = binding_repo.get_binding_record_by_group_sam(action.parent_group_sam)
                    if parent_binding and parent_binding.status != 'active':
                        parent_binding = None

                    child_group_sam = child_binding.group_sam if child_binding else action.child_group_sam
                    child_group_dn = child_binding.group_dn if child_binding and child_binding.group_dn else action.child_group_dn
                    parent_group_sam = parent_binding.group_sam if parent_binding else action.parent_group_sam
                    parent_group_dn = parent_binding.group_dn if parent_binding and parent_binding.group_dn else action.parent_group_dn
                    expected_parent_group_sam = action.expected_parent_group_sam

                    if expected_parent_group_sam:
                        expected_pair = (action.connector_id, child_group_sam, expected_parent_group_sam)
                        if expected_pair in planned_hierarchy_pairs and expected_pair not in successful_hierarchy_pairs:
                            record_skip_detail(
                                stage_name='apply',
                                action_type='group_relation_cleanup_deferred',
                                group_sam=child_group_sam,
                                group_dn=child_group_dn,
                                reason=f"skip cleanup for {child_group_sam} because expected parent relation was not ensured",
                                matched_rules=[],
                            )
                            record_event(
                                'WARNING',
                                'group_relation_cleanup_deferred',
                                f"skip cleanup for {child_group_sam} because expected parent relation was not ensured",
                                stage_name='apply',
                                payload={
                                    'child_group_sam': child_group_sam,
                                    'expected_parent_group_sam': expected_parent_group_sam,
                                    'stale_parent_group_sam': parent_group_sam,
                                },
                            )
                            continue

                    child_policy = evaluate_group_policy(group_sam=child_group_sam, group_dn=child_group_dn)
                    parent_policy = evaluate_group_policy(group_sam=parent_group_sam, group_dn=parent_group_dn)
                    if child_policy.is_excluded:
                        record_group_policy_skip(
                            'apply',
                            'group_relation_cleanup_child',
                            ManagedGroupTarget(
                                exists=True,
                                group_sam=child_group_sam,
                                group_cn=child_group_sam,
                                group_dn=child_group_dn,
                                display_name=child_binding.display_name if child_binding else "",
                                description="",
                                binding_source="runtime",
                                created=False,
                                binding_exists=True,
                                department_id=action.child_department_id,
                                parent_department_id=None,
                                ou_name="",
                                ou_dn="",
                                full_path=[],
                                policy=child_policy,
                            ),
                            f"skip relation cleanup for child group {child_group_sam}",
                        )
                        continue
                    if parent_policy.is_excluded:
                        record_group_policy_skip(
                            'apply',
                            'group_relation_cleanup_parent',
                            ManagedGroupTarget(
                                exists=True,
                                group_sam=parent_group_sam,
                                group_cn=parent_group_sam,
                                group_dn=parent_group_dn,
                                display_name=parent_binding.display_name if parent_binding else "",
                                description="",
                                binding_source="runtime",
                                created=False,
                                binding_exists=True,
                                department_id=action.child_department_id,
                                parent_department_id=None,
                                ou_name="",
                                ou_dn="",
                                full_path=[],
                                policy=parent_policy,
                            ),
                            f"skip relation cleanup for parent group {parent_group_sam}",
                        )
                        continue

                    if not child_group_dn or not parent_group_dn:
                        raise Exception('group DN missing for cleanup relation')
                    if not get_ad_sync(action.connector_id).remove_group_from_group(child_group_dn, parent_group_dn):
                        raise Exception(f"failed to remove stale group relation {child_group_sam} -> {parent_group_sam}")

                    sync_stats['operations']['group_relations_removed'] += 1
                    record_operation(
                        stage_name='apply',
                        object_type='group_hierarchy',
                        operation_type='remove_group_from_group',
                        status='succeeded',
                        message=f"removed stale group relation {child_group_sam} -> {parent_group_sam}",
                        source_id=child_group_sam,
                        department_id=str(action.child_department_id),
                        target_id=parent_group_sam,
                        target_dn=parent_group_dn,
                        risk_level='high',
                        details={'expected_parent_group_sam': expected_parent_group_sam},
                    )
                    executed_count += 1
                    sync_stats['executed_operation_count'] = executed_count
                except Exception as cleanup_error:
                    sync_stats['errors']['group_relation_cleanup_errors'].append(
                        {
                            'child_group_sam': action.child_group_sam,
                            'parent_group_sam': action.parent_group_sam,
                            'error': str(cleanup_error),
                        }
                    )
                    sync_stats['error_count'] += 1
                    record_operation(
                        stage_name='apply',
                        object_type='group_hierarchy',
                        operation_type='remove_group_from_group',
                        status='error',
                        message=f"failed to remove stale group relation {action.child_group_sam} -> {action.parent_group_sam}: {cleanup_error}",
                        source_id=action.child_group_sam,
                        department_id=str(action.child_department_id),
                        target_id=action.parent_group_sam,
                        target_dn=action.parent_group_dn,
                        risk_level='high',
                        details={'error': str(cleanup_error)},
                    )

        if stats_callback:
            stats_callback('disable_stage_start', True)
            stats_callback('users_to_disable', len(disable_actions))

        if disable_actions:
            log_dir = 'logs'
            os.makedirs(log_dir, exist_ok=True)
            disable_log_filename = os.path.join(
                log_dir,
                f"disabled_accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            )
            with open(disable_log_filename, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        'SamAccountName',
                        'DisplayName',
                        'Mail',
                        'Created',
                        'Modified',
                        'LastLogonDate',
                        'Description',
                        'ConnectorID',
                        'DisableTime',
                    ],
                )
                writer.writeheader()
                for index, action in enumerate(disable_actions, start=1):
                    if index % 5 == 0 and is_cancelled():
                        raise InterruptedError('sync cancelled by user')
                    try:
                        if action.source_user_id and has_exception_rule('skip_user_sync', action.source_user_id):
                            record_exception_skip(
                                stage_name='apply',
                                object_type='user',
                                operation_type='disable_user',
                                exception_rule_type='skip_user_sync',
                                match_value=action.source_user_id,
                                reason=f"skip disable for AD user {action.username}: matched exception rule skip_user_sync",
                                source_id=action.source_user_id,
                                target_id=action.username,
                                risk_level='high',
                                details={'source_user_id': action.source_user_id, 'ad_username': action.username},
                            )
                            continue
                        if action.source_user_id and has_exception_rule('skip_user_disable', action.source_user_id):
                            record_exception_skip(
                                stage_name='apply',
                                object_type='user',
                                operation_type='disable_user',
                                exception_rule_type='skip_user_disable',
                                match_value=action.source_user_id,
                                reason=f"skip disable for AD user {action.username}: matched exception rule skip_user_disable",
                                source_id=action.source_user_id,
                                target_id=action.username,
                                risk_level='high',
                                details={'source_user_id': action.source_user_id, 'ad_username': action.username},
                            )
                            continue
                        connector_ad_sync = get_ad_sync(action.connector_id)
                        user_details = connector_ad_sync.get_user_details(action.username)
                        if user_details:
                            user_details['DisableTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            user_details['ConnectorID'] = action.connector_id
                            writer.writerow(user_details)

                        if not connector_ad_sync.disable_user(action.username):
                            raise Exception('failed to disable user')

                        offboarding_repo.mark_disabled(
                            connector_id=action.connector_id,
                            ad_username=action.username,
                            last_job_id=job_id,
                        )
                        sync_stats['operations']['users_disabled'] += 1
                        record_operation(
                            stage_name='apply',
                            object_type='user',
                            operation_type='disable_user',
                            status='succeeded',
                            message=f"disabled AD user {action.username}",
                            source_id=action.source_user_id or action.username,
                            target_id=action.username,
                            risk_level='high',
                            reason_code=action.reason or 'missing_from_wecom',
                            details={
                                'source_user_id': action.source_user_id,
                                'connector_id': action.connector_id,
                                'employment_type': action.employment_type,
                                'sponsor_userid': action.sponsor_userid,
                                'effective_at': action.effective_at,
                            },
                        )
                        if action.reason == 'contractor_expired' and action.source_user_id:
                            lifecycle_repo.mark_completed_for_source_user(
                                lifecycle_type='contractor_expiry',
                                connector_id=action.connector_id,
                                source_user_id=action.source_user_id,
                                last_job_id=job_id,
                            )
                        executed_count += 1
                        sync_stats['executed_operation_count'] = executed_count
                        if stats_callback and (len(disable_actions) < 50 or index % 5 == 0 or index == 1):
                            stats_callback('user_disable_progress', index / max(len(disable_actions), 1))
                    except Exception as disable_error:
                        sync_stats['errors']['user_disable_errors'].append(
                            {
                                'username': action.username,
                                'userid': action.source_user_id,
                                'error': str(disable_error),
                            }
                        )
                        sync_stats['error_count'] += 1
                        record_operation(
                            stage_name='apply',
                            object_type='user',
                            operation_type='disable_user',
                            status='error',
                            message=f"failed to disable AD user {action.username}: {disable_error}",
                            source_id=action.source_user_id or action.username,
                            target_id=action.username,
                            risk_level='high',
                            reason_code=action.reason or 'missing_from_wecom',
                            details={
                                'error': str(disable_error),
                                'source_user_id': action.source_user_id,
                                'employment_type': action.employment_type,
                                'sponsor_userid': action.sponsor_userid,
                                'effective_at': action.effective_at,
                            },
                        )

                sync_stats['disabled_users'] = [f"{action.connector_id}:{action.username}" for action in disable_actions]

        state_manager.cleanup_old_users(wecom_users)
        state_manager.set_sync_complete(sync_stats['error_count'] == 0)

        duration = format_time_duration(time.time() - start_time)
        if stats_callback:
            stats_callback('sync_duration', duration)

        if bot:
            result_line = 'SUCCESS' if sync_stats['error_count'] == 0 else f"COMPLETED WITH {sync_stats['error_count']} ERRORS"
            bot.send_message(
                f'## {source_provider_name}-AD sync finished (LDAPS)\n\n'
                f"> Finish time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"> Duration: {duration}\n"
                f"> Result: {result_line}\n"
                f"> Planned operations: {planned_count}\n"
                f"> Executed operations: {executed_count}\n"
                f"> Conflicts: {sync_stats['conflict_count']}\n"
                f"> High-risk operations: {high_risk_operation_count}\n"
                f"> Users created/updated/disabled: {sync_stats['operations']['users_created']}/{sync_stats['operations']['users_updated']}/{sync_stats['operations']['users_disabled']}"
            )

        sync_stats['skip_detail_report'] = _generate_skip_detail_report(sync_stats)
        _generate_sync_operation_log(sync_stats, start_time, config)
        current_wecom_ad_usernames = sorted(
            {
                f"{connector_id}:{username}"
                for connector_id, usernames in current_wecom_ad_usernames_by_connector.items()
                for username in usernames
            }
        )
        managed_missing_ad_usernames = sorted(
            {
                f"{connector_id}:{username}"
                for connector_id, connector_enabled_users in enabled_ad_users_by_connector.items()
                for username in connector_enabled_users
                if (connector_id, username) in managed_ad_identities
                and username not in current_wecom_ad_usernames_by_connector.get(connector_id, set())
            }
        )
        _generate_sync_validation_report(
            sync_stats,
            current_wecom_ad_usernames,
            managed_missing_ad_usernames,
        )

        summary = {
            'org_id': organization.org_id,
            'organization_name': organization.name,
            'mode': execution_mode,
            'planned_operation_count': planned_count,
            'executed_operation_count': executed_count,
            'error_count': sync_stats['error_count'],
            'duration': duration,
            'conflict_count': sync_stats['conflict_count'],
            'high_risk_operation_count': high_risk_operation_count,
            'review_required': False,
            'approved_review_job_id': approved_review.job_id if approved_review else '',
            'plan_fingerprint': plan_fingerprint,
            'field_ownership_policy': dict(FIELD_OWNERSHIP_POLICY),
            'skipped_operation_count': sync_stats['skipped_operations']['total'],
            'skipped_by_action': dict(sync_stats['skipped_operations']['by_action']),
            'automatic_replay_request_count': len(started_replay_requests),
            'automatic_replay_request_ids': [
                int(request.id) for request in started_replay_requests if request.id is not None
            ],
        }
        try:
            summary['history_cleanup'] = run_history_cleanup()
        except Exception as cleanup_error:
            logger.warning("history cleanup failed: %s", cleanup_error)
            record_event(
                'WARNING',
                'history_cleanup_failed',
                f"failed to prune old history: {cleanup_error}",
                stage_name='finalize',
                payload={'error': str(cleanup_error)},
            )
            summary['history_cleanup'] = {'error': str(cleanup_error)}
        if started_replay_requests:
            replay_result_summary = {
                'job_id': job_id,
                'org_id': organization.org_id,
                'mode': execution_mode,
                'status': 'completed' if sync_stats['error_count'] == 0 else 'completed_with_errors',
                'planned_operation_count': planned_count,
                'executed_operation_count': executed_count,
                'error_count': sync_stats['error_count'],
                'conflict_count': sync_stats['conflict_count'],
            }
            for replay_request in started_replay_requests:
                replay_request_repo.mark_finished(
                    int(replay_request.id),
                    status='completed',
                    last_job_id=job_id,
                    result_summary=replay_result_summary,
                )
        sync_stats['summary'] = summary
        sync_stats['job_summary'] = SyncJobSummary.from_sync_stats(sync_stats).to_dict()
        mark_job(
            'COMPLETED' if sync_stats['error_count'] == 0 else 'COMPLETED_WITH_ERRORS',
            ended=True,
            summary=summary,
        )
        return sync_stats.to_dict()

    except InterruptedError as interrupted_error:
        sync_stats['error_count'] += 1
        for replay_request in started_replay_requests:
            replay_request_repo.mark_finished(
                int(replay_request.id),
                status='canceled',
                last_job_id=job_id,
                result_summary={'mode': execution_mode, 'error': str(interrupted_error)},
            )
        mark_job('CANCELED', ended=True, summary={'mode': execution_mode, 'error': str(interrupted_error)})
        sync_stats['job_summary'] = SyncJobSummary.from_sync_stats(sync_stats).to_dict()
        if execution_mode == 'apply' and config.webhook_url:
            try:
                WeChatBot(config.webhook_url).send_message(
                    f'## {source_provider_name}-AD sync cancelled (LDAPS)\n\n'
                    f"> Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    '> Result: canceled by user'
                )
            except Exception:
                logger.error('failed to send cancel notification')
        return sync_stats.to_dict()

    except Exception as sync_error:
        sync_stats['error_count'] += 1
        for replay_request in started_replay_requests:
            replay_request_repo.mark_finished(
                int(replay_request.id),
                status='failed',
                last_job_id=job_id,
                result_summary={'mode': execution_mode, 'error': str(sync_error)},
            )
        mark_job('FAILED', ended=True, summary={'mode': execution_mode, 'error': str(sync_error)})
        sync_stats['job_summary'] = SyncJobSummary.from_sync_stats(sync_stats).to_dict()
        if execution_mode == 'apply' and config.webhook_url:
            try:
                WeChatBot(config.webhook_url).send_message(
                    f'## {source_provider_name}-AD sync failed (LDAPS)\n\n'
                    f"> Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    f"### Error\n{sync_error}"
                )
            except Exception:
                logger.error('failed to send error notification')
        logger.error(f"sync job failed: {sync_error}")
        raise
    finally:
        if source_provider is not None:
            source_provider.close()
