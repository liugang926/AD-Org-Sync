import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from sync_app.clients.wechat_bot import WebhookNotificationClient
from sync_app.core import logging_utils as sync_logging
from sync_app.core.common import APP_VERSION, generate_job_id
from sync_app.core.config import (
    load_sync_config,
    run_config_security_self_check,
    test_ldap_connection,
    test_source_connection,
    validate_config,
)
from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.core.exception_rules import (
    get_exception_rule_match_type,
    normalize_exception_match_value,
)
from sync_app.core.sync_policies import (
    normalize_mapping_direction,
    render_template,
)
from sync_app.core.models import (
    DepartmentNode,
    GroupPolicyEvaluation,
    ManagedGroupTarget,
    SyncRunStats,
    UserDepartmentBundle,
)
from sync_app.providers.source import build_source_provider, get_source_provider_display_name
from sync_app.providers.target import TargetDirectoryProvider, build_target_provider
from sync_app.services.ad_sync import (
    ADSyncLDAPS,
    build_custom_group_sam,
)
from sync_app.services.reports import (
    _generate_skip_detail_report,
    _generate_sync_operation_log,
    _generate_sync_validation_report,
)
from sync_app.services.runtime_bootstrap import bootstrap_sync_runtime
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
from sync_app.services.runtime_context import SyncContext, SyncRuntimeHooks
from sync_app.services.runtime_finalize import (
    finalize_failed_sync,
    finalize_interrupted_sync,
    finalize_successful_sync,
)
from sync_app.services.runtime_group_phase import (
    get_department_group_target as resolve_department_group_target,
    get_effective_parent_department_id as resolve_effective_parent_department_id,
    plan_directory_and_custom_groups,
    plan_group_relationship_cleanup,
)
from sync_app.services.runtime_connectors import (
    build_department_connector_map,
    build_department_scope_root_map,
    is_department_in_connector_scope,
    load_connector_specs,
    sanitize_source_writeback_payload,
    select_mapping_rules,
    trim_department_paths_to_scope,
)
from sync_app.services.runtime_identity import build_identity_candidates, resolve_target_department
from sync_app.services.runtime_plan import complete_plan_phase, compute_plan_fingerprint
from sync_app.services.runtime_source_phase import (
    collect_source_user_departments,
    resolve_identity_bindings_phase,
)
from sync_app.services.runtime_user_phase import (
    evaluate_disable_circuit_breaker,
    plan_disable_actions,
    plan_user_actions,
)
from sync_app.storage.local_db import SyncConnectorRepository


FIELD_OWNERSHIP_POLICY = {
    'display_name': 'source_authoritative',
    'email': 'initialize_if_missing_then_preserve_ad',
    'user_principal_name': 'sync_managed',
    'account_status': 'managed_scope_follows_source_membership',
    'ou_placement': 'department_override_then_source_strategy',
    'group_membership': 'managed_department_groups_only',
    'custom_attribute_mapping': 'connector_specific_source_to_ad_rules',
    'write_back': 'configured_ad_to_source_rules_after_apply',
    'connector_routing': 'top_level_department_connector_assignment',
    'offboarding': 'source_departure_enters_grace_period_before_disable',
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
_compute_plan_fingerprint = compute_plan_fingerprint
_build_identity_candidates = build_identity_candidates
_resolve_target_department = resolve_target_department
_load_connector_specs = load_connector_specs
_build_department_connector_map = build_department_connector_map
_build_department_scope_root_map = build_department_scope_root_map
_select_mapping_rules = select_mapping_rules
_sanitize_source_writeback_payload = sanitize_source_writeback_payload


def _run_automatic_replay_stage(ctx: SyncContext) -> None:
    replay_request_repo = ctx.repositories.replay_request_repo
    if ctx.policy_settings.automatic_replay_enabled:
        for replay_request in replay_request_repo.list_request_records(status='pending', limit=100):
            request_execution_mode = str(replay_request.execution_mode or '').strip().lower()
            if request_execution_mode and request_execution_mode != ctx.execution_mode:
                continue
            replay_request_repo.mark_started(int(replay_request.id))
            ctx.plan.started_replay_requests.append(replay_request)
        if ctx.plan.started_replay_requests:
            ctx.hooks.record_event(
                'INFO',
                'automatic_replay_started',
                f"picked up {len(ctx.plan.started_replay_requests)} pending replay requests",
                stage_name='plan',
                payload={
                    'request_ids': [
                        int(request.id)
                        for request in ctx.plan.started_replay_requests
                        if request.id is not None
                    ],
                    'execution_mode': ctx.execution_mode,
                },
            )
    ctx.sync_stats['automatic_replay_request_count'] = len(ctx.plan.started_replay_requests)
    ctx.sync_stats['automatic_replay_request_ids'] = [
        int(request.id) for request in ctx.plan.started_replay_requests if request.id is not None
    ]


def _prepare_sync_environment(ctx: SyncContext) -> None:
    logger = ctx.logger
    config = ctx.config
    organization = ctx.organization
    policy_settings = ctx.policy_settings
    connector_repo = ctx.repositories.connector_repo
    source_provider_name = get_source_provider_display_name(getattr(config, 'source_provider', 'wecom'))
    ctx.environment.source_provider_name = source_provider_name

    is_valid, validation_errors = validate_config(config)
    if not is_valid:
        raise ValueError("config validation failed:\n" + "\n".join([f"  - {err}" for err in validation_errors]))

    source_success, source_message = test_source_connection(
        config.source_connector.corpid,
        config.source_connector.corpsecret,
        config.source_connector.agentid,
        source_provider=getattr(config, 'source_provider', 'wecom'),
    )
    if not source_success:
        raise ConnectionError(f"{source_provider_name} connection test failed: {source_message}")

    connector_specs = load_connector_specs(
        config,
        connector_repo,
        connectors_enabled=policy_settings.connector_routing_enabled,
        org_id=organization.org_id,
        default_root_department_ids=policy_settings.source_root_unit_ids,
        default_disabled_users_ou=policy_settings.default_disabled_users_ou_path,
        default_custom_group_ou_path=policy_settings.global_custom_group_ou_path,
        default_user_root_ou_path=policy_settings.default_directory_root_ou_path,
        load_sync_config_fn=load_sync_config,
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
            ctx.hooks.record_event("WARNING", "security_self_check", warning, stage_name="config")
            logger.warning("security self-check warning: %s", warning)

    if ctx.execution_mode == 'apply' and config.webhook_url:
        ctx.environment.bot = WebhookNotificationClient(config.webhook_url)

    ctx.environment.source_provider = build_source_provider(
        app_config=config,
        logger=logger,
    )
    for connector_spec in connector_specs:
        connector_config = connector_spec['config']
        ctx.environment.ad_sync_clients[connector_spec['connector_id']] = build_target_provider(
            client_factory=ADSyncLDAPS,
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
            disabled_users_ou_name=connector_spec.get('disabled_users_ou') or policy_settings.default_disabled_users_ou_path,
            managed_group_type=connector_spec.get('group_type') or policy_settings.global_group_type,
            managed_group_mail_domain=connector_spec.get('group_mail_domain') or policy_settings.global_group_mail_domain,
            custom_group_ou_path=connector_spec.get('custom_group_ou_path') or policy_settings.global_custom_group_ou_path,
            user_root_ou_path=connector_spec.get('user_root_ou_path') or policy_settings.default_directory_root_ou_path,
        )
    ctx.environment.connector_specs = connector_specs
    ctx.environment.connector_specs_by_id = {
        spec['connector_id']: spec for spec in connector_specs
    }
    ctx.environment.protected_ad_accounts_by_connector = {
        connector_spec['connector_id']: {
            str(account).strip().lower()
            for account in connector_spec['config'].exclude_accounts
            if str(account).strip()
        }
        for connector_spec in connector_specs
    }

    departments = ctx.environment.source_provider.list_departments()
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

    ctx.environment.departments = departments
    ctx.environment.dept_tree = dept_tree
    ctx.environment.department_connector_map = build_department_connector_map(dept_tree, connector_specs)
    ctx.environment.department_scope_root_map = build_department_scope_root_map(
        dept_tree,
        connector_specs,
        ctx.environment.department_connector_map,
    )
    trim_department_paths_to_scope(dept_tree, ctx.environment.department_scope_root_map)
    ctx.environment.excluded_department_names = set(config.exclude_departments)
    ctx.environment.placement_blocked_department_ids = {
        int(rule.match_value)
        for rule in policy_settings.enabled_exception_rules
        if rule.rule_type == 'skip_department_placement'
        and str(rule.match_value).strip().isdigit()
    }


def _notify_sync_cancelled(ctx: SyncContext, interrupted_error: InterruptedError) -> None:
    if ctx.execution_mode == 'apply' and ctx.config.webhook_url:
        try:
            WebhookNotificationClient(ctx.config.webhook_url).send_message(
                f'## {ctx.environment.source_provider_name} to AD sync cancelled (LDAPS)\n\n'
                f"> Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                '> Result: canceled by user'
            )
        except Exception:
            ctx.logger.error('failed to send cancel notification')


def _notify_sync_failed(ctx: SyncContext, sync_error: Exception) -> None:
    if ctx.execution_mode == 'apply' and ctx.config.webhook_url:
        try:
            WebhookNotificationClient(ctx.config.webhook_url).send_message(
                f'## {ctx.environment.source_provider_name} to AD sync failed (LDAPS)\n\n'
                f"> Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                f"### Error\n{sync_error}"
            )
        except Exception:
            ctx.logger.error('failed to send error notification')

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

    bootstrap = bootstrap_sync_runtime(
        config_path=config_path,
        db_path=db_path,
        org_id=org_id,
        load_sync_config_fn=load_sync_config,
    )
    logger = bootstrap.logger
    sync_stats['log_file'] = sync_logging.log_filename

    db_manager = bootstrap.db_manager
    db_init_result = bootstrap.db_init_result
    sync_stats['db_path'] = db_manager.db_path
    sync_stats['db_backup_dir'] = db_manager.backup_dir
    sync_stats['db_startup_snapshot_path'] = db_init_result.get('startup_snapshot_path') or ''
    sync_stats['db_migration_source_path'] = db_init_result.get('migration_source_path') or ''
    sync_stats['db_integrity_check'] = db_init_result.get('integrity_check') or {}

    repositories = bootstrap.repositories
    policy_settings = bootstrap.policy_settings
    organization = bootstrap.organization
    config = bootstrap.config
    config_hash = bootstrap.config_hash
    resolved_config_path = organization.config_path or config_path
    settings_repo = repositories.settings_repo
    job_repo = repositories.job_repo
    event_repo = repositories.event_repo
    plan_repo = repositories.plan_repo
    operation_log_repo = repositories.operation_log_repo
    conflict_repo = repositories.conflict_repo
    review_repo = repositories.review_repo
    replay_request_repo = repositories.replay_request_repo
    exception_rule_repo = repositories.exception_rule_repo
    enabled_group_rules = policy_settings.enabled_group_rules
    enabled_exception_rules = policy_settings.enabled_exception_rules
    exception_match_values_by_rule_type = policy_settings.exception_match_values_by_rule_type
    connector_routing_enabled = policy_settings.connector_routing_enabled
    offboarding_lifecycle_enabled = policy_settings.offboarding_lifecycle_enabled
    field_conflict_queue_enabled = policy_settings.field_conflict_queue_enabled
    rehire_restore_enabled = policy_settings.rehire_restore_enabled
    custom_group_archive_enabled = policy_settings.custom_group_archive_enabled
    scheduled_review_execution_enabled = policy_settings.scheduled_review_execution_enabled
    group_recursive_enabled = policy_settings.group_recursive_enabled
    managed_relation_cleanup_enabled = policy_settings.managed_relation_cleanup_enabled
    user_ou_placement_strategy = policy_settings.user_ou_placement_strategy
    disable_breaker_requires_approval = policy_settings.disable_breaker_requires_approval
    display_separator = policy_settings.display_separator
    sync_stats['org_id'] = organization.org_id
    sync_stats['organization_name'] = organization.name
    sync_stats['organization_config_path'] = config.config_path
    job_id = generate_job_id()
    sync_stats['job_id'] = job_id
    ctx = SyncContext(
        start_time=start_time,
        execution_mode=execution_mode,
        trigger_type=trigger_type,
        db_path=db_path,
        config_path=config_path,
        org_id=org_id,
        bootstrap=bootstrap,
        sync_stats=sync_stats,
        job_id=job_id,
        hooks=None,
    )

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
        ctx.planned_count += 1
        sync_stats['planned_operation_count'] = ctx.planned_count
        if risk_level == 'high' or operation_type in HIGH_RISK_OPERATION_TYPES:
            ctx.high_risk_operation_count += 1
            sync_stats['high_risk_operation_count'] = ctx.high_risk_operation_count
        ctx.plan.plan_fingerprint_items.append(
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
            planned_operation_count=ctx.planned_count,
            executed_operation_count=ctx.executed_count,
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

    def has_exception_rule(rule_type: str, match_value: Optional[str]) -> bool:
        normalized_rule_type = str(rule_type or '').strip().lower()
        normalized_match_type = get_exception_rule_match_type(normalized_rule_type)
        normalized_match_value = normalize_exception_match_value(normalized_match_type, match_value)
        if not normalized_rule_type or not normalized_match_type or not normalized_match_value:
            return False
        return normalized_match_value in exception_match_values_by_rule_type.get(normalized_rule_type, set())

    ctx.hooks = SyncRuntimeHooks(
        record_event=record_event,
        record_operation=record_operation,
        record_conflict=record_conflict,
        add_planned_operation=add_planned_operation,
        mark_job=mark_job,
        run_history_cleanup=run_history_cleanup,
        evaluate_group_policy=evaluate_group_policy,
        has_exception_rule=has_exception_rule,
        generate_skip_detail_report=_generate_skip_detail_report,
        generate_sync_operation_log=_generate_sync_operation_log,
        generate_sync_validation_report=_generate_sync_validation_report,
        stats_callback=stats_callback,
        is_cancelled=is_cancelled,
    )

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
    department_actions = ctx.actions.department_actions
    custom_group_actions = ctx.actions.custom_group_actions
    user_actions = ctx.actions.user_actions
    membership_actions = ctx.actions.membership_actions
    group_hierarchy_actions = ctx.actions.group_hierarchy_actions
    group_cleanup_actions = ctx.actions.group_cleanup_actions
    disable_actions = ctx.actions.disable_actions

    try:
        mark_job('PLANNING')
        _run_automatic_replay_stage(ctx)
        _prepare_sync_environment(ctx)
        bot = ctx.environment.bot
        source_provider = ctx.environment.source_provider
        source_provider_name = ctx.environment.source_provider_name
        connector_specs = ctx.environment.connector_specs
        ad_sync_clients = ctx.environment.ad_sync_clients
        protected_ad_accounts_by_connector = ctx.environment.protected_ad_accounts_by_connector
        default_ad_sync = ad_sync_clients['default']
        dept_tree = ctx.environment.dept_tree
        department_connector_map = ctx.environment.department_connector_map
        connector_specs_by_id = ctx.environment.connector_specs_by_id
        department_scope_root_map = ctx.environment.department_scope_root_map
        excluded_department_names = ctx.environment.excluded_department_names
        department_group_targets = ctx.environment.department_group_targets
        policy_skip_markers = ctx.environment.policy_skip_markers
        placement_blocked_department_ids = ctx.environment.placement_blocked_department_ids

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
                return 'default'
            return department_connector_map.get(dept_info.department_id, 'default')

        def get_connector_spec(connector_id: str) -> dict[str, Any]:
            return connector_specs_by_id.get(connector_id, connector_specs_by_id['default'])

        def get_ad_sync(connector_id: str) -> TargetDirectoryProvider:
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

        collect_source_user_departments(ctx)
        resolve_identity_bindings_phase(
            ctx,
            get_connector_id_for_department=get_connector_id_for_department,
            get_connector_spec=get_connector_spec,
            get_ad_sync=get_ad_sync,
            is_protected_ad_account=is_protected_ad_account,
            record_exception_skip=record_exception_skip,
            record_protected_account_skip=record_protected_account_skip,
        )
        existing_users_map_by_connector = ctx.identity.existing_users_map_by_connector

        department_actions.clear()
        custom_group_actions.clear()
        user_actions.clear()
        membership_actions.clear()
        group_hierarchy_actions.clear()
        group_cleanup_actions.clear()
        disable_actions.clear()
        planned_memberships = plan_directory_and_custom_groups(
            ctx,
            is_department_excluded=is_department_excluded,
            get_connector_id_for_department=get_connector_id_for_department,
            get_ad_sync=get_ad_sync,
            record_group_policy_skip=record_group_policy_skip,
            display_separator=display_separator,
        )
        plan_user_actions(
            ctx,
            planned_memberships=planned_memberships,
            is_department_excluded=is_department_excluded,
            is_department_blocked_for_placement=is_department_blocked_for_placement,
            get_connector_id_for_department=get_connector_id_for_department,
            get_connector_spec=get_connector_spec,
            get_ad_sync=get_ad_sync,
            get_department_group_target=get_department_group_target,
            is_protected_ad_account=is_protected_ad_account,
            record_exception_skip=record_exception_skip,
            record_protected_account_skip=record_protected_account_skip,
            record_group_policy_skip=record_group_policy_skip,
            field_ownership_policy=FIELD_OWNERSHIP_POLICY,
        )

        planned_hierarchy_pairs = plan_group_relationship_cleanup(
            ctx,
            is_department_excluded=is_department_excluded,
            get_connector_id_for_department=get_connector_id_for_department,
            get_ad_sync=get_ad_sync,
            record_group_policy_skip=record_group_policy_skip,
            record_skip_detail=record_skip_detail,
            record_exception_skip=record_exception_skip,
            display_separator=display_separator,
        )
        plan_disable_actions(
            ctx,
            is_protected_ad_account=is_protected_ad_account,
            record_exception_skip=record_exception_skip,
            record_protected_account_skip=record_protected_account_skip,
        )
        evaluate_disable_circuit_breaker(ctx)
        early_response = complete_plan_phase(ctx)
        if early_response is not None:
            return early_response

        mark_job('RUNNING')
        if bot:
            bot.send_message(
                f"## {source_provider_name}-AD sync started (LDAPS)\n\n"
                f"> Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"> Domain: {config.domain}\n"
                f"> LDAP server: {config.ldap.server}\n"
                f"> SSL: {'yes' if config.ldap.use_ssl else 'no'}"
            )

        apply_department_actions(
            ctx,
            get_ad_sync=get_ad_sync,
            display_separator=display_separator,
            record_group_policy_skip=record_group_policy_skip,
        )

        successful_hierarchy_pairs = apply_group_hierarchy_actions(
            ctx,
            get_ad_sync=get_ad_sync,
            record_group_policy_skip=record_group_policy_skip,
        )

        apply_user_actions(
            ctx,
            get_ad_sync=get_ad_sync,
            field_ownership_policy=FIELD_OWNERSHIP_POLICY,
        )

        apply_custom_group_actions(
            ctx,
            get_ad_sync=get_ad_sync,
        )

        apply_group_membership_actions(
            ctx,
            get_ad_sync=get_ad_sync,
            record_exception_skip=record_exception_skip,
            record_group_policy_skip=record_group_policy_skip,
        )

        apply_group_cleanup_actions(
            ctx,
            get_ad_sync=get_ad_sync,
            planned_hierarchy_pairs=planned_hierarchy_pairs,
            successful_hierarchy_pairs=successful_hierarchy_pairs,
            record_exception_skip=record_exception_skip,
            record_group_policy_skip=record_group_policy_skip,
            record_skip_detail=record_skip_detail,
        )

        apply_disable_actions(
            ctx,
            get_ad_sync=get_ad_sync,
            record_exception_skip=record_exception_skip,
        )

        apply_final_state_updates(ctx)
        return finalize_successful_sync(ctx)

    except InterruptedError as interrupted_error:
        canceled_result = finalize_interrupted_sync(ctx, interrupted_error)
        _notify_sync_cancelled(ctx, interrupted_error)
        return canceled_result

    except Exception as sync_error:
        finalize_failed_sync(ctx, sync_error)
        _notify_sync_failed(ctx, sync_error)
        logger.error(f"sync job failed: {sync_error}")
        raise
    finally:
        if ctx.environment.source_provider is not None:
            ctx.environment.source_provider.close()
