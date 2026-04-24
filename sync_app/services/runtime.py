import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from sync_app.core import logging_utils as sync_logging
from sync_app.core.common import APP_VERSION, generate_job_id
from sync_app.core.config import (
    load_sync_config,
    run_config_security_self_check,
    test_ldap_connection,
    test_source_connection,
    validate_config,
)
from sync_app.core.models import (
    DepartmentNode,
    SyncRunStats,
)
from sync_app.providers.source import build_source_provider, get_source_provider_display_name
from sync_app.providers.target import build_target_provider
from sync_app.services.external_integrations import OutboxWebhookNotificationClient as WebhookNotificationClient
from sync_app.services.external_integrations import emit_job_lifecycle_events
from sync_app.services.ad_sync import (
    ADSyncLDAPS,
    build_custom_group_sam,
)
from sync_app.services.notification_automation_center import (
    build_notification_automation_policy_settings,
    evaluate_scheduled_apply_readiness,
)
from sync_app.services.reports import (
    _generate_skip_detail_report,
    _generate_sync_operation_log,
    _generate_sync_validation_report,
)
from sync_app.services.runtime_bootstrap import bootstrap_sync_runtime
from sync_app.services.runtime_context import SyncContext, SyncRuntimeHooks
from sync_app.services.runtime_finalize import (
    finalize_failed_sync,
    finalize_interrupted_sync,
    finalize_successful_sync,
)
from sync_app.services.runtime_orchestrator import run_apply_phase, run_planning_phase
from sync_app.services.runtime_connectors import (
    build_department_connector_map,
    build_department_scope_root_map,
    load_connector_specs,
    trim_department_paths_to_scope,
)
from sync_app.services.runtime_identity import build_identity_candidates, resolve_target_department
from sync_app.services.runtime_plan import compute_plan_fingerprint
from sync_app.services.runtime_services import (
    build_execution_services,
    evaluate_group_policy as evaluate_group_policy_rule_set,
    has_exception_rule as has_exception_rule_match,
)
from sync_app.web.rule_governance import build_rule_governance_summary


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
build_custom_group_sam = build_custom_group_sam


def _build_notification_client(
    ctx: SyncContext,
    *,
    webhook_url: str,
    source: str,
):
    try:
        return WebhookNotificationClient(
            db_manager=ctx.db_manager,
            org_id=ctx.organization.org_id,
            webhook_url=webhook_url,
            source=source,
            dispatch_inline=False,
            dispatch_async=True,
        )
    except TypeError:
        # Tests may patch the client with a simpler fake that only accepts webhook_url.
        return WebhookNotificationClient(webhook_url)


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
        ctx.environment.bot = _build_notification_client(
            ctx,
            webhook_url=config.webhook_url,
            source="sync.apply",
        )

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
    department_ou_mappings_by_connector: Dict[str, list[Any]] = {}
    for record in policy_settings.enabled_department_ou_mappings:
        department_ou_mappings_by_connector.setdefault(
            str(record.connector_id or "").strip(),
            [],
        ).append(record)
    ctx.environment.department_ou_mappings_by_connector = department_ou_mappings_by_connector


def _notify_sync_cancelled(ctx: SyncContext, interrupted_error: InterruptedError) -> None:
    if ctx.execution_mode == 'apply' and ctx.config.webhook_url:
        try:
            _build_notification_client(
                ctx,
                webhook_url=ctx.config.webhook_url,
                source="sync.cancelled",
            ).send_message(
                f'## {ctx.environment.source_provider_name} to AD sync cancelled (LDAPS)\n\n'
                f"> Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                '> Result: canceled by user'
            )
        except Exception:
            ctx.logger.error('failed to send cancel notification')


def _send_webhook_notification(ctx: SyncContext, message: str) -> None:
    if not ctx.config.webhook_url:
        return
    try:
        _build_notification_client(
            ctx,
            webhook_url=ctx.config.webhook_url,
            source="sync.operations",
        ).send_message(message)
    except Exception:
        ctx.logger.error('failed to send webhook notification')


def _notify_post_dry_run_digest(ctx: SyncContext, dry_run_result: dict[str, Any]) -> None:
    if ctx.execution_mode != 'dry_run' or not ctx.config.webhook_url:
        return

    policy_settings = build_notification_automation_policy_settings(
        ctx.repositories.settings_repo,
        ctx.organization.org_id,
    )
    summary = dict(dry_run_result.get("summary") or {})
    open_conflicts_total = ctx.repositories.conflict_repo.list_conflict_records_page(
        limit=1,
        offset=0,
        status="open",
        org_id=ctx.organization.org_id,
    )[1]
    reminder_lines: list[str] = []

    if (
        policy_settings["notify_conflict_backlog_enabled"]
        and int(open_conflicts_total or 0) >= int(policy_settings["notify_conflict_backlog_threshold"] or 1)
    ):
        reminder_lines.append(
            f"- Conflict backlog reached {int(open_conflicts_total or 0)} open conflict(s) "
            f"(threshold: {int(policy_settings['notify_conflict_backlog_threshold'])})."
        )

    if policy_settings["notify_review_pending_enabled"] and bool(summary.get("review_required") or False):
        reminder_lines.append(
            f"- High-risk dry run {ctx.job_id} still needs approval before apply can continue."
        )

    if policy_settings["notify_rule_governance_enabled"]:
        governance = build_rule_governance_summary(
            bindings=ctx.repositories.user_binding_repo.list_binding_records(org_id=ctx.organization.org_id),
            overrides=ctx.repositories.department_override_repo.list_override_records(org_id=ctx.organization.org_id),
            exception_rules=ctx.repositories.exception_rule_repo.list_rule_records(org_id=ctx.organization.org_id),
        )
        governance_issue_count = (
            int(governance.get("expired_exception_count") or 0)
            + int(governance.get("expiring_exception_count") or 0)
            + int(governance.get("review_due_count") or 0)
        )
        if governance_issue_count > 0:
            reminder_lines.append(
                f"- Rule governance has {governance_issue_count} reminder(s): "
                f"{int(governance.get('expired_exception_count') or 0)} expired, "
                f"{int(governance.get('expiring_exception_count') or 0)} expiring, "
                f"{int(governance.get('review_due_count') or 0)} overdue for review."
            )

    if not reminder_lines:
        return

    scheduled_apply_readiness = evaluate_scheduled_apply_readiness(
        settings_repo=ctx.repositories.settings_repo,
        job_repo=ctx.repositories.job_repo,
        conflict_repo=ctx.repositories.conflict_repo,
        review_repo=ctx.repositories.review_repo,
        org_id=ctx.organization.org_id,
        policy_settings=policy_settings,
    )
    readiness_line = (
        f"> Scheduled apply gate: {scheduled_apply_readiness.get('summary') or '-'}"
        if scheduled_apply_readiness.get("mode") == "apply"
        else "> Scheduled apply gate: schedule mode is dry run"
    )
    if scheduled_apply_readiness.get("reasons"):
        readiness_line += f" ({'; '.join(str(item) for item in scheduled_apply_readiness['reasons'])})"

    _send_webhook_notification(
        ctx,
        (
            f"## {ctx.environment.source_provider_name}-AD dry-run reminders\n\n"
            f"> Job: {ctx.job_id}\n"
            f"> Finish time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"> Open conflicts: {int(open_conflicts_total or 0)}\n"
            f"> High-risk operations: {int(summary.get('high_risk_operation_count') or 0)}\n"
            f"{readiness_line}\n\n"
            "### Attention Required\n"
            + "\n".join(reminder_lines)
        ),
    )


def _notify_sync_failed(ctx: SyncContext, sync_error: Exception) -> None:
    should_notify = False
    title = f"{ctx.environment.source_provider_name} to AD sync failed (LDAPS)"
    if ctx.execution_mode == 'apply':
        should_notify = True
    elif ctx.execution_mode == 'dry_run':
        should_notify = ctx.repositories.settings_repo.get_bool(
            "ops_notify_dry_run_failure_enabled",
            False,
            org_id=ctx.organization.org_id,
        )
        title = f"{ctx.environment.source_provider_name} dry run failed"
    if should_notify and ctx.config.webhook_url:
        _send_webhook_notification(
            ctx,
            (
                f"## {title}\n\n"
                f"> Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                f"### Error\n{sync_error}"
            ),
        )


def _emit_external_job_events(ctx: SyncContext) -> None:
    try:
        emit_job_lifecycle_events(
            ctx.db_manager,
            job_id=ctx.job_id,
            dispatch_inline=False,
            dispatch_async=True,
        )
    except Exception as exc:
        ctx.logger.warning("failed to emit external integration events for job %s: %s", ctx.job_id, exc)

def run_sync_job(
    stats_callback=None,
    cancel_flag=None,
    execution_mode: str = 'apply',
    trigger_type: str = 'manual',
    db_path: Optional[str] = None,
    config_path: str = 'config.ini',
    org_id: str = 'default',
    job_id: Optional[str] = None,
    active_job_guard_id: Optional[str] = None,
    requested_by: str = '',
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
        active_job_guard_id=active_job_guard_id or job_id,
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
    job_id = str(job_id or generate_job_id()).strip() or generate_job_id()
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

    existing_job_record = job_repo.get_job_record(job_id)
    if existing_job_record:
        job_repo.update_job(
            job_id,
            status='CREATED',
            trigger_type=trigger_type,
            execution_mode=execution_mode,
            app_version=APP_VERSION,
            config_snapshot_hash=config_hash,
            requested_by=requested_by or existing_job_record.requested_by,
            requested_config_path=resolved_config_path,
            clear_summary=True,
        )
    else:
        job_repo.create_job(
            job_id=job_id,
            org_id=organization.org_id,
            trigger_type=trigger_type,
            execution_mode=execution_mode,
            status='CREATED',
            requested_by=requested_by,
            requested_config_path=resolved_config_path,
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
            clear_lease=ended,
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
    ):
        return evaluate_group_policy_rule_set(
            enabled_group_rules=enabled_group_rules,
            group_sam=group_sam,
            group_dn=group_dn,
            display_name=display_name,
        )

    def has_exception_rule(rule_type: str, match_value: Optional[str]) -> bool:
        return has_exception_rule_match(
            exception_match_values_by_rule_type=exception_match_values_by_rule_type,
            rule_type=rule_type,
            match_value=match_value,
        )

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
    try:
        mark_job('PLANNING')
        _run_automatic_replay_stage(ctx)
        _prepare_sync_environment(ctx)
        services = build_execution_services(
            ctx,
            enabled_group_rules=enabled_group_rules,
            exception_match_values_by_rule_type=exception_match_values_by_rule_type,
            display_separator=display_separator,
        )
        early_response, planned_hierarchy_pairs = run_planning_phase(
            ctx,
            services=services,
            field_ownership_policy=FIELD_OWNERSHIP_POLICY,
            display_separator=display_separator,
        )
        if early_response is not None:
            _emit_external_job_events(ctx)
            if execution_mode == 'dry_run' and isinstance(early_response, dict):
                _notify_post_dry_run_digest(ctx, early_response)
            return early_response

        mark_job('RUNNING')
        run_apply_phase(
            ctx,
            services=services,
            field_ownership_policy=FIELD_OWNERSHIP_POLICY,
            display_separator=display_separator,
            planned_hierarchy_pairs=planned_hierarchy_pairs,
        )
        successful_result = finalize_successful_sync(ctx)
        _emit_external_job_events(ctx)
        return successful_result

    except InterruptedError as interrupted_error:
        canceled_result = finalize_interrupted_sync(ctx, interrupted_error)
        _notify_sync_cancelled(ctx, interrupted_error)
        return canceled_result

    except Exception as sync_error:
        finalize_failed_sync(ctx, sync_error)
        _notify_sync_failed(ctx, sync_error)
        _emit_external_job_events(ctx)
        logger.error(f"sync job failed: {sync_error}")
        raise
    finally:
        if ctx.environment.source_provider is not None:
            ctx.environment.source_provider.close()
