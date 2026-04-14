from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Optional

from sync_app.core import logging_utils as sync_logging
from sync_app.core.config import load_sync_config
from sync_app.core.models import AppConfig, OrganizationRecord
from sync_app.core.sync_policies import normalize_group_type
from sync_app.services.state import SyncStateManager
from sync_app.storage.local_db import (
    AttributeMappingRuleRepository,
    CustomManagedGroupBindingRepository,
    DatabaseManager,
    DepartmentOuMappingRepository,
    GroupExclusionRuleRepository,
    ManagedGroupBindingRepository,
    ObjectStateRepository,
    OffboardingQueueRepository,
    OrganizationConfigRepository,
    OrganizationRepository,
    PlannedOperationRepository,
    SettingsRepository,
    SyncConnectorRepository,
    SyncConflictRepository,
    SyncEventRepository,
    SyncExceptionRuleRepository,
    SyncJobRepository,
    SyncOperationLogRepository,
    SyncPlanReviewRepository,
    SyncReplayRequestRepository,
    UserDepartmentOverrideRepository,
    UserIdentityBindingRepository,
    UserLifecycleQueueRepository,
)


@dataclass(frozen=True)
class RuntimeRepositories:
    settings_repo: SettingsRepository
    organization_repo: OrganizationRepository
    organization_config_repo: OrganizationConfigRepository
    exclusion_repo: GroupExclusionRuleRepository
    connector_repo: SyncConnectorRepository
    mapping_rule_repo: AttributeMappingRuleRepository
    department_ou_mapping_repo: DepartmentOuMappingRepository
    job_repo: SyncJobRepository
    event_repo: SyncEventRepository
    plan_repo: PlannedOperationRepository
    operation_log_repo: SyncOperationLogRepository
    conflict_repo: SyncConflictRepository
    review_repo: SyncPlanReviewRepository
    binding_repo: ManagedGroupBindingRepository
    user_binding_repo: UserIdentityBindingRepository
    department_override_repo: UserDepartmentOverrideRepository
    custom_group_binding_repo: CustomManagedGroupBindingRepository
    offboarding_repo: OffboardingQueueRepository
    lifecycle_repo: UserLifecycleQueueRepository
    replay_request_repo: SyncReplayRequestRepository
    state_repo: ObjectStateRepository
    exception_rule_repo: SyncExceptionRuleRepository
    state_manager: SyncStateManager


@dataclass(frozen=True)
class RuntimePolicySettings:
    enabled_group_rules: list[Any]
    enabled_exception_rules: list[Any]
    exception_match_values_by_rule_type: dict[str, set[str]]
    enabled_mapping_rules: list[Any]
    enabled_department_ou_mappings: list[Any]
    connector_routing_enabled: bool
    attribute_mapping_enabled: bool
    write_back_enabled: bool
    custom_group_sync_enabled: bool
    offboarding_lifecycle_enabled: bool
    field_conflict_queue_enabled: bool
    rehire_restore_enabled: bool
    custom_group_archive_enabled: bool
    scheduled_review_execution_enabled: bool
    automatic_replay_enabled: bool
    future_onboarding_enabled: bool
    future_onboarding_start_field: str
    contractor_lifecycle_enabled: bool
    lifecycle_employment_type_field: str
    contractor_end_field: str
    lifecycle_sponsor_field: str
    contractor_type_values: set[str]
    group_recursive_enabled: bool
    managed_relation_cleanup_enabled: bool
    user_ou_placement_strategy: str
    source_root_unit_ids: list[int]
    default_directory_root_ou_path: str
    default_disabled_users_ou_path: str
    offboarding_grace_days: int
    offboarding_notify_managers: bool
    disable_breaker_enabled: bool
    disable_breaker_percent: float
    disable_breaker_min_count: int
    disable_breaker_requires_approval: bool
    global_group_type: str
    global_group_mail_domain: str
    global_custom_group_ou_path: str
    display_separator: str


@dataclass(frozen=True)
class SyncRuntimeBootstrap:
    logger: Any
    db_manager: DatabaseManager
    db_init_result: dict[str, Any]
    repositories: RuntimeRepositories
    organization: OrganizationRecord
    config: AppConfig
    policy_settings: RuntimePolicySettings
    config_hash: str


def _parse_root_unit_ids(raw_value: Any) -> list[int]:
    values: list[int] = []
    for item in str(raw_value or "").replace("\n", ",").split(","):
        candidate = item.strip()
        if candidate.isdigit():
            values.append(int(candidate))
    return values


def _normalize_ou_path(raw_value: Any, *, default: str = "") -> str:
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return default
    dn_segments = [
        part.split("=", 1)[1].strip()
        for part in raw_text.split(",")
        if "=" in part and part.strip().lower().startswith("ou=") and part.split("=", 1)[1].strip()
    ]
    if dn_segments:
        segments = list(reversed(dn_segments))
    else:
        segments = [
            segment.strip()
            for segment in raw_text.replace("\\", "/").split("/")
            if segment.strip()
        ]
    normalized = "/".join(segments)
    return normalized or default


def _build_policy_settings(
    *,
    settings_repo: SettingsRepository,
    exclusion_repo: GroupExclusionRuleRepository,
    exception_rule_repo: SyncExceptionRuleRepository,
    mapping_rule_repo: AttributeMappingRuleRepository,
    department_ou_mapping_repo: DepartmentOuMappingRepository,
    organization: OrganizationRecord,
) -> RuntimePolicySettings:
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
    enabled_mapping_rules = mapping_rule_repo.list_rule_records(enabled_only=True, org_id=organization.org_id)
    enabled_department_ou_mappings = department_ou_mapping_repo.list_mapping_records(
        enabled_only=True,
        org_id=organization.org_id,
    )

    offboarding_lifecycle_enabled = get_org_setting_bool("offboarding_lifecycle_enabled", False)
    offboarding_grace_days = max(get_org_setting_int("offboarding_grace_days", 0), 0)
    offboarding_notify_managers = get_org_setting_bool("offboarding_notify_managers", False)
    if not offboarding_lifecycle_enabled:
        offboarding_grace_days = 0
        offboarding_notify_managers = False

    exception_match_values_by_rule_type: dict[str, set[str]] = {}
    for rule in enabled_exception_rules:
        exception_match_values_by_rule_type.setdefault(rule.rule_type, set()).add(rule.match_value)

    contractor_type_values = {
        str(value).strip().lower()
        for value in str(get_org_setting_value("contractor_type_values", "contractor,intern,vendor,temp") or "").split(",")
        if str(value).strip()
    }

    return RuntimePolicySettings(
        enabled_group_rules=enabled_group_rules,
        enabled_exception_rules=enabled_exception_rules,
        exception_match_values_by_rule_type=exception_match_values_by_rule_type,
        enabled_mapping_rules=enabled_mapping_rules,
        enabled_department_ou_mappings=enabled_department_ou_mappings,
        connector_routing_enabled=get_org_setting_bool("advanced_connector_routing_enabled", False),
        attribute_mapping_enabled=get_org_setting_bool("attribute_mapping_enabled", False),
        write_back_enabled=get_org_setting_bool("write_back_enabled", False),
        custom_group_sync_enabled=get_org_setting_bool("custom_group_sync_enabled", False),
        offboarding_lifecycle_enabled=offboarding_lifecycle_enabled,
        field_conflict_queue_enabled=get_org_setting_bool("field_conflict_queue_enabled", False),
        rehire_restore_enabled=get_org_setting_bool("rehire_restore_enabled", False),
        custom_group_archive_enabled=get_org_setting_bool("custom_group_archive_enabled", False),
        scheduled_review_execution_enabled=get_org_setting_bool("scheduled_review_execution_enabled", False),
        automatic_replay_enabled=get_org_setting_bool("automatic_replay_enabled", False),
        future_onboarding_enabled=get_org_setting_bool("future_onboarding_enabled", False),
        future_onboarding_start_field=get_org_setting_value("future_onboarding_start_field", "hire_date") or "hire_date",
        contractor_lifecycle_enabled=get_org_setting_bool("contractor_lifecycle_enabled", False),
        lifecycle_employment_type_field=(
            get_org_setting_value("lifecycle_employment_type_field", "employment_type") or "employment_type"
        ),
        contractor_end_field=get_org_setting_value("contractor_end_field", "contract_end_date") or "contract_end_date",
        lifecycle_sponsor_field=get_org_setting_value("lifecycle_sponsor_field", "sponsor_userid") or "sponsor_userid",
        contractor_type_values=contractor_type_values,
        group_recursive_enabled=get_org_setting_bool("group_recursive_enabled", True),
        managed_relation_cleanup_enabled=get_org_setting_bool("managed_relation_cleanup_enabled", False),
        user_ou_placement_strategy=(
            get_org_setting_value("user_ou_placement_strategy", "source_primary_department")
            or "source_primary_department"
        ),
        source_root_unit_ids=_parse_root_unit_ids(get_org_setting_value("source_root_unit_ids", "")),
        default_directory_root_ou_path=_normalize_ou_path(
            get_org_setting_value("directory_root_ou_path", ""),
        ),
        default_disabled_users_ou_path=_normalize_ou_path(
            get_org_setting_value("disabled_users_ou_path", "Disabled Users"),
            default="Disabled Users",
        ),
        offboarding_grace_days=offboarding_grace_days,
        offboarding_notify_managers=offboarding_notify_managers,
        disable_breaker_enabled=get_org_setting_bool("disable_circuit_breaker_enabled", False),
        disable_breaker_percent=max(get_org_setting_float("disable_circuit_breaker_percent", 5.0), 0.0),
        disable_breaker_min_count=max(get_org_setting_int("disable_circuit_breaker_min_count", 10), 0),
        disable_breaker_requires_approval=get_org_setting_bool("disable_circuit_breaker_requires_approval", True),
        global_group_type=normalize_group_type(get_org_setting_value("managed_group_type", "security")),
        global_group_mail_domain=get_org_setting_value("managed_group_mail_domain", "") or "",
        global_custom_group_ou_path=get_org_setting_value("custom_group_ou_path", "Managed Groups") or "Managed Groups",
        display_separator=get_org_setting_value("group_display_separator", "-") or "-",
    )


def _build_config_hash(
    *,
    config: AppConfig,
    connector_repo: SyncConnectorRepository,
    enabled_mapping_rules: list[Any],
    enabled_department_ou_mappings: list[Any],
    organization: OrganizationRecord,
    policy_settings: RuntimePolicySettings,
) -> str:
    config_snapshot_payload = {
        "organization": organization.to_dict(),
        "primary_config": config.to_hash_payload(),
        "connectors": [
            record.to_dict()
            for record in connector_repo.list_connector_records(enabled_only=True, org_id=organization.org_id)
        ],
        "attribute_mappings": [record.to_dict() for record in enabled_mapping_rules],
        "department_ou_mappings": [record.to_dict() for record in enabled_department_ou_mappings],
        "settings": {
            "advanced_connector_routing_enabled": policy_settings.connector_routing_enabled,
            "attribute_mapping_enabled": policy_settings.attribute_mapping_enabled,
            "write_back_enabled": policy_settings.write_back_enabled,
            "custom_group_sync_enabled": policy_settings.custom_group_sync_enabled,
            "offboarding_lifecycle_enabled": policy_settings.offboarding_lifecycle_enabled,
            "field_conflict_queue_enabled": policy_settings.field_conflict_queue_enabled,
            "rehire_restore_enabled": policy_settings.rehire_restore_enabled,
            "custom_group_archive_enabled": policy_settings.custom_group_archive_enabled,
            "scheduled_review_execution_enabled": policy_settings.scheduled_review_execution_enabled,
            "automatic_replay_enabled": policy_settings.automatic_replay_enabled,
            "future_onboarding_enabled": policy_settings.future_onboarding_enabled,
            "future_onboarding_start_field": policy_settings.future_onboarding_start_field,
            "contractor_lifecycle_enabled": policy_settings.contractor_lifecycle_enabled,
            "lifecycle_employment_type_field": policy_settings.lifecycle_employment_type_field,
            "contractor_end_field": policy_settings.contractor_end_field,
            "lifecycle_sponsor_field": policy_settings.lifecycle_sponsor_field,
            "contractor_type_values": sorted(policy_settings.contractor_type_values),
            "group_recursive_enabled": policy_settings.group_recursive_enabled,
            "managed_relation_cleanup_enabled": policy_settings.managed_relation_cleanup_enabled,
            "user_ou_placement_strategy": policy_settings.user_ou_placement_strategy,
            "source_root_unit_ids": policy_settings.source_root_unit_ids,
            "directory_root_ou_path": policy_settings.default_directory_root_ou_path,
            "disabled_users_ou_path": policy_settings.default_disabled_users_ou_path,
            "offboarding_grace_days": policy_settings.offboarding_grace_days,
            "disable_circuit_breaker_percent": policy_settings.disable_breaker_percent,
            "disable_circuit_breaker_min_count": policy_settings.disable_breaker_min_count,
            "managed_group_type": policy_settings.global_group_type,
            "managed_group_mail_domain": policy_settings.global_group_mail_domain,
            "custom_group_ou_path": policy_settings.global_custom_group_ou_path,
        },
    }
    return hashlib.md5(json.dumps(config_snapshot_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def bootstrap_sync_runtime(
    *,
    config_path: str = "config.ini",
    db_path: str | None = None,
    org_id: str = "default",
    active_job_guard_id: str | None = None,
    load_sync_config_fn=load_sync_config,
) -> SyncRuntimeBootstrap:
    logger = sync_logging.setup_logging()
    db_manager = DatabaseManager(db_path=db_path)
    db_init_result = db_manager.initialize()

    settings_repo = SettingsRepository(db_manager)
    organization_repo = OrganizationRepository(db_manager)
    organization_config_repo = OrganizationConfigRepository(db_manager)
    connector_repo = SyncConnectorRepository(db_manager)
    mapping_rule_repo = AttributeMappingRuleRepository(db_manager)
    department_ou_mapping_repo = DepartmentOuMappingRepository(db_manager)
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

    active_job = job_repo.get_execution_job_record()
    if active_job and str(active_job.job_id or "").strip() != str(active_job_guard_id or "").strip():
        raise RuntimeError(f"active sync job already exists: {active_job.job_id}")

    policy_settings = _build_policy_settings(
        settings_repo=settings_repo,
        exclusion_repo=exclusion_repo,
        exception_rule_repo=exception_rule_repo,
        mapping_rule_repo=mapping_rule_repo,
        department_ou_mapping_repo=department_ou_mapping_repo,
        organization=organization,
    )

    resolved_config_path = organization.config_path or config_path
    if organization_config_repo.has_config(organization.org_id) or os.path.exists(resolved_config_path):
        config = organization_config_repo.get_app_config(organization.org_id, config_path=resolved_config_path)
    else:
        config = load_sync_config_fn(resolved_config_path)

    repositories = RuntimeRepositories(
        settings_repo=settings_repo,
        organization_repo=organization_repo,
        organization_config_repo=organization_config_repo,
        exclusion_repo=exclusion_repo,
        connector_repo=connector_repo,
        mapping_rule_repo=mapping_rule_repo,
        department_ou_mapping_repo=department_ou_mapping_repo,
        job_repo=job_repo,
        event_repo=event_repo,
        plan_repo=plan_repo,
        operation_log_repo=operation_log_repo,
        conflict_repo=conflict_repo,
        review_repo=review_repo,
        binding_repo=binding_repo,
        user_binding_repo=user_binding_repo,
        department_override_repo=department_override_repo,
        custom_group_binding_repo=custom_group_binding_repo,
        offboarding_repo=offboarding_repo,
        lifecycle_repo=lifecycle_repo,
        replay_request_repo=replay_request_repo,
        state_repo=state_repo,
        exception_rule_repo=exception_rule_repo,
        state_manager=state_manager,
    )
    config_hash = _build_config_hash(
        config=config,
        connector_repo=connector_repo,
        enabled_mapping_rules=policy_settings.enabled_mapping_rules,
        enabled_department_ou_mappings=policy_settings.enabled_department_ou_mappings,
        organization=organization,
        policy_settings=policy_settings,
    )
    return SyncRuntimeBootstrap(
        logger=logger,
        db_manager=db_manager,
        db_init_result=db_init_result,
        repositories=repositories,
        organization=organization,
        config=config,
        policy_settings=policy_settings,
        config_hash=config_hash,
    )
