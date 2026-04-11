from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from sync_app.services.runtime_bootstrap import SyncRuntimeBootstrap


@dataclass
class SyncEnvironmentState:
    source_provider_name: str = ""
    source_provider: Any | None = None
    bot: Any | None = None
    connector_specs: list[dict[str, Any]] = field(default_factory=list)
    connector_specs_by_id: dict[str, dict[str, Any]] = field(default_factory=dict)
    ad_sync_clients: dict[str, Any] = field(default_factory=dict)
    protected_ad_accounts_by_connector: dict[str, set[str]] = field(default_factory=dict)
    departments: list[Any] = field(default_factory=list)
    dept_tree: dict[int, Any] = field(default_factory=dict)
    department_connector_map: dict[int, str] = field(default_factory=dict)
    department_scope_root_map: dict[int, Any] = field(default_factory=dict)
    excluded_department_names: set[str] = field(default_factory=set)
    department_group_targets: dict[int, Any] = field(default_factory=dict)
    current_parent_groups_cache: dict[str, list[Any]] = field(default_factory=dict)
    effective_parent_cache: dict[int, Optional[int]] = field(default_factory=dict)
    policy_skip_markers: set[tuple[Any, ...]] = field(default_factory=set)
    placement_blocked_department_ids: set[int] = field(default_factory=set)


@dataclass
class SyncPlanState:
    started_replay_requests: list[Any] = field(default_factory=list)
    plan_fingerprint_items: list[dict[str, Any]] = field(default_factory=list)
    plan_fingerprint: str = ""
    approved_review: Any | None = None
    review_required_for_high_risk: bool = False
    disable_breaker_triggered: bool = False
    disable_breaker_threshold: int = 0
    managed_user_baseline: int = 0


@dataclass
class SyncActionState:
    department_actions: list[Any] = field(default_factory=list)
    custom_group_actions: list[Any] = field(default_factory=list)
    user_actions: list[Any] = field(default_factory=list)
    membership_actions: list[Any] = field(default_factory=list)
    group_hierarchy_actions: list[Any] = field(default_factory=list)
    group_cleanup_actions: list[Any] = field(default_factory=list)
    disable_actions: list[Any] = field(default_factory=list)


@dataclass
class SyncWorkingState:
    source_user_ids: set[str] = field(default_factory=set)
    current_source_ad_usernames_by_connector: dict[str, set[str]] = field(default_factory=dict)
    enabled_ad_users_by_connector: dict[str, list[str]] = field(default_factory=dict)
    enabled_ad_users_flat: list[str] = field(default_factory=list)
    managed_ad_identities: set[tuple[str, str]] = field(default_factory=set)


@dataclass
class SyncIdentityState:
    user_departments: dict[str, Any] = field(default_factory=dict)
    active_user_bindings: dict[str, str] = field(default_factory=dict)
    binding_resolution_details: dict[str, dict[str, Any]] = field(default_factory=dict)
    user_connector_id_by_userid: dict[str, str] = field(default_factory=dict)
    disabled_bound_userids: set[str] = field(default_factory=set)
    exception_skipped_userids: set[str] = field(default_factory=set)
    source_user_detail_cache: dict[str, dict[str, Any]] = field(default_factory=dict)
    existing_users_map_by_connector: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class SyncRuntimeHooks:
    record_event: Callable[..., None]
    record_operation: Callable[..., None]
    record_conflict: Callable[..., None]
    add_planned_operation: Callable[..., None]
    mark_job: Callable[..., None]
    run_history_cleanup: Callable[[], dict[str, Any]]
    evaluate_group_policy: Callable[..., Any]
    has_exception_rule: Callable[[str, Optional[str]], bool]
    generate_skip_detail_report: Callable[[dict[str, Any]], Any]
    generate_sync_operation_log: Callable[[dict[str, Any], float, Any], Any]
    generate_sync_validation_report: Callable[[dict[str, Any], Any, Any], Any]
    stats_callback: Any | None
    is_cancelled: Callable[[], bool]


@dataclass(frozen=True)
class SyncExecutionServices:
    is_department_excluded: Callable[[Optional[Any]], bool]
    get_connector_id_for_department: Callable[[Optional[Any]], str]
    get_connector_spec: Callable[[str], dict[str, Any]]
    get_ad_sync: Callable[[str], Any]
    is_protected_ad_account: Callable[[str, str], bool]
    is_department_blocked_for_placement: Callable[[Optional[Any]], bool]
    record_group_policy_skip: Callable[..., None]
    record_skip_detail: Callable[..., None]
    record_protected_account_skip: Callable[..., None]
    record_exception_skip: Callable[..., None]
    get_department_group_target: Callable[[Any], Any]
    get_effective_parent_department_id: Callable[[Any], Optional[int]]


@dataclass
class SyncContext:
    start_time: float
    execution_mode: str
    trigger_type: str
    db_path: Optional[str]
    config_path: str
    org_id: str
    bootstrap: SyncRuntimeBootstrap
    sync_stats: Any
    job_id: str
    hooks: SyncRuntimeHooks
    planned_count: int = 0
    executed_count: int = 0
    high_risk_operation_count: int = 0
    environment: SyncEnvironmentState = field(default_factory=SyncEnvironmentState)
    plan: SyncPlanState = field(default_factory=SyncPlanState)
    actions: SyncActionState = field(default_factory=SyncActionState)
    working: SyncWorkingState = field(default_factory=SyncWorkingState)
    identity: SyncIdentityState = field(default_factory=SyncIdentityState)

    @property
    def logger(self) -> Any:
        return self.bootstrap.logger

    @property
    def db_manager(self) -> Any:
        return self.bootstrap.db_manager

    @property
    def db_init_result(self) -> dict[str, Any]:
        return self.bootstrap.db_init_result

    @property
    def repositories(self) -> Any:
        return self.bootstrap.repositories

    @property
    def organization(self) -> Any:
        return self.bootstrap.organization

    @property
    def config(self) -> Any:
        return self.bootstrap.config

    @property
    def policy_settings(self) -> Any:
        return self.bootstrap.policy_settings

    @property
    def config_hash(self) -> str:
        return self.bootstrap.config_hash

    @property
    def resolved_config_path(self) -> str:
        return self.organization.config_path or self.config_path
