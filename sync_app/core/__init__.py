from __future__ import annotations

from importlib import import_module
from typing import Any

_ATTRIBUTE_MODULES = {
    "logging_utils": "sync_app.core.logging_utils",
    "APP_VERSION": "sync_app.core.common",
    "format_time_duration": "sync_app.core.common",
    "generate_job_id": "sync_app.core.common",
    "hash_department_state": "sync_app.core.common",
    "hash_user_state": "sync_app.core.common",
    "load_sync_config": "sync_app.core.config",
    "run_config_security_self_check": "sync_app.core.config",
    "test_ldap_connection": "sync_app.core.config",
    "test_source_connection": "sync_app.core.config",
    "test_wecom_connection": "sync_app.core.config",
    "validate_config": "sync_app.core.config",
    "log_filename": "sync_app.core.logging_utils",
    "setup_logging": "sync_app.core.logging_utils",
    "AppConfig": "sync_app.core.models",
    "DepartmentAction": "sync_app.core.models",
    "DepartmentGroupInfo": "sync_app.core.models",
    "DepartmentNode": "sync_app.core.models",
    "DirectoryGroupRecord": "sync_app.core.models",
    "DirectoryUserRecord": "sync_app.core.models",
    "DisableUserAction": "sync_app.core.models",
    "ExclusionRuleRecord": "sync_app.core.models",
    "GroupCleanupAction": "sync_app.core.models",
    "GroupHierarchyAction": "sync_app.core.models",
    "GroupMembershipAction": "sync_app.core.models",
    "GroupPolicyEvaluation": "sync_app.core.models",
    "LDAPConfig": "sync_app.core.models",
    "ManagedGroupBindingRecord": "sync_app.core.models",
    "ManagedGroupTarget": "sync_app.core.models",
    "SkipOperationSummary": "sync_app.core.models",
    "SourceConfig": "sync_app.core.models",
    "SourceConnectorConfig": "sync_app.core.models",
    "SourceDirectoryUser": "sync_app.core.models",
    "SourceUser": "sync_app.core.models",
    "SyncErrorBuckets": "sync_app.core.models",
    "SyncJobRecord": "sync_app.core.models",
    "SyncJobSummary": "sync_app.core.models",
    "SyncOperationCounters": "sync_app.core.models",
    "SyncRunStats": "sync_app.core.models",
    "UserAction": "sync_app.core.models",
    "UserDepartmentBundle": "sync_app.core.models",
    "WeComConfig": "sync_app.core.models",
    "WeComUser": "sync_app.core.models",
}

__all__ = sorted(_ATTRIBUTE_MODULES)


def __getattr__(name: str) -> Any:
    module_name = _ATTRIBUTE_MODULES.get(name)
    if not module_name:
        raise AttributeError(f"module 'sync_app.core' has no attribute {name!r}")
    module = import_module(module_name)
    value = module if name == "logging_utils" else getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
