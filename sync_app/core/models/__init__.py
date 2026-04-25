from __future__ import annotations

from sync_app.core.models.base import MappingLikeModel
from sync_app.core.models.actions import (
    DepartmentAction,
    DisableUserAction,
    GroupCleanupAction,
    GroupHierarchyAction,
    GroupMembershipAction,
    ManagedGroupTarget,
    UserAction,
)
from sync_app.core.models.config import (
    AccountConfig,
    AppConfig,
    LDAPConfig,
    SourceConfig,
    SourceConnectorConfig,
    WeComConfig,
)
from sync_app.core.models.config_records import (
    AttributeMappingRuleRecord,
    ConfigReleaseSnapshotRecord,
    DataQualitySnapshotRecord,
    DepartmentOuMappingRecord,
    ExclusionRuleRecord,
    ManagedGroupBindingRecord,
    OrganizationRecord,
)
from sync_app.core.models.conflicts import (
    SyncConflictRecord,
    SyncExceptionRuleRecord,
    UserDepartmentOverrideRecord,
    UserIdentityBindingRecord,
)
from sync_app.core.models.directory import (
    DepartmentGroupInfo,
    DepartmentNode,
    DirectoryGroupRecord,
    DirectoryUserRecord,
    GroupPolicyEvaluation,
    SourceDirectoryUser,
    SourceUser,
    UserDepartmentBundle,
    WeComUser,
)
from sync_app.core.models.integrations import (
    IntegrationWebhookOutboxRecord,
    IntegrationWebhookSubscriptionRecord,
    SyncConnectorRecord,
)
from sync_app.core.models.lifecycle import (
    CustomManagedGroupBindingRecord,
    OffboardingRecord,
    UserLifecycleRecord,
)
from sync_app.core.models.sync_job import (
    SkipOperationSummary,
    SyncErrorBuckets,
    SyncJobRecord,
    SyncJobSummary,
    SyncOperationCounters,
    SyncOperationRecord,
    SyncPlanReviewRecord,
    SyncReplayRequestRecord,
    SyncRunStats,
)
from sync_app.core.models.utils import (
    _coerce_int_list,
    _extract_department_ids,
    _normalize_mapping_direction_value,
)
from sync_app.core.models.web_admin import WebAdminUserRecord, WebAuditLogRecord

__all__ = ['MappingLikeModel', '_normalize_mapping_direction_value', '_coerce_int_list', '_extract_department_ids', 'SourceConnectorConfig', 'SourceConfig', 'WeComConfig', 'LDAPConfig', 'AccountConfig', 'AppConfig', 'DepartmentNode', 'SourceDirectoryUser', 'SourceUser', 'WeComUser', 'UserDepartmentBundle', 'GroupPolicyEvaluation', 'DepartmentGroupInfo', 'DirectoryUserRecord', 'DirectoryGroupRecord', 'SkipOperationSummary', 'SyncErrorBuckets', 'SyncOperationCounters', 'SyncRunStats', 'SyncJobSummary', 'SyncJobRecord', 'SyncOperationRecord', 'SyncPlanReviewRecord', 'SyncReplayRequestRecord', 'ManagedGroupTarget', 'DepartmentAction', 'UserAction', 'GroupMembershipAction', 'GroupHierarchyAction', 'GroupCleanupAction', 'DisableUserAction', 'SyncConflictRecord', 'SyncExceptionRuleRecord', 'UserIdentityBindingRecord', 'UserDepartmentOverrideRecord', 'ExclusionRuleRecord', 'ManagedGroupBindingRecord', 'ConfigReleaseSnapshotRecord', 'DataQualitySnapshotRecord', 'AttributeMappingRuleRecord', 'OrganizationRecord', 'DepartmentOuMappingRecord', 'IntegrationWebhookSubscriptionRecord', 'IntegrationWebhookOutboxRecord', 'SyncConnectorRecord', 'OffboardingRecord', 'UserLifecycleRecord', 'CustomManagedGroupBindingRecord', 'WebAdminUserRecord', 'WebAuditLogRecord']
