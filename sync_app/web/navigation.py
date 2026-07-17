from __future__ import annotations

from dataclasses import dataclass

from sync_app.web.authz import ROLE_AUDITOR, role_capabilities


CANONICAL_ROUTE_PATHS = {
    "dashboard": "/overview/control-tower",
    "config": "/data-sources/connectors",
    "source-directory": "/data-sources/source-directory",
    "snapshots": "/data-sources/snapshots",
    "data-quality": "/data-sources/data-quality",
    "identity-matching": "/identity-governance/identity-matching",
    "binding-reconciliation": "/identity-governance/binding-reconciliation",
    "conflicts": "/identity-governance/conflicts",
    "mappings": "/identity-governance/manual-overrides",
    "exceptions": "/identity-governance/exception-rules",
    "advanced-sync": "/sync-policies",
    "sync-scope": "/sync-policies/scope",
    "sync-account-naming": "/sync-policies/account-naming",
    "sync-attribute-mappings": "/sync-policies/attribute-mappings",
    "sync-department-ou-routing": "/sync-policies/department-ou-routing",
    "sync-group-rules": "/sync-policies/group-rules",
    "sync-lifecycle-policy": "/sync-policies/lifecycle",
    "sync-security-policy": "/sync-policies/security",
    "sync-policy-releases": "/sync-policies/releases",
    "jobs": "/execution-center/run-review",
    "execution-dry-run": "/execution-center/dry-run",
    "execution-plan-review": "/execution-center/plan-review",
    "execution-apply": "/execution-center/apply",
    "execution-jobs": "/execution-center/jobs",
    "lifecycle": "/operations-center/lifecycle-queue",
    "automation-center": "/operations-center/automation",
    "integrations": "/operations-center/notifications",
    "audit": "/operations-center/audit-log",
    "organizations": "/system-management/organizations",
    "users": "/system-management/administrators",
    "database": "/system-management/database",
    "employee-self-service": "/system-management/employee-self-service",
    "branding": "/system-management/branding",
    "deployment": "/system-management/deployment",
    "account": "/system-management/account",
}


PHASE7_LEGACY_GET_REDIRECTS = {
    "/lifecycle": CANONICAL_ROUTE_PATHS["lifecycle"],
    "/automation-center": CANONICAL_ROUTE_PATHS["automation-center"],
    "/integrations": CANONICAL_ROUTE_PATHS["integrations"],
    "/audit": CANONICAL_ROUTE_PATHS["audit"],
    "/organizations": CANONICAL_ROUTE_PATHS["organizations"],
    "/users": CANONICAL_ROUTE_PATHS["users"],
    "/database": CANONICAL_ROUTE_PATHS["database"],
    "/account": CANONICAL_ROUTE_PATHS["account"],
}


@dataclass(frozen=True, slots=True)
class NavigationItem:
    page: str
    label: str
    icon: str
    capability: str
    legacy_paths: tuple[str, ...]
    advanced_only: bool = False
    basic_roles: tuple[str, ...] = ()

    @property
    def href(self) -> str:
        return CANONICAL_ROUTE_PATHS[self.page]


@dataclass(frozen=True, slots=True)
class NavigationGroup:
    label: str
    items: tuple[NavigationItem, ...]


NAVIGATION_GROUPS = (
    NavigationGroup(
        label="Overview",
        items=(
            NavigationItem(
                page="dashboard",
                label="Control Tower",
                icon="layout-dashboard",
                capability="dashboard.read",
                legacy_paths=("/dashboard",),
            ),
        ),
    ),
    NavigationGroup(
        label="Data Sources",
        items=(
            NavigationItem(
                page="config",
                label="Connectors",
                icon="plug-zap",
                capability="config.read",
                legacy_paths=("/config", "/config/releases"),
                advanced_only=True,
            ),
            NavigationItem(
                page="source-directory",
                label="Source Directory",
                icon="users-round",
                capability="config.read",
                legacy_paths=("/source-directory",),
            ),
            NavigationItem(
                page="snapshots",
                label="Snapshot History",
                icon="history",
                capability="config.read",
                legacy_paths=(),
                advanced_only=True,
            ),
            NavigationItem(
                page="data-quality",
                label="Data Quality",
                icon="circle-check-big",
                capability="config.read",
                legacy_paths=("/data-quality",),
                advanced_only=True,
            ),
        ),
    ),
    NavigationGroup(
        label="Identity Governance",
        items=(
            NavigationItem(
                page="identity-matching",
                label="Identity Matching",
                icon="user-round-search",
                capability="mappings.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="binding-reconciliation",
                label="Binding Reconciliation",
                icon="scan-search",
                capability="mappings.read",
                legacy_paths=(),
                advanced_only=True,
            ),
            NavigationItem(
                page="conflicts",
                label="Conflict Queue",
                icon="shield-alert",
                capability="jobs.read",
                legacy_paths=("/conflicts",),
            ),
            NavigationItem(
                page="mappings",
                label="Manual Overrides",
                icon="layers",
                capability="mappings.read",
                legacy_paths=("/mappings",),
                advanced_only=True,
            ),
            NavigationItem(
                page="exceptions",
                label="Exception Rules",
                icon="ban",
                capability="exceptions.read",
                legacy_paths=("/exceptions",),
                advanced_only=True,
            ),
        ),
    ),
    NavigationGroup(
        label="Sync Policies",
        items=(
            NavigationItem(
                page="sync-scope",
                label="Sync Scope",
                icon="list-filter",
                capability="config.read",
                legacy_paths=(),
                advanced_only=True,
            ),
            NavigationItem(
                page="sync-account-naming",
                label="Account Naming",
                icon="badge-check",
                capability="config.read",
                legacy_paths=("/advanced-sync",),
                advanced_only=True,
            ),
            NavigationItem(
                page="sync-attribute-mappings",
                label="Attribute Mappings",
                icon="arrow-left-right",
                capability="config.read",
                legacy_paths=(),
                advanced_only=True,
            ),
            NavigationItem(
                page="sync-department-ou-routing",
                label="Department & OU Routing",
                icon="route",
                capability="config.read",
                legacy_paths=(),
                advanced_only=True,
            ),
            NavigationItem(
                page="sync-group-rules",
                label="Group Rules",
                icon="users-round",
                capability="config.read",
                legacy_paths=(),
                advanced_only=True,
            ),
            NavigationItem(
                page="sync-lifecycle-policy",
                label="Lifecycle Policy",
                icon="refresh-cw",
                capability="config.read",
                legacy_paths=(),
                advanced_only=True,
            ),
            NavigationItem(
                page="sync-security-policy",
                label="Security Policy",
                icon="shield-check",
                capability="config.read",
                legacy_paths=(),
                advanced_only=True,
            ),
        ),
    ),
    NavigationGroup(
        label="Execution Center",
        items=(
            NavigationItem(
                page="execution-dry-run",
                label="Dry Run",
                icon="play-circle",
                capability="jobs.read",
                legacy_paths=("/jobs", "/execution-center/run-review"),
            ),
            NavigationItem(
                page="execution-plan-review",
                label="Plan Review",
                icon="clipboard-check",
                capability="jobs.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="execution-apply",
                label="Apply",
                icon="shield-check",
                capability="jobs.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="execution-jobs",
                label="Job History",
                icon="history",
                capability="jobs.read",
                legacy_paths=(),
            ),
        ),
    ),
    NavigationGroup(
        label="Operations Center",
        items=(
            NavigationItem(
                page="lifecycle",
                label="Lifecycle Queue",
                icon="clock-3",
                capability="config.read",
                legacy_paths=("/lifecycle",),
                advanced_only=True,
            ),
            NavigationItem(
                page="automation-center",
                label="Automation & Schedules",
                icon="calendar-clock",
                capability="config.read",
                legacy_paths=("/automation-center",),
                advanced_only=True,
            ),
            NavigationItem(
                page="integrations",
                label="Notifications",
                icon="bell-ring",
                capability="config.read",
                legacy_paths=("/integrations",),
                advanced_only=True,
            ),
            NavigationItem(
                page="audit",
                label="Audit Logs",
                icon="scroll-text",
                capability="audit.read",
                legacy_paths=("/audit",),
                advanced_only=True,
                basic_roles=(ROLE_AUDITOR,),
            ),
        ),
    ),
    NavigationGroup(
        label="System Management",
        items=(
            NavigationItem(
                page="organizations",
                label="Organizations",
                icon="building-2",
                capability="organizations.manage",
                legacy_paths=("/organizations",),
                advanced_only=True,
            ),
            NavigationItem(
                page="users",
                label="Administrators & Permissions",
                icon="users-cog",
                capability="users.manage",
                legacy_paths=("/users",),
                advanced_only=True,
            ),
            NavigationItem(
                page="employee-self-service",
                label="Employee Self-Service",
                icon="key-round",
                capability="config.read",
                legacy_paths=("/config#config-section-security",),
                advanced_only=True,
            ),
            NavigationItem(
                page="database",
                label="Database",
                icon="database",
                capability="database.read",
                legacy_paths=("/database",),
                advanced_only=True,
            ),
            NavigationItem(
                page="branding",
                label="Branding & Appearance",
                icon="palette",
                capability="config.read",
                legacy_paths=("/config#config-section-web",),
                advanced_only=True,
            ),
            NavigationItem(
                page="deployment",
                label="Deployment Settings",
                icon="server-cog",
                capability="config.read",
                legacy_paths=("/config#config-section-web",),
                advanced_only=True,
            ),
        ),
    ),
)


def build_navigation_groups(
    role: str | None,
    *,
    show_advanced_navigation: bool,
) -> tuple[NavigationGroup, ...]:
    capabilities = role_capabilities(role)
    visible_groups: list[NavigationGroup] = []
    for group in NAVIGATION_GROUPS:
        visible_items = tuple(
            item
            for item in group.items
            if item.capability in capabilities
            and (
                not item.advanced_only
                or show_advanced_navigation
                or role in item.basic_roles
            )
        )
        if visible_items:
            visible_groups.append(NavigationGroup(label=group.label, items=visible_items))
    return tuple(visible_groups)
