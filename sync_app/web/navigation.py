from __future__ import annotations

from dataclasses import dataclass

from sync_app.web.authz import ROLE_AUDITOR, role_capabilities


CANONICAL_ROUTE_PATHS = {
    "getting-started": "/getting-started",
    "dashboard": "/overview/control-tower",
    "config": "/data-sources/connectors",
    "source-directory": "/data-sources/source-directory",
    "snapshots": "/data-sources/snapshots",
    "data-quality": "/data-sources/data-quality",
    "identity-matching": "/identity-governance/identity-matching",
    "identity-match-rules": "/identity-governance/match-rules",
    "account-takeover": "/identity-governance/account-takeover",
    "binding-reconciliation": "/identity-governance/binding-reconciliation",
    "conflicts": "/identity-governance/conflicts",
    "mappings": "/identity-governance/manual-overrides",
    "exceptions": "/identity-governance/exception-rules",
    "advanced-sync": "/sync-policies",
    "sync-scope": "/sync-policies/scope",
    "sync-account-naming": "/sync-policies/account-naming",
    "sync-attribute-mappings": "/sync-policies/attribute-mappings",
    "sync-field-authority": "/sync-policies/field-authority",
    "sync-department-ou-routing": "/sync-policies/department-ou-routing",
    "sync-group-rules": "/sync-policies/group-rules",
    "sync-lifecycle-policy": "/sync-policies/lifecycle",
    "sync-security-policy": "/sync-policies/security",
    "sync-policy-releases": "/sync-policies/releases",
    "jobs": "/jobs",
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
    "/config": CANONICAL_ROUTE_PATHS["config"],
    "/advanced-sync": CANONICAL_ROUTE_PATHS["sync-scope"],
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
        label="Configuration Guide",
        items=(
            NavigationItem(
                page="getting-started",
                label="Configuration Guide",
                icon="map",
                capability="dashboard.read",
                legacy_paths=(),
            ),
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
        label="Data Sources & AD",
        items=(
            NavigationItem(
                page="config",
                label="Connectors",
                icon="plug-zap",
                capability="config.read",
                legacy_paths=("/config", "/config/releases"),
            ),
            NavigationItem(
                page="source-directory",
                label="Source Directory & Snapshots",
                icon="users-round",
                capability="config.read",
                legacy_paths=("/source-directory",),
            ),
            NavigationItem(
                page="data-quality",
                label="Data Quality Review",
                icon="circle-check-big",
                capability="config.read",
                legacy_paths=("/data-quality",),
            ),
        ),
    ),
    NavigationGroup(
        label="Identity Matching",
        items=(
            NavigationItem(
                page="identity-matching",
                label="Identity Matching Workbench",
                icon="user-round-search",
                capability="mappings.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="identity-match-rules",
                label="Identity Match Rules",
                icon="list-checks",
                capability="mappings.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="account-takeover",
                label="Existing Account Takeover",
                icon="link-2",
                capability="mappings.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="mappings",
                label="Manual Identity Overrides",
                icon="layers",
                capability="mappings.read",
                legacy_paths=("/mappings",),
                advanced_only=True,
            ),
            NavigationItem(
                page="binding-reconciliation",
                label="Binding Reconciliation",
                icon="scan-search",
                capability="mappings.read",
                legacy_paths=(),
                advanced_only=True,
            ),
        ),
    ),
    NavigationGroup(
        label="Organization & OU",
        items=(
            NavigationItem(
                page="sync-account-naming",
                label="Account Naming",
                icon="badge-check",
                capability="config.read",
                legacy_paths=("/advanced-sync",),
            ),
            NavigationItem(
                page="sync-field-authority",
                label="Field Authority",
                icon="shield-check",
                capability="config.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="sync-attribute-mappings",
                label="Attribute Mappings",
                icon="arrow-left-right",
                capability="config.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="sync-department-ou-routing",
                label="Department & OU Routing",
                icon="route",
                capability="config.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="sync-group-rules",
                label="Group Rules",
                icon="users-round",
                capability="config.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="sync-lifecycle-policy",
                label="Lifecycle Policy",
                icon="refresh-cw",
                capability="config.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="sync-security-policy",
                label="Security Gates",
                icon="shield-alert",
                capability="config.read",
                legacy_paths=(),
            ),
        ),
    ),
    NavigationGroup(
        label="Sync Preview",
        items=(
            NavigationItem(
                page="sync-scope",
                label="Synchronization Scope",
                icon="list-filter",
                capability="config.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="sync-policy-releases",
                label="Policy Summary & Release",
                icon="package-check",
                capability="config.read",
                legacy_paths=(),
            ),
            NavigationItem(
                page="execution-dry-run",
                label="Dry Run",
                icon="play-circle",
                capability="jobs.read",
                legacy_paths=("/jobs", "/execution-center/run-review"),
            ),
        ),
    ),
    NavigationGroup(
        label="Conflict Resolution",
        items=(
            NavigationItem(
                page="conflicts",
                label="Conflict Queue",
                icon="shield-alert",
                capability="jobs.read",
                legacy_paths=("/conflicts",),
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
        label="Execution & History",
        items=(
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
                label="Execution History",
                icon="history",
                capability="jobs.read",
                legacy_paths=(),
            ),
        ),
    ),
    NavigationGroup(
        label="Audit Center",
        items=(
            NavigationItem(
                page="audit",
                label="Audit Logs",
                icon="scroll-text",
                capability="audit.read",
                legacy_paths=("/audit",),
                basic_roles=(ROLE_AUDITOR,),
            ),
        ),
    ),
    NavigationGroup(
        label="Advanced Operations",
        items=(
            NavigationItem(
                page="lifecycle",
                label="Lifecycle Workbench",
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
                page="employee-self-service",
                label="Employee Self-Service",
                icon="key-round",
                capability="config.read",
                legacy_paths=("/config#config-section-security",),
                advanced_only=True,
            ),
        ),
    ),
    NavigationGroup(
        label="System Settings",
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
                label="Platform Accounts",
                icon="users-cog",
                capability="users.manage",
                legacy_paths=("/users",),
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
                capability="system.manage",
                legacy_paths=(),
                advanced_only=True,
            ),
            NavigationItem(
                page="deployment",
                label="Deployment Settings",
                icon="server-cog",
                capability="system.manage",
                legacy_paths=(),
                advanced_only=True,
            ),
            NavigationItem(
                page="account",
                label="My Account",
                icon="circle-user-round",
                capability="account.manage",
                legacy_paths=("/account",),
                advanced_only=True,
            ),
        ),
    ),
)


ADVANCED_NAV_PAGES = frozenset(
    item.page
    for group in NAVIGATION_GROUPS
    for item in group.items
    if item.advanced_only
)

# Pages in this set remain directly addressable in Basic mode, but the shell
# explains that they are infrequent operational or system-administration tools.
# Business rollout configuration pages deliberately stay out of this gate.
ADVANCED_MODE_PAGE_GATES = ADVANCED_NAV_PAGES


def build_navigation_groups(
    role: str | None,
    *,
    show_advanced_navigation: bool,
    current_page: str = "",
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
                or item.page == current_page
                or role in item.basic_roles
            )
        )
        if visible_items:
            visible_groups.append(NavigationGroup(label=group.label, items=visible_items))
    return tuple(visible_groups)
