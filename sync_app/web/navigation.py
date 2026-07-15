from __future__ import annotations

from dataclasses import dataclass

from sync_app.web.authz import ROLE_AUDITOR, role_capabilities


CANONICAL_ROUTE_PATHS = {
    "dashboard": "/overview/control-tower",
    "config": "/data-sources/connectors",
    "source-directory": "/data-sources/source-directory",
    "snapshots": "/data-sources/snapshots",
    "data-quality": "/data-sources/data-quality",
    "binding-reconciliation": "/identity-governance/binding-reconciliation",
    "conflicts": "/identity-governance/conflicts",
    "mappings": "/identity-governance/manual-overrides",
    "exceptions": "/identity-governance/exception-rules",
    "advanced-sync": "/sync-policies",
    "sync-scope": "/sync-policies/scope",
    "jobs": "/execution-center/run-review",
    "lifecycle": "/operations-center/lifecycle-queue",
    "automation-center": "/operations-center/automation",
    "integrations": "/operations-center/notifications",
    "audit": "/operations-center/audit-log",
    "organizations": "/system-management/organizations",
    "users": "/system-management/administrators",
    "database": "/system-management/database",
    "account": "/system-management/account",
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
                page="conflicts",
                label="Conflict Queue",
                icon="shield-alert",
                capability="jobs.read",
                legacy_paths=("/conflicts",),
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
                page="advanced-sync",
                label="Policy Center",
                icon="waypoints",
                capability="config.read",
                legacy_paths=("/advanced-sync",),
                advanced_only=True,
            ),
        ),
    ),
    NavigationGroup(
        label="Execution Center",
        items=(
            NavigationItem(
                page="jobs",
                label="Run Review",
                icon="play-circle",
                capability="jobs.read",
                legacy_paths=("/jobs",),
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
                label="Notifications & Integrations",
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
                page="database",
                label="Database",
                icon="database",
                capability="database.read",
                legacy_paths=("/database",),
                advanced_only=True,
            ),
            NavigationItem(
                page="account",
                label="My Account",
                icon="user-cog",
                capability="account.manage",
                legacy_paths=("/account",),
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
