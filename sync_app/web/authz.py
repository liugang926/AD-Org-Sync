from __future__ import annotations

from typing import Iterable


ROLE_SUPER_ADMIN = "super_admin"
ROLE_CONNECTOR_ADMIN = "connector_admin"
ROLE_AD_ADMIN = "ad_admin"
ROLE_MAPPING_REVIEWER = "mapping_reviewer"
ROLE_SYNC_EXECUTOR = "sync_executor"
ROLE_OPERATOR = "operator"
ROLE_AUDITOR = "auditor"

WEB_ADMIN_ROLES = (
    ROLE_SUPER_ADMIN,
    ROLE_CONNECTOR_ADMIN,
    ROLE_AD_ADMIN,
    ROLE_MAPPING_REVIEWER,
    ROLE_SYNC_EXECUTOR,
    ROLE_OPERATOR,
    ROLE_AUDITOR,
)

ROLE_CAPABILITIES = {
    ROLE_SUPER_ADMIN: {
        "dashboard.read",
        "organizations.manage",
        "system.manage",
        "config.read",
        "config.write",
        "connectors.write",
        "ad.write",
        "mappings.read",
        "mappings.write",
        "exceptions.read",
        "exceptions.write",
        "jobs.read",
        "jobs.run",
        "jobs.review",
        "database.read",
        "database.manage",
        "audit.read",
        "users.manage",
        "account.manage",
    },
    ROLE_CONNECTOR_ADMIN: {
        "dashboard.read",
        "config.read",
        "config.write",
        "connectors.write",
        "mappings.read",
        "database.read",
        "audit.read",
        "account.manage",
    },
    ROLE_AD_ADMIN: {
        "dashboard.read",
        "config.read",
        "config.write",
        "ad.write",
        "mappings.read",
        "jobs.read",
        "database.read",
        "audit.read",
        "account.manage",
    },
    ROLE_MAPPING_REVIEWER: {
        "dashboard.read",
        "mappings.read",
        "mappings.write",
        "exceptions.read",
        "exceptions.write",
        "jobs.read",
        "jobs.review",
        "database.read",
        "audit.read",
        "account.manage",
    },
    ROLE_SYNC_EXECUTOR: {
        "dashboard.read",
        "mappings.read",
        "exceptions.read",
        "jobs.read",
        "jobs.run",
        "database.read",
        "audit.read",
        "account.manage",
    },
    ROLE_OPERATOR: {
        "dashboard.read",
        "mappings.read",
        "exceptions.read",
        "jobs.read",
        "jobs.run",
        "database.read",
        "audit.read",
        "account.manage",
    },
    ROLE_AUDITOR: {
        "dashboard.read",
        "mappings.read",
        "exceptions.read",
        "jobs.read",
        "database.read",
        "audit.read",
        "account.manage",
    },
}


def normalize_role(role: str | None, *, default: str = ROLE_OPERATOR) -> str:
    candidate = (role or "").strip().lower()
    if candidate in WEB_ADMIN_ROLES:
        return candidate
    return default


def has_capability(role: str | None, capability: str) -> bool:
    normalized_role = normalize_role(role, default="")
    if not normalized_role:
        return False
    return capability in ROLE_CAPABILITIES.get(normalized_role, set())


def role_capabilities(role: str | None) -> set[str]:
    normalized_role = normalize_role(role, default="")
    return set(ROLE_CAPABILITIES.get(normalized_role, set()))


def any_capability(role: str | None, capabilities: Iterable[str]) -> bool:
    return any(has_capability(role, capability) for capability in capabilities)
