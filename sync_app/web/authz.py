from __future__ import annotations

from typing import Iterable


ROLE_SUPER_ADMIN = "super_admin"
ROLE_OPERATOR = "operator"
ROLE_AUDITOR = "auditor"

WEB_ADMIN_ROLES = (
    ROLE_SUPER_ADMIN,
    ROLE_OPERATOR,
    ROLE_AUDITOR,
)

ROLE_CAPABILITIES = {
    ROLE_SUPER_ADMIN: {
        "dashboard.read",
        "organizations.manage",
        "config.read",
        "config.write",
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
