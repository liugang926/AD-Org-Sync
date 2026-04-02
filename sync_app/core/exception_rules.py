from __future__ import annotations

from typing import Optional


EXCEPTION_RULE_DEFINITIONS: dict[str, dict[str, str]] = {
    "skip_user_sync": {
        "label": "Skip user sync",
        "match_type": "source_user_id",
        "match_label": "Source user ID",
        "description": "The user is excluded from automatic create, update, OU placement, and disable actions.",
    },
    "skip_user_disable": {
        "label": "Skip automatic disable",
        "match_type": "source_user_id",
        "match_label": "Source user ID",
        "description": "The AD account stays enabled even after the user leaves the managed source scope.",
    },
    "skip_user_group_membership": {
        "label": "Skip user group membership",
        "match_type": "source_user_id",
        "match_label": "Source user ID",
        "description": "The user is excluded from managed department group membership updates.",
    },
    "skip_department_placement": {
        "label": "Skip department placement",
        "match_type": "department_id",
        "match_label": "Department ID",
        "description": "The department is excluded from OU placement target selection.",
    },
    "skip_group_relation_cleanup": {
        "label": "Skip group relation cleanup",
        "match_type": "group_sam",
        "match_label": "AD group sAMAccountName",
        "description": "Managed recursive parent-child group cleanup is skipped for the group.",
    },
}

EXCEPTION_MATCH_TYPE_LABELS = {
    "source_user_id": "Source user ID",
    "wecom_userid": "Source user ID",
    "department_id": "Department ID",
    "group_sam": "AD group sAMAccountName",
}


def normalize_exception_rule_type(rule_type: str | None) -> str:
    candidate = str(rule_type or "").strip().lower()
    return candidate if candidate in EXCEPTION_RULE_DEFINITIONS else ""


def get_exception_rule_definition(rule_type: str | None) -> Optional[dict[str, str]]:
    normalized = normalize_exception_rule_type(rule_type)
    if not normalized:
        return None
    return dict(EXCEPTION_RULE_DEFINITIONS[normalized])


def get_exception_rule_match_type(rule_type: str | None) -> str:
    definition = get_exception_rule_definition(rule_type)
    return str(definition.get("match_type") or "") if definition else ""


def normalize_exception_match_value(match_type: str | None, match_value: str | None) -> str:
    normalized_type = str(match_type or "").strip().lower()
    normalized_value = str(match_value or "").strip()
    if normalized_type in {"source_user_id", "wecom_userid", "group_sam"}:
        return normalized_value.lower()
    return normalized_value
