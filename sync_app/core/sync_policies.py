from __future__ import annotations

import re
from typing import Any, Iterable

from sync_app.core.models import DepartmentNode, SourceDirectoryUser

try:
    from pypinyin import lazy_pinyin
except Exception:  # pragma: no cover - optional dependency at runtime
    lazy_pinyin = None


ATTRIBUTE_MAPPING_DIRECTIONS = ("source_to_ad", "ad_to_source")
ATTRIBUTE_MAPPING_DIRECTION_ALIASES = {
    "wecom_to_ad": "source_to_ad",
    "ad_to_wecom": "ad_to_source",
    "source_to_ad": "source_to_ad",
    "ad_to_source": "ad_to_source",
}
ATTRIBUTE_SYNC_MODES = ("replace", "fill_if_empty", "preserve")
MANAGED_GROUP_TYPES = ("security", "distribution", "mail_enabled_security")

EMPLOYEE_ID_FIELD_CANDIDATES = (
    "employee_id",
    "employeeid",
    "job_number",
    "jobnumber",
    "staff_no",
    "staffno",
    "workcode",
    "work_code",
    "userid",
)

PHONE_FIELD_CANDIDATES = (
    "mobile",
    "telephone",
    "phone",
    "tel",
)

POSITION_FIELD_CANDIDATES = (
    "position",
    "title",
)

MANAGER_FIELD_CANDIDATES = (
    "direct_leader",
    "direct_leader_userid",
    "manager_userid",
    "manager",
)


def normalize_mapping_direction(value: str | None, *, default: str = "source_to_ad") -> str:
    candidate = str(value or "").strip().lower()
    if candidate in ATTRIBUTE_MAPPING_DIRECTION_ALIASES:
        return ATTRIBUTE_MAPPING_DIRECTION_ALIASES[candidate]
    return ATTRIBUTE_MAPPING_DIRECTION_ALIASES.get(str(default or "").strip().lower(), "source_to_ad")


def normalize_sync_mode(value: str | None, *, default: str = "replace") -> str:
    candidate = str(value or "").strip().lower()
    if candidate in ATTRIBUTE_SYNC_MODES:
        return candidate
    return default


def normalize_group_type(value: str | None, *, default: str = "security") -> str:
    candidate = str(value or "").strip().lower()
    if candidate in MANAGED_GROUP_TYPES:
        return candidate
    return default


def _normalize_placeholder_key(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower()).strip("_")


def _first_payload_value(payload: dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, (list, tuple)):
            if not value:
                continue
            return str(value[0]).strip()
        return str(value).strip()
    return ""


def _compute_pinyin_initials(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if lazy_pinyin is not None:
        try:
            return "".join(item[:1] for item in lazy_pinyin(text) if item).lower()
        except Exception:
            pass
    ascii_initials = [segment[:1].lower() for segment in re.findall(r"[A-Za-z0-9]+", text) if segment]
    return "".join(ascii_initials)


def build_template_context(
    user: SourceDirectoryUser,
    *,
    connector_id: str = "default",
    ad_username: str = "",
    email: str = "",
    target_department: DepartmentNode | None = None,
) -> dict[str, str]:
    payload = user.to_state_payload()
    context: dict[str, str] = {
        "userid": str(user.userid or "").strip(),
        "name": str(user.name or "").strip(),
        "display_name": str(user.name or "").strip(),
        "email": str(email or payload.get("email") or "").strip(),
        "email_localpart": "",
        "ad_username": str(ad_username or "").strip(),
        "connector_id": str(connector_id or "default").strip() or "default",
        "employee_id": _first_payload_value(payload, EMPLOYEE_ID_FIELD_CANDIDATES),
        "position": _first_payload_value(payload, POSITION_FIELD_CANDIDATES),
        "mobile": _first_payload_value(payload, PHONE_FIELD_CANDIDATES),
        "pinyin_initials": _compute_pinyin_initials(user.name),
    }
    if "@" in context["email"]:
        context["email_localpart"] = context["email"].split("@", 1)[0].strip()
    if target_department:
        context["department_id"] = str(target_department.department_id)
        context["department_name"] = str(target_department.name or "").strip()
        context["department_path"] = "/".join(target_department.path or [])
    else:
        context["department_id"] = ""
        context["department_name"] = ""
        context["department_path"] = ""

    for key, value in payload.items():
        normalized_key = _normalize_placeholder_key(key)
        if not normalized_key or normalized_key in context:
            continue
        if isinstance(value, (list, tuple)):
            normalized_value = ",".join(str(item).strip() for item in value if str(item).strip())
        elif isinstance(value, dict):
            continue
        else:
            normalized_value = str(value or "").strip()
        context[normalized_key] = normalized_value

    return context


def render_template(template: str, context: dict[str, Any]) -> str:
    raw_template = str(template or "").strip()
    if not raw_template:
        return ""

    def replace(match: re.Match[str]) -> str:
        key = _normalize_placeholder_key(match.group(1))
        return str(context.get(key) or "").strip()

    rendered = re.sub(r"\{([^{}]+)\}", replace, raw_template)
    return rendered.strip()


def build_identity_candidates(user: SourceDirectoryUser, *, username_template: str = "") -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    seen = set()
    template_context = build_template_context(user)

    def add_candidate(rule_name: str, username: str, explanation: str) -> None:
        normalized = re.sub(r"[^A-Za-z0-9._-]+", "", str(username or "").strip())
        if not normalized:
            return
        lowered = normalized.lower()
        if lowered in seen:
            return
        seen.add(lowered)
        candidates.append(
            {
                "rule": rule_name,
                "username": normalized,
                "explanation": explanation,
            }
        )

    generated_username = render_template(username_template, template_context)
    if generated_username:
        add_candidate(
            "template_generated_username",
            generated_username,
            "Configured username template produced a managed AD username candidate",
        )

    add_candidate("existing_ad_userid", user.userid, "Source user ID maps directly to an existing AD username")
    email = (user.email or user.raw_payload.get("email") or "").strip()
    if "@" in email:
        localpart = email.split("@", 1)[0].strip()
        add_candidate(
            "existing_ad_email_localpart",
            localpart,
            "Source email local part maps to an existing AD username",
        )

    add_candidate(
        "derived_default_userid",
        generated_username or user.userid,
        (
            "No existing AD user matched, use the configured username template"
            if generated_username
            else "No existing AD user matched, default to userid for managed account naming"
        ),
    )
    return candidates


def build_source_to_ad_mapping_payload(
    user: SourceDirectoryUser,
    *,
    connector_id: str,
    ad_username: str,
    email: str,
    target_department: DepartmentNode | None,
    rules: Iterable[Any],
) -> dict[str, dict[str, str]]:
    context = build_template_context(
        user,
        connector_id=connector_id,
        ad_username=ad_username,
        email=email,
        target_department=target_department,
    )
    mapped: dict[str, dict[str, str]] = {}
    for rule in rules:
        raw_connector_id = str(getattr(rule, "connector_id", "") or "").strip()
        if raw_connector_id and raw_connector_id != connector_id:
            continue
        source_field = str(getattr(rule, "source_field", "") or "").strip()
        target_field = str(getattr(rule, "target_field", "") or "").strip()
        if not source_field or not target_field:
            continue
        template = str(getattr(rule, "transform_template", "") or "").strip()
        value = render_template(template, context) if template else context.get(_normalize_placeholder_key(source_field), "")
        if value == "":
            continue
        mapped[target_field] = {
            "value": value,
            "mode": normalize_sync_mode(getattr(rule, "sync_mode", "replace")),
            "source_field": source_field,
            "template": template,
        }
    return mapped


def build_wecom_to_ad_mapping_payload(
    user: SourceDirectoryUser,
    *,
    connector_id: str,
    ad_username: str,
    email: str,
    target_department: DepartmentNode | None,
    rules: Iterable[Any],
) -> dict[str, dict[str, str]]:
    return build_source_to_ad_mapping_payload(
        user,
        connector_id=connector_id,
        ad_username=ad_username,
        email=email,
        target_department=target_department,
        rules=rules,
    )


def build_ad_to_source_mapping_payload(
    ad_attributes: dict[str, Any],
    source_payload: dict[str, Any],
    *,
    connector_id: str,
    rules: Iterable[Any],
) -> dict[str, Any]:
    normalized_ad_attrs = {
        _normalize_placeholder_key(key): (
            ",".join(str(item).strip() for item in value if str(item).strip())
            if isinstance(value, (list, tuple))
            else str(value or "").strip()
        )
        for key, value in (ad_attributes or {}).items()
    }
    normalized_source = {
        _normalize_placeholder_key(key): (
            ",".join(str(item).strip() for item in value if str(item).strip())
            if isinstance(value, (list, tuple))
            else str(value or "").strip()
        )
        for key, value in (source_payload or {}).items()
    }
    update_payload: dict[str, Any] = {}
    for rule in rules:
        raw_connector_id = str(getattr(rule, "connector_id", "") or "").strip()
        if raw_connector_id and raw_connector_id != connector_id:
            continue
        source_field = _normalize_placeholder_key(getattr(rule, "source_field", ""))
        target_field = _normalize_placeholder_key(getattr(rule, "target_field", ""))
        if not source_field or not target_field:
            continue
        template = str(getattr(rule, "transform_template", "") or "").strip()
        source_value = render_template(template, normalized_ad_attrs) if template else normalized_ad_attrs.get(source_field, "")
        if source_value == "":
            continue
        current_value = normalized_source.get(target_field, "")
        sync_mode = normalize_sync_mode(getattr(rule, "sync_mode", "replace"))
        if sync_mode == "preserve" and current_value:
            continue
        if sync_mode == "fill_if_empty" and current_value:
            continue
        if current_value == source_value:
            continue
        update_payload[target_field] = source_value
    return update_payload


def build_ad_to_wecom_mapping_payload(
    ad_attributes: dict[str, Any],
    wecom_payload: dict[str, Any],
    *,
    connector_id: str,
    rules: Iterable[Any],
) -> dict[str, Any]:
    return build_ad_to_source_mapping_payload(
        ad_attributes,
        wecom_payload,
        connector_id=connector_id,
        rules=rules,
    )


def extract_manager_userids(user: SourceDirectoryUser) -> list[str]:
    payload = user.to_state_payload()
    manager_userids: list[str] = []
    direct_values = _first_payload_value(payload, MANAGER_FIELD_CANDIDATES)
    if direct_values:
        manager_userids.extend(
            value.strip()
            for value in re.split(r"[,;/\s]+", direct_values)
            if value and value.strip()
        )
    leader_entries = payload.get("leader_in_dept")
    if isinstance(leader_entries, list):
        for entry in leader_entries:
            if isinstance(entry, dict):
                candidate = str(entry.get("leader_userid") or entry.get("userid") or "").strip()
            else:
                candidate = str(entry or "").strip()
            if candidate:
                manager_userids.append(candidate)
    seen = set()
    normalized: list[str] = []
    for userid in manager_userids:
        lowered = userid.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(userid)
    return normalized
