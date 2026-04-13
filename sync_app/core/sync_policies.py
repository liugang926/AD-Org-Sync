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
USERNAME_STRATEGIES = (
    "userid",
    "email_localpart",
    "employee_id",
    "pinyin_initials_employee_id",
    "pinyin_full_employee_id",
    "family_name_pinyin_given_initials",
    "family_name_pinyin_given_name_pinyin",
    "custom_template",
)
USERNAME_COLLISION_POLICIES = (
    "append_employee_id",
    "append_userid",
    "append_numeric_counter",
    "append_2digit_counter",
    "append_3digit_counter",
    "append_hash",
    "custom_template",
)
AD_USERNAME_MAX_LENGTH = 20

COMPOUND_CHINESE_SURNAMES = (
    "欧阳",
    "司马",
    "上官",
    "诸葛",
    "司徒",
    "夏侯",
    "皇甫",
    "尉迟",
    "公孙",
    "长孙",
    "慕容",
    "令狐",
    "宇文",
    "轩辕",
)

EMPLOYEE_ID_FIELD_CANDIDATES = (
    "employee_id",
    "employeeid",
    "job_number",
    "jobnumber",
    "staff_no",
    "staffno",
    "workcode",
    "work_code",
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
    ascii_tokens = [segment.lower() for segment in re.findall(r"[A-Za-z0-9]+", text) if segment]
    if ascii_tokens and re.fullmatch(r"[A-Za-z0-9\s._-]+", text):
        return "".join(segment[:1] for segment in ascii_tokens if segment)
    if lazy_pinyin is not None:
        try:
            return "".join(item[:1] for item in lazy_pinyin(text) if item).lower()
        except Exception:
            pass
    return "".join(segment[:1] for segment in ascii_tokens if segment)


def _romanize_text(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    ascii_tokens = [segment.lower() for segment in re.findall(r"[A-Za-z0-9]+", text) if segment]
    if ascii_tokens and re.fullmatch(r"[A-Za-z0-9\s._-]+", text):
        return ascii_tokens
    if lazy_pinyin is not None:
        try:
            return [segment.lower() for segment in lazy_pinyin(text) if segment]
        except Exception:
            pass
    return ascii_tokens


def _split_person_name(value: str) -> tuple[str, str]:
    text = str(value or "").strip()
    if not text:
        return "", ""
    ascii_tokens = [segment for segment in re.split(r"[\s._-]+", text) if segment]
    if ascii_tokens and all(re.fullmatch(r"[A-Za-z0-9]+", token) for token in ascii_tokens):
        if len(ascii_tokens) == 1:
            return ascii_tokens[0], ""
        return ascii_tokens[-1], "".join(ascii_tokens[:-1])
    for surname in COMPOUND_CHINESE_SURNAMES:
        if text.startswith(surname) and len(text) > len(surname):
            return surname, text[len(surname) :]
    if len(text) == 1:
        return text, ""
    return text[:1], text[1:]


def _normalize_username_candidate(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "", str(value or "").strip())
    if not normalized:
        return ""
    return normalized[:AD_USERNAME_MAX_LENGTH]


def _with_username_suffix(base: str, suffix: str) -> str:
    normalized_base = _normalize_username_candidate(base)
    normalized_suffix = _normalize_username_candidate(suffix)
    if not normalized_suffix:
        return normalized_base
    if normalized_base and (
        normalized_base == normalized_suffix
        or normalized_base.endswith(normalized_suffix)
    ):
        return normalized_base[:AD_USERNAME_MAX_LENGTH]
    if not normalized_base:
        return normalized_suffix
    base_budget = max(AD_USERNAME_MAX_LENGTH - len(normalized_suffix), 0)
    return f"{normalized_base[:base_budget]}{normalized_suffix}"[:AD_USERNAME_MAX_LENGTH]


def normalize_username_strategy(value: str | None, *, default: str = "custom_template") -> str:
    candidate = str(value or "").strip().lower()
    if candidate in USERNAME_STRATEGIES:
        return candidate
    return default


def normalize_username_collision_policy(value: str | None, *, default: str = "append_employee_id") -> str:
    candidate = str(value or "").strip().lower()
    if candidate in USERNAME_COLLISION_POLICIES:
        return candidate
    return default


def render_username_collision_template(
    template: str,
    *,
    base_username: str,
    employee_id: str,
    userid: str,
    counter: int,
) -> str:
    template_context = {
        "base": str(base_username or "").strip(),
        "employee_id": str(employee_id or "").strip(),
        "userid": str(userid or "").strip(),
        "counter": str(counter),
        "counter2": f"{counter:02d}",
        "counter3": f"{counter:03d}",
    }
    return render_template(template, template_context)


def resolve_username_template(username_strategy: str | None, username_template: str | None = "") -> str:
    strategy = normalize_username_strategy(username_strategy)
    custom_template = str(username_template or "").strip()
    strategy_templates = {
        "userid": "{userid}",
        "email_localpart": "{email_localpart}",
        "employee_id": "{employee_id}",
        "pinyin_initials_employee_id": "{pinyin_initials}{employee_id}",
        "pinyin_full_employee_id": "{pinyin_full}{employee_id}",
        "family_name_pinyin_given_initials": "{family_name_pinyin}{given_initials}",
        "family_name_pinyin_given_name_pinyin": "{family_name_pinyin}{given_name_pinyin}",
        "custom_template": custom_template,
    }
    return strategy_templates.get(strategy, custom_template).strip()


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
    family_name, given_name = _split_person_name(user.name)
    family_name_pinyin = "".join(_romanize_text(family_name))
    given_name_pinyin = "".join(_romanize_text(given_name))
    context["pinyin_full"] = "".join(_romanize_text(user.name))
    context["family_name"] = family_name
    context["given_name"] = given_name
    context["family_name_pinyin"] = family_name_pinyin
    context["given_name_pinyin"] = given_name_pinyin
    context["family_initial"] = family_name_pinyin[:1]
    context["given_initials"] = "".join(segment[:1] for segment in _romanize_text(given_name) if segment)
    context["name_ascii"] = "".join(_romanize_text(user.name))
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

def build_managed_username_candidates(
    user: SourceDirectoryUser,
    *,
    username_strategy: str = "custom_template",
    username_template: str = "",
    username_collision_policy: str = "append_employee_id",
    username_collision_template: str = "",
) -> list[dict[str, str]]:
    template_context = build_template_context(user)
    strategy = normalize_username_strategy(username_strategy)
    collision_policy = normalize_username_collision_policy(username_collision_policy)
    resolved_template = resolve_username_template(strategy, username_template)
    employee_id = template_context.get("employee_id", "")
    userid = template_context.get("userid", "")
    email_localpart = template_context.get("email_localpart", "")
    base_candidate = _normalize_username_candidate(render_template(resolved_template, template_context))

    candidates: list[dict[str, str]] = []
    seen = set()

    def add_candidate(rule_name: str, username: str, explanation: str) -> None:
        normalized = _normalize_username_candidate(username)
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
                "managed": True,
                "allow_existing_match": False,
            }
        )

    if base_candidate:
        add_candidate(
            "managed_username_primary",
            base_candidate,
            "Primary managed username candidate generated from the selected naming strategy",
        )

    if collision_policy == "append_employee_id" and employee_id:
        add_candidate(
            "managed_username_employee_id_suffix",
            _with_username_suffix(base_candidate or userid or email_localpart, employee_id),
            "Fallback candidate appends employee ID to separate users with the same base name",
        )
    if collision_policy == "append_userid" and userid:
        add_candidate(
            "managed_username_userid_suffix",
            _with_username_suffix(base_candidate or email_localpart, userid),
            "Fallback candidate appends source user ID to avoid same-name collisions",
        )

    if collision_policy == "append_numeric_counter":
        for number in range(2, 6):
            add_candidate(
                f"managed_username_numeric_suffix_{number}",
                _with_username_suffix(base_candidate or userid or email_localpart, str(number)),
                "Fallback candidate appends a short numeric suffix",
            )
    if collision_policy == "append_2digit_counter":
        for number in range(1, 21):
            add_candidate(
                f"managed_username_2digit_suffix_{number:02d}",
                _with_username_suffix(base_candidate or userid or email_localpart, f"{number:02d}"),
                "Fallback candidate appends a stable two-digit sequence suffix",
            )
    if collision_policy == "append_3digit_counter":
        for number in range(1, 51):
            add_candidate(
                f"managed_username_3digit_suffix_{number:03d}",
                _with_username_suffix(base_candidate or userid or email_localpart, f"{number:03d}"),
                "Fallback candidate appends a stable three-digit sequence suffix",
            )
    if collision_policy == "append_hash":
        hash_suffix = f"{abs(hash(f'{userid}:{employee_id}:{user.name}')) % 10000:04d}"
        add_candidate(
            "managed_username_hash_suffix",
            _with_username_suffix(base_candidate or userid or email_localpart, hash_suffix),
            "Fallback candidate appends a deterministic short hash suffix",
        )
    if collision_policy == "custom_template":
        normalized_template = str(username_collision_template or "").strip()
        if normalized_template:
            for number in range(1, 51):
                add_candidate(
                    f"managed_username_custom_suffix_{number}",
                    render_username_collision_template(
                        normalized_template,
                        base_username=base_candidate or userid or email_localpart,
                        employee_id=employee_id,
                        userid=userid,
                        counter=number,
                    ),
                    "Fallback candidate uses the custom collision template for enterprise naming rules",
                )
    if employee_id:
        add_candidate(
            "managed_username_employee_id",
            employee_id,
            "Fallback candidate uses employee ID directly for organizations that require unique staff numbers",
        )
    if userid:
        add_candidate(
            "managed_username_userid",
            userid,
            "Fallback candidate uses the source user ID directly",
        )
    if email_localpart:
        add_candidate(
            "managed_username_email_localpart",
            email_localpart,
            "Fallback candidate uses the source email local part",
        )
    return candidates


def build_identity_candidates(
    user: SourceDirectoryUser,
    *,
    username_template: str = "",
    username_strategy: str = "custom_template",
    username_collision_policy: str = "append_employee_id",
    username_collision_template: str = "",
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, bool]] = set()
    template_context = build_template_context(user)

    def add_candidate(
        rule_name: str,
        username: str,
        explanation: str,
        *,
        allow_existing_match: bool,
        managed: bool,
    ) -> None:
        normalized = _normalize_username_candidate(username)
        if not normalized:
            return
        lowered = normalized.lower()
        candidate_key = (lowered, managed)
        if candidate_key in seen:
            return
        seen.add(candidate_key)
        candidates.append(
            {
                "rule": rule_name,
                "username": normalized,
                "explanation": explanation,
                "allow_existing_match": allow_existing_match,
                "managed": managed,
            }
        )

    add_candidate(
        "existing_ad_userid",
        user.userid,
        "Source user ID maps directly to an existing AD username",
        allow_existing_match=True,
        managed=False,
    )
    employee_id = template_context.get("employee_id", "")
    if employee_id:
        add_candidate(
            "existing_ad_employee_id",
            employee_id,
            "Employee ID maps directly to an existing AD username",
            allow_existing_match=True,
            managed=False,
        )
    email = (user.email or user.raw_payload.get("email") or "").strip()
    if "@" in email:
        localpart = email.split("@", 1)[0].strip()
        add_candidate(
            "existing_ad_email_localpart",
            localpart,
            "Source email local part maps to an existing AD username",
            allow_existing_match=True,
            managed=False,
        )

    for managed_candidate in build_managed_username_candidates(
        user,
        username_strategy=username_strategy,
        username_template=username_template,
        username_collision_policy=username_collision_policy,
        username_collision_template=username_collision_template,
    ):
        add_candidate(
            str(managed_candidate["rule"]),
            str(managed_candidate["username"]),
            str(managed_candidate["explanation"]),
            allow_existing_match=False,
            managed=True,
        )

    if not any(candidate.get("managed") for candidate in candidates):
        add_candidate(
            "managed_username_fallback_userid",
            user.userid,
            "Fallback to source user ID because no managed naming candidate could be generated",
            allow_existing_match=False,
            managed=True,
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
