from __future__ import annotations

import hashlib
import re
from datetime import datetime
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
MAPPING_ROLES = (
    "PRIMARY_ASSOCIATION",
    "SUGGESTION",
    "ATTRIBUTE_SYNC",
    "RELATIONSHIP",
    "LIFECYCLE",
    "DIRECTORY_ROUTING",
    "READ_ONLY_REFERENCE",
)
NULL_POLICIES = ("IGNORE", "PRESERVE_TARGET", "CLEAR", "USE_DEFAULT", "BLOCK")
SAFE_TRANSFORM_OPERATIONS = frozenset(
    {
        "trim",
        "uppercase",
        "lowercase",
        "normalize_whitespace",
        "normalize_mobile",
        "remove_country_code",
        "normalize_email",
        "enum_map",
        "regex_replace",
        "date_format",
        "join",
        "split",
        "default_value",
        "template",
        "department_lookup",
        "account_template",
    }
)
FORBIDDEN_GENERIC_AD_ATTRIBUTES = frozenset(
    {
        "objectguid",
        "objectsid",
        "distinguishedname",
        "whencreated",
        "whenchanged",
        "ntsecuritydescriptor",
        "unicodepwd",
        "useraccountcontrol",
        "member",
        "memberof",
        "manager",
        "proxyaddresses",
        "cn",
    }
)
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
    "employee_no",
    "employee_number",
    "job_number",
    "jobnumber",
    "staff_no",
    "staffno",
    "staff_id",
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
    "manager_account_id",
    "manager_source_user_id",
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


class MappingEvaluationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _mapping_value_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (list, tuple, set)):
        return "array"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, (int, float)):
        return "number"
    return "string"


def _is_empty_mapping_value(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == () or value == {}


def _validate_transform_type(value: Any, expected: Any, *, stage: str) -> None:
    normalized_expected = str(expected or "").strip().lower()
    if not normalized_expected or normalized_expected == "any":
        return
    actual = _mapping_value_type(value)
    if actual != normalized_expected:
        raise MappingEvaluationError(
            "transform_type_mismatch",
            f"transform {stage} type must be {normalized_expected}, got {actual}",
        )


def apply_transform_pipeline(
    value: Any,
    pipeline: Iterable[dict[str, Any]],
    *,
    context: dict[str, Any] | None = None,
) -> Any:
    result = value
    transform_context = dict(context or {})
    for raw_step in pipeline:
        step = dict(raw_step or {})
        operation = str(step.get("op") or step.get("type") or "").strip().lower()
        if operation not in SAFE_TRANSFORM_OPERATIONS:
            raise MappingEvaluationError(
                "unsafe_transform",
                f"unsupported or unsafe transform operation: {operation or 'missing'}",
            )
        _validate_transform_type(result, step.get("input_type"), stage="input")
        if operation == "trim":
            result = str(result or "").strip()
        elif operation == "uppercase":
            result = str(result or "").upper()
        elif operation == "lowercase":
            result = str(result or "").lower()
        elif operation == "normalize_whitespace":
            result = " ".join(str(result or "").split())
        elif operation in {"normalize_mobile", "remove_country_code"}:
            digits = re.sub(r"\D", "", str(result or ""))
            if digits.startswith("0086") and len(digits) > 11:
                digits = digits[4:]
            elif digits.startswith("86") and len(digits) > 11:
                digits = digits[2:]
            result = digits
        elif operation == "normalize_email":
            result = str(result or "").strip().casefold()
        elif operation == "enum_map":
            enum_values = dict(step.get("values") or step.get("map") or {})
            lookup_key = str(result or "")
            if lookup_key in enum_values:
                result = enum_values[lookup_key]
            elif bool(step.get("reject_unmapped", False)):
                raise MappingEvaluationError(
                    "enum_value_unmapped", f"enum value is not mapped: {lookup_key}"
                )
        elif operation == "regex_replace":
            pattern = str(step.get("pattern") or "")
            replacement = str(step.get("replacement") or step.get("replace") or "")
            if not pattern or len(pattern) > 200 or len(replacement) > 500:
                raise MappingEvaluationError(
                    "invalid_regex_transform", "regex transform is missing or too large"
                )
            try:
                result = re.sub(pattern, replacement, str(result or ""))
            except re.error as exc:
                raise MappingEvaluationError(
                    "invalid_regex_transform", f"invalid regex transform: {exc}"
                ) from exc
        elif operation == "date_format":
            source_format = str(step.get("source_format") or "").strip()
            target_format = str(step.get("target_format") or "%Y-%m-%d").strip()
            try:
                parsed = (
                    datetime.strptime(str(result), source_format)
                    if source_format
                    else datetime.fromisoformat(str(result).replace("Z", "+00:00"))
                )
                result = parsed.strftime(target_format)
            except (TypeError, ValueError) as exc:
                raise MappingEvaluationError(
                    "invalid_date_transform", "date value does not match the declared format"
                ) from exc
        elif operation == "join":
            if not isinstance(result, (list, tuple, set)):
                raise MappingEvaluationError(
                    "transform_type_mismatch", "join requires an array input"
                )
            result = str(step.get("separator") or ",").join(
                str(item).strip() for item in result if str(item).strip()
            )
        elif operation == "split":
            result = [
                item.strip()
                for item in str(result or "").split(str(step.get("separator") or ","))
                if item.strip()
            ]
        elif operation == "default_value":
            if _is_empty_mapping_value(result):
                result = step.get("value")
        elif operation in {"template", "account_template"}:
            template_context = {**transform_context, "value": result}
            result = render_template(str(step.get("template") or "{value}"), template_context)
        elif operation == "department_lookup":
            lookup = dict(step.get("values") or step.get("map") or {})
            result = lookup.get(str(result or ""), step.get("default", result))
        _validate_transform_type(result, step.get("output_type"), stage="output")
    return result


def _mapping_source_value(
    user: SourceDirectoryUser,
    field_name: str,
    context: dict[str, Any],
) -> Any:
    normalized_key = _normalize_placeholder_key(field_name)
    if normalized_key in context:
        return context[normalized_key]
    current: Any = user.to_state_payload()
    for segment in str(field_name or "").split("."):
        if not isinstance(current, dict) or segment not in current:
            return ""
        current = current[segment]
    return current


def _normalized_email_address(value: Any) -> str:
    normalized = str(value or "").strip().casefold()
    if (
        not normalized
        or "@" not in normalized
        or normalized.startswith("@")
        or normalized.endswith("@")
        or any(character.isspace() for character in normalized)
    ):
        return ""
    return normalized


def merge_proxy_addresses(
    current_addresses: Iterable[Any],
    *,
    primary_address: str,
    aliases: Iterable[Any] = (),
) -> list[str]:
    """Merge SMTP aliases while preserving non-SMTP values and one primary."""

    primary = _normalized_email_address(primary_address)
    if not primary:
        raise MappingEvaluationError(
            "proxy_primary_email_missing",
            "proxyAddresses requires a valid primary enterprise email",
        )
    merged = [f"SMTP:{primary}"]
    seen = {merged[0].casefold()}

    def add_alias(raw_value: Any) -> None:
        rendered = str(raw_value or "").strip()
        if not rendered:
            return
        if ":" in rendered:
            prefix, address = rendered.split(":", 1)
            if prefix.casefold() != "smtp":
                candidate = rendered
            else:
                normalized_address = _normalized_email_address(address)
                if not normalized_address or normalized_address == primary:
                    return
                candidate = f"smtp:{normalized_address}"
        else:
            normalized_address = _normalized_email_address(rendered)
            if not normalized_address or normalized_address == primary:
                return
            candidate = f"smtp:{normalized_address}"
        key = candidate.casefold()
        if key not in seen:
            seen.add(key)
            merged.append(candidate)

    for current in current_addresses:
        add_alias(current)
    for alias in aliases:
        add_alias(alias)
    return merged


def build_proxy_address_values(
    user: SourceDirectoryUser,
    *,
    connector_id: str,
    primary_email: str,
    rules: Iterable[Any],
    attribute_capabilities: dict[str, Any] | None = None,
    strict_capabilities: bool = False,
) -> tuple[str, list[str]] | None:
    """Evaluate governed proxyAddresses relationship rules without generic mapping."""

    normalized_capabilities = {
        str(key or "").strip().casefold(): value
        for key, value in dict(attribute_capabilities or {}).items()
    }
    context = build_template_context(
        user,
        connector_id=connector_id,
        email=primary_email,
    )
    aliases: list[str] = []
    matched = False
    for rule in rules:
        role = str(getattr(rule, "mapping_role", "") or "").strip().upper()
        target = str(getattr(rule, "target_field", "") or "").strip()
        if role != "RELATIONSHIP" or target.casefold() != "proxyaddresses":
            continue
        raw_connector_id = str(getattr(rule, "connector_id", "") or "").strip()
        ad_connector_id = str(
            getattr(rule, "ad_connector_id", "") or raw_connector_id
        ).strip()
        if ad_connector_id and ad_connector_id != "default" and ad_connector_id != connector_id:
            continue
        provider_scope = str(
            getattr(rule, "provider_scope", "*") or "*"
        ).strip().lower()
        if provider_scope not in {"*", str(user.provider_id or "").strip().lower()}:
            continue
        matched = True
        capability = normalized_capabilities.get("proxyaddresses")
        if strict_capabilities:
            if capability is None:
                raise MappingEvaluationError(
                    "ad_attribute_capability_missing",
                    "proxyAddresses is missing from the current AD capability catalog",
                )
            capability_value = (
                capability.get
                if isinstance(capability, dict)
                else lambda key, default=None: getattr(capability, key, default)
            )
            if (
                not bool(capability_value("schema_detected", False))
                or not bool(capability_value("is_writable", False))
                or str(capability_value("special_handler_type", "")).strip()
                != "proxy_addresses"
            ):
                raise MappingEvaluationError(
                    "proxy_addresses_capability_invalid",
                    "proxyAddresses is not writable through its dedicated handler",
                )
        source_field = str(
            getattr(rule, "raw_source_field_path", "")
            or getattr(rule, "canonical_source_field", "")
            or getattr(rule, "source_field", "")
            or ""
        ).strip()
        value = _mapping_source_value(user, source_field, context)
        pipeline = list(getattr(rule, "transform_pipeline", []) or [])
        if pipeline:
            value = apply_transform_pipeline(value, pipeline, context=context)
        if _is_empty_mapping_value(value):
            null_policy = str(
                getattr(rule, "null_policy", "PRESERVE_TARGET")
                or "PRESERVE_TARGET"
            ).strip().upper()
            if null_policy == "BLOCK":
                raise MappingEvaluationError(
                    "mapping_null_blocked",
                    f"empty source value is blocked by mapping policy: {source_field}",
                )
            continue
        values = value if isinstance(value, (list, tuple, set)) else [value]
        aliases.extend(
            normalized
            for normalized in (_normalized_email_address(item) for item in values)
            if normalized
        )
    if not matched:
        return None
    primary = _normalized_email_address(primary_email or user.enterprise_email or user.email)
    if not primary:
        raise MappingEvaluationError(
            "proxy_primary_email_missing",
            "proxyAddresses requires a valid primary enterprise email",
        )
    return primary, list(dict.fromkeys(alias for alias in aliases if alias != primary))

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
        digest = hashlib.sha256(
            f"{userid}:{employee_id}:{user.name}".encode("utf-8")
        ).hexdigest()
        hash_suffix = f"{int(digest[:8], 16) % 10000:04d}"
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

    employee_id = str(user.employee_id or template_context.get("employee_id", "")).strip()
    employee_number = str(user.employee_number or "").strip()
    if employee_id:
        add_candidate(
            "existing_ad_employee_id",
            employee_id,
            "Employee ID maps directly to an existing AD username",
            allow_existing_match=True,
            managed=False,
        )
    if employee_number:
        add_candidate(
            "existing_ad_employee_number",
            employee_number,
            "Employee number maps directly to an existing AD username",
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
    attribute_capabilities: dict[str, Any] | None = None,
    strict_capabilities: bool = False,
) -> dict[str, dict[str, Any]]:
    context = build_template_context(
        user,
        connector_id=connector_id,
        ad_username=ad_username,
        email=email,
        target_department=target_department,
    )
    mapped: dict[str, dict[str, Any]] = {}
    normalized_capabilities = {
        str(key or "").strip().casefold(): value
        for key, value in dict(attribute_capabilities or {}).items()
    }
    for rule in rules:
        raw_connector_id = str(getattr(rule, "connector_id", "") or "").strip()
        ad_connector_id = str(
            getattr(rule, "ad_connector_id", "") or raw_connector_id
        ).strip()
        if ad_connector_id and ad_connector_id != "default" and ad_connector_id != connector_id:
            continue
        provider_scope = str(getattr(rule, "provider_scope", "*") or "*").strip().lower()
        if provider_scope not in {"*", str(user.provider_id or "").strip().lower()}:
            continue
        mapping_role = str(
            getattr(rule, "mapping_role", "ATTRIBUTE_SYNC") or "ATTRIBUTE_SYNC"
        ).strip().upper()
        if mapping_role != "ATTRIBUTE_SYNC":
            continue
        source_field = str(
            getattr(rule, "raw_source_field_path", "")
            or getattr(rule, "canonical_source_field", "")
            or getattr(rule, "source_field", "")
            or ""
        ).strip()
        target_field = str(getattr(rule, "target_field", "") or "").strip()
        if not source_field or not target_field:
            continue
        if target_field.casefold() in FORBIDDEN_GENERIC_AD_ATTRIBUTES:
            raise MappingEvaluationError(
                "forbidden_ad_attribute",
                f"AD attribute requires a dedicated handler and cannot use scalar mapping: {target_field}",
            )
        capability = normalized_capabilities.get(target_field.casefold())
        if strict_capabilities:
            if capability is None:
                raise MappingEvaluationError(
                    "ad_attribute_capability_missing",
                    f"AD attribute is not present in the current capability catalog: {target_field}",
                )
            capability_value = (
                capability.get if isinstance(capability, dict) else lambda key, default=None: getattr(capability, key, default)
            )
            if not bool(capability_value("schema_detected", False)):
                raise MappingEvaluationError(
                    "ad_attribute_not_detected",
                    f"AD schema does not expose target attribute: {target_field}",
                )
            if bool(capability_value("is_read_only", True)) or not bool(
                capability_value("is_writable", False)
            ):
                raise MappingEvaluationError(
                    "ad_attribute_not_writable",
                    f"AD target attribute is not verified writable: {target_field}",
                )
            if bool(capability_value("requires_special_handler", False)):
                raise MappingEvaluationError(
                    "ad_attribute_special_handler_required",
                    f"AD attribute requires its dedicated handler: {target_field}",
                )
        template = str(getattr(rule, "transform_template", "") or "").strip()
        value = _mapping_source_value(user, source_field, context)
        pipeline = list(getattr(rule, "transform_pipeline", []) or [])
        if pipeline:
            value = apply_transform_pipeline(value, pipeline, context=context)
        elif template:
            value = render_template(template, {**context, "value": value})
        null_policy = str(
            getattr(rule, "null_policy", "PRESERVE_TARGET") or "PRESERVE_TARGET"
        ).strip().upper()
        if _is_empty_mapping_value(value):
            if null_policy in {"IGNORE", "PRESERVE_TARGET"}:
                continue
            if null_policy == "BLOCK":
                raise MappingEvaluationError(
                    "mapping_null_blocked",
                    f"empty source value is blocked by mapping policy: {source_field}",
                )
            if null_policy == "USE_DEFAULT":
                raise MappingEvaluationError(
                    "mapping_default_missing",
                    f"USE_DEFAULT requires a default_value transform: {source_field}",
                )
            if null_policy == "CLEAR":
                mapped[target_field] = {
                    "value": None,
                    "mode": "clear",
                    "clear": True,
                    "source_field": source_field,
                    "mapping_role": mapping_role,
                    "null_policy": null_policy,
                    "version": int(getattr(rule, "version", 1) or 1),
                }
                continue
        if capability is not None:
            capability_value = (
                capability.get if isinstance(capability, dict) else lambda key, default=None: getattr(capability, key, default)
            )
            target_is_multi = bool(capability_value("is_multi_value", False))
            source_is_multi = isinstance(value, (list, tuple, set))
            if source_is_multi and not target_is_multi:
                raise MappingEvaluationError(
                    "mapping_cardinality_mismatch",
                    f"multi-value source requires an explicit join before {target_field}",
                )
            if target_is_multi and not source_is_multi:
                raise MappingEvaluationError(
                    "mapping_cardinality_mismatch",
                    f"multi-value AD target requires an explicit split before {target_field}",
                )
        write_policy = str(
            getattr(rule, "write_policy", "") or ""
        ).strip().upper()
        mode = {
            "REPLACE": "replace",
            "FILL_IF_EMPTY": "fill_if_empty",
            "PRESERVE_TARGET": "preserve",
            "CREATE_ONLY": "create_only",
            "COMPARE_ONLY": "compare_only",
        }.get(write_policy, normalize_sync_mode(getattr(rule, "sync_mode", "replace")))
        mapped[target_field] = {
            "value": value,
            "mode": mode,
            "source_field": source_field,
            "template": template,
            "transform_pipeline": pipeline,
            "mapping_role": mapping_role,
            "null_policy": null_policy,
            "conflict_policy": str(
                getattr(rule, "conflict_policy", "REJECT_ON_CONFLICT")
                or "REJECT_ON_CONFLICT"
            ).strip().upper(),
            "write_policy": write_policy or mode.upper(),
            "version": int(getattr(rule, "version", 1) or 1),
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
