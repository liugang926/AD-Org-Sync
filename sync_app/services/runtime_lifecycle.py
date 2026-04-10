from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from sync_app.core.models import SourceDirectoryUser


def normalize_lifecycle_field_name(value: Optional[str]) -> str:
    normalized = "".join(
        char.lower() if char.isalnum() else "_"
        for char in str(value or "").strip()
    )
    return normalized.strip("_")


def get_payload_field_value(payload: dict[str, Any], field_name: str) -> str:
    normalized_field_name = normalize_lifecycle_field_name(field_name)
    if not normalized_field_name:
        return ""
    for key, value in (payload or {}).items():
        if normalize_lifecycle_field_name(str(key)) != normalized_field_name:
            continue
        if value in (None, ""):
            return ""
        if isinstance(value, (list, tuple)):
            return ",".join(str(item).strip() for item in value if str(item).strip())
        return str(value).strip()
    return ""


def parse_lifecycle_datetime(value: str) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    normalized_text = text.replace("Z", "+00:00")
    for candidate in (
        normalized_text,
        normalized_text.replace("/", "-"),
    ):
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def build_user_lifecycle_profile(
    user: SourceDirectoryUser,
    *,
    future_onboarding_start_field: str,
    contractor_end_field: str,
    lifecycle_employment_type_field: str,
    lifecycle_sponsor_field: str,
    contractor_type_values: set[str],
) -> dict[str, Any]:
    payload = user.to_state_payload()
    start_value = get_payload_field_value(payload, future_onboarding_start_field)
    end_value = get_payload_field_value(payload, contractor_end_field)
    employment_type = get_payload_field_value(payload, lifecycle_employment_type_field)
    sponsor_userid = get_payload_field_value(payload, lifecycle_sponsor_field)
    start_at = parse_lifecycle_datetime(start_value)
    end_at = parse_lifecycle_datetime(end_value)
    normalized_employment_type = str(employment_type or "").strip().lower()
    return {
        "start_field": future_onboarding_start_field,
        "start_value": start_value,
        "start_at": start_at,
        "end_field": contractor_end_field,
        "end_value": end_value,
        "end_at": end_at,
        "employment_type_field": lifecycle_employment_type_field,
        "employment_type": str(employment_type or "").strip(),
        "normalized_employment_type": normalized_employment_type,
        "is_contractor": bool(normalized_employment_type and normalized_employment_type in contractor_type_values),
        "sponsor_field": lifecycle_sponsor_field,
        "sponsor_userid": sponsor_userid,
    }


def serialize_lifecycle_profile(profile: Optional[dict[str, Any]]) -> dict[str, Any]:
    serialized: dict[str, Any] = {}
    for key, value in dict(profile or {}).items():
        if isinstance(value, datetime):
            serialized[key] = value.astimezone(timezone.utc).isoformat(timespec="seconds")
        else:
            serialized[key] = value
    return serialized
