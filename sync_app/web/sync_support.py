from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from fastapi import FastAPI

from sync_app.core.models import SourceDirectoryUser
from sync_app.web.app_state import get_web_repositories
from sync_app.web.sync_conflict_support import SyncConflictSupportMixin
from sync_app.web.sync_data_quality_support import SyncDataQualitySupportMixin
from sync_app.web.sync_directory_support import SyncDirectorySupportMixin
from sync_app.web.request_support import RequestSupport


class SyncSupport(
    SyncConflictSupportMixin,
    SyncDirectorySupportMixin,
    SyncDataQualitySupportMixin,
):
    def __init__(
        self,
        *,
        app: FastAPI,
        logger: Any,
        request_support: RequestSupport,
        department_name_cache: dict[str, Any],
        to_bool: Callable[[Optional[str], bool], bool],
        validate_config_fn: Callable[..., Any],
        build_source_provider_fn: Callable[..., Any],
        build_target_provider_fn: Callable[..., Any],
        get_source_provider_display_name_fn: Callable[[str], str],
        is_protected_ad_account_name_fn: Callable[[str, list[str]], bool],
        recommend_conflict_resolution_fn: Callable[[Any], Optional[dict[str, Any]]],
        recommendation_requires_confirmation_fn: Callable[[dict[str, Any]], bool],
    ) -> None:
        self.app = app
        self.logger = logger
        self.request_support = request_support
        self.department_name_cache = department_name_cache
        self.to_bool = to_bool
        self.validate_config = validate_config_fn
        self.build_source_provider = build_source_provider_fn
        self.build_target_provider = build_target_provider_fn
        self.get_source_provider_display_name = get_source_provider_display_name_fn
        self.is_protected_ad_account_name = is_protected_ad_account_name_fn
        self.recommend_conflict_resolution = recommend_conflict_resolution_fn
        self.recommendation_requires_confirmation = recommendation_requires_confirmation_fn

    @staticmethod
    def _build_quality_issue(
        *,
        key: str,
        label: str,
        severity: str,
        description: str,
        action: str,
        samples: list[dict[str, str]],
        count: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        normalized_count = int(count if count is not None else len(samples))
        if normalized_count <= 0:
            return None
        return {
            "key": key,
            "label": label,
            "severity": severity,
            "count": normalized_count,
            "description": description,
            "action": action,
            "samples": list(samples[:5]),
        }

    @staticmethod
    def _format_quality_user_title(user: SourceDirectoryUser) -> str:
        normalized_source_user_id = str(user.source_user_id or "").strip()
        normalized_name = str(user.name or "").strip()
        if normalized_name and normalized_name != normalized_source_user_id:
            return f"{normalized_name} [{normalized_source_user_id}]"
        return normalized_source_user_id or normalized_name or "-"

    @staticmethod
    def _describe_naming_prerequisite_gap(preview: dict[str, Any], user: SourceDirectoryUser) -> str:
        strategy = str(preview.get("strategy") or "").strip().lower()
        template_context = dict(preview.get("template_context") or {})
        employee_id = str(template_context.get("employee_id") or "").strip()
        email_localpart = str(template_context.get("email_localpart") or "").strip()
        normalized_name = str(user.name or "").strip()
        if strategy == "email_localpart" and not email_localpart:
            return "Configured naming strategy depends on a work email local part, but the source email is blank."
        if strategy in {
            "employee_id",
            "pinyin_initials_employee_id",
            "pinyin_full_employee_id",
        } and not employee_id:
            return "Configured naming strategy depends on employee ID, but the source record does not expose one."
        if strategy in {
            "family_name_pinyin_given_initials",
            "family_name_pinyin_given_name_pinyin",
            "pinyin_initials_employee_id",
            "pinyin_full_employee_id",
        } and not normalized_name:
            return "Configured naming strategy depends on display name, but the source record is blank."
        if not preview.get("primary_candidate"):
            return "No managed username candidate could be generated from the current source record."
        return ""

    @staticmethod
    def _build_quality_repair_item(
        *,
        key: str,
        label: str,
        severity: str,
        title: str,
        detail: str,
        action: str,
        source_user_id: str = "",
        display_name: str = "",
        source_user_ids: Optional[list[str]] = None,
        connector_id: str = "",
        connector_name: str = "",
    ) -> dict[str, Any]:
        normalized_source_user_ids = [
            str(value or "").strip()
            for value in list(source_user_ids or [])
            if str(value or "").strip()
        ]
        return {
            "key": key,
            "label": label,
            "severity": severity,
            "title": str(title or "").strip() or "-",
            "detail": str(detail or "").strip(),
            "action": str(action or "").strip(),
            "source_user_id": str(source_user_id or "").strip(),
            "display_name": str(display_name or "").strip(),
            "source_user_ids": normalized_source_user_ids,
            "connector_id": str(connector_id or "").strip(),
            "connector_name": str(connector_name or "").strip(),
        }

    @staticmethod
    def _build_quality_samples(repair_items: list[dict[str, Any]], *, limit: int = 5) -> list[dict[str, str]]:
        return [
            {
                "title": str(item.get("title") or "-"),
                "detail": str(item.get("detail") or ""),
            }
            for item in list(repair_items[: max(int(limit or 0), 0)])
        ]

    def parse_bulk_exception_rules(self, raw_text: str) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        errors: list[str] = []
        reader = csv.reader(io.StringIO(raw_text or ""))
        for line_number, columns in enumerate(reader, start=1):
            trimmed_columns = [str(item or "").strip() for item in columns]
            if not any(trimmed_columns):
                continue
            if line_number == 1 and trimmed_columns[:2] == ["rule_type", "match_value"]:
                continue
            if len(trimmed_columns) < 2:
                errors.append(f"Line {line_number}: expected at least rule_type,match_value")
                continue
            if len(trimmed_columns) >= 8:
                rule_owner = trimmed_columns[2]
                effective_reason = trimmed_columns[3]
                notes = trimmed_columns[4]
                enabled_value = trimmed_columns[5] if len(trimmed_columns) >= 6 else "true"
                expires_at = trimmed_columns[6] if len(trimmed_columns) >= 7 else ""
                next_review_at = trimmed_columns[7] if len(trimmed_columns) >= 8 else ""
                is_once = self.to_bool(trimmed_columns[8], False) if len(trimmed_columns) >= 9 else False
            else:
                rule_owner = ""
                effective_reason = ""
                notes = trimmed_columns[2] if len(trimmed_columns) >= 3 else ""
                enabled_value = trimmed_columns[3] if len(trimmed_columns) >= 4 else "true"
                expires_at = trimmed_columns[4] if len(trimmed_columns) >= 5 else ""
                next_review_at = ""
                is_once = self.to_bool(trimmed_columns[5], False) if len(trimmed_columns) >= 6 else False
            rows.append(
                {
                    "line_number": line_number,
                    "rule_type": trimmed_columns[0],
                    "match_value": trimmed_columns[1],
                    "rule_owner": rule_owner,
                    "effective_reason": effective_reason,
                    "notes": notes,
                    "is_enabled": self.to_bool(enabled_value, True),
                    "expires_at": expires_at,
                    "next_review_at": next_review_at,
                    "is_once": is_once,
                }
            )
        return rows, errors

    def normalize_optional_datetime_input(self, value: str) -> str:
        normalized_value = str(value or "").strip()
        if not normalized_value:
            return ""
        candidate = normalized_value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise ValueError("Invalid date/time format. Use ISO 8601 or datetime-local input.") from exc
        if parsed.tzinfo is None:
            local_tz = datetime.now().astimezone().tzinfo or timezone.utc
            parsed = parsed.replace(tzinfo=local_tz)
        return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")

    def enqueue_replay_request(
        self,
        *,
        app: FastAPI,
        request_type: str,
        requested_by: str,
        org_id: str,
        target_scope: str = "full",
        target_id: str = "",
        trigger_reason: str = "",
        payload: Optional[dict[str, Any]] = None,
        execution_mode: str = "apply",
    ) -> Optional[int]:
        repositories = get_web_repositories(app)
        if not repositories.settings_repo.get_bool("automatic_replay_enabled", False, org_id=org_id):
            return None
        return repositories.replay_request_repo.enqueue_request(
            request_type=request_type,
            execution_mode=execution_mode,
            requested_by=requested_by,
            org_id=org_id,
            target_scope=target_scope,
            target_id=target_id,
            trigger_reason=trigger_reason,
            payload=payload,
        )
