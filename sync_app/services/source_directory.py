from __future__ import annotations

import re
from collections import Counter
from typing import Any

from sync_app.core.fingerprints import fingerprint_json
from sync_app.core.models import SourceDirectoryUser
from sync_app.core.sync_policies import (
    AD_USERNAME_MAX_LENGTH,
    build_identity_candidates,
    build_managed_username_candidates,
    build_template_context,
    normalize_username_collision_policy,
    normalize_username_strategy,
    render_template,
    resolve_username_template,
)
from sync_app.storage.repositories.source_directory import SourceDirectoryRepository
from sync_app.storage.repositories.field_registry import (
    canonical_field_for_path,
    category_for_canonical_field,
    is_secret_field,
    is_sensitive_field,
)


# These describe the normalized directory model, not provider-specific raw
# field names. A discovered raw field receives one of these labels only when
# its values in the current snapshot feed the matching normalized field.
NORMALIZED_FIELD_LABELS = (
    ("employee_id", "Employee ID"),
    ("source_user_id", "Platform User ID"),
    ("email", "Email"),
    ("display_name", "Display Name"),
    ("position", "Position"),
    ("primary_department_id", "Primary Department ID"),
    ("account_status", "Status"),
    ("is_active", "Status"),
    ("provider_id", "Provider"),
)


def mask_mobile(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) <= 4:
        return "*" * len(text)
    if len(text) <= 7:
        return f"{text[:2]}{'*' * (len(text) - 4)}{text[-2:]}"
    return f"{text[:3]}{'*' * (len(text) - 7)}{text[-4:]}"


def bounded_scalar_payload(payload: dict[str, Any], *, max_fields: int = 50) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in _flatten_catalog_payload(payload).items():
        if len(result) >= max_fields:
            break
        name = str(key or "").strip()
        if not name or is_secret_field(name) or is_sensitive_field(name):
            continue
        if isinstance(value, (dict, list, tuple, set)) or value is None:
            continue
        rendered = str(value)
        if len(rendered) > 256:
            rendered = rendered[:256]
        result[name] = rendered
    return result


def _flatten_catalog_payload(
    payload: dict[str, Any],
    *,
    prefix: str = "",
    max_fields: int = 200,
    max_depth: int = 4,
) -> dict[str, Any]:
    result: dict[str, Any] = {}

    def visit(value: Any, path: str, depth: int) -> None:
        if len(result) >= max_fields or not path or is_secret_field(path):
            return
        if isinstance(value, dict) and depth < max_depth:
            for child_key in sorted(value):
                child_name = str(child_key or "").strip()
                if child_name:
                    visit(value[child_key], f"{path}.{child_name}" if path else child_name, depth + 1)
            return
        if isinstance(value, (list, tuple, set)):
            scalar_values = [item for item in value if not isinstance(item, (dict, list, tuple, set))]
            result[path] = scalar_values if len(scalar_values) == len(value) else list(value)
            return
        if value is not None:
            result[path] = value

    for key in sorted(payload):
        name = str(key or "").strip()
        if name:
            visit(payload[key], f"{prefix}.{name}" if prefix else name, 0)
    return result


def _field_data_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, (list, tuple, set)):
        return "array"
    text = str(value or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:[T ][^ ]+)?", text):
        return "date"
    return "string"


def _comparable_scalar(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value).strip()


def _catalog_field_label(
    field_name: str,
    *,
    coverage_count: int,
    semantic_matches: Counter[str],
) -> str:
    normalized_label = dict(NORMALIZED_FIELD_LABELS).get(field_name)
    if normalized_label:
        return normalized_label
    for semantic_name, label in NORMALIZED_FIELD_LABELS:
        if semantic_matches.get(semantic_name, 0) == coverage_count:
            return label
    return field_name.replace("_", " ").title()


class SourceDirectoryService:
    def __init__(self, repository: SourceDirectoryRepository, *, logger=None) -> None:
        self.repository = repository
        self.logger = logger

    @staticmethod
    def _department_rows(departments: list[Any]) -> list[dict[str, Any]]:
        by_id = {int(item.department_id): item for item in departments}
        result: list[dict[str, Any]] = []
        for department in departments:
            path_ids: list[int] = []
            path_names: list[str] = []
            current = department
            seen: set[int] = set()
            while current and int(current.department_id) not in seen:
                seen.add(int(current.department_id))
                path_ids.append(int(current.department_id))
                path_names.append(str(current.name or ""))
                current = by_id.get(int(current.parent_id))
            path_ids.reverse()
            path_names.reverse()
            department.set_hierarchy(path_names, path_ids)
            result.append(
                {
                    "source_department_id": str(department.department_id),
                    "name": department.name,
                    "parent_department_id": str(department.parent_id),
                    "path_ids": [str(value) for value in path_ids],
                    "path_names": path_names,
                }
            )
        return result

    @staticmethod
    def _merge_user(existing: SourceDirectoryUser, incoming: SourceDirectoryUser) -> None:
        existing.merge_payload(incoming.to_state_payload())
        existing.departments = sorted(set(existing.departments) | set(incoming.departments))

    def refresh(
        self,
        *,
        org_id: str,
        provider_id: str,
        provider: Any,
        connector_id: str = "default",
        created_by: str = "",
        ttl_minutes: int = 60,
    ) -> dict[str, Any]:
        snapshot_id = self.repository.start_refresh(
            org_id=org_id,
            provider_id=provider_id,
            connector_id=connector_id,
            created_by=created_by,
        )
        try:
            departments = provider.list_departments()
            if not departments:
                raise RuntimeError("No visible departments were returned. Check the application's contact data scope.")
            department_rows = self._department_rows(departments)
            department_names = {str(row["source_department_id"]): row["name"] for row in department_rows}
            users_by_id: dict[str, SourceDirectoryUser] = {}
            warnings: list[str] = []
            for department in departments:
                try:
                    users = provider.list_department_users(int(department.department_id))
                except Exception as exc:
                    warnings.append(f"{department.name or department.department_id}: {str(exc)[:160]}")
                    continue
                for user in users:
                    if not user.source_user_id:
                        continue
                    existing = users_by_id.get(user.source_user_id)
                    if existing is None:
                        users_by_id[user.source_user_id] = user
                    else:
                        self._merge_user(existing, user)
            if not users_by_id:
                detail = " No department user page could be read." if warnings else ""
                raise RuntimeError(f"No visible users were returned.{detail}")

            normalized_rows: list[dict[str, Any]] = []
            field_coverage: Counter[str] = Counter()
            field_samples: dict[str, list[str]] = {}
            field_semantic_matches: dict[str, Counter[str]] = {}
            field_types: dict[str, set[str]] = {}
            multi_value_fields: set[str] = set()
            for source_user_id in sorted(users_by_id):
                user = users_by_id[source_user_id]
                try:
                    detail = provider.get_user_detail(source_user_id)
                except Exception as exc:
                    warnings.append(f"user {source_user_id}: {str(exc)[:160]}")
                    detail = {}
                if detail:
                    user.merge_payload(detail)
                department_ids = [str(value) for value in user.departments]
                names = [department_names[value] for value in department_ids if value in department_names]
                user.department_names = names
                if user.primary_department_id is None and user.departments:
                    user.primary_department_id = user.departments[0]
                safe_payload = bounded_scalar_payload(user.to_state_payload())
                normalized_scalar_fields = {
                    "source_user_id": user.source_user_id,
                    "display_name": user.name,
                    "employee_id": user.employee_id,
                    "email": user.email,
                    "position": user.position,
                    "primary_department_id": user.primary_department_id,
                    "account_status": user.account_status,
                    "is_active": user.is_active,
                    "provider_id": user.provider_id,
                    "platform_union_id": user.platform_union_id,
                    "platform_open_id": user.platform_open_id,
                    "employee_number": user.employee_number,
                    "given_name": user.given_name,
                    "family_name": user.family_name,
                    "enterprise_email": user.enterprise_email,
                    "mobile": user.mobile,
                    "telephone": user.telephone,
                    "job_title": user.job_title,
                    "employee_type": user.employee_type,
                    "employment_status": user.employment_status,
                    "manager_account_id": user.manager_account_id,
                    "work_station": user.work_station,
                    "city": user.city,
                    "hire_date": user.hire_date,
                    "leave_date": user.leave_date,
                    "department_ids": department_ids,
                }
                catalog_payload = _flatten_catalog_payload(user.to_state_payload())
                for name, value in {**catalog_payload, **normalized_scalar_fields}.items():
                    if value in (None, "", [], {}):
                        continue
                    field_coverage[name] += 1
                    field_types.setdefault(name, set()).add(_field_data_type(value))
                    if isinstance(value, (list, tuple, set)):
                        multi_value_fields.add(name)
                        sample_values = list(value)
                    else:
                        sample_values = [value]
                    samples = field_samples.setdefault(name, [])
                    for sample_value in sample_values:
                        masked_sample = self._mask_field_sample(name, str(sample_value))
                        if masked_sample not in samples and len(samples) < 3:
                            samples.append(masked_sample)
                    comparable_value = _comparable_scalar(value)
                    semantic_matches = field_semantic_matches.setdefault(name, Counter())
                    for semantic_name, _label in NORMALIZED_FIELD_LABELS:
                        semantic_value = _comparable_scalar(normalized_scalar_fields.get(semantic_name))
                        if comparable_value and comparable_value == semantic_value:
                            semantic_matches[semantic_name] += 1
                normalized_rows.append(
                    {
                        "source_user_id": user.source_user_id,
                        "display_name": user.name,
                        "employee_id": user.employee_id,
                        "email": user.email,
                        "mobile_masked": mask_mobile(user.mobile),
                        "position": user.position,
                        "department_ids": department_ids,
                        "department_names": names,
                        "primary_department_id": str(user.primary_department_id or ""),
                        "account_status": user.account_status,
                        "is_active": user.is_active,
                        "raw_payload": safe_payload,
                        "search_text": " ".join(
                            [user.source_user_id, user.name, user.employee_id, user.email, user.position, *names]
                        ),
                    }
                )
            fields = [
                {
                    "name": name,
                    "raw_field_path": name,
                    "raw_field_name": name.rsplit(".", 1)[-1],
                    "label": _catalog_field_label(
                        name,
                        coverage_count=field_coverage[name],
                        semantic_matches=field_semantic_matches.get(name, Counter()),
                    ),
                    "canonical_field_key": canonical_field_for_path(name),
                    "category": category_for_canonical_field(canonical_field_for_path(name)),
                    "data_type": (
                        next(iter(field_types.get(name, {"string"})))
                        if len(field_types.get(name, {"string"})) == 1
                        else "mixed"
                    ),
                    "is_multi_value": name in multi_value_fields,
                    "is_sensitive": is_sensitive_field(name),
                    "is_identifier_candidate": canonical_field_for_path(name) in {
                        "platform_account_id", "platform_union_id", "platform_open_id",
                        "source_user_id", "employee_id", "employee_number", "personnel_number",
                    },
                    "is_custom": not bool(canonical_field_for_path(name)),
                    "availability_status": (
                        "type_conflict" if len(field_types.get(name, set())) > 1 else "available"
                    ),
                    "permission_status": "granted",
                    "coverage": field_coverage[name],
                    "samples": field_samples.get(name, []),
                }
                for name in sorted(field_coverage)
            ]
            fingerprint = fingerprint_json(
                {
                    "org_id": org_id,
                    "provider_id": provider_id,
                    "departments": department_rows,
                    "users": [
                        {
                            key: row[key]
                            for key in (
                                "source_user_id", "display_name", "employee_id", "email", "position",
                                "department_ids", "primary_department_id", "account_status", "is_active",
                            )
                        }
                        for row in normalized_rows
                    ],
                },
                namespace="source-directory-snapshot",
            )
            warning_summary = "; ".join(warnings[:5])
            if len(warnings) > 5:
                warning_summary += f"; and {len(warnings) - 5} more partial failures"
            self.repository.replace_snapshot(
                snapshot_id,
                departments=department_rows,
                users=normalized_rows,
                fields=fields,
                fingerprint=fingerprint,
                warning_summary=warning_summary,
                ttl_minutes=ttl_minutes,
            )
            previous_scope = self.repository.get_scope_selection(
                org_id=org_id,
                provider_id=provider_id,
                connector_id=connector_id,
            )
            if previous_scope:
                self.repository.save_scope_selection(
                    org_id=org_id,
                    provider_id=provider_id,
                    connector_id=connector_id,
                    scope_type=previous_scope["scope_type"],
                    selected_department_ids=previous_scope["selected_department_ids"],
                    selected_source_user_ids=previous_scope["selected_source_user_ids"],
                    username_strategy=previous_scope["username_strategy"],
                    username_template=previous_scope["username_template"],
                    source_field=previous_scope["source_field"],
                    snapshot_id=snapshot_id,
                    requested_by=created_by or previous_scope.get("requested_by") or "",
                )
            return dict(self.repository.get_snapshot(snapshot_id, org_id=org_id))
        except Exception as exc:
            self.repository.fail_refresh(snapshot_id, str(exc))
            raise

    def test_connection(self, provider: Any) -> dict[str, Any]:
        departments = provider.list_departments()
        sample_users: list[SourceDirectoryUser] = []
        for department in departments[:3]:
            sample_users = provider.list_department_users(int(department.department_id))
            if sample_users:
                break
        if not departments or not sample_users:
            raise RuntimeError(
                "The token is valid, but no readable department/user page was returned. Check contact permissions and application visibility."
            )
        fields: set[str] = set()
        samples: list[dict[str, Any]] = []
        for user in sample_users[:3]:
            payload = user.to_state_payload()
            fields.update(name for name, value in payload.items() if value not in (None, "", [], {}))
            samples.append(
                {
                    "source_user_id": user.source_user_id,
                    "display_name": user.name,
                    "employee_id": user.employee_id,
                    "email": self._mask_email(user.email),
                    "mobile": mask_mobile(user.mobile),
                }
            )
        return {
            "ok": True,
            "provider_id": provider.provider_id,
            "department_count": len(departments),
            "sample_user_count": len(sample_users),
            "available_fields": sorted(fields),
            "samples": samples,
            "employee_id_available": any(user.employee_id for user in sample_users),
        }

    def build_mapping_quality_report(
        self,
        *,
        snapshot_id: int,
        org_id: str,
        provider_id: str,
        username_strategy: str,
        username_template: str = "",
        source_field: str = "source_user_id",
    ) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = self.repository.list_users(
                snapshot_id,
                org_id=org_id,
                provider_id=provider_id,
                limit=200,
                offset=offset,
            )
            rows.extend(page["items"])
            offset += len(page["items"])
            if offset >= int(page["total"]) or not page["items"]:
                break
        previews: dict[str, dict[str, Any]] = {}
        username_counts: Counter[str] = Counter()
        employee_counts: Counter[str] = Counter()
        for row in rows:
            preview = self.preview_username(
                row,
                username_strategy=username_strategy,
                username_template=username_template,
                source_field=source_field,
            )
            previews[row["source_user_id"]] = preview
            if preview["username"]:
                username_counts[preview["username"].lower()] += 1
            if str(row.get("employee_id") or "").strip():
                employee_counts[str(row["employee_id"]).strip().lower()] += 1
        issues_by_user: dict[str, list[str]] = {}
        for row in rows:
            user_id = row["source_user_id"]
            risks = list(previews[user_id]["risks"])
            employee_id = str(row.get("employee_id") or "").strip().lower()
            username = str(previews[user_id]["username"] or "").strip().lower()
            if employee_id and employee_counts[employee_id] > 1:
                risks.append("duplicate_employee_id")
            if username and username_counts[username] > 1:
                risks.append("normalized_username_collision")
            if not row.get("department_ids"):
                risks.append("missing_department")
            if risks:
                issues_by_user[user_id] = sorted(set(risks))
        covered = sum(1 for preview in previews.values() if preview["username"])
        return {
            "total_users": len(rows),
            "mapping_covered_users": covered,
            "mapping_coverage_percent": round((covered / len(rows) * 100.0) if rows else 0.0, 2),
            "normalized_username_collision_count": sum(count for count in username_counts.values() if count > 1),
            "duplicate_employee_id_count": sum(count for count in employee_counts.values() if count > 1),
            "issue_user_count": len(issues_by_user),
            "issues_by_user": issues_by_user,
        }

    @staticmethod
    def _mask_email(value: str) -> str:
        text = str(value or "").strip()
        if "@" not in text:
            return ""
        local, domain = text.split("@", 1)
        return f"{local[:1]}***@{domain}"

    @classmethod
    def _mask_field_sample(cls, field_name: str, value: str) -> str:
        text = str(value or "")[:80]
        lowered = str(field_name or "").lower()
        if "email" in lowered and "@" in text:
            return cls._mask_email(text)
        if lowered in {"display_name", "name"}:
            return text[:1] + ("***" if len(text) > 1 else "")
        if len(text) <= 2:
            return "*" * len(text)
        if len(text) <= 6:
            return f"{text[:1]}***{text[-1:]}"
        return f"{text[:2]}***{text[-2:]}"

    @classmethod
    def preview_username(
        cls,
        row: dict[str, Any],
        *,
        username_strategy: str,
        username_template: str = "",
        source_field: str = "",
        username_collision_policy: str = "append_employee_id",
        username_collision_template: str = "",
        field_label: str = "",
    ) -> dict[str, Any]:
        raw_payload = dict(row.get("raw_payload") or {})
        raw_payload.update(
            {
                "userid": row.get("source_user_id") or "",
                "name": row.get("display_name") or "",
                "employee_id": row.get("employee_id") or "",
                "email": row.get("email") or "",
                "position": row.get("position") or "",
                "department": row.get("department_ids") or [],
            }
        )
        strategy = normalize_username_strategy(
            "userid" if username_strategy == "source_user_id" else username_strategy
        )
        template = username_template
        normalized_source_field = str(source_field or "").strip()
        if normalized_source_field and normalized_source_field not in {
            "source_user_id",
            "employee_id",
            "email_localpart",
            "pinyin_initials_employee_id",
            "pinyin_full_employee_id",
            "family_name_pinyin_given_initials",
            "family_name_pinyin_given_name_pinyin",
            "custom_template",
        }:
            strategy = "custom_template"
            template = "{" + normalized_source_field + "}"
        user = SourceDirectoryUser.from_source_payload(raw_payload)
        template_context = build_template_context(user)
        resolved_template = resolve_username_template(strategy, template)
        rendered_value = render_template(resolved_template, template_context)
        candidates = build_managed_username_candidates(
            user,
            username_strategy=strategy,
            username_template=template,
            username_collision_policy=normalize_username_collision_policy(
                username_collision_policy
            ),
            username_collision_template=username_collision_template,
        )
        resolution_candidates = build_identity_candidates(
            user,
            username_strategy=strategy,
            username_template=template,
            username_collision_policy=normalize_username_collision_policy(
                username_collision_policy
            ),
            username_collision_template=username_collision_template,
        )
        primary_candidate = next(
            (
                item
                for item in candidates
                if item.get("rule") == "managed_username_primary"
            ),
            None,
        )
        username = str((primary_candidate or {}).get("username") or "")
        field_context_key = {
            "source_user_id": "userid",
            "employee_id": "employee_id",
            "email_localpart": "email_localpart",
        }.get(normalized_source_field, normalized_source_field)
        source_value = str(template_context.get(field_context_key) or "") if field_context_key else ""
        if normalized_source_field in {
            "pinyin_initials_employee_id",
            "pinyin_full_employee_id",
            "family_name_pinyin_given_initials",
            "family_name_pinyin_given_name_pinyin",
            "custom_template",
        }:
            source_value = rendered_value
        cleaned_value = re.sub(r"[^A-Za-z0-9._-]+", "", rendered_value.strip())
        risks: list[str] = []
        if not username:
            risks.append("mapping_field_missing")
        if cleaned_value and len(cleaned_value) > AD_USERNAME_MAX_LENGTH:
            risks.append("username_truncated")
        if rendered_value and cleaned_value != rendered_value.strip():
            risks.append("illegal_characters_removed")
        case_normalized = bool(username and rendered_value and username != rendered_value and username.lower() == rendered_value.lower())
        masked_value = source_value
        lowered_field = normalized_source_field.lower()
        if lowered_field == "email_localpart":
            masked_value = cls._mask_field_sample(
                normalized_source_field, source_value
            )
        elif "email" in lowered_field:
            masked_value = cls._mask_email(source_value)
        elif any(token in lowered_field for token in ("mobile", "phone", "telephone", "secret", "token", "password")):
            masked_value = cls._mask_field_sample(normalized_source_field, source_value)
        rendered_display_value = rendered_value
        if lowered_field == "email_localpart":
            rendered_display_value = cls._mask_field_sample(
                normalized_source_field, rendered_value
            )
        elif "email" in lowered_field:
            rendered_display_value = cls._mask_email(rendered_value)
        elif any(
            token in lowered_field
            for token in ("mobile", "phone", "telephone", "secret", "token", "password")
        ):
            rendered_display_value = cls._mask_field_sample(
                normalized_source_field, rendered_value
            )
        return {
            "username": username,
            "risks": risks,
            "candidates": candidates[:5],
            "resolution_candidates": resolution_candidates,
            "mapping_input": {
                "field_name": normalized_source_field or strategy,
                "field_label": field_label or normalized_source_field or strategy,
                "value": masked_value,
                "method": strategy,
                "template": resolved_template,
            },
            "candidate_mapping": {
                "ad_username": username,
                "source": str((primary_candidate or {}).get("rule") or "unresolved"),
                "normalized_value": username,
                "raw_rendered_value": rendered_display_value,
                "illegal_characters_removed": "illegal_characters_removed" in risks,
                "truncated": "username_truncated" in risks,
                "case_normalized": case_normalized,
                "risks": list(risks),
            },
        }


__all__ = [
    "SourceDirectoryService",
    "bounded_scalar_payload",
    "mask_mobile",
]
