from __future__ import annotations

import csv
import io
import json
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional
from urllib.parse import urlencode

from fastapi import FastAPI, Request

from sync_app.core.models import SourceDirectoryUser, UserDepartmentBundle
from sync_app.core.sync_policies import (
    build_template_context,
    normalize_username_collision_policy,
    normalize_username_strategy,
    resolve_username_template,
)
from sync_app.services.runtime_connectors import (
    build_department_connector_map,
    build_department_scope_root_map,
    is_department_in_connector_scope,
    load_connector_specs,
    resolve_department_ou_path,
    trim_department_paths_to_scope,
)
from sync_app.services.runtime_identity import (
    build_identity_candidates,
    resolve_target_department,
)
from sync_app.services.conflict_decision import build_binding_decision_summary
from sync_app.web.request_support import RequestSupport


class SyncSupport:
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
    def _parse_root_unit_ids(raw_value: Any) -> list[int]:
        values: list[int] = []
        for item in str(raw_value or "").replace("\n", ",").split(","):
            candidate = item.strip()
            if candidate.isdigit():
                values.append(int(candidate))
        return values

    @staticmethod
    def _normalize_ou_path(raw_value: Any, *, default: str = "") -> str:
        raw_text = str(raw_value or "").strip()
        if not raw_text:
            return default
        dn_segments = [
            part.split("=", 1)[1].strip()
            for part in raw_text.split(",")
            if "=" in part and part.strip().lower().startswith("ou=") and part.split("=", 1)[1].strip()
        ]
        if dn_segments:
            segments = list(reversed(dn_segments))
        else:
            segments = [
                segment.strip()
                for segment in raw_text.replace("\\", "/").split("/")
                if segment.strip()
            ]
        normalized = "/".join(segments)
        return normalized or default

    def _build_department_tree(self, departments: list[Any]) -> dict[int, Any]:
        department_tree = {
            int(item.department_id): item
            for item in departments
            if getattr(item, "department_id", None)
        }
        for department_id in list(department_tree):
            path_names: list[str] = []
            path_ids: list[int] = []
            current_id = department_id
            seen: set[int] = set()
            while current_id and current_id in department_tree and current_id not in seen:
                seen.add(current_id)
                current_node = department_tree[current_id]
                path_names.insert(0, current_node.name)
                path_ids.insert(0, current_node.department_id)
                current_id = current_node.parent_id
            department_tree[department_id].set_hierarchy(path_names, path_ids)
        return department_tree

    def _load_source_user_from_provider(self, source_provider: Any, source_user_id: str) -> Optional[SourceDirectoryUser]:
        normalized_source_user_id = str(source_user_id or "").strip()
        if not normalized_source_user_id:
            return None
        detail_payload = source_provider.get_user_detail(normalized_source_user_id) or {}
        if detail_payload:
            source_user = SourceDirectoryUser.from_source_payload(detail_payload)
            source_user.merge_payload(detail_payload)
            return source_user
        return next(
            (
                item
                for item in source_provider.search_users(normalized_source_user_id, limit=50)
                if str(item.source_user_id or "").strip() == normalized_source_user_id
            ),
            None,
        )

    @staticmethod
    def _merge_source_directory_user(
        users_by_id: dict[str, SourceDirectoryUser],
        user: SourceDirectoryUser,
    ) -> None:
        normalized_source_user_id = str(user.source_user_id or "").strip()
        if not normalized_source_user_id:
            return
        existing = users_by_id.get(normalized_source_user_id)
        if existing is None:
            users_by_id[normalized_source_user_id] = SourceDirectoryUser.from_source_payload(
                user.to_state_payload()
            )
            return
        existing.merge_payload(user.raw_payload)
        merged_departments = {
            int(value)
            for value in existing.departments
            if str(value).strip().lstrip("-").isdigit()
        }
        merged_departments.update(
            int(value)
            for value in user.departments
            if str(value).strip().lstrip("-").isdigit()
        )
        existing.departments = sorted(merged_departments)

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

    def _build_runtime_connector_context(
        self,
        request: Request,
        *,
        config: Any,
        department_tree: dict[int, Any],
    ) -> dict[str, Any]:
        current_org = self.request_support.get_current_org(request)
        org_id = current_org.org_id
        settings_repo = request.app.state.settings_repo
        connector_specs = load_connector_specs(
            config,
            request.app.state.connector_repo,
            connectors_enabled=settings_repo.get_bool(
                "advanced_connector_routing_enabled",
                False,
                org_id=org_id,
            ),
            org_id=org_id,
            default_root_department_ids=self._parse_root_unit_ids(
                settings_repo.get_value("source_root_unit_ids", "", org_id=org_id)
            ),
            default_disabled_users_ou=self._normalize_ou_path(
                settings_repo.get_value("disabled_users_ou_path", "Disabled Users", org_id=org_id),
                default="Disabled Users",
            ),
            default_custom_group_ou_path=self._normalize_ou_path(
                settings_repo.get_value("custom_group_ou_path", "Managed Groups", org_id=org_id),
                default="Managed Groups",
            ),
            default_user_root_ou_path=self._normalize_ou_path(
                settings_repo.get_value("directory_root_ou_path", "", org_id=org_id),
            ),
        )
        department_connector_map = build_department_connector_map(department_tree, connector_specs)
        department_scope_root_map = build_department_scope_root_map(
            department_tree,
            connector_specs,
            department_connector_map,
        )
        trim_department_paths_to_scope(department_tree, department_scope_root_map)
        department_ou_mappings_by_connector: dict[str, list[Any]] = {}
        for record in request.app.state.department_ou_mapping_repo.list_mapping_records(
            enabled_only=True,
            org_id=org_id,
        ):
            department_ou_mappings_by_connector.setdefault(str(record.connector_id or "").strip(), []).append(record)
        placement_blocked_department_ids = {
            int(rule.match_value)
            for rule in request.app.state.exception_rule_repo.list_enabled_rule_records(org_id=org_id)
            if rule.rule_type == "skip_department_placement" and str(rule.match_value).strip().isdigit()
        }
        return {
            "connector_specs": connector_specs,
            "connector_specs_by_id": {
                str(spec.get("connector_id") or "default"): spec for spec in connector_specs
            },
            "department_connector_map": department_connector_map,
            "department_scope_root_map": department_scope_root_map,
            "department_ou_mappings_by_connector": department_ou_mappings_by_connector,
            "placement_blocked_department_ids": placement_blocked_department_ids,
            "excluded_department_names": {str(name or "") for name in config.exclude_departments},
            "user_ou_placement_strategy": settings_repo.get_value(
                "user_ou_placement_strategy",
                "source_primary_department",
                org_id=org_id,
            )
            or "source_primary_department",
        }

    def _build_username_preview_from_user(
        self,
        user: SourceDirectoryUser,
        *,
        connector_spec: dict[str, Any],
    ) -> dict[str, Any]:
        strategy = normalize_username_strategy(connector_spec.get("username_strategy"))
        collision_policy = normalize_username_collision_policy(
            connector_spec.get("username_collision_policy")
        )
        username_template = str(connector_spec.get("username_template") or "").strip()
        collision_template = str(connector_spec.get("username_collision_template") or "").strip()
        candidates = build_identity_candidates(
            user,
            username_template=username_template,
            username_strategy=strategy,
            username_collision_policy=collision_policy,
            username_collision_template=collision_template,
        )
        template_context = build_template_context(user)
        common_context_keys = (
            "userid",
            "name",
            "email",
            "email_localpart",
            "employee_id",
            "pinyin_initials",
            "pinyin_full",
            "family_name_pinyin",
            "given_initials",
            "given_name_pinyin",
            "name_ascii",
            "position",
            "mobile",
        )
        return {
            "connector": {
                "connector_id": str(connector_spec.get("connector_id") or "default"),
                "name": str(connector_spec.get("name") or "Default Connector"),
            },
            "strategy": strategy,
            "resolved_template": resolve_username_template(strategy, username_template),
            "username_template": username_template,
            "collision_policy": collision_policy,
            "collision_template": collision_template,
            "template_context": {
                key: str(template_context.get(key) or "") for key in common_context_keys
            },
            "primary_candidate": next(
                (candidate for candidate in candidates if candidate.get("managed")),
                candidates[0] if candidates else None,
            ),
            "candidates": candidates,
        }

    def _close_directory_resource(self, resource: Any) -> None:
        close_fn = getattr(resource, "close", None)
        if callable(close_fn):
            close_fn()
            return
        client = getattr(resource, "client", None)
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            close_fn()

    def _get_org_app_config(self, request: Request) -> Any:
        current_org = self.request_support.get_current_org(request)
        return request.app.state.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=self.request_support.get_org_config_path(request),
        )

    def _get_source_provider(self, request: Request) -> tuple[Any, Any]:
        current_org = self.request_support.get_current_org(request)
        config = request.app.state.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=self.request_support.get_org_config_path(request),
        )
        provider = self.build_source_provider(
            app_config=config,
            logger=self.logger,
        )
        return config, provider

    def _get_target_provider(self, request: Request) -> tuple[Any, Any]:
        current_org = self.request_support.get_current_org(request)
        config = request.app.state.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=self.request_support.get_org_config_path(request),
        )
        provider = self.build_target_provider(
            server=config.ldap.server,
            domain=config.ldap.domain,
            username=config.ldap.username,
            password=config.ldap.password,
            use_ssl=config.ldap.use_ssl,
            port=config.ldap.port,
            validate_cert=config.ldap.validate_cert,
            ca_cert_path=config.ldap.ca_cert_path,
            default_password=config.default_password,
            force_change_password=config.force_change_password,
            password_complexity=config.password_complexity,
            exclude_accounts=config.exclude_accounts,
            disabled_users_ou_name=config.disabled_users_ou_name,
            managed_group_type=config.managed_group_type,
            managed_group_mail_domain=config.managed_group_mail_domain,
            custom_group_ou_path=config.custom_group_ou_path,
            user_root_ou_path=config.directory_root_ou_path,
        )
        return config, provider

    def validate_binding_target(self, request: Request, source_user_id: str, ad_username: str) -> Optional[str]:
        current_org = self.request_support.get_current_org(request)
        config = request.app.state.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=self.request_support.get_org_config_path(request),
        )
        if self.is_protected_ad_account_name(ad_username, config.exclude_accounts):
            return f"AD account {ad_username} is system-protected and cannot be managed by sync."
        existing_by_ad = request.app.state.user_binding_repo.get_binding_record_by_ad_username(
            ad_username,
            org_id=current_org.org_id,
        )
        if existing_by_ad and existing_by_ad.source_user_id != source_user_id:
            return (
                f"AD account {ad_username} is already bound to source user "
                f"{existing_by_ad.source_user_id}. Resolve the existing binding first."
            )
        source_exists, source_error = self.source_user_exists_in_source_provider(request, source_user_id)
        if not source_exists:
            return source_error or f"Source user {source_user_id} does not exist"
        target_exists, target_error = self.target_user_exists_in_directory(request, ad_username)
        if not target_exists:
            return target_error or f"AD account {ad_username} does not exist"
        return None

    def search_source_users(self, request: Request, query: str, *, limit: int = 20) -> list[dict[str, Any]]:
        normalized_query = str(query or "").strip()
        if not normalized_query:
            return []
        try:
            _config, source_provider = self._get_source_provider(request)
            try:
                users = source_provider.search_users(normalized_query, limit=limit)
            finally:
                self._close_directory_resource(source_provider)
        except Exception as exc:
            self.logger.warning("failed to search source users: %s", exc)
            return []
        results: list[dict[str, Any]] = []
        for user in users:
            user_id = str(user.source_user_id or "").strip()
            if not user_id:
                continue
            results.append(
                {
                    "id": user_id,
                    "name": str(user.name or user_id),
                    "email": str(user.email or ""),
                    "departments": [str(value) for value in user.departments if str(value).strip()],
                }
            )
        return results[: max(int(limit or 20), 1)]

    def list_source_user_departments(self, request: Request, source_user_id: str) -> list[dict[str, Any]]:
        normalized_source_user_id = str(source_user_id or "").strip()
        if not normalized_source_user_id:
            return []
        try:
            _config, source_provider = self._get_source_provider(request)
            try:
                departments = source_provider.list_departments()
                department_map = {int(item.department_id): item for item in departments if item.department_id}
                for department_id in list(department_map):
                    path_names: list[str] = []
                    path_ids: list[int] = []
                    current_id = department_id
                    seen: set[int] = set()
                    while current_id and current_id in department_map and current_id not in seen:
                        seen.add(current_id)
                        current_node = department_map[current_id]
                        path_names.insert(0, current_node.name)
                        path_ids.insert(0, current_node.department_id)
                        current_id = current_node.parent_id
                    department_map[department_id].set_hierarchy(path_names, path_ids)
                detail_payload = source_provider.get_user_detail(normalized_source_user_id) or {}
                if detail_payload:
                    source_user = SourceDirectoryUser.from_source_payload(detail_payload)
                else:
                    source_user = next(
                        (
                            item
                            for item in source_provider.search_users(normalized_source_user_id, limit=50)
                            if str(item.source_user_id or "").strip() == normalized_source_user_id
                        ),
                        None,
                    )
                if not source_user:
                    return []
                results: list[dict[str, Any]] = []
                for department_id in source_user.departments:
                    node = department_map.get(int(department_id))
                    if node is None:
                        continue
                    results.append(
                        {
                            "id": str(node.department_id),
                            "name": str(node.name or node.department_id),
                            "path_display": " / ".join(node.path or [node.name]),
                            "level": max(len(node.path) - 1, 0),
                        }
                    )
                return results
            finally:
                self._close_directory_resource(source_provider)
        except Exception as exc:
            self.logger.warning("failed to load source user departments: %s", exc)
            return []

    def search_target_users(self, request: Request, query: str, *, limit: int = 20) -> list[dict[str, Any]]:
        normalized_query = str(query or "").strip()
        if not normalized_query:
            return []
        try:
            _config, target_provider = self._get_target_provider(request)
            try:
                users = target_provider.search_users(normalized_query, limit=limit)
            finally:
                self._close_directory_resource(target_provider)
        except Exception as exc:
            self.logger.warning("failed to search AD users: %s", exc)
            return []
        results: list[dict[str, Any]] = []
        for user in users:
            username = str(getattr(user, "username", "") or "").strip()
            if not username:
                continue
            results.append(
                {
                    "id": username,
                    "name": str(getattr(user, "display_name", "") or username),
                    "mail": str(getattr(user, "email", "") or ""),
                    "dn": str(getattr(user, "dn", "") or ""),
                    "upn": str(getattr(user, "user_principal_name", "") or ""),
                }
            )
        return results[: max(int(limit or 20), 1)]

    def build_username_preview(
        self,
        request: Request,
        *,
        connector_id: str,
        sample_userid: str,
        sample_name: str,
        sample_email: str,
        sample_employee_id: str = "",
        sample_position: str = "",
        sample_mobile: str = "",
        sample_payload_json: str = "",
    ) -> dict[str, Any]:
        normalized_connector_id = str(connector_id or "default").strip() or "default"
        extra_payload: dict[str, Any] = {}
        normalized_payload_json = str(sample_payload_json or "").strip()
        if normalized_payload_json:
            try:
                parsed_payload = json.loads(normalized_payload_json)
            except json.JSONDecodeError as exc:
                raise ValueError("Additional source payload JSON must be valid JSON.") from exc
            if not isinstance(parsed_payload, dict):
                raise ValueError("Additional source payload JSON must be an object.")
            extra_payload = dict(parsed_payload)
        payload = dict(extra_payload)
        payload.update(
            {
                "userid": str(sample_userid or "").strip(),
                "name": str(sample_name or "").strip(),
                "email": str(sample_email or "").strip(),
            }
        )
        if str(sample_employee_id or "").strip():
            payload["employee_id"] = str(sample_employee_id or "").strip()
        if str(sample_position or "").strip():
            payload["position"] = str(sample_position or "").strip()
        if str(sample_mobile or "").strip():
            payload["mobile"] = str(sample_mobile or "").strip()
        if not any(str(payload.get(key) or "").strip() for key in ("userid", "name", "email", "employee_id")):
            raise ValueError("Fill at least one sample identity field before previewing.")

        sample_user = SourceDirectoryUser.from_source_payload(payload)
        current_org = self.request_support.get_current_org(request)
        runtime_context = self._build_runtime_connector_context(
            request,
            config=self._get_org_app_config(request),
            department_tree={},
        )
        connector_spec = runtime_context["connector_specs_by_id"].get(normalized_connector_id)
        if connector_spec is None and normalized_connector_id != "default":
            enabled_connectors = request.app.state.connector_repo.list_connector_records(
                enabled_only=True,
                org_id=current_org.org_id,
            )
            if len(enabled_connectors) == 1 and enabled_connectors[0].connector_id == normalized_connector_id:
                default_spec = runtime_context["connector_specs_by_id"].get("default")
                if default_spec is not None:
                    connector_spec = {
                        **default_spec,
                        "connector_id": normalized_connector_id,
                        "name": enabled_connectors[0].name or default_spec.get("name") or "Default Connector",
                    }
        if connector_spec is None:
            raise ValueError(f"Connector {normalized_connector_id} was not found.")
        preview = self._build_username_preview_from_user(sample_user, connector_spec=connector_spec)
        preview["sample_user"] = {
            "userid": sample_user.userid,
            "name": sample_user.name,
            "email": sample_user.email,
        }
        return preview

    def explain_identity_routing(self, request: Request, source_user_id: str) -> dict[str, Any]:
        normalized_source_user_id = str(source_user_id or "").strip()
        if not normalized_source_user_id:
            raise ValueError("Source user ID is required.")

        current_org = self.request_support.get_current_org(request)
        binding_record = request.app.state.user_binding_repo.get_binding_record_by_source_user_id(
            normalized_source_user_id,
            org_id=current_org.org_id,
        )
        override_record = request.app.state.department_override_repo.get_override_record_by_source_user_id(
            normalized_source_user_id,
            org_id=current_org.org_id,
        )
        config, source_provider = self._get_source_provider(request)
        try:
            departments = source_provider.list_departments()
            department_tree = self._build_department_tree(departments)
            source_user = self._load_source_user_from_provider(source_provider, normalized_source_user_id)
            if not source_user:
                raise ValueError(f"Source user {normalized_source_user_id} was not found in the configured source directory.")

            original_paths = {
                department_id: {
                    "path": list(node.path or []),
                    "path_ids": list(node.path_ids or []),
                }
                for department_id, node in department_tree.items()
            }
            runtime_context = self._build_runtime_connector_context(
                request,
                config=config,
                department_tree=department_tree,
            )
            connector_specs_by_id = runtime_context["connector_specs_by_id"]
            department_connector_map = runtime_context["department_connector_map"]
            department_scope_root_map = runtime_context["department_scope_root_map"]
            excluded_department_names = runtime_context["excluded_department_names"]
            placement_blocked_department_ids = runtime_context["placement_blocked_department_ids"]

            def is_department_excluded(dept_info: Optional[Any]) -> bool:
                return (
                    not dept_info
                    or dept_info.name in excluded_department_names
                    or not is_department_in_connector_scope(
                        dept_info,
                        connector_specs_by_id=connector_specs_by_id,
                        department_connector_map=department_connector_map,
                        department_scope_root_map=department_scope_root_map,
                    )
                )

            def is_department_blocked_for_placement(dept_info: Optional[Any]) -> bool:
                return is_department_excluded(dept_info) or (
                    bool(dept_info) and dept_info.department_id in placement_blocked_department_ids
                )

            bundle = UserDepartmentBundle(user=source_user)
            department_rows: list[dict[str, Any]] = []
            connector_candidates: set[str] = set()
            for department_id in source_user.departments:
                department = department_tree.get(int(department_id))
                if department is None:
                    continue
                bundle.add_department(department)
                connector_id = department_connector_map.get(department.department_id, "default")
                connector_spec = connector_specs_by_id.get(connector_id, connector_specs_by_id["default"])
                connector_candidates.add(connector_id)
                original_path = original_paths.get(department.department_id, {})
                department_rows.append(
                    {
                        "department_id": int(department.department_id),
                        "name": str(department.name or ""),
                        "path_display": " / ".join(list(original_path.get("path") or []) or [department.name]),
                        "scoped_path_display": " / ".join(list(department.path or []) or [department.name]),
                        "connector_id": connector_id,
                        "connector_name": str(connector_spec.get("name") or "Default Connector"),
                        "scope_root_id": runtime_context["department_scope_root_map"].get(department.department_id),
                        "is_excluded": is_department_excluded(department),
                        "is_blocked_for_placement": is_department_blocked_for_placement(department),
                    }
                )

            if not connector_candidates:
                connector_candidates = {"default"}
            resolved_connector_ids = sorted(connector_candidates)
            selected_connector_id = resolved_connector_ids[0] if len(resolved_connector_ids) == 1 else ""
            selected_connector_spec = connector_specs_by_id.get(selected_connector_id) if selected_connector_id else None

            override_department_id = None
            if override_record and override_record.primary_department_id:
                try:
                    override_department_id = int(override_record.primary_department_id)
                except (TypeError, ValueError):
                    override_department_id = None

            target_department, placement_reason = resolve_target_department(
                bundle,
                placement_strategy=runtime_context["user_ou_placement_strategy"],
                is_department_excluded=is_department_blocked_for_placement,
                override_department_id=override_department_id,
            )
            target_ou_segments = (
                resolve_department_ou_path(
                    target_department,
                    connector_id=selected_connector_id,
                    mappings_by_connector=runtime_context["department_ou_mappings_by_connector"],
                )
                if target_department and selected_connector_id
                else []
            )
            preview = (
                self._build_username_preview_from_user(source_user, connector_spec=selected_connector_spec)
                if selected_connector_spec
                else None
            )
            return {
                "user": {
                    "userid": source_user.userid,
                    "name": source_user.name,
                    "email": source_user.email,
                },
                "binding": (
                    {
                        "ad_username": binding_record.ad_username,
                        "connector_id": binding_record.connector_id or "",
                        "source": binding_record.source,
                        "is_enabled": binding_record.is_enabled,
                    }
                    if binding_record
                    else None
                ),
                "department_override": (
                    {
                        "primary_department_id": override_record.primary_department_id,
                        "notes": override_record.notes,
                    }
                    if override_record
                    else None
                ),
                "routing_status": "resolved" if selected_connector_id else "multiple_connector_candidates",
                "connector_candidates": [
                    {
                        "connector_id": connector_id,
                        "name": str(
                            (connector_specs_by_id.get(connector_id) or {}).get("name") or "Default Connector"
                        ),
                    }
                    for connector_id in resolved_connector_ids
                ],
                "selected_connector": (
                    {
                        "connector_id": selected_connector_id,
                        "name": str(selected_connector_spec.get("name") or "Default Connector"),
                        "root_department_ids": list(selected_connector_spec.get("root_department_ids") or []),
                    }
                    if selected_connector_spec
                    else None
                ),
                "departments": department_rows,
                "placement_strategy": runtime_context["user_ou_placement_strategy"],
                "placement_reason": placement_reason,
                "target_department": (
                    {
                        "department_id": int(target_department.department_id),
                        "name": str(target_department.name or ""),
                        "path_display": " / ".join(list(target_department.path or []) or [target_department.name]),
                    }
                    if target_department
                    else None
                ),
                "target_ou_path": "/".join(target_ou_segments),
                "username_preview": preview,
            }
        finally:
            self._close_directory_resource(source_provider)

    def _build_conflict_candidate_options(
        self,
        conflict: Any,
        recommendation: Optional[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        details = getattr(conflict, "details", None) or {}
        if not isinstance(details, dict):
            details = {}

        candidates_by_username: dict[str, dict[str, Any]] = {}

        def add_candidate(
            username: str,
            *,
            rule: str = "",
            explanation: str = "",
            is_recommended: bool = False,
        ) -> None:
            normalized_username = str(username or "").strip()
            if not normalized_username:
                return
            key = normalized_username.lower()
            existing = candidates_by_username.get(key)
            if existing is None:
                candidates_by_username[key] = {
                    "username": normalized_username,
                    "rule": str(rule or ""),
                    "explanation": str(explanation or ""),
                    "is_recommended": bool(is_recommended),
                }
                return
            if rule and not existing["rule"]:
                existing["rule"] = str(rule)
            if explanation and not existing["explanation"]:
                existing["explanation"] = str(explanation)
            if is_recommended:
                existing["is_recommended"] = True

        if recommendation and recommendation.get("ad_username"):
            add_candidate(
                str(recommendation.get("ad_username") or ""),
                rule="recommended_action",
                explanation=str(recommendation.get("reason") or ""),
                is_recommended=True,
            )

        for candidate in list(details.get("candidates") or []):
            if not isinstance(candidate, dict):
                continue
            add_candidate(
                str(candidate.get("username") or ""),
                rule=str(candidate.get("rule") or ""),
                explanation=str(candidate.get("explanation") or ""),
            )

        conflict_type = str(getattr(conflict, "conflict_type", "") or "").strip().lower()
        if conflict_type == "shared_ad_account":
            add_candidate(
                str(getattr(conflict, "target_key", "") or details.get("ad_username") or ""),
                rule="shared_ad_account",
                explanation="This AD account is currently shared by multiple source users.",
            )

        return sorted(
            candidates_by_username.values(),
            key=lambda item: (
                0 if item["is_recommended"] else 1,
                str(item["username"] or "").lower(),
            ),
        )

    def _load_target_account_summary(self, request: Request, ad_username: str) -> dict[str, Any]:
        normalized_ad_username = str(ad_username or "").strip()
        if not normalized_ad_username:
            return {
                "username": "",
                "exists": False,
                "enabled": None,
                "display_name": "",
                "mail": "",
                "title": "",
                "description": "",
                "telephone_number": "",
                "last_logon": "",
                "distinguished_name": "",
                "ou_path": "",
            }

        user_details: dict[str, Any] = {}
        batch_record = None
        enabled: bool | None = None
        try:
            _config, target_provider = self._get_target_provider(request)
            try:
                if hasattr(target_provider, "get_users_batch"):
                    batch_records = dict(target_provider.get_users_batch([normalized_ad_username]) or {})
                    batch_record = next(
                        (
                            item
                            for key, item in batch_records.items()
                            if str(key or "").strip().lower() == normalized_ad_username.lower()
                        ),
                        None,
                    )
                if hasattr(target_provider, "get_user_details"):
                    user_details = dict(target_provider.get_user_details(normalized_ad_username) or {})
                is_user_active = getattr(target_provider, "is_user_active", None)
                if callable(is_user_active):
                    enabled = bool(is_user_active(normalized_ad_username))
            finally:
                self._close_directory_resource(target_provider)
        except Exception as exc:
            self.logger.warning("failed to load target account summary for %s: %s", normalized_ad_username, exc)

        exists = bool(user_details) or batch_record is not None
        distinguished_name = str(
            user_details.get("DistinguishedName")
            or getattr(batch_record, "dn", "")
            or ""
        )
        return {
            "username": normalized_ad_username,
            "exists": exists,
            "enabled": enabled if exists else None,
            "display_name": str(
                user_details.get("DisplayName")
                or getattr(batch_record, "display_name", "")
                or ""
            ),
            "mail": str(
                user_details.get("Mail")
                or getattr(batch_record, "email", "")
                or ""
            ),
            "title": str(user_details.get("Title") or ""),
            "description": str(user_details.get("Description") or ""),
            "telephone_number": str(user_details.get("TelephoneNumber") or ""),
            "last_logon": str(user_details.get("LastLogonDate") or ""),
            "distinguished_name": distinguished_name,
            "ou_path": self._normalize_ou_path(distinguished_name),
        }

    def _build_conflict_field_updates(
        self,
        request: Request,
        *,
        connector_id: str,
    ) -> list[dict[str, str]]:
        current_org = self.request_support.get_current_org(request)
        items: list[dict[str, str]] = []
        seen_fields: set[str] = set()

        def add_item(name: str, *, source: str) -> None:
            normalized_name = str(name or "").strip()
            if not normalized_name:
                return
            key = normalized_name.lower()
            if key in seen_fields:
                return
            seen_fields.add(key)
            items.append(
                {
                    "name": normalized_name,
                    "source": str(source or ""),
                }
            )

        add_item("displayName", source="Core user sync")
        add_item("mail", source="Core user sync")
        add_item("target OU", source="OU placement")

        for rule in request.app.state.mapping_rule_repo.list_rule_records(
            direction="source_to_ad",
            connector_id=str(connector_id or "").strip() or "default",
            enabled_only=True,
            org_id=current_org.org_id,
        ):
            add_item(
                str(getattr(rule, "target_field", "") or ""),
                source=str(getattr(rule, "source_field", "") or "Attribute mapping"),
            )
        return items

    def build_conflict_decision_guide(
        self,
        request: Request,
        conflict: Any,
        *,
        ad_username: str = "",
    ) -> dict[str, Any]:
        current_org = self.request_support.get_current_org(request)
        current_org_id = current_org.org_id
        details = getattr(conflict, "details", None) or {}
        if not isinstance(details, dict):
            details = {}

        recommendation = self.recommend_conflict_resolution(conflict)
        explanation = None
        explanation_error = ""
        if str(getattr(conflict, "source_id", "") or "").strip():
            try:
                explanation = self.explain_identity_routing(request, str(conflict.source_id))
            except Exception as exc:
                explanation_error = str(exc)

        candidate_options = self._build_conflict_candidate_options(conflict, recommendation)
        selected_target_username = str(ad_username or "").strip()
        if not selected_target_username:
            selected_target_username = next(
                (
                    str(item.get("username") or "").strip()
                    for item in candidate_options
                    if item.get("is_recommended")
                ),
                "",
            )
        if not selected_target_username:
            selected_target_username = next(
                (str(item.get("username") or "").strip() for item in candidate_options),
                "",
            )
        if not selected_target_username:
            selected_target_username = str(getattr(conflict, "target_key", "") or "").strip()
        for item in candidate_options:
            item["is_selected"] = (
                str(item.get("username") or "").strip().lower()
                == selected_target_username.lower()
            )

        selected_connector = dict((explanation or {}).get("selected_connector") or {})
        current_binding = dict((explanation or {}).get("binding") or {})
        connector_id = str(
            selected_connector.get("connector_id")
            or current_binding.get("connector_id")
            or "default"
        ).strip() or "default"

        target_account = self._load_target_account_summary(request, selected_target_username)
        existing_binding_owner = (
            request.app.state.user_binding_repo.get_binding_record_by_ad_username(
                selected_target_username,
                org_id=current_org_id,
            )
            if selected_target_username
            else None
        )
        field_updates = self._build_conflict_field_updates(
            request,
            connector_id=connector_id,
        )
        config = self._get_org_app_config(request)
        shared_source_user_ids = [
            str(item or "").strip()
            for item in list(details.get("source_user_ids") or details.get("wecom_userids") or [])
            if str(item or "").strip()
        ]
        decision = build_binding_decision_summary(
            conflict_type=str(getattr(conflict, "conflict_type", "") or ""),
            source_user_id=str(getattr(conflict, "source_id", "") or ""),
            selected_target_username=selected_target_username,
            target_exists=bool(target_account.get("exists")),
            target_enabled=target_account.get("enabled"),
            current_binding_owner=(
                str(getattr(existing_binding_owner, "source_user_id", "") or "")
                if existing_binding_owner
                else ""
            ),
            is_protected_account=(
                self.is_protected_ad_account_name(selected_target_username, config.exclude_accounts)
                if selected_target_username
                else False
            ),
            shared_source_user_ids=shared_source_user_ids,
            rehire_restore_enabled=request.app.state.settings_repo.get_bool(
                "rehire_restore_enabled",
                False,
                org_id=current_org_id,
            ),
        )
        return {
            "source_user": dict((explanation or {}).get("user") or {})
            or {
                "userid": str(getattr(conflict, "source_id", "") or ""),
                "name": str(getattr(conflict, "source_id", "") or ""),
                "email": "",
            },
            "routing": explanation or {},
            "routing_error": explanation_error,
            "recommendation": recommendation or {},
            "candidate_options": candidate_options,
            "selected_target_username": selected_target_username,
            "selected_connector": selected_connector,
            "target_account": target_account,
            "existing_binding_owner": (
                {
                    "source_user_id": str(getattr(existing_binding_owner, "source_user_id", "") or ""),
                    "connector_id": str(getattr(existing_binding_owner, "connector_id", "") or ""),
                    "source": str(getattr(existing_binding_owner, "source", "") or ""),
                    "notes": str(getattr(existing_binding_owner, "notes", "") or ""),
                }
                if existing_binding_owner
                else None
            ),
            "field_updates": field_updates,
            "shared_source_user_ids": shared_source_user_ids,
            "decision": decision,
        }

    def build_source_data_quality_snapshot(self, request: Request) -> dict[str, Any]:
        config, source_provider = self._get_source_provider(request)
        try:
            department_tree = self._build_department_tree(source_provider.list_departments())
            users_by_id: dict[str, SourceDirectoryUser] = {}
            for department_id in sorted(department_tree):
                for user in source_provider.list_department_users(int(department_id)) or []:
                    self._merge_source_directory_user(users_by_id, user)

            for user in users_by_id.values():
                template_context = build_template_context(user)
                if str(user.name or "").strip() and str(user.email or "").strip() and str(
                    template_context.get("employee_id") or ""
                ).strip():
                    continue
                detail_payload = source_provider.get_user_detail(str(user.source_user_id or "").strip()) or {}
                if detail_payload:
                    user.merge_payload(detail_payload)
        finally:
            self._close_directory_resource(source_provider)

        runtime_context = self._build_runtime_connector_context(
            request,
            config=config,
            department_tree=department_tree,
        )
        connector_specs_by_id = runtime_context["connector_specs_by_id"]
        department_connector_map = runtime_context["department_connector_map"]
        department_scope_root_map = runtime_context["department_scope_root_map"]
        excluded_department_names = runtime_context["excluded_department_names"]
        placement_blocked_department_ids = runtime_context["placement_blocked_department_ids"]
        placement_strategy = runtime_context["user_ou_placement_strategy"]

        def is_department_excluded(dept_info: Optional[Any]) -> bool:
            return (
                not dept_info
                or dept_info.name in excluded_department_names
                or not is_department_in_connector_scope(
                    dept_info,
                    connector_specs_by_id=connector_specs_by_id,
                    department_connector_map=department_connector_map,
                    department_scope_root_map=department_scope_root_map,
                )
            )

        def is_department_blocked_for_placement(dept_info: Optional[Any]) -> bool:
            return is_department_excluded(dept_info) or (
                bool(dept_info) and dept_info.department_id in placement_blocked_department_ids
            )

        missing_email_repair_items: list[dict[str, Any]] = []
        missing_employee_id_repair_items: list[dict[str, Any]] = []
        missing_department_repair_items: list[dict[str, Any]] = []
        placement_gap_repair_items: list[dict[str, Any]] = []
        routing_ambiguity_repair_items: list[dict[str, Any]] = []
        naming_gap_repair_items: list[dict[str, Any]] = []
        duplicate_emails: dict[str, list[SourceDirectoryUser]] = {}
        duplicate_employee_ids: dict[str, list[SourceDirectoryUser]] = {}
        managed_username_groups: dict[tuple[str, str], list[dict[str, str]]] = {}
        connector_counts: dict[tuple[str, str], int] = {}
        users_with_multiple_departments = 0

        for source_user_id in sorted(users_by_id):
            user = users_by_id[source_user_id]
            template_context = build_template_context(user)
            normalized_email = str(user.email or "").strip()
            normalized_employee_id = str(template_context.get("employee_id") or "").strip()
            user_title = self._format_quality_user_title(user)

            if normalized_email:
                duplicate_emails.setdefault(normalized_email.lower(), []).append(user)
            else:
                missing_email_repair_items.append(
                    self._build_quality_repair_item(
                        key="missing_email",
                        label="Users missing work email",
                        severity="warning",
                        title=user_title,
                        detail="No work email was found on the source directory record.",
                        action="Backfill email on the source directory record where it is supposed to exist.",
                        source_user_id=user.source_user_id,
                        display_name=user.name,
                    )
                )

            if normalized_employee_id:
                duplicate_employee_ids.setdefault(normalized_employee_id.lower(), []).append(user)
            else:
                missing_employee_id_repair_items.append(
                    self._build_quality_repair_item(
                        key="missing_employee_id",
                        label="Users missing employee ID",
                        severity="warning",
                        title=user_title,
                        detail="No employee ID was found on the source directory record.",
                        action="Backfill employee ID or switch the naming strategy away from employee-ID-driven rules.",
                        source_user_id=user.source_user_id,
                        display_name=user.name,
                    )
                )

            if len(user.departments) > 1:
                users_with_multiple_departments += 1

            valid_departments = [
                department_tree.get(int(department_id))
                for department_id in user.departments
                if str(department_id).strip().lstrip("-").isdigit()
            ]
            valid_departments = [item for item in valid_departments if item is not None]
            if not valid_departments:
                missing_department_repair_items.append(
                    self._build_quality_repair_item(
                        key="missing_departments",
                        label="Users without valid departments",
                        severity="error",
                        title=user_title,
                        detail="No valid source department membership was found for routing and OU placement.",
                        action="Fix the source department assignment first, then rerun dry run.",
                        source_user_id=user.source_user_id,
                        display_name=user.name,
                    )
                )
                connector_counts[("__unrouted__", "No valid department routing")] = (
                    connector_counts.get(("__unrouted__", "No valid department routing"), 0) + 1
                )
                continue

            connector_candidates: dict[str, str] = {}
            for department in valid_departments:
                connector_id = department_connector_map.get(int(department.department_id), "default")
                connector_spec = connector_specs_by_id.get(connector_id, connector_specs_by_id["default"])
                connector_candidates[connector_id] = str(
                    connector_spec.get("name") or "Default Connector"
                )

            if len(connector_candidates) > 1:
                connector_counts[("__multiple__", "Multiple connector matches")] = (
                    connector_counts.get(("__multiple__", "Multiple connector matches"), 0) + 1
                )
                routing_ambiguity_repair_items.append(
                    self._build_quality_repair_item(
                        key="routing_ambiguity",
                        label="Users matching multiple connectors",
                        severity="error",
                        title=user_title,
                        detail="Matches multiple connector scopes: "
                        + ", ".join(
                            f"{name} [{connector_id}]"
                            for connector_id, name in sorted(connector_candidates.items())
                        ),
                        action="Adjust connector root units or source department ownership so each user resolves to one connector.",
                        source_user_id=user.source_user_id,
                        display_name=user.name,
                        connector_name=", ".join(
                            f"{name} [{connector_id}]"
                            for connector_id, name in sorted(connector_candidates.items())
                        ),
                    )
                )
                continue

            selected_connector_id = next(iter(connector_candidates.keys()), "default")
            selected_connector_spec = connector_specs_by_id.get(
                selected_connector_id,
                connector_specs_by_id["default"],
            )
            selected_connector_name = str(
                selected_connector_spec.get("name") or "Default Connector"
            )
            connector_counts[(selected_connector_id, selected_connector_name)] = (
                connector_counts.get((selected_connector_id, selected_connector_name), 0) + 1
            )

            bundle = UserDepartmentBundle(user=user)
            for department in valid_departments:
                bundle.add_department(department)
            target_department, _placement_reason = resolve_target_department(
                bundle,
                placement_strategy=placement_strategy,
                is_department_excluded=is_department_blocked_for_placement,
                override_department_id=None,
            )
            if target_department is None:
                placement_gap_repair_items.append(
                    self._build_quality_repair_item(
                        key="placement_unresolved",
                        label="Users blocked by placement rules",
                        severity="warning",
                        title=user_title,
                        detail="Current placement rules excluded every source department for this user."
                        f" Effective connector: {selected_connector_name} [{selected_connector_id}].",
                        action="Review placement strategy, exclusion rules, and connector root-unit scope.",
                        source_user_id=user.source_user_id,
                        display_name=user.name,
                        connector_id=selected_connector_id,
                        connector_name=selected_connector_name,
                    )
                )

            preview = self._build_username_preview_from_user(
                user,
                connector_spec=selected_connector_spec,
            )
            naming_gap_detail = self._describe_naming_prerequisite_gap(preview, user)
            if naming_gap_detail:
                naming_gap_repair_items.append(
                    self._build_quality_repair_item(
                        key="naming_prerequisite_gap",
                        label="Users missing naming prerequisites",
                        severity="warning",
                        title=user_title,
                        detail=naming_gap_detail
                        + f" Effective connector: {selected_connector_name} [{selected_connector_id}].",
                        action="Backfill the required fields or switch to a naming rule that fits the available source data.",
                        source_user_id=user.source_user_id,
                        display_name=user.name,
                        connector_id=selected_connector_id,
                        connector_name=selected_connector_name,
                    )
                )

            primary_managed_candidate = next(
                (
                    candidate
                    for candidate in list(preview.get("candidates") or [])
                    if candidate.get("managed")
                ),
                None,
            )
            if primary_managed_candidate:
                normalized_username = str(primary_managed_candidate.get("username") or "").strip().lower()
                if normalized_username:
                    managed_username_groups.setdefault(
                        (selected_connector_id, normalized_username),
                        [],
                    ).append(
                        {
                            "title": user_title,
                            "source_user_id": user.source_user_id,
                            "display_name": user.name,
                            "connector_id": selected_connector_id,
                            "connector_name": selected_connector_name,
                        }
                    )

        duplicate_email_repair_items = [
            self._build_quality_repair_item(
                key="duplicate_email",
                label="Duplicate work emails",
                severity="warning",
                title=email_value,
                detail="Shared by "
                + ", ".join(item.source_user_id for item in matched_users[:5]),
                action="Confirm whether the duplicates are legitimate shared accounts or source-data defects.",
                source_user_ids=[item.source_user_id for item in matched_users],
            )
            for email_value, matched_users in sorted(duplicate_emails.items())
            if len({str(item.source_user_id or "").strip().lower() for item in matched_users}) > 1
        ]
        duplicate_employee_id_repair_items = [
            self._build_quality_repair_item(
                key="duplicate_employee_id",
                label="Duplicate employee IDs",
                severity="warning",
                title=employee_id,
                detail="Shared by "
                + ", ".join(item.source_user_id for item in matched_users[:5]),
                action="Fix the source HR identifiers before relying on employee-ID naming or collision fallbacks.",
                source_user_ids=[item.source_user_id for item in matched_users],
            )
            for employee_id, matched_users in sorted(duplicate_employee_ids.items())
            if len({str(item.source_user_id or "").strip().lower() for item in matched_users}) > 1
        ]
        managed_username_collision_repair_items = [
            self._build_quality_repair_item(
                key="managed_username_collision",
                label="Predicted managed username collisions",
                severity="error",
                title=f"{username} [{matched_users[0]['connector_name']}]",
                detail="Would be generated for "
                + ", ".join(item["title"] for item in matched_users[:5]),
                action="Tune the username strategy or collision policy before running apply.",
                source_user_ids=[str(item.get("source_user_id") or "").strip() for item in matched_users],
                connector_id=connector_id,
                connector_name=str(matched_users[0]["connector_name"] or ""),
            )
            for (connector_id, username), matched_users in sorted(managed_username_groups.items())
            if len({str(item.get('title') or '').strip().lower() for item in matched_users}) > 1
        ]

        missing_department_samples = self._build_quality_samples(missing_department_repair_items)
        routing_ambiguity_samples = self._build_quality_samples(routing_ambiguity_repair_items)
        managed_username_collision_samples = self._build_quality_samples(managed_username_collision_repair_items)
        placement_gap_samples = self._build_quality_samples(placement_gap_repair_items)
        naming_gap_samples = self._build_quality_samples(naming_gap_repair_items)
        missing_email_samples = self._build_quality_samples(missing_email_repair_items)
        missing_employee_id_samples = self._build_quality_samples(missing_employee_id_repair_items)
        duplicate_email_samples = self._build_quality_samples(duplicate_email_repair_items)
        duplicate_employee_id_samples = self._build_quality_samples(duplicate_employee_id_repair_items)

        issues = [
            self._build_quality_issue(
                key="missing_departments",
                label="Users without valid departments",
                severity="error",
                description="These users cannot be routed into the managed OU tree because the source directory does not expose a valid department membership.",
                action="Fix the source department assignment first, then rerun dry run.",
                samples=missing_department_samples,
                count=len(missing_department_repair_items),
            ),
            self._build_quality_issue(
                key="routing_ambiguity",
                label="Users matching multiple connectors",
                severity="error",
                description="These users span more than one connector scope, so runtime cannot choose a single provisioning target.",
                action="Adjust connector root units or source department ownership so each user resolves to one connector.",
                samples=routing_ambiguity_samples,
                count=len(routing_ambiguity_repair_items),
            ),
            self._build_quality_issue(
                key="managed_username_collision",
                label="Predicted managed username collisions",
                severity="error",
                description="Different users would generate the same primary managed AD username inside the same connector.",
                action="Tune the username strategy or collision policy before running apply.",
                samples=managed_username_collision_samples,
                count=len(managed_username_collision_repair_items),
            ),
            self._build_quality_issue(
                key="placement_unresolved",
                label="Users blocked by placement rules",
                severity="warning",
                description="These users have departments, but the current placement policy excludes every candidate branch.",
                action="Review placement strategy, exclusion rules, and connector root-unit scope.",
                samples=placement_gap_samples,
                count=len(placement_gap_repair_items),
            ),
            self._build_quality_issue(
                key="naming_prerequisite_gap",
                label="Users missing naming prerequisites",
                severity="warning",
                description="These source records are missing fields required by the currently selected naming strategy.",
                action="Backfill the required fields or switch to a naming rule that fits the available source data.",
                samples=naming_gap_samples,
                count=len(naming_gap_repair_items),
            ),
            self._build_quality_issue(
                key="missing_email",
                label="Users missing work email",
                severity="warning",
                description="Blank work email makes email-based naming, write-back, and notification workflows harder to operate safely.",
                action="Backfill email on the source directory record where it is supposed to exist.",
                samples=missing_email_samples,
                count=len(missing_email_repair_items),
            ),
            self._build_quality_issue(
                key="missing_employee_id",
                label="Users missing employee ID",
                severity="warning",
                description="Blank employee ID reduces the quality of employee-ID-based naming and same-name collision handling.",
                action="Backfill employee ID or switch the naming strategy away from employee-ID-driven rules.",
                samples=missing_employee_id_samples,
                count=len(missing_employee_id_repair_items),
            ),
            self._build_quality_issue(
                key="duplicate_email",
                label="Duplicate work emails",
                severity="warning",
                description="Multiple source users share the same work email.",
                action="Confirm whether the duplicates are legitimate shared accounts or source-data defects.",
                samples=duplicate_email_samples,
                count=len(duplicate_email_repair_items),
            ),
            self._build_quality_issue(
                key="duplicate_employee_id",
                label="Duplicate employee IDs",
                severity="warning",
                description="Multiple source users share the same employee ID.",
                action="Fix the source HR identifiers before relying on employee-ID naming or collision fallbacks.",
                samples=duplicate_employee_id_samples,
                count=len(duplicate_employee_id_repair_items),
            ),
        ]
        normalized_issues = [issue for issue in issues if issue]
        severity_rank = {"error": 0, "warning": 1, "info": 2, "success": 3}
        normalized_issues.sort(
            key=lambda item: (
                severity_rank.get(str(item.get("severity") or "warning"), 9),
                -int(item.get("count") or 0),
                str(item.get("label") or ""),
            )
        )
        error_issue_count = sum(1 for item in normalized_issues if item["severity"] == "error")
        warning_issue_count = sum(1 for item in normalized_issues if item["severity"] == "warning")
        repair_items = [
            *missing_department_repair_items,
            *routing_ambiguity_repair_items,
            *managed_username_collision_repair_items,
            *placement_gap_repair_items,
            *naming_gap_repair_items,
            *missing_email_repair_items,
            *missing_employee_id_repair_items,
            *duplicate_email_repair_items,
            *duplicate_employee_id_repair_items,
        ]
        high_risk_items = [
            item
            for item in repair_items
            if str(item.get("severity") or "").strip().lower() == "error"
            or str(item.get("key") or "").strip().lower() in {"placement_unresolved", "naming_prerequisite_gap"}
        ]

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "analysis_notes": [
                "Counts reflect unique source users merged across all returned department memberships.",
                "Manual bindings and per-user department overrides are not expanded in this snapshot.",
            ],
            "summary": {
                "total_users": len(users_by_id),
                "department_count": len(department_tree),
                "users_with_multiple_departments": users_with_multiple_departments,
                "users_missing_email": len(missing_email_repair_items),
                "users_missing_employee_id": len(missing_employee_id_repair_items),
                "users_without_departments": len(missing_department_repair_items),
                "placement_unresolved_count": len(placement_gap_repair_items),
                "routing_ambiguity_count": len(routing_ambiguity_repair_items),
                "naming_prerequisite_gap_count": len(naming_gap_repair_items),
                "duplicate_email_count": len(duplicate_email_repair_items),
                "duplicate_employee_id_count": len(duplicate_employee_id_repair_items),
                "managed_username_collision_count": len(managed_username_collision_repair_items),
                "department_anomaly_count": (
                    len(missing_department_repair_items)
                    + len(placement_gap_repair_items)
                    + len(routing_ambiguity_repair_items)
                ),
                "naming_risk_count": (
                    len(naming_gap_repair_items)
                    + len(managed_username_collision_repair_items)
                ),
                "error_issue_count": error_issue_count,
                "warning_issue_count": warning_issue_count,
            },
            "connector_breakdown": [
                {
                    "connector_id": connector_id,
                    "name": connector_name,
                    "user_count": user_count,
                }
                for (connector_id, connector_name), user_count in sorted(
                    connector_counts.items(),
                    key=lambda item: (-item[1], item[0][1], item[0][0]),
                )
            ],
            "issues": normalized_issues,
            "repair_items": repair_items,
            "high_risk_items": high_risk_items,
        }

    def source_user_exists_in_source_provider(self, request: Request, source_user_id: str) -> tuple[bool, Optional[str]]:
        normalized_source_user_id = str(source_user_id or "").strip()
        if not normalized_source_user_id:
            return False, "Source user ID is required"
        try:
            _config, source_provider = self._get_source_provider(request)
            try:
                detail_payload = source_provider.get_user_detail(normalized_source_user_id) or {}
                if detail_payload:
                    return True, None
                results = source_provider.search_users(normalized_source_user_id, limit=100)
            finally:
                self._close_directory_resource(source_provider)
            if any(str(item.source_user_id or "").strip() == normalized_source_user_id for item in results):
                return True, None
        except Exception as exc:
            self.logger.warning("failed to validate source user existence: %s", exc)
            return True, None
        return False, f"Source user {normalized_source_user_id} does not exist in the configured source directory"

    def source_user_has_department(
        self,
        request: Request,
        source_user_id: str,
        department_id: str,
    ) -> tuple[bool, Optional[str]]:
        normalized_source_user_id = str(source_user_id or "").strip()
        normalized_department_id = str(department_id or "").strip()
        if not normalized_source_user_id or not normalized_department_id:
            return False, "Source user ID and primary department ID are required"
        departments = self.list_source_user_departments(request, normalized_source_user_id)
        if any(str(item.get("id") or "").strip() == normalized_department_id for item in departments):
            return True, None
        return (
            False,
            f"Department {normalized_department_id} is not one of source user {normalized_source_user_id}'s departments",
        )

    def target_user_exists_in_directory(self, request: Request, ad_username: str) -> tuple[bool, Optional[str]]:
        normalized_ad_username = str(ad_username or "").strip()
        if not normalized_ad_username:
            return False, "AD username is required"
        try:
            _config, target_provider = self._get_target_provider(request)
            try:
                records = target_provider.get_users_batch([normalized_ad_username])
            finally:
                self._close_directory_resource(target_provider)
            if normalized_ad_username in dict(records or {}):
                return True, None
        except Exception as exc:
            self.logger.warning("failed to validate AD user existence: %s", exc)
            return True, None
        return False, f"AD account {normalized_ad_username} does not exist in the configured directory"

    def department_exists_in_source_provider(self, request: Request, department_id: str) -> tuple[bool, Optional[str]]:
        try:
            int(department_id)
        except (TypeError, ValueError):
            return False, "Primary department ID must be an integer"

        try:
            current_org = self.request_support.get_current_org(request)
            config = request.app.state.org_config_repo.get_app_config(
                current_org.org_id,
                config_path=self.request_support.get_org_config_path(request),
            )
            is_valid, _errors = self.validate_config(config)
            if not is_valid:
                return True, None
            source_provider_name = self.get_source_provider_display_name(config.source_provider)
            source_provider = self.build_source_provider(
                app_config=config,
                logger=self.logger,
            )
            try:
                department_ids = {
                    str(item.department_id)
                    for item in source_provider.list_departments()
                    if item.department_id
                }
            finally:
                source_provider.close()
            if department_id not in department_ids:
                return False, f"{source_provider_name} department ID {department_id} does not exist"
        except Exception as exc:
            self.logger.warning("failed to validate department existence via source provider: %s", exc)

        return True, None

    def load_department_name_map(self, request: Request) -> dict[str, str]:
        try:
            organization = self.request_support.get_current_org(request)
            config = request.app.state.org_config_repo.get_app_config(
                organization.org_id,
                config_path=organization.config_path or request.app.state.config_path,
            )
            is_valid, _errors = self.validate_config(config)
            if not is_valid:
                return {}
            config_fingerprint = json.dumps(
                {"org_id": organization.org_id, "config": config.to_public_dict()},
                ensure_ascii=False,
                sort_keys=True,
            )
            cache_ttl = max(request.app.state.settings_repo.get_int("wecom_department_cache_ttl_seconds", 300), 0)
            now = time.time()
            if (
                cache_ttl > 0
                and self.department_name_cache["value"]
                and self.department_name_cache["config_fingerprint"] == config_fingerprint
                and self.department_name_cache["expires_at"] > now
            ):
                return dict(self.department_name_cache["value"])
            source_provider = self.build_source_provider(
                app_config=config,
                logger=self.logger,
            )
            try:
                department_name_map = {
                    str(item.department_id): str(item.name or "")
                    for item in source_provider.list_departments()
                    if item.department_id
                }
            finally:
                source_provider.close()
            self.department_name_cache["value"] = dict(department_name_map)
            self.department_name_cache["config_fingerprint"] = config_fingerprint
            self.department_name_cache["expires_at"] = now + cache_ttl
            return department_name_map
        except Exception as exc:
            self.logger.warning("failed to load department names via source provider: %s", exc)
            return {}

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
        if not app.state.settings_repo.get_bool("automatic_replay_enabled", False, org_id=org_id):
            return None
        return app.state.replay_request_repo.enqueue_request(
            request_type=request_type,
            execution_mode=execution_mode,
            requested_by=requested_by,
            org_id=org_id,
            target_scope=target_scope,
            target_id=target_id,
            trigger_reason=trigger_reason,
            payload=payload,
        )

    def build_conflicts_return_url(self, query: str, status: str, job_id: str) -> str:
        query_parts: dict[str, str] = {}
        if query:
            query_parts["q"] = query
        if status:
            query_parts["status"] = status
        if job_id:
            query_parts["job_id"] = job_id
        if not query_parts:
            return "/conflicts"
        return "/conflicts?" + urlencode(query_parts)

    def resolve_conflict_records_for_source(
        self,
        *,
        app: FastAPI,
        job_id: str,
        source_id: str,
        resolution_payload: dict[str, Any],
        actor_username: str,
    ) -> int:
        resolved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        return app.state.conflict_repo.resolve_open_conflicts_for_source(
            job_id=job_id,
            source_id=source_id,
            resolution_payload={
                **resolution_payload,
                "actor_username": actor_username,
            },
            resolved_at=resolved_at,
        )

    def apply_conflict_manual_binding(
        self,
        *,
        app: FastAPI,
        conflict: Any,
        ad_username: str,
        actor_username: str,
        org_id: str,
        notes: str = "",
    ) -> tuple[bool, str, int]:
        normalized_ad_username = str(ad_username or "").strip()
        if not conflict.source_id or not normalized_ad_username:
            return False, "Conflict does not support manual binding", 0

        conflict_message = None
        config = app.state.org_config_repo.get_app_config(org_id, config_path="")
        if self.is_protected_ad_account_name(normalized_ad_username, config.exclude_accounts):
            conflict_message = (
                f"AD account {normalized_ad_username} is system-protected and cannot be managed by sync."
            )
        else:
            existing_by_ad = app.state.user_binding_repo.get_binding_record_by_ad_username(
                normalized_ad_username,
                org_id=org_id,
            )
            if existing_by_ad and existing_by_ad.source_user_id != conflict.source_id:
                conflict_message = (
                    f"AD account {normalized_ad_username} is already bound to source user "
                    f"{existing_by_ad.source_user_id}. Resolve the existing binding first."
                )
        if conflict_message:
            return False, conflict_message, 0

        binding_notes = str(notes or "").strip() or f"resolved from conflict {conflict.id}"
        app.state.user_binding_repo.upsert_binding_for_source_user(
            conflict.source_id,
            normalized_ad_username,
            org_id=org_id,
            source="manual",
            notes=binding_notes,
            preserve_manual=False,
        )
        resolved_count = self.resolve_conflict_records_for_source(
            app=app,
            job_id=conflict.job_id,
            source_id=conflict.source_id,
            resolution_payload={
                "action": "manual_binding",
                "ad_username": normalized_ad_username,
                "notes": binding_notes,
                "source_conflict_id": conflict.id,
            },
            actor_username=actor_username,
        )
        self.enqueue_replay_request(
            app=app,
            request_type="conflict_resolution",
            requested_by=actor_username,
            org_id=org_id,
            target_scope="source_user",
            target_id=conflict.source_id,
            trigger_reason="manual_binding_resolved",
            payload={
                "conflict_id": conflict.id,
                "job_id": conflict.job_id,
                "action": "manual_binding",
                "ad_username": normalized_ad_username,
            },
        )
        return True, normalized_ad_username, resolved_count

    def apply_conflict_skip_user_sync(
        self,
        *,
        app: FastAPI,
        conflict: Any,
        actor_username: str,
        org_id: str,
        notes: str = "",
    ) -> tuple[bool, str, int]:
        if not conflict.source_id:
            return False, "Conflict does not have a source user to whitelist", 0

        rule_notes = str(notes or "").strip() or f"resolved from conflict {conflict.id}"
        app.state.exception_rule_repo.upsert_rule(
            rule_type="skip_user_sync",
            match_value=conflict.source_id,
            org_id=org_id,
            notes=rule_notes,
            is_enabled=True,
        )
        resolved_count = self.resolve_conflict_records_for_source(
            app=app,
            job_id=conflict.job_id,
            source_id=conflict.source_id,
            resolution_payload={
                "action": "skip_user_sync",
                "notes": rule_notes,
                "source_conflict_id": conflict.id,
            },
            actor_username=actor_username,
        )
        self.enqueue_replay_request(
            app=app,
            request_type="conflict_resolution",
            requested_by=actor_username,
            org_id=org_id,
            target_scope="source_user",
            target_id=conflict.source_id,
            trigger_reason="skip_user_sync_added",
            payload={
                "conflict_id": conflict.id,
                "job_id": conflict.job_id,
                "action": "skip_user_sync",
            },
        )
        return True, rule_notes, resolved_count

    def apply_conflict_recommendation(
        self,
        *,
        app: FastAPI,
        conflict: Any,
        actor_username: str,
        org_id: str,
        confirmation_reason: str = "",
    ) -> tuple[bool, str, int, Optional[dict[str, Any]]]:
        recommendation = self.recommend_conflict_resolution(conflict)
        if not recommendation:
            return False, "No recommendation is available for this conflict", 0, None

        action = str(recommendation.get("action") or "").strip().lower()
        reason = str(recommendation.get("reason") or "").strip()
        normalized_confirmation_reason = str(confirmation_reason or "").strip()
        if self.recommendation_requires_confirmation(recommendation) and not normalized_confirmation_reason:
            return False, "This recommendation requires a confirmation reason before it can be applied", 0, recommendation

        notes = normalized_confirmation_reason or reason or f"recommended resolution from conflict {conflict.id}"
        if action == "manual_binding":
            ok, detail, resolved_count = self.apply_conflict_manual_binding(
                app=app,
                conflict=conflict,
                ad_username=str(recommendation.get("ad_username") or ""),
                actor_username=actor_username,
                org_id=org_id,
                notes=notes,
            )
            return ok, detail, resolved_count, recommendation
        if action == "skip_user_sync":
            ok, detail, resolved_count = self.apply_conflict_skip_user_sync(
                app=app,
                conflict=conflict,
                actor_username=actor_username,
                org_id=org_id,
                notes=notes,
            )
            return ok, detail, resolved_count, recommendation
        return False, f"Unsupported recommendation action: {action or '-'}", 0, recommendation
