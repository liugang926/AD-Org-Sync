from __future__ import annotations

import csv
import io
import json
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional
from urllib.parse import urlencode

from fastapi import FastAPI, Request

from sync_app.core.models import SourceDirectoryUser
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
            enabled_value = trimmed_columns[3] if len(trimmed_columns) >= 4 else "true"
            rows.append(
                {
                    "line_number": line_number,
                    "rule_type": trimmed_columns[0],
                    "match_value": trimmed_columns[1],
                    "notes": trimmed_columns[2] if len(trimmed_columns) >= 3 else "",
                    "is_enabled": self.to_bool(enabled_value, True),
                    "expires_at": trimmed_columns[4] if len(trimmed_columns) >= 5 else "",
                    "is_once": self.to_bool(trimmed_columns[5], False) if len(trimmed_columns) >= 6 else False,
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
