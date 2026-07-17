from __future__ import annotations

import logging
import secrets
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import parse_qsl, urlencode

from fastapi import BackgroundTasks, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from sync_app.core.observability import redact_sensitive_text
from sync_app.providers.source import build_source_provider, get_source_provider_display_name
from sync_app.core.models import DepartmentNode
from sync_app.services.identity_relationships import (
    IdentityRelationshipPreviewService,
    classify_identity_relationship,
    filter_identity_workbench_rows,
    summarize_identity_workbench_rows,
)
from sync_app.services.high_risk_operations import (
    HighRiskOperationContext,
    HighRiskOperationPolicy,
    high_risk_audit_payload,
)
from sync_app.web.navigation import CANONICAL_ROUTE_PATHS
from sync_app.services.runtime_connectors import (
    build_department_connector_map,
    load_connector_specs,
)
from sync_app.services.runtime_bootstrap import build_runtime_config_fingerprint
from sync_app.services.source_directory import SourceDirectoryService
from sync_app.services.sync_policy_center import (
    USERNAME_STRATEGY_BY_SOURCE_FIELD,
    build_connector_policy_upsert,
)
from sync_app.web.app_state import (
    get_web_repositories,
    get_web_runtime_state,
    get_web_services,
)


LOGGER = logging.getLogger(__name__)
BINDING_CLEANUP_PREVIEW_SESSION_KEY = "_binding_cleanup_preview"
BINDING_CLEANUP_PREVIEW_MAX_AGE_SECONDS = 900
IDENTITY_WORKBENCH_DEFERRED_SESSION_KEY = "_identity_workbench_deferred"
SOURCE_DIRECTORY_VIEWS = {"overview", "users", "departments", "history"}
SOURCE_DIRECTORY_QUALITY_FILTERS = {
    "missing_employee_id",
    "duplicate_employee_id",
    "username_collision",
    "mapping_gap",
}


def _row_dict(row: Any) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def _is_expired(snapshot: Any) -> bool:
    if not snapshot or not str(snapshot["expires_at"] or ""):
        return False
    try:
        expires = datetime.fromisoformat(str(snapshot["expires_at"]))
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        return expires <= datetime.now(timezone.utc)
    except ValueError:
        return False


def _parse_timestamp(value: Any) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _refresh_duration_label(snapshot: dict[str, Any]) -> str:
    started_at = _parse_timestamp(snapshot.get("started_at"))
    completed_at = _parse_timestamp(snapshot.get("completed_at"))
    if started_at is None or completed_at is None:
        return "-"
    duration_seconds = max(int((completed_at - started_at).total_seconds()), 0)
    minutes, seconds = divmod(duration_seconds, 60)
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def register_source_directory_routes(
    app: FastAPI,
    *,
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    build_target_provider_for_connector: Callable[[Request, str], Any] | None = None,
) -> None:
    def current_environment_label(request: Request) -> str:
        return str(
            getattr(
                request.app.state,
                "environment_label",
                "Unlabeled environment",
            )
            or "Unlabeled environment"
        )

    def cleanup_context(
        request: Request,
        *,
        current_org: Any,
        snapshot: Any,
        impact_count: int,
        preview_id: str,
    ) -> HighRiskOperationContext:
        snapshot_id = int(snapshot["id"] or 0) if snapshot is not None else 0
        return HighRiskOperationContext.create(
            operation_code="binding.cleanup",
            organization_id=current_org.org_id,
            organization_name=current_org.name,
            environment_label=current_environment_label(request),
            snapshot_version=f"#{snapshot_id}" if snapshot_id else "Not available",
            impact_count=impact_count,
            preview_id=preview_id,
        )

    def context_from_preview(
        request: Request,
        *,
        current_org: Any,
        preview: dict[str, Any],
    ) -> HighRiskOperationContext:
        stored = dict(preview.get("context") or {})
        return HighRiskOperationContext.create(
            operation_code="binding.cleanup",
            organization_id=current_org.org_id,
            organization_name=current_org.name,
            environment_label=current_environment_label(request),
            snapshot_version=stored.get("snapshot_version") or "Not available",
            impact_count=int(stored.get("impact_count") or 0),
            preview_id=str(stored.get("preview_id") or ""),
        )

    def cleanup_workflow(preview: dict[str, Any] | None) -> list[dict[str, str]]:
        if not preview:
            return HighRiskOperationPolicy.workflow(scan_state="current")
        status = str(preview.get("status") or "").strip().lower()
        blocked_stage = str(preview.get("blocked_stage") or "").strip().lower()
        if status == "completed":
            return HighRiskOperationPolicy.workflow(
                scan_state="complete",
                preview_state="complete",
                confirm_state="complete",
                execute_state="complete",
                audit_state="complete",
            )
        if status == "blocked":
            return HighRiskOperationPolicy.workflow(
                scan_state="blocked" if blocked_stage == "scan" else "complete",
                preview_state="blocked" if blocked_stage == "preview" else "pending",
                confirm_state="blocked" if blocked_stage == "confirm" else "pending",
                execute_state="blocked" if blocked_stage == "execute" else "pending",
                audit_state="complete" if preview.get("audit_recorded") else "pending",
            )
        context = dict(preview.get("context") or {})
        confirmation_allowed = bool(context.get("environment_marked")) and int(
            context.get("impact_count") or 0
        ) > 0
        return HighRiskOperationPolicy.workflow(
            scan_state="complete",
            preview_state="complete",
            confirm_state="current" if confirmation_allowed else "blocked",
        )

    def stored_cleanup_preview(
        request: Request,
        *,
        current_org: Any,
        provider_id: str,
    ) -> dict[str, Any] | None:
        stored = dict(request.session.get(BINDING_CLEANUP_PREVIEW_SESSION_KEY) or {})
        if (
            str(stored.get("organization_id") or "") != current_org.org_id
            or str(stored.get("provider_id") or "") != provider_id
        ):
            return None
        context = context_from_preview(
            request,
            current_org=current_org,
            preview=stored,
        )
        stored["context"] = context.to_dict()
        stored["gate"] = HighRiskOperationPolicy.evaluate(context).to_dict()
        return stored

    def provider_for_current_config(request: Request, provider_type: str = ""):
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        runtime_state = get_web_runtime_state(request)
        config = repositories.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        provider = build_source_provider(
            app_config=config,
            provider_type=provider_type or config.source_provider,
            logger=LOGGER,
        )
        if hasattr(provider, "employee_id_attribute"):
            provider.employee_id_attribute = repositories.settings_repo.get_value(
                "source_employee_id_attribute", "", org_id=current_org.org_id
            ) or ""
        return config, provider

    def refresh_task(
        *,
        db_path: str,
        org_id: str,
        provider_id: str,
        config_path: str,
        created_by: str,
    ) -> None:
        from sync_app.storage.local_db import DatabaseManager
        from sync_app.storage.repositories import OrganizationConfigRepository, SettingsRepository, SourceDirectoryRepository

        manager = DatabaseManager(db_path=db_path)
        manager.initialize(create_startup_snapshot=False, verify_integrity=False)
        config = OrganizationConfigRepository(manager).get_app_config(org_id, config_path=config_path)
        provider = build_source_provider(app_config=config, provider_type=provider_id, logger=LOGGER)
        if hasattr(provider, "employee_id_attribute"):
            provider.employee_id_attribute = SettingsRepository(manager).get_value(
                "source_employee_id_attribute", "", org_id=org_id
            ) or ""
        try:
            SourceDirectoryService(SourceDirectoryRepository(manager), logger=LOGGER).refresh(
                org_id=org_id,
                provider_id=provider_id,
                provider=provider,
                created_by=created_by,
            )
        finally:
            provider.close()

    def relationship_context(
        request: Request,
        *,
        config: Any,
        snapshot: Any,
        scope: dict[str, Any] | None,
        departments: list[dict[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, str], dict[str, str]]:
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        connector_specs = load_connector_specs(
            config,
            repositories.connector_repo,
            connectors_enabled=repositories.settings_repo.get_bool(
                "advanced_connector_routing_enabled",
                False,
                org_id=current_org.org_id,
            ),
            org_id=current_org.org_id,
            default_root_department_ids=repositories.settings_repo.get_value(
                "source_root_unit_ids", "", org_id=current_org.org_id
            ),
            default_disabled_users_ou=repositories.settings_repo.get_value(
                "disabled_users_ou_path", "Disabled Users", org_id=current_org.org_id
            )
            or "Disabled Users",
            default_custom_group_ou_path=repositories.settings_repo.get_value(
                "custom_group_ou_path", "Managed Groups", org_id=current_org.org_id
            )
            or "Managed Groups",
            default_user_root_ou_path=repositories.settings_repo.get_value(
                "directory_root_ou_path", "", org_id=current_org.org_id
            )
            or "",
        )
        if scope:
            for spec in connector_specs:
                if str(spec.get("connector_id") or "default") != str(
                    scope.get("connector_id") or "default"
                ):
                    continue
                spec["username_strategy"] = scope.get("username_strategy") or "userid"
                spec["username_template"] = scope.get("username_template") or ""
        specs_by_id = {
            str(spec.get("connector_id") or "default"): spec for spec in connector_specs
        }
        dept_tree = {
            int(row["source_department_id"]): DepartmentNode(
                department_id=int(row["source_department_id"]),
                name=str(row.get("name") or ""),
                parent_id=int(row.get("parent_department_id") or 0),
                path=list(row.get("path_names") or []),
                path_ids=[int(value) for value in row.get("path_ids") or []],
            )
            for row in departments
            if str(row.get("source_department_id") or "").isdigit()
        }
        department_connector_map = build_department_connector_map(dept_tree, connector_specs)
        field_labels = {
            str(row.get("field_name") or ""): str(
                row.get("field_label") or row.get("field_name") or ""
            )
            for row in repositories.source_directory_repo.list_field_catalog(
                int(snapshot["id"]), org_id=current_org.org_id
            )
        }
        return specs_by_id, {
            str(key): str(value) for key, value in department_connector_map.items()
        }, field_labels

    def connector_assignments(
        users: list[dict[str, Any]],
        department_connector_map: dict[str, str],
    ) -> dict[str, str]:
        assignments: dict[str, str] = {}
        for row in users:
            candidates = {
                department_connector_map.get(str(value), "default")
                for value in row.get("department_ids") or []
            }
            candidates.discard("")
            assignments[str(row.get("source_user_id") or "")] = (
                "__conflict__" if len(candidates) > 1 else next(iter(candidates or {"default"}))
            )
        return assignments

    def build_source_catalog_page(
        request: Request,
        *,
        page_number: int,
        page_size: int,
        search: str,
        department_id: str,
        status: str,
        employee_id_state: str,
        snapshot_id: int | None = None,
        quality_filter: str = "",
        include_quality: bool = False,
        include_users: bool = True,
        include_department_counts: bool = False,
    ) -> dict[str, Any]:
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        runtime_state = get_web_runtime_state(request)
        config = repositories.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        provider_id = str(config.source_provider or "").strip().lower()
        active_snapshot = repositories.source_directory_repo.get_latest_successful_snapshot(
            org_id=current_org.org_id,
            provider_id=provider_id,
        )
        requested_snapshot = (
            repositories.source_directory_repo.get_snapshot(
                int(snapshot_id),
                org_id=current_org.org_id,
            )
            if snapshot_id
            else None
        )
        if (
            requested_snapshot is not None
            and (
                str(requested_snapshot["provider_id"] or "") != provider_id
                or str(requested_snapshot["status"] or "") != "succeeded"
            )
        ):
            requested_snapshot = None
        snapshot = requested_snapshot or active_snapshot
        latest_refresh = repositories.source_directory_repo.get_latest_refresh(
            org_id=current_org.org_id,
            provider_id=provider_id,
        )
        if not snapshot:
            return {
                "provider_id": provider_id,
                "config": config,
                "snapshot": None,
                "active_snapshot": active_snapshot,
                "latest_refresh": latest_refresh,
                "departments": [],
                "department_rows": [],
                "users": [],
                "total": 0,
                "field_count": 0,
                "scope": None,
                "mapping_quality": {
                    "mapping_coverage_percent": 0.0,
                    "normalized_username_collision_count": 0,
                    "issues_by_user": {},
                },
            }
        departments = repositories.source_directory_repo.list_departments(
            int(snapshot["id"]),
            org_id=current_org.org_id,
        )
        scope = repositories.source_directory_repo.get_scope_selection(
            org_id=current_org.org_id,
            provider_id=provider_id,
        ) or {}
        mapping_quality = {
            "mapping_coverage_percent": 0.0,
            "normalized_username_collision_count": 0,
            "issues_by_user": {},
        }
        normalized_quality_filter = str(quality_filter or "").strip().lower()
        if normalized_quality_filter not in SOURCE_DIRECTORY_QUALITY_FILTERS:
            normalized_quality_filter = ""
        if include_quality:
            mapping_quality = SourceDirectoryService(
                repositories.source_directory_repo
            ).build_mapping_quality_report(
                snapshot_id=int(snapshot["id"]),
                org_id=current_org.org_id,
                provider_id=provider_id,
                username_strategy=str(scope.get("username_strategy") or "userid"),
                username_template=str(scope.get("username_template") or ""),
                source_field=str(scope.get("source_field") or "source_user_id"),
            )
        filtered_source_user_ids: list[str] | None = None
        if normalized_quality_filter in {"username_collision", "mapping_gap"}:
            issue_key = (
                "normalized_username_collision"
                if normalized_quality_filter == "username_collision"
                else "mapping_field_missing"
            )
            filtered_source_user_ids = [
                str(source_user_id)
                for source_user_id, issues in dict(
                    mapping_quality.get("issues_by_user") or {}
                ).items()
                if issue_key in list(issues or [])
            ]
        effective_employee_id_state = str(employee_id_state or "").strip().lower()
        if normalized_quality_filter == "missing_employee_id":
            effective_employee_id_state = "missing"
        elif normalized_quality_filter == "duplicate_employee_id":
            effective_employee_id_state = "duplicate"
        if not include_users:
            result = {
                "items": [],
                "total": int(snapshot["user_count"] or 0),
            }
        elif filtered_source_user_ids == []:
            result = {
                "items": [],
                "total": 0,
            }
        else:
            result = repositories.source_directory_repo.list_users(
                int(snapshot["id"]),
                org_id=current_org.org_id,
                provider_id=provider_id,
                search=search,
                department_id=department_id,
                status=status,
                employee_id_state=effective_employee_id_state,
                source_user_ids=filtered_source_user_ids,
                limit=page_size,
                offset=(max(int(page_number or 1), 1) - 1) * page_size,
            )
        users = []
        for row in result["items"]:
            quality_issues = list(
                dict(mapping_quality.get("issues_by_user") or {}).get(
                    str(row.get("source_user_id") or ""),
                    [],
                )
            )
            if not str(row.get("employee_id") or "").strip():
                quality_issues.append("missing_employee_id")
            if not str(row.get("email") or "").strip():
                quality_issues.append("missing_email")
            if not list(row.get("department_ids") or []):
                quality_issues.append("missing_department")
            users.append(
                {
                    **row,
                    "quality_issues": sorted(set(quality_issues)),
                }
            )
        selected_source_user_ids = (
            list(scope.get("selected_source_user_ids") or [])
            if str(scope.get("scope_type") or "") in {"selected_users", "source_user"}
            else None
        )
        department_counts = (
            repositories.source_directory_repo.get_department_user_counts(
                int(snapshot["id"]),
                org_id=current_org.org_id,
                selected_source_user_ids=selected_source_user_ids,
            )
            if include_department_counts
            else {}
        )
        selected_department_ids = {
            str(value)
            for value in list(scope.get("selected_department_ids") or [])
        }
        scope_type = str(scope.get("scope_type") or "")
        department_rows = []
        department_name_by_id = {
            str(item.get("source_department_id") or ""): str(item.get("name") or "")
            for item in departments
        }
        for department in (departments if include_department_counts else []):
            department_id_value = str(
                department.get("source_department_id") or ""
            )
            path_ids = {
                str(value)
                for value in list(department.get("path_ids") or [])
            }
            count_data = department_counts.get(
                department_id_value,
                {"total": 0, "selected": 0},
            )
            if not scope_type:
                scope_status = "Not configured"
            elif scope_type == "full":
                scope_status = "Included"
            elif scope_type == "department":
                scope_status = (
                    "Included"
                    if path_ids & selected_department_ids
                    else "Excluded"
                )
            else:
                selected_count = int(count_data.get("selected") or 0)
                total_count = int(count_data.get("total") or 0)
                if selected_count <= 0:
                    scope_status = "Excluded"
                elif selected_count >= total_count:
                    scope_status = "Included"
                else:
                    scope_status = "Partial"
            parent_department_id = str(
                department.get("parent_department_id") or ""
            )
            department_rows.append(
                {
                    **department,
                    "user_count": int(count_data.get("total") or 0),
                    "parent_department_name": (
                        department_name_by_id.get(parent_department_id)
                        if parent_department_id not in {"", "0"}
                        else ""
                    ),
                    "scope_status": scope_status,
                    "depth": min(
                        max(
                            len(list(department.get("path_names") or [])) - 1,
                            0,
                        ),
                        8,
                    ),
                }
            )
        return {
            "provider_id": provider_id,
            "config": config,
            "snapshot": snapshot,
            "active_snapshot": active_snapshot,
            "latest_refresh": latest_refresh,
            "departments": departments,
            "department_rows": department_rows,
            "users": users,
            "total": int(result["total"]),
            "field_count": int(snapshot["field_count"] or 0),
            "scope": scope,
            "mapping_quality": mapping_quality,
        }

    def build_relationship_page(
        request: Request,
        *,
        page_number: int,
        page_size: int,
        search: str,
        department_id: str,
        status: str,
        employee_id_state: str,
        relationship_status: str,
        verify_ad: bool,
        source_user_ids: list[str] | None = None,
        workbench_queue: str = "",
        identity_status: str = "",
        ad_status: str = "",
        include_workbench_summary: bool = False,
        deferred_source_user_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        runtime_state = get_web_runtime_state(request)
        config = repositories.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        provider_id = str(config.source_provider or "").strip().lower()
        config_fingerprint = build_runtime_config_fingerprint(
            config=config,
            organization=current_org,
            settings_repo=repositories.settings_repo,
            exclusion_repo=repositories.exclusion_repo,
            exception_rule_repo=repositories.exception_rule_repo,
            mapping_rule_repo=repositories.attribute_mapping_repo,
            department_ou_mapping_repo=repositories.department_ou_mapping_repo,
            connector_repo=repositories.connector_repo,
        )
        snapshot = repositories.source_directory_repo.get_latest_successful_snapshot(
            org_id=current_org.org_id,
            provider_id=provider_id,
        )
        if not snapshot:
            return {
                "config": config,
                "provider_id": provider_id,
                "snapshot": None,
                "scope": None,
                "departments": [],
                "fields": [],
                "relationships": [],
                "total": 0,
                "ad_verified": False,
                "candidate_missing_count": 0,
                "creation_eligible_count": 0,
                "mapping_quality": {},
                "workbench_rows": [],
                "workbench_counts": summarize_identity_workbench_rows([]),
            }
        scope = repositories.source_directory_repo.get_scope_selection(
            org_id=current_org.org_id,
            provider_id=provider_id,
        ) or {}
        departments = repositories.source_directory_repo.list_departments(
            int(snapshot["id"]), org_id=current_org.org_id
        )
        fields = repositories.source_directory_repo.list_field_catalog(
            int(snapshot["id"]), org_id=current_org.org_id
        )
        specs_by_id, department_connector_map, field_labels = relationship_context(
            request,
            config=config,
            snapshot=snapshot,
            scope=scope,
            departments=departments,
        )
        mapping_quality = SourceDirectoryService(
            repositories.source_directory_repo
        ).build_mapping_quality_report(
            snapshot_id=int(snapshot["id"]),
            org_id=current_org.org_id,
            provider_id=provider_id,
            username_strategy=str((scope or {}).get("username_strategy") or "userid"),
            username_template=str((scope or {}).get("username_template") or ""),
            source_field=str((scope or {}).get("source_field") or "source_user_id"),
        )
        candidate_collision_source_ids = {
            source_user_id
            for source_user_id, issues in dict(
                mapping_quality.get("issues_by_user") or {}
            ).items()
            if "normalized_username_collision" in issues
        }
        offset = (max(int(page_number or 1), 1) - 1) * page_size
        requires_relationship_filter = (
            str(relationship_status or "all").strip().lower() not in {"", "all"}
        )
        requires_full_relationship_set = bool(
            requires_relationship_filter
            or include_workbench_summary
            or str(workbench_queue or "").strip()
            or str(identity_status or "").strip()
            or str(ad_status or "").strip()
        )
        if source_user_ids:
            result = repositories.source_directory_repo.list_users(
                int(snapshot["id"]),
                org_id=current_org.org_id,
                provider_id=provider_id,
                source_user_ids=source_user_ids,
                limit=min(max(len(source_user_ids), 1), 200),
            )
            base_users = result["items"]
            total = int(result["total"])
        elif requires_full_relationship_set:
            base_users = []
            source_offset = 0
            total = 0
            while True:
                result = repositories.source_directory_repo.list_users(
                    int(snapshot["id"]),
                    org_id=current_org.org_id,
                    provider_id=provider_id,
                    search=search,
                    department_id=department_id,
                    status=status,
                    employee_id_state=employee_id_state,
                    limit=200,
                    offset=source_offset,
                )
                base_users.extend(result["items"])
                total = int(result["total"])
                source_offset += len(result["items"])
                if source_offset >= total or not result["items"]:
                    break
        else:
            result = repositories.source_directory_repo.list_users(
                int(snapshot["id"]),
                org_id=current_org.org_id,
                provider_id=provider_id,
                search=search,
                department_id=department_id,
                status=status,
                employee_id_state=employee_id_state,
                limit=page_size,
                offset=offset,
            )
            base_users = result["items"]
            total = int(result["total"])

        service = IdentityRelationshipPreviewService(
            source_directory_repo=repositories.source_directory_repo,
            user_binding_repo=repositories.user_binding_repo,
            operation_log_repo=repositories.operation_log_repo,
            planned_operation_repo=repositories.planned_operation_repo,
        )
        assignments = connector_assignments(base_users, department_connector_map)
        relationships = service.build_relationships(
            base_users,
            org_id=current_org.org_id,
            source_provider=provider_id,
            snapshot=snapshot,
            scope=scope,
            connector_specs_by_id=specs_by_id,
            connector_ids_by_source_user=assignments,
            field_labels=field_labels,
            config_fingerprint=config_fingerprint,
            candidate_collision_source_ids=candidate_collision_source_ids,
        )

        ad_verified = False
        if verify_ad and relationships and build_target_provider_for_connector:
            usernames_by_connector: dict[str, set[str]] = {}
            for item in relationships:
                for username in item.ad_query_usernames:
                    if item.connector_id != "__conflict__":
                        usernames_by_connector.setdefault(item.connector_id, set()).add(
                            username
                        )
                for username in (
                    item.before_state.get("bound_ad_username"),
                    item.candidate_mapping.get("ad_username"),
                    item.planned_after_state.get("ad_username"),
                ):
                    if str(username or "").strip() and item.connector_id != "__conflict__":
                        usernames_by_connector.setdefault(item.connector_id, set()).add(
                            str(username).strip()
                        )
            protected_by_connector = {
                connector_id: list(
                    getattr(spec.get("config"), "exclude_accounts", []) or []
                )
                for connector_id, spec in specs_by_id.items()
            }
            ad_states = service.load_ad_states(
                lambda connector_id: build_target_provider_for_connector(
                    request, connector_id
                ),
                usernames_by_connector,
                protected_accounts_by_connector=protected_by_connector,
            )
            relationship_user_ids = [item.source_user_id for item in relationships]
            users_by_id = {
                str(item.get("source_user_id") or ""): item for item in base_users
            }
            relationship_users = [
                users_by_id[user_id] for user_id in relationship_user_ids
            ]
            relationships = service.build_relationships(
                relationship_users,
                org_id=current_org.org_id,
                source_provider=provider_id,
                snapshot=snapshot,
                scope=scope,
                connector_specs_by_id=specs_by_id,
                connector_ids_by_source_user={
                    user_id: assignments[user_id] for user_id in relationship_user_ids
                },
                field_labels=field_labels,
                ad_states=ad_states,
                config_fingerprint=config_fingerprint,
                candidate_collision_source_ids=candidate_collision_source_ids,
            )
            ad_verified = True

        if requires_relationship_filter:
            relationships = [
                item
                for item in relationships
                if service.matches_filter(item, relationship_status)
            ]

        relationship_source_user_ids = {
            item.source_user_id for item in relationships
        }
        if relationship_source_user_ids:
            conflict_records = repositories.conflict_repo.list_conflict_records(
                limit=500,
                org_id=current_org.org_id,
            )
            audit_records, _audit_total = repositories.audit_repo.list_recent_logs_page(
                limit=500,
                offset=0,
                org_id=current_org.org_id,
                include_global=False,
            )
            for item in relationships:
                item.evidence["conflict_records"] = [
                    {
                        "id": int(record.id or 0),
                        "job_id": record.job_id,
                        "type": record.conflict_type,
                        "status": record.status,
                        "message": record.message,
                        "created_at": record.created_at,
                    }
                    for record in conflict_records
                    if record.source_id == item.source_user_id
                ][:10]
                item.evidence["audit_records"] = [
                    {
                        "id": int(record.id or 0),
                        "actor": record.actor_username,
                        "action": record.action_type,
                        "result": record.result,
                        "message": record.message,
                        "created_at": record.created_at,
                    }
                    for record in audit_records
                    if (
                        record.target_id == item.source_user_id
                        or str((record.payload or {}).get("source_user_id") or "")
                        == item.source_user_id
                        or item.source_user_id
                        in {
                            str(value or "").strip()
                            for value in (
                                (record.payload or {}).get("source_user_ids") or []
                            )
                            if str(value or "").strip()
                        }
                    )
                ][:10]

        deferred_ids = set(deferred_source_user_ids or set())
        all_workbench_rows = [
            {
                "relationship": item,
                "workbench": classify_identity_relationship(
                    item,
                    deferred=item.source_user_id in deferred_ids,
                ),
            }
            for item in relationships
        ]
        filtered_for_counts = filter_identity_workbench_rows(
            all_workbench_rows,
            queue="all",
            identity_status=identity_status,
            ad_status=ad_status,
        )
        workbench_counts = summarize_identity_workbench_rows(filtered_for_counts)
        filtered_workbench_rows = filter_identity_workbench_rows(
            filtered_for_counts,
            queue=workbench_queue or "all",
        )

        if requires_full_relationship_set:
            total = len(filtered_workbench_rows)
            workbench_rows = filtered_workbench_rows[offset : offset + page_size]
            relationships = [row["relationship"] for row in workbench_rows]
        else:
            workbench_rows = filtered_workbench_rows

        candidate_missing_count = sum(
            1
            for item in relationships
            if item.candidate_ad_state.get("status") == "missing"
        )
        creation_eligible_count = sum(
            1 for item in relationships if item.creation_eligibility.get("eligible")
        )

        return {
            "config": config,
            "provider_id": provider_id,
            "snapshot": snapshot,
            "scope": scope,
            "departments": departments,
            "fields": fields,
            "relationships": relationships,
            "total": total,
            "ad_verified": ad_verified,
            "candidate_missing_count": candidate_missing_count,
            "creation_eligible_count": creation_eligible_count,
            "mapping_quality": mapping_quality,
            "workbench_rows": workbench_rows,
            "workbench_counts": workbench_counts,
        }

    def render_source_directory_page(
        request: Request,
        *,
        active_view: str,
        page_number: int,
        search: str = "",
        department_id: str = "",
        user_status: str = "",
        employee_id_state: str = "",
        quality_filter: str = "",
        snapshot_id: int | None = None,
        history_provider_id: str = "",
        history_status: str = "",
    ):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        normalized_view = str(active_view or "overview").strip().lower()
        if normalized_view not in SOURCE_DIRECTORY_VIEWS:
            normalized_view = "overview"
        current_org = get_current_org(request)
        page_size = 50
        page_number = max(int(page_number or 1), 1)
        normalized_quality_filter = str(quality_filter or "").strip().lower()
        if normalized_quality_filter not in SOURCE_DIRECTORY_QUALITY_FILTERS:
            normalized_quality_filter = ""
        page_data = build_source_catalog_page(
            request,
            page_number=page_number,
            page_size=page_size,
            search=search,
            department_id=department_id,
            status=user_status,
            employee_id_state=employee_id_state,
            snapshot_id=snapshot_id,
            quality_filter=normalized_quality_filter,
            include_quality=(
                normalized_view == "overview"
                or (
                    normalized_view == "users"
                    and normalized_quality_filter
                    in {"username_collision", "mapping_gap"}
                )
            ),
            include_users=normalized_view == "users",
            include_department_counts=normalized_view == "departments",
        )
        provider_id = page_data["provider_id"]
        snapshot = page_data["snapshot"]
        total_pages = max((int(page_data["total"]) + page_size - 1) // page_size, 1)
        active_snapshot = page_data["active_snapshot"]
        viewing_historical_snapshot = bool(
            snapshot
            and active_snapshot
            and int(snapshot["id"]) != int(active_snapshot["id"])
        )
        latest_refresh = page_data["latest_refresh"]
        refresh_in_progress = bool(
            latest_refresh
            and str(latest_refresh["status"] or "") == "refreshing"
        )
        snapshot_expired = _is_expired(snapshot)

        runtime_state = get_web_runtime_state(request)
        editable = get_web_repositories(request).org_config_repo.get_editable_config(
            current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        source_connection_ready = bool(
            str(editable.get("corpid") or "").strip()
            and bool(editable.get("corpsecret_configured"))
        )

        history_rows: list[dict[str, Any]] = []
        total_snapshots = 0
        history_total_pages = 1
        selected_history_snapshot: dict[str, Any] | None = None
        if normalized_view == "history":
            history_page_size = 30
            effective_history_provider_id = (
                str(history_provider_id or "").strip().lower() or provider_id
            )
            history = get_web_repositories(
                request
            ).source_directory_repo.list_snapshots(
                org_id=current_org.org_id,
                provider_id=effective_history_provider_id,
                status=history_status,
                limit=history_page_size,
                offset=(page_number - 1) * history_page_size,
            )
            all_recent = get_web_repositories(
                request
            ).source_directory_repo.list_snapshots(
                org_id=current_org.org_id,
                provider_id=effective_history_provider_id,
                limit=200,
            )["items"]
            previous_by_id: dict[int, dict[str, Any] | None] = {}
            for item in all_recent:
                previous_by_id[int(item["id"])] = next(
                    (
                        candidate
                        for candidate in all_recent
                        if int(candidate["id"]) < int(item["id"])
                        and str(candidate.get("provider_id") or "")
                        == str(item.get("provider_id") or "")
                        and str(candidate.get("status") or "") == "succeeded"
                    ),
                    None,
                )
            for item in history["items"]:
                previous = previous_by_id.get(int(item["id"]))
                current_quality_issues = (
                    int(item.get("missing_employee_id_count") or 0)
                    + int(item.get("duplicate_employee_id_count") or 0)
                )
                previous_quality_issues = (
                    int(previous.get("missing_employee_id_count") or 0)
                    + int(previous.get("duplicate_employee_id_count") or 0)
                    if previous
                    else None
                )
                history_rows.append(
                    {
                        "snapshot": {
                            **item,
                            "error_summary": redact_sensitive_text(
                                item.get("error_summary")
                            ),
                        },
                        "quality_issue_count": current_quality_issues,
                        "quality_delta": (
                            current_quality_issues - previous_quality_issues
                            if (
                                previous_quality_issues is not None
                                and str(item.get("status") or "") == "succeeded"
                            )
                            else None
                        ),
                        "duration_label": _refresh_duration_label(item),
                    }
                )
            total_snapshots = int(history["total"])
            history_total_pages = max(
                (total_snapshots + history_page_size - 1) // history_page_size,
                1,
            )
            requested_history_snapshot = (
                get_web_repositories(request).source_directory_repo.get_snapshot(
                    int(snapshot_id),
                    org_id=current_org.org_id,
                )
                if snapshot_id
                else None
            )
            selected_history_snapshot = _row_dict(
                requested_history_snapshot
                or (history["items"][0] if history["items"] else None)
            )

        latest_refresh_context = _row_dict(latest_refresh)
        if latest_refresh_context is not None:
            latest_refresh_context["error_summary"] = redact_sensitive_text(
                latest_refresh_context.get("error_summary")
            )
        return render(
            request,
            "source_directory.html",
            page="source-directory",
            title="Source Directory",
            current_org=current_org,
            active_view=normalized_view,
            provider_id=provider_id,
            provider_name=get_source_provider_display_name(provider_id),
            snapshot=_row_dict(snapshot),
            active_snapshot=_row_dict(active_snapshot),
            latest_refresh=latest_refresh_context,
            refresh_in_progress=refresh_in_progress,
            snapshot_expired=snapshot_expired,
            viewing_historical_snapshot=viewing_historical_snapshot,
            high_risk_actions_blocked=(
                snapshot_expired
                or viewing_historical_snapshot
                or refresh_in_progress
                or bool(
                    latest_refresh
                    and str(latest_refresh["status"] or "") == "failed"
                )
            ),
            source_connection_ready=source_connection_ready,
            users=page_data["users"],
            total_users=page_data["total"],
            departments=page_data["departments"],
            department_rows=page_data["department_rows"],
            field_count=page_data["field_count"],
            mapping_quality=page_data["mapping_quality"],
            page_number=page_number,
            total_pages=total_pages,
            search=search,
            selected_department_id=department_id,
            selected_status=user_status,
            selected_employee_id_state=employee_id_state,
            selected_quality_filter=normalized_quality_filter,
            history_rows=history_rows,
            total_snapshots=total_snapshots,
            history_total_pages=history_total_pages,
            selected_history_provider_id=(
                str(history_provider_id or "").strip().lower() or provider_id
            ),
            selected_history_status=history_status,
            selected_history_snapshot=selected_history_snapshot,
        )

    @app.get(CANONICAL_ROUTE_PATHS["source-directory"], response_class=HTMLResponse)
    @app.get("/source-directory", response_class=HTMLResponse)
    def source_directory_page(
        request: Request,
        view: str = "overview",
        page_number: int = 1,
        search: str = "",
        department_id: str = "",
        status: str = "",
        employee_id_state: str = "",
        quality: str = "",
        snapshot_id: int | None = None,
        snapshot_status: str = "",
    ):
        return render_source_directory_page(
            request,
            active_view=view,
            page_number=page_number,
            search=search,
            department_id=department_id,
            user_status=status,
            employee_id_state=employee_id_state,
            quality_filter=quality,
            snapshot_id=snapshot_id,
            history_status=snapshot_status,
        )

    @app.get(CANONICAL_ROUTE_PATHS["snapshots"], response_class=HTMLResponse)
    def source_snapshot_history_page(
        request: Request,
        page_number: int = 1,
        provider_id: str = "",
        status: str = "",
        snapshot_id: int | None = None,
    ):
        return render_source_directory_page(
            request,
            active_view="history",
            page_number=page_number,
            snapshot_id=snapshot_id,
            history_provider_id=provider_id,
            history_status=status,
        )

    @app.get(CANONICAL_ROUTE_PATHS["identity-matching"], response_class=HTMLResponse)
    def identity_matching_page(
        request: Request,
        page_number: int = 1,
        search: str = "",
        relationship_status: str = "all",
        verify_ad: bool = False,
        queue: str = "pending",
        department_id: str = "",
        employee_status: str = "",
        identity_status: str = "",
        ad_status: str = "",
        mode: str = "basic",
    ):
        user = require_capability(request, "mappings.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        page_size = 50
        page_number = max(int(page_number or 1), 1)
        normalized_queue = str(queue or "pending").strip().lower()
        if normalized_queue not in {
            "pending",
            "creatable",
            "unbound",
            "bound",
            "conflict",
            "all",
        }:
            normalized_queue = "pending"
        normalized_employee_status = str(employee_status or "").strip().lower()
        if normalized_employee_status not in {"", "active", "inactive"}:
            normalized_employee_status = ""
        normalized_identity_status = str(identity_status or "").strip().lower()
        allowed_identity_statuses = {
            "bound_account_exists",
            "unbound_candidate_exists",
            "creatable",
            "saved_binding_expired",
            "candidate_binding_mismatch",
            "multiple_user_candidate_conflict",
            "ad_status_unknown",
            "connector_unavailable",
            "candidate_unavailable",
            "unbound_candidate_missing",
        }
        if normalized_identity_status not in allowed_identity_statuses:
            normalized_identity_status = ""
        normalized_ad_status = str(ad_status or "").strip().lower()
        if normalized_ad_status not in {
            "",
            "exists",
            "enabled",
            "disabled",
            "locked",
            "missing",
            "protected",
            "unknown",
            "unavailable",
        }:
            normalized_ad_status = ""
        normalized_mode = (
            "advanced" if str(mode or "").strip().lower() == "advanced" else "basic"
        )
        repositories = get_web_repositories(request)
        runtime_state = get_web_runtime_state(request)
        config = repositories.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        provider_id = str(config.source_provider or "").strip().lower()
        current_snapshot = (
            repositories.source_directory_repo.get_latest_successful_snapshot(
                org_id=current_org.org_id,
                provider_id=provider_id,
            )
        )
        deferred_state = dict(
            request.session.get(IDENTITY_WORKBENCH_DEFERRED_SESSION_KEY) or {}
        )
        deferred_scope_matches = bool(
            current_snapshot
            and deferred_state.get("org_id") == current_org.org_id
            and deferred_state.get("provider_id") == provider_id
            and int(deferred_state.get("snapshot_id") or 0)
            == int(current_snapshot["id"] or 0)
        )
        deferred_source_user_ids = (
            {
                str(value or "").strip()
                for value in deferred_state.get("source_user_ids") or []
                if str(value or "").strip()
            }
            if deferred_scope_matches
            else set()
        )
        page_data = build_relationship_page(
            request,
            page_number=page_number,
            page_size=page_size,
            search=search,
            department_id=department_id,
            status=normalized_employee_status,
            employee_id_state="",
            relationship_status=relationship_status,
            verify_ad=verify_ad,
            workbench_queue=normalized_queue,
            identity_status=normalized_identity_status,
            ad_status=normalized_ad_status,
            include_workbench_summary=True,
            deferred_source_user_ids=deferred_source_user_ids,
        )

        base_query = {
            "queue": normalized_queue,
            "search": str(search or "").strip(),
            "department_id": str(department_id or "").strip(),
            "employee_status": normalized_employee_status,
            "identity_status": normalized_identity_status,
            "ad_status": normalized_ad_status,
            "mode": normalized_mode,
        }
        if verify_ad:
            base_query["verify_ad"] = "true"

        def workbench_url(
            *,
            updates: dict[str, Any] | None = None,
            remove: tuple[str, ...] = (),
        ) -> str:
            query_values = {**base_query, **dict(updates or {})}
            for key in remove:
                query_values.pop(key, None)
            query_values = {
                key: value
                for key, value in query_values.items()
                if str(value or "").strip()
            }
            return CANONICAL_ROUTE_PATHS["identity-matching"] + (
                "?" + urlencode(query_values) if query_values else ""
            )

        queue_labels = {
            "pending": "Pending identities",
            "creatable": "Creatable",
            "unbound": "Unbound",
            "bound": "Bound",
            "conflict": "Conflict",
            "all": "All",
        }
        queue_tabs = [
            {
                "value": value,
                "label": label,
                "count": int(page_data["workbench_counts"].get(value) or 0),
                "href": workbench_url(
                    updates={"queue": value, "page_number": 1},
                    remove=("page_number",),
                ),
            }
            for value, label in queue_labels.items()
        ]
        department_names = {
            str(item.get("source_department_id") or ""): str(item.get("name") or "")
            for item in page_data["departments"]
        }
        identity_labels = {
            "bound_account_exists": "Bound and AD account exists",
            "unbound_candidate_exists": "No binding; candidate AD account exists",
            "creatable": "Can create account",
            "saved_binding_expired": "Saved binding has expired",
            "candidate_binding_mismatch": "Candidate differs from saved binding",
            "multiple_user_candidate_conflict": "Candidate account conflict",
            "ad_status_unknown": "AD status unknown",
            "connector_unavailable": "Connector unavailable",
            "candidate_unavailable": "Candidate unavailable",
            "unbound_candidate_missing": "Unbound candidate is missing",
        }
        ad_status_labels = {
            "exists": "Exists",
            "enabled": "Enabled",
            "disabled": "Disabled",
            "locked": "Locked",
            "missing": "Missing",
            "protected": "Protected",
            "unknown": "Unknown",
            "unavailable": "Connector unavailable",
        }
        active_filters = []
        for key, label, value, display_value in (
            ("search", "Keyword", str(search or "").strip(), str(search or "").strip()),
            (
                "department_id",
                "Department",
                str(department_id or "").strip(),
                department_names.get(str(department_id or "").strip(), str(department_id or "").strip()),
            ),
            (
                "employee_status",
                "Employee status",
                normalized_employee_status,
                normalized_employee_status.capitalize(),
            ),
            (
                "identity_status",
                "Identity status",
                normalized_identity_status,
                identity_labels.get(normalized_identity_status, normalized_identity_status),
            ),
            (
                "ad_status",
                "AD status",
                normalized_ad_status,
                ad_status_labels.get(normalized_ad_status, normalized_ad_status),
            ),
        ):
            if value:
                active_filters.append(
                    {
                        "key": key,
                        "label": label,
                        "value": display_value,
                        "remove_href": workbench_url(
                            updates={"page_number": 1},
                            remove=(key, "page_number"),
                        ),
                    }
                )
        total_pages = max(
            (int(page_data["total"]) + page_size - 1) // page_size,
            1,
        )
        return render(
            request,
            "identity_matching.html",
            page="identity-matching",
            title="Identity Matching",
            current_org=current_org,
            provider_id=page_data["provider_id"],
            provider_name=get_source_provider_display_name(page_data["provider_id"]),
            snapshot=_row_dict(page_data["snapshot"]),
            snapshot_expired=_is_expired(page_data["snapshot"]),
            identity_matches=page_data["workbench_rows"],
            workbench_counts=page_data["workbench_counts"],
            queue_tabs=queue_tabs,
            total_users=page_data["total"],
            page_number=page_number,
            total_pages=total_pages,
            search=search,
            selected_queue=normalized_queue,
            selected_department_id=str(department_id or "").strip(),
            selected_employee_status=normalized_employee_status,
            selected_identity_status=normalized_identity_status,
            selected_ad_status=normalized_ad_status,
            selected_mode=normalized_mode,
            departments=page_data["departments"],
            identity_status_options=identity_labels.items(),
            ad_status_options=ad_status_labels.items(),
            active_filters=active_filters,
            ad_verified=page_data["ad_verified"],
            creation_eligible_count=page_data["creation_eligible_count"],
            verify_url=workbench_url(
                updates={"verify_ad": "true", "page_number": 1},
                remove=("page_number",),
            ),
            reset_filters_url=workbench_url(
                updates={"queue": normalized_queue, "mode": normalized_mode},
                remove=(
                    "search",
                    "department_id",
                    "employee_status",
                    "identity_status",
                    "ad_status",
                    "page_number",
                ),
            ),
            mode_toggle_url=workbench_url(
                updates={
                    "mode": "basic" if normalized_mode == "advanced" else "advanced"
                },
                remove=("page_number",),
            ),
            previous_page_url=(
                workbench_url(updates={"page_number": page_number - 1})
                if page_number > 1
                else ""
            ),
            next_page_url=(
                workbench_url(updates={"page_number": page_number + 1})
                if page_number < total_pages
                else ""
            ),
            return_query=urlencode(
                {
                    key: value
                    for key, value in base_query.items()
                    if str(value or "").strip()
                }
            ),
        )

    @app.post("/identity-governance/identity-matching/defer")
    def defer_identity_workbench_users(
        request: Request,
        csrf_token: str = Form(""),
        selected_source_user_ids: list[str] = Form(default=[]),
        return_query: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        return_path = CANONICAL_ROUTE_PATHS["identity-matching"]
        allowed_query_keys = {
            "queue",
            "search",
            "department_id",
            "employee_status",
            "identity_status",
            "ad_status",
            "mode",
            "verify_ad",
        }
        safe_query = [
            (key, value)
            for key, value in parse_qsl(str(return_query or ""), keep_blank_values=False)
            if key in allowed_query_keys
        ]
        if safe_query:
            return_path += "?" + urlencode(safe_query)
        csrf_error = reject_invalid_csrf(
            request,
            csrf_token,
            return_path,
        )
        if csrf_error:
            return csrf_error

        normalized_source_user_ids = list(
            dict.fromkeys(
                str(value or "").strip()
                for value in selected_source_user_ids
                if str(value or "").strip()
            )
        )
        if not normalized_source_user_ids or len(normalized_source_user_ids) > 100:
            flash_t(
                request,
                "error",
                "Select between 1 and 100 users to defer.",
            )
            return RedirectResponse(url=return_path, status_code=303)

        page_data = build_relationship_page(
            request,
            page_number=1,
            page_size=len(normalized_source_user_ids),
            search="",
            department_id="",
            status="",
            employee_id_state="",
            relationship_status="all",
            verify_ad=False,
            source_user_ids=normalized_source_user_ids,
        )
        relationship_ids = {
            item.source_user_id for item in page_data["relationships"]
        }
        if (
            not page_data["snapshot"]
            or relationship_ids != set(normalized_source_user_ids)
        ):
            flash_t(
                request,
                "error",
                "One or more selected users are not in the current organization snapshot.",
            )
            return RedirectResponse(url=return_path, status_code=303)

        current_org = get_current_org(request)
        previous = dict(
            request.session.get(IDENTITY_WORKBENCH_DEFERRED_SESSION_KEY) or {}
        )
        previous_ids = (
            {
                str(value or "").strip()
                for value in previous.get("source_user_ids") or []
                if str(value or "").strip()
            }
            if (
                previous.get("org_id") == current_org.org_id
                and previous.get("provider_id") == page_data["provider_id"]
                and int(previous.get("snapshot_id") or 0)
                == int(page_data["snapshot"]["id"] or 0)
            )
            else set()
        )
        deferred_ids = sorted(previous_ids.union(normalized_source_user_ids))
        request.session[IDENTITY_WORKBENCH_DEFERRED_SESSION_KEY] = {
            "org_id": current_org.org_id,
            "provider_id": page_data["provider_id"],
            "snapshot_id": int(page_data["snapshot"]["id"] or 0),
            "source_user_ids": deferred_ids,
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        repositories = get_web_repositories(request)
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="identity_workbench.defer",
            target_type="source_directory_snapshot",
            target_id=str(page_data["snapshot"]["id"]),
            result="success",
            message="Selected source identities were temporarily deferred in the current workbench session",
            payload={
                "source_provider": page_data["provider_id"],
                "source_user_ids": normalized_source_user_ids,
                "selected_user_count": len(normalized_source_user_ids),
            },
        )
        flash_t(
            request,
            "success",
            "Temporarily deferred {count} user(s).",
            count=len(normalized_source_user_ids),
        )
        return RedirectResponse(url=return_path, status_code=303)

    @app.get(
        CANONICAL_ROUTE_PATHS["binding-reconciliation"],
        response_class=HTMLResponse,
    )
    def binding_reconciliation_page(
        request: Request,
        page_number: int = 1,
        search: str = "",
        relationship_status: str = "all",
        verify_ad: bool = False,
    ):
        user = require_capability(request, "mappings.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        page_size = 50
        page_number = max(int(page_number or 1), 1)
        page_data = build_relationship_page(
            request,
            page_number=page_number,
            page_size=page_size,
            search=search,
            department_id="",
            status="",
            employee_id_state="",
            relationship_status=relationship_status,
            verify_ad=verify_ad,
        )
        preview = stored_cleanup_preview(
            request,
            current_org=current_org,
            provider_id=page_data["provider_id"],
        )
        return render(
            request,
            "binding_reconciliation.html",
            page="binding-reconciliation",
            title="Binding Reconciliation",
            current_org=current_org,
            provider_id=page_data["provider_id"],
            provider_name=get_source_provider_display_name(page_data["provider_id"]),
            snapshot=_row_dict(page_data["snapshot"]),
            snapshot_expired=_is_expired(page_data["snapshot"]),
            relationships=page_data["relationships"],
            total_users=page_data["total"],
            page_number=page_number,
            total_pages=max((int(page_data["total"]) + page_size - 1) // page_size, 1),
            search=search,
            selected_relationship_status=relationship_status,
            ad_verified=page_data["ad_verified"],
            binding_cleanup_preview=preview,
            binding_cleanup_workflow=cleanup_workflow(preview),
        )

    @app.get(CANONICAL_ROUTE_PATHS["sync-scope"], response_class=HTMLResponse)
    def sync_scope_page(
        request: Request,
        page_number: int = 1,
        search: str = "",
        department_id: str = "",
        status: str = "",
        employee_id_state: str = "",
        connector_id: str = "",
    ):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        page_size = 50
        page_number = max(int(page_number or 1), 1)
        page_data = build_relationship_page(
            request,
            page_number=page_number,
            page_size=page_size,
            search=search,
            department_id=department_id,
            status=status,
            employee_id_state=employee_id_state,
            relationship_status="all",
            verify_ad=False,
        )
        connectors = repositories.connector_repo.list_connector_records(
            org_id=current_org.org_id,
        )
        selected_connector = next(
            (
                record
                for record in connectors
                if record.connector_id == str(connector_id or "").strip()
            ),
            None,
        )
        release = get_web_services(request).config.build_release_center_context(
            current_org=current_org,
        )
        return render(
            request,
            "sync_scope.html",
            page="sync-scope",
            title="Sync Scope",
            current_org=current_org,
            provider_id=page_data["provider_id"],
            provider_name=get_source_provider_display_name(page_data["provider_id"]),
            snapshot=_row_dict(page_data["snapshot"]),
            snapshot_expired=_is_expired(page_data["snapshot"]),
            relationships=page_data["relationships"],
            total_users=page_data["total"],
            departments=page_data["departments"],
            fields=page_data["fields"],
            scope=page_data["scope"],
            page_number=page_number,
            total_pages=max((int(page_data["total"]) + page_size - 1) // page_size, 1),
            search=search,
            selected_department_id=department_id,
            selected_status=status,
            selected_employee_id_state=employee_id_state,
            connectors=connectors,
            selected_connector=selected_connector,
            has_unpublished_changes=bool(release.get("has_unpublished_changes")),
            latest_snapshot_title=str(release.get("latest_snapshot_title") or ""),
            employee_id_attribute=repositories.settings_repo.get_value(
                "source_employee_id_attribute",
                "",
                org_id=current_org.org_id,
            )
            or "",
        )

    @app.post("/source-directory/reconcile-stale-bindings")
    def reconcile_source_directory_stale_bindings(
        request: Request,
        csrf_token: str = Form(""),
        page_number: int = Form(1),
        search: str = Form(""),
        department_id: str = Form(""),
        status: str = Form(""),
        employee_id_state: str = Form(""),
        relationship_status: str = Form("all"),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(
            request,
            csrf_token,
            CANONICAL_ROUTE_PATHS["binding-reconciliation"],
        )
        if csrf_error:
            return csrf_error

        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        normalized_page_number = max(int(page_number or 1), 1)
        filters = {
            "page_number": normalized_page_number,
            "search": search,
            "department_id": department_id,
            "status": status,
            "employee_id_state": employee_id_state,
            "relationship_status": relationship_status or "all",
        }
        redirect_url = CANONICAL_ROUTE_PATHS["binding-reconciliation"] + "?" + urlencode(
            {
                **filters,
                "verify_ad": "true",
                "cleanup_preview": "true",
            }
        )
        page_data = build_relationship_page(
            request,
            page_number=normalized_page_number,
            page_size=50,
            search=search,
            department_id=department_id,
            status=status,
            employee_id_state=employee_id_state,
            relationship_status=relationship_status,
            verify_ad=True,
        )
        snapshot = page_data["snapshot"]
        preview_id = secrets.token_urlsafe(18)
        if not page_data["snapshot"] or not page_data["ad_verified"]:
            context = cleanup_context(
                request,
                current_org=current_org,
                snapshot=snapshot,
                impact_count=0,
                preview_id=preview_id,
            )
            request.session[BINDING_CLEANUP_PREVIEW_SESSION_KEY] = {
                "status": "blocked",
                "blocked_stage": "scan",
                "reason_code": "high_risk.blocker.ad_verification_unavailable",
                "audit_recorded": True,
                "organization_id": current_org.org_id,
                "provider_id": page_data["provider_id"],
                "snapshot_id": int(snapshot["id"] or 0) if snapshot else 0,
                "snapshot_fingerprint": str(snapshot["snapshot_fingerprint"] or "")
                if snapshot
                else "",
                "filters": filters,
                "context": context.to_dict(),
                "scanned_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            repositories.audit_repo.add_log(
                org_id=current_org.org_id,
                actor_username=user.username,
                action_type="high_risk.binding_cleanup.scan",
                target_type="source_directory_snapshot",
                target_id=str(int(snapshot["id"] or 0) if snapshot else 0),
                result="blocked",
                message="Binding cleanup scan could not verify live AD state; no bindings were removed",
                payload=high_risk_audit_payload(
                    context,
                    reason_code="high_risk.blocker.ad_verification_unavailable",
                ),
            )
            flash_t(
                request,
                "error",
                "Live AD verification is unavailable. No saved bindings were removed.",
            )
            return RedirectResponse(url=redirect_url, status_code=303)

        targets = [
            target
            for item in page_data["relationships"]
            if (
                target
                := IdentityRelationshipPreviewService.verified_stale_binding_cleanup_target(
                    item
                )
            )
        ]
        unverified_binding_count = sum(
            1
            for item in page_data["relationships"]
            if item.before_state.get("bound_ad_username")
            and str(
                item.before_state.get("ad_account_state", {}).get("status") or ""
            )
            in {"", "not_checked", "unavailable"}
        )
        context = cleanup_context(
            request,
            current_org=current_org,
            snapshot=snapshot,
            impact_count=len(targets),
            preview_id=preview_id,
        )
        gate = HighRiskOperationPolicy.evaluate(context)
        target_fingerprint = HighRiskOperationPolicy.target_fingerprint(targets)
        verification_blocked = bool(unverified_binding_count and not targets)
        reason_code = (
            "high_risk.blocker.ad_verification_unavailable"
            if verification_blocked
            else gate.reason_code
        )
        request.session[BINDING_CLEANUP_PREVIEW_SESSION_KEY] = {
            "status": "blocked" if verification_blocked else "preview",
            "blocked_stage": "scan" if verification_blocked else "",
            "reason_code": reason_code,
            "audit_recorded": True,
            "organization_id": current_org.org_id,
            "provider_id": page_data["provider_id"],
            "snapshot_id": int(snapshot["id"] or 0),
            "snapshot_fingerprint": str(snapshot["snapshot_fingerprint"] or ""),
            "target_fingerprint": target_fingerprint,
            "unverified_binding_count": unverified_binding_count,
            "filters": filters,
            "context": context.to_dict(),
            "gate": gate.to_dict(),
            "scanned_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="high_risk.binding_cleanup.scan",
            target_type="source_directory_snapshot",
            target_id=str(snapshot["id"]),
            result="blocked" if verification_blocked else "success",
            message="Scanned live AD state and prepared a binding cleanup preview without deleting bindings",
            payload=high_risk_audit_payload(
                context,
                target_fingerprint=target_fingerprint,
                unverified_binding_count=unverified_binding_count,
                gate_allowed=gate.allowed and not verification_blocked,
                gate_reason_code=reason_code,
            ),
        )
        if verification_blocked:
            flash_t(
                request,
                "error",
                "Live AD verification is unavailable. No saved bindings were removed.",
            )
            return RedirectResponse(url=redirect_url, status_code=303)
        flash_t(
            request,
            "success" if targets else "warning",
            "Cleanup scan completed. Review {impact_count} verified stale binding(s) before confirming execution.",
            impact_count=len(targets),
        )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post("/source-directory/reconcile-stale-bindings/confirm")
    def confirm_source_directory_stale_binding_cleanup(
        request: Request,
        csrf_token: str = Form(""),
        operation_code: str = Form(""),
        organization_id: str = Form(""),
        environment_label: str = Form(""),
        snapshot_version: str = Form(""),
        impact_count: str = Form(""),
        preview_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(
            request,
            csrf_token,
            CANONICAL_ROUTE_PATHS["binding-reconciliation"],
        )
        if csrf_error:
            return csrf_error

        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        preview = dict(request.session.get(BINDING_CLEANUP_PREVIEW_SESSION_KEY) or {})
        filters = dict(preview.get("filters") or {})
        redirect_url = CANONICAL_ROUTE_PATHS["binding-reconciliation"]
        if filters:
            redirect_url += "?" + urlencode(
                {
                    **filters,
                    "verify_ad": "true",
                    "cleanup_preview": "true",
                }
            )
        context = context_from_preview(
            request,
            current_org=current_org,
            preview=preview,
        )

        def block_execution(reason_code: str, message: str, *, stage: str = "confirm"):
            blocked_preview = {
                **preview,
                "status": "blocked",
                "blocked_stage": stage,
                "reason_code": reason_code,
                "audit_recorded": True,
                "context": context.to_dict(),
            }
            request.session[BINDING_CLEANUP_PREVIEW_SESSION_KEY] = blocked_preview
            repositories.audit_repo.add_log(
                org_id=current_org.org_id,
                actor_username=user.username,
                action_type="high_risk.binding_cleanup.execute",
                target_type="source_directory_snapshot",
                target_id=str(preview.get("snapshot_id") or 0),
                result="blocked",
                message=message,
                payload=high_risk_audit_payload(
                    context,
                    reason_code=reason_code,
                    target_fingerprint=str(preview.get("target_fingerprint") or ""),
                ),
            )
            flash_t(request, "error", reason_code)
            return RedirectResponse(url=redirect_url, status_code=303)

        if (
            not preview
            or str(preview.get("organization_id") or "") != current_org.org_id
            or str(preview.get("status") or "") != "preview"
        ):
            return block_execution(
                "high_risk.blocker.preview_missing",
                "Binding cleanup confirmation was rejected because no current organization preview exists",
            )

        confirmation = HighRiskOperationPolicy.validate_confirmation(
            context,
            {
                "operation_code": operation_code,
                "organization_id": organization_id,
                "environment_label": environment_label,
                "snapshot_version": snapshot_version,
                "impact_count": impact_count,
                "preview_id": preview_id,
            },
        )
        if not confirmation.allowed:
            return block_execution(
                confirmation.reason_code,
                "Binding cleanup confirmation no longer matches the current high-risk context",
            )
        if HighRiskOperationPolicy.preview_expired(
            str(preview.get("scanned_at") or ""),
            max_age_seconds=BINDING_CLEANUP_PREVIEW_MAX_AGE_SECONDS,
        ):
            return block_execution(
                "high_risk.blocker.preview_expired",
                "Binding cleanup preview expired before execution",
            )
        if context.impact_count <= 0:
            return block_execution(
                "high_risk.blocker.no_impact",
                "Binding cleanup execution was rejected because the preview contains no targets",
            )

        page_data = build_relationship_page(
            request,
            page_number=max(int(filters.get("page_number") or 1), 1),
            page_size=50,
            search=str(filters.get("search") or ""),
            department_id=str(filters.get("department_id") or ""),
            status=str(filters.get("status") or ""),
            employee_id_state=str(filters.get("employee_id_state") or ""),
            relationship_status=str(filters.get("relationship_status") or "all"),
            verify_ad=True,
        )
        snapshot = page_data["snapshot"]
        if not snapshot or not page_data["ad_verified"]:
            return block_execution(
                "high_risk.blocker.ad_verification_unavailable",
                "Binding cleanup execution could not reverify live AD state; no bindings were removed",
                stage="execute",
            )
        if (
            int(snapshot["id"] or 0) != int(preview.get("snapshot_id") or 0)
            or str(snapshot["snapshot_fingerprint"] or "")
            != str(preview.get("snapshot_fingerprint") or "")
        ):
            return block_execution(
                "high_risk.blocker.preview_changed",
                "Source directory snapshot changed after the binding cleanup preview",
                stage="execute",
            )

        targets = [
            target
            for item in page_data["relationships"]
            if (
                target
                := IdentityRelationshipPreviewService.verified_stale_binding_cleanup_target(
                    item
                )
            )
        ]
        current_target_fingerprint = HighRiskOperationPolicy.target_fingerprint(targets)
        if (
            len(targets) != context.impact_count
            or current_target_fingerprint
            != str(preview.get("target_fingerprint") or "")
        ):
            return block_execution(
                "high_risk.blocker.preview_changed",
                "Verified binding cleanup targets changed after preview; no bindings were removed",
                stage="execute",
            )

        unverified_binding_count = sum(
            1
            for item in page_data["relationships"]
            if item.before_state.get("bound_ad_username")
            and str(
                item.before_state.get("ad_account_state", {}).get("status") or ""
            )
            in {"", "not_checked", "unavailable"}
        )
        removed_count = 0
        changed_count = 0
        for target in targets:
            removed = repositories.user_binding_repo.delete_binding_if_target_matches(
                target["source_user_id"],
                target["ad_username"],
                org_id=current_org.org_id,
                source_provider=target["source_provider"],
                connector_id=target["connector_id"],
            )
            if not removed:
                changed_count += 1
                continue
            removed_count += 1
            repositories.audit_repo.add_log(
                org_id=current_org.org_id,
                actor_username=user.username,
                action_type="mapping.binding_stale_cleanup",
                target_type="user_identity_binding",
                target_id=target["source_user_id"],
                result="success",
                message="Removed saved binding after live AD verification confirmed the target was missing",
                payload={
                    **high_risk_audit_payload(context),
                    "source_provider": target["source_provider"],
                    "connector_id": target["connector_id"],
                    "source_user_id": target["source_user_id"],
                    "source_display_name": target["source_display_name"],
                    "removed_ad_username": target["ad_username"],
                    "binding_source": target["binding_source"],
                    "candidate_ad_username": target["candidate_ad_username"],
                    "verified_at": target["verified_at"],
                },
            )

        execution_result = "success" if not changed_count else "partial"
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="high_risk.binding_cleanup.execute",
            target_type="source_directory_snapshot",
            target_id=str(snapshot["id"]),
            result=execution_result,
            message="Executed the confirmed binding cleanup preview after live AD reverification",
            payload=high_risk_audit_payload(
                context,
                removed_count=removed_count,
                changed_count=changed_count,
                unverified_binding_count=unverified_binding_count,
                target_fingerprint=current_target_fingerprint,
            ),
        )
        request.session[BINDING_CLEANUP_PREVIEW_SESSION_KEY] = {
            **preview,
            "status": "completed" if not changed_count else "blocked",
            "blocked_stage": "" if not changed_count else "execute",
            "reason_code": "" if not changed_count else "high_risk.blocker.preview_changed",
            "audit_recorded": True,
            "context": context.to_dict(),
            "removed_count": removed_count,
            "changed_count": changed_count,
            "unverified_binding_count": unverified_binding_count,
            "completed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        if removed_count and unverified_binding_count:
            flash_t(
                request,
                "success",
                "Removed {removed_count} verified stale binding(s). {unverified_count} binding(s) could not be verified and were left unchanged.",
                removed_count=removed_count,
                unverified_count=unverified_binding_count,
            )
        elif removed_count:
            flash_t(
                request,
                "success",
                "Removed {removed_count} verified stale binding(s). Recheck the candidates before selecting account creation.",
                removed_count=removed_count,
            )
        elif changed_count:
            flash_t(
                request,
                "error",
                "The verified binding changed before cleanup, so nothing was removed. Review the current binding and try again.",
            )
        elif unverified_binding_count:
            flash_t(
                request,
                "error",
                "Could not verify {unverified_count} saved binding(s). Nothing was removed.",
                unverified_count=unverified_binding_count,
            )
        else:
            flash_t(
                request,
                "success",
                "No verified stale saved bindings were found. Nothing was removed.",
            )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post("/source-directory/test")
    def test_source_directory_connection(request: Request, csrf_token: str = Form("")):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(
            request,
            csrf_token,
            CANONICAL_ROUTE_PATHS["config"],
        )
        if csrf_error:
            return csrf_error
        provider = None
        try:
            config, provider = provider_for_current_config(request)
            result = SourceDirectoryService(get_web_repositories(request).source_directory_repo).test_connection(provider)
            get_web_repositories(request).audit_repo.add_log(
                org_id=get_current_org(request).org_id,
                actor_username=user.username,
                action_type="source_directory.connection_test",
                target_type="source_provider",
                target_id=config.source_provider,
                result="success",
                message="Source connection and a contact data page were verified",
                payload={key: value for key, value in result.items() if key != "samples"},
            )
            flash(request, "success", f"Connection verified: {result['department_count']} departments and a readable user page.")
        except Exception as exc:
            flash(request, "error", str(exc)[:500])
        finally:
            if provider is not None:
                provider.close()
        return RedirectResponse(url=CANONICAL_ROUTE_PATHS["config"], status_code=303)

    @app.post("/source-directory/refresh")
    def refresh_source_directory(
        request: Request,
        background_tasks: BackgroundTasks,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(
            request,
            csrf_token,
            CANONICAL_ROUTE_PATHS["source-directory"],
        )
        if csrf_error:
            return csrf_error
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        runtime_state = get_web_runtime_state(request)
        config = repositories.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        background_tasks.add_task(
            refresh_task,
            db_path=repositories.db_manager.db_path,
            org_id=current_org.org_id,
            provider_id=config.source_provider,
            config_path=current_org.config_path or runtime_state.config_path,
            created_by=user.username,
        )
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="source_directory.refresh",
            target_type="source_provider",
            target_id=config.source_provider,
            result="accepted",
            message="Source directory refresh started",
            payload={"provider_id": config.source_provider},
        )
        flash(request, "success", "Source directory refresh started. The previous successful snapshot remains available until completion.")
        return RedirectResponse(
            url=CANONICAL_ROUTE_PATHS["source-directory"],
            status_code=303,
        )

    @app.post(CANONICAL_ROUTE_PATHS["sync-scope"])
    @app.post("/source-directory/scope")
    def save_source_directory_scope(
        request: Request,
        csrf_token: str = Form(""),
        scope_type: str = Form("full"),
        selected_department_ids: list[str] = Form(default=[]),
        selected_source_user_ids: list[str] = Form(default=[]),
        source_field: str = Form(""),
        username_template: str | None = Form(None),
        employee_id_attribute: str | None = Form(None),
        connector_id: str = Form(""),
        root_department_ids: str = Form(""),
        selection_mode: str = Form("explicit"),
        selection_search: str = Form(""),
        selection_department_id: str = Form(""),
        selection_status: str = Form(""),
        selection_employee_id_state: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(
            request,
            csrf_token,
            CANONICAL_ROUTE_PATHS["sync-scope"],
        )
        if csrf_error:
            return csrf_error
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        runtime_state = get_web_runtime_state(request)
        config = repositories.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        existing_scope = repositories.source_directory_repo.get_scope_selection(
            org_id=current_org.org_id,
            provider_id=config.source_provider,
        )
        submitted_source_field = source_field if isinstance(source_field, str) else ""
        normalized_source_field = str(submitted_source_field or "").strip() or str(
            (existing_scope or {}).get("source_field") or "source_user_id"
        )
        normalized_username_template = (
            str(username_template).strip()
            if username_template is not None
            else str((existing_scope or {}).get("username_template") or "")
        )
        strategy = (
            USERNAME_STRATEGY_BY_SOURCE_FIELD.get(
                normalized_source_field, "custom_template"
            )
            if str(submitted_source_field or "").strip()
            else str((existing_scope or {}).get("username_strategy") or "userid")
        )
        if (
            str(submitted_source_field or "").strip()
            and normalized_source_field not in USERNAME_STRATEGY_BY_SOURCE_FIELD
        ):
            normalized_username_template = "{" + normalized_source_field + "}"
        normalized_connector_id = (
            str(connector_id or "").strip() if isinstance(connector_id, str) else ""
        )
        connector_record = None
        normalized_root_department_ids: list[int] = []
        if normalized_connector_id:
            connector_record = repositories.connector_repo.get_connector_record(
                normalized_connector_id,
                org_id=current_org.org_id,
            )
            if connector_record is None:
                flash(request, "error", "Connector was not found in the selected organization")
                return RedirectResponse(
                    url=CANONICAL_ROUTE_PATHS["sync-scope"],
                    status_code=303,
                )
            try:
                normalized_root_department_ids = [
                    int(item.strip())
                    for item in (
                        str(root_department_ids or "")
                        if isinstance(root_department_ids, str)
                        else ""
                    ).split(",")
                    if item.strip()
                ]
            except ValueError:
                flash(request, "error", "Connector root department IDs must be integers")
                return RedirectResponse(
                    url=CANONICAL_ROUTE_PATHS["sync-scope"],
                    status_code=303,
                )
        try:
            if selection_mode == "all_filtered" and scope_type == "selected_users":
                snapshot = repositories.source_directory_repo.get_latest_successful_snapshot(
                    org_id=current_org.org_id,
                    provider_id=config.source_provider,
                )
                if not snapshot:
                    raise ValueError("A successful source directory snapshot is required")
                selected_source_user_ids = []
                offset = 0
                while True:
                    page = repositories.source_directory_repo.list_users(
                        int(snapshot["id"]),
                        org_id=current_org.org_id,
                        provider_id=config.source_provider,
                        search=selection_search,
                        department_id=selection_department_id,
                        status=selection_status,
                        employee_id_state=selection_employee_id_state,
                        limit=200,
                        offset=offset,
                    )
                    selected_source_user_ids.extend(row["source_user_id"] for row in page["items"])
                    offset += len(page["items"])
                    if offset >= int(page["total"]) or not page["items"]:
                        break
            if employee_id_attribute is not None:
                repositories.settings_repo.set_value(
                    "source_employee_id_attribute",
                    str(employee_id_attribute or "").strip(),
                    "string",
                    org_id=current_org.org_id,
                )
            selection = repositories.source_directory_repo.save_scope_selection(
                org_id=current_org.org_id,
                provider_id=config.source_provider,
                scope_type=scope_type,
                selected_department_ids=selected_department_ids,
                selected_source_user_ids=selected_source_user_ids,
                username_strategy=strategy,
                username_template=normalized_username_template,
                source_field=normalized_source_field,
                requested_by=user.username,
            )
            if connector_record is not None:
                repositories.connector_repo.upsert_connector(
                    **build_connector_policy_upsert(
                        connector_record,
                        "scope",
                        {"root_department_ids": normalized_root_department_ids},
                    )
                )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(
                url=CANONICAL_ROUTE_PATHS["sync-scope"],
                status_code=303,
            )
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="source_directory.scope.update",
            target_type="sync_scope",
            target_id=config.source_provider,
            result="success",
            message="Source and connector synchronization scope were updated",
            payload={
                "scope_type": selection["scope_type"],
                "selected_department_count": len(selection["selected_department_ids"]),
                "selected_user_count": len(selection["selected_source_user_ids"]),
                "connector_id": normalized_connector_id,
                "connector_root_department_count": len(normalized_root_department_ids),
                "selection_fingerprint": selection["selection_fingerprint"],
            },
        )
        flash(request, "success", "Sync scope saved. Run a new Dry Run before Apply.")
        return RedirectResponse(
            url=CANONICAL_ROUTE_PATHS["sync-scope"],
            status_code=303,
        )

    @app.post("/source-directory/create-selection")
    def prepare_source_directory_account_creations(
        request: Request,
        csrf_token: str = Form(""),
        selected_source_user_ids: list[str] = Form(default=[]),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        preparation_path = CANONICAL_ROUTE_PATHS["identity-matching"]
        csrf_error = reject_invalid_csrf(
            request,
            csrf_token,
            preparation_path,
        )
        if csrf_error:
            return csrf_error

        normalized_source_user_ids = list(
            dict.fromkeys(
                str(value).strip()
                for value in selected_source_user_ids
                if str(value).strip()
            )
        )
        if not normalized_source_user_ids:
            flash(
                request,
                "error",
                "Select at least one verified missing candidate account.",
            )
            return RedirectResponse(
                url=preparation_path,
                status_code=303,
            )
        if len(normalized_source_user_ids) > 100:
            flash(
                request,
                "error",
                "Prepare no more than 100 candidate accounts at a time.",
            )
            return RedirectResponse(
                url=preparation_path,
                status_code=303,
            )

        page_data = build_relationship_page(
            request,
            page_number=1,
            page_size=len(normalized_source_user_ids),
            search="",
            department_id="",
            status="",
            employee_id_state="",
            relationship_status="all",
            verify_ad=True,
            source_user_ids=normalized_source_user_ids,
        )
        relationships_by_id = {
            item.source_user_id: item for item in page_data["relationships"]
        }
        missing_source_user_ids = sorted(
            set(normalized_source_user_ids) - set(relationships_by_id)
        )
        ineligible = [
            item
            for item in relationships_by_id.values()
            if not item.creation_eligibility.get("eligible")
        ]
        if (
            not page_data["snapshot"]
            or not page_data["ad_verified"]
            or missing_source_user_ids
            or ineligible
        ):
            blocked_reasons = sorted(
                {
                    str(item.creation_eligibility.get("reason") or "").strip()
                    for item in ineligible
                    if str(item.creation_eligibility.get("reason") or "").strip()
                }
            )
            message = (
                blocked_reasons[0]
                if len(blocked_reasons) == 1
                else "One or more selected candidates are no longer eligible for account creation. Verify the page and review their bindings."
            )
            flash(request, "error", message)
            return RedirectResponse(
                url=preparation_path + "?verify_ad=true",
                status_code=303,
            )

        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        snapshot = page_data["snapshot"]
        scope = page_data["scope"] or {}
        provider_id = page_data["provider_id"]
        try:
            selection = repositories.source_directory_repo.save_scope_selection(
                org_id=current_org.org_id,
                provider_id=provider_id,
                connector_id=str(scope.get("connector_id") or "default"),
                scope_type="selected_users",
                selected_source_user_ids=normalized_source_user_ids,
                username_strategy=str(scope.get("username_strategy") or "userid"),
                username_template=str(scope.get("username_template") or ""),
                source_field=str(scope.get("source_field") or "source_user_id"),
                snapshot_id=int(snapshot["id"]),
                requested_by=user.username,
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(
                url=preparation_path + "?verify_ad=true",
                status_code=303,
            )

        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="source_directory.creation_selection.prepare",
            target_type="sync_scope",
            target_id=provider_id,
            result="success",
            message="Verified missing candidate accounts were prepared as an exact Dry Run scope",
            payload={
                "selected_user_count": len(normalized_source_user_ids),
                "selection_fingerprint": selection["selection_fingerprint"],
                "source_field": selection["source_field"],
            },
        )
        flash(
            request,
            "success",
            "Verified missing accounts are selected. Start a Dry Run to review the create operations; no AD changes have been made.",
        )
        return RedirectResponse(
            url="/execution-center/dry-run",
            status_code=303,
        )

    @app.get("/api/source-directory/status")
    def source_directory_status(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        config = repositories.org_config_repo.get_app_config(current_org.org_id)
        latest = repositories.source_directory_repo.get_latest_refresh(
            org_id=current_org.org_id, provider_id=config.source_provider
        )
        successful = repositories.source_directory_repo.get_latest_successful_snapshot(
            org_id=current_org.org_id, provider_id=config.source_provider
        )
        return JSONResponse({"ok": True, "latest_refresh": _row_dict(latest), "active_snapshot": _row_dict(successful)})

    @app.get("/api/source-directory/users")
    def source_directory_users_api(
        request: Request,
        limit: int = 50,
        offset: int = 0,
        search: str = "",
        department_id: str = "",
        status: str = "",
        employee_id_state: str = "",
    ):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        config = repositories.org_config_repo.get_app_config(current_org.org_id)
        snapshot = repositories.source_directory_repo.get_latest_successful_snapshot(
            org_id=current_org.org_id, provider_id=config.source_provider
        )
        if not snapshot:
            return JSONResponse({"ok": True, "items": [], "total": 0})
        result = repositories.source_directory_repo.list_users(
            int(snapshot["id"]), org_id=current_org.org_id, provider_id=config.source_provider,
            search=search, department_id=department_id, status=status,
            employee_id_state=employee_id_state, limit=limit, offset=offset,
        )
        for item in result["items"]:
            item.pop("raw_payload", None)
            item.pop("raw_payload_json", None)
            item.pop("search_text", None)
        return JSONResponse({"ok": True, **result, "snapshot_fingerprint": snapshot["snapshot_fingerprint"]})

    @app.get("/api/source-directory/fields")
    def source_directory_fields_api(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        config = repositories.org_config_repo.get_app_config(current_org.org_id)
        snapshot = repositories.source_directory_repo.get_latest_successful_snapshot(
            org_id=current_org.org_id, provider_id=config.source_provider
        )
        fields = repositories.source_directory_repo.list_field_catalog(int(snapshot["id"]), org_id=current_org.org_id) if snapshot else []
        return JSONResponse({"ok": True, "items": fields})

    @app.get("/api/source-directory/relationships")
    def source_directory_relationships_api(
        request: Request,
        page_number: int = 1,
        page_size: int = 50,
        search: str = "",
        department_id: str = "",
        status: str = "",
        employee_id_state: str = "",
        relationship_status: str = "all",
        verify_ad: bool = False,
    ):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
        bounded_page_size = min(max(int(page_size or 50), 1), 100)
        page_data = build_relationship_page(
            request,
            page_number=max(int(page_number or 1), 1),
            page_size=bounded_page_size,
            search=search,
            department_id=department_id,
            status=status,
            employee_id_state=employee_id_state,
            relationship_status=relationship_status,
            verify_ad=verify_ad,
        )
        snapshot = page_data["snapshot"]
        return JSONResponse(
            {
                "ok": True,
                "items": [item.to_dict() for item in page_data["relationships"]],
                "total": int(page_data["total"]),
                "page_number": max(int(page_number or 1), 1),
                "page_size": bounded_page_size,
                "ad_verified": bool(page_data["ad_verified"]),
                "candidate_missing_count": int(page_data["candidate_missing_count"]),
                "creation_eligible_count": int(page_data["creation_eligible_count"]),
                "snapshot_fingerprint": str(snapshot["snapshot_fingerprint"] or "")
                if snapshot
                else "",
            }
        )

    @app.get("/api/source-directory/preview")
    def source_directory_preview_api(request: Request, source_user_id: str = ""):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
        normalized_source_user_id = str(source_user_id or "").strip()
        if not normalized_source_user_id:
            return JSONResponse({"ok": False, "error": "Source user ID is required"}, status_code=422)
        page_data = build_relationship_page(
            request,
            page_number=1,
            page_size=1,
            search="",
            department_id="",
            status="",
            employee_id_state="",
            relationship_status="all",
            verify_ad=False,
            source_user_ids=[normalized_source_user_id],
        )
        if not page_data["snapshot"]:
            return JSONResponse({"ok": False, "error": "No successful source snapshot"}, status_code=409)
        if not page_data["relationships"]:
            return JSONResponse({"ok": False, "error": "Source user not found"}, status_code=404)
        relationship = page_data["relationships"][0]
        return JSONResponse({"ok": True, **relationship.to_dict()})


__all__ = ["register_source_directory_routes"]
