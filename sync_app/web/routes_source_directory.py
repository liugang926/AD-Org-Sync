from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import BackgroundTasks, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from sync_app.providers.source import build_source_provider, get_source_provider_display_name
from sync_app.core.models import DepartmentNode
from sync_app.services.identity_relationships import IdentityRelationshipPreviewService
from sync_app.services.runtime_connectors import (
    build_department_connector_map,
    load_connector_specs,
)
from sync_app.services.runtime_bootstrap import build_runtime_config_fingerprint
from sync_app.services.source_directory import SourceDirectoryService
from sync_app.web.app_state import get_web_repositories, get_web_runtime_state


LOGGER = logging.getLogger(__name__)
STRATEGY_BY_SOURCE_FIELD = {
    "source_user_id": "userid",
    "employee_id": "employee_id",
    "email_localpart": "email_localpart",
    "pinyin_initials_employee_id": "pinyin_initials_employee_id",
    "pinyin_full_employee_id": "pinyin_full_employee_id",
    "family_name_pinyin_given_initials": "family_name_pinyin_given_initials",
    "family_name_pinyin_given_name_pinyin": "family_name_pinyin_given_name_pinyin",
    "custom_template": "custom_template",
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


def register_source_directory_routes(
    app: FastAPI,
    *,
    flash: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    build_target_provider_for_connector: Callable[[Request, str], Any] | None = None,
) -> None:
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
                "mapping_quality": {},
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
        requires_relationship_filter = str(relationship_status or "all").strip().lower() not in {
            "",
            "all",
        }
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
        elif requires_relationship_filter:
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
        if requires_relationship_filter:
            relationships = [
                item
                for item in relationships
                if service.matches_filter(item, relationship_status)
            ]
            total = len(relationships)
            relationships = relationships[offset : offset + page_size]

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
            page_user_ids = [item.source_user_id for item in relationships]
            page_users_by_id = {
                str(item.get("source_user_id") or ""): item for item in base_users
            }
            page_users = [page_users_by_id[user_id] for user_id in page_user_ids]
            relationships = service.build_relationships(
                page_users,
                org_id=current_org.org_id,
                source_provider=provider_id,
                snapshot=snapshot,
                scope=scope,
                connector_specs_by_id=specs_by_id,
                connector_ids_by_source_user={
                    user_id: assignments[user_id] for user_id in page_user_ids
                },
                field_labels=field_labels,
                ad_states=ad_states,
                config_fingerprint=config_fingerprint,
                candidate_collision_source_ids=candidate_collision_source_ids,
            )
            ad_verified = True

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
            "mapping_quality": mapping_quality,
        }

    @app.get("/source-directory", response_class=HTMLResponse)
    def source_directory_page(
        request: Request,
        page_number: int = 1,
        search: str = "",
        department_id: str = "",
        status: str = "",
        employee_id_state: str = "",
        relationship_status: str = "all",
        verify_ad: bool = False,
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
            relationship_status=relationship_status,
            verify_ad=verify_ad,
        )
        provider_id = page_data["provider_id"]
        snapshot = page_data["snapshot"]
        latest_refresh = repositories.source_directory_repo.get_latest_refresh(
            org_id=current_org.org_id, provider_id=provider_id
        )
        scope = page_data["scope"]
        total_pages = max((int(page_data["total"]) + page_size - 1) // page_size, 1)
        return render(
            request,
            "source_directory.html",
            page="source-directory",
            title="Source Directory",
            current_org=current_org,
            provider_id=provider_id,
            provider_name=get_source_provider_display_name(provider_id),
            snapshot=_row_dict(snapshot),
            latest_refresh=_row_dict(latest_refresh),
            snapshot_expired=_is_expired(snapshot),
            users=page_data["relationships"],
            total_users=page_data["total"],
            departments=page_data["departments"],
            fields=page_data["fields"],
            scope=scope,
            page_number=page_number,
            total_pages=total_pages,
            search=search,
            selected_department_id=department_id,
            selected_status=status,
            selected_employee_id_state=employee_id_state,
            selected_relationship_status=relationship_status,
            ad_verified=page_data["ad_verified"],
            mapping_quality=page_data["mapping_quality"],
            employee_id_attribute=repositories.settings_repo.get_value(
                "source_employee_id_attribute", "", org_id=current_org.org_id
            ) or "",
        )

    @app.post("/source-directory/test")
    def test_source_directory_connection(request: Request, csrf_token: str = Form("")):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/source-directory")
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
        return RedirectResponse(url="/source-directory", status_code=303)

    @app.post("/source-directory/refresh")
    def refresh_source_directory(
        request: Request,
        background_tasks: BackgroundTasks,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/source-directory")
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
        return RedirectResponse(url="/source-directory", status_code=303)

    @app.post("/source-directory/scope")
    def save_source_directory_scope(
        request: Request,
        csrf_token: str = Form(""),
        scope_type: str = Form("full"),
        selected_department_ids: list[str] = Form(default=[]),
        selected_source_user_ids: list[str] = Form(default=[]),
        source_field: str = Form("source_user_id"),
        username_template: str = Form(""),
        employee_id_attribute: str = Form(""),
        selection_mode: str = Form("explicit"),
        selection_search: str = Form(""),
        selection_department_id: str = Form(""),
        selection_status: str = Form(""),
        selection_employee_id_state: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/source-directory")
        if csrf_error:
            return csrf_error
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        runtime_state = get_web_runtime_state(request)
        config = repositories.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        strategy = STRATEGY_BY_SOURCE_FIELD.get(source_field, "custom_template")
        if source_field not in STRATEGY_BY_SOURCE_FIELD:
            username_template = "{" + source_field + "}"
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
                username_template=username_template,
                source_field=source_field,
                requested_by=user.username,
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/source-directory", status_code=303)
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="source_directory.scope.update",
            target_type="sync_scope",
            target_id=config.source_provider,
            result="success",
            message="Source sync scope and AD username mapping were updated",
            payload={
                "scope_type": selection["scope_type"],
                "selected_department_count": len(selection["selected_department_ids"]),
                "selected_user_count": len(selection["selected_source_user_ids"]),
                "source_field": source_field,
                "selection_fingerprint": selection["selection_fingerprint"],
            },
        )
        flash(request, "success", "Sync scope saved. Run a new Dry Run before Apply.")
        return RedirectResponse(url="/source-directory", status_code=303)

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
