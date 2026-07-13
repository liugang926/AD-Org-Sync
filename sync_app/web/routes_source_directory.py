from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import BackgroundTasks, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from sync_app.providers.source import build_source_provider, get_source_provider_display_name
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

    @app.get("/source-directory", response_class=HTMLResponse)
    def source_directory_page(
        request: Request,
        page_number: int = 1,
        search: str = "",
        department_id: str = "",
        status: str = "",
        employee_id_state: str = "",
    ):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        runtime_state = get_web_runtime_state(request)
        config = repositories.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        provider_id = config.source_provider
        snapshot = repositories.source_directory_repo.get_latest_successful_snapshot(
            org_id=current_org.org_id, provider_id=provider_id
        )
        latest_refresh = repositories.source_directory_repo.get_latest_refresh(
            org_id=current_org.org_id, provider_id=provider_id
        )
        page_size = 50
        page_number = max(int(page_number or 1), 1)
        result = {"items": [], "total": 0, "limit": page_size, "offset": 0}
        departments: list[dict[str, Any]] = []
        fields: list[dict[str, Any]] = []
        mapping_quality: dict[str, Any] = {}
        scope = repositories.source_directory_repo.get_scope_selection(
            org_id=current_org.org_id, provider_id=provider_id
        )
        if snapshot:
            result = repositories.source_directory_repo.list_users(
                int(snapshot["id"]),
                org_id=current_org.org_id,
                provider_id=provider_id,
                search=search,
                department_id=department_id,
                status=status,
                employee_id_state=employee_id_state,
                limit=page_size,
                offset=(page_number - 1) * page_size,
            )
            departments = repositories.source_directory_repo.list_departments(
                int(snapshot["id"]), org_id=current_org.org_id
            )
            fields = repositories.source_directory_repo.list_field_catalog(
                int(snapshot["id"]), org_id=current_org.org_id
            )
            mapping_quality = SourceDirectoryService(repositories.source_directory_repo).build_mapping_quality_report(
                snapshot_id=int(snapshot["id"]),
                org_id=current_org.org_id,
                provider_id=provider_id,
                username_strategy=str((scope or {}).get("username_strategy") or "userid"),
                username_template=str((scope or {}).get("username_template") or ""),
                source_field=str((scope or {}).get("source_field") or "source_user_id"),
            )
            for item in result["items"]:
                preview = SourceDirectoryService.preview_username(
                    item,
                    username_strategy=str((scope or {}).get("username_strategy") or "userid"),
                    username_template=str((scope or {}).get("username_template") or ""),
                    source_field=str((scope or {}).get("source_field") or "source_user_id"),
                )
                item["mapping_preview"] = preview["username"]
                item["mapping_risks"] = sorted(set(preview["risks"] + list(mapping_quality.get("issues_by_user", {}).get(item["source_user_id"], []))))
        total_pages = max((int(result["total"]) + page_size - 1) // page_size, 1)
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
            users=result["items"],
            total_users=result["total"],
            departments=departments,
            fields=fields,
            scope=scope,
            page_number=page_number,
            total_pages=total_pages,
            search=search,
            selected_department_id=department_id,
            selected_status=status,
            selected_employee_id_state=employee_id_state,
            mapping_quality=mapping_quality,
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

    @app.get("/api/source-directory/preview")
    def source_directory_preview_api(request: Request, source_user_id: str = ""):
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
            return JSONResponse({"ok": False, "error": "No successful source snapshot"}, status_code=409)
        rows = repositories.source_directory_repo.list_users(
            int(snapshot["id"]),
            org_id=current_org.org_id,
            provider_id=config.source_provider,
            source_user_ids=[source_user_id],
            limit=1,
        )["items"]
        if not rows:
            return JSONResponse({"ok": False, "error": "Source user not found"}, status_code=404)
        scope = repositories.source_directory_repo.get_scope_selection(
            org_id=current_org.org_id, provider_id=config.source_provider
        ) or {}
        preview = SourceDirectoryService.preview_username(
            rows[0],
            username_strategy=str(scope.get("username_strategy") or "userid"),
            username_template=str(scope.get("username_template") or ""),
            source_field=str(scope.get("source_field") or "source_user_id"),
        )
        return JSONResponse(
            {
                "ok": True,
                "source_user_id": source_user_id,
                "username": preview["username"],
                "risks": preview["risks"],
                "snapshot_fingerprint": snapshot["snapshot_fingerprint"],
                "selection_fingerprint": scope.get("selection_fingerprint") or "",
            }
        )


__all__ = ["register_source_directory_routes"]
