from __future__ import annotations

import logging
from typing import Any, Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse

from sync_app.providers.source import build_source_provider
from sync_app.storage.local_db import OrganizationConfigRepository


def register_metadata_routes(
    app: FastAPI,
    *,
    list_source_user_departments: Callable[[Request, str], list[dict[str, Any]]],
    search_source_users: Callable[[Request, str], list[dict[str, Any]]],
    search_target_users: Callable[[Request, str], list[dict[str, Any]]],
    require_capability: Callable[[Request, str], Any],
    org_config_repo: OrganizationConfigRepository,
) -> None:
    @app.get("/api/metadata/departments")
    def metadata_departments(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)

        org_id = request.session.get("selected_org_id", "default")
        config = org_config_repo.get_app_config(org_id)
        
        try:
            provider = build_source_provider(app_config=config)
            try:
                departments = provider.list_departments()
                options = [{"id": str(d.department_id), "name": d.name} for d in departments]
                return JSONResponse({"ok": True, "options": options})
            finally:
                provider.close()
        except Exception as exc:
            logging.warning("Failed to fetch departments metadata: %s", exc)
            return JSONResponse({"ok": False, "error": str(exc), "options": []})

    @app.get("/api/metadata/tags")
    def metadata_tags(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)

        org_id = request.session.get("selected_org_id", "default")
        config = org_config_repo.get_app_config(org_id)
        
        try:
            provider = build_source_provider(app_config=config)
            try:
                tags = provider.list_tag_records()
                # WeCom tag has 'tagid' and 'tagname'
                options = [
                    {"id": str(t.get("tagid", "")), "name": str(t.get("tagname", ""))}
                    for t in tags if "tagid" in t
                ]
                return JSONResponse({"ok": True, "options": options})
            finally:
                provider.close()
        except Exception as exc:
            logging.warning("Failed to fetch tags metadata: %s", exc)
            return JSONResponse({"ok": False, "error": str(exc), "options": []})

    @app.get("/api/metadata/external-chats")
    def metadata_external_chats(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)

        org_id = request.session.get("selected_org_id", "default")
        config = org_config_repo.get_app_config(org_id)
        
        try:
            provider = build_source_provider(app_config=config)
            try:
                chats = provider.list_external_group_chats()
                # WeCom chat has 'chat_id' and 'name'
                options = [
                    {"id": str(c.get("chat_id", "")), "name": str(c.get("name", ""))}
                    for c in chats if "chat_id" in c
                ]
                return JSONResponse({"ok": True, "options": options})
            finally:
                provider.close()
        except Exception as exc:
            logging.warning("Failed to fetch external chats metadata: %s", exc)
            return JSONResponse({"ok": False, "error": str(exc), "options": []})

    @app.get("/api/metadata/source-users")
    def metadata_source_users(request: Request):
        user = require_capability(request, "mappings.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)

        query = str(request.query_params.get("q") or "").strip()
        limit = max(min(int(request.query_params.get("limit") or 20), 50), 1)
        options = search_source_users(request, query, limit=limit)
        return JSONResponse({"ok": True, "options": options})

    @app.get("/api/metadata/source-user-departments")
    def metadata_source_user_departments(request: Request):
        user = require_capability(request, "mappings.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)

        source_user_id = str(request.query_params.get("user_id") or "").strip()
        if not source_user_id:
            return JSONResponse({"ok": True, "options": []})
        options = list_source_user_departments(request, source_user_id)
        return JSONResponse({"ok": True, "options": options})

    @app.get("/api/metadata/ad-users")
    def metadata_ad_users(request: Request):
        user = require_capability(request, "mappings.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)

        query = str(request.query_params.get("q") or "").strip()
        limit = max(min(int(request.query_params.get("limit") or 20), 50), 1)
        options = search_target_users(request, query, limit=limit)
        return JSONResponse({"ok": True, "options": options})
