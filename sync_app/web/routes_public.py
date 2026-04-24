from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response

from sync_app.web.app_state import get_web_repositories


def register_public_routes(
    app: FastAPI,
    *,
    app_version: str,
    favicon_path: Path,
    legacy_favicon_path: Path,
    get_current_user: Callable[[Request], Any],
) -> None:
    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "version": app_version}

    @app.get("/readyz")
    def readyz(request: Request):
        repositories = get_web_repositories(request)
        db_ok = False
        db_error = ""
        try:
            with repositories.db_manager.connection() as conn:
                conn.execute("SELECT 1").fetchone()
            db_ok = True
        except Exception as exc:  # pragma: no cover - defensive reporting path
            db_error = str(exc)

        default_org = repositories.organization_repo.get_organization_record("default")
        static_assets_ok = favicon_path.parent.exists()
        admin_bootstrapped = repositories.user_repo.has_any_user()
        ready = db_ok and static_assets_ok and default_org is not None and admin_bootstrapped
        status = "ready" if ready else ("setup_required" if db_ok and static_assets_ok and default_org else "degraded")
        payload = {
            "status": status,
            "version": app_version,
            "checks": {
                "database": db_ok,
                "static_assets": static_assets_ok,
                "default_organization": default_org is not None,
                "admin_bootstrapped": admin_bootstrapped,
            },
            "db_path": repositories.db_manager.db_path,
            "setup_url": "/setup" if not admin_bootstrapped else "",
        }
        if db_error:
            payload["database_error"] = db_error
        return JSONResponse(payload, status_code=200 if ready else 503)

    @app.get("/favicon.ico")
    def favicon(request: Request):
        if favicon_path.exists():
            return FileResponse(str(favicon_path), media_type="image/x-icon")
        if legacy_favicon_path.exists():
            return FileResponse(str(legacy_favicon_path), media_type="image/x-icon")
        return Response(status_code=204)

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        if not get_web_repositories(request).user_repo.has_any_user():
            return RedirectResponse(url="/setup", status_code=303)
        if get_current_user(request):
            return RedirectResponse(url="/dashboard", status_code=303)
        return RedirectResponse(url="/login", status_code=303)
