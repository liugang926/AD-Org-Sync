from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.services.data_quality_center import (
    build_data_quality_center_context,
    build_data_quality_export_rows,
    persist_data_quality_snapshot,
)
from sync_app.web.app_state import get_web_repositories
from sync_app.web.navigation import CANONICAL_ROUTE_PATHS


def _parse_optional_int(value: str | None) -> Optional[int]:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        return int(normalized)
    except (TypeError, ValueError):
        return None


def register_data_quality_routes(
    app: FastAPI,
    *,
    build_source_data_quality_snapshot: Callable[[Request], dict[str, Any]],
    flash: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    stream_csv: Callable[..., Any],
) -> None:
    @app.get(CANONICAL_ROUTE_PATHS["data-quality"], response_class=HTMLResponse)
    @app.get("/data-quality", response_class=HTMLResponse)
    def data_quality_center_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        config = repositories.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=current_org.config_path,
        )
        source_snapshot = repositories.source_directory_repo.get_latest_successful_snapshot(
            org_id=current_org.org_id,
            provider_id=config.source_provider,
        )
        source_snapshot_id = int(source_snapshot["id"] or 0) if source_snapshot else 0
        quality_review = (
            repositories.data_quality_review_repo.get_review_for_snapshot(
                org_id=current_org.org_id,
                source_snapshot_id=source_snapshot_id,
            )
            if source_snapshot_id
            else None
        )
        return render(
            request,
            "data_quality_center.html",
            page="data-quality",
            title="Data Quality Center",
            current_org=current_org,
            source_snapshot=source_snapshot,
            quality_review=quality_review,
            **build_data_quality_center_context(
                repositories.db_manager,
                current_org.org_id,
                snapshot_id=_parse_optional_int(request.query_params.get("snapshot_id")),
            ),
        )

    @app.post(f"{CANONICAL_ROUTE_PATHS['data-quality']}/review")
    def data_quality_review_confirm(
        request: Request,
        csrf_token: str = Form(""),
        source_snapshot_id: int = Form(0),
        source_snapshot_fingerprint: str = Form(""),
        review_notes: str = Form(""),
        confirmed: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = CANONICAL_ROUTE_PATHS["data-quality"]
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        if str(confirmed or "").strip().lower() not in {"1", "true", "yes", "on"}:
            flash(request, "error", "Confirm that the current source snapshot was reviewed.")
            return RedirectResponse(url=redirect_url, status_code=303)
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        try:
            review = repositories.data_quality_review_repo.confirm_snapshot(
                org_id=current_org.org_id,
                source_snapshot_id=int(source_snapshot_id),
                source_snapshot_fingerprint=source_snapshot_fingerprint,
                reviewer_username=user.username,
                review_notes=review_notes,
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url=redirect_url, status_code=303)
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="data_quality.review.confirm",
            target_type="source_directory_snapshot",
            target_id=str(review.source_snapshot_id),
            result="success",
            message="Confirmed data quality review for immutable source snapshot",
            payload={
                "source_snapshot_id": review.source_snapshot_id,
                "source_snapshot_fingerprint": review.source_snapshot_fingerprint,
            },
        )
        flash(request, "success", "Data quality review confirmed for the current source snapshot.")
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post(f"{CANONICAL_ROUTE_PATHS['data-quality']}/run")
    @app.post("/data-quality/run")
    def data_quality_center_run(
        request: Request,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = CANONICAL_ROUTE_PATHS["data-quality"]
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        try:
            snapshot = build_source_data_quality_snapshot(request)
            result = persist_data_quality_snapshot(
                repositories.db_manager,
                current_org.org_id,
                created_by=user.username,
                snapshot=snapshot,
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url=redirect_url, status_code=303)
        except Exception as exc:
            flash(request, "error", f"Data quality snapshot failed: {exc}")
            return RedirectResponse(url=redirect_url, status_code=303)

        snapshot_record = result.get("snapshot")
        if snapshot_record is not None:
            repositories.audit_repo.add_log(
                org_id=current_org.org_id,
                actor_username=user.username,
                action_type="data_quality_snapshot.run",
                target_type="data_quality_snapshot",
                target_id=str(getattr(snapshot_record, "id", "") or ""),
                result="success",
                message="Captured data quality snapshot",
                payload=dict(getattr(snapshot_record, "summary", {}) or {}),
            )
            flash(
                request,
                "success",
                f"Captured data quality snapshot {snapshot_record.id}",
            )
            return RedirectResponse(
                url=f"{redirect_url}?snapshot_id={snapshot_record.id}",
                status_code=303,
            )

        flash(request, "warning", "Data quality snapshot completed but no record was stored.")
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.get(f"{CANONICAL_ROUTE_PATHS['data-quality']}/export")
    @app.get("/data-quality/export")
    def data_quality_export(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        snapshot_id = _parse_optional_int(request.query_params.get("snapshot_id"))
        repositories = get_web_repositories(request)
        if snapshot_id is not None:
            snapshot = repositories.data_quality_snapshot_repo.get_snapshot_record(
                snapshot_id,
                org_id=current_org.org_id,
            )
        else:
            snapshot = repositories.data_quality_snapshot_repo.get_latest_snapshot_record(
                org_id=current_org.org_id,
            )
        if snapshot is None:
            flash(request, "warning", "Run a data quality snapshot before exporting repair items.")
            return RedirectResponse(
                url=CANONICAL_ROUTE_PATHS["data-quality"],
                status_code=303,
            )
        return stream_csv(
            header=[
                "issue_key",
                "issue_label",
                "severity",
                "title",
                "source_user_id",
                "source_user_ids",
                "display_name",
                "connector_id",
                "connector_name",
                "detail",
                "action",
            ],
            row_iterable=build_data_quality_export_rows(snapshot),
            filename=f"{current_org.org_id}-data-quality-repair-items.csv",
        )
