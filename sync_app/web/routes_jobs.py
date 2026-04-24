from __future__ import annotations

import json
from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.web.app_state import get_web_repositories, get_web_runtime_state, get_web_services


def register_job_routes(
    app: FastAPI,
    *,
    build_preflight_snapshot: Callable[..., dict[str, Any]],
    fetch_page: Callable[..., Any],
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    get_ui_language: Callable[[Request], str],
    merge_saved_preflight_snapshot_data: Callable[[Any, dict[str, Any]], dict[str, Any]],
    parse_page_number: Callable[[str | None, int], int],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    translate_text: Callable[..., str],
) -> None:
    @app.get("/jobs", response_class=HTMLResponse)
    def jobs_page(request: Request):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user
        services = get_web_services(request)
        repositories = get_web_repositories(request)
        runtime_state = get_web_runtime_state(request)
        current_org = get_current_org(request)
        preflight_summary = merge_saved_preflight_snapshot_data(
            request.session.get("_preflight_snapshot"),
            build_preflight_snapshot(
                request,
                include_live=False,
                current_org=current_org,
            ),
        )
        return render(
            request,
            "jobs.html",
            page="jobs",
            title="Job Center",
            jobs=services.jobs.list_recent_jobs(org_id=current_org.org_id, limit=30),
            job_center_summary=services.jobs.build_job_center_summary(
                org_id=current_org.org_id,
                preflight_summary=preflight_summary,
            ),
            active_job=services.jobs.get_active_job(org_id=current_org.org_id),
            sync_runner_error=runtime_state.sync_runner.last_error,
            current_org=current_org,
        )

    @app.post("/jobs/{job_id}/approve")
    def approve_job_review(
        request: Request,
        job_id: str,
        csrf_token: str = Form(""),
        review_notes: str = Form(""),
    ):
        user = require_capability(request, "jobs.review")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, f"/jobs/{job_id}")
        if csrf_error:
            return csrf_error

        services = get_web_services(request)
        review_record = services.jobs.get_review_record(job_id)
        if not review_record:
            flash(request, "error", "This job does not have a pending high-risk review")
            return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)
        current_org = get_current_org(request)
        job_record = services.jobs.get_job_record(job_id)
        if not job_record or (job_record.org_id and job_record.org_id != current_org.org_id):
            flash(request, "error", "Job does not belong to the current organization")
            return RedirectResponse(url="/jobs", status_code=303)

        services.jobs.approve_review(
            org_id=current_org.org_id,
            job_id=job_id,
            reviewer_username=user.username,
            review_notes=review_notes.strip(),
        )
        flash(request, "success", "High-risk plan approved. You can rerun apply now.")
        return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)

    @app.post("/jobs/run")
    def run_job(
        request: Request,
        csrf_token: str = Form(""),
        mode: str = Form(...),
    ):
        user = require_capability(request, "jobs.run")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/jobs")
        if csrf_error:
            return csrf_error

        normalized_mode = "dry_run" if mode == "dry_run" else "apply"
        runtime_state = get_web_runtime_state(request)
        current_org = get_current_org(request)
        ok, message = runtime_state.sync_runner.launch(
            mode=normalized_mode,
            actor_username=user.username,
            org_id=current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        flash(request, "success" if ok else "error", message)
        return RedirectResponse(url="/jobs", status_code=303)

    @app.get("/jobs/{job_id}", response_class=HTMLResponse)
    def job_detail(request: Request, job_id: str):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user

        services = get_web_services(request)
        repositories = get_web_repositories(request)
        job = services.jobs.get_job_record(job_id)
        if not job:
            flash_t(request, "error", "Job not found: {job_id}", job_id=job_id)
            return RedirectResponse(url="/jobs", status_code=303)
        current_org = get_current_org(request)
        if job.org_id and job.org_id != current_org.org_id:
            flash(request, "error", "Job does not belong to the current organization")
            return RedirectResponse(url="/jobs", status_code=303)
        return render(
            request,
            "job_detail.html",
            page="jobs",
            title=translate_text(get_ui_language(request), "Job Detail {job_id}", job_id=job_id),
            job=job,
            current_org=current_org,
            job_comparison_sections=services.jobs.build_job_comparison_sections(
                org_id=current_org.org_id,
                job=job,
            ),
            events=(events_result := fetch_page(
                lambda *, limit, offset: repositories.event_repo.list_events_for_job_page(
                    job_id,
                    limit=limit,
                    offset=offset,
                ),
                page=parse_page_number(request.query_params.get("events_page"), 1),
                page_size=25,
            ))[0],
            events_page_data=events_result[1],
            planned_operations=(planned_result := fetch_page(
                lambda *, limit, offset: repositories.planned_operation_repo.list_operations_for_job_page(
                    job_id,
                    limit=limit,
                    offset=offset,
                ),
                page=parse_page_number(request.query_params.get("planned_page"), 1),
                page_size=25,
            ))[0],
            planned_operations_page_data=planned_result[1],
            operation_records=(operations_result := fetch_page(
                lambda *, limit, offset: repositories.operation_log_repo.list_records_for_job_page(
                    job_id,
                    limit=limit,
                    offset=offset,
                ),
                page=parse_page_number(request.query_params.get("operations_page"), 1),
                page_size=25,
            ))[0],
            operation_records_page_data=operations_result[1],
            conflicts=(conflicts_result := fetch_page(
                lambda *, limit, offset: repositories.conflict_repo.list_conflicts_for_job_page(
                    job_id,
                    limit=limit,
                    offset=offset,
                ),
                page=parse_page_number(request.query_params.get("conflicts_page"), 1),
                page_size=25,
            ))[0],
            job_conflicts_page_data=conflicts_result[1],
            review_record=services.jobs.get_review_record(job_id),
            summary_json=json.dumps(job.summary or {}, ensure_ascii=False, indent=2),
        )

    @app.get("/database", response_class=HTMLResponse)
    def database_page(request: Request):
        user = require_capability(request, "database.read")
        if isinstance(user, RedirectResponse):
            return user

        repositories = get_web_repositories(request)
        db_manager = repositories.db_manager
        integrity = db_manager.last_integrity_check or db_manager.run_integrity_check()
        return render(
            request,
            "database.html",
            page="database",
            title="Database Operations",
            db_info=db_manager.runtime_info(),
            integrity=integrity,
            retention_settings={
                "job_history_retention_days": repositories.settings_repo.get_int("job_history_retention_days", 30),
                "event_history_retention_days": repositories.settings_repo.get_int("event_history_retention_days", 30),
                "audit_log_retention_days": repositories.settings_repo.get_int("audit_log_retention_days", 90),
                "backup_retention_days": repositories.settings_repo.get_int("backup_retention_days", 30),
                "backup_retention_max_files": repositories.settings_repo.get_int("backup_retention_max_files", 30),
            },
        )

    @app.post("/database/check")
    def database_check(request: Request, csrf_token: str = Form("")):
        user = require_capability(request, "database.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/database")
        if csrf_error:
            return csrf_error

        repositories = get_web_repositories(request)
        result = repositories.db_manager.run_integrity_check()
        repositories.audit_repo.add_log(
            actor_username=user.username,
            action_type="database.check",
            target_type="sqlite",
            target_id=repositories.db_manager.db_path,
            result="success" if result.get("ok") else "error",
            message=f"Ran integrity check: {result.get('result')}",
            payload=result,
        )
        flash_t(
            request,
            "success" if result.get("ok") else "error",
            "Integrity check result: {result}",
            result=str(result.get("result") or "-"),
        )
        return RedirectResponse(url="/database", status_code=303)

    @app.post("/database/backup")
    def database_backup(request: Request, csrf_token: str = Form("")):
        user = require_capability(request, "database.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/database")
        if csrf_error:
            return csrf_error

        repositories = get_web_repositories(request)
        backup_path = repositories.db_manager.backup_database(label="web_manual")
        backup_cleanup = repositories.db_manager.cleanup_backups(
            retention_days=repositories.settings_repo.get_int("backup_retention_days", 30),
            max_files=repositories.settings_repo.get_int("backup_retention_max_files", 30),
        )
        repositories.audit_repo.add_log(
            actor_username=user.username,
            action_type="database.backup",
            target_type="sqlite",
            target_id=repositories.db_manager.db_path,
            result="success",
            message="Created database backup",
            payload={
                "backup_path": backup_path,
                "backup_cleanup": backup_cleanup,
            },
        )
        deleted_backups = int(backup_cleanup.get("deleted_backups", 0))
        if deleted_backups:
            flash_t(
                request,
                "success",
                "Backup created: {backup_path}. Pruned {deleted_backups} old backups.",
                backup_path=backup_path,
                deleted_backups=deleted_backups,
            )
        else:
            flash_t(request, "success", "Backup created: {backup_path}", backup_path=backup_path)
        return RedirectResponse(url="/database", status_code=303)
