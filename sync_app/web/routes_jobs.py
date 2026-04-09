from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse


def register_job_routes(
    app: FastAPI,
    *,
    enqueue_replay_request: Callable[..., Any],
    fetch_page: Callable[..., Any],
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    get_ui_language: Callable[[Request], str],
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
        current_org = get_current_org(request)
        return render(
            request,
            "jobs.html",
            page="jobs",
            title="Job Center",
            jobs=request.app.state.job_repo.list_recent_job_records(limit=30, org_id=current_org.org_id),
            active_job=request.app.state.job_repo.get_active_job_record(org_id=current_org.org_id),
            sync_runner_error=request.app.state.sync_runner.last_error,
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

        review_record = request.app.state.review_repo.get_review_record_by_job_id(job_id)
        if not review_record:
            flash(request, "error", "This job does not have a pending high-risk review")
            return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)
        current_org = get_current_org(request)
        job_record = request.app.state.job_repo.get_job_record(job_id)
        if not job_record or (job_record.org_id and job_record.org_id != current_org.org_id):
            flash(request, "error", "Job does not belong to the current organization")
            return RedirectResponse(url="/jobs", status_code=303)

        review_ttl_minutes = max(request.app.state.settings_repo.get_int("high_risk_review_ttl_minutes", 240), 1)
        expires_at = time.time() + review_ttl_minutes * 60
        expires_at_iso = datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat(timespec="seconds")
        request.app.state.review_repo.approve_review(
            job_id,
            reviewer_username=user.username,
            review_notes=review_notes.strip(),
            expires_at=expires_at_iso,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="job.review_approve",
            target_type="sync_job",
            target_id=job_id,
            result="success",
            message="Approved high-risk synchronization plan",
            payload={"expires_at": expires_at_iso},
        )
        enqueue_replay_request(
            app=request.app,
            request_type="plan_approval",
            requested_by=user.username,
            org_id=current_org.org_id,
            target_scope="job",
            target_id=job_id,
            trigger_reason="high_risk_plan_approved",
            payload={"expires_at": expires_at_iso},
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
        current_org = get_current_org(request)
        ok, message = request.app.state.sync_runner.launch(
            mode=normalized_mode,
            actor_username=user.username,
            org_id=current_org.org_id,
            config_path=current_org.config_path or request.app.state.config_path,
        )
        flash(request, "success" if ok else "error", message)
        return RedirectResponse(url="/jobs", status_code=303)

    @app.get("/jobs/{job_id}", response_class=HTMLResponse)
    def job_detail(request: Request, job_id: str):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user

        job = request.app.state.job_repo.get_job_record(job_id)
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
            events=(events_result := fetch_page(
                lambda *, limit, offset: request.app.state.event_repo.list_events_for_job_page(
                    job_id,
                    limit=limit,
                    offset=offset,
                ),
                page=parse_page_number(request.query_params.get("events_page"), 1),
                page_size=25,
            ))[0],
            events_page_data=events_result[1],
            planned_operations=(planned_result := fetch_page(
                lambda *, limit, offset: request.app.state.planned_operation_repo.list_operations_for_job_page(
                    job_id,
                    limit=limit,
                    offset=offset,
                ),
                page=parse_page_number(request.query_params.get("planned_page"), 1),
                page_size=25,
            ))[0],
            planned_operations_page_data=planned_result[1],
            operation_records=(operations_result := fetch_page(
                lambda *, limit, offset: request.app.state.operation_log_repo.list_records_for_job_page(
                    job_id,
                    limit=limit,
                    offset=offset,
                ),
                page=parse_page_number(request.query_params.get("operations_page"), 1),
                page_size=25,
            ))[0],
            operation_records_page_data=operations_result[1],
            conflicts=(conflicts_result := fetch_page(
                lambda *, limit, offset: request.app.state.conflict_repo.list_conflicts_for_job_page(
                    job_id,
                    limit=limit,
                    offset=offset,
                ),
                page=parse_page_number(request.query_params.get("conflicts_page"), 1),
                page_size=25,
            ))[0],
            job_conflicts_page_data=conflicts_result[1],
            review_record=request.app.state.review_repo.get_review_record_by_job_id(job_id),
            summary_json=json.dumps(job.summary or {}, ensure_ascii=False, indent=2),
        )

    @app.get("/database", response_class=HTMLResponse)
    def database_page(request: Request):
        user = require_capability(request, "database.read")
        if isinstance(user, RedirectResponse):
            return user

        db_manager = request.app.state.db_manager
        integrity = db_manager.last_integrity_check or db_manager.run_integrity_check()
        return render(
            request,
            "database.html",
            page="database",
            title="Database Operations",
            db_info=db_manager.runtime_info(),
            integrity=integrity,
            retention_settings={
                "job_history_retention_days": request.app.state.settings_repo.get_int("job_history_retention_days", 30),
                "event_history_retention_days": request.app.state.settings_repo.get_int("event_history_retention_days", 30),
                "audit_log_retention_days": request.app.state.settings_repo.get_int("audit_log_retention_days", 90),
                "backup_retention_days": request.app.state.settings_repo.get_int("backup_retention_days", 30),
                "backup_retention_max_files": request.app.state.settings_repo.get_int("backup_retention_max_files", 30),
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

        result = request.app.state.db_manager.run_integrity_check()
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="database.check",
            target_type="sqlite",
            target_id=request.app.state.db_manager.db_path,
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

        backup_path = request.app.state.db_manager.backup_database(label="web_manual")
        backup_cleanup = request.app.state.db_manager.cleanup_backups(
            retention_days=request.app.state.settings_repo.get_int("backup_retention_days", 30),
            max_files=request.app.state.settings_repo.get_int("backup_retention_max_files", 30),
        )
        request.app.state.audit_repo.add_log(
            actor_username=user.username,
            action_type="database.backup",
            target_type="sqlite",
            target_id=request.app.state.db_manager.db_path,
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
