from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.services.external_integrations import approve_job_review as approve_job_review_action
from sync_app.services.job_diff import build_job_comparison_summary


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
    def normalize_job_status(value: str | None) -> str:
        return str(value or "").strip().upper()

    def is_successful_dry_run(job: Any) -> bool:
        return (
            str(getattr(job, "execution_mode", "") or "").strip().lower() == "dry_run"
            and normalize_job_status(getattr(job, "status", "")) in {"COMPLETED", "COMPLETED_WITH_ERRORS"}
        )

    def is_successful_apply(job: Any) -> bool:
        return (
            str(getattr(job, "execution_mode", "") or "").strip().lower() == "apply"
            and normalize_job_status(getattr(job, "status", "")) in {"COMPLETED", "COMPLETED_WITH_ERRORS"}
        )

    def parse_job_started_at(job: Any) -> datetime | None:
        raw_value = str(getattr(job, "started_at", "") or "").strip()
        if not raw_value:
            return None
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def find_previous_job(
        recent_jobs: list[Any],
        current_job: Any,
        matcher: Callable[[Any], bool],
    ) -> Any | None:
        current_started_at = parse_job_started_at(current_job)
        for candidate in recent_jobs:
            if str(getattr(candidate, "job_id", "") or "") == str(getattr(current_job, "job_id", "") or ""):
                continue
            if not matcher(candidate):
                continue
            if current_started_at is not None:
                candidate_started_at = parse_job_started_at(candidate)
                if candidate_started_at is None or candidate_started_at >= current_started_at:
                    continue
            return candidate
        return None

    def build_job_comparison_sections(request: Request, current_org: Any, job: Any) -> list[dict[str, Any]]:
        recent_jobs = request.app.state.job_repo.list_recent_job_records(limit=200, org_id=current_org.org_id)
        sections: list[dict[str, Any]] = []

        previous_successful_dry_run = find_previous_job(recent_jobs, job, is_successful_dry_run)
        if previous_successful_dry_run is not None:
            sections.append(
                {
                    "label": "Compared With Previous Successful Dry Run",
                    "comparison": build_job_comparison_summary(
                        current_job=job,
                        baseline_job=previous_successful_dry_run,
                        planned_operation_repo=request.app.state.planned_operation_repo,
                        conflict_repo=request.app.state.conflict_repo,
                    ),
                }
            )

        previous_successful_apply = find_previous_job(recent_jobs, job, is_successful_apply)
        if previous_successful_apply is not None:
            sections.append(
                {
                    "label": "Compared With Previous Apply",
                    "comparison": build_job_comparison_summary(
                        current_job=job,
                        baseline_job=previous_successful_apply,
                        planned_operation_repo=request.app.state.planned_operation_repo,
                        conflict_repo=request.app.state.conflict_repo,
                    ),
                }
            )

        return sections

    def build_job_center_summary(request: Request, current_org: Any) -> dict[str, Any]:
        preflight_summary = merge_saved_preflight_snapshot_data(
            request.session.get("_preflight_snapshot"),
            build_preflight_snapshot(
                request,
                include_live=False,
                current_org=current_org,
            ),
        )
        recent_jobs = request.app.state.job_repo.list_recent_job_records(limit=30, org_id=current_org.org_id)
        latest_dry_run = next(
            (
                job
                for job in recent_jobs
                if str(getattr(job, "execution_mode", "") or "").strip().lower() == "dry_run"
            ),
            None,
        )
        latest_successful_dry_run = next((job for job in recent_jobs if is_successful_dry_run(job)), None)
        latest_apply = next(
            (
                job
                for job in recent_jobs
                if str(getattr(job, "execution_mode", "") or "").strip().lower() == "apply"
            ),
            None,
        )

        review_record = None
        review_required = False
        if latest_successful_dry_run:
            summary = dict(getattr(latest_successful_dry_run, "summary", {}) or {})
            review_required = bool(summary.get("review_required") or False)
            if review_required:
                review_record = request.app.state.review_repo.get_review_record_by_job_id(
                    latest_successful_dry_run.job_id
                )

        blocked_reasons: list[str] = []
        if str(preflight_summary.get("overall_status") or "") == "error":
            blocked_reasons.append("Fix organization configuration or connectivity errors before running apply.")
        if latest_dry_run and not latest_successful_dry_run:
            blocked_reasons.append("The most recent dry run did not complete successfully. Re-run dry run after fixing errors.")
        if not latest_successful_dry_run:
            blocked_reasons.append("No successful dry run has been recorded for this organization yet.")
        open_conflict_count = int(preflight_summary.get("open_conflict_count") or 0)
        if open_conflict_count > 0:
            blocked_reasons.append("Resolve the open conflict queue before running apply.")
        if review_required and (
            review_record is None or str(review_record.status or "").strip().lower() != "approved"
        ):
            blocked_reasons.append("Latest high-risk dry run still needs review approval before apply can continue.")

        if str(preflight_summary.get("overall_status") or "") == "error":
            overall_status = "error"
            overall_label = "Blocked"
        elif blocked_reasons:
            overall_status = "warning"
            overall_label = "Needs Attention"
        else:
            overall_status = "success"
            overall_label = "Ready"

        if str(preflight_summary.get("overall_status") or "") == "error":
            next_action_url = "/config"
            next_action_label = "Fix Configuration"
        elif latest_dry_run and not latest_successful_dry_run:
            next_action_url = f"/jobs/{latest_dry_run.job_id}"
            next_action_label = "Inspect Dry Run Errors"
        elif not latest_successful_dry_run:
            next_action_url = "/jobs"
            next_action_label = "Run Dry Run"
        elif open_conflict_count > 0:
            next_action_url = "/conflicts"
            next_action_label = "Review Conflicts"
        elif review_required and (
            review_record is None or str(review_record.status or "").strip().lower() != "approved"
        ):
            next_action_url = f"/jobs/{latest_successful_dry_run.job_id}"
            next_action_label = "Approve High-Risk Plan"
        elif not latest_apply:
            next_action_url = "/jobs"
            next_action_label = "Run Apply"
        else:
            next_action_url = "/jobs"
            next_action_label = "Review Latest Apply"

        impact_job = latest_successful_dry_run or latest_dry_run
        impact_summary = dict(getattr(impact_job, "summary", {}) or {}) if impact_job else {}
        return {
            "overall_status": overall_status,
            "overall_label": overall_label,
            "blocked_reasons": blocked_reasons,
            "next_action_url": next_action_url,
            "next_action_label": next_action_label,
            "preflight_summary": preflight_summary,
            "latest_dry_run": latest_dry_run,
            "latest_successful_dry_run": latest_successful_dry_run,
            "latest_apply": latest_apply,
            "review_record": review_record,
            "review_required": review_required,
            "impact_preview": {
                "job_id": getattr(impact_job, "job_id", ""),
                "planned_operation_count": int(
                    impact_summary.get("planned_operation_count")
                    or getattr(impact_job, "planned_operation_count", 0)
                    or 0
                ),
                "high_risk_operation_count": int(impact_summary.get("high_risk_operation_count") or 0),
                "conflict_count": int(impact_summary.get("conflict_count") or 0),
                "error_count": int(getattr(impact_job, "error_count", 0) or 0),
            },
        }

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
            job_center_summary=build_job_center_summary(request, current_org),
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

        result = approve_job_review_action(
            request.app.state.db_manager,
            org_id=current_org.org_id,
            job_id=job_id,
            reviewer_username=user.username,
            review_notes=review_notes.strip(),
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="job.review_approve",
            target_type="sync_job",
            target_id=job_id,
            result="success",
            message="Approved high-risk synchronization plan",
            payload={
                "expires_at": result["expires_at_iso"],
                "replay_request_id": result["replay_request_id"],
                "fresh_approval": result["fresh_approval"],
            },
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
            job_comparison_sections=build_job_comparison_sections(request, current_org, job),
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
