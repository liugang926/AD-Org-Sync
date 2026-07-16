from __future__ import annotations

import json
from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from sync_app.services.high_risk_operations import (
    HighRiskOperationContext,
    HighRiskOperationPolicy,
    high_risk_audit_payload,
)
from sync_app.services.identity_relationships import IdentityRelationshipPreviewService
from sync_app.services.runtime_bootstrap import resolve_runtime_config_fingerprint
from sync_app.web.app_state import get_web_repositories, get_web_runtime_state, get_web_services
from sync_app.web.navigation import CANONICAL_ROUTE_PATHS


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
    def current_config_fingerprint(
        request: Request,
        current_org: Any,
    ) -> str | None:
        repositories = get_web_repositories(request)
        try:
            return resolve_runtime_config_fingerprint(
                db_manager=repositories.db_manager,
                org_id=current_org.org_id,
                config_path=(
                    str(getattr(current_org, "config_path", "") or "")
                    or get_web_runtime_state(request).config_path
                ),
            )
        except Exception:
            return None

    def build_job_center_state(
        request: Request,
        current_org: Any,
    ) -> tuple[dict[str, Any], HighRiskOperationContext]:
        services = get_web_services(request)
        preflight_summary = merge_saved_preflight_snapshot_data(
            request.session.get("_preflight_snapshot"),
            build_preflight_snapshot(
                request,
                include_live=False,
                current_org=current_org,
            ),
        )
        summary = services.jobs.build_job_center_summary(
            org_id=current_org.org_id,
            preflight_summary=preflight_summary,
        )
        impact_preview = summary["impact_preview"]
        evaluation = services.jobs.evaluate_plan(
            org_id=current_org.org_id,
            organization_name=current_org.name,
            environment_label=getattr(
                request.app.state,
                "environment_label",
                "Unlabeled environment",
            ),
            plan_job_id=str(impact_preview.get("job_id") or ""),
            require_approval=True,
            current_config_fingerprint=current_config_fingerprint(
                request,
                current_org,
            ),
        )
        context = evaluation.context
        if not evaluation.allowed:
            summary["blocked_reasons"] = [
                {
                    "message_code": evaluation.reason_code,
                    "params": {},
                },
                *summary["blocked_reasons"],
            ]
            summary["overall_status"] = "error"
            summary["overall_label_code"] = "jobs.status.blocked"
            summary["next_action_url"] = execution_next_action_url(
                evaluation.next_action_code
            )
            summary["next_action_label_code"] = evaluation.next_action_code

        latest_apply = summary.get("latest_apply")
        apply_status = str(getattr(latest_apply, "status", "") or "").upper()
        preview_complete = bool(summary.get("latest_successful_dry_run"))
        confirmation_ready = (
            preview_complete
            and evaluation.allowed
            and not summary["blocked_reasons"]
        )
        summary["high_risk_context"] = context.to_dict()
        summary["high_risk_gate"] = {
            "allowed": evaluation.allowed,
            "reason_code": evaluation.reason_code,
            "next_action_code": evaluation.next_action_code,
        }
        summary["plan_evaluation"] = evaluation
        summary["high_risk_workflow"] = HighRiskOperationPolicy.workflow(
            scan_state=(
                "complete"
                if str(preflight_summary.get("overall_status") or "") != "error"
                else "blocked"
            ),
            preview_state="complete" if preview_complete else "pending",
            confirm_state=(
                "complete"
                if apply_status in {"COMPLETED", "FAILED"}
                else (
                    "current"
                    if confirmation_ready
                    else ("blocked" if preview_complete else "pending")
                )
            ),
            execute_state=(
                "complete"
                if apply_status == "COMPLETED"
                else ("blocked" if apply_status == "FAILED" else ("current" if confirmation_ready else "pending"))
            ),
            audit_state="complete" if apply_status in {"COMPLETED", "FAILED"} else "pending",
        )
        return summary, context

    def execution_next_action_url(action_code: str) -> str:
        return {
            "execution.action.run_dry_run": CANONICAL_ROUTE_PATHS[
                "execution-dry-run"
            ],
            "execution.action.select_dry_run": CANONICAL_ROUTE_PATHS[
                "execution-dry-run"
            ],
            "execution.action.review_plan": CANONICAL_ROUTE_PATHS[
                "execution-plan-review"
            ],
            "execution.action.save_scope": CANONICAL_ROUTE_PATHS["sync-scope"],
            "execution.action.refresh_source": CANONICAL_ROUTE_PATHS[
                "source-directory"
            ],
            "high_risk.action.label_environment": "/config",
        }.get(action_code, CANONICAL_ROUTE_PATHS["execution-dry-run"])

    def build_preflight_summary(request: Request, current_org: Any) -> dict[str, Any]:
        return merge_saved_preflight_snapshot_data(
            request.session.get("_preflight_snapshot"),
            build_preflight_snapshot(
                request,
                include_live=False,
                current_org=current_org,
            ),
        )

    def current_environment_label(request: Request) -> str:
        return str(
            getattr(
                request.app.state,
                "environment_label",
                "Unlabeled environment",
            )
            or "Unlabeled environment"
        )

    @app.get(CANONICAL_ROUTE_PATHS["jobs"], response_class=HTMLResponse)
    @app.get("/jobs", response_class=HTMLResponse)
    def jobs_page(request: Request):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user
        services = get_web_services(request)
        runtime_state = get_web_runtime_state(request)
        current_org = get_current_org(request)
        job_center_summary, _high_risk_context = build_job_center_state(
            request,
            current_org,
        )
        return render(
            request,
            "jobs.html",
            page="execution-dry-run",
            title="Job Center",
            jobs=services.jobs.list_recent_jobs(org_id=current_org.org_id, limit=30),
            job_center_summary=job_center_summary,
            active_job=services.jobs.get_active_job(org_id=current_org.org_id),
            sync_runner_error=runtime_state.sync_runner.last_error,
            current_org=current_org,
        )

    @app.get(
        CANONICAL_ROUTE_PATHS["execution-dry-run"],
        response_class=HTMLResponse,
    )
    def execution_dry_run_page(request: Request):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user
        services = get_web_services(request)
        repositories = get_web_repositories(request)
        runtime_state = get_web_runtime_state(request)
        current_org = get_current_org(request)
        preflight_summary = build_preflight_summary(request, current_org)
        active_job = services.jobs.get_active_job(org_id=current_org.org_id)
        scopes = repositories.source_directory_repo.list_scope_selections(
            org_id=current_org.org_id
        )
        blocker_code = ""
        next_action_url = CANONICAL_ROUTE_PATHS["execution-plan-review"]
        next_action_code = "execution.action.review_plan"
        if active_job:
            blocker_code = "execution.blocker.job_active"
            next_action_url = CANONICAL_ROUTE_PATHS["execution-jobs"]
            next_action_code = "execution.action.view_job_history"
        elif str(preflight_summary.get("overall_status") or "") == "error":
            blocker_code = "jobs.blocker.config_or_connectivity"
            next_action_url = CANONICAL_ROUTE_PATHS["config"]
            next_action_code = "jobs.action.fix_configuration"
        elif not scopes:
            blocker_code = "execution.blocker.scope_missing"
            next_action_url = CANONICAL_ROUTE_PATHS["sync-scope"]
            next_action_code = "execution.action.save_scope"
        return render(
            request,
            "execution_dry_run.html",
            page="execution-dry-run",
            title="Dry Run",
            current_org=current_org,
            environment_label=current_environment_label(request),
            preflight_summary=preflight_summary,
            active_job=active_job,
            scopes=scopes,
            blocker_code=blocker_code,
            next_action_url=next_action_url,
            next_action_code=next_action_code,
            recent_dry_runs=services.jobs.list_jobs_by_mode(
                org_id=current_org.org_id,
                execution_mode="dry_run",
                limit=12,
            ),
            sync_runner_error=runtime_state.sync_runner.last_error,
        )

    @app.get(
        CANONICAL_ROUTE_PATHS["execution-plan-review"],
        response_class=HTMLResponse,
    )
    def execution_plan_review_page(request: Request, plan_id: str = ""):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user
        services = get_web_services(request)
        current_org = get_current_org(request)
        environment_label = current_environment_label(request)
        review_items = services.jobs.list_review_items(
            org_id=current_org.org_id,
            organization_name=current_org.name,
            environment_label=environment_label,
            current_config_fingerprint=current_config_fingerprint(
                request,
                current_org,
            ),
            limit=50,
        )
        requested_plan_id = str(plan_id or "").strip()
        selected_item = next(
            (
                item
                for item in review_items
                if str(item["review"].job_id or "") == requested_plan_id
            ),
            None,
        )
        if selected_item is None:
            selected_item = next(
                (
                    item
                    for item in review_items
                    if str(item["review"].status or "").lower() == "pending"
                ),
                review_items[0] if review_items else None,
            )
        selected_evaluation = (
            selected_item["evaluation"] if selected_item is not None else None
        )
        selected_review = selected_item["review"] if selected_item else None
        can_approve = bool(
            selected_review
            and str(selected_review.status or "").lower() == "pending"
            and selected_evaluation
            and selected_evaluation.allowed
        )
        blocker_code = ""
        next_action_url = CANONICAL_ROUTE_PATHS["execution-apply"]
        next_action_code = "execution.action.open_apply"
        if selected_item is None:
            blocker_code = "execution.blocker.review_queue_empty"
            next_action_url = CANONICAL_ROUTE_PATHS["execution-dry-run"]
            next_action_code = "execution.action.run_dry_run"
        elif selected_evaluation and not selected_evaluation.allowed:
            blocker_code = selected_evaluation.reason_code
            next_action_url = execution_next_action_url(
                selected_evaluation.next_action_code
            )
            next_action_code = selected_evaluation.next_action_code
        elif selected_review and str(selected_review.status or "").lower() == "approved":
            next_action_url = (
                CANONICAL_ROUTE_PATHS["execution-apply"]
                + f"?plan_id={selected_review.job_id}"
            )
        return render(
            request,
            "execution_plan_review.html",
            page="execution-plan-review",
            title="Plan Review",
            current_org=current_org,
            environment_label=environment_label,
            review_items=review_items,
            selected_item=selected_item,
            can_approve=can_approve,
            blocker_code=blocker_code,
            next_action_url=next_action_url,
            next_action_code=next_action_code,
        )

    def build_apply_page_state(
        request: Request,
        current_org: Any,
        *,
        plan_id: str,
    ) -> dict[str, Any]:
        services = get_web_services(request)
        evaluation = services.jobs.evaluate_plan(
            org_id=current_org.org_id,
            organization_name=current_org.name,
            environment_label=current_environment_label(request),
            plan_job_id=str(plan_id or "").strip(),
            require_approval=True,
            current_config_fingerprint=current_config_fingerprint(
                request,
                current_org,
            ),
        )
        preflight_summary = build_preflight_summary(request, current_org)
        active_job = services.jobs.get_active_job(org_id=current_org.org_id)
        blockers: list[dict[str, Any]] = []
        if not evaluation.allowed:
            blockers.append(
                {
                    "message_code": evaluation.reason_code,
                    "params": {},
                    "next_action_code": evaluation.next_action_code,
                    "next_action_url": execution_next_action_url(
                        evaluation.next_action_code
                    ),
                }
            )
        if str(preflight_summary.get("overall_status") or "") == "error":
            blockers.append(
                {
                    "message_code": "jobs.blocker.config_or_connectivity",
                    "params": {},
                    "next_action_code": "jobs.action.fix_configuration",
                    "next_action_url": CANONICAL_ROUTE_PATHS["config"],
                }
            )
        open_conflict_count = int(
            preflight_summary.get("open_conflict_count") or 0
        )
        if open_conflict_count:
            blockers.append(
                {
                    "message_code": "jobs.blocker.open_conflicts",
                    "params": {"count": open_conflict_count},
                    "next_action_code": "jobs.action.review_conflicts",
                    "next_action_url": CANONICAL_ROUTE_PATHS["conflicts"],
                }
            )
        if active_job:
            blockers.append(
                {
                    "message_code": "execution.blocker.job_active",
                    "params": {"job_id": active_job.job_id},
                    "next_action_code": "execution.action.view_job_history",
                    "next_action_url": CANONICAL_ROUTE_PATHS["execution-jobs"],
                }
            )
        selected_apply = services.jobs.get_apply_job_for_plan(
            org_id=current_org.org_id,
            plan_job_id=str(getattr(evaluation.job, "job_id", "") or ""),
        )
        apply_status = str(getattr(selected_apply, "status", "") or "").upper()
        workflow = HighRiskOperationPolicy.workflow(
            scan_state=(
                "complete"
                if str(preflight_summary.get("overall_status") or "") != "error"
                else "blocked"
            ),
            preview_state="complete" if evaluation.job else "pending",
            confirm_state=(
                "complete"
                if evaluation.review
                and str(evaluation.review.status or "").lower() == "approved"
                and evaluation.allowed
                else ("blocked" if evaluation.job else "pending")
            ),
            execute_state=(
                "complete"
                if apply_status == "COMPLETED"
                else ("current" if not blockers else "blocked")
            ),
            audit_state=(
                "complete" if apply_status in {"COMPLETED", "FAILED"} else "pending"
            ),
        )
        return {
            "evaluation": evaluation,
            "preflight_summary": preflight_summary,
            "active_job": active_job,
            "blockers": blockers,
            "workflow": workflow,
            "latest_apply": selected_apply,
        }

    @app.get(
        CANONICAL_ROUTE_PATHS["execution-apply"],
        response_class=HTMLResponse,
    )
    def execution_apply_page(request: Request, plan_id: str = ""):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        state = build_apply_page_state(
            request,
            current_org,
            plan_id=plan_id,
        )
        return render(
            request,
            "execution_apply.html",
            page="execution-apply",
            title="Apply",
            current_org=current_org,
            environment_label=current_environment_label(request),
            **state,
        )

    @app.get(
        CANONICAL_ROUTE_PATHS["execution-jobs"],
        response_class=HTMLResponse,
    )
    def execution_job_history_page(request: Request):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user
        services = get_web_services(request)
        current_org = get_current_org(request)
        jobs = services.jobs.list_recent_jobs(org_id=current_org.org_id, limit=50)
        return render(
            request,
            "execution_job_history.html",
            page="execution-jobs",
            title="Job History",
            current_org=current_org,
            environment_label=current_environment_label(request),
            jobs=jobs,
            latest_job=jobs[0] if jobs else None,
            active_job=services.jobs.get_active_job(org_id=current_org.org_id),
        )

    @app.post(
        CANONICAL_ROUTE_PATHS["execution-plan-review"]
        + "/{job_id}/approve"
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
        canonical_request = request.url.path.startswith(
            CANONICAL_ROUTE_PATHS["execution-plan-review"]
        )
        return_url = (
            CANONICAL_ROUTE_PATHS["execution-plan-review"]
            + f"?plan_id={job_id}"
            if canonical_request
            else f"/jobs/{job_id}"
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, return_url)
        if csrf_error:
            return csrf_error

        services = get_web_services(request)
        review_record = services.jobs.get_review_record(job_id)
        if not review_record:
            flash(request, "error", "This job does not have a pending high-risk review")
            return RedirectResponse(url=return_url, status_code=303)
        current_org = get_current_org(request)
        job_record = services.jobs.get_job_record(job_id)
        if not job_record or (job_record.org_id and job_record.org_id != current_org.org_id):
            flash(request, "error", "Job does not belong to the current organization")
            return RedirectResponse(url=return_url, status_code=303)

        try:
            services.jobs.approve_review(
                org_id=current_org.org_id,
                job_id=job_id,
                reviewer_username=user.username,
                review_notes=review_notes.strip(),
            )
        except ValueError as exc:
            error_text = str(exc)
            reason_code = error_text.rsplit(" ", 1)[-1]
            if not reason_code.startswith(("execution.", "high_risk.")):
                reason_code = ""
            get_web_repositories(request).audit_repo.add_log(
                org_id=current_org.org_id,
                actor_username=user.username,
                action_type="plan_review.approve_blocked",
                target_type="sync_job",
                target_id=job_id,
                result="blocked",
                message="Plan approval was blocked because the plan is not eligible",
                payload={
                    "job_id": job_id,
                    "reason": reason_code or error_text,
                    "environment_label": current_environment_label(request),
                },
            )
            if reason_code:
                flash_t(request, "error", reason_code)
            else:
                flash(request, "error", error_text)
            return RedirectResponse(url=return_url, status_code=303)
        flash(request, "success", "Plan approved. Continue to Apply before the approval expires.")
        return RedirectResponse(url=return_url, status_code=303)

    @app.post(CANONICAL_ROUTE_PATHS["execution-dry-run"] + "/run")
    @app.post(CANONICAL_ROUTE_PATHS["execution-apply"] + "/run")
    @app.post("/jobs/run")
    def run_job(
        request: Request,
        csrf_token: str = Form(""),
        mode: str = Form(""),
        operation_code: str = Form(""),
        organization_id: str = Form(""),
        environment_label: str = Form(""),
        snapshot_version: str = Form(""),
        impact_count: str = Form(""),
        preview_id: str = Form(""),
    ):
        user = require_capability(request, "jobs.run")
        if isinstance(user, RedirectResponse):
            return user
        request_path = request.url.path
        canonical_dry_run = request_path.startswith(
            CANONICAL_ROUTE_PATHS["execution-dry-run"]
        )
        canonical_apply = request_path.startswith(
            CANONICAL_ROUTE_PATHS["execution-apply"]
        )
        return_url = (
            CANONICAL_ROUTE_PATHS["execution-dry-run"]
            if canonical_dry_run
            else (
                CANONICAL_ROUTE_PATHS["execution-apply"]
                + (f"?plan_id={preview_id}" if preview_id else "")
                if canonical_apply
                else "/jobs"
            )
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, return_url)
        if csrf_error:
            return csrf_error

        normalized_mode = (
            "dry_run"
            if canonical_dry_run
            else (
                "apply"
                if canonical_apply
                else str(mode or "").strip().lower()
            )
        )
        if normalized_mode not in {"dry_run", "apply"}:
            flash_t(request, "error", "Unsupported synchronization mode")
            return RedirectResponse(url=return_url, status_code=303)
        runtime_state = get_web_runtime_state(request)
        current_org = get_current_org(request)
        high_risk_context: HighRiskOperationContext | None = None
        selected_plan_job_id = ""
        repositories = get_web_repositories(request)
        if normalized_mode == "apply":
            apply_state = build_apply_page_state(
                request,
                current_org,
                plan_id=preview_id,
            )
            evaluation = apply_state["evaluation"]
            high_risk_context = evaluation.context
            confirmation = HighRiskOperationPolicy.validate_confirmation(
                high_risk_context,
                {
                    "operation_code": operation_code,
                    "organization_id": organization_id,
                    "environment_label": environment_label,
                    "snapshot_version": snapshot_version,
                    "impact_count": impact_count,
                    "preview_id": preview_id,
                },
            )
            if not confirmation.allowed or apply_state["blockers"]:
                reason_code = confirmation.reason_code or str(
                    apply_state["blockers"][0].get("message_code")
                    if apply_state["blockers"]
                    else "high_risk.blocker.apply_gate_not_ready"
                )
                repositories.audit_repo.add_log(
                    org_id=current_org.org_id,
                    actor_username=user.username,
                    action_type="high_risk.apply.blocked",
                    target_type="sync_apply",
                    target_id=high_risk_context.preview_id or current_org.org_id,
                    result="blocked",
                    message="Apply confirmation was blocked by the high-risk operation gate",
                    payload=high_risk_audit_payload(
                        high_risk_context,
                        reason_code=reason_code,
                    ),
                )
                flash_t(request, "error", reason_code)
                return RedirectResponse(url=return_url, status_code=303)
            selected_plan_job_id = str(evaluation.job.job_id or "")
        ok, message = runtime_state.sync_runner.launch(
            mode=normalized_mode,
            actor_username=user.username,
            org_id=current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
            plan_source_job_id=selected_plan_job_id,
        )
        if high_risk_context is not None:
            repositories.audit_repo.add_log(
                org_id=current_org.org_id,
                actor_username=user.username,
                action_type="high_risk.apply.requested",
                target_type="sync_apply",
                target_id=high_risk_context.preview_id or current_org.org_id,
                result="success" if ok else "blocked",
                message=message,
                payload=high_risk_audit_payload(high_risk_context),
            )
        flash(request, "success" if ok else "error", message)
        return RedirectResponse(url=return_url, status_code=303)

    @app.get(
        CANONICAL_ROUTE_PATHS["execution-jobs"] + "/{job_id}",
        response_class=HTMLResponse,
    )
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
            return RedirectResponse(
                url=CANONICAL_ROUTE_PATHS["execution-jobs"],
                status_code=303,
            )
        current_org = get_current_org(request)
        if job.org_id and job.org_id != current_org.org_id:
            flash(request, "error", "Job does not belong to the current organization")
            return RedirectResponse(
                url=CANONICAL_ROUTE_PATHS["execution-jobs"],
                status_code=303,
            )
        identity_service = IdentityRelationshipPreviewService(
            source_directory_repo=repositories.source_directory_repo,
            user_binding_repo=repositories.user_binding_repo,
            operation_log_repo=repositories.operation_log_repo,
            planned_operation_repo=repositories.planned_operation_repo,
        )
        identity_rows = identity_service.build_job_identity_resolutions(
            job_id,
            org_id=current_org.org_id,
        )
        identity_result = fetch_page(
            lambda *, limit, offset: (
                identity_rows[offset : offset + limit],
                len(identity_rows),
            ),
            page=parse_page_number(request.query_params.get("identity_page"), 1),
            page_size=25,
        )
        return render(
            request,
            "job_detail.html",
            page="execution-jobs",
            title=translate_text(get_ui_language(request), "Job Detail {job_id}", job_id=job_id),
            job=job,
            current_org=current_org,
            job_comparison_sections=services.jobs.build_job_comparison_sections(
                org_id=current_org.org_id,
                job=job,
            ),
            identity_resolutions=identity_result[0],
            identity_resolutions_page_data=identity_result[1],
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

    @app.get("/api/jobs/{job_id}/identity-resolutions")
    def job_identity_resolutions_api(
        request: Request,
        job_id: str,
        page_number: int = 1,
        page_size: int = 100,
    ):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
        services = get_web_services(request)
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        job = services.jobs.get_job_record(job_id)
        if not job or (job.org_id and job.org_id != current_org.org_id):
            return JSONResponse({"ok": False, "error": "Job not found"}, status_code=404)
        identity_service = IdentityRelationshipPreviewService(
            source_directory_repo=repositories.source_directory_repo,
            user_binding_repo=repositories.user_binding_repo,
            operation_log_repo=repositories.operation_log_repo,
            planned_operation_repo=repositories.planned_operation_repo,
        )
        items = identity_service.build_job_identity_resolutions(
            job_id,
            org_id=current_org.org_id,
        )
        bounded_page = max(int(page_number or 1), 1)
        bounded_size = min(max(int(page_size or 100), 1), 100)
        offset = (bounded_page - 1) * bounded_size
        return JSONResponse(
            {
                "ok": True,
                "job_id": job_id,
                "items": items[offset : offset + bounded_size],
                "total": len(items),
                "page_number": bounded_page,
                "page_size": bounded_size,
            }
        )

    @app.get(CANONICAL_ROUTE_PATHS["database"], response_class=HTMLResponse)
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
