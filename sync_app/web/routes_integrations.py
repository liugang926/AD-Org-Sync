from __future__ import annotations

from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from sync_app.services.external_integrations import (
    approve_job_review,
    build_integration_center_context,
    extract_bearer_token,
    generate_integration_api_token,
    is_valid_integration_api_token,
    organization_exists,
    serialize_conflict_record,
    serialize_job_record,
    serialize_job_records,
    validate_integration_subscription_payload,
)
from sync_app.storage.local_db import normalize_org_id


def register_integration_routes(
    app: FastAPI,
    *,
    flash: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    to_bool: Callable[[str | None, bool], bool],
) -> None:
    def _json_error(message: str, *, status_code: int) -> JSONResponse:
        response = JSONResponse({"ok": False, "error": message}, status_code=status_code)
        if status_code == 401:
            response.headers["WWW-Authenticate"] = "Bearer"
        return response

    def _authorize_integration_request(request: Request, org_id: str) -> str | JSONResponse:
        normalized_org_id = normalize_org_id(org_id, fallback="default") or "default"
        if not organization_exists(request.app.state.db_manager, normalized_org_id):
            return _json_error("Organization not found", status_code=404)
        token = extract_bearer_token(request.headers.get("authorization"))
        if not is_valid_integration_api_token(
            request.app.state.settings_repo,
            org_id=normalized_org_id,
            token=token,
        ):
            return _json_error("Invalid or missing integration API token", status_code=401)
        return normalized_org_id

    @app.get("/integrations", response_class=HTMLResponse)
    def integration_center_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        return render(
            request,
            "integration_center.html",
            page="integrations",
            title="External Integration Center",
            current_org=current_org,
            **build_integration_center_context(request.app.state.db_manager, current_org.org_id),
        )

    @app.post("/integrations/token/rotate")
    def rotate_integration_api_token(
        request: Request,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/integrations")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        token = generate_integration_api_token()
        request.app.state.settings_repo.set_value(
            "integration_api_token",
            token,
            "string",
            org_id=current_org.org_id,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="integration.token_rotate",
            target_type="integration_api",
            target_id=current_org.org_id,
            result="success",
            message="Rotated integration API token",
        )
        flash(
            request,
            "success",
            f"Integration API token rotated. Save this token now: {token}",
        )
        return RedirectResponse(url="/integrations", status_code=303)

    @app.post("/integrations/token/clear")
    def clear_integration_api_token(
        request: Request,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/integrations")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        request.app.state.settings_repo.set_value(
            "integration_api_token",
            "",
            "string",
            org_id=current_org.org_id,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="integration.token_clear",
            target_type="integration_api",
            target_id=current_org.org_id,
            result="success",
            message="Cleared integration API token",
        )
        flash(request, "success", "Integration API token cleared.")
        return RedirectResponse(url="/integrations", status_code=303)

    @app.post("/integrations/subscriptions")
    def save_integration_subscription(
        request: Request,
        csrf_token: str = Form(""),
        event_type: str = Form(""),
        target_url: str = Form(""),
        secret: str = Form(""),
        description: str = Form(""),
        is_enabled: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/integrations")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        try:
            normalized_event_type, normalized_target_url = validate_integration_subscription_payload(
                event_type=event_type,
                target_url=target_url,
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/integrations", status_code=303)

        record = request.app.state.integration_webhook_subscription_repo.upsert_subscription(
            org_id=current_org.org_id,
            event_type=normalized_event_type,
            target_url=normalized_target_url,
            secret=secret,
            description=description,
            is_enabled=to_bool(is_enabled, False),
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="integration.subscription_save",
            target_type="integration_webhook_subscription",
            target_id=str(record.id or ""),
            result="success",
            message="Saved integration webhook subscription",
            payload={
                "event_type": normalized_event_type,
                "target_url": normalized_target_url,
                "is_enabled": bool(record.is_enabled),
            },
        )
        flash(request, "success", "Webhook subscription saved.")
        return RedirectResponse(url="/integrations", status_code=303)

    @app.post("/integrations/subscriptions/{subscription_id}/delete")
    def delete_integration_subscription(
        request: Request,
        subscription_id: int,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/integrations")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        subscription = request.app.state.integration_webhook_subscription_repo.get_subscription_record(
            subscription_id,
            org_id=current_org.org_id,
        )
        if subscription is None:
            flash(request, "error", "Webhook subscription not found.")
            return RedirectResponse(url="/integrations", status_code=303)
        request.app.state.integration_webhook_subscription_repo.delete_subscription(
            subscription_id,
            org_id=current_org.org_id,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="integration.subscription_delete",
            target_type="integration_webhook_subscription",
            target_id=str(subscription_id),
            result="success",
            message="Deleted integration webhook subscription",
            payload={
                "event_type": subscription.event_type,
                "target_url": subscription.target_url,
            },
        )
        flash(request, "success", "Webhook subscription deleted.")
        return RedirectResponse(url="/integrations", status_code=303)

    @app.get("/api/integrations/orgs/{org_id}/jobs")
    def integration_jobs_api(request: Request, org_id: str):
        authorized_org_id = _authorize_integration_request(request, org_id)
        if isinstance(authorized_org_id, JSONResponse):
            return authorized_org_id
        limit = max(min(int(request.query_params.get("limit") or 20), 100), 1)
        status_filter = str(request.query_params.get("status") or "").strip().upper()
        jobs = request.app.state.job_repo.list_recent_job_records(limit=limit * 3, org_id=authorized_org_id)
        if status_filter:
            jobs = [job for job in jobs if str(getattr(job, "status", "") or "").strip().upper() == status_filter]
        jobs = jobs[:limit]
        return JSONResponse(
            {
                "ok": True,
                "org_id": authorized_org_id,
                "count": len(jobs),
                "items": serialize_job_records(jobs, request.app.state.review_repo),
            }
        )

    @app.get("/api/integrations/orgs/{org_id}/jobs/{job_id}")
    def integration_job_detail_api(request: Request, org_id: str, job_id: str):
        authorized_org_id = _authorize_integration_request(request, org_id)
        if isinstance(authorized_org_id, JSONResponse):
            return authorized_org_id
        job_record = request.app.state.job_repo.get_job_record(job_id)
        if job_record is None or (job_record.org_id or "default") != authorized_org_id:
            return _json_error("Job not found", status_code=404)
        review_record = request.app.state.review_repo.get_review_record_by_job_id(job_id)
        return JSONResponse(
            {
                "ok": True,
                "org_id": authorized_org_id,
                "item": serialize_job_record(job_record, review_record=review_record),
            }
        )

    @app.get("/api/integrations/orgs/{org_id}/conflicts")
    def integration_conflicts_api(request: Request, org_id: str):
        authorized_org_id = _authorize_integration_request(request, org_id)
        if isinstance(authorized_org_id, JSONResponse):
            return authorized_org_id
        limit = max(min(int(request.query_params.get("limit") or 50), 200), 1)
        status_filter = str(request.query_params.get("status") or "open").strip()
        job_id_filter = str(request.query_params.get("job_id") or "").strip() or None
        conflicts = request.app.state.conflict_repo.list_conflict_records(
            limit=limit,
            job_id=job_id_filter,
            status=status_filter or None,
            org_id=authorized_org_id,
        )
        return JSONResponse(
            {
                "ok": True,
                "org_id": authorized_org_id,
                "count": len(conflicts),
                "items": [serialize_conflict_record(conflict) for conflict in conflicts],
            }
        )

    @app.post("/api/integrations/orgs/{org_id}/reviews/{job_id}/approve")
    async def integration_review_approval_api(request: Request, org_id: str, job_id: str):
        authorized_org_id = _authorize_integration_request(request, org_id)
        if isinstance(authorized_org_id, JSONResponse):
            return authorized_org_id
        payload: dict[str, Any] = {}
        try:
            if str(request.headers.get("content-type") or "").lower().startswith("application/json"):
                parsed_payload = await request.json()
                if isinstance(parsed_payload, dict):
                    payload = dict(parsed_payload)
        except Exception:
            payload = {}
        reviewer_username = str(payload.get("reviewer_username") or "integration_api").strip() or "integration_api"
        review_notes = str(payload.get("review_notes") or "").strip()
        try:
            result = approve_job_review(
                request.app.state.db_manager,
                org_id=authorized_org_id,
                job_id=job_id,
                reviewer_username=reviewer_username,
                review_notes=review_notes,
            )
        except ValueError as exc:
            return _json_error(str(exc), status_code=404 if "not found" in str(exc).lower() else 400)

        request.app.state.audit_repo.add_log(
            org_id=authorized_org_id,
            actor_username=reviewer_username,
            action_type="integration.review_approve",
            target_type="sync_job",
            target_id=job_id,
            result="success",
            message="Approved high-risk synchronization plan through integration API",
            payload={
                "expires_at": result["expires_at_iso"],
                "replay_request_id": result["replay_request_id"],
                "fresh_approval": result["fresh_approval"],
            },
        )
        return JSONResponse(
            {
                "ok": True,
                "org_id": authorized_org_id,
                "job_id": job_id,
                "expires_at": result["expires_at_iso"],
                "replay_request_id": result["replay_request_id"],
                "fresh_approval": result["fresh_approval"],
                "review": serialize_job_record(result["job"], review_record=result["review"])["review"],
            }
        )
