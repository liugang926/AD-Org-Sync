from __future__ import annotations

from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from sync_app.web.app_state import get_web_runtime_state, get_web_services


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
    def _wake_outbox_worker(request: Request) -> None:
        worker = get_web_runtime_state(request).integration_outbox_worker
        if worker is not None:
            worker.wake()

    def _json_error(message: str, *, status_code: int) -> JSONResponse:
        response = JSONResponse({"ok": False, "error": message}, status_code=status_code)
        if status_code == 401:
            response.headers["WWW-Authenticate"] = "Bearer"
        return response

    def _authorize_integration_request(request: Request, org_id: str) -> str | JSONResponse:
        auth_result = get_web_services(request).integrations.authorize_api_request(
            org_id=org_id,
            authorization_header=request.headers.get("authorization"),
        )
        if not auth_result.get("ok"):
            return _json_error(
                str(auth_result.get("error") or "Unauthorized"),
                status_code=int(auth_result.get("status_code") or 401),
            )
        return str(auth_result["org_id"])

    @app.get("/integrations", response_class=HTMLResponse)
    def integration_center_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        services = get_web_services(request)
        return render(
            request,
            "integration_center.html",
            page="integrations",
            title="External Integration Center",
            current_org=current_org,
            **services.integrations.build_center_context(org_id=current_org.org_id),
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
        token = get_web_services(request).integrations.rotate_api_token(
            org_id=current_org.org_id,
            actor_username=user.username,
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
        get_web_services(request).integrations.clear_api_token(
            org_id=current_org.org_id,
            actor_username=user.username,
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
            get_web_services(request).integrations.save_subscription(
                org_id=current_org.org_id,
                actor_username=user.username,
                event_type=event_type,
                target_url=target_url,
                secret=secret,
                description=description,
                is_enabled=to_bool(is_enabled, False),
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/integrations", status_code=303)
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
        deleted = get_web_services(request).integrations.delete_subscription(
            org_id=current_org.org_id,
            actor_username=user.username,
            subscription_id=subscription_id,
        )
        if not deleted:
            flash(request, "error", "Webhook subscription not found.")
            return RedirectResponse(url="/integrations", status_code=303)
        flash(request, "success", "Webhook subscription deleted.")
        return RedirectResponse(url="/integrations", status_code=303)

    @app.post("/integrations/deliveries/{delivery_id}/retry")
    def retry_integration_delivery(
        request: Request,
        delivery_id: int,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/integrations")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        try:
            get_web_services(request).integrations.retry_delivery(
                org_id=current_org.org_id,
                actor_username=user.username,
                delivery_id=delivery_id,
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/integrations", status_code=303)

        _wake_outbox_worker(request)
        flash(request, "success", "Failed delivery requeued and scheduled for retry.")
        return RedirectResponse(url="/integrations", status_code=303)

    @app.post("/integrations/deliveries/retry-failed")
    def retry_failed_integration_deliveries(
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
        retried_count = get_web_services(request).integrations.retry_failed_deliveries(
            org_id=current_org.org_id,
            actor_username=user.username,
        )
        if retried_count <= 0:
            flash(request, "warning", "No failed deliveries are waiting for manual replay.")
            return RedirectResponse(url="/integrations", status_code=303)

        _wake_outbox_worker(request)
        flash(request, "success", f"Requeued {retried_count} failed deliveries.")
        return RedirectResponse(url="/integrations", status_code=303)

    @app.get("/api/integrations/orgs/{org_id}/jobs")
    def integration_jobs_api(request: Request, org_id: str):
        authorized_org_id = _authorize_integration_request(request, org_id)
        if isinstance(authorized_org_id, JSONResponse):
            return authorized_org_id
        limit = max(min(int(request.query_params.get("limit") or 20), 100), 1)
        status_filter = str(request.query_params.get("status") or "").strip().upper()
        return JSONResponse(
            get_web_services(request).integrations.build_jobs_api_payload(
                org_id=authorized_org_id,
                limit=limit,
                status_filter=status_filter,
            )
        )

    @app.get("/api/integrations/orgs/{org_id}/jobs/{job_id}")
    def integration_job_detail_api(request: Request, org_id: str, job_id: str):
        authorized_org_id = _authorize_integration_request(request, org_id)
        if isinstance(authorized_org_id, JSONResponse):
            return authorized_org_id
        payload = get_web_services(request).integrations.build_job_detail_api_payload(
            org_id=authorized_org_id,
            job_id=job_id,
        )
        if payload is None:
            return _json_error("Job not found", status_code=404)
        return JSONResponse(payload)

    @app.get("/api/integrations/orgs/{org_id}/conflicts")
    def integration_conflicts_api(request: Request, org_id: str):
        authorized_org_id = _authorize_integration_request(request, org_id)
        if isinstance(authorized_org_id, JSONResponse):
            return authorized_org_id
        limit = max(min(int(request.query_params.get("limit") or 50), 200), 1)
        status_filter = str(request.query_params.get("status") or "open").strip()
        job_id_filter = str(request.query_params.get("job_id") or "").strip() or None
        return JSONResponse(
            get_web_services(request).integrations.build_conflicts_api_payload(
                org_id=authorized_org_id,
                limit=limit,
                status_filter=status_filter,
                job_id_filter=job_id_filter,
            )
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
            result = get_web_services(request).integrations.approve_review_via_api(
                org_id=authorized_org_id,
                job_id=job_id,
                reviewer_username=reviewer_username,
                review_notes=review_notes,
            )
        except ValueError as exc:
            return _json_error(str(exc), status_code=404 if "not found" in str(exc).lower() else 400)

        return JSONResponse(result)
