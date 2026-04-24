from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.services.lifecycle_workbench import (
    apply_lifecycle_bulk_action,
    apply_offboarding_bulk_action,
    apply_replay_bulk_action,
    build_lifecycle_workbench_data,
)
from sync_app.web.app_state import get_web_repositories


def _normalize_id_list(values: list[int] | None) -> list[int]:
    normalized_ids: list[int] = []
    seen: set[int] = set()
    for value in list(values or []):
        try:
            candidate = int(value)
        except (TypeError, ValueError):
            continue
        if candidate <= 0 or candidate in seen:
            continue
        seen.add(candidate)
        normalized_ids.append(candidate)
    return normalized_ids


def _action_label(action: str) -> str:
    normalized = str(action or "").strip().lower()
    return {
        "approve": "approved",
        "defer": "deferred",
        "skip": "skipped",
        "retry": "retried",
    }.get(normalized, normalized or "updated")


def _build_result_message(prefix: str, result: dict[str, Any]) -> str:
    processed_count = int(result.get("processed_count") or 0)
    if processed_count <= 0:
        return f"No {prefix.lower()} items were updated."
    message_parts = [f"{_action_label(str(result.get('action') or 'updated')).capitalize()} {processed_count} {prefix.lower()} item(s)"]
    if int(result.get("replay_request_count") or 0) > 0:
        message_parts.append(f"queued {int(result['replay_request_count'])} replay request(s)")
    if int(result.get("exception_rule_count") or 0) > 0:
        message_parts.append(f"saved {int(result['exception_rule_count'])} exception rule(s)")
    if int(result.get("hold_cleared_count") or 0) > 0:
        message_parts.append(f"cleared {int(result['hold_cleared_count'])} manual hold(s)")
    if int(result.get("unsupported_count") or 0) > 0:
        message_parts.append(f"left {int(result['unsupported_count'])} unsupported item(s) unchanged")
    if int(result.get("skipped_count") or 0) > 0:
        message_parts.append(f"skipped {int(result['skipped_count'])} missing or stale item(s)")
    return ". ".join(message_parts) + "."


def register_lifecycle_routes(
    app: FastAPI,
    *,
    flash: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
) -> None:
    @app.get("/lifecycle", response_class=HTMLResponse)
    def lifecycle_workbench_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        return render(
            request,
            "lifecycle_workbench.html",
            page="lifecycle",
            title="Lifecycle Workbench",
            current_org=current_org,
            **build_lifecycle_workbench_data(
                repositories.db_manager,
                current_org.org_id,
            ),
        )

    @app.post("/lifecycle/offboarding")
    def lifecycle_offboarding_action(
        request: Request,
        csrf_token: str = Form(""),
        action: str = Form(""),
        record_ids: list[int] = Form([]),
        delay_days: int = Form(0),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/lifecycle")
        if csrf_error:
            return csrf_error

        normalized_ids = _normalize_id_list(record_ids)
        if not normalized_ids:
            flash(request, "warning", "Select at least one offboarding queue item.")
            return RedirectResponse(url="/lifecycle", status_code=303)

        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        try:
            result = apply_offboarding_bulk_action(
                repositories.db_manager,
                current_org.org_id,
                actor_username=user.username,
                action=action,
                record_ids=normalized_ids,
                delay_days=max(int(delay_days or 0), 0),
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/lifecycle", status_code=303)

        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type=f"lifecycle_workbench.offboarding_{str(action or '').strip().lower() or 'update'}",
            target_type="offboarding_queue",
            target_id="bulk",
            result=("success" if int(result.get("processed_count") or 0) > 0 else "warning"),
            message="Processed lifecycle workbench offboarding action",
            payload={
                "record_ids": normalized_ids,
                "delay_days": max(int(delay_days or 0), 0),
                **result,
            },
        )
        flash(
            request,
            ("success" if int(result.get("processed_count") or 0) > 0 else "warning"),
            _build_result_message("Offboarding Queue", result),
        )
        return RedirectResponse(url="/lifecycle", status_code=303)

    @app.post("/lifecycle/lifecycle-queue")
    def lifecycle_queue_action(
        request: Request,
        csrf_token: str = Form(""),
        lifecycle_type: str = Form(""),
        action: str = Form(""),
        record_ids: list[int] = Form([]),
        delay_days: int = Form(0),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/lifecycle")
        if csrf_error:
            return csrf_error

        normalized_ids = _normalize_id_list(record_ids)
        if not normalized_ids:
            flash(request, "warning", "Select at least one lifecycle queue item.")
            return RedirectResponse(url="/lifecycle", status_code=303)

        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        try:
            result = apply_lifecycle_bulk_action(
                repositories.db_manager,
                current_org.org_id,
                actor_username=user.username,
                lifecycle_type=lifecycle_type,
                action=action,
                record_ids=normalized_ids,
                delay_days=max(int(delay_days or 0), 0),
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/lifecycle", status_code=303)

        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type=f"lifecycle_workbench.{str(lifecycle_type or '').strip().lower() or 'lifecycle'}_{str(action or '').strip().lower() or 'update'}",
            target_type="user_lifecycle_queue",
            target_id="bulk",
            result=("success" if int(result.get("processed_count") or 0) > 0 else "warning"),
            message="Processed lifecycle workbench lifecycle action",
            payload={
                "lifecycle_type": str(lifecycle_type or "").strip().lower(),
                "record_ids": normalized_ids,
                "delay_days": max(int(delay_days or 0), 0),
                **result,
            },
        )
        flash(
            request,
            ("success" if int(result.get("processed_count") or 0) > 0 else "warning"),
            _build_result_message(
                (
                    "Future Onboarding Queue"
                    if str(lifecycle_type or "").strip().lower() == "future_onboarding"
                    else "Contractor Expiry Queue"
                ),
                result,
            ),
        )
        return RedirectResponse(url="/lifecycle", status_code=303)

    @app.post("/lifecycle/replay")
    def lifecycle_replay_action(
        request: Request,
        csrf_token: str = Form(""),
        action: str = Form(""),
        request_ids: list[int] = Form([]),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/lifecycle")
        if csrf_error:
            return csrf_error

        normalized_ids = _normalize_id_list(request_ids)
        if not normalized_ids:
            flash(request, "warning", "Select at least one replay request.")
            return RedirectResponse(url="/lifecycle", status_code=303)

        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        try:
            result = apply_replay_bulk_action(
                repositories.db_manager,
                current_org.org_id,
                actor_username=user.username,
                action=action,
                request_ids=normalized_ids,
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/lifecycle", status_code=303)

        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type=f"lifecycle_workbench.replay_{str(action or '').strip().lower() or 'update'}",
            target_type="sync_replay_request",
            target_id="bulk",
            result=("success" if int(result.get("processed_count") or 0) > 0 else "warning"),
            message="Processed lifecycle workbench replay action",
            payload={
                "request_ids": normalized_ids,
                **result,
            },
        )
        flash(
            request,
            ("success" if int(result.get("processed_count") or 0) > 0 else "warning"),
            _build_result_message("Replay Queue", result),
        )
        return RedirectResponse(url="/lifecycle", status_code=303)
