from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.core.conflict_recommendations import (
    recommend_conflict_resolution,
    recommendation_requires_confirmation,
)


def register_conflict_routes(
    app: FastAPI,
    *,
    apply_conflict_manual_binding: Callable[..., tuple[bool, str, int]],
    apply_conflict_recommendation: Callable[..., tuple[bool, str, int, dict[str, Any] | None]],
    apply_conflict_skip_user_sync: Callable[..., tuple[bool, str, int]],
    build_conflict_decision_guide: Callable[..., dict[str, Any]],
    build_conflicts_return_url: Callable[[str, str, str], str],
    fetch_page: Callable[..., tuple[list[Any], dict[str, Any]]],
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    parse_page_number: Callable[[str | None, int], int],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    resolve_remembered_filters: Callable[..., dict[str, Any]],
    to_text: Callable[[Any], str],
) -> None:
    @app.get("/conflicts", response_class=HTMLResponse)
    def conflicts_page(request: Request):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        remembered_filters = resolve_remembered_filters(
            request,
            page_name="conflicts",
            defaults={"q": "", "status": "open", "job_id": ""},
        )
        query = str(remembered_filters["q"])
        status = str(remembered_filters["status"] or "open").strip().lower()
        job_id = str(remembered_filters["job_id"])
        page_number = parse_page_number(request.query_params.get("page_number"), 1)

        status_filter = status if status in {"open", "resolved", "dismissed"} else None
        conflicts, page_data = fetch_page(
            lambda *, limit, offset: request.app.state.conflict_repo.list_conflict_records_page(
                limit=limit,
                offset=offset,
                job_id=job_id or None,
                status=status_filter,
                query=query,
                org_id=current_org.org_id,
            ),
            page=page_number,
            page_size=30,
        )
        conflict_recommendations = {
            item.id: recommend_conflict_resolution(item)
            for item in conflicts
        }
        return render(
            request,
            "conflicts.html",
            page="conflicts",
            title="Conflict Queue",
            conflicts=conflicts,
            conflict_recommendations=conflict_recommendations,
            conflict_page_data=page_data,
            conflict_query=query,
            conflict_status=status if status_filter else "all",
            conflict_job_id=job_id,
            current_org=current_org,
            filters_are_remembered=True,
        )

    @app.get("/conflicts/{conflict_id}/decision-guide", response_class=HTMLResponse)
    def conflict_decision_guide_page(request: Request, conflict_id: int):
        user = require_capability(request, "jobs.read")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
        if not conflict:
            flash(request, "error", "Conflict record not found")
            return RedirectResponse(url="/conflicts", status_code=303)

        return_query = to_text(request.query_params.get("return_query"))
        return_status = to_text(request.query_params.get("return_status")) or "open"
        return_job_id = to_text(request.query_params.get("return_job_id")) or conflict.job_id
        return_url = build_conflicts_return_url(return_query, return_status, return_job_id)
        decision_guide = build_conflict_decision_guide(
            request,
            conflict,
            ad_username=to_text(request.query_params.get("ad_username")),
        )
        return render(
            request,
            "conflict_decision_guide.html",
            page="conflicts",
            title=f"Decision Guide {conflict.source_id or conflict.id}",
            conflict=conflict,
            decision_guide=decision_guide,
            current_org=current_org,
            return_url=return_url,
            return_query=return_query,
            return_status=return_status,
            return_job_id=return_job_id,
        )

    @app.post("/conflicts/{conflict_id}/resolve-binding")
    def resolve_conflict_binding(
        request: Request,
        conflict_id: int,
        csrf_token: str = Form(""),
        ad_username: str = Form(...),
        return_query: str = Form(""),
        return_status: str = Form(""),
        return_job_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
        if not conflict:
            flash(request, "error", "Conflict record not found")
            return RedirectResponse(url="/conflicts", status_code=303)
        fallback_url = build_conflicts_return_url(
            to_text(return_query),
            to_text(return_status),
            to_text(return_job_id) or conflict.job_id,
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        if conflict.status != "open":
            flash(request, "error", "Conflict is already processed")
            return RedirectResponse(url=fallback_url, status_code=303)

        ok, normalized_ad_username, resolved_count = apply_conflict_manual_binding(
            app=request.app,
            conflict=conflict,
            ad_username=ad_username,
            actor_username=user.username,
            org_id=current_org.org_id,
        )
        if not ok:
            flash(request, "error", normalized_ad_username)
            return RedirectResponse(url=fallback_url, status_code=303)
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="conflict.resolve_manual_binding",
            target_type="sync_conflict",
            target_id=str(conflict.id),
            result="success",
            message="Resolved conflict by creating manual binding",
            payload={
                "job_id": conflict.job_id,
                "source_user_id": conflict.source_id,
                "ad_username": normalized_ad_username,
                "resolved_count": resolved_count,
            },
        )
        flash_t(
            request,
            "success",
            "Resolved conflict with manual binding {source_id} -> {ad_username}",
            source_id=conflict.source_id,
            ad_username=normalized_ad_username,
        )
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/conflicts/{conflict_id}/skip-user")
    def resolve_conflict_with_skip_user(
        request: Request,
        conflict_id: int,
        csrf_token: str = Form(""),
        notes: str = Form(""),
        return_query: str = Form(""),
        return_status: str = Form(""),
        return_job_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
        if not conflict:
            flash(request, "error", "Conflict record not found")
            return RedirectResponse(url="/conflicts", status_code=303)
        fallback_url = build_conflicts_return_url(
            to_text(return_query),
            to_text(return_status),
            to_text(return_job_id) or conflict.job_id,
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        if conflict.status != "open":
            flash(request, "error", "Conflict is already processed")
            return RedirectResponse(url=fallback_url, status_code=303)
        if not conflict.source_id:
            flash(request, "error", "Conflict does not have a source user to whitelist")
            return RedirectResponse(url=fallback_url, status_code=303)

        ok, rule_notes, resolved_count = apply_conflict_skip_user_sync(
            app=request.app,
            conflict=conflict,
            actor_username=user.username,
            org_id=current_org.org_id,
            notes=to_text(notes),
        )
        if not ok:
            flash(request, "error", rule_notes)
            return RedirectResponse(url=fallback_url, status_code=303)
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="conflict.resolve_skip_user",
            target_type="sync_conflict",
            target_id=str(conflict.id),
            result="success",
            message="Resolved conflict by adding skip_user_sync exception",
            payload={
                "job_id": conflict.job_id,
                "source_user_id": conflict.source_id,
                "notes": rule_notes,
                "resolved_count": resolved_count,
            },
        )
        flash_t(request, "success", "Added skip_user_sync for {source_id}", source_id=conflict.source_id)
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/conflicts/{conflict_id}/apply-recommendation")
    def apply_conflict_recommendation_route(
        request: Request,
        conflict_id: int,
        csrf_token: str = Form(""),
        confirmation_reason: str = Form(""),
        return_query: str = Form(""),
        return_status: str = Form(""),
        return_job_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
        if not conflict:
            flash(request, "error", "Conflict record not found")
            return RedirectResponse(url="/conflicts", status_code=303)
        fallback_url = build_conflicts_return_url(
            to_text(return_query),
            to_text(return_status),
            to_text(return_job_id) or conflict.job_id,
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        if conflict.status != "open":
            flash(request, "error", "Conflict is already processed")
            return RedirectResponse(url=fallback_url, status_code=303)

        ok, detail, resolved_count, recommendation = apply_conflict_recommendation(
            app=request.app,
            conflict=conflict,
            actor_username=user.username,
            org_id=current_org.org_id,
            confirmation_reason=to_text(confirmation_reason),
        )
        if not ok:
            flash(request, "error", detail)
            return RedirectResponse(url=fallback_url, status_code=303)

        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="conflict.apply_recommendation",
            target_type="sync_conflict",
            target_id=str(conflict.id),
            result="success",
            message="Applied recommended conflict resolution",
            payload={
                "job_id": conflict.job_id,
                "source_user_id": conflict.source_id,
                "recommendation": recommendation,
                "detail": detail,
                "resolved_count": resolved_count,
            },
        )
        flash_t(
            request,
            "success",
            "Applied recommendation: {label}",
            label=str(recommendation.get("label") or "-"),
        )
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/conflicts/{conflict_id}/dismiss")
    def dismiss_conflict(
        request: Request,
        conflict_id: int,
        csrf_token: str = Form(""),
        notes: str = Form(""),
        return_query: str = Form(""),
        return_status: str = Form(""),
        return_job_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
        if not conflict:
            flash(request, "error", "Conflict record not found")
            return RedirectResponse(url="/conflicts", status_code=303)
        fallback_url = build_conflicts_return_url(
            to_text(return_query),
            to_text(return_status),
            to_text(return_job_id) or conflict.job_id,
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error

        request.app.state.conflict_repo.update_conflict_status(
            conflict.id,
            status="dismissed",
            resolution_payload={
                "action": "dismissed",
                "notes": to_text(notes),
                "actor_username": user.username,
            },
            resolved_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="conflict.dismiss",
            target_type="sync_conflict",
            target_id=str(conflict.id),
            result="success",
            message="Dismissed sync conflict",
            payload={"job_id": conflict.job_id, "notes": to_text(notes)},
        )
        flash(request, "success", "Conflict dismissed")
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/conflicts/{conflict_id}/reopen")
    def reopen_conflict(
        request: Request,
        conflict_id: int,
        csrf_token: str = Form(""),
        return_query: str = Form(""),
        return_status: str = Form(""),
        return_job_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
        if not conflict:
            flash(request, "error", "Conflict record not found")
            return RedirectResponse(url="/conflicts", status_code=303)
        fallback_url = build_conflicts_return_url(
            to_text(return_query),
            to_text(return_status),
            to_text(return_job_id) or conflict.job_id,
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error
        if conflict.status == "open":
            flash(request, "error", "Conflict is already open")
            return RedirectResponse(url=fallback_url, status_code=303)

        request.app.state.conflict_repo.update_conflict_status(
            conflict.id,
            status="open",
            resolution_payload=None,
            resolved_at=None,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="conflict.reopen",
            target_type="sync_conflict",
            target_id=str(conflict.id),
            result="success",
            message="Reopened sync conflict",
            payload={"job_id": conflict.job_id, "previous_status": conflict.status},
        )
        flash(request, "success", "Conflict reopened")
        return RedirectResponse(url=fallback_url, status_code=303)

    @app.post("/conflicts/bulk")
    def bulk_conflict_action(
        request: Request,
        csrf_token: str = Form(""),
        action: str = Form(...),
        conflict_ids: list[str] = Form([]),
        notes: str = Form(""),
        return_query: str = Form(""),
        return_status: str = Form(""),
        return_job_id: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user

        fallback_url = build_conflicts_return_url(
            to_text(return_query),
            to_text(return_status),
            to_text(return_job_id),
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, fallback_url)
        if csrf_error:
            return csrf_error

        normalized_action = to_text(action).lower()
        current_org = get_current_org(request)
        raw_conflict_ids = [str(item or "").strip() for item in conflict_ids] if isinstance(conflict_ids, list) else []
        selected_conflict_ids = [int(item) for item in raw_conflict_ids if item.isdigit()]
        if normalized_action not in {"apply_recommendation", "skip_user_sync", "dismiss", "reopen"}:
            flash(request, "error", "Unsupported bulk conflict action")
            return RedirectResponse(url=fallback_url, status_code=303)
        if not selected_conflict_ids:
            flash(request, "error", "No conflicts selected")
            return RedirectResponse(url=fallback_url, status_code=303)
        if normalized_action == "apply_recommendation" and not to_text(notes):
            for conflict_id in selected_conflict_ids:
                conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
                if not conflict or conflict.status != "open":
                    continue
                if recommendation_requires_confirmation(recommend_conflict_resolution(conflict)):
                    flash(request, "error", "Low-confidence recommendations require a confirmation reason for bulk apply")
                    return RedirectResponse(url=fallback_url, status_code=303)

        updated_count = 0
        skipped_count = 0
        for conflict_id in selected_conflict_ids:
            conflict = request.app.state.conflict_repo.get_conflict_record(conflict_id, org_id=current_org.org_id)
            if not conflict:
                skipped_count += 1
                continue

            if normalized_action == "reopen":
                if conflict.status == "open":
                    skipped_count += 1
                    continue
                request.app.state.conflict_repo.update_conflict_status(
                    conflict.id,
                    status="open",
                    resolution_payload=None,
                    resolved_at=None,
                )
                updated_count += 1
                continue

            if conflict.status != "open":
                skipped_count += 1
                continue

            if normalized_action == "dismiss":
                request.app.state.conflict_repo.update_conflict_status(
                    conflict.id,
                    status="dismissed",
                    resolution_payload={
                        "action": "dismissed",
                        "notes": to_text(notes),
                        "actor_username": user.username,
                        "bulk": True,
                    },
                    resolved_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                )
                updated_count += 1
                continue

            if normalized_action == "apply_recommendation":
                ok, _detail, resolved_count, _recommendation = apply_conflict_recommendation(
                    app=request.app,
                    conflict=conflict,
                    actor_username=user.username,
                    org_id=current_org.org_id,
                    confirmation_reason=to_text(notes),
                )
                if ok and resolved_count:
                    updated_count += 1
                else:
                    skipped_count += 1
                continue

            if normalized_action == "skip_user_sync":
                ok, _rule_notes, resolved_count = apply_conflict_skip_user_sync(
                    app=request.app,
                    conflict=conflict,
                    actor_username=user.username,
                    org_id=current_org.org_id,
                    notes=to_text(notes) or f"bulk resolved from conflict {conflict.id}",
                )
                if ok and resolved_count:
                    updated_count += 1
                else:
                    skipped_count += 1

        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="conflict.bulk_action",
            target_type="sync_conflict",
            target_id="bulk",
            result="success" if updated_count else "warning",
            message="Executed bulk conflict action",
            payload={
                "action": normalized_action,
                "selected_count": len(selected_conflict_ids),
                "updated_count": updated_count,
                "skipped_count": skipped_count,
            },
        )
        flash(
            request,
            "success" if updated_count else "warning",
            f"Bulk action {normalized_action} updated {updated_count} conflicts, skipped {skipped_count}",
        )
        return RedirectResponse(url=fallback_url, status_code=303)
