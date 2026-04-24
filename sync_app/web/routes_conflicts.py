from __future__ import annotations

from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.web.app_state import get_web_services


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
        services = get_web_services(request)

        status_filter = status if status in {"open", "resolved", "dismissed"} else None
        conflicts, page_data = fetch_page(
            lambda *, limit, offset: services.conflicts.list_conflicts_page(
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
        conflict_recommendations = services.conflicts.build_recommendations(conflicts)
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
        services = get_web_services(request)
        conflict = services.conflicts.get_conflict_record(conflict_id, org_id=current_org.org_id)
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
        services = get_web_services(request)
        conflict = services.conflicts.get_conflict_record(conflict_id, org_id=current_org.org_id)
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

        ok, normalized_ad_username, _resolved_count = services.conflicts.resolve_manual_binding(
            app=request.app,
            conflict=conflict,
            org_id=current_org.org_id,
            actor_username=user.username,
            ad_username=ad_username,
            apply_conflict_manual_binding=apply_conflict_manual_binding,
        )
        if not ok:
            flash(request, "error", normalized_ad_username)
            return RedirectResponse(url=fallback_url, status_code=303)
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
        services = get_web_services(request)
        conflict = services.conflicts.get_conflict_record(conflict_id, org_id=current_org.org_id)
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

        ok, rule_notes, _resolved_count = services.conflicts.resolve_skip_user_sync(
            app=request.app,
            conflict=conflict,
            org_id=current_org.org_id,
            actor_username=user.username,
            notes=to_text(notes),
            apply_conflict_skip_user_sync=apply_conflict_skip_user_sync,
        )
        if not ok:
            flash(request, "error", rule_notes)
            return RedirectResponse(url=fallback_url, status_code=303)
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
        services = get_web_services(request)
        conflict = services.conflicts.get_conflict_record(conflict_id, org_id=current_org.org_id)
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

        ok, detail, _resolved_count, recommendation = services.conflicts.apply_recommendation(
            app=request.app,
            conflict=conflict,
            org_id=current_org.org_id,
            actor_username=user.username,
            confirmation_reason=to_text(confirmation_reason),
            apply_conflict_recommendation=apply_conflict_recommendation,
        )
        if not ok:
            flash(request, "error", detail)
            return RedirectResponse(url=fallback_url, status_code=303)
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
        services = get_web_services(request)
        conflict = services.conflicts.get_conflict_record(conflict_id, org_id=current_org.org_id)
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

        services.conflicts.dismiss_conflict(
            conflict=conflict,
            org_id=current_org.org_id,
            actor_username=user.username,
            notes=to_text(notes),
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
        services = get_web_services(request)
        conflict = services.conflicts.get_conflict_record(conflict_id, org_id=current_org.org_id)
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

        services.conflicts.reopen_conflict(
            conflict=conflict,
            org_id=current_org.org_id,
            actor_username=user.username,
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
        services = get_web_services(request)
        raw_conflict_ids = [str(item or "").strip() for item in conflict_ids] if isinstance(conflict_ids, list) else []
        selected_conflict_ids = [int(item) for item in raw_conflict_ids if item.isdigit()]
        if normalized_action not in {"apply_recommendation", "skip_user_sync", "dismiss", "reopen"}:
            flash(request, "error", "Unsupported bulk conflict action")
            return RedirectResponse(url=fallback_url, status_code=303)
        if not selected_conflict_ids:
            flash(request, "error", "No conflicts selected")
            return RedirectResponse(url=fallback_url, status_code=303)
        if (
            normalized_action == "apply_recommendation"
            and not to_text(notes)
            and services.conflicts.bulk_apply_requires_confirmation(
                org_id=current_org.org_id,
                conflict_ids=selected_conflict_ids,
            )
        ):
            flash(request, "error", "Low-confidence recommendations require a confirmation reason for bulk apply")
            return RedirectResponse(url=fallback_url, status_code=303)

        updated_count, skipped_count = services.conflicts.execute_bulk_action(
            app=request.app,
            org_id=current_org.org_id,
            actor_username=user.username,
            action=normalized_action,
            selected_conflict_ids=selected_conflict_ids,
            notes=to_text(notes),
            apply_conflict_recommendation=apply_conflict_recommendation,
            apply_conflict_skip_user_sync=apply_conflict_skip_user_sync,
        )
        flash(
            request,
            "success" if updated_count else "warning",
            f"Bulk action {normalized_action} updated {updated_count} conflicts, skipped {skipped_count}",
        )
        return RedirectResponse(url=fallback_url, status_code=303)
