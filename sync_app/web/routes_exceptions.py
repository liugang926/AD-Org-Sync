from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.core.exception_rules import (
    EXCEPTION_MATCH_TYPE_LABELS,
    EXCEPTION_RULE_DEFINITIONS,
    get_exception_rule_definition,
    normalize_exception_rule_type,
)
from sync_app.storage.local_db import utcnow_iso
from sync_app.web.rule_governance import build_rule_governance_summary


def register_exception_routes(
    app: FastAPI,
    *,
    department_exists_in_source_provider: Callable[[Request, str], tuple[bool, str | None]],
    enqueue_replay_request: Callable[..., None],
    fetch_page: Callable[..., tuple[list[Any], dict[str, Any]]],
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    iter_all_pages: Callable[..., Any],
    load_department_name_map: Callable[[Request], dict[str, str]],
    normalize_optional_datetime_input: Callable[[str], str],
    parse_bulk_exception_rules: Callable[[str], tuple[list[dict[str, Any]], list[str]]],
    parse_page_number: Callable[[str | None, int], int],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    resolve_remembered_filters: Callable[..., dict[str, Any]],
    stream_csv: Callable[..., Any],
    to_bool: Callable[[str | None, bool], bool],
) -> None:
    @app.get("/exceptions", response_class=HTMLResponse)
    def exceptions_page(request: Request):
        user = require_capability(request, "exceptions.read")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        remembered_filters = resolve_remembered_filters(
            request,
            page_name="exceptions",
            defaults={"q": "", "status": "all", "rule_type": "all"},
        )
        query = str(remembered_filters["q"])
        status = str(remembered_filters["status"] or "all").strip().lower()
        requested_rule_type = str(remembered_filters["rule_type"] or "all").strip().lower()
        normalized_rule_type = normalize_exception_rule_type(requested_rule_type)
        page_number = parse_page_number(request.query_params.get("page_number"), 1)
        rules, page_data = fetch_page(
            lambda *, limit, offset: request.app.state.exception_rule_repo.list_rule_records_page(
                limit=limit,
                offset=offset,
                query=query,
                rule_type="" if requested_rule_type == "all" else normalized_rule_type,
                status=status,
                org_id=current_org.org_id,
            ),
            page=page_number,
            page_size=25,
        )
        rule_governance_summary = build_rule_governance_summary(
            bindings=request.app.state.user_binding_repo.list_binding_records(org_id=current_org.org_id),
            overrides=request.app.state.department_override_repo.list_override_records(org_id=current_org.org_id),
            exception_rules=request.app.state.exception_rule_repo.list_rule_records(org_id=current_org.org_id),
        )
        return render(
            request,
            "exceptions.html",
            page="exceptions",
            title="Exception Rules",
            exception_rules=rules,
            exception_page_data=page_data,
            exception_query=query,
            exception_status=status,
            exception_rule_type=normalized_rule_type if normalized_rule_type else "all",
            exception_rule_definitions=EXCEPTION_RULE_DEFINITIONS,
            exception_match_type_labels=EXCEPTION_MATCH_TYPE_LABELS,
            user_exception_rule_types=[
                rule_name
                for rule_name, definition in EXCEPTION_RULE_DEFINITIONS.items()
                if definition.get("match_type") in {"source_user_id", "wecom_userid"}
            ],
            department_exception_rule_types=[
                rule_name
                for rule_name, definition in EXCEPTION_RULE_DEFINITIONS.items()
                if definition.get("match_type") == "department_id"
            ],
            group_exception_rule_types=[
                rule_name
                for rule_name, definition in EXCEPTION_RULE_DEFINITIONS.items()
                if definition.get("match_type") == "group_sam"
            ],
            department_name_map=load_department_name_map(request),
            filters_are_remembered=True,
            rule_governance_summary=rule_governance_summary,
        )

    @app.post("/exceptions")
    def exceptions_submit(
        request: Request,
        csrf_token: str = Form(""),
        rule_type: str = Form(...),
        match_value: str = Form(...),
        rule_owner: str = Form(""),
        effective_reason: str = Form(""),
        next_review_at: str = Form(""),
        notes: str = Form(""),
        expires_at: str = Form(""),
        is_once: Optional[str] = Form(None),
    ):
        user = require_capability(request, "exceptions.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/exceptions")
        if csrf_error:
            return csrf_error

        normalized_rule_type = normalize_exception_rule_type(rule_type)
        rule_definition = get_exception_rule_definition(normalized_rule_type)
        normalized_match_value = match_value.strip()
        if not rule_definition or not normalized_match_value:
            flash(request, "error", "Invalid exception rule input")
            return RedirectResponse(url="/exceptions", status_code=303)

        if rule_definition.get("match_type") == "department_id":
            department_exists, department_error = department_exists_in_source_provider(request, normalized_match_value)
            if not department_exists:
                flash(request, "error", department_error or "Invalid department id")
                return RedirectResponse(url="/exceptions", status_code=303)
        try:
            normalized_expires_at = normalize_optional_datetime_input(expires_at)
            normalized_next_review_at = normalize_optional_datetime_input(next_review_at)
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/exceptions", status_code=303)

        try:
            current_org = get_current_org(request)
            reviewed_at = utcnow_iso()
            request.app.state.exception_rule_repo.upsert_rule(
                rule_type=normalized_rule_type,
                match_value=normalized_match_value,
                org_id=current_org.org_id,
                notes=notes.strip(),
                expires_at=normalized_expires_at,
                is_once=to_bool(is_once, False),
            )
            request.app.state.exception_rule_repo.update_governance_metadata(
                rule_type=normalized_rule_type,
                match_value=normalized_match_value,
                org_id=current_org.org_id,
                rule_owner=rule_owner,
                effective_reason=effective_reason,
                next_review_at=normalized_next_review_at,
                last_reviewed_at=reviewed_at,
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/exceptions", status_code=303)
        enqueue_replay_request(
            app=request.app,
            request_type="exception_rule_changed",
            requested_by=user.username,
            org_id=current_org.org_id,
            target_scope="rule",
            target_id=f"{normalized_rule_type}:{normalized_match_value}",
            trigger_reason="exception_rule_saved",
            payload={
                "rule_type": normalized_rule_type,
                "match_value": normalized_match_value,
            },
        )

        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="exception_rule.upsert",
            target_type="sync_exception_rule",
            target_id=f"{normalized_rule_type}:{normalized_match_value}",
            result="success",
            message="Saved sync exception rule",
            payload={
                "rule_type": normalized_rule_type,
                "match_type": rule_definition.get("match_type"),
                "match_value": normalized_match_value,
                "expires_at": normalized_expires_at,
                "is_once": to_bool(is_once, False),
                "rule_owner": str(rule_owner or "").strip(),
                "next_review_at": normalized_next_review_at,
            },
        )
        flash(request, "success", "Exception rule saved")
        return RedirectResponse(url="/exceptions", status_code=303)

    @app.post("/exceptions/import")
    def exceptions_import_submit(
        request: Request,
        csrf_token: str = Form(""),
        bulk_rules: str = Form(""),
    ):
        user = require_capability(request, "exceptions.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/exceptions")
        if csrf_error:
            return csrf_error

        rows, parse_errors = parse_bulk_exception_rules(bulk_rules)
        if parse_errors:
            flash(request, "error", "; ".join(parse_errors[:5]))
            return RedirectResponse(url="/exceptions", status_code=303)
        if not rows:
            flash(request, "error", "Bulk exception rule content is empty")
            return RedirectResponse(url="/exceptions", status_code=303)

        imported_count = 0
        import_errors: list[str] = []
        current_org = get_current_org(request)
        reviewed_at = utcnow_iso()
        for row in rows:
            normalized_rule_type = normalize_exception_rule_type(row["rule_type"])
            rule_definition = get_exception_rule_definition(normalized_rule_type)
            if not rule_definition:
                import_errors.append(f"Line {row['line_number']}: unsupported rule_type {row['rule_type']}")
                continue
            if rule_definition.get("match_type") == "department_id":
                department_exists, department_error = department_exists_in_source_provider(request, str(row["match_value"]))
                if not department_exists:
                    import_errors.append(
                        f"Line {row['line_number']}: {department_error or 'invalid department id'}"
                    )
                    continue
            try:
                normalized_expires_at = normalize_optional_datetime_input(str(row["expires_at"]))
                normalized_next_review_at = normalize_optional_datetime_input(str(row.get("next_review_at") or ""))
                request.app.state.exception_rule_repo.upsert_rule(
                    rule_type=normalized_rule_type,
                    match_value=str(row["match_value"]),
                    org_id=current_org.org_id,
                    notes=str(row["notes"]),
                    is_enabled=bool(row["is_enabled"]),
                    expires_at=normalized_expires_at,
                    is_once=bool(row["is_once"]),
                )
                request.app.state.exception_rule_repo.update_governance_metadata(
                    rule_type=normalized_rule_type,
                    match_value=str(row["match_value"]),
                    org_id=current_org.org_id,
                    rule_owner=str(row.get("rule_owner") or ""),
                    effective_reason=str(row.get("effective_reason") or ""),
                    next_review_at=normalized_next_review_at,
                    last_reviewed_at=reviewed_at,
                )
            except ValueError as exc:
                import_errors.append(f"Line {row['line_number']}: {exc}")
                continue
            imported_count += 1
        if imported_count:
            enqueue_replay_request(
                app=request.app,
                request_type="exception_rule_import",
                requested_by=user.username,
                org_id=current_org.org_id,
                target_scope="bulk",
                target_id="exceptions",
                trigger_reason="exception_rules_imported",
                payload={"imported_count": imported_count},
            )

        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="exception_rule.import",
            target_type="sync_exception_rule",
            target_id="bulk",
            result="success" if not import_errors else "warning",
            message="Imported sync exception rules",
            payload={"imported_count": imported_count, "error_count": len(import_errors)},
        )
        if import_errors:
            flash(
                request,
                "error",
                f"Imported {imported_count} rows, skipped {len(import_errors)} rows: "
                f"{'; '.join(import_errors[:3])}",
            )
        else:
            flash_t(request, "success", "Imported {imported_count} exception rules", imported_count=imported_count)
        return RedirectResponse(url="/exceptions", status_code=303)

    @app.get("/exceptions/export")
    def exceptions_export(request: Request):
        user = require_capability(request, "exceptions.read")
        if isinstance(user, RedirectResponse):
            return user

        query = (request.query_params.get("q") or "").strip()
        status = (request.query_params.get("status") or "all").strip().lower()
        requested_rule_type = (request.query_params.get("rule_type") or "all").strip().lower()
        current_org = get_current_org(request)

        def iter_rows():
            for item in iter_all_pages(
                lambda *, limit, offset: request.app.state.exception_rule_repo.list_rule_records_page(
                    limit=limit,
                    offset=offset,
                    query=query,
                    rule_type="" if requested_rule_type == "all" else normalize_exception_rule_type(requested_rule_type),
                    status=status,
                    org_id=current_org.org_id,
                )
            ):
                yield [
                    item.rule_type,
                    item.match_value,
                    item.rule_owner or "",
                    item.effective_reason or "",
                    item.notes or "",
                    "true" if item.is_enabled else "false",
                    item.expires_at or "",
                    item.next_review_at or "",
                    "true" if item.is_once else "false",
                    item.last_reviewed_at or "",
                    str(item.hit_count or 0),
                    item.last_hit_at or "",
                ]

        return stream_csv(
            header=[
                "rule_type",
                "match_value",
                "rule_owner",
                "effective_reason",
                "notes",
                "is_enabled",
                "expires_at",
                "next_review_at",
                "is_once",
                "last_reviewed_at",
                "hit_count",
                "last_hit_at",
            ],
            row_iterable=iter_rows(),
            filename="exception-rules-export.csv",
        )

    @app.post("/exceptions/{rule_id}/toggle")
    def exceptions_toggle(
        request: Request,
        rule_id: int,
        csrf_token: str = Form(""),
        enabled: str = Form(...),
    ):
        user = require_capability(request, "exceptions.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/exceptions")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        rule_record = request.app.state.exception_rule_repo.get_rule_record(rule_id, org_id=current_org.org_id)
        if not rule_record:
            flash(request, "error", "Exception rule not found")
            return RedirectResponse(url="/exceptions", status_code=303)

        new_state = to_bool(enabled, rule_record.is_enabled)
        request.app.state.exception_rule_repo.set_enabled(rule_id, new_state, org_id=current_org.org_id)
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="exception_rule.toggle",
            target_type="sync_exception_rule",
            target_id=str(rule_id),
            result="success",
            message=f"{'Enabled' if new_state else 'Disabled'} sync exception rule",
            payload={
                "rule_type": rule_record.rule_type,
                "match_type": rule_record.match_type,
                "match_value": rule_record.match_value,
            },
        )
        flash(
            request,
            "success",
            "Exception rule enabled" if new_state else "Exception rule disabled",
        )
        return RedirectResponse(url="/exceptions", status_code=303)

    @app.post("/exceptions/{rule_id}/delete")
    def exceptions_delete(
        request: Request,
        rule_id: int,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "exceptions.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/exceptions")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        rule_record = request.app.state.exception_rule_repo.get_rule_record(rule_id, org_id=current_org.org_id)
        if not rule_record:
            flash(request, "error", "Exception rule not found")
            return RedirectResponse(url="/exceptions", status_code=303)

        request.app.state.exception_rule_repo.delete_rule(rule_id, org_id=current_org.org_id)
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="exception_rule.delete",
            target_type="sync_exception_rule",
            target_id=str(rule_id),
            result="success",
            message="Deleted sync exception rule",
            payload={
                "rule_type": rule_record.rule_type,
                "match_type": rule_record.match_type,
                "match_value": rule_record.match_value,
            },
        )
        flash(request, "success", "Exception rule deleted")
        return RedirectResponse(url="/exceptions", status_code=303)
