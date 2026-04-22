from __future__ import annotations

from typing import Any, Callable

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.storage.local_db import utcnow_iso
from sync_app.web.rule_governance import build_rule_governance_summary


def register_mapping_routes(
    app: FastAPI,
    *,
    department_exists_in_source_provider: Callable[[Request, str], tuple[bool, str | None]],
    fetch_page: Callable[..., tuple[list[Any], dict[str, Any]]],
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    iter_all_pages: Callable[..., Any],
    load_department_name_map: Callable[[Request], dict[str, str]],
    normalize_optional_datetime_input: Callable[[str], str],
    parse_bulk_bindings: Callable[[str], tuple[list[dict[str, str]], list[str]]],
    parse_page_number: Callable[[str | None, int], int],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    resolve_remembered_filters: Callable[..., dict[str, Any]],
    source_user_exists_in_source_provider: Callable[[Request, str], tuple[bool, str | None]],
    source_user_has_department: Callable[[Request, str, str], tuple[bool, str | None]],
    stream_csv: Callable[..., Any],
    to_bool: Callable[[str | None, bool], bool],
    validate_binding_target: Callable[[Request, str, str], str | None],
) -> None:
    @app.get("/mappings", response_class=HTMLResponse)
    def mappings_page(request: Request):
        user = require_capability(request, "mappings.read")
        if isinstance(user, RedirectResponse):
            return user

        current_org = get_current_org(request)
        remembered_filters = resolve_remembered_filters(
            request,
            page_name="mappings",
            defaults={"q": "", "status": "all"},
        )
        query = str(remembered_filters["q"])
        status = str(remembered_filters["status"] or "all").strip().lower()
        binding_page = parse_page_number(request.query_params.get("binding_page"), 1)
        override_page = parse_page_number(request.query_params.get("override_page"), 1)
        bindings, binding_page_data = fetch_page(
            lambda *, limit, offset: request.app.state.user_binding_repo.list_binding_records_page(
                limit=limit,
                offset=offset,
                query=query,
                status=status,
                org_id=current_org.org_id,
            ),
            page=binding_page,
            page_size=20,
        )
        overrides, override_page_data = fetch_page(
            lambda *, limit, offset: request.app.state.department_override_repo.list_override_records_page(
                limit=limit,
                offset=offset,
                query=query,
                org_id=current_org.org_id,
            ),
            page=override_page,
            page_size=20,
        )
        rule_governance_summary = build_rule_governance_summary(
            bindings=request.app.state.user_binding_repo.list_binding_records(org_id=current_org.org_id),
            overrides=request.app.state.department_override_repo.list_override_records(org_id=current_org.org_id),
            exception_rules=request.app.state.exception_rule_repo.list_rule_records(org_id=current_org.org_id),
        )
        return render(
            request,
            "mappings.html",
            page="mappings",
            title="Identity Overrides",
            bindings=bindings,
            overrides=overrides,
            mapping_query=query,
            mapping_status=status,
            binding_page_data=binding_page_data,
            override_page_data=override_page_data,
            department_name_map=load_department_name_map(request),
            filters_are_remembered=True,
            rule_governance_summary=rule_governance_summary,
        )

    @app.get("/mappings/export")
    def mappings_export(request: Request):
        user = require_capability(request, "mappings.read")
        if isinstance(user, RedirectResponse):
            return user

        query = (request.query_params.get("q") or "").strip()
        status = (request.query_params.get("status") or "all").strip().lower()
        current_org = get_current_org(request)

        def iter_rows():
            for item in iter_all_pages(
                lambda *, limit, offset: request.app.state.user_binding_repo.list_binding_records_page(
                    limit=limit,
                    offset=offset,
                    query=query,
                    status=status,
                    org_id=current_org.org_id,
                )
            ):
                yield [
                    "binding",
                    item.source_user_id,
                    item.ad_username,
                    "",
                    "true" if item.is_enabled else "false",
                    item.source,
                    item.rule_owner or "",
                    item.effective_reason or "",
                    item.next_review_at or "",
                    item.notes,
                    item.updated_at,
                    item.last_reviewed_at or "",
                    str(item.hit_count or 0),
                    item.last_hit_at or "",
                ]
            for item in iter_all_pages(
                lambda *, limit, offset: request.app.state.department_override_repo.list_override_records_page(
                    limit=limit,
                    offset=offset,
                    query=query,
                    org_id=current_org.org_id,
                )
            ):
                yield [
                    "override",
                    item.source_user_id,
                    "",
                    item.primary_department_id,
                    "",
                    "",
                    item.rule_owner or "",
                    item.effective_reason or "",
                    item.next_review_at or "",
                    item.notes,
                    item.updated_at,
                    item.last_reviewed_at or "",
                    str(item.hit_count or 0),
                    item.last_hit_at or "",
                ]

        return stream_csv(
            header=[
                "record_type",
                "source_user_id",
                "ad_username",
                "primary_department_id",
                "is_enabled",
                "source",
                "rule_owner",
                "effective_reason",
                "next_review_at",
                "notes",
                "updated_at",
                "last_reviewed_at",
                "hit_count",
                "last_hit_at",
            ],
            row_iterable=iter_rows(),
            filename="mappings-export.csv",
        )

    @app.post("/mappings/bind")
    def mappings_bind_submit(
        request: Request,
        csrf_token: str = Form(""),
        source_user_id: str = Form(""),
        legacy_source_user_id: str = Form("", alias="wecom_userid"),
        ad_username: str = Form(...),
        rule_owner: str = Form(""),
        effective_reason: str = Form(""),
        next_review_at: str = Form(""),
        notes: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/mappings")
        if csrf_error:
            return csrf_error

        source_user_id = (source_user_id or "").strip() or (legacy_source_user_id or "").strip()
        ad_username = ad_username.strip()
        if not source_user_id or not ad_username:
            flash(request, "error", "Source user ID and AD username are required")
            return RedirectResponse(url="/mappings", status_code=303)

        source_exists, source_error = source_user_exists_in_source_provider(request, source_user_id)
        if not source_exists:
            flash(request, "error", source_error or "Source user validation failed")
            return RedirectResponse(url="/mappings", status_code=303)

        conflict_message = validate_binding_target(request, source_user_id, ad_username)
        if conflict_message:
            flash(request, "error", conflict_message)
            return RedirectResponse(url="/mappings", status_code=303)

        try:
            normalized_next_review_at = normalize_optional_datetime_input(next_review_at)
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/mappings", status_code=303)

        current_org = get_current_org(request)
        reviewed_at = utcnow_iso()
        request.app.state.user_binding_repo.upsert_binding_for_source_user(
            source_user_id,
            ad_username,
            org_id=current_org.org_id,
            source="manual",
            notes=notes.strip(),
            preserve_manual=False,
        )
        request.app.state.user_binding_repo.update_governance_metadata_for_source_user(
            source_user_id,
            org_id=current_org.org_id,
            rule_owner=rule_owner,
            effective_reason=effective_reason,
            next_review_at=normalized_next_review_at,
            last_reviewed_at=reviewed_at,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="mapping.bind_upsert",
            target_type="user_identity_binding",
            target_id=source_user_id,
            result="success",
            message="Saved source to AD identity binding",
            payload={
                "source_user_id": source_user_id,
                "ad_username": ad_username,
                "rule_owner": str(rule_owner or "").strip(),
                "next_review_at": normalized_next_review_at,
            },
        )
        flash(request, "success", "Identity binding saved")
        return RedirectResponse(url="/mappings", status_code=303)

    @app.post("/mappings/import")
    def mappings_import_submit(
        request: Request,
        csrf_token: str = Form(""),
        bulk_bindings: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/mappings")
        if csrf_error:
            return csrf_error

        rows, parse_errors = parse_bulk_bindings(bulk_bindings)
        if parse_errors:
            flash(request, "error", "; ".join(parse_errors[:5]))
            return RedirectResponse(url="/mappings", status_code=303)
        if not rows:
            flash(request, "error", "Bulk import content is empty")
            return RedirectResponse(url="/mappings", status_code=303)

        imported_count = 0
        conflicts: list[str] = []
        current_org = get_current_org(request)
        reviewed_at = utcnow_iso()
        for row in rows:
            conflict_message = validate_binding_target(request, row["source_user_id"], row["ad_username"])
            if conflict_message:
                conflicts.append(conflict_message)
                continue
            try:
                normalized_next_review_at = normalize_optional_datetime_input(str(row.get("next_review_at") or ""))
            except ValueError as exc:
                conflicts.append(f"{row['source_user_id']}: {exc}")
                continue
            request.app.state.user_binding_repo.upsert_binding_for_source_user(
                row["source_user_id"],
                row["ad_username"],
                org_id=current_org.org_id,
                source="manual",
                notes=row["notes"],
                preserve_manual=False,
            )
            request.app.state.user_binding_repo.update_governance_metadata_for_source_user(
                row["source_user_id"],
                org_id=current_org.org_id,
                rule_owner=row.get("rule_owner"),
                effective_reason=row.get("effective_reason"),
                next_review_at=normalized_next_review_at,
                last_reviewed_at=reviewed_at,
            )
            imported_count += 1

        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="mapping.bind_import",
            target_type="user_identity_binding",
            target_id="bulk",
            result="success" if not conflicts else "warning",
            message="Imported identity bindings in bulk",
            payload={"imported_count": imported_count, "conflict_count": len(conflicts)},
        )
        if conflicts:
            flash(
                request,
                "error",
                f"Imported {imported_count} rows, skipped {len(conflicts)} conflict rows: "
                f"{'; '.join(conflicts[:3])}",
            )
        else:
            flash_t(request, "success", "Imported {imported_count} identity bindings", imported_count=imported_count)
        return RedirectResponse(url="/mappings", status_code=303)

    @app.post("/mappings/bind/{source_user_id}/toggle")
    def mappings_toggle_binding(
        request: Request,
        source_user_id: str,
        csrf_token: str = Form(""),
        enabled: str = Form(...),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/mappings")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        binding = request.app.state.user_binding_repo.get_binding_record_by_source_user_id(
            source_user_id,
            org_id=current_org.org_id,
        )
        if not binding:
            flash_t(request, "error", "Binding not found: {source_user_id}", source_user_id=source_user_id)
            return RedirectResponse(url="/mappings", status_code=303)

        new_state = to_bool(enabled, binding.is_enabled)
        request.app.state.user_binding_repo.set_enabled_for_source_user(
            source_user_id,
            new_state,
            org_id=current_org.org_id,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="mapping.bind_toggle",
            target_type="user_identity_binding",
            target_id=source_user_id,
            result="success",
            message=f"{'Enabled' if new_state else 'Disabled'} identity binding",
            payload={"source_user_id": source_user_id, "ad_username": binding.ad_username},
        )
        flash_t(
            request,
            "success",
            "Binding {source_user_id} enabled" if new_state else "Binding {source_user_id} disabled",
            source_user_id=source_user_id,
        )
        return RedirectResponse(url="/mappings", status_code=303)

    @app.post("/mappings/override")
    def mappings_override_submit(
        request: Request,
        csrf_token: str = Form(""),
        source_user_id: str = Form(""),
        legacy_source_user_id: str = Form("", alias="wecom_userid"),
        primary_department_id: str = Form(...),
        rule_owner: str = Form(""),
        effective_reason: str = Form(""),
        next_review_at: str = Form(""),
        notes: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/mappings")
        if csrf_error:
            return csrf_error

        source_user_id = (source_user_id or "").strip() or (legacy_source_user_id or "").strip()
        primary_department_id = primary_department_id.strip()
        if not source_user_id or not primary_department_id:
            flash(request, "error", "Source user ID and primary department ID are required")
            return RedirectResponse(url="/mappings", status_code=303)

        source_exists, source_error = source_user_exists_in_source_provider(request, source_user_id)
        if not source_exists:
            flash(request, "error", source_error or "Source user validation failed")
            return RedirectResponse(url="/mappings", status_code=303)

        department_exists, department_error = department_exists_in_source_provider(request, primary_department_id)
        if not department_exists:
            flash(request, "error", department_error or "Primary department validation failed")
            return RedirectResponse(url="/mappings", status_code=303)

        department_belongs_to_user, override_error = source_user_has_department(
            request,
            source_user_id,
            primary_department_id,
        )
        if not department_belongs_to_user:
            flash(request, "error", override_error or "Selected department does not belong to the source user")
            return RedirectResponse(url="/mappings", status_code=303)

        try:
            normalized_next_review_at = normalize_optional_datetime_input(next_review_at)
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/mappings", status_code=303)

        current_org = get_current_org(request)
        reviewed_at = utcnow_iso()
        request.app.state.department_override_repo.upsert_override_for_source_user(
            source_user_id,
            primary_department_id,
            org_id=current_org.org_id,
            notes=notes.strip(),
        )
        request.app.state.department_override_repo.update_governance_metadata_for_source_user(
            source_user_id,
            org_id=current_org.org_id,
            rule_owner=rule_owner,
            effective_reason=effective_reason,
            next_review_at=normalized_next_review_at,
            last_reviewed_at=reviewed_at,
        )
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="mapping.department_override_upsert",
            target_type="user_department_override",
            target_id=source_user_id,
            result="success",
            message="Saved primary department override",
            payload={
                "source_user_id": source_user_id,
                "primary_department_id": primary_department_id,
                "rule_owner": str(rule_owner or "").strip(),
                "next_review_at": normalized_next_review_at,
            },
        )
        flash(request, "success", "Primary department override saved")
        return RedirectResponse(url="/mappings", status_code=303)

    @app.post("/mappings/override/{source_user_id}/delete")
    def mappings_override_delete(
        request: Request,
        source_user_id: str,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "mappings.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/mappings")
        if csrf_error:
            return csrf_error

        request.app.state.department_override_repo.delete_override_for_source_user(
            source_user_id,
            org_id=get_current_org(request).org_id,
        )
        request.app.state.audit_repo.add_log(
            org_id=get_current_org(request).org_id,
            actor_username=user.username,
            action_type="mapping.department_override_delete",
            target_type="user_department_override",
            target_id=source_user_id,
            result="success",
            message="Deleted primary department override",
        )
        flash_t(
            request,
            "success",
            "Deleted primary department override for {source_user_id}",
            source_user_id=source_user_id,
        )
        return RedirectResponse(url="/mappings", status_code=303)
