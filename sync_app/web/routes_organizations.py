from __future__ import annotations

import json
from typing import Any, Callable, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from sync_app.storage.local_db import ManagedGroupBindingRepository, ObjectStateRepository
from sync_app.web.app_state import get_web_repositories


def register_organization_routes(
    app: FastAPI,
    *,
    export_organization_bundle: Callable[..., dict[str, Any]],
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    import_organization_bundle: Callable[..., dict[str, Any]],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    require_user: Callable[[Request], Any],
    safe_redirect_target: Callable[[str | None, str], str],
    to_bool: Callable[[Optional[str], bool], bool],
) -> None:
    @app.get("/organizations", response_class=HTMLResponse)
    def organizations_page(request: Request):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        return render(
            request,
            "organizations.html",
            page="organizations",
            title="Organizations",
        )

    @app.post("/organization-switch")
    def organization_switch(
        request: Request,
        csrf_token: str = Form(""),
        org_id: str = Form("default"),
        return_url: str = Form("/dashboard"),
    ):
        user = require_user(request)
        if isinstance(user, RedirectResponse):
            return user
        repositories = get_web_repositories(request)
        csrf_error = reject_invalid_csrf(request, csrf_token, safe_redirect_target(return_url, "/dashboard"))
        if csrf_error:
            return csrf_error
        organization = repositories.organization_repo.get_organization_record(org_id)
        if not organization or not organization.is_enabled:
            flash(request, "error", "Organization not found or disabled")
            return RedirectResponse(url="/dashboard", status_code=303)
        request.session["selected_org_id"] = organization.org_id
        flash_t(request, "success", "Switched to organization {name}", name=organization.name)
        return RedirectResponse(url=safe_redirect_target(return_url, "/dashboard"), status_code=303)

    @app.post("/organizations")
    def organization_submit(
        request: Request,
        csrf_token: str = Form(""),
        org_id: str = Form(""),
        name: str = Form(""),
        config_path_value: str = Form("", alias="config_path"),
        description: str = Form(""),
        is_enabled: Optional[str] = Form(None),
    ):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/organizations")
        if csrf_error:
            return csrf_error
        normalized_org_id = str(org_id or "").strip()
        if not normalized_org_id:
            flash(request, "error", "Organization ID is required")
            return RedirectResponse(url="/organizations", status_code=303)
        repositories = get_web_repositories(request)
        try:
            repositories.organization_repo.upsert_organization(
                org_id=normalized_org_id,
                name=name,
                config_path=config_path_value,
                description=description,
                is_enabled=to_bool(is_enabled, True),
            )
        except Exception as exc:
            flash_t(request, "error", "Failed to save organization: {error}", error=str(exc))
            return RedirectResponse(url="/organizations", status_code=303)
        repositories.org_config_repo.ensure_loaded(normalized_org_id, config_path=config_path_value)
        organization = repositories.organization_repo.get_organization_record(normalized_org_id)
        repositories.audit_repo.add_log(
            actor_username=user.username,
            action_type="organization.upsert",
            target_type="organization",
            target_id=normalized_org_id.lower(),
            result="success",
            message="Saved organization definition",
            payload=organization.to_dict() if organization else {"org_id": org_id},
        )
        flash_t(request, "success", "Organization {org_id} saved", org_id=normalized_org_id.lower())
        return RedirectResponse(url="/organizations", status_code=303)

    @app.post("/organizations/{org_id}/select")
    def organization_select(
        request: Request,
        org_id: str,
        csrf_token: str = Form(""),
        return_url: str = Form("/dashboard"),
    ):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        repositories = get_web_repositories(request)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/organizations")
        if csrf_error:
            return csrf_error
        organization = repositories.organization_repo.get_organization_record(org_id)
        if not organization or not organization.is_enabled:
            flash(request, "error", "Organization not found or disabled")
            return RedirectResponse(url="/organizations", status_code=303)
        request.session["selected_org_id"] = organization.org_id
        flash_t(request, "success", "Switched to organization {name}", name=organization.name)
        return RedirectResponse(url=safe_redirect_target(return_url, "/dashboard"), status_code=303)

    @app.get("/organizations/{org_id}/export")
    def organization_export(request: Request, org_id: str):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        repositories = get_web_repositories(request)
        organization = repositories.organization_repo.get_organization_record(org_id)
        if not organization:
            flash(request, "error", "Organization not found")
            return RedirectResponse(url="/organizations", status_code=303)
        try:
            bundle = export_organization_bundle(repositories.db_manager, organization.org_id)
        except Exception as exc:
            flash_t(request, "error", "Failed to export organization bundle: {error}", error=str(exc))
            return RedirectResponse(url="/organizations", status_code=303)
        repositories.audit_repo.add_log(
            org_id=organization.org_id,
            actor_username=user.username,
            action_type="organization.bundle_export",
            target_type="organization",
            target_id=organization.org_id,
            result="success",
            message="Exported configuration bundle",
            payload={"organization_name": organization.name},
        )
        filename = f"{organization.org_id}-config-bundle.json"
        return Response(
            content=json.dumps(bundle, ensure_ascii=False, indent=2).encode("utf-8"),
            media_type="application/json; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.post("/organizations/import")
    def organization_import(
        request: Request,
        csrf_token: str = Form(""),
        bundle_json: str = Form(""),
        target_org_id: str = Form(""),
        replace_existing: Optional[str] = Form(None),
    ):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/organizations")
        if csrf_error:
            return csrf_error
        bundle_payload = str(bundle_json or "").strip()
        if not bundle_payload:
            flash(request, "error", "Configuration bundle content is required")
            return RedirectResponse(url="/organizations", status_code=303)
        repositories = get_web_repositories(request)
        try:
            bundle = json.loads(bundle_payload)
        except json.JSONDecodeError as exc:
            flash_t(request, "error", "Invalid configuration bundle JSON: {error}", error=str(exc))
            return RedirectResponse(url="/organizations", status_code=303)
        try:
            summary = import_organization_bundle(
                repositories.db_manager,
                bundle,
                target_org_id=str(target_org_id or "").strip() or None,
                replace_existing=to_bool(replace_existing, False),
            )
        except Exception as exc:
            flash_t(request, "error", "Failed to import organization bundle: {error}", error=str(exc))
            return RedirectResponse(url="/organizations", status_code=303)
        repositories.audit_repo.add_log(
            org_id=summary["org_id"],
            actor_username=user.username,
            action_type="organization.bundle_import",
            target_type="organization",
            target_id=summary["org_id"],
            result="success",
            message="Imported configuration bundle",
            payload=summary,
        )
        flash_t(
            request,
            "success",
            "Imported configuration bundle into {org_id} ({connectors} connectors, {mappings} mappings, {rules} group rules)",
            org_id=summary["org_id"],
            connectors=summary["imported_connectors"],
            mappings=summary["imported_mappings"],
            rules=summary["imported_group_rules"],
        )
        return RedirectResponse(url="/organizations", status_code=303)

    @app.post("/organizations/{org_id}/toggle")
    def organization_toggle(
        request: Request,
        org_id: str,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        repositories = get_web_repositories(request)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/organizations")
        if csrf_error:
            return csrf_error
        organization = repositories.organization_repo.get_organization_record(org_id)
        if not organization:
            flash(request, "error", "Organization not found")
            return RedirectResponse(url="/organizations", status_code=303)
        try:
            repositories.organization_repo.set_enabled(org_id, not organization.is_enabled)
        except Exception as exc:
            flash_t(request, "error", "Failed to update organization: {error}", error=str(exc))
            return RedirectResponse(url="/organizations", status_code=303)
        if request.session.get("selected_org_id") == organization.org_id and organization.is_enabled:
            request.session["selected_org_id"] = "default"
        flash_t(
            request,
            "success",
            "Organization {name} enabled" if not organization.is_enabled else "Organization {name} disabled",
            name=organization.name,
        )
        return RedirectResponse(url="/organizations", status_code=303)

    @app.post("/organizations/{org_id}/delete")
    def organization_delete(
        request: Request,
        org_id: str,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "organizations.manage")
        if isinstance(user, RedirectResponse):
            return user
        repositories = get_web_repositories(request)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/organizations")
        if csrf_error:
            return csrf_error
        organization = repositories.organization_repo.get_organization_record(org_id)
        if not organization:
            flash(request, "error", "Organization not found")
            return RedirectResponse(url="/organizations", status_code=303)
        if repositories.job_repo.count_jobs(org_id=organization.org_id):
            flash(request, "error", "Organization has job history and cannot be deleted")
            return RedirectResponse(url="/organizations", status_code=303)
        repositories.connector_repo.delete_connectors_for_org(organization.org_id)
        repositories.exclusion_repo.delete_rules_for_org(organization.org_id)
        ManagedGroupBindingRepository(repositories.db_manager).delete_bindings_for_org(organization.org_id)
        ObjectStateRepository(repositories.db_manager).delete_states_for_org(organization.org_id)
        repositories.attribute_mapping_repo.delete_rules_for_org(organization.org_id)
        repositories.user_binding_repo.delete_bindings_for_org(organization.org_id)
        repositories.department_override_repo.delete_overrides_for_org(organization.org_id)
        repositories.exception_rule_repo.delete_rules_for_org(organization.org_id)
        repositories.offboarding_repo.delete_records_for_org(organization.org_id)
        repositories.lifecycle_repo.delete_records_for_org(organization.org_id)
        repositories.custom_group_binding_repo.delete_bindings_for_org(organization.org_id)
        repositories.replay_request_repo.delete_requests_for_org(organization.org_id)
        repositories.audit_repo.delete_logs_for_org(organization.org_id)
        repositories.org_config_repo.delete_config(organization.org_id)
        repositories.settings_repo.delete_org_scoped_values(organization.org_id)
        try:
            repositories.organization_repo.delete_organization(organization.org_id)
        except Exception as exc:
            flash_t(request, "error", "Failed to delete organization: {error}", error=str(exc))
            return RedirectResponse(url="/organizations", status_code=303)
        if request.session.get("selected_org_id") == organization.org_id:
            request.session["selected_org_id"] = "default"
        repositories.audit_repo.add_log(
            actor_username=user.username,
            action_type="organization.delete",
            target_type="organization",
            target_id=organization.org_id,
            result="success",
            message="Deleted organization definition",
            payload={"name": organization.name},
        )
        flash_t(request, "success", "Organization {name} deleted", name=organization.name)
        return RedirectResponse(url="/organizations", status_code=303)
