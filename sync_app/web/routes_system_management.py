from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sync_app.services.typed_settings import BrandingSettings, SSPRSettings, WebRuntimeSettings
from sync_app.web.app_state import get_web_repositories, get_web_runtime_state
from sync_app.web.navigation import CANONICAL_ROUTE_PATHS
from sync_app.web.runtime import resolve_web_runtime_settings, web_runtime_requires_restart


def register_system_management_routes(
    app: FastAPI,
    *,
    default_brand_display_name: str,
    default_brand_mark_text: str,
    default_brand_attribution: str,
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    to_bool: Callable[[Optional[str], bool], bool],
) -> None:
    def _branding_settings(request: Request) -> BrandingSettings:
        return BrandingSettings.load(
            get_web_repositories(request).settings_repo,
            default_display_name=default_brand_display_name,
            default_mark_text=default_brand_mark_text,
            default_attribution=default_brand_attribution,
        )

    @app.get(CANONICAL_ROUTE_PATHS["employee-self-service"], response_class=HTMLResponse)
    def employee_self_service_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        settings = SSPRSettings.load(repositories.settings_repo, org_id=current_org.org_id)
        runtime_state = get_web_runtime_state(request)
        persisted_web_settings = resolve_web_runtime_settings(repositories.settings_repo)
        editable_connector = repositories.org_config_repo.get_editable_config(
            current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        callback_path = "/sspr/callback/dingtalk"
        public_base_url = str(persisted_web_settings.get("public_base_url") or "").rstrip("/")
        enabled_binding_count = sum(
            1
            for record in repositories.user_binding_repo.list_binding_records(org_id=current_org.org_id)
            if bool(getattr(record, "is_enabled", False))
        )
        return render(
            request,
            "employee_self_service.html",
            page="employee-self-service",
            title="Employee Self-Service",
            current_org=current_org,
            settings=settings.to_dict(),
            enabled_binding_count=enabled_binding_count,
            callback_path=callback_path,
            callback_url=(
                f"{public_base_url}{callback_path}" if public_base_url else callback_path
            ),
            callback_public_base_configured=bool(public_base_url),
            source_provider=editable_connector.get("source_provider", ""),
            source_credentials_configured=bool(
                editable_connector.get("corpsecret_configured")
            ),
        )

    @app.post(CANONICAL_ROUTE_PATHS["employee-self-service"])
    def employee_self_service_save(
        request: Request,
        csrf_token: str = Form(""),
        sspr_enabled: Optional[str] = Form(None),
        sspr_dingtalk_corp_id: str = Form(""),
        sspr_min_password_length: int = Form(12),
        sspr_unlock_account_default: Optional[str] = Form(None),
        sspr_verification_session_ttl_seconds: int = Form(600),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        return_path = CANONICAL_ROUTE_PATHS["employee-self-service"]
        csrf_error = reject_invalid_csrf(request, csrf_token, return_path)
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        settings = SSPRSettings.from_mapping(
            {
                "sspr_enabled": to_bool(sspr_enabled, False),
                "sspr_dingtalk_corp_id": sspr_dingtalk_corp_id,
                "sspr_min_password_length": sspr_min_password_length,
                "sspr_unlock_account_default": to_bool(sspr_unlock_account_default, False),
                "sspr_verification_session_ttl_seconds": sspr_verification_session_ttl_seconds,
            }
        )
        settings.persist(repositories.settings_repo, org_id=current_org.org_id)
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="system_management.employee_self_service.update",
            target_type="settings",
            target_id="employee_self_service",
            result="success",
            message="Updated employee self-service settings",
            payload={
                "org_id": current_org.org_id,
                "enabled": settings.enabled,
                "min_password_length": settings.min_password_length,
                "unlock_account_default": settings.unlock_account_default,
                "verification_session_ttl_seconds": settings.verification_session_ttl_seconds,
                "dingtalk_corp_id_configured": bool(settings.dingtalk_corp_id),
            },
        )
        flash_t(request, "success", "Employee self-service settings saved.")
        return RedirectResponse(url=return_path, status_code=303)

    @app.get(CANONICAL_ROUTE_PATHS["branding"], response_class=HTMLResponse)
    def branding_page(request: Request):
        user = require_capability(request, "system.manage")
        if isinstance(user, RedirectResponse):
            return user
        return render(
            request,
            "branding.html",
            page="branding",
            title="Branding And Appearance",
            current_org=get_current_org(request),
            settings=_branding_settings(request).to_dict(),
        )

    @app.post(CANONICAL_ROUTE_PATHS["branding"])
    def branding_save(
        request: Request,
        csrf_token: str = Form(""),
        brand_display_name: str = Form(""),
        brand_mark_text: str = Form(""),
        brand_attribution: str = Form(""),
    ):
        user = require_capability(request, "system.manage")
        if isinstance(user, RedirectResponse):
            return user
        return_path = CANONICAL_ROUTE_PATHS["branding"]
        csrf_error = reject_invalid_csrf(request, csrf_token, return_path)
        if csrf_error:
            return csrf_error
        repositories = get_web_repositories(request)
        settings = BrandingSettings.from_mapping(
            {
                "brand_display_name": brand_display_name,
                "brand_mark_text": brand_mark_text,
                "brand_attribution": brand_attribution,
            },
            default_display_name=default_brand_display_name,
            default_mark_text=default_brand_mark_text,
            default_attribution=default_brand_attribution,
        )
        settings.persist(repositories.settings_repo)
        repositories.audit_repo.add_log(
            actor_username=user.username,
            action_type="system_management.branding.update",
            target_type="settings",
            target_id="branding",
            result="success",
            message="Updated global branding settings",
            payload=settings.to_dict(),
        )
        flash_t(request, "success", "Branding and appearance settings saved.")
        return RedirectResponse(url=return_path, status_code=303)

    @app.get(CANONICAL_ROUTE_PATHS["deployment"], response_class=HTMLResponse)
    def deployment_page(request: Request):
        user = require_capability(request, "system.manage")
        if isinstance(user, RedirectResponse):
            return user
        repositories = get_web_repositories(request)
        runtime = get_web_runtime_state(request)
        persisted = resolve_web_runtime_settings(repositories.settings_repo)
        active = dict(runtime.web_runtime_settings)
        return render(
            request,
            "deployment_settings.html",
            page="deployment",
            title="Deployment Settings",
            current_org=get_current_org(request),
            settings=persisted,
            active_settings=active,
            restart_required=web_runtime_requires_restart(active, persisted),
        )

    @app.post(CANONICAL_ROUTE_PATHS["deployment"])
    def deployment_save(
        request: Request,
        csrf_token: str = Form(""),
        web_bind_host: str = Form("127.0.0.1"),
        web_bind_port: int = Form(8000),
        web_public_base_url: str = Form(""),
        web_session_cookie_secure_mode: str = Form("auto"),
        web_trust_proxy_headers: Optional[str] = Form(None),
        web_forwarded_allow_ips: str = Form("127.0.0.1"),
    ):
        user = require_capability(request, "system.manage")
        if isinstance(user, RedirectResponse):
            return user
        return_path = CANONICAL_ROUTE_PATHS["deployment"]
        csrf_error = reject_invalid_csrf(request, csrf_token, return_path)
        if csrf_error:
            return csrf_error
        repositories = get_web_repositories(request)
        settings = WebRuntimeSettings.from_mapping(
            {
                "web_bind_host": web_bind_host,
                "web_bind_port": web_bind_port,
                "web_public_base_url": web_public_base_url,
                "web_session_cookie_secure_mode": web_session_cookie_secure_mode,
                "web_trust_proxy_headers": to_bool(web_trust_proxy_headers, False),
                "web_forwarded_allow_ips": web_forwarded_allow_ips,
            }
        )
        settings.persist(repositories.settings_repo)
        persisted = resolve_web_runtime_settings(repositories.settings_repo)
        restart_required = web_runtime_requires_restart(
            get_web_runtime_state(request).web_runtime_settings,
            persisted,
        )
        repositories.audit_repo.add_log(
            actor_username=user.username,
            action_type="system_management.deployment.update",
            target_type="settings",
            target_id="web_runtime",
            result="success",
            message="Updated global web deployment settings",
            payload={**settings.to_dict(), "restart_required": restart_required},
        )
        flash_t(
            request,
            "warning" if restart_required else "success",
            "Deployment settings saved. Restart the service to activate the changes."
            if restart_required
            else "Deployment settings saved.",
        )
        return RedirectResponse(url=return_path, status_code=303)
