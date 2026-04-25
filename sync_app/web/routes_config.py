from __future__ import annotations

import secrets
from typing import Any, Callable, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from sync_app.web.app_state import get_web_runtime_state, get_web_services

CONFIG_SUBMISSION_FIELD_NAMES = (
    "source_provider",
    "corpid",
    "agentid",
    "corpsecret",
    "webhook_url",
    "ldap_server",
    "ldap_domain",
    "ldap_username",
    "ldap_password",
    "ldap_port",
    "ldap_use_ssl",
    "ldap_validate_cert",
    "ldap_ca_cert_path",
    "default_password",
    "force_change_password",
    "password_complexity",
    "schedule_time",
    "retry_interval",
    "max_retries",
    "group_display_separator",
    "group_recursive_enabled",
    "managed_relation_cleanup_enabled",
    "schedule_execution_mode",
    "web_bind_host",
    "web_bind_port",
    "web_public_base_url",
    "web_session_cookie_secure_mode",
    "web_trust_proxy_headers",
    "web_forwarded_allow_ips",
    "sspr_enabled",
    "sspr_min_password_length",
    "sspr_unlock_account_default",
    "sspr_verification_session_ttl_seconds",
    "brand_display_name",
    "brand_mark_text",
    "brand_attribution",
    "user_ou_placement_strategy",
    "source_root_unit_ids",
    "source_root_unit_display_text",
    "directory_root_ou_path",
    "disabled_users_ou_path",
    "custom_group_ou_path",
    "soft_excluded_groups",
)


def _collect_config_submission_values(values: dict[str, Any]) -> dict[str, Any]:
    return {
        field_name: values[field_name]
        for field_name in CONFIG_SUBMISSION_FIELD_NAMES
        if field_name in values
    }


def _build_config_submission_from_values(
    request: Request,
    *,
    build_config_submission: Callable[..., dict[str, Any]],
    values: dict[str, Any],
) -> dict[str, Any]:
    return build_config_submission(
        request,
        **_collect_config_submission_values(values),
    )


def _config_saved_message(
    request: Request,
) -> str:
    runtime_state = get_web_runtime_state(request)
    return get_web_services(request).config.build_saved_message(
        current_web_runtime_settings=runtime_state.web_runtime_settings,
    )


def _parse_optional_int(value: str | None) -> Optional[int]:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        return int(normalized)
    except (TypeError, ValueError):
        return None


def register_config_routes(
    app: FastAPI,
    *,
    apply_config_submission: Callable[..., None],
    build_config_change_preview: Callable[..., dict[str, Any]],
    build_config_editable_override: Callable[..., dict[str, Any]],
    build_config_page_context: Callable[..., dict[str, Any]],
    build_source_unit_catalog: Callable[..., dict[str, Any]],
    build_target_ou_catalog: Callable[..., dict[str, Any]],
    build_config_submission: Callable[..., dict[str, Any]],
    config_preview_session_key: str,
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
) -> None:
    @app.get("/config", response_class=HTMLResponse)
    def config_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        request.session.pop(config_preview_session_key, None)
        return render(
            request,
            "config.html",
            **build_config_page_context(request),
        )

    @app.get("/config/releases", response_class=HTMLResponse)
    def config_release_center_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        return render(
            request,
            "config_release_center.html",
            **get_web_services(request).config.build_release_center_context(
                current_org=get_current_org(request),
                current_snapshot_id=_parse_optional_int(request.query_params.get("current_snapshot_id")),
                baseline_snapshot_id=_parse_optional_int(request.query_params.get("baseline_snapshot_id")),
            ),
        )

    @app.post("/config/releases/publish")
    def config_release_publish(
        request: Request,
        csrf_token: str = Form(""),
        snapshot_name: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/config/releases")
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        result = get_web_services(request).config.publish_release_snapshot(
            org_id=current_org.org_id,
            actor_username=user.username,
            snapshot_name=snapshot_name,
        )
        snapshot = result.get("snapshot")
        if not result.get("created"):
            flash(
                request,
                "warning",
                "Current configuration already matches the latest published snapshot.",
            )
            return RedirectResponse(url="/config/releases", status_code=303)
        flash_t(
            request,
            "success",
            "Published configuration snapshot {snapshot_id}",
            snapshot_id=str(getattr(snapshot, "id", "") or ""),
        )
        return RedirectResponse(url="/config/releases", status_code=303)

    @app.post("/config/releases/{snapshot_id}/rollback")
    def config_release_rollback(
        request: Request,
        snapshot_id: int,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/config/releases")
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        try:
            get_web_services(request).config.rollback_release_snapshot(
                org_id=current_org.org_id,
                actor_username=user.username,
                snapshot_id=snapshot_id,
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/config/releases", status_code=303)
        flash_t(
            request,
            "success",
            "Rolled back to configuration snapshot {snapshot_id}",
            snapshot_id=str(snapshot_id),
        )
        return RedirectResponse(url="/config/releases", status_code=303)

    @app.get("/config/releases/{snapshot_id}/download")
    def config_release_download(request: Request, snapshot_id: int):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        download = get_web_services(request).config.build_release_download(
            org_id=current_org.org_id,
            snapshot_id=snapshot_id,
        )
        if download is None:
            flash(request, "error", "Configuration snapshot not found")
            return RedirectResponse(url="/config/releases", status_code=303)
        return Response(
            content=download["content"],
            media_type=download["media_type"],
            headers={"Content-Disposition": f'attachment; filename="{download["filename"]}"'},
        )

    @app.post("/config/source-units/catalog")
    def config_source_unit_catalog(
        request: Request,
        csrf_token: str = Form(""),
        source_provider: str = Form("wecom"),
        corpid: str = Form(""),
        agentid: str = Form(""),
        corpsecret: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/config")
        if csrf_error:
            return JSONResponse(
                {"ok": False, "error": "The configuration session expired. Refresh the page and try again."},
                status_code=400,
            )
        return JSONResponse(
            build_source_unit_catalog(
                request,
                source_provider=source_provider,
                corpid=corpid,
                agentid=agentid,
                corpsecret=corpsecret,
            )
        )

    @app.post("/config/target-ou/catalog")
    def config_target_ou_catalog(
        request: Request,
        csrf_token: str = Form(""),
        ldap_server: str = Form(""),
        ldap_domain: str = Form(""),
        ldap_username: str = Form(""),
        ldap_password: str = Form(""),
        ldap_port: int = Form(636),
        ldap_use_ssl: Optional[str] = Form(None),
        ldap_validate_cert: Optional[str] = Form(None),
        ldap_ca_cert_path: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
        csrf_error = reject_invalid_csrf(request, csrf_token, "/config")
        if csrf_error:
            return JSONResponse(
                {"ok": False, "error": "The configuration session expired. Refresh the page and try again."},
                status_code=400,
            )
        return JSONResponse(
            build_target_ou_catalog(
                request,
                ldap_server=ldap_server,
                ldap_domain=ldap_domain,
                ldap_username=ldap_username,
                ldap_password=ldap_password,
                ldap_port=ldap_port,
                ldap_use_ssl=ldap_use_ssl,
                ldap_validate_cert=ldap_validate_cert,
                ldap_ca_cert_path=ldap_ca_cert_path,
            )
        )

    @app.post("/config/preview")
    def config_preview(
        request: Request,
        csrf_token: str = Form(""),
        source_provider: str = Form("wecom"),
        corpid: str = Form(""),
        agentid: str = Form(""),
        corpsecret: str = Form(""),
        webhook_url: str = Form(""),
        ldap_server: str = Form(""),
        ldap_domain: str = Form(""),
        ldap_username: str = Form(""),
        ldap_password: str = Form(""),
        ldap_port: int = Form(636),
        ldap_use_ssl: Optional[str] = Form(None),
        ldap_validate_cert: Optional[str] = Form(None),
        ldap_ca_cert_path: str = Form(""),
        default_password: str = Form(""),
        force_change_password: Optional[str] = Form(None),
        password_complexity: str = Form("strong"),
        schedule_time: str = Form("03:00"),
        retry_interval: int = Form(60),
        max_retries: int = Form(3),
        group_display_separator: str = Form("-"),
        group_recursive_enabled: Optional[str] = Form(None),
        managed_relation_cleanup_enabled: Optional[str] = Form(None),
        schedule_execution_mode: str = Form("apply"),
        web_bind_host: str = Form("127.0.0.1"),
        web_bind_port: int = Form(8000),
        web_public_base_url: str = Form(""),
        web_session_cookie_secure_mode: str = Form("auto"),
        web_trust_proxy_headers: Optional[str] = Form(None),
        web_forwarded_allow_ips: str = Form("127.0.0.1"),
        sspr_enabled: Optional[str] = Form(None),
        sspr_min_password_length: int = Form(12),
        sspr_unlock_account_default: Optional[str] = Form(None),
        sspr_verification_session_ttl_seconds: int = Form(600),
        brand_display_name: str = Form("AD Org Sync"),
        brand_mark_text: str = Form("AD"),
        brand_attribution: str = Form("微信公众号：大刘讲IT"),
        user_ou_placement_strategy: str = Form("source_primary_department"),
        source_root_unit_ids: str = Form(""),
        source_root_unit_display_text: str = Form(""),
        directory_root_ou_path: str = Form(""),
        disabled_users_ou_path: str = Form("Disabled Users"),
        custom_group_ou_path: str = Form("Managed Groups"),
        soft_excluded_groups: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/config")
        if csrf_error:
            return csrf_error

        submission = _build_config_submission_from_values(
            request,
            build_config_submission=build_config_submission,
            values=locals(),
        )
        preview = build_config_change_preview(request, submission)
        if preview["changed_count"] == 0:
            request.session.pop(config_preview_session_key, None)
            flash(request, "warning", "No configuration changes were detected")
            return RedirectResponse(url="/config", status_code=303)

        preview_token = secrets.token_urlsafe(12)
        request.session[config_preview_session_key] = {
            "token": preview_token,
            "submission": submission,
        }
        return render(
            request,
            "config.html",
            **build_config_page_context(
                request,
                editable_override=build_config_editable_override(request, submission),
                config_change_preview=preview,
                preview_token=preview_token,
            ),
        )

    @app.post("/config/confirm")
    def config_confirm(request: Request, csrf_token: str = Form(""), preview_token: str = Form("")):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/config")
        if csrf_error:
            return csrf_error

        preview_payload = request.session.get(config_preview_session_key)
        if not isinstance(preview_payload, dict) or str(preview_payload.get("token") or "") != str(preview_token or ""):
            flash(request, "error", "The pending configuration preview has expired. Preview the changes again.")
            return RedirectResponse(url="/config", status_code=303)

        try:
            apply_config_submission(
                request,
                user=user,
                submission=dict(preview_payload.get("submission") or {}),
            )
        except ValueError as exc:
            flash(request, "error", str(exc))
            return RedirectResponse(url="/config", status_code=303)
        finally:
            request.session.pop(config_preview_session_key, None)

        flash(
            request,
            "success",
            _config_saved_message(request),
        )
        return RedirectResponse(url="/config", status_code=303)

    @app.post("/config")
    def config_submit(
        request: Request,
        csrf_token: str = Form(""),
        source_provider: str = Form("wecom"),
        corpid: str = Form(""),
        agentid: str = Form(""),
        corpsecret: str = Form(""),
        webhook_url: str = Form(""),
        ldap_server: str = Form(""),
        ldap_domain: str = Form(""),
        ldap_username: str = Form(""),
        ldap_password: str = Form(""),
        ldap_port: int = Form(636),
        ldap_use_ssl: Optional[str] = Form(None),
        ldap_validate_cert: Optional[str] = Form(None),
        ldap_ca_cert_path: str = Form(""),
        default_password: str = Form(""),
        force_change_password: Optional[str] = Form(None),
        password_complexity: str = Form("strong"),
        schedule_time: str = Form("03:00"),
        retry_interval: int = Form(60),
        max_retries: int = Form(3),
        group_display_separator: str = Form("-"),
        group_recursive_enabled: Optional[str] = Form(None),
        managed_relation_cleanup_enabled: Optional[str] = Form(None),
        schedule_execution_mode: str = Form("apply"),
        web_bind_host: str = Form("127.0.0.1"),
        web_bind_port: int = Form(8000),
        web_public_base_url: str = Form(""),
        web_session_cookie_secure_mode: str = Form("auto"),
        web_trust_proxy_headers: Optional[str] = Form(None),
        web_forwarded_allow_ips: str = Form("127.0.0.1"),
        sspr_enabled: Optional[str] = Form(None),
        sspr_min_password_length: int = Form(12),
        sspr_unlock_account_default: Optional[str] = Form(None),
        sspr_verification_session_ttl_seconds: int = Form(600),
        brand_display_name: str = Form("AD Org Sync"),
        brand_mark_text: str = Form("AD"),
        brand_attribution: str = Form("微信公众号：大刘讲IT"),
        user_ou_placement_strategy: str = Form("source_primary_department"),
        source_root_unit_ids: str = Form(""),
        source_root_unit_display_text: str = Form(""),
        directory_root_ou_path: str = Form(""),
        disabled_users_ou_path: str = Form("Disabled Users"),
        custom_group_ou_path: str = Form("Managed Groups"),
        soft_excluded_groups: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/config")
        if csrf_error:
            return csrf_error

        submission = _build_config_submission_from_values(
            request,
            build_config_submission=build_config_submission,
            values=locals(),
        )
        apply_config_submission(request, user=user, submission=submission)
        request.session.pop(config_preview_session_key, None)
        flash(
            request,
            "success",
            _config_saved_message(request),
        )
        return RedirectResponse(url="/config", status_code=303)
