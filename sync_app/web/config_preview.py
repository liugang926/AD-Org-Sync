from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urlparse

from fastapi import Request

from sync_app.providers.source import get_source_provider_schema, list_source_provider_options, normalize_source_provider
from sync_app.services.typed_settings import SSPRSettings
from sync_app.web.app_state import get_web_repositories, get_web_runtime_state
from sync_app.web.config_presentation import (
    build_config_preview_groups,
    build_source_provider_field_models,
    build_source_provider_fields,
    build_source_provider_ui_catalog,
    format_config_change_value,
)
from sync_app.web.runtime import resolve_web_runtime_settings, web_runtime_requires_restart


HIGH_RISK_CONFIG_FIELDS = {
    "source_provider",
    "corpsecret",
    "ldap_server",
    "ldap_domain",
    "ldap_username",
    "ldap_password",
    "ldap_use_ssl",
    "ldap_validate_cert",
    "ldap_ca_cert_path",
    "default_password",
    "source_root_unit_ids",
    "directory_root_ou_path",
    "disabled_users_ou_path",
    "custom_group_ou_path",
    "managed_relation_cleanup_enabled",
    "schedule_execution_mode",
    "web_bind_host",
    "web_bind_port",
    "web_public_base_url",
    "web_session_cookie_secure_mode",
    "web_trust_proxy_headers",
    "web_forwarded_allow_ips",
}


def _config_value_is_empty(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == ()


def _config_change_category(field_type: str, current_value: Any, proposed_value: Any) -> str:
    if field_type == "bool" and bool(current_value) and not bool(proposed_value):
        return "Disabled"
    if _config_value_is_empty(current_value) and not _config_value_is_empty(proposed_value):
        return "Added"
    if not _config_value_is_empty(current_value) and _config_value_is_empty(proposed_value):
        return "Deleted"
    return "Modified"


def _normalize_job_status(value: str | None) -> str:
    return str(value or "").strip().upper()


def _is_successful_dry_run(job: Any) -> bool:
    return (
        str(getattr(job, "execution_mode", "") or "").strip().lower() == "dry_run"
        and _normalize_job_status(getattr(job, "status", "")) == "COMPLETED"
    )


def _message(code: str, **params: Any) -> dict[str, Any]:
    return {"code": code, "params": params}


def _build_config_rollout_status(request: Request, current_org: Any) -> dict[str, Any]:
    repositories = get_web_repositories(request)
    recent_jobs = repositories.job_repo.list_recent_job_records(limit=20, org_id=current_org.org_id)
    latest_dry_run = next(
        (
            job
            for job in recent_jobs
            if str(getattr(job, "execution_mode", "") or "").strip().lower() == "dry_run"
        ),
        None,
    )
    latest_successful_dry_run = next((job for job in recent_jobs if _is_successful_dry_run(job)), None)
    latest_apply = next(
        (
            job
            for job in recent_jobs
            if str(getattr(job, "execution_mode", "") or "").strip().lower() == "apply"
        ),
        None,
    )
    active_job = repositories.job_repo.get_active_job_record(org_id=current_org.org_id)
    open_conflict_count = (
        repositories.conflict_repo.count_unresolved_conflicts_for_plan(
            latest_successful_dry_run.job_id
        )
        if latest_successful_dry_run
        else 0
    )

    if active_job:
        return {
            "phase": "active_job",
            "title_message": _message("config.rollout.active_job.title"),
            "description_message": _message("config.rollout.active_job.description"),
            "badge_message": _message("config.rollout.active_job.badge"),
            "badge_level": "info",
            "primary_action_message": _message("config.rollout.active_job.action"),
            "primary_action_url": f"/execution-center/jobs/{active_job.job_id}",
            "show_run_dry_run": False,
            "open_conflict_count": int(open_conflict_count or 0),
            "latest_dry_run": latest_dry_run,
            "latest_successful_dry_run": latest_successful_dry_run,
            "latest_apply": latest_apply,
            "active_job": active_job,
        }

    if not latest_successful_dry_run:
        return {
            "phase": "first_dry_run",
            "title_message": _message("config.rollout.first_dry_run.title"),
            "description_message": _message("config.rollout.first_dry_run.description"),
            "badge_message": _message("config.rollout.first_dry_run.badge"),
            "badge_level": "warning",
            "primary_action_message": _message("config.rollout.first_dry_run.action"),
            "primary_action_url": "/execution-center/dry-run",
            "show_run_dry_run": True,
            "open_conflict_count": int(open_conflict_count or 0),
            "latest_dry_run": latest_dry_run,
            "latest_successful_dry_run": latest_successful_dry_run,
            "latest_apply": latest_apply,
            "active_job": active_job,
        }

    if int(open_conflict_count or 0) > 0:
        return {
            "phase": "review_conflicts",
            "title_message": _message("config.rollout.review_conflicts.title"),
            "description_message": _message("config.rollout.review_conflicts.description"),
            "badge_message": _message("config.rollout.review_conflicts.badge"),
            "badge_level": "warning",
            "primary_action_message": _message("config.rollout.review_conflicts.action"),
            "primary_action_url": "/conflicts",
            "show_run_dry_run": False,
            "open_conflict_count": int(open_conflict_count or 0),
            "latest_dry_run": latest_dry_run,
            "latest_successful_dry_run": latest_successful_dry_run,
            "latest_apply": latest_apply,
            "active_job": active_job,
        }

    if not latest_apply:
        return {
            "phase": "ready_for_apply",
            "title_message": _message("config.rollout.ready_for_apply.title"),
            "description_message": _message("config.rollout.ready_for_apply.description"),
            "badge_message": _message("config.rollout.ready_for_apply.badge"),
            "badge_level": "success",
            "primary_action_message": _message("config.rollout.ready_for_apply.action"),
            "primary_action_url": "/execution-center/apply",
            "show_run_dry_run": False,
            "open_conflict_count": int(open_conflict_count or 0),
            "latest_dry_run": latest_dry_run,
            "latest_successful_dry_run": latest_successful_dry_run,
            "latest_apply": latest_apply,
            "active_job": active_job,
        }

    return {
        "phase": "review_latest",
        "title_message": _message("config.rollout.review_latest.title"),
        "description_message": _message("config.rollout.review_latest.description"),
        "badge_message": _message("config.rollout.review_latest.badge"),
        "badge_level": "info",
        "primary_action_message": _message("config.rollout.review_latest.action"),
        "primary_action_url": "/execution-center/jobs",
        "show_run_dry_run": False,
        "open_conflict_count": int(open_conflict_count or 0),
        "latest_dry_run": latest_dry_run,
        "latest_successful_dry_run": latest_successful_dry_run,
        "latest_apply": latest_apply,
        "active_job": active_job,
    }


def _url_with_public_base(public_base_url: str, path: str) -> str:
    normalized_base_url = str(public_base_url or "").strip().rstrip("/")
    normalized_path = "/" + str(path or "").strip().lstrip("/")
    return f"{normalized_base_url}{normalized_path}" if normalized_base_url else normalized_path


def _build_sspr_status_context(
    editable: dict[str, Any],
    *,
    repositories: Any,
    org_id: str,
) -> dict[str, Any]:
    public_base_url = str(editable.get("web_public_base_url") or "").strip()
    parsed_public_url = urlparse(public_base_url)
    enabled = bool(editable.get("sspr_enabled"))
    configured_corp_id = str(editable.get("sspr_dingtalk_corp_id") or "").strip()
    duplicate_org_ids = []
    for organization in repositories.organization_repo.list_organization_records(enabled_only=True):
        if organization.org_id == org_id:
            continue
        other_settings = SSPRSettings.load(
            repositories.settings_repo,
            org_id=organization.org_id,
        )
        if not other_settings.enabled or other_settings.dingtalk_corp_id != configured_corp_id:
            continue
        try:
            other_config = repositories.org_config_repo.get_app_config(
                organization.org_id,
                config_path=organization.config_path,
            )
        except Exception:
            continue
        if normalize_source_provider(other_config.source_provider) == "dingtalk":
            duplicate_org_ids.append(organization.org_id)
    corp_id_unique = bool(configured_corp_id) and not duplicate_org_ids
    enabled_bindings = [
        binding
        for binding in repositories.user_binding_repo.list_enabled_binding_records(
            org_id=org_id,
            source_provider="dingtalk",
        )
        if str(binding.ad_username or "").strip()
    ]
    verify_logs, _ = repositories.audit_repo.list_recent_logs_page(
        limit=1,
        query="sspr.verify",
        org_id=org_id,
        include_global=False,
    )
    reset_logs, _ = repositories.audit_repo.list_recent_logs_page(
        limit=1,
        query="sspr.password_reset",
        org_id=org_id,
        include_global=False,
    )
    try:
        public_port = parsed_public_url.port or (443 if parsed_public_url.scheme == "https" else None)
    except ValueError:
        public_port = None
    checks = {
        "Enable SSPR": enabled,
        "DingTalk source provider": normalize_source_provider(editable.get("source_provider")) == "dingtalk",
        "DingTalk CorpId": bool(str(editable.get("sspr_dingtalk_corp_id") or "").strip()),
        "Unique DingTalk CorpId mapping": corp_id_unique,
        "DingTalk AppKey": bool(str(editable.get("corpid") or "").strip()),
        "DingTalk AppSecret": bool(editable.get("corpsecret_configured")),
        "AD / LDAPS connector": all(
            (
                str(editable.get("ldap_server") or "").strip(),
                str(editable.get("ldap_domain") or "").strip(),
                str(editable.get("ldap_username") or "").strip(),
                bool(editable.get("ldap_password_configured")),
            )
        ),
        "Public HTTPS base URL": parsed_public_url.scheme == "https" and bool(parsed_public_url.netloc),
    }
    missing = [label for label, ready in checks.items() if not ready]
    ready = not missing
    return {
        "enabled": enabled,
        "ready": ready,
        "badge_text": "Ready" if ready else "Incomplete",
        "badge_level": "success" if ready else "warning",
        "portal_url": _url_with_public_base(public_base_url, "/sspr?corpid=$CORPID$"),
        "auth_url": _url_with_public_base(public_base_url, "/sspr/auth/dingtalk"),
        "callback_url": _url_with_public_base(public_base_url, "/sspr/callback/dingtalk"),
        "public_base_url": public_base_url or "—",
        "public_port": public_port or "—",
        "enabled_binding_count": len(enabled_bindings),
        "last_verification": verify_logs[0] if verify_logs else None,
        "last_password_reset": reset_logs[0] if reset_logs else None,
        "missing": missing,
    }


def build_config_change_preview(support: Any, request: Request, submission: dict[str, Any]) -> dict[str, Any]:
    repositories = get_web_repositories(request)
    runtime_state = get_web_runtime_state(request)
    current_org = support.request_support.get_current_org(request)
    current_state = support.build_current_config_state(request, current_org)
    proposed_state = {
        **submission["org_values"],
        **submission["settings_values"],
        "soft_excluded_groups": submission["soft_excluded_groups"],
    }
    groups: list[dict[str, Any]] = []
    changed_count = 0
    category_counts = {
        "added": 0,
        "modified": 0,
        "disabled": 0,
        "deleted": 0,
        "high_risk": 0,
    }
    provider_schema = get_source_provider_schema(submission["org_values"].get("source_provider"))
    for group_title, fields in build_config_preview_groups(provider_schema):
        group_changes: list[dict[str, Any]] = []
        for field_name, label, field_type in fields:
            current_value = current_state.get(field_name)
            proposed_value = proposed_state.get(field_name)
            if current_value == proposed_value:
                continue
            before_display, before_translate = format_config_change_value(
                field_type,
                current_value,
                source_provider_label=support.request_support.source_provider_label,
                placement_strategies=support.placement_strategies,
                split_csv_values=support.split_csv_values,
                previous_value=None,
            )
            after_display, after_translate = format_config_change_value(
                field_type,
                proposed_value,
                source_provider_label=support.request_support.source_provider_label,
                placement_strategies=support.placement_strategies,
                split_csv_values=support.split_csv_values,
                previous_value=current_value,
            )
            category = _config_change_category(field_type, current_value, proposed_value)
            high_risk = field_name in HIGH_RISK_CONFIG_FIELDS
            category_counts[category.lower()] += 1
            if high_risk:
                category_counts["high_risk"] += 1
            group_changes.append(
                {
                    "field_name": field_name,
                    "label": label,
                    "before": before_display,
                    "after": after_display,
                    "translate_before": before_translate,
                    "translate_after": after_translate,
                    "category": category,
                    "high_risk": high_risk,
                }
            )
        if group_changes:
            groups.append({"title": group_title, "changes": group_changes})
            changed_count += len(group_changes)

    proposed_runtime_settings = resolve_web_runtime_settings(
        repositories.settings_repo,
        bind_host=str(submission["settings_values"]["web_bind_host"]),
        bind_port=int(submission["settings_values"]["web_bind_port"]),
        public_base_url=str(submission["settings_values"]["web_public_base_url"]),
        session_cookie_secure_mode=str(submission["settings_values"]["web_session_cookie_secure_mode"]),
        trust_proxy_headers=bool(submission["settings_values"]["web_trust_proxy_headers"]),
        forwarded_allow_ips=str(submission["settings_values"]["web_forwarded_allow_ips"]),
    )
    return {
        "groups": groups,
        "changed_count": changed_count,
        "category_counts": category_counts,
        "restart_required": web_runtime_requires_restart(
            runtime_state.web_runtime_settings,
            proposed_runtime_settings,
        ),
    }


def build_config_editable_override(support: Any, request: Request, submission: dict[str, Any]) -> dict[str, Any]:
    current_org = support.request_support.get_current_org(request)
    repositories = get_web_repositories(request)
    editable = repositories.org_config_repo.get_editable_config(
        current_org.org_id,
        config_path=support.request_support.get_org_config_path(request),
    )
    editable.update(
        {
            "source_provider": submission["org_values"]["source_provider"],
            "corpid": submission["org_values"]["corpid"],
            "agentid": submission["org_values"]["agentid"],
            "corpsecret": "",
            "corpsecret_configured": bool(submission["org_values"]["corpsecret"]),
            "webhook_url": "",
            "webhook_url_configured": bool(submission["org_values"]["webhook_url"]),
            "ldap_server": submission["org_values"]["ldap_server"],
            "ldap_domain": submission["org_values"]["ldap_domain"],
            "ldap_username": submission["org_values"]["ldap_username"],
            "ldap_password": "",
            "ldap_password_configured": bool(submission["org_values"]["ldap_password"]),
            "ldap_port": submission["org_values"]["ldap_port"],
            "ldap_use_ssl": submission["org_values"]["ldap_use_ssl"],
            "ldap_validate_cert": submission["org_values"]["ldap_validate_cert"],
            "ldap_ca_cert_path": submission["org_values"]["ldap_ca_cert_path"],
            "default_password": "",
            "default_password_configured": bool(submission["org_values"]["default_password"]),
            "force_change_password": submission["org_values"]["force_change_password"],
            "password_complexity": submission["org_values"]["password_complexity"],
            "schedule_time": submission["org_values"]["schedule_time"],
            "retry_interval": submission["org_values"]["retry_interval"],
            "max_retries": submission["org_values"]["max_retries"],
            "protected_accounts": list(submission["org_values"]["exclude_accounts"]),
            "group_display_separator": submission["settings_values"]["group_display_separator"],
            "group_recursive_enabled": submission["settings_values"]["group_recursive_enabled"],
            "managed_relation_cleanup_enabled": submission["settings_values"]["managed_relation_cleanup_enabled"],
            "schedule_execution_mode": submission["settings_values"]["schedule_execution_mode"],
            "web_bind_host": submission["settings_values"]["web_bind_host"],
            "web_bind_port": submission["settings_values"]["web_bind_port"],
            "web_public_base_url": submission["settings_values"]["web_public_base_url"],
            "web_session_cookie_secure_mode": submission["settings_values"]["web_session_cookie_secure_mode"],
            "web_trust_proxy_headers": submission["settings_values"]["web_trust_proxy_headers"],
            "web_forwarded_allow_ips": submission["settings_values"]["web_forwarded_allow_ips"],
            "brand_display_name": submission["settings_values"]["brand_display_name"],
            "brand_mark_text": submission["settings_values"]["brand_mark_text"],
            "brand_attribution": submission["settings_values"]["brand_attribution"],
            "user_ou_placement_strategy": submission["settings_values"]["user_ou_placement_strategy"],
            "source_root_unit_ids": submission["settings_values"]["source_root_unit_ids"],
            "source_root_unit_display_text": submission["settings_values"]["source_root_unit_display_text"],
            "directory_root_ou_path": submission["settings_values"]["directory_root_ou_path"],
            "disabled_users_ou_path": submission["settings_values"]["disabled_users_ou_path"],
            "custom_group_ou_path": submission["settings_values"]["custom_group_ou_path"],
            "soft_excluded_groups": submission["soft_excluded_groups"],
        }
    )
    return editable


def build_config_page_context(
    support: Any,
    request: Request,
    *,
    editable_override: Optional[dict[str, Any]] = None,
    config_change_preview: Optional[dict[str, Any]] = None,
    preview_token: str = "",
) -> dict[str, Any]:
    current_org = support.request_support.get_current_org(request)
    repositories = get_web_repositories(request)
    editable = editable_override or repositories.org_config_repo.get_editable_config(
        current_org.org_id,
        config_path=support.request_support.get_org_config_path(request),
    )
    if "protected_accounts" not in editable:
        effective_config = repositories.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=support.request_support.get_org_config_path(request),
        )
        editable["protected_accounts"] = list(effective_config.exclude_accounts)
    editable.setdefault(
        "brand_display_name",
        repositories.settings_repo.get_value("brand_display_name", support.default_brand_display_name),
    )
    editable.setdefault(
        "brand_mark_text",
        repositories.settings_repo.get_value("brand_mark_text", support.default_brand_mark_text),
    )
    editable.setdefault(
        "brand_attribution",
        repositories.settings_repo.get_value("brand_attribution", support.default_brand_attribution),
    )
    editable.setdefault(
        "source_root_unit_ids",
        repositories.settings_repo.get_value("source_root_unit_ids", "", org_id=current_org.org_id),
    )
    editable.setdefault(
        "source_root_unit_display_text",
        repositories.settings_repo.get_value("source_root_unit_display_text", "", org_id=current_org.org_id),
    )
    editable.setdefault(
        "directory_root_ou_path",
        repositories.settings_repo.get_value("directory_root_ou_path", "", org_id=current_org.org_id),
    )
    editable.setdefault(
        "disabled_users_ou_path",
        repositories.settings_repo.get_value("disabled_users_ou_path", "Disabled Users", org_id=current_org.org_id),
    )
    editable.setdefault(
        "custom_group_ou_path",
        repositories.settings_repo.get_value("custom_group_ou_path", "Managed Groups", org_id=current_org.org_id),
    )
    sspr_settings = SSPRSettings.load(repositories.settings_repo, org_id=current_org.org_id)
    editable.setdefault("sspr_enabled", sspr_settings.enabled)
    editable.setdefault("sspr_dingtalk_corp_id", sspr_settings.dingtalk_corp_id)
    editable.setdefault("sspr_min_password_length", sspr_settings.min_password_length)
    editable.setdefault("sspr_unlock_account_default", sspr_settings.unlock_account_default)
    editable.setdefault("sspr_verification_session_ttl_seconds", sspr_settings.verification_session_ttl_seconds)
    current_source_provider = normalize_source_provider(editable.get("source_provider"))
    provider_schema = get_source_provider_schema(current_source_provider)
    ui_language = support.request_support.get_ui_language(request)
    source_provider_name = support.request_support.source_provider_label(current_source_provider)
    localized_source_provider_name = support.request_support.translate_text(ui_language, source_provider_name)
    source_provider_options = list_source_provider_options(include_unimplemented=True)
    source_provider_ui_catalog = build_source_provider_ui_catalog(
        support.request_support,
        ui_language,
    )
    protected_rules = repositories.exclusion_repo.list_rules(
        rule_type="protect",
        protection_level="hard",
        org_id=current_org.org_id,
    )
    config_rollout_status = _build_config_rollout_status(request, current_org)
    sspr_status = _build_sspr_status_context(
        editable,
        repositories=repositories,
        org_id=current_org.org_id,
    )
    return {
        "page": "config",
        "title": support.request_support.translate_text(
            ui_language,
            "{provider} Connector Configuration",
            provider=localized_source_provider_name,
        ),
        "editable": editable,
        "current_org": current_org,
        "source_provider_name": source_provider_name,
        "source_provider_options": source_provider_options,
        "source_provider_schema": provider_schema,
        "source_provider_ui_catalog": source_provider_ui_catalog,
        "source_connection_fields": build_source_provider_field_models(editable, provider_schema.connection_fields),
        "source_notification_fields": build_source_provider_field_models(editable, provider_schema.notification_fields),
        "source_provider_fields": build_source_provider_fields(editable),
        "protected_rules": protected_rules,
        "config_change_preview": config_change_preview,
        "config_preview_token": preview_token,
        "config_rollout_status": config_rollout_status,
        "sspr_status": sspr_status,
        "filters_are_remembered": True,
    }
