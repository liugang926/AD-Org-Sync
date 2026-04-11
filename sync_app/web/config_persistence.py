from __future__ import annotations

from typing import Any

from fastapi import Request

from sync_app.core.models import WebAdminUserRecord


def apply_config_submission(
    support: Any,
    request: Request,
    *,
    user: WebAdminUserRecord,
    submission: dict[str, Any],
) -> None:
    current_org = support.request_support.get_current_org(request)
    if current_org.org_id != str(submission.get("org_id") or current_org.org_id):
        raise ValueError("Pending configuration preview no longer matches the selected organization.")

    request.app.state.org_config_repo.save_config(
        current_org.org_id,
        submission["org_values"],
        config_path=str(submission["legacy_config_path"]),
    )
    request.app.state.settings_repo.set_value("group_display_separator", submission["settings_values"]["group_display_separator"], "string", org_id=current_org.org_id)
    request.app.state.settings_repo.set_value("group_recursive_enabled", str(bool(submission["settings_values"]["group_recursive_enabled"])).lower(), "bool", org_id=current_org.org_id)
    request.app.state.settings_repo.set_value("group_recursive_enabled_user_override", "true", "bool", org_id=current_org.org_id)
    request.app.state.settings_repo.set_value("managed_relation_cleanup_enabled", str(bool(submission["settings_values"]["managed_relation_cleanup_enabled"])).lower(), "bool", org_id=current_org.org_id)
    request.app.state.settings_repo.set_value("schedule_execution_mode", str(submission["settings_values"]["schedule_execution_mode"]), "string", org_id=current_org.org_id)
    request.app.state.settings_repo.set_value("web_bind_host", str(submission["settings_values"]["web_bind_host"]), "string")
    request.app.state.settings_repo.set_value("web_bind_port", str(submission["settings_values"]["web_bind_port"]), "int")
    request.app.state.settings_repo.set_value("web_public_base_url", str(submission["settings_values"]["web_public_base_url"]), "string")
    request.app.state.settings_repo.set_value("web_session_cookie_secure_mode", str(submission["settings_values"]["web_session_cookie_secure_mode"]), "string")
    request.app.state.settings_repo.set_value("web_trust_proxy_headers", str(bool(submission["settings_values"]["web_trust_proxy_headers"])).lower(), "bool")
    request.app.state.settings_repo.set_value("web_forwarded_allow_ips", str(submission["settings_values"]["web_forwarded_allow_ips"]), "string")
    request.app.state.settings_repo.set_value("brand_display_name", str(submission["settings_values"]["brand_display_name"]), "string")
    request.app.state.settings_repo.set_value("brand_mark_text", str(submission["settings_values"]["brand_mark_text"]), "string")
    request.app.state.settings_repo.set_value("brand_attribution", str(submission["settings_values"]["brand_attribution"]), "string")
    request.app.state.settings_repo.set_value("user_ou_placement_strategy", str(submission["settings_values"]["user_ou_placement_strategy"]), "string", org_id=current_org.org_id)
    request.app.state.settings_repo.set_value("source_root_unit_ids", str(submission["settings_values"]["source_root_unit_ids"]), "string", org_id=current_org.org_id)
    request.app.state.settings_repo.set_value("directory_root_ou_path", str(submission["settings_values"]["directory_root_ou_path"]), "string", org_id=current_org.org_id)
    request.app.state.settings_repo.set_value("disabled_users_ou_path", str(submission["settings_values"]["disabled_users_ou_path"]), "string", org_id=current_org.org_id)
    request.app.state.settings_repo.set_value("custom_group_ou_path", str(submission["settings_values"]["custom_group_ou_path"]), "string", org_id=current_org.org_id)
    request.app.state.exclusion_repo.replace_soft_excluded_rules(
        (
            {
                "match_value": line.strip(),
                "display_name": line.strip(),
                "is_enabled": True,
                "source": "web_ui",
            }
            for line in str(submission["soft_excluded_groups"]).splitlines()
            if line.strip()
        ),
        org_id=current_org.org_id,
    )
    request.app.state.audit_repo.add_log(
        org_id=current_org.org_id,
        actor_username=user.username,
        action_type="config.update",
        target_type="organization_config",
        target_id=current_org.org_id,
        result="success",
        message="Updated system configuration",
        payload={
            "org_id": current_org.org_id,
            "legacy_config_path": str(submission["legacy_config_path"]),
            "user_ou_placement_strategy": submission["settings_values"]["user_ou_placement_strategy"],
            "web_bind_host": submission["settings_values"]["web_bind_host"],
            "web_bind_port": submission["settings_values"]["web_bind_port"],
            "web_public_base_url": submission["settings_values"]["web_public_base_url"],
            "web_session_cookie_secure_mode": submission["settings_values"]["web_session_cookie_secure_mode"],
            "web_trust_proxy_headers": bool(submission["settings_values"]["web_trust_proxy_headers"]),
            "web_forwarded_allow_ips": submission["settings_values"]["web_forwarded_allow_ips"],
            "ldap_validate_cert": bool(submission["org_values"]["ldap_validate_cert"]),
            "force_change_password": bool(submission["org_values"]["force_change_password"]),
            "password_complexity": submission["org_values"]["password_complexity"],
            "source_root_unit_ids": submission["settings_values"]["source_root_unit_ids"],
            "directory_root_ou_path": submission["settings_values"]["directory_root_ou_path"],
            "disabled_users_ou_path": submission["settings_values"]["disabled_users_ou_path"],
            "custom_group_ou_path": submission["settings_values"]["custom_group_ou_path"],
        },
    )
