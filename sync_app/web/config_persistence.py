from __future__ import annotations

from typing import Any

from fastapi import Request

from sync_app.core.models import WebAdminUserRecord
from sync_app.services.typed_settings import BrandingSettings, DirectoryUiSettings, SSPRSettings, WebRuntimeSettings
from sync_app.web.app_state import get_web_repositories


def apply_config_submission(
    support: Any,
    request: Request,
    *,
    user: WebAdminUserRecord,
    submission: dict[str, Any],
) -> None:
    current_org = support.request_support.get_current_org(request)
    repositories = get_web_repositories(request)
    if current_org.org_id != str(submission.get("org_id") or current_org.org_id):
        raise ValueError("Pending configuration preview no longer matches the selected organization.")

    repositories.org_config_repo.save_config(
        current_org.org_id,
        submission["org_values"],
        config_path=str(submission["legacy_config_path"]),
    )
    DirectoryUiSettings.from_mapping(submission["settings_values"]).persist(
        repositories.settings_repo,
        org_id=current_org.org_id,
    )
    WebRuntimeSettings.from_mapping(submission["settings_values"]).persist(repositories.settings_repo)
    SSPRSettings.from_mapping(submission["settings_values"]).persist(
        repositories.settings_repo,
        org_id=current_org.org_id,
    )
    BrandingSettings.from_mapping(
        submission["settings_values"],
        default_display_name=support.default_brand_display_name,
        default_mark_text=support.default_brand_mark_text,
        default_attribution=support.default_brand_attribution,
    ).persist(repositories.settings_repo)
    repositories.exclusion_repo.replace_soft_excluded_rules(
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
    repositories.audit_repo.add_log(
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
            "sspr_enabled": bool(submission["settings_values"]["sspr_enabled"]),
            "sspr_min_password_length": int(submission["settings_values"]["sspr_min_password_length"]),
            "sspr_unlock_account_default": bool(submission["settings_values"]["sspr_unlock_account_default"]),
            "sspr_verification_session_ttl_seconds": int(
                submission["settings_values"]["sspr_verification_session_ttl_seconds"]
            ),
            "ldap_validate_cert": bool(submission["org_values"]["ldap_validate_cert"]),
            "force_change_password": bool(submission["org_values"]["force_change_password"]),
            "password_complexity": submission["org_values"]["password_complexity"],
            "source_root_unit_ids": submission["settings_values"]["source_root_unit_ids"],
            "source_root_unit_display_text": submission["settings_values"]["source_root_unit_display_text"],
            "directory_root_ou_path": submission["settings_values"]["directory_root_ou_path"],
            "disabled_users_ou_path": submission["settings_values"]["disabled_users_ou_path"],
            "custom_group_ou_path": submission["settings_values"]["custom_group_ou_path"],
        },
    )
