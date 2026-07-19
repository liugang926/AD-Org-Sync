from __future__ import annotations

from typing import Any, Mapping

from sync_app.services.typed_settings import AdvancedSyncPolicySettings, DirectoryUiSettings


USERNAME_STRATEGY_BY_SOURCE_FIELD: dict[str, str] = {
    "source_user_id": "userid",
    "employee_id": "employee_id",
    "email_localpart": "email_localpart",
    "pinyin_initials_employee_id": "pinyin_initials_employee_id",
    "pinyin_full_employee_id": "pinyin_full_employee_id",
    "family_name_pinyin_given_initials": "family_name_pinyin_given_initials",
    "family_name_pinyin_given_name_pinyin": "family_name_pinyin_given_name_pinyin",
    "custom_template": "custom_template",
}


POLICY_SECTION_FIELDS: dict[str, frozenset[str]] = {
    "attribute_mappings": frozenset({"attribute_mapping_enabled", "write_back_enabled"}),
    "group_rules": frozenset(
        {
            "custom_group_sync_enabled",
            "managed_group_type",
            "managed_group_mail_domain",
            "custom_group_ou_path",
        }
    ),
    "lifecycle": frozenset(
        {
            "offboarding_grace_days",
            "offboarding_notify_managers",
            "offboarding_lifecycle_enabled",
            "rehire_restore_enabled",
            "automatic_replay_enabled",
            "future_onboarding_enabled",
            "future_onboarding_start_field",
            "contractor_lifecycle_enabled",
            "lifecycle_employment_type_field",
            "contractor_end_field",
            "lifecycle_sponsor_field",
            "contractor_type_values",
        }
    ),
    "security": frozenset(
        {
            "advanced_connector_routing_enabled",
            "disable_circuit_breaker_enabled",
            "disable_circuit_breaker_percent",
            "disable_circuit_breaker_min_count",
            "disable_circuit_breaker_requires_approval",
            "first_sync_identity_claim_mode",
        }
    ),
}


CONNECTOR_POLICY_SECTION_FIELDS: dict[str, frozenset[str]] = {
    "scope": frozenset({"root_department_ids"}),
    "account_naming": frozenset(
        {
            "username_strategy",
            "username_collision_policy",
            "username_collision_template",
            "username_template",
        }
    ),
    "group_rules": frozenset(
        {
            "group_type",
            "group_mail_domain",
            "custom_group_ou_path",
            "managed_tag_ids",
            "managed_external_chat_ids",
        }
    ),
    "lifecycle": frozenset({"disabled_users_ou"}),
    "security": frozenset({"force_change_password", "password_complexity"}),
}


DIRECTORY_POLICY_SECTION_FIELDS: dict[str, frozenset[str]] = {
    "department_ou_routing": frozenset(
        {
            "user_ou_placement_strategy",
            "source_root_unit_ids",
            "source_root_unit_display_text",
            "directory_root_ou_path",
            "disabled_users_ou_path",
        }
    ),
    "group_rules": frozenset(
        {
            "group_display_separator",
            "group_recursive_enabled",
            "managed_relation_cleanup_enabled",
            "custom_group_ou_path",
        }
    ),
}


POLICY_AUDIT_ACTION_PREFIXES = (
    "sync_policy.",
    "source_directory.scope.",
    "config.release_",
)


def update_directory_policy_section(
    current: DirectoryUiSettings,
    section: str,
    values: Mapping[str, Any],
) -> DirectoryUiSettings:
    allowed_fields = DIRECTORY_POLICY_SECTION_FIELDS.get(str(section or "").strip())
    if allowed_fields is None:
        raise ValueError(f"Unknown directory policy section: {section}")
    unknown_fields = set(values) - set(allowed_fields)
    if unknown_fields:
        raise ValueError(
            "Fields do not belong to this directory policy section: "
            + ", ".join(sorted(unknown_fields))
        )
    merged = current.to_dict()
    merged.update({field: values[field] for field in allowed_fields if field in values})
    return DirectoryUiSettings.from_mapping(merged)


def estimate_scope_affected_users(
    source_directory_repo: Any,
    *,
    org_id: str,
    provider_id: str,
    snapshot: Any,
    scope: Mapping[str, Any] | None,
) -> int:
    if not snapshot:
        return 0
    snapshot_id = int(snapshot["id"])
    normalized_scope = dict(scope or {})
    scope_type = str(normalized_scope.get("scope_type") or "full").strip().lower()
    if scope_type in {"selected_users", "source_user"}:
        return len(
            {
                str(value).strip()
                for value in normalized_scope.get("selected_source_user_ids") or []
                if str(value).strip()
            }
        )
    if scope_type == "department":
        selected_departments = {
            str(value).strip()
            for value in normalized_scope.get("selected_department_ids") or []
            if str(value).strip()
        }
        if not selected_departments:
            return 0
        descendants = {
            str(department.get("source_department_id") or "").strip()
            for department in source_directory_repo.list_departments(
                snapshot_id,
                org_id=org_id,
            )
            if selected_departments
            & {
                str(value).strip()
                for value in department.get("path_ids") or []
                if str(value).strip()
            }
        }
        affected_source_ids: set[str] = set()
        offset = 0
        while True:
            page = source_directory_repo.list_users(
                snapshot_id,
                org_id=org_id,
                provider_id=provider_id,
                status="active",
                limit=200,
                offset=offset,
            )
            for item in page["items"]:
                if descendants & {
                    str(value).strip()
                    for value in item.get("department_ids") or []
                    if str(value).strip()
                }:
                    affected_source_ids.add(str(item.get("source_user_id") or ""))
            offset += len(page["items"])
            if offset >= int(page["total"]) or not page["items"]:
                break
        return len(affected_source_ids)
    return int(
        source_directory_repo.list_users(
            snapshot_id,
            org_id=org_id,
            provider_id=provider_id,
            status="active",
            limit=1,
            offset=0,
        )["total"]
    )


def build_policy_governance_context(
    *,
    repositories: Any,
    current_org: Any,
    provider_id: str,
    snapshot: Any,
    scope: Mapping[str, Any] | None,
    current_config_fingerprint: str,
    release: Mapping[str, Any],
) -> dict[str, Any]:
    recent_jobs = repositories.job_repo.list_recent_job_records(
        limit=30,
        org_id=current_org.org_id,
    )
    latest_dry_run = next(
        (
            job
            for job in recent_jobs
            if str(getattr(job, "execution_mode", "") or "").strip().lower()
            == "dry_run"
            and str(getattr(job, "status", "") or "").strip().upper()
            == "COMPLETED"
        ),
        None,
    )
    dry_run_invalidated = False
    dry_run_invalid_reason = ""
    if latest_dry_run is not None:
        if (
            not str(getattr(latest_dry_run, "config_snapshot_hash", "") or "").strip()
            or str(getattr(latest_dry_run, "config_snapshot_hash", "") or "").strip()
            != str(current_config_fingerprint or "").strip()
        ):
            dry_run_invalidated = True
            dry_run_invalid_reason = "The synchronization policy changed after this Dry Run."
        else:
            job_scope = repositories.source_directory_repo.get_job_scope(
                latest_dry_run.job_id,
                org_id=current_org.org_id,
            )
            current_selection_fingerprint = str(
                (scope or {}).get("selection_fingerprint") or ""
            ).strip()
            if (
                not job_scope
                or str(job_scope.get("selection_fingerprint") or "").strip()
                != current_selection_fingerprint
            ):
                dry_run_invalidated = True
                dry_run_invalid_reason = "The synchronization scope changed after this Dry Run."

    latest_snapshot = release.get("latest_snapshot")
    audit_records, _audit_total = repositories.audit_repo.list_recent_logs_page(
        limit=100,
        offset=0,
        org_id=current_org.org_id,
        include_global=False,
    )
    latest_policy_audit = next(
        (
            record
            for record in audit_records
            if str(getattr(record, "action_type", "") or "").startswith(
                POLICY_AUDIT_ACTION_PREFIXES
            )
        ),
        None,
    )
    last_modified_by = str(
        getattr(latest_policy_audit, "actor_username", "")
        or getattr(latest_snapshot, "created_by", "")
        or "-"
    )
    last_modified_at = str(
        getattr(latest_policy_audit, "created_at", "")
        or getattr(latest_snapshot, "created_at", "")
        or ""
    )
    version = (
        f"v{getattr(latest_snapshot, 'id', 0)}"
        if latest_snapshot is not None
        else "Draft v0"
    )
    if bool(release.get("has_unpublished_changes")):
        version = f"{version} + draft"

    return {
        "policy_version": version,
        "policy_last_modified_by": last_modified_by,
        "policy_last_modified_at": last_modified_at,
        "estimated_affected_users": estimate_scope_affected_users(
            repositories.source_directory_repo,
            org_id=current_org.org_id,
            provider_id=provider_id,
            snapshot=snapshot,
            scope=scope,
        ),
        "latest_policy_dry_run": latest_dry_run,
        "dry_run_invalidated": dry_run_invalidated,
        "dry_run_invalid_reason": dry_run_invalid_reason,
        "requires_new_dry_run": latest_dry_run is None or dry_run_invalidated,
    }


def update_policy_section(
    current: AdvancedSyncPolicySettings,
    section: str,
    values: Mapping[str, Any],
) -> AdvancedSyncPolicySettings:
    allowed_fields = POLICY_SECTION_FIELDS.get(str(section or "").strip())
    if allowed_fields is None:
        raise ValueError(f"Unknown sync policy section: {section}")
    unknown_fields = set(values) - set(allowed_fields)
    if unknown_fields:
        raise ValueError(
            "Fields do not belong to this sync policy section: "
            + ", ".join(sorted(unknown_fields))
        )
    merged = current.to_dict()
    merged.update({field: values[field] for field in allowed_fields if field in values})
    return AdvancedSyncPolicySettings.from_mapping(merged)


def build_connector_policy_upsert(
    record: Any,
    section: str,
    values: Mapping[str, Any],
) -> dict[str, Any]:
    allowed_fields = CONNECTOR_POLICY_SECTION_FIELDS.get(str(section or "").strip())
    if allowed_fields is None:
        raise ValueError(f"Unknown connector policy section: {section}")
    unknown_fields = set(values) - set(allowed_fields)
    if unknown_fields:
        raise ValueError(
            "Fields do not belong to this connector policy section: "
            + ", ".join(sorted(unknown_fields))
        )
    payload = {
        "connector_id": record.connector_id,
        "org_id": record.org_id,
        "name": record.name,
        "config_path": record.config_path,
        "ldap_server": record.ldap_server,
        "ldap_domain": record.ldap_domain,
        "ldap_username": record.ldap_username,
        "ldap_password": record.ldap_password,
        "ldap_use_ssl": record.ldap_use_ssl,
        "ldap_port": record.ldap_port,
        "ldap_validate_cert": record.ldap_validate_cert,
        "ldap_ca_cert_path": record.ldap_ca_cert_path,
        "default_password": record.default_password,
        "force_change_password": record.force_change_password,
        "password_complexity": record.password_complexity,
        "root_department_ids": list(record.root_department_ids),
        "username_strategy": record.username_strategy,
        "username_collision_policy": record.username_collision_policy,
        "username_collision_template": record.username_collision_template,
        "username_template": record.username_template,
        "disabled_users_ou": record.disabled_users_ou,
        "group_type": record.group_type,
        "group_mail_domain": record.group_mail_domain,
        "custom_group_ou_path": record.custom_group_ou_path,
        "managed_tag_ids": list(record.managed_tag_ids),
        "managed_external_chat_ids": list(record.managed_external_chat_ids),
        "is_enabled": record.is_enabled,
    }
    payload.update({field: values[field] for field in allowed_fields if field in values})
    return payload
