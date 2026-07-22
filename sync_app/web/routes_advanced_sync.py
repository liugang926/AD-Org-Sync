from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from sync_app.core.sync_policies import (
    ATTRIBUTE_SYNC_MODES,
    MANAGED_GROUP_TYPES,
    normalize_username_collision_policy,
    normalize_username_strategy,
)
from sync_app.services.high_risk_operations import (
    HighRiskOperationContext,
    HighRiskOperationPolicy,
    high_risk_audit_payload,
)
from sync_app.services.runtime_bootstrap import resolve_runtime_config_fingerprint
from sync_app.services.sync_policy_center import (
    USERNAME_STRATEGY_BY_SOURCE_FIELD,
    build_policy_governance_context,
    build_connector_policy_upsert,
    update_directory_policy_section,
    update_policy_section,
)
from sync_app.services.typed_settings import AdvancedSyncPolicySettings, DirectoryUiSettings
from sync_app.web.app_state import (
    get_web_repositories,
    get_web_runtime_state,
    get_web_services,
)
from sync_app.web.navigation import CANONICAL_ROUTE_PATHS


ADVANCED_SYNC_CLIENT_I18N_KEYS = (
    "AD Group",
    "Affected records",
    "Append 2-Digit Sequence",
    "Append 3-Digit Sequence",
    "Append Deterministic Hash",
    "Append Employee ID",
    "Append Numeric Counter",
    "Append Source User ID",
    "ASCII Name",
    "Auto-claim safe existing AD matches",
    "Auto-bind unique unprotected match",
    "Backfill email on the source directory record where it is supposed to exist.",
    "Backfill employee ID or switch the naming strategy away from employee-ID-driven rules.",
    "Blank employee ID reduces the quality of employee-ID-based naming and same-name collision handling.",
    "Blank work email makes email-based naming, write-back, and notification workflows harder to operate safely.",
    "Building username candidates from the sample payload...",
    "Candidate Count",
    "Candidate Order",
    "Claim Candidate Count",
    "Collision Template",
    "Connector",
    "Connector Ambiguity",
    "Connector Candidates",
    "Counts reflect unique source users merged across all returned department memberships.",
    "Custom Collision Template",
    "Custom Template",
    "Data quality snapshot failed.",
    "Default Connector",
    "Department Routing",
    "Department override forces primary department {department_id}.",
    "Departments",
    "Detected Issues",
    "Different users would generate the same primary managed AD username inside the same connector.",
    "Display Name",
    "Duplicate Emails",
    "Duplicate Employee IDs",
    "Duplicate employee IDs",
    "Duplicate work emails",
    "Effective Naming Preview",
    "Effective connector: {value}",
    "Email",
    "Email Local Part",
    "Employee ID",
    "Employee ID maps directly to an existing AD username",
    "Enter a source user ID before running the explainer.",
    "Error",
    "Existing AD Claim Candidate",
    "Existing AD email local part",
    "Existing AD employee ID",
    "Existing AD source user ID",
    "Existing Match",
    "Existing Match Behavior",
    "Existing binding",
    "Explanation failed.",
    "Fallback candidate appends a deterministic short hash suffix",
    "Fallback candidate appends a short numeric suffix",
    "Fallback candidate appends a stable three-digit sequence suffix",
    "Fallback candidate appends a stable two-digit sequence suffix",
    "Fallback candidate appends employee ID to separate users with the same base name",
    "Fallback candidate appends source user ID to avoid same-name collisions",
    "Fallback candidate uses employee ID directly for organizations that require unique staff numbers",
    "Fallback candidate uses the custom collision template for enterprise naming rules",
    "Fallback candidate uses the source email local part",
    "Fallback candidate uses the source user ID directly",
    "Fallback to source user ID because no managed naming candidate could be generated",
    "Family Name Pinyin",
    "Family Pinyin + Given Initials",
    "Family Pinyin + Given Pinyin",
    "First Sync Identity Claim",
    "Fix the source department assignment first, then rerun dry run.",
    "Full Pinyin",
    "Full Pinyin + Employee ID",
    "Generated at",
    "Given Initials",
    "Given Name Pinyin",
    "Healthy",
    "Info",
    "Managed",
    "Managed Candidate",
    "Managed username custom collision template",
    "Managed username deterministic hash suffix",
    "Managed username direct email local part",
    "Managed username direct employee ID",
    "Managed username direct source user ID",
    "Managed username employee ID suffix",
    "Managed username fallback source user ID",
    "Managed username numeric suffix",
    "Managed username primary",
    "Managed username source user ID suffix",
    "Managed username three-digit suffix",
    "Managed username two-digit suffix",
    "Manual bindings and per-user department overrides are not expanded in this snapshot.",
    "Map exact department only",
    "Map subtree",
    "Matches multiple connector scopes: {value}",
    "Missing Email",
    "Missing Employee ID",
    "Mobile",
    "Multiple connector matches",
    "Multiple source users share the same employee ID.",
    "Multiple source users share the same work email.",
    "Name",
    "Naming Gaps",
    "Needs Attention",
    "No employee ID was found on the source directory record.",
    "No existing-AD claim candidates were generated from the current source identity.",
    "No obvious source-data blockers were detected in this snapshot.",
    "No source department membership was returned for this user.",
    "No username candidate could be generated from the current sample payload.",
    "No valid department routing",
    "No valid source department membership was found for routing and OU placement.",
    "Pinyin Initials",
    "Pinyin Initials + Employee ID",
    "Placement Gaps",
    "Placement Strategy",
    "Pick the first valid department in source order",
    "Pick the lowest department ID",
    "Pick the shortest department path",
    "Position",
    "Predicted managed username collisions",
    "Prefer source primary department",
    "Preview failed.",
    "Primary managed username candidate generated from the selected naming strategy",
    "Queue existing match for review",
    "Recommended action",
    "Resolved Template",
    "Resolving connector scope, placement, and naming rules...",
    "Review Recommended",
    "Review existing AD matches first",
    "Review placement strategy, exclusion rules, and connector root-unit scope.",
    "Rule",
    "Runtime will use connector {connector} for this identity.",
    "Scanning source users, departments, and naming outcomes. This can take a little while on larger directories...",
    "Scope Root",
    "Scoped Path",
    "Selected Connector",
    "Shared by {value}",
    "Source User",
    "Source User ID",
    "Source email local part maps to an existing AD username",
    "Source user ID maps directly to an existing AD username",
    "Target Department",
    "Target OU Path",
    "Template Context",
    "The source directory request failed.",
    "These source records are missing fields required by the currently selected naming strategy.",
    "These users cannot be routed into the managed OU tree because the source directory does not expose a valid department membership.",
    "These users have departments, but the current placement policy excludes every candidate branch.",
    "These users span more than one connector scope, so runtime cannot choose a single provisioning target.",
    "This preview did not need any additional placeholder values beyond the fields you entered.",
    "This user currently spans multiple connector roots. Runtime would raise a connector-assignment conflict until the scope is simplified.",
    "Tune the username strategy or collision policy before running apply.",
    "User ID",
    "Username Collisions",
    "Users",
    "Users blocked by placement rules",
    "Users matching multiple connectors",
    "Users missing employee ID",
    "Users missing naming prerequisites",
    "Users missing work email",
    "Users without valid departments",
    "Warning",
    "Would be generated for {value}",
)


def register_advanced_sync_routes(
    app: FastAPI,
    *,
    build_source_data_quality_snapshot: Callable[[Request], dict[str, Any]],
    attribute_mapping_direction_labels: dict[str, str],
    build_username_preview: Callable[..., dict[str, Any]],
    describe_connector_config_source: Callable[[Any], str],
    explain_identity_routing: Callable[[Request, str], dict[str, Any]],
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    get_ui_language: Callable[[Request], str],
    list_org_attribute_mapping_rules: Callable[[Request], list[Any]],
    list_org_connector_records: Callable[[Request], list[Any]],
    normalize_mapping_direction: Callable[[str], str],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    split_csv_values: Callable[[str | None], list[str]],
    to_bool: Callable[[Optional[str], bool], bool],
    translate_text: Callable[..., str],
) -> None:
    def current_policy_config_fingerprint(
        request: Request,
        current_org: Any,
    ) -> str:
        repositories = get_web_repositories(request)
        try:
            return resolve_runtime_config_fingerprint(
                db_manager=repositories.db_manager,
                org_id=current_org.org_id,
                config_path=(
                    str(getattr(current_org, "config_path", "") or "")
                    or get_web_runtime_state(request).config_path
                ),
            )
        except Exception:
            # Keep policy and legacy jump pages available while connection
            # configuration is incomplete. An empty fingerprint intentionally
            # invalidates any historical Dry Run instead of treating it as safe.
            return ""

    def policy_release_context(request: Request, current_org: Any) -> dict[str, Any]:
        release = get_web_services(request).config.build_release_center_context(
            current_org=current_org,
        )
        repositories = get_web_repositories(request)
        runtime_state = get_web_runtime_state(request)
        config = repositories.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        source_snapshot = repositories.source_directory_repo.get_latest_successful_snapshot(
            org_id=current_org.org_id,
            provider_id=config.source_provider,
        )
        source_scope = repositories.source_directory_repo.get_scope_selection(
            org_id=current_org.org_id,
            provider_id=config.source_provider,
        )
        context = {
            "has_unpublished_changes": bool(release.get("has_unpublished_changes")),
            "latest_snapshot_title": str(release.get("latest_snapshot_title") or ""),
            "snapshot_count": int(release.get("snapshot_count") or 0),
            "latest_snapshot": release.get("latest_snapshot"),
            "source_snapshot": source_snapshot,
            "source_scope": source_scope,
            "source_departments": (
                repositories.source_directory_repo.list_departments(
                    int(source_snapshot["id"]),
                    org_id=current_org.org_id,
                )
                if source_snapshot
                else []
            ),
            "excluded_departments": list(config.exclude_departments),
            "protected_accounts": list(config.exclude_accounts),
        }
        context.update(
            build_policy_governance_context(
                repositories=repositories,
                current_org=current_org,
                provider_id=config.source_provider,
                snapshot=source_snapshot,
                scope=source_scope,
                current_config_fingerprint=current_policy_config_fingerprint(
                    request,
                    current_org,
                ),
                release=release,
            )
        )
        return context

    def policy_page_context(
        request: Request,
        *,
        page: str,
        title: str,
        selected_connector_id: str = "",
    ) -> dict[str, Any]:
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        connectors = list_org_connector_records(request)
        selected_connector = next(
            (
                record
                for record in connectors
                if record.connector_id == str(selected_connector_id or "").strip()
            ),
            None,
        )
        return {
            "page": page,
            "title": title,
            "current_org": current_org,
            "connectors": connectors,
            "selected_connector": selected_connector,
            "policy_settings": AdvancedSyncPolicySettings.load(
                repositories.settings_repo,
                org_id=current_org.org_id,
            ).to_dict(),
            "directory_policy_settings": DirectoryUiSettings.load(
                repositories.settings_repo,
                org_id=current_org.org_id,
            ).to_dict(),
            "protected_group_rules": [
                record
                for record in repositories.exclusion_repo.list_enabled_rule_records(
                    org_id=current_org.org_id,
                )
                if record.rule_type == "protect"
            ],
            "soft_excluded_group_rules": [
                record
                for record in repositories.exclusion_repo.list_enabled_rule_records(
                    org_id=current_org.org_id,
                )
                if record.rule_type == "exclude"
                and record.protection_level == "soft"
            ],
            **policy_release_context(request, current_org),
        }

    def policy_redirect_path(request: Request, canonical_path: str) -> str:
        return canonical_path if request.url.path.startswith("/sync-policies/") else "/advanced-sync"

    def persist_connector_policy(
        request: Request,
        *,
        connector_id: str,
        section: str,
        values: dict[str, Any],
    ) -> Any:
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        record = repositories.connector_repo.get_connector_record(
            str(connector_id or "").strip(),
            org_id=current_org.org_id,
        )
        if record is None:
            raise ValueError("Connector was not found in the selected organization")
        repositories.connector_repo.upsert_connector(
            **build_connector_policy_upsert(record, section, values)
        )
        return record

    def audit_policy_change(
        request: Request,
        *,
        user: Any,
        section: str,
        target_type: str,
        target_id: str,
        payload: dict[str, Any],
        action: str = "update",
    ) -> None:
        current_org = get_current_org(request)
        action_label = "Deleted" if action == "delete" else "Updated"
        get_web_repositories(request).audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type=f"sync_policy.{section}.{action}",
            target_type=target_type,
            target_id=target_id,
            result="success",
            message=f"{action_label} {section.replace('_', ' ')} sync policy",
            payload={"org_id": current_org.org_id, **payload},
        )

    def persist_policy_settings_section(
        request: Request,
        *,
        section: str,
        values: dict[str, Any],
    ) -> AdvancedSyncPolicySettings:
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        current = AdvancedSyncPolicySettings.load(
            repositories.settings_repo,
            org_id=current_org.org_id,
        )
        updated = update_policy_section(current, section, values)
        updated.persist(repositories.settings_repo, org_id=current_org.org_id)
        return updated

    def persist_directory_policy_settings_section(
        request: Request,
        *,
        section: str,
        values: dict[str, Any],
    ) -> DirectoryUiSettings:
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        current = DirectoryUiSettings.load(
            repositories.settings_repo,
            org_id=current_org.org_id,
        )
        updated = update_directory_policy_section(current, section, values)
        updated.persist(repositories.settings_repo, org_id=current_org.org_id)
        return updated

    @app.get(CANONICAL_ROUTE_PATHS["advanced-sync"], response_class=HTMLResponse)
    def sync_policy_landing(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        return RedirectResponse(url=CANONICAL_ROUTE_PATHS["sync-scope"], status_code=307)

    @app.get("/advanced-sync", response_class=HTMLResponse)
    def advanced_sync_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        return render(
            request,
            "sync_policy_legacy_entry.html",
            page="advanced-sync",
            title="Advanced Sync",
            current_org=current_org,
            **policy_release_context(request, current_org),
        )

    @app.get(CANONICAL_ROUTE_PATHS["sync-account-naming"], response_class=HTMLResponse)
    def sync_account_naming_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        runtime_state = get_web_runtime_state(request)
        config = repositories.org_config_repo.get_app_config(
            current_org.org_id,
            config_path=current_org.config_path or runtime_state.config_path,
        )
        source_snapshot = repositories.source_directory_repo.get_latest_successful_snapshot(
            org_id=current_org.org_id,
            provider_id=config.source_provider,
        )
        context = policy_page_context(
            request,
            page="sync-account-naming",
            title="Account Naming",
            selected_connector_id=str(request.query_params.get("connector_id") or ""),
        )
        return render(
            request,
            "sync_policy_account_naming.html",
            **context,
            base_account_policy=repositories.org_config_repo.get_editable_config(
                current_org.org_id,
                config_path=current_org.config_path or runtime_state.config_path,
            ),
            source_provider=config.source_provider,
            source_fields=(
                repositories.source_directory_repo.list_field_catalog(
                    int(source_snapshot["id"]),
                    org_id=current_org.org_id,
                )
                if source_snapshot
                else []
            ),
            employee_id_attribute=repositories.settings_repo.get_value(
                "source_employee_id_attribute",
                "",
                org_id=current_org.org_id,
            )
            or "",
            advanced_sync_client_i18n={
                key: translate_text(get_ui_language(request), key)
                for key in ADVANCED_SYNC_CLIENT_I18N_KEYS
            },
            username_strategy_options=[
                ("userid", "Source User ID"),
                ("email_localpart", "Email Local Part"),
                ("employee_id", "Employee ID"),
                ("pinyin_initials_employee_id", "Pinyin Initials + Employee ID"),
                ("pinyin_full_employee_id", "Full Pinyin + Employee ID"),
                ("family_name_pinyin_given_initials", "Family Pinyin + Given Initials"),
                ("family_name_pinyin_given_name_pinyin", "Family Pinyin + Given Pinyin"),
                ("custom_template", "Custom Template"),
            ],
            username_collision_policy_options=[
                ("append_employee_id", "Append Employee ID"),
                ("append_userid", "Append Source User ID"),
                ("append_numeric_counter", "Append Numeric Counter"),
                ("append_2digit_counter", "Append 2-Digit Sequence"),
                ("append_3digit_counter", "Append 3-Digit Sequence"),
                ("append_hash", "Append Deterministic Hash"),
                ("custom_template", "Custom Collision Template"),
            ],
        )

    @app.get(CANONICAL_ROUTE_PATHS["sync-attribute-mappings"], response_class=HTMLResponse)
    def sync_attribute_mappings_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        repositories.field_authority_rule_repo.seed_defaults(
            org_id=current_org.org_id
        )
        return render(
            request,
            "sync_policy_attribute_mappings.html",
            **policy_page_context(
                request,
                page="sync-attribute-mappings",
                title="Attribute Mappings",
            ),
            attribute_mappings=list_org_attribute_mapping_rules(request),
            mapping_direction_options=[
                ("source_to_ad", attribute_mapping_direction_labels["source_to_ad"]),
                ("ad_to_source", attribute_mapping_direction_labels["ad_to_source"]),
            ],
            mapping_direction_labels=attribute_mapping_direction_labels,
            mapping_mode_options=[(value, value) for value in ATTRIBUTE_SYNC_MODES],
            field_authority_rules=repositories.field_authority_rule_repo.list_rules(
                org_id=current_org.org_id
            ),
            field_authority_fields=(
                "employee_id",
                "display_name",
                "email",
                "mobile",
                "primary_department_id",
                "manager_account_id",
                "account_status",
            ),
            field_authority_providers=("dingtalk", "wecom", "feishu", "*"),
            field_authority_directions=(
                "source_to_ad",
                "ad_to_source",
                "bidirectional",
                "compare_only",
                "manual",
                "create_only",
            ),
            field_authority_modes=(
                "replace",
                "fill_if_empty",
                "preserve",
                "compare_only",
                "manual",
                "create_only",
            ),
        )

    @app.get(CANONICAL_ROUTE_PATHS["sync-department-ou-routing"], response_class=HTMLResponse)
    def sync_department_ou_routing_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        selected_connector_id = str(
            request.query_params.get("connector_id") or "default"
        ).strip() or "default"
        latest_ad_snapshot = (
            repositories.ad_directory_snapshot_repo.get_latest_successful_snapshot(
                org_id=current_org.org_id,
                connector_id=selected_connector_id,
            )
        )
        return render(
            request,
            "sync_policy_department_ou_routing.html",
            **policy_page_context(
                request,
                page="sync-department-ou-routing",
                title="Department & OU Routing",
            ),
            department_ou_mappings=repositories.department_ou_mapping_repo.list_mapping_records(
                org_id=current_org.org_id
            ),
            department_ou_apply_mode_options=[
                ("subtree", "Map subtree"),
                ("exact", "Map exact department only"),
            ],
            selected_connector_id=selected_connector_id,
            latest_ad_snapshot=dict(latest_ad_snapshot) if latest_ad_snapshot else None,
            ad_ou_nodes=(
                repositories.ad_directory_snapshot_repo.list_ous(
                    int(latest_ad_snapshot["id"]),
                    org_id=current_org.org_id,
                )
                if latest_ad_snapshot
                else []
            ),
        )

    @app.get(CANONICAL_ROUTE_PATHS["sync-group-rules"], response_class=HTMLResponse)
    def sync_group_rules_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        return render(
            request,
            "sync_policy_group_rules.html",
            **policy_page_context(
                request,
                page="sync-group-rules",
                title="Group Rules",
                selected_connector_id=str(request.query_params.get("connector_id") or ""),
            ),
            group_type_options=[
                (value, value.replace("_", " ").title()) for value in MANAGED_GROUP_TYPES
            ],
            custom_group_bindings=get_web_repositories(
                request
            ).custom_group_binding_repo.list_active_records(org_id=current_org.org_id),
        )

    @app.get(CANONICAL_ROUTE_PATHS["sync-lifecycle-policy"], response_class=HTMLResponse)
    def sync_lifecycle_policy_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        return render(
            request,
            "sync_policy_lifecycle.html",
            **policy_page_context(
                request,
                page="sync-lifecycle-policy",
                title="Lifecycle & Security",
                selected_connector_id=str(request.query_params.get("connector_id") or ""),
            ),
            first_sync_identity_claim_mode_options=[
                ("auto_safe", "Auto-claim safe existing AD matches"),
                ("review", "Review existing AD matches first"),
            ],
        )

    @app.get(CANONICAL_ROUTE_PATHS["sync-security-policy"], response_class=HTMLResponse)
    def sync_security_policy_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        return render(
            request,
            "sync_policy_security.html",
            **policy_page_context(
                request,
                page="sync-security-policy",
                title="Security Policy",
            ),
        )

    @app.post(CANONICAL_ROUTE_PATHS["sync-account-naming"] + "/preview")
    @app.post("/advanced-sync/username-preview")
    def advanced_sync_username_preview(
        request: Request,
        connector_id: str = Form("default"),
        sample_userid: str = Form(""),
        sample_name: str = Form(""),
        sample_email: str = Form(""),
        sample_employee_id: str = Form(""),
        sample_position: str = Form(""),
        sample_mobile: str = Form(""),
        sample_payload_json: str = Form(""),
    ):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
        try:
            preview = build_username_preview(
                request,
                connector_id=connector_id,
                sample_userid=sample_userid,
                sample_name=sample_name,
                sample_email=sample_email,
                sample_employee_id=sample_employee_id,
                sample_position=sample_position,
                sample_mobile=sample_mobile,
                sample_payload_json=sample_payload_json,
            )
        except ValueError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
        except Exception as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
        return JSONResponse({"ok": True, "preview": preview})

    @app.get("/advanced-sync/identity-explain")
    def advanced_sync_identity_explain(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
        source_user_id = str(request.query_params.get("user_id") or "").strip()
        if not source_user_id:
            return JSONResponse({"ok": False, "error": "Source user ID is required."}, status_code=400)
        try:
            explanation = explain_identity_routing(request, source_user_id)
        except ValueError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
        except Exception as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
        return JSONResponse({"ok": True, "explanation": explanation})

    @app.get("/advanced-sync/data-quality-snapshot")
    def advanced_sync_data_quality_snapshot(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
        try:
            snapshot = build_source_data_quality_snapshot(request)
        except ValueError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
        except Exception as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
        return JSONResponse({"ok": True, "snapshot": snapshot})

    @app.post(CANONICAL_ROUTE_PATHS["sync-account-naming"])
    def sync_account_naming_submit(
        request: Request,
        csrf_token: str = Form(""),
        policy_target: str = Form("source"),
        connector_id: str = Form(""),
        username_strategy: str = Form("userid"),
        username_template: str = Form(""),
        username_collision_policy: str = Form("append_employee_id"),
        username_collision_template: str = Form(""),
        employee_id_attribute: str = Form(""),
        source_field: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = CANONICAL_ROUTE_PATHS["sync-account-naming"]
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        normalized_target = str(policy_target or "source").strip().lower()
        normalized_source_field = (
            str(source_field or "").strip() if isinstance(source_field, str) else ""
        )
        normalized_strategy = normalize_username_strategy(username_strategy)
        normalized_collision = normalize_username_collision_policy(username_collision_policy)
        try:
            if normalized_target == "source":
                runtime_state = get_web_runtime_state(request)
                config = repositories.org_config_repo.get_app_config(
                    current_org.org_id,
                    config_path=current_org.config_path or runtime_state.config_path,
                )
                scope = repositories.source_directory_repo.get_scope_selection(
                    org_id=current_org.org_id,
                    provider_id=config.source_provider,
                )
                if not scope:
                    raise ValueError("Save synchronization scope before configuring source naming")
                if normalized_source_field:
                    normalized_strategy = USERNAME_STRATEGY_BY_SOURCE_FIELD.get(
                        normalized_source_field,
                        "custom_template",
                    )
                source_field_by_strategy = {
                    strategy: field
                    for field, strategy in USERNAME_STRATEGY_BY_SOURCE_FIELD.items()
                }
                normalized_template = str(username_template or "").strip()
                if (
                    normalized_source_field
                    and normalized_source_field
                    not in USERNAME_STRATEGY_BY_SOURCE_FIELD
                ):
                    normalized_template = "{" + normalized_source_field + "}"
                repositories.source_directory_repo.save_scope_selection(
                    org_id=current_org.org_id,
                    provider_id=config.source_provider,
                    connector_id=str(scope.get("connector_id") or "default"),
                    scope_type=str(scope.get("scope_type") or "full"),
                    selected_department_ids=scope.get("selected_department_ids") or (),
                    selected_source_user_ids=scope.get("selected_source_user_ids") or (),
                    username_strategy=normalized_strategy,
                    username_template=normalized_template,
                    source_field=(
                        normalized_source_field
                        or source_field_by_strategy[normalized_strategy]
                    ),
                    snapshot_id=int(scope.get("snapshot_id") or 0) or None,
                    requested_by=user.username,
                )
                repositories.settings_repo.set_value(
                    "source_employee_id_attribute",
                    str(employee_id_attribute or "").strip(),
                    "string",
                    org_id=current_org.org_id,
                )
                target_id = config.source_provider
            elif normalized_target == "connector":
                normalized_connector_id = str(connector_id or "").strip()
                persist_connector_policy(
                    request,
                    connector_id=normalized_connector_id,
                    section="account_naming",
                    values={
                        "username_strategy": normalized_strategy,
                        "username_template": str(username_template or "").strip(),
                        "username_collision_policy": normalized_collision,
                        "username_collision_template": str(
                            username_collision_template or ""
                        ).strip(),
                    },
                )
                target_id = normalized_connector_id
            else:
                raise ValueError("Unsupported account naming target")
        except (KeyError, TypeError, ValueError) as exc:
            flash_t(request, "error", "Failed to save account naming: {error}", error=str(exc))
            return RedirectResponse(url=redirect_url, status_code=303)
        audit_policy_change(
            request,
            user=user,
            section="account_naming",
            target_type="source_provider" if normalized_target == "source" else "connector",
            target_id=target_id,
            payload={
                "policy_target": normalized_target,
                "connector_id": str(connector_id or "").strip(),
                "username_strategy": normalized_strategy,
                "username_collision_policy": normalized_collision,
            },
        )
        flash(
            request,
            "success",
            "Account naming policy saved. The previous Dry Run is now invalid; run and review a new Dry Run before Apply.",
        )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post(CANONICAL_ROUTE_PATHS["sync-account-naming"] + "/account-creation")
    def sync_account_creation_submit(
        request: Request,
        csrf_token: str = Form(""),
        connector_id: str = Form(""),
        default_password: str = Form(""),
        force_change_password: str = Form("true"),
        password_complexity: str = Form("strong"),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        normalized_connector_id = (
            str(connector_id or "").strip()
            if isinstance(connector_id, str)
            else ""
        )
        redirect_url = CANONICAL_ROUTE_PATHS["sync-account-naming"]
        if normalized_connector_id:
            redirect_url = f"{redirect_url}?connector_id={normalized_connector_id}"
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        password_was_updated = bool(str(default_password or "").strip())
        normalized_force_change = to_bool(force_change_password, True)
        normalized_complexity = str(password_complexity or "strong").strip() or "strong"
        try:
            if normalized_connector_id:
                connector_values: dict[str, Any] = {
                    "force_change_password": normalized_force_change,
                    "password_complexity": normalized_complexity,
                }
                if password_was_updated:
                    connector_values["default_password"] = str(default_password).strip()
                persist_connector_policy(
                    request,
                    connector_id=normalized_connector_id,
                    section="account_creation",
                    values=connector_values,
                )
            else:
                runtime_state = get_web_runtime_state(request)
                config_path = current_org.config_path or runtime_state.config_path
                current_values = repositories.org_config_repo.get_raw_config(
                    current_org.org_id,
                    config_path=config_path,
                )
                repositories.org_config_repo.save_config(
                    current_org.org_id,
                    {
                        **current_values,
                        "default_password": (
                            str(default_password).strip()
                            if password_was_updated
                            else current_values.get("default_password", "")
                        ),
                        "force_change_password": normalized_force_change,
                        "password_complexity": normalized_complexity,
                    },
                    config_path=config_path,
                )
        except (TypeError, ValueError) as exc:
            flash_t(
                request,
                "error",
                "Failed to save account creation policy: {error}",
                error=str(exc),
            )
            return RedirectResponse(url=redirect_url, status_code=303)

        audit_policy_change(
            request,
            user=user,
            section="account_creation",
            target_type="connector" if normalized_connector_id else "settings",
            target_id=normalized_connector_id or "organization_default",
            payload={
                "connector_id": normalized_connector_id,
                "default_password_updated": password_was_updated,
                "force_change_password": normalized_force_change,
                "password_complexity": normalized_complexity,
            },
        )
        flash(
            request,
            "success",
            "Account creation policy saved. The previous Dry Run is now invalid; run and review a new Dry Run before Apply.",
        )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post(CANONICAL_ROUTE_PATHS["sync-attribute-mappings"])
    def sync_attribute_mapping_submit(
        request: Request,
        csrf_token: str = Form(""),
        attribute_mapping_enabled: Optional[str] = Form(None),
        write_back_enabled: Optional[str] = Form(None),
        connector_id: str = Form(""),
        direction: str = Form("source_to_ad"),
        source_field: str = Form(""),
        target_field: str = Form(""),
        transform_template: str = Form(""),
        sync_mode: str = Form("replace"),
        notes: str = Form(""),
        is_enabled: Optional[str] = Form(None),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = CANONICAL_ROUTE_PATHS["sync-attribute-mappings"]
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        normalized_connector_id = str(connector_id or "").strip()
        normalized_source_field = str(source_field or "").strip()
        normalized_target_field = str(target_field or "").strip()
        if bool(normalized_source_field) != bool(normalized_target_field):
            flash_t(
                request,
                "error",
                "Failed to save mapping rule: {error}",
                error="Both source and target fields are required",
            )
            return RedirectResponse(url=redirect_url, status_code=303)
        if normalized_connector_id and not repositories.connector_repo.get_connector_record(
            normalized_connector_id,
            org_id=current_org.org_id,
        ):
            flash_t(
                request,
                "error",
                "Failed to save mapping rule: {error}",
                error="Connector was not found in the selected organization",
            )
            return RedirectResponse(url=redirect_url, status_code=303)
        try:
            updated = persist_policy_settings_section(
                request,
                section="attribute_mappings",
                values={
                    "attribute_mapping_enabled": to_bool(attribute_mapping_enabled, False),
                    "write_back_enabled": to_bool(write_back_enabled, False),
                },
            )
            if normalized_source_field:
                repositories.attribute_mapping_repo.upsert_rule(
                    connector_id=normalized_connector_id,
                    direction=normalize_mapping_direction(direction),
                    source_field=normalized_source_field,
                    target_field=normalized_target_field,
                    transform_template=str(transform_template or "").strip(),
                    sync_mode=str(sync_mode or "replace").strip(),
                    notes=str(notes or "").strip(),
                    is_enabled=to_bool(is_enabled, True),
                    org_id=current_org.org_id,
                )
        except (TypeError, ValueError) as exc:
            flash_t(request, "error", "Failed to save mapping rule: {error}", error=str(exc))
            return RedirectResponse(url=redirect_url, status_code=303)
        audit_policy_change(
            request,
            user=user,
            section="attribute_mappings",
            target_type="attribute_mapping_rule",
            target_id=(
                f"{normalized_connector_id or 'global'}:{str(source_field or '').strip()}"
                f"->{str(target_field or '').strip()}"
            ),
            payload={
                "attribute_mapping_enabled": updated.attribute_mapping_enabled,
                "write_back_enabled": updated.write_back_enabled,
                "connector_id": normalized_connector_id,
                "direction": normalize_mapping_direction(direction),
                "source_field": str(source_field or "").strip(),
                "target_field": str(target_field or "").strip(),
            },
        )
        flash(
            request,
            "success",
            "Attribute mapping policy saved. The previous Dry Run is now invalid; run and review a new Dry Run before Apply.",
        )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post(CANONICAL_ROUTE_PATHS["sync-attribute-mappings"] + "/field-authority")
    def field_authority_rule_submit(
        request: Request,
        csrf_token: str = Form(""),
        field_name: str = Form(""),
        source_provider: str = Form("*"),
        source_priority: int = Form(100),
        sync_direction: str = Form("source_to_ad"),
        sync_mode: str = Form("replace"),
        prevent_loop: Optional[str] = Form(None),
        is_enabled: Optional[str] = Form(None),
        notes: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = CANONICAL_ROUTE_PATHS["sync-attribute-mappings"]
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        normalized_field_name = str(field_name or "").strip()
        normalized_provider = str(source_provider or "*").strip().lower() or "*"
        try:
            rule = repositories.field_authority_rule_repo.upsert_rule(
                org_id=current_org.org_id,
                field_name=normalized_field_name,
                source_provider=normalized_provider,
                source_priority=int(source_priority),
                sync_direction=str(sync_direction or "source_to_ad"),
                sync_mode=str(sync_mode or "replace"),
                prevent_loop=to_bool(prevent_loop, True),
                is_enabled=to_bool(is_enabled, True),
                notes=str(notes or "").strip(),
                created_by=user.username,
            )
        except (TypeError, ValueError) as exc:
            flash_t(
                request,
                "error",
                "Failed to save field authority rule: {error}",
                error=str(exc),
            )
            return RedirectResponse(url=redirect_url, status_code=303)
        audit_policy_change(
            request,
            user=user,
            section="field_authority",
            target_type="field_authority_rule",
            target_id=f"{normalized_field_name}:{normalized_provider}",
            payload={
                "field_name": rule.field_name,
                "source_provider": rule.source_provider,
                "source_priority": rule.source_priority,
                "sync_direction": rule.sync_direction,
                "sync_mode": rule.sync_mode,
                "prevent_loop": rule.prevent_loop,
                "is_enabled": rule.is_enabled,
                "rule_revision": rule.rule_revision,
            },
        )
        flash(
            request,
            "success",
            "Field authority rule saved. The previous Dry Run is now invalid; run and review a new Dry Run before Apply.",
        )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post(CANONICAL_ROUTE_PATHS["sync-group-rules"])
    def sync_group_rules_submit(
        request: Request,
        csrf_token: str = Form(""),
        custom_group_sync_enabled: Optional[str] = Form(None),
        managed_group_type: str = Form("security"),
        managed_group_mail_domain: str = Form(""),
        custom_group_ou_path: str = Form("Managed Groups"),
        connector_id: str = Form(""),
        connector_group_type: str = Form("security"),
        connector_group_mail_domain: str = Form(""),
        connector_custom_group_ou_path: str = Form("Managed Groups"),
        managed_tag_ids: str = Form(""),
        managed_external_chat_ids: str = Form(""),
        group_display_separator: str | None = Form(None),
        group_recursive_enabled: str | None = Form(None),
        managed_relation_cleanup_enabled: str | None = Form(None),
        soft_excluded_groups: str | None = Form(None),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = CANONICAL_ROUTE_PATHS["sync-group-rules"]
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        normalized_connector_id = str(connector_id or "").strip()
        if normalized_connector_id and not get_web_repositories(
            request
        ).connector_repo.get_connector_record(
            normalized_connector_id,
            org_id=get_current_org(request).org_id,
        ):
            flash_t(
                request,
                "error",
                "Failed to save group rules: {error}",
                error="Connector was not found in the selected organization",
            )
            return RedirectResponse(url=redirect_url, status_code=303)
        updated = persist_policy_settings_section(
            request,
            section="group_rules",
            values={
                "custom_group_sync_enabled": to_bool(custom_group_sync_enabled, False),
                "managed_group_type": managed_group_type,
                "managed_group_mail_domain": managed_group_mail_domain,
                "custom_group_ou_path": custom_group_ou_path,
            },
        )
        directory_values: dict[str, Any] = {}
        if isinstance(group_display_separator, str):
            directory_values["group_display_separator"] = str(
                group_display_separator or "-"
            )
        if isinstance(group_recursive_enabled, str):
            directory_values["group_recursive_enabled"] = to_bool(
                group_recursive_enabled,
                True,
            )
        if isinstance(managed_relation_cleanup_enabled, str):
            directory_values["managed_relation_cleanup_enabled"] = to_bool(
                managed_relation_cleanup_enabled,
                False,
            )
        directory_values["custom_group_ou_path"] = str(
            custom_group_ou_path or "Managed Groups"
        ).strip()
        directory_settings = persist_directory_policy_settings_section(
            request,
            section="group_rules",
            values=directory_values,
        )
        try:
            if normalized_connector_id:
                persist_connector_policy(
                    request,
                    connector_id=normalized_connector_id,
                    section="group_rules",
                    values={
                        "group_type": str(connector_group_type or "security").strip(),
                        "group_mail_domain": str(connector_group_mail_domain or "").strip(),
                        "custom_group_ou_path": str(
                            connector_custom_group_ou_path or ""
                        ).strip(),
                        "managed_tag_ids": split_csv_values(managed_tag_ids),
                        "managed_external_chat_ids": split_csv_values(
                            managed_external_chat_ids
                        ),
                    },
                )
            if isinstance(soft_excluded_groups, str):
                get_web_repositories(request).exclusion_repo.replace_soft_excluded_rules(
                    (
                        {
                            "match_value": line.strip(),
                            "display_name": line.strip(),
                            "is_enabled": True,
                            "source": "sync_policy",
                        }
                        for line in str(soft_excluded_groups or "").splitlines()
                        if line.strip()
                    ),
                    org_id=get_current_org(request).org_id,
                )
        except (TypeError, ValueError) as exc:
            flash_t(request, "error", "Failed to save group rules: {error}", error=str(exc))
            return RedirectResponse(url=redirect_url, status_code=303)
        audit_policy_change(
            request,
            user=user,
            section="group_rules",
            target_type="connector" if normalized_connector_id else "settings",
            target_id=normalized_connector_id or "group_rules",
            payload={
                "custom_group_sync_enabled": updated.custom_group_sync_enabled,
                "managed_group_type": updated.managed_group_type,
                "connector_id": normalized_connector_id,
                "managed_tag_ids": split_csv_values(managed_tag_ids),
                "managed_external_chat_ids": split_csv_values(managed_external_chat_ids),
                "group_recursive_enabled": directory_settings.group_recursive_enabled,
                "managed_relation_cleanup_enabled": (
                    directory_settings.managed_relation_cleanup_enabled
                ),
            },
        )
        flash(
            request,
            "success",
            "Group rules saved. The previous Dry Run is now invalid; run and review a new Dry Run before Apply.",
        )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post(CANONICAL_ROUTE_PATHS["sync-lifecycle-policy"])
    def sync_lifecycle_policy_submit(
        request: Request,
        csrf_token: str = Form(""),
        offboarding_grace_days: int = Form(0),
        offboarding_notify_managers: Optional[str] = Form(None),
        offboarding_lifecycle_enabled: Optional[str] = Form(None),
        rehire_restore_enabled: Optional[str] = Form(None),
        automatic_replay_enabled: Optional[str] = Form(None),
        future_onboarding_enabled: Optional[str] = Form(None),
        future_onboarding_start_field: str = Form("hire_date"),
        contractor_lifecycle_enabled: Optional[str] = Form(None),
        lifecycle_employment_type_field: str = Form("employment_type"),
        contractor_end_field: str = Form("contract_end_date"),
        lifecycle_sponsor_field: str = Form("sponsor_userid"),
        contractor_type_values: str = Form("contractor,intern,vendor,temp"),
        connector_id: str = Form(""),
        disabled_users_ou: str = Form("Disabled Users"),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = CANONICAL_ROUTE_PATHS["sync-lifecycle-policy"]
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        normalized_connector_id = str(connector_id or "").strip()
        if normalized_connector_id and not get_web_repositories(
            request
        ).connector_repo.get_connector_record(
            normalized_connector_id,
            org_id=get_current_org(request).org_id,
        ):
            flash_t(
                request,
                "error",
                "Failed to save lifecycle policy: {error}",
                error="Connector was not found in the selected organization",
            )
            return RedirectResponse(url=redirect_url, status_code=303)
        updated = persist_policy_settings_section(
            request,
            section="lifecycle",
            values={
                "offboarding_grace_days": offboarding_grace_days,
                "offboarding_notify_managers": to_bool(offboarding_notify_managers, False),
                "offboarding_lifecycle_enabled": to_bool(offboarding_lifecycle_enabled, False),
                "rehire_restore_enabled": to_bool(rehire_restore_enabled, False),
                "automatic_replay_enabled": to_bool(automatic_replay_enabled, False),
                "future_onboarding_enabled": to_bool(future_onboarding_enabled, False),
                "future_onboarding_start_field": future_onboarding_start_field,
                "contractor_lifecycle_enabled": to_bool(contractor_lifecycle_enabled, False),
                "lifecycle_employment_type_field": lifecycle_employment_type_field,
                "contractor_end_field": contractor_end_field,
                "lifecycle_sponsor_field": lifecycle_sponsor_field,
                "contractor_type_values": contractor_type_values,
            },
        )
        try:
            if normalized_connector_id:
                persist_connector_policy(
                    request,
                    connector_id=normalized_connector_id,
                    section="lifecycle",
                    values={"disabled_users_ou": str(disabled_users_ou or "").strip()},
                )
        except (TypeError, ValueError) as exc:
            flash_t(request, "error", "Failed to save lifecycle policy: {error}", error=str(exc))
            return RedirectResponse(url=redirect_url, status_code=303)
        audit_policy_change(
            request,
            user=user,
            section="lifecycle",
            target_type="connector" if normalized_connector_id else "settings",
            target_id=normalized_connector_id or "lifecycle",
            payload={
                "offboarding_lifecycle_enabled": updated.offboarding_lifecycle_enabled,
                "future_onboarding_enabled": updated.future_onboarding_enabled,
                "contractor_lifecycle_enabled": updated.contractor_lifecycle_enabled,
                "connector_id": normalized_connector_id,
            },
        )
        flash(
            request,
            "success",
            "Lifecycle policy saved. The previous Dry Run is now invalid; run and review a new Dry Run before Apply.",
        )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post(CANONICAL_ROUTE_PATHS["sync-security-policy"])
    def sync_security_policy_submit(
        request: Request,
        csrf_token: str = Form(""),
        advanced_connector_routing_enabled: Optional[str] = Form(None),
        disable_circuit_breaker_enabled: Optional[str] = Form(None),
        disable_circuit_breaker_percent: float = Form(5.0),
        disable_circuit_breaker_min_count: int = Form(10),
        disable_circuit_breaker_requires_approval: Optional[str] = Form(None),
        first_sync_identity_claim_mode: str = Form("auto_safe"),
        connector_id: str = Form(""),
        force_change_password: str = Form(""),
        password_complexity: str = Form(""),
        return_to: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = (
            CANONICAL_ROUTE_PATHS["sync-lifecycle-policy"] + "#security"
            if isinstance(return_to, str) and return_to.strip() == "lifecycle"
            else CANONICAL_ROUTE_PATHS["sync-security-policy"]
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        normalized_connector_id = str(connector_id or "").strip()
        if normalized_connector_id and not get_web_repositories(
            request
        ).connector_repo.get_connector_record(
            normalized_connector_id,
            org_id=get_current_org(request).org_id,
        ):
            flash_t(
                request,
                "error",
                "Failed to save security policy: {error}",
                error="Connector was not found in the selected organization",
            )
            return RedirectResponse(url=redirect_url, status_code=303)
        updated = persist_policy_settings_section(
            request,
            section="security",
            values={
                "advanced_connector_routing_enabled": to_bool(
                    advanced_connector_routing_enabled, False
                ),
                "disable_circuit_breaker_enabled": to_bool(
                    disable_circuit_breaker_enabled, False
                ),
                "disable_circuit_breaker_percent": disable_circuit_breaker_percent,
                "disable_circuit_breaker_min_count": disable_circuit_breaker_min_count,
                "disable_circuit_breaker_requires_approval": to_bool(
                    disable_circuit_breaker_requires_approval, False
                ),
                "first_sync_identity_claim_mode": first_sync_identity_claim_mode,
            },
        )
        try:
            if normalized_connector_id:
                connector_values: dict[str, Any] = {
                    "password_complexity": str(password_complexity or "").strip()
                }
                if str(force_change_password or "").strip().lower() in {"true", "false"}:
                    connector_values["force_change_password"] = (
                        str(force_change_password).strip().lower() == "true"
                    )
                persist_connector_policy(
                    request,
                    connector_id=normalized_connector_id,
                    section="security",
                    values=connector_values,
                )
        except (TypeError, ValueError) as exc:
            flash_t(request, "error", "Failed to save security policy: {error}", error=str(exc))
            return RedirectResponse(url=redirect_url, status_code=303)
        audit_policy_change(
            request,
            user=user,
            section="security",
            target_type="connector" if normalized_connector_id else "settings",
            target_id=normalized_connector_id or "security",
            payload={
                "advanced_connector_routing_enabled": updated.advanced_connector_routing_enabled,
                "disable_circuit_breaker_enabled": updated.disable_circuit_breaker_enabled,
                "disable_circuit_breaker_percent": updated.disable_circuit_breaker_percent,
                "disable_circuit_breaker_min_count": updated.disable_circuit_breaker_min_count,
                "disable_circuit_breaker_requires_approval": updated.disable_circuit_breaker_requires_approval,
                "first_sync_identity_claim_mode": updated.first_sync_identity_claim_mode,
                "connector_id": normalized_connector_id,
            },
        )
        flash(
            request,
            "success",
            "Security policy saved. The previous Dry Run is now invalid; run and review a new Dry Run before Apply.",
        )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post("/advanced-sync/policies")
    def advanced_sync_policy_submit(
        request: Request,
        csrf_token: str = Form(""),
        offboarding_grace_days: int = Form(0),
        offboarding_notify_managers: Optional[str] = Form(None),
        advanced_connector_routing_enabled: Optional[str] = Form(None),
        attribute_mapping_enabled: Optional[str] = Form(None),
        write_back_enabled: Optional[str] = Form(None),
        custom_group_sync_enabled: Optional[str] = Form(None),
        offboarding_lifecycle_enabled: Optional[str] = Form(None),
        rehire_restore_enabled: Optional[str] = Form(None),
        automatic_replay_enabled: Optional[str] = Form(None),
        future_onboarding_enabled: Optional[str] = Form(None),
        future_onboarding_start_field: str = Form("hire_date"),
        contractor_lifecycle_enabled: Optional[str] = Form(None),
        lifecycle_employment_type_field: str = Form("employment_type"),
        contractor_end_field: str = Form("contract_end_date"),
        lifecycle_sponsor_field: str = Form("sponsor_userid"),
        contractor_type_values: str = Form("contractor,intern,vendor,temp"),
        disable_circuit_breaker_enabled: Optional[str] = Form(None),
        disable_circuit_breaker_percent: float = Form(5.0),
        disable_circuit_breaker_min_count: int = Form(10),
        disable_circuit_breaker_requires_approval: Optional[str] = Form(None),
        first_sync_identity_claim_mode: str = Form("auto_safe"),
        managed_group_type: str = Form("security"),
        managed_group_mail_domain: str = Form(""),
        custom_group_ou_path: str = Form("Managed Groups"),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error

        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        policy_settings = AdvancedSyncPolicySettings.from_mapping(
            {
                "offboarding_grace_days": offboarding_grace_days,
                "offboarding_notify_managers": to_bool(offboarding_notify_managers, False),
                "advanced_connector_routing_enabled": to_bool(advanced_connector_routing_enabled, False),
                "attribute_mapping_enabled": to_bool(attribute_mapping_enabled, False),
                "write_back_enabled": to_bool(write_back_enabled, False),
                "custom_group_sync_enabled": to_bool(custom_group_sync_enabled, False),
                "offboarding_lifecycle_enabled": to_bool(offboarding_lifecycle_enabled, False),
                "rehire_restore_enabled": to_bool(rehire_restore_enabled, False),
                "automatic_replay_enabled": to_bool(automatic_replay_enabled, False),
                "future_onboarding_enabled": to_bool(future_onboarding_enabled, False),
                "future_onboarding_start_field": future_onboarding_start_field,
                "contractor_lifecycle_enabled": to_bool(contractor_lifecycle_enabled, False),
                "lifecycle_employment_type_field": lifecycle_employment_type_field,
                "contractor_end_field": contractor_end_field,
                "lifecycle_sponsor_field": lifecycle_sponsor_field,
                "contractor_type_values": contractor_type_values,
                "disable_circuit_breaker_enabled": to_bool(disable_circuit_breaker_enabled, False),
                "disable_circuit_breaker_percent": disable_circuit_breaker_percent,
                "disable_circuit_breaker_min_count": disable_circuit_breaker_min_count,
                "disable_circuit_breaker_requires_approval": to_bool(
                    disable_circuit_breaker_requires_approval,
                    False,
                ),
                "first_sync_identity_claim_mode": first_sync_identity_claim_mode,
                "managed_group_type": managed_group_type,
                "managed_group_mail_domain": managed_group_mail_domain,
                "custom_group_ou_path": custom_group_ou_path,
            }
        )
        policy_settings.persist(repositories.settings_repo, org_id=current_org.org_id)
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="sync_policy.legacy.update",
            target_type="settings",
            target_id="advanced_sync",
            result="success",
            message="Updated advanced sync policies",
            payload={
                "org_id": current_org.org_id,
                **policy_settings.to_dict(),
            },
        )
        flash(
            request,
            "success",
            "Legacy policy values saved. The previous Dry Run is now invalid; use Sync Policies and run a new Dry Run before Apply.",
        )
        return RedirectResponse(url="/advanced-sync", status_code=303)

    @app.post("/advanced-sync/connectors")
    def advanced_sync_connector_submit(
        request: Request,
        csrf_token: str = Form(""),
        connector_id: str = Form(""),
        name: str = Form(""),
        config_path: str = Form(""),
        ldap_server: str = Form(""),
        ldap_domain: str = Form(""),
        ldap_username: str = Form(""),
        ldap_password: str = Form(""),
        ldap_use_ssl: str = Form(""),
        ldap_port: str = Form(""),
        ldap_validate_cert: str = Form(""),
        ldap_ca_cert_path: str = Form(""),
        default_password: str = Form(""),
        force_change_password: str = Form(""),
        password_complexity: str = Form(""),
        root_department_ids: str = Form(""),
        username_strategy: str = Form("custom_template"),
        username_collision_policy: str = Form("append_employee_id"),
        username_collision_template: str = Form(""),
        username_template: str = Form(""),
        disabled_users_ou: str = Form("Disabled Users"),
        group_type: str = Form("security"),
        group_mail_domain: str = Form(""),
        custom_group_ou_path: str = Form("Managed Groups"),
        managed_tag_ids: str = Form(""),
        managed_external_chat_ids: str = Form(""),
        is_enabled: Optional[str] = Form(None),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        try:
            repositories.connector_repo.upsert_connector(
                connector_id=connector_id.strip(),
                org_id=current_org.org_id,
                name=name.strip() or connector_id.strip(),
                config_path=config_path.strip(),
                ldap_server=ldap_server.strip(),
                ldap_domain=ldap_domain.strip(),
                ldap_username=ldap_username.strip(),
                ldap_password=ldap_password.strip(),
                ldap_use_ssl=ldap_use_ssl.strip(),
                ldap_port=ldap_port.strip(),
                ldap_validate_cert=ldap_validate_cert.strip(),
                ldap_ca_cert_path=ldap_ca_cert_path.strip(),
                default_password=default_password.strip(),
                force_change_password=force_change_password.strip(),
                password_complexity=password_complexity.strip(),
                root_department_ids=[int(item) for item in split_csv_values(root_department_ids)],
                username_strategy=normalize_username_strategy(username_strategy),
                username_collision_policy=normalize_username_collision_policy(username_collision_policy),
                username_collision_template=username_collision_template.strip(),
                username_template=username_template.strip(),
                disabled_users_ou=disabled_users_ou.strip(),
                group_type=group_type.strip(),
                group_mail_domain=group_mail_domain.strip(),
                custom_group_ou_path=custom_group_ou_path.strip(),
                managed_tag_ids=split_csv_values(managed_tag_ids),
                managed_external_chat_ids=split_csv_values(managed_external_chat_ids),
                is_enabled=to_bool(is_enabled, True),
            )
        except Exception as exc:
            flash_t(request, "error", "Failed to save connector: {error}", error=str(exc))
            return RedirectResponse(url="/advanced-sync", status_code=303)
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="advanced_sync.connector_upsert",
            target_type="connector",
            target_id=connector_id.strip(),
            result="success",
            message="Saved connector definition",
            payload={
                "org_id": current_org.org_id,
                "root_department_ids": split_csv_values(root_department_ids),
                "legacy_import_path": config_path.strip(),
                "ldap_server": ldap_server.strip(),
                "ldap_domain": ldap_domain.strip(),
                "has_database_overrides": any(
                    [
                        ldap_server.strip(),
                        ldap_domain.strip(),
                        ldap_username.strip(),
                        ldap_password.strip(),
                        ldap_use_ssl.strip(),
                        ldap_port.strip(),
                        ldap_validate_cert.strip(),
                        ldap_ca_cert_path.strip(),
                        default_password.strip(),
                        force_change_password.strip(),
                        password_complexity.strip(),
                    ]
                ),
            },
        )
        flash_t(request, "success", "Connector {connector_id} saved", connector_id=connector_id.strip())
        return RedirectResponse(url="/advanced-sync", status_code=303)

    @app.post(CANONICAL_ROUTE_PATHS["sync-department-ou-routing"] + "/defaults")
    def sync_department_ou_defaults_submit(
        request: Request,
        csrf_token: str = Form(""),
        advanced_connector_routing_enabled: Optional[str] = Form(None),
        user_ou_placement_strategy: str = Form("source_primary_department"),
        source_root_unit_ids: str = Form(""),
        source_root_unit_display_text: str = Form(""),
        directory_root_ou_path: str = Form(""),
        disabled_users_ou_path: str = Form("Disabled Users"),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = CANONICAL_ROUTE_PATHS["sync-department-ou-routing"]
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        directory_settings = persist_directory_policy_settings_section(
            request,
            section="department_ou_routing",
            values={
                "user_ou_placement_strategy": str(
                    user_ou_placement_strategy or "source_primary_department"
                ).strip(),
                "source_root_unit_ids": str(source_root_unit_ids or "").strip(),
                "source_root_unit_display_text": str(
                    source_root_unit_display_text or ""
                ).strip(),
                "directory_root_ou_path": str(directory_root_ou_path or "").strip(),
                "disabled_users_ou_path": str(
                    disabled_users_ou_path or "Disabled Users"
                ).strip(),
            },
        )
        advanced_settings = persist_policy_settings_section(
            request,
            section="security",
            values={
                "advanced_connector_routing_enabled": to_bool(
                    advanced_connector_routing_enabled,
                    False,
                ),
            },
        )
        audit_policy_change(
            request,
            user=user,
            section="department_ou_routing",
            target_type="settings",
            target_id="organization_defaults",
            payload={
                "advanced_connector_routing_enabled": (
                    advanced_settings.advanced_connector_routing_enabled
                ),
                "user_ou_placement_strategy": (
                    directory_settings.user_ou_placement_strategy
                ),
                "source_root_unit_ids": directory_settings.source_root_unit_ids,
                "directory_root_ou_path": directory_settings.directory_root_ou_path,
                "disabled_users_ou_path": directory_settings.disabled_users_ou_path,
            },
        )
        flash(
            request,
            "success",
            "Department and OU defaults saved. The previous Dry Run is now invalid; run and review a new Dry Run before Apply.",
        )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post(CANONICAL_ROUTE_PATHS["sync-department-ou-routing"])
    @app.post("/advanced-sync/department-ou-mappings")
    def advanced_sync_department_ou_mapping_submit(
        request: Request,
        csrf_token: str = Form(""),
        connector_id: str = Form(""),
        source_department_id: str = Form(""),
        source_department_name: str = Form(""),
        target_ou_path: str = Form(""),
        apply_mode: str = Form("subtree"),
        notes: str = Form(""),
        is_enabled: Optional[str] = Form(None),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = policy_redirect_path(
            request,
            CANONICAL_ROUTE_PATHS["sync-department-ou-routing"],
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        normalized_connector_id = connector_id.strip()
        repositories = get_web_repositories(request)
        if normalized_connector_id and not repositories.connector_repo.get_connector_record(
            normalized_connector_id,
            org_id=current_org.org_id,
        ):
            flash_t(
                request,
                "error",
                "Connector {connector_id} was not found in the selected organization",
                connector_id=normalized_connector_id,
            )
            return RedirectResponse(url=redirect_url, status_code=303)
        try:
            repositories.department_ou_mapping_repo.upsert_mapping(
                org_id=current_org.org_id,
                connector_id=normalized_connector_id,
                source_department_id=source_department_id.strip(),
                source_department_name=source_department_name.strip(),
                target_ou_path=target_ou_path.strip(),
                apply_mode=str(apply_mode or "subtree").strip().lower(),
                notes=notes.strip(),
                is_enabled=to_bool(is_enabled, True),
            )
        except Exception as exc:
            flash_t(request, "error", "Failed to save department routing: {error}", error=str(exc))
            return RedirectResponse(url=redirect_url, status_code=303)
        audit_policy_change(
            request,
            user=user,
            section="department_ou_routing",
            target_type="department_ou_mapping",
            target_id=f"{normalized_connector_id or 'global'}:{source_department_id.strip()}",
            payload={
                "connector_id": normalized_connector_id,
                "source_department_id": source_department_id.strip(),
                "source_department_name": source_department_name.strip(),
                "target_ou_path": target_ou_path.strip(),
                "apply_mode": str(apply_mode or "subtree").strip().lower(),
                "is_enabled": to_bool(is_enabled, True),
            },
        )
        flash(
            request,
            "success",
            "Department routing saved. The previous Dry Run is now invalid; run and review a new Dry Run before Apply.",
        )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post(CANONICAL_ROUTE_PATHS["sync-department-ou-routing"] + "/{mapping_id}/delete")
    @app.post("/advanced-sync/department-ou-mappings/{mapping_id}/delete")
    def advanced_sync_department_ou_mapping_delete(
        request: Request,
        mapping_id: int,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = policy_redirect_path(
            request,
            CANONICAL_ROUTE_PATHS["sync-department-ou-routing"],
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        record = next(
            (
                item
                for item in repositories.department_ou_mapping_repo.list_mapping_records(org_id=current_org.org_id)
                if item.id == mapping_id
            ),
            None,
        )
        if not record:
            flash_t(request, "error", "Department routing rule not found")
            return RedirectResponse(url=redirect_url, status_code=303)
        repositories.department_ou_mapping_repo.delete_mapping(
            record.source_department_id,
            connector_id=record.connector_id,
            org_id=current_org.org_id,
        )
        audit_policy_change(
            request,
            user=user,
            section="department_ou_routing",
            target_type="department_ou_mapping",
            target_id=str(mapping_id),
            action="delete",
            payload={
                "connector_id": record.connector_id,
                "source_department_id": record.source_department_id,
                "target_ou_path": record.target_ou_path,
            },
        )
        flash(
            request,
            "success",
            "Department routing deleted. The previous Dry Run is now invalid; run and review a new Dry Run before Apply.",
        )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post("/advanced-sync/connectors/{connector_id}/toggle")
    def advanced_sync_connector_toggle(
        request: Request,
        connector_id: str,
        csrf_token: str = Form(""),
        return_url: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = (
            CANONICAL_ROUTE_PATHS["config"]
            if return_url == CANONICAL_ROUTE_PATHS["config"]
            else "/advanced-sync"
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        record = repositories.connector_repo.get_connector_record(connector_id, org_id=current_org.org_id)
        if not record:
            flash(request, "error", "Connector not found")
            return RedirectResponse(url=redirect_url, status_code=303)
        repositories.connector_repo.set_enabled(connector_id, not record.is_enabled, org_id=current_org.org_id)
        flash_t(
            request,
            "success",
            "Connector {connector_id} enabled" if not record.is_enabled else "Connector {connector_id} disabled",
            connector_id=connector_id,
        )
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post("/advanced-sync/connectors/{connector_id}/delete")
    def advanced_sync_connector_delete(
        request: Request,
        connector_id: str,
        csrf_token: str = Form(""),
        operation_code: str = Form(""),
        organization_id: str = Form(""),
        environment_label: str = Form(""),
        snapshot_version: str = Form(""),
        impact_count: str = Form(""),
        preview_id: str = Form(""),
        return_url: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = (
            CANONICAL_ROUTE_PATHS["config"]
            if return_url == CANONICAL_ROUTE_PATHS["config"]
            else "/advanced-sync"
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        repositories = get_web_repositories(request)
        current_org = get_current_org(request)
        record = repositories.connector_repo.get_connector_record(
            connector_id,
            org_id=current_org.org_id,
        )
        if not record:
            flash(request, "error", "Connector not found")
            return RedirectResponse(url=redirect_url, status_code=303)
        context = HighRiskOperationContext.create(
            operation_code="connector.delete",
            organization_id=current_org.org_id,
            organization_name=current_org.name,
            environment_label=getattr(
                request.app.state,
                "environment_label",
                "Unlabeled environment",
            ),
            snapshot_version="Not applicable",
            impact_count=1,
            preview_id=connector_id,
        )
        gate = HighRiskOperationPolicy.validate_confirmation(
            context,
            {
                "operation_code": operation_code,
                "organization_id": organization_id,
                "environment_label": environment_label,
                "snapshot_version": snapshot_version,
                "impact_count": impact_count,
                "preview_id": preview_id,
            },
        )
        if not gate.allowed:
            repositories.audit_repo.add_log(
                org_id=current_org.org_id,
                actor_username=user.username,
                action_type="high_risk.connector_delete.blocked",
                target_type="sync_connector",
                target_id=connector_id,
                result="blocked",
                message="Connector deletion was blocked by high-risk validation",
                payload=high_risk_audit_payload(
                    context,
                    reason_code=gate.reason_code,
                ),
            )
            flash_t(request, "error", gate.reason_code)
            return RedirectResponse(url=redirect_url, status_code=303)
        repositories.connector_repo.delete_connector(connector_id, org_id=current_org.org_id)
        repositories.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="high_risk.connector_delete.execute",
            target_type="sync_connector",
            target_id=connector_id,
            result="success",
            message="Deleted connector after high-risk environment validation",
            payload=high_risk_audit_payload(context),
        )
        flash_t(request, "success", "Connector {connector_id} deleted", connector_id=connector_id)
        return RedirectResponse(url=redirect_url, status_code=303)

    @app.post("/advanced-sync/mappings")
    def advanced_sync_mapping_submit(
        request: Request,
        csrf_token: str = Form(""),
        connector_id: str = Form(""),
        direction: str = Form("source_to_ad"),
        source_field: str = Form(""),
        target_field: str = Form(""),
        transform_template: str = Form(""),
        sync_mode: str = Form("replace"),
        notes: str = Form(""),
        is_enabled: Optional[str] = Form(None),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        normalized_connector_id = connector_id.strip()
        repositories = get_web_repositories(request)
        if normalized_connector_id and not repositories.connector_repo.get_connector_record(
            normalized_connector_id,
            org_id=current_org.org_id,
        ):
            flash_t(
                request,
                "error",
                "Connector {connector_id} was not found in the selected organization",
                connector_id=normalized_connector_id,
            )
            return RedirectResponse(url="/advanced-sync", status_code=303)
        try:
            repositories.attribute_mapping_repo.upsert_rule(
                connector_id=normalized_connector_id,
                direction=normalize_mapping_direction(direction),
                source_field=source_field.strip(),
                target_field=target_field.strip(),
                transform_template=transform_template.strip(),
                sync_mode=sync_mode.strip(),
                notes=notes.strip(),
                is_enabled=to_bool(is_enabled, True),
                org_id=current_org.org_id,
            )
        except Exception as exc:
            flash_t(request, "error", "Failed to save mapping rule: {error}", error=str(exc))
            return RedirectResponse(url="/advanced-sync", status_code=303)
        audit_policy_change(
            request,
            user=user,
            section="attribute_mappings",
            target_type="attribute_mapping_rule",
            target_id=(
                f"{normalized_connector_id or 'global'}:{source_field.strip()}"
                f"->{target_field.strip()}"
            ),
            payload={
                "connector_id": normalized_connector_id,
                "direction": normalize_mapping_direction(direction),
                "source_field": source_field.strip(),
                "target_field": target_field.strip(),
                "sync_mode": sync_mode.strip(),
                "is_enabled": to_bool(is_enabled, True),
            },
        )
        flash(
            request,
            "success",
            "Mapping rule saved. The previous Dry Run is now invalid; run and review a new Dry Run before Apply.",
        )
        return RedirectResponse(url="/advanced-sync", status_code=303)

    @app.post(CANONICAL_ROUTE_PATHS["sync-attribute-mappings"] + "/{rule_id}/delete")
    @app.post("/advanced-sync/mappings/{rule_id}/delete")
    def advanced_sync_mapping_delete(
        request: Request,
        rule_id: int,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        redirect_url = policy_redirect_path(
            request,
            CANONICAL_ROUTE_PATHS["sync-attribute-mappings"],
        )
        csrf_error = reject_invalid_csrf(request, csrf_token, redirect_url)
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        repositories = get_web_repositories(request)
        record = repositories.attribute_mapping_repo.get_rule_record(
            rule_id,
            org_id=current_org.org_id,
        )
        if not record:
            flash_t(request, "error", "Mapping rule not found in the selected organization")
            return RedirectResponse(url=redirect_url, status_code=303)
        repositories.attribute_mapping_repo.delete_rule(rule_id, org_id=current_org.org_id)
        audit_policy_change(
            request,
            user=user,
            section="attribute_mappings",
            target_type="attribute_mapping_rule",
            target_id=str(rule_id),
            action="delete",
            payload={
                "connector_id": record.connector_id,
                "source_field": record.source_field,
                "target_field": record.target_field,
            },
        )
        flash(
            request,
            "success",
            "Mapping rule deleted. The previous Dry Run is now invalid; run and review a new Dry Run before Apply.",
        )
        return RedirectResponse(url=redirect_url, status_code=303)
