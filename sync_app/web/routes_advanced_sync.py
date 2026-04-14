from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from sync_app.core.sync_policies import (
    ATTRIBUTE_SYNC_MODES,
    MANAGED_GROUP_TYPES,
    USERNAME_COLLISION_POLICIES,
    USERNAME_STRATEGIES,
    normalize_username_collision_policy,
    normalize_username_strategy,
)


def register_advanced_sync_routes(
    app: FastAPI,
    *,
    attribute_mapping_direction_labels: dict[str, str],
    build_username_preview: Callable[..., dict[str, Any]],
    describe_connector_config_source: Callable[[Any], str],
    explain_identity_routing: Callable[[Request, str], dict[str, Any]],
    flash: Callable[..., None],
    flash_t: Callable[..., None],
    get_current_org: Callable[[Request], Any],
    list_org_attribute_mapping_rules: Callable[[Request], list[Any]],
    list_org_connector_records: Callable[[Request], list[Any]],
    normalize_mapping_direction: Callable[[str], str],
    reject_invalid_csrf: Callable[[Request, str, str], Any],
    render: Callable[..., Any],
    require_capability: Callable[[Request, str], Any],
    split_csv_values: Callable[[str | None], list[str]],
    to_bool: Callable[[Optional[str], bool], bool],
) -> None:
    @app.get("/advanced-sync", response_class=HTMLResponse)
    def advanced_sync_page(request: Request):
        user = require_capability(request, "config.read")
        if isinstance(user, RedirectResponse):
            return user
        current_org = get_current_org(request)
        connectors = list_org_connector_records(request)

        return render(
            request,
            "advanced_sync.html",
            page="advanced-sync",
            title="Advanced Sync",
            connectors=connectors,
            connector_config_sources={
                record.connector_id: describe_connector_config_source(record)
                for record in connectors
            },
            attribute_mappings=list_org_attribute_mapping_rules(request),
            department_ou_mappings=request.app.state.department_ou_mapping_repo.list_mapping_records(
                org_id=current_org.org_id
            ),
            custom_group_bindings=request.app.state.custom_group_binding_repo.list_active_records(
                org_id=current_org.org_id
            ),
            offboarding_records=request.app.state.offboarding_repo.list_pending_records(org_id=current_org.org_id),
            lifecycle_records=request.app.state.lifecycle_repo.list_pending_records(org_id=current_org.org_id),
            replay_requests=request.app.state.replay_request_repo.list_request_records(
                status="pending",
                limit=20,
                org_id=current_org.org_id,
            ),
            current_org=current_org,
            policy_settings={
                "offboarding_grace_days": request.app.state.settings_repo.get_int(
                    "offboarding_grace_days", 0, org_id=current_org.org_id
                ),
                "offboarding_notify_managers": request.app.state.settings_repo.get_bool(
                    "offboarding_notify_managers", False, org_id=current_org.org_id
                ),
                "advanced_connector_routing_enabled": request.app.state.settings_repo.get_bool(
                    "advanced_connector_routing_enabled", False, org_id=current_org.org_id
                ),
                "attribute_mapping_enabled": request.app.state.settings_repo.get_bool(
                    "attribute_mapping_enabled", False, org_id=current_org.org_id
                ),
                "write_back_enabled": request.app.state.settings_repo.get_bool(
                    "write_back_enabled", False, org_id=current_org.org_id
                ),
                "custom_group_sync_enabled": request.app.state.settings_repo.get_bool(
                    "custom_group_sync_enabled", False, org_id=current_org.org_id
                ),
                "offboarding_lifecycle_enabled": request.app.state.settings_repo.get_bool(
                    "offboarding_lifecycle_enabled", False, org_id=current_org.org_id
                ),
                "rehire_restore_enabled": request.app.state.settings_repo.get_bool(
                    "rehire_restore_enabled", False, org_id=current_org.org_id
                ),
                "automatic_replay_enabled": request.app.state.settings_repo.get_bool(
                    "automatic_replay_enabled", False, org_id=current_org.org_id
                ),
                "future_onboarding_enabled": request.app.state.settings_repo.get_bool(
                    "future_onboarding_enabled", False, org_id=current_org.org_id
                ),
                "future_onboarding_start_field": request.app.state.settings_repo.get_value(
                    "future_onboarding_start_field", "hire_date", org_id=current_org.org_id
                ),
                "contractor_lifecycle_enabled": request.app.state.settings_repo.get_bool(
                    "contractor_lifecycle_enabled", False, org_id=current_org.org_id
                ),
                "lifecycle_employment_type_field": request.app.state.settings_repo.get_value(
                    "lifecycle_employment_type_field", "employment_type", org_id=current_org.org_id
                ),
                "contractor_end_field": request.app.state.settings_repo.get_value(
                    "contractor_end_field", "contract_end_date", org_id=current_org.org_id
                ),
                "lifecycle_sponsor_field": request.app.state.settings_repo.get_value(
                    "lifecycle_sponsor_field", "sponsor_userid", org_id=current_org.org_id
                ),
                "contractor_type_values": request.app.state.settings_repo.get_value(
                    "contractor_type_values", "contractor,intern,vendor,temp", org_id=current_org.org_id
                ),
                "disable_circuit_breaker_enabled": request.app.state.settings_repo.get_bool(
                    "disable_circuit_breaker_enabled", False, org_id=current_org.org_id
                ),
                "disable_circuit_breaker_percent": request.app.state.settings_repo.get_float(
                    "disable_circuit_breaker_percent", 5.0, org_id=current_org.org_id
                ),
                "disable_circuit_breaker_min_count": request.app.state.settings_repo.get_int(
                    "disable_circuit_breaker_min_count", 10, org_id=current_org.org_id
                ),
                "disable_circuit_breaker_requires_approval": request.app.state.settings_repo.get_bool(
                    "disable_circuit_breaker_requires_approval", True, org_id=current_org.org_id
                ),
                "managed_group_type": request.app.state.settings_repo.get_value(
                    "managed_group_type", "security", org_id=current_org.org_id
                ),
                "managed_group_mail_domain": request.app.state.settings_repo.get_value(
                    "managed_group_mail_domain", "", org_id=current_org.org_id
                ),
                "custom_group_ou_path": request.app.state.settings_repo.get_value(
                    "custom_group_ou_path", "Managed Groups", org_id=current_org.org_id
                ),
            },
            mapping_direction_options=[
                ("source_to_ad", attribute_mapping_direction_labels["source_to_ad"]),
                ("ad_to_source", attribute_mapping_direction_labels["ad_to_source"]),
            ],
            mapping_direction_labels=attribute_mapping_direction_labels,
            mapping_mode_options=[(value, value) for value in ATTRIBUTE_SYNC_MODES],
            group_type_options=[(value, value.replace("_", " ").title()) for value in MANAGED_GROUP_TYPES],
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
            department_ou_apply_mode_options=[
                ("subtree", "Map subtree"),
                ("exact", "Map exact department only"),
            ],
        )

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
        request.app.state.settings_repo.set_value("offboarding_grace_days", str(max(int(offboarding_grace_days or 0), 0)), "int", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("offboarding_notify_managers", str(to_bool(offboarding_notify_managers, False)).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("advanced_connector_routing_enabled", str(to_bool(advanced_connector_routing_enabled, False)).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("attribute_mapping_enabled", str(to_bool(attribute_mapping_enabled, False)).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("write_back_enabled", str(to_bool(write_back_enabled, False)).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("custom_group_sync_enabled", str(to_bool(custom_group_sync_enabled, False)).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("offboarding_lifecycle_enabled", str(to_bool(offboarding_lifecycle_enabled, False)).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("rehire_restore_enabled", str(to_bool(rehire_restore_enabled, False)).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("automatic_replay_enabled", str(to_bool(automatic_replay_enabled, False)).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("future_onboarding_enabled", str(to_bool(future_onboarding_enabled, False)).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("future_onboarding_start_field", future_onboarding_start_field.strip() or "hire_date", "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("contractor_lifecycle_enabled", str(to_bool(contractor_lifecycle_enabled, False)).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("lifecycle_employment_type_field", lifecycle_employment_type_field.strip() or "employment_type", "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("contractor_end_field", contractor_end_field.strip() or "contract_end_date", "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("lifecycle_sponsor_field", lifecycle_sponsor_field.strip() or "sponsor_userid", "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("contractor_type_values", contractor_type_values.strip() or "contractor,intern,vendor,temp", "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("disable_circuit_breaker_enabled", str(to_bool(disable_circuit_breaker_enabled, False)).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("disable_circuit_breaker_percent", str(max(float(disable_circuit_breaker_percent or 0.0), 0.0)), "float", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("disable_circuit_breaker_min_count", str(max(int(disable_circuit_breaker_min_count or 0), 0)), "int", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("disable_circuit_breaker_requires_approval", str(to_bool(disable_circuit_breaker_requires_approval, False)).lower(), "bool", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("managed_group_type", managed_group_type, "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("managed_group_mail_domain", managed_group_mail_domain.strip(), "string", org_id=current_org.org_id)
        request.app.state.settings_repo.set_value("custom_group_ou_path", custom_group_ou_path.strip(), "string", org_id=current_org.org_id)
        request.app.state.audit_repo.add_log(
            org_id=current_org.org_id,
            actor_username=user.username,
            action_type="advanced_sync.policy_update",
            target_type="settings",
            target_id="advanced_sync",
            result="success",
            message="Updated advanced sync policies",
            payload={
                "org_id": current_org.org_id,
                "offboarding_grace_days": max(int(offboarding_grace_days or 0), 0),
                "advanced_connector_routing_enabled": to_bool(advanced_connector_routing_enabled, False),
                "attribute_mapping_enabled": to_bool(attribute_mapping_enabled, False),
                "write_back_enabled": to_bool(write_back_enabled, False),
                "custom_group_sync_enabled": to_bool(custom_group_sync_enabled, False),
                "offboarding_lifecycle_enabled": to_bool(offboarding_lifecycle_enabled, False),
                "rehire_restore_enabled": to_bool(rehire_restore_enabled, False),
                "automatic_replay_enabled": to_bool(automatic_replay_enabled, False),
                "future_onboarding_enabled": to_bool(future_onboarding_enabled, False),
                "future_onboarding_start_field": future_onboarding_start_field.strip() or "hire_date",
                "contractor_lifecycle_enabled": to_bool(contractor_lifecycle_enabled, False),
                "lifecycle_employment_type_field": lifecycle_employment_type_field.strip() or "employment_type",
                "contractor_end_field": contractor_end_field.strip() or "contract_end_date",
                "lifecycle_sponsor_field": lifecycle_sponsor_field.strip() or "sponsor_userid",
                "contractor_type_values": contractor_type_values.strip() or "contractor,intern,vendor,temp",
                "offboarding_notify_managers": to_bool(offboarding_notify_managers, False),
                "disable_circuit_breaker_enabled": to_bool(disable_circuit_breaker_enabled, False),
                "disable_circuit_breaker_percent": max(float(disable_circuit_breaker_percent or 0.0), 0.0),
                "disable_circuit_breaker_min_count": max(int(disable_circuit_breaker_min_count or 0), 0),
                "disable_circuit_breaker_requires_approval": to_bool(disable_circuit_breaker_requires_approval, False),
                "managed_group_type": managed_group_type,
            },
        )
        flash_t(request, "success", "Advanced sync policies saved")
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
        try:
            request.app.state.connector_repo.upsert_connector(
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
        request.app.state.audit_repo.add_log(
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
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        normalized_connector_id = connector_id.strip()
        if normalized_connector_id and not request.app.state.connector_repo.get_connector_record(
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
            request.app.state.department_ou_mapping_repo.upsert_mapping(
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
            return RedirectResponse(url="/advanced-sync", status_code=303)
        flash_t(request, "success", "Department routing saved")
        return RedirectResponse(url="/advanced-sync", status_code=303)

    @app.post("/advanced-sync/department-ou-mappings/{mapping_id}/delete")
    def advanced_sync_department_ou_mapping_delete(
        request: Request,
        mapping_id: int,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        record = next(
            (
                item
                for item in request.app.state.department_ou_mapping_repo.list_mapping_records(org_id=current_org.org_id)
                if item.id == mapping_id
            ),
            None,
        )
        if not record:
            flash_t(request, "error", "Department routing rule not found")
            return RedirectResponse(url="/advanced-sync", status_code=303)
        request.app.state.department_ou_mapping_repo.delete_mapping(
            record.source_department_id,
            connector_id=record.connector_id,
            org_id=current_org.org_id,
        )
        flash_t(request, "success", "Department routing deleted")
        return RedirectResponse(url="/advanced-sync", status_code=303)

    @app.post("/advanced-sync/connectors/{connector_id}/toggle")
    def advanced_sync_connector_toggle(
        request: Request,
        connector_id: str,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        record = request.app.state.connector_repo.get_connector_record(connector_id, org_id=current_org.org_id)
        if not record:
            flash(request, "error", "Connector not found")
            return RedirectResponse(url="/advanced-sync", status_code=303)
        request.app.state.connector_repo.set_enabled(connector_id, not record.is_enabled, org_id=current_org.org_id)
        flash_t(
            request,
            "success",
            "Connector {connector_id} enabled" if not record.is_enabled else "Connector {connector_id} disabled",
            connector_id=connector_id,
        )
        return RedirectResponse(url="/advanced-sync", status_code=303)

    @app.post("/advanced-sync/connectors/{connector_id}/delete")
    def advanced_sync_connector_delete(
        request: Request,
        connector_id: str,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error
        request.app.state.connector_repo.delete_connector(connector_id, org_id=get_current_org(request).org_id)
        flash_t(request, "success", "Connector {connector_id} deleted", connector_id=connector_id)
        return RedirectResponse(url="/advanced-sync", status_code=303)

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
        if normalized_connector_id and not request.app.state.connector_repo.get_connector_record(
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
            request.app.state.attribute_mapping_repo.upsert_rule(
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
        flash_t(request, "success", "Mapping rule saved")
        return RedirectResponse(url="/advanced-sync", status_code=303)

    @app.post("/advanced-sync/mappings/{rule_id}/delete")
    def advanced_sync_mapping_delete(
        request: Request,
        rule_id: int,
        csrf_token: str = Form(""),
    ):
        user = require_capability(request, "config.write")
        if isinstance(user, RedirectResponse):
            return user
        csrf_error = reject_invalid_csrf(request, csrf_token, "/advanced-sync")
        if csrf_error:
            return csrf_error
        current_org = get_current_org(request)
        if not request.app.state.attribute_mapping_repo.get_rule_record(rule_id, org_id=current_org.org_id):
            flash_t(request, "error", "Mapping rule not found in the selected organization")
            return RedirectResponse(url="/advanced-sync", status_code=303)
        request.app.state.attribute_mapping_repo.delete_rule(rule_id, org_id=current_org.org_id)
        flash_t(request, "success", "Mapping rule deleted")
        return RedirectResponse(url="/advanced-sync", status_code=303)
