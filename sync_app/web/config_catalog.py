from __future__ import annotations

from typing import Any, Optional

from fastapi import Request

from sync_app.providers.source import get_source_provider_display_name, get_source_provider_schema


def build_source_unit_catalog(
    support: Any,
    request: Request,
    *,
    source_provider: str = "wecom",
    corpid: str = "",
    agentid: str = "",
    corpsecret: str = "",
) -> dict[str, Any]:
    submission = support.build_config_submission(
        request,
        source_provider=source_provider,
        corpid=corpid,
        agentid=agentid,
        corpsecret=corpsecret,
    )
    preview_config = support.build_preview_app_config(request, submission)
    provider_schema = get_source_provider_schema(preview_config.source_provider)
    missing_fields = [
        field.label
        for field in provider_schema.connection_fields
        if field.required and not str(getattr(preview_config.source_connector, field.name, "") or "").strip()
    ]
    if missing_fields:
        return {
            "ok": False,
            "error": support.translate(
                "Complete the required source connector fields first: {fields}",
                support.request_support.get_ui_language(request),
                fields=", ".join(missing_fields),
            ),
        }
    try:
        source_provider_client = support.build_source_provider(app_config=preview_config, logger=support.logger)
        try:
            departments = source_provider_client.list_departments()
        finally:
            source_provider_client.close()
    except Exception as exc:
        support.logger.warning("failed to load source unit catalog: %s", exc)
        return {
            "ok": False,
            "error": str(exc) or support.translate("Unable to load source departments.", support.request_support.get_ui_language(request)),
        }

    dept_tree = {item.department_id: item for item in departments if item.department_id}
    for dept_id in list(dept_tree):
        path_names: list[str] = []
        path_ids: list[int] = []
        current_id = dept_id
        seen: set[int] = set()
        while current_id and current_id in dept_tree and current_id not in seen:
            seen.add(current_id)
            current_node = dept_tree[current_id]
            path_names.insert(0, current_node.name)
            path_ids.insert(0, current_node.department_id)
            current_id = current_node.parent_id
        dept_tree[dept_id].set_hierarchy(path_names, path_ids)

    selected_ids = {
        candidate
        for candidate in support.split_csv_values(submission["settings_values"].get("source_root_unit_ids", ""))
        if candidate.isdigit()
    }
    items = [
        {
            "department_id": str(node.department_id),
            "name": node.name,
            "path_display": " / ".join(node.path or [node.name]),
            "level": max(len(node.path) - 1, 0),
            "selected": str(node.department_id) in selected_ids,
        }
        for node in sorted(
            dept_tree.values(),
            key=lambda item: (len(item.path_ids), [str(part).lower() for part in item.path or [item.name]], item.department_id),
        )
    ]
    return {
        "ok": True,
        "provider": get_source_provider_display_name(preview_config.source_provider),
        "items": items,
    }


def build_target_ou_catalog(
    support: Any,
    request: Request,
    *,
    ldap_server: str = "",
    ldap_domain: str = "",
    ldap_username: str = "",
    ldap_password: str = "",
    ldap_port: int = 636,
    ldap_use_ssl: Optional[str] = None,
    ldap_validate_cert: Optional[str] = None,
    ldap_ca_cert_path: str = "",
) -> dict[str, Any]:
    submission = support.build_config_submission(
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
    preview_config = support.build_preview_app_config(request, submission)
    required_values = {
        "LDAP Server": preview_config.ldap.server,
        "LDAP Domain": preview_config.ldap.domain,
        "LDAP Username": preview_config.ldap.username,
        "LDAP Password": preview_config.ldap.password,
    }
    missing_fields = [label for label, value in required_values.items() if not str(value or "").strip()]
    if missing_fields:
        return {
            "ok": False,
            "error": support.translate(
                "Complete the required LDAP fields first: {fields}",
                support.request_support.get_ui_language(request),
                fields=", ".join(missing_fields),
            ),
        }
    try:
        target_provider = support.build_target_provider(
            server=preview_config.ldap.server,
            domain=preview_config.ldap.domain,
            username=preview_config.ldap.username,
            password=preview_config.ldap.password,
            use_ssl=preview_config.ldap.use_ssl,
            port=preview_config.ldap.port,
            validate_cert=preview_config.ldap.validate_cert,
            ca_cert_path=preview_config.ldap.ca_cert_path,
        )
        try:
            organizational_units = target_provider.list_organizational_units()
        finally:
            close_fn = getattr(getattr(target_provider, "client", None), "close", None)
            if callable(close_fn):
                close_fn()
    except Exception as exc:
        support.logger.warning("failed to load target OU catalog: %s", exc)
        return {
            "ok": False,
            "error": str(exc) or support.translate("Unable to load AD OU list.", support.request_support.get_ui_language(request)),
        }

    ui_language = support.request_support.get_ui_language(request)
    items = [
        {
            "name": str(item.get("name") or ""),
            "dn": str(item.get("dn") or ""),
            "guid": str(item.get("guid") or ""),
            "path": list(item.get("path") or []),
            "path_value": "/".join(item.get("path") or []),
            "path_display": " / ".join(item.get("path") or []) or support.translate("Domain Root", ui_language),
            "level": max(len(item.get("path") or []), 0),
        }
        for item in organizational_units
    ]
    return {
        "ok": True,
        "provider": "AD / LDAPS",
        "base_dn": preview_config.ldap.domain,
        "items": items,
    }
