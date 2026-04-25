from __future__ import annotations

import logging
import os
from typing import List, Tuple

from sync_app.core.config import test_ldap_connection as _test_ldap_connection
from sync_app.core.models import AppConfig, SourceConnectorConfig
from sync_app.providers.source import (
    build_source_provider,
    get_source_provider_display_name,
    get_source_provider_schema,
)
from sync_app.providers.source.base import normalize_source_provider


def validate_config(config: AppConfig) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    logger = logging.getLogger(__name__)
    source_provider = normalize_source_provider(getattr(config, "source_provider", "wecom"))
    provider_schema = get_source_provider_schema(source_provider)
    source_values = {
        "corpid": str(config.source_connector.corpid or "").strip(),
        "agentid": str(config.source_connector.agentid or "").strip(),
        "corpsecret": str(config.source_connector.corpsecret or "").strip(),
        "webhook_url": str(config.webhook_url or "").strip(),
    }

    if not provider_schema.implemented:
        errors.append(
            provider_schema.implementation_status
            or f"Source provider '{get_source_provider_display_name(source_provider)}' is not implemented in this build"
        )
    for field in provider_schema.connection_fields:
        if field.required and not source_values.get(field.name):
            errors.append(f"{provider_schema.display_name} {field.label} is not configured")

    ldap_config = config.ldap
    if not ldap_config.server:
        errors.append("LDAP server is not configured")
    if not ldap_config.domain:
        errors.append("LDAP domain is not configured")
    if not ldap_config.username:
        errors.append("LDAP username is not configured")
    if not ldap_config.password:
        errors.append("LDAP password is not configured")
    if (
        ldap_config.use_ssl
        and ldap_config.validate_cert
        and ldap_config.ca_cert_path
        and not os.path.exists(ldap_config.ca_cert_path)
    ):
        errors.append(f"LDAP CA certificate file does not exist: {ldap_config.ca_cert_path}")

    port = ldap_config.port
    if port and (port < 1 or port > 65535):
        errors.append(f"LDAP port is invalid: {port}")

    if not config.account.default_password:
        errors.append("Default password is not configured")

    if errors:
        logger.error("config validation failed with %s error(s)", len(errors))
        for error in errors:
            logger.error("  - %s", error)
        return False, errors

    logger.info("config validation passed")
    return True, []


def run_config_security_self_check(config: AppConfig) -> List[str]:
    warnings: List[str] = []
    source_provider = normalize_source_provider(getattr(config, "source_provider", "wecom"))
    provider_schema = get_source_provider_schema(source_provider)

    if not config.ldap.use_ssl:
        warnings.append("LDAP is not using SSL/TLS.")
    elif not config.ldap.validate_cert:
        warnings.append("LDAPS certificate validation is disabled.")

    default_password = config.account.default_password.strip()
    insecure_passwords = {"notting8899", "changeme123!", "password123!", "admin123!"}
    if default_password and default_password.lower() in insecure_passwords:
        warnings.append("Default password is still a sample or weak password. Replace it immediately.")
    elif default_password and len(default_password) < 12:
        warnings.append("Default password is shorter than 12 characters. Increase its strength.")

    if default_password and not config.account.force_change_password:
        warnings.append("New users are not forced to change password at first sign-in.")

    webhook_url = str(config.webhook_url or "").strip()
    if webhook_url and source_provider == "wecom" and "key=" not in webhook_url:
        notification_label = (
            provider_schema.notification_fields[0].label
            if provider_schema.notification_fields
            else "Notification Webhook"
        )
        warnings.append(f"{notification_label} format is invalid")

    if webhook_url and not webhook_url.startswith("https://"):
        provider_name = get_source_provider_display_name(source_provider)
        warnings.append(f"{provider_name} webhook is not using HTTPS.")

    return warnings


def test_source_connection(
    corpid: str,
    corpsecret: str,
    agentid: str = None,
    *,
    source_provider: str = "wecom",
) -> Tuple[bool, str]:
    logger = logging.getLogger(__name__)
    provider_client = None
    normalized_provider = normalize_source_provider(source_provider)
    provider_label = get_source_provider_display_name(normalized_provider)
    try:
        provider_client = build_source_provider(
            source_connector_config=SourceConnectorConfig(corpid=corpid, corpsecret=corpsecret, agentid=agentid),
            provider_type=normalized_provider,
            logger=logger,
        )
        departments = provider_client.list_departments()
        auth_type = "self-built app" if agentid else "generic"
        message = f"{provider_label} connection succeeded ({auth_type}), departments: {len(departments)}"
        logger.info(message)
        return True, message
    except Exception as exc:
        message = f"{provider_label} connection failed: {exc}"
        logger.error(message)
        return False, message
    finally:
        if provider_client is not None:
            provider_client.close()


def test_wecom_connection(corpid: str, corpsecret: str, agentid: str = None) -> Tuple[bool, str]:
    return test_source_connection(
        corpid,
        corpsecret,
        agentid,
        source_provider="wecom",
    )


def test_ldap_connection(
    server: str,
    domain: str,
    username: str,
    password: str,
    use_ssl: bool = True,
    port: int = None,
    validate_cert: bool = True,
    ca_cert_path: str = "",
) -> Tuple[bool, str]:
    return _test_ldap_connection(
        server,
        domain,
        username,
        password,
        use_ssl,
        port,
        validate_cert,
        ca_cert_path,
    )


# These are runtime diagnostics/helpers, not pytest test cases.
test_source_connection.__test__ = False
test_wecom_connection.__test__ = False
test_ldap_connection.__test__ = False

