import configparser
import logging
import os
import ssl
from typing import List, Tuple

from sync_app.core.directory_protection import merge_protected_ad_accounts
from sync_app.core.models import AccountConfig, AppConfig, LDAPConfig, SourceConnectorConfig
from sync_app.infra.ldap_compat import ALL, NTLM, SIMPLE, Connection, Server, Tls, ensure_ldap3_available
from sync_app.providers.source import (
    build_source_provider,
    get_source_provider_display_name,
    get_source_provider_schema,
)
from sync_app.providers.source.base import normalize_source_provider


def _get_optional_str(parser: configparser.ConfigParser, section: str, option: str) -> str | None:
    value = parser.get(section, option, fallback="").strip()
    return value or None


def _get_config_value(
    parser: configparser.ConfigParser,
    sections: tuple[str, ...],
    option: str,
    *,
    fallback: str = "",
) -> str:
    for section in sections:
        if parser.has_option(section, option):
            return parser.get(section, option, fallback=fallback)
    return fallback


def build_tls_config(*, validate_cert: bool, ca_cert_path: str = ""):
    if not validate_cert:
        return Tls(validate=ssl.CERT_NONE, version=ssl.PROTOCOL_TLSv1_2)

    tls_kwargs = {
        "validate": ssl.CERT_REQUIRED,
        "version": ssl.PROTOCOL_TLSv1_2,
    }
    if ca_cert_path:
        tls_kwargs["ca_certs_file"] = ca_cert_path
    return Tls(**tls_kwargs)


def load_sync_config(config_path: str = "config.ini") -> AppConfig:
    config_parser = configparser.ConfigParser()
    config_parser.read(config_path, encoding="utf-8")

    domain_name = config_parser.get("LDAP", "Domain", fallback=config_parser.get("Domain", "Name", fallback=""))

    return AppConfig(
        wecom=SourceConnectorConfig(
            corpid=_get_config_value(config_parser, ("SourceConnector", "WeChat"), "CorpID", fallback=""),
            corpsecret=_get_config_value(config_parser, ("SourceConnector", "WeChat"), "CorpSecret", fallback=""),
            agentid=(
                _get_config_value(config_parser, ("SourceConnector", "WeChat"), "AgentID", fallback="").strip() or None
            ),
        ),
        ldap=LDAPConfig(
            server=config_parser.get("LDAP", "Server"),
            domain=domain_name,
            username=config_parser.get("LDAP", "Username"),
            password=config_parser.get("LDAP", "Password"),
            use_ssl=config_parser.getboolean("LDAP", "UseSSL", fallback=True),
            port=config_parser.getint("LDAP", "Port", fallback=None) if config_parser.has_option("LDAP", "Port") else None,
            validate_cert=config_parser.getboolean("LDAP", "ValidateCert", fallback=True),
            ca_cert_path=config_parser.get("LDAP", "CACertPath", fallback="").strip(),
        ),
        domain=domain_name,
        source_provider=normalize_source_provider(config_parser.get("Source", "Provider", fallback="wecom")),
        account=AccountConfig(
            default_password=config_parser.get("Account", "DefaultPassword", fallback="").strip(),
            force_change_password=config_parser.getboolean("Account", "ForceChangePassword", fallback=True),
            password_complexity=config_parser.get("Account", "PasswordComplexity", fallback="strong").strip() or "strong",
        ),
        exclude_departments=[
            item.strip()
            for item in config_parser.get("ExcludeDepartments", "Names", fallback="").split(",")
            if item.strip()
        ],
        exclude_accounts=merge_protected_ad_accounts(
            [
                *[
                    item.strip()
                    for item in config_parser.get("ExcludeUsers", "SystemAccounts", fallback="").split(",")
                    if item.strip()
                ],
                *[
                    item.strip()
                    for item in config_parser.get("ExcludeUsers", "CustomAccounts", fallback="").split(",")
                    if item.strip()
                ],
            ]
        ),
        webhook_url=_get_config_value(config_parser, ("Notification", "WeChatBot"), "WebhookUrl", fallback=""),
        config_path=config_path,
    )


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

    webhook_url = config.webhook_url
    if not webhook_url:
        notification_label = (
            provider_schema.notification_fields[0].label
            if provider_schema.notification_fields
            else "Notification Webhook"
        )
        errors.append(f"{notification_label} is not configured")
    elif source_provider == "wecom" and "key=" not in webhook_url:
        notification_label = (
            provider_schema.notification_fields[0].label
            if provider_schema.notification_fields
            else "Notification Webhook"
        )
        errors.append(f"{notification_label} format is invalid")

    ldap_config = config.ldap
    if not ldap_config.server:
        errors.append("LDAP server is not configured")
    if not ldap_config.domain:
        errors.append("LDAP domain is not configured")
    if not ldap_config.username:
        errors.append("LDAP username is not configured")
    if not ldap_config.password:
        errors.append("LDAP password is not configured")
    if ldap_config.use_ssl and ldap_config.validate_cert and ldap_config.ca_cert_path and not os.path.exists(ldap_config.ca_cert_path):
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

    if config.webhook_url and not config.webhook_url.startswith("https://"):
        provider_name = get_source_provider_display_name(getattr(config, "source_provider", "wecom"))
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


def _to_upn(username: str, domain: str) -> str:
    if "\\" in username:
        parts = username.split("\\", 1)
        return f"{parts[1]}@{domain}"
    if "@" in username:
        return username
    return f"{username}@{domain}"


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
    ensure_ldap3_available()

    logger = logging.getLogger(__name__)
    try:
        if port is None:
            port = 636 if use_ssl else 389

        if use_ssl:
            tls_config = build_tls_config(validate_cert=validate_cert, ca_cert_path=ca_cert_path)
            server_obj = Server(server, port=port, use_ssl=True, tls=tls_config, get_info=ALL)
        else:
            server_obj = Server(server, port=port, get_info=ALL)

        try:
            conn = Connection(
                server_obj,
                user=username,
                password=password,
                authentication=NTLM,
                auto_bind=True,
                receive_timeout=10,
            )
            auth_type = "NTLM"
        except Exception:
            conn = Connection(
                server_obj,
                user=_to_upn(username, domain),
                password=password,
                authentication=SIMPLE,
                auto_bind=True,
                receive_timeout=10,
            )
            auth_type = "SIMPLE"

        base_dn = ",".join([f"DC={part}" for part in domain.split(".")])
        conn.search(base_dn, "(objectClass=domain)", search_scope="BASE")
        conn.unbind()

        tls_mode = "LDAPS" if use_ssl else "LDAP"
        message = f"LDAP connection succeeded (auth: {auth_type}, protocol: {tls_mode})"
        logger.info(message)
        return True, message
    except Exception as exc:
        message = f"LDAP connection failed: {exc}"
        logger.error(message)
        return False, message
