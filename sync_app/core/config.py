import configparser
import logging
import ssl
from typing import List, Tuple

from sync_app.core.directory_protection import merge_protected_ad_accounts
from sync_app.core.models import AccountConfig, AppConfig, LDAPConfig, SourceConnectorConfig
from sync_app.infra.ldap_compat import ALL, NTLM, SIMPLE, Connection, Server, Tls, ensure_ldap3_available


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


def _normalize_source_provider(value: str | None, *, default: str = "wecom") -> str:
    normalized = str(value or "").strip().lower()
    return normalized or default


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
        source_connector=SourceConnectorConfig(
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
        source_provider=_normalize_source_provider(config_parser.get("Source", "Provider", fallback="wecom")),
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
    from sync_app.services.config_validation import validate_config as _validate_config

    return _validate_config(config)


def run_config_security_self_check(config: AppConfig) -> List[str]:
    from sync_app.services.config_validation import (
        run_config_security_self_check as _run_config_security_self_check,
    )

    return _run_config_security_self_check(config)


def test_source_connection(
    corpid: str,
    corpsecret: str,
    agentid: str = None,
    *,
    source_provider: str = "wecom",
) -> Tuple[bool, str]:
    from sync_app.services.config_validation import test_source_connection as _test_source_connection

    return _test_source_connection(
        corpid,
        corpsecret,
        agentid,
        source_provider=source_provider,
    )


def test_wecom_connection(corpid: str, corpsecret: str, agentid: str = None) -> Tuple[bool, str]:
    from sync_app.services.config_validation import test_wecom_connection as _test_wecom_connection

    return _test_wecom_connection(corpid, corpsecret, agentid)


# These are runtime diagnostics/helpers, not pytest test cases.
test_source_connection.__test__ = False
test_wecom_connection.__test__ = False


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


test_ldap_connection.__test__ = False
