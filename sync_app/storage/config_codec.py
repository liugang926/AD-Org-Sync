from __future__ import annotations

import configparser
import os
from typing import Any, Dict, Optional

from sync_app.core.directory_protection import (
    DEFAULT_PROTECTED_AD_ACCOUNTS,
    merge_protected_ad_accounts,
)
from sync_app.core.models import (
    AccountConfig,
    AppConfig,
    LDAPConfig,
    SourceConnectorConfig,
    SyncConnectorRecord,
)
from sync_app.providers.source.base import normalize_source_provider

ORGANIZATION_CONFIG_VALUE_TYPES = {
    "source_provider": "string",
    "corpid": "string",
    "agentid": "string",
    "corpsecret": "string",
    "webhook_url": "string",
    "ldap_server": "string",
    "ldap_domain": "string",
    "ldap_username": "string",
    "ldap_password": "string",
    "ldap_use_ssl": "bool",
    "ldap_port": "int",
    "ldap_validate_cert": "bool",
    "ldap_ca_cert_path": "string",
    "default_password": "string",
    "force_change_password": "bool",
    "password_complexity": "string",
    "schedule_time": "string",
    "retry_interval": "int",
    "max_retries": "int",
    "exclude_accounts": "json",
    "exclude_departments": "json",
}

CONNECTOR_CONFIG_FIELDS = (
    "ldap_server",
    "ldap_domain",
    "ldap_username",
    "ldap_password",
    "ldap_use_ssl",
    "ldap_port",
    "ldap_validate_cert",
    "ldap_ca_cert_path",
    "default_password",
    "force_change_password",
    "password_complexity",
)


def normalize_config_path(config_path: str = "config.ini") -> str:
    return os.path.abspath(str(config_path or "").strip() or "config.ini")


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


def to_bool_value(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def to_int_value(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_list_value(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def default_org_config_values(config_path: str = "config.ini") -> Dict[str, Any]:
    return {
        "config_path": normalize_config_path(config_path),
        "source_provider": "wecom",
        "corpid": "",
        "agentid": "",
        "corpsecret": "",
        "webhook_url": "",
        "ldap_server": "",
        "ldap_domain": "",
        "ldap_username": "",
        "ldap_password": "",
        "ldap_use_ssl": True,
        "ldap_port": 636,
        "ldap_validate_cert": True,
        "ldap_ca_cert_path": "",
        "default_password": "",
        "force_change_password": True,
        "password_complexity": "strong",
        "schedule_time": "03:00",
        "retry_interval": 60,
        "max_retries": 3,
        "exclude_accounts": list(DEFAULT_PROTECTED_AD_ACCOUNTS),
        "exclude_departments": [],
    }


def load_org_config_values_from_file(config_path: str) -> Dict[str, Any]:
    normalized_path = normalize_config_path(config_path)
    parser = configparser.ConfigParser()
    parser.read(normalized_path, encoding="utf-8")
    use_ssl = parser.getboolean("LDAP", "UseSSL", fallback=True)
    ldap_domain = parser.get("LDAP", "Domain", fallback=parser.get("Domain", "Name", fallback=""))
    values = default_org_config_values(normalized_path)
    values.update(
        {
            "source_provider": normalize_source_provider(parser.get("Source", "Provider", fallback="wecom")),
            "corpid": _get_config_value(parser, ("SourceConnector", "WeChat"), "CorpID", fallback=""),
            "agentid": _get_config_value(parser, ("SourceConnector", "WeChat"), "AgentID", fallback=""),
            "corpsecret": _get_config_value(parser, ("SourceConnector", "WeChat"), "CorpSecret", fallback=""),
            "webhook_url": _get_config_value(parser, ("Notification", "WeChatBot"), "WebhookUrl", fallback=""),
            "ldap_server": parser.get("LDAP", "Server", fallback=""),
            "ldap_domain": ldap_domain,
            "ldap_username": parser.get("LDAP", "Username", fallback=""),
            "ldap_password": parser.get("LDAP", "Password", fallback=""),
            "ldap_use_ssl": use_ssl,
            "ldap_port": parser.getint("LDAP", "Port", fallback=636 if use_ssl else 389),
            "ldap_validate_cert": parser.getboolean("LDAP", "ValidateCert", fallback=True),
            "ldap_ca_cert_path": parser.get("LDAP", "CACertPath", fallback=""),
            "default_password": parser.get("Account", "DefaultPassword", fallback="").strip(),
            "force_change_password": parser.getboolean("Account", "ForceChangePassword", fallback=True),
            "password_complexity": parser.get("Account", "PasswordComplexity", fallback="strong").strip() or "strong",
            "schedule_time": parser.get("Schedule", "Time", fallback="03:00"),
            "retry_interval": parser.getint("Schedule", "RetryInterval", fallback=60),
            "max_retries": parser.getint("Schedule", "MaxRetries", fallback=3),
            "exclude_departments": [
                item.strip()
                for item in parser.get("ExcludeDepartments", "Names", fallback="").split(",")
                if item.strip()
            ],
            "exclude_accounts": [
                *[
                    item.strip()
                    for item in parser.get("ExcludeUsers", "SystemAccounts", fallback="").split(",")
                    if item.strip()
                ],
                *[
                    item.strip()
                    for item in parser.get("ExcludeUsers", "CustomAccounts", fallback="").split(",")
                    if item.strip()
                ],
            ],
        }
    )
    return values


def normalize_org_config_values(
    values: Dict[str, Any],
    *,
    existing: Optional[Dict[str, Any]] = None,
    config_path: str = "config.ini",
) -> Dict[str, Any]:
    normalized = default_org_config_values(config_path)
    if existing:
        normalized.update({key: existing.get(key) for key in normalized.keys() if key in existing})
    normalized["config_path"] = normalize_config_path(
        str(values.get("config_path") or normalized.get("config_path") or config_path or "config.ini")
    )
    normalized["source_provider"] = normalize_source_provider(
        values.get("source_provider") or (existing or {}).get("source_provider") or "wecom"
    )
    normalized["corpid"] = str(values.get("corpid") or "").strip()
    normalized["agentid"] = str(values.get("agentid") or "").strip()
    normalized["corpsecret"] = (
        str(values.get("corpsecret") or "").strip()
        if str(values.get("corpsecret") or "").strip()
        else str((existing or {}).get("corpsecret") or "")
    )
    normalized["webhook_url"] = (
        str(values.get("webhook_url") or "").strip()
        if str(values.get("webhook_url") or "").strip()
        else str((existing or {}).get("webhook_url") or "")
    )
    normalized["ldap_server"] = str(values.get("ldap_server") or "").strip()
    normalized["ldap_domain"] = str(values.get("ldap_domain") or "").strip()
    normalized["ldap_username"] = str(values.get("ldap_username") or "").strip()
    normalized["ldap_password"] = (
        str(values.get("ldap_password") or "").strip()
        if str(values.get("ldap_password") or "").strip()
        else str((existing or {}).get("ldap_password") or "")
    )
    normalized["ldap_use_ssl"] = to_bool_value(
        values.get("ldap_use_ssl"),
        bool((existing or {}).get("ldap_use_ssl", True)),
    )
    normalized["ldap_port"] = to_int_value(
        values.get("ldap_port"),
        to_int_value((existing or {}).get("ldap_port"), 636),
    )
    normalized["ldap_validate_cert"] = to_bool_value(
        values.get("ldap_validate_cert"),
        bool((existing or {}).get("ldap_validate_cert", True)),
    )
    normalized["ldap_ca_cert_path"] = str(values.get("ldap_ca_cert_path") or "").strip()
    normalized["default_password"] = (
        str(values.get("default_password") or "").strip()
        if str(values.get("default_password") or "").strip()
        else str((existing or {}).get("default_password") or "")
    )
    normalized["force_change_password"] = to_bool_value(
        values.get("force_change_password"),
        bool((existing or {}).get("force_change_password", True)),
    )
    normalized["password_complexity"] = str(values.get("password_complexity") or "strong").strip() or "strong"
    normalized["schedule_time"] = str(values.get("schedule_time") or "03:00").strip() or "03:00"
    normalized["retry_interval"] = to_int_value(
        values.get("retry_interval"),
        to_int_value((existing or {}).get("retry_interval"), 60),
    )
    normalized["max_retries"] = to_int_value(
        values.get("max_retries"),
        to_int_value((existing or {}).get("max_retries"), 3),
    )
    normalized["exclude_accounts"] = merge_protected_ad_accounts(
        normalize_list_value(values.get("exclude_accounts", (existing or {}).get("exclude_accounts", [])))
    )
    normalized["exclude_departments"] = normalize_list_value(
        values.get("exclude_departments", (existing or {}).get("exclude_departments", []))
    )
    return normalized


def build_editable_org_config(values: Dict[str, Any], *, config_source: str) -> Dict[str, Any]:
    normalized = normalize_org_config_values(
        values,
        existing=values,
        config_path=str(values.get("config_path") or "config.ini"),
    )
    return {
        "config_path": normalized["config_path"],
        "config_source": config_source,
        "source_provider": normalized["source_provider"],
        "corpid": normalized["corpid"],
        "agentid": normalized["agentid"],
        "corpsecret": "",
        "corpsecret_configured": bool(normalized["corpsecret"]),
        "webhook_url": "",
        "webhook_url_configured": bool(normalized["webhook_url"]),
        "ldap_server": normalized["ldap_server"],
        "ldap_domain": normalized["ldap_domain"],
        "ldap_username": normalized["ldap_username"],
        "ldap_password": "",
        "ldap_password_configured": bool(normalized["ldap_password"]),
        "ldap_use_ssl": normalized["ldap_use_ssl"],
        "ldap_port": normalized["ldap_port"],
        "ldap_validate_cert": normalized["ldap_validate_cert"],
        "ldap_ca_cert_path": normalized["ldap_ca_cert_path"],
        "default_password": "",
        "default_password_configured": bool(normalized["default_password"]),
        "force_change_password": normalized["force_change_password"],
        "password_complexity": normalized["password_complexity"],
        "schedule_time": normalized["schedule_time"],
        "retry_interval": normalized["retry_interval"],
        "max_retries": normalized["max_retries"],
        "protected_accounts": list(normalized["exclude_accounts"]),
    }


def build_app_config_from_org_values(values: Dict[str, Any], *, config_source: str) -> AppConfig:
    normalized = normalize_org_config_values(
        values,
        existing=values,
        config_path=str(values.get("config_path") or "config.ini"),
    )
    domain_name = normalized["ldap_domain"]
    return AppConfig(
        source_connector=SourceConnectorConfig(
            corpid=normalized["corpid"],
            corpsecret=normalized["corpsecret"],
            agentid=normalized["agentid"] or None,
        ),
        ldap=LDAPConfig(
            server=normalized["ldap_server"],
            domain=domain_name,
            username=normalized["ldap_username"],
            password=normalized["ldap_password"],
            use_ssl=bool(normalized["ldap_use_ssl"]),
            port=int(normalized["ldap_port"]) if normalized["ldap_port"] else None,
            validate_cert=bool(normalized["ldap_validate_cert"]),
            ca_cert_path=normalized["ldap_ca_cert_path"],
        ),
        domain=domain_name,
        source_provider=normalized["source_provider"],
        account=AccountConfig(
            default_password=normalized["default_password"],
            force_change_password=bool(normalized["force_change_password"]),
            password_complexity=normalized["password_complexity"],
        ),
        exclude_departments=list(normalized["exclude_departments"]),
        exclude_accounts=list(normalized["exclude_accounts"]),
        webhook_url=normalized["webhook_url"],
        config_path=str(config_source or normalized["config_path"]),
    )


def default_connector_config_values(config_path: str = "") -> Dict[str, Any]:
    normalized_path = str(config_path or "").strip()
    return {
        "config_path": os.path.abspath(normalized_path) if normalized_path else "",
        "ldap_server": "",
        "ldap_domain": "",
        "ldap_username": "",
        "ldap_password": "",
        "ldap_use_ssl": None,
        "ldap_port": None,
        "ldap_validate_cert": None,
        "ldap_ca_cert_path": "",
        "default_password": "",
        "force_change_password": None,
        "password_complexity": "",
    }


def load_connector_config_values_from_file(config_path: str) -> Dict[str, Any]:
    normalized_path = os.path.abspath(str(config_path or "").strip())
    parser = configparser.ConfigParser()
    parser.read(normalized_path, encoding="utf-8")
    use_ssl = parser.getboolean("LDAP", "UseSSL", fallback=True)
    ldap_domain = parser.get("LDAP", "Domain", fallback=parser.get("Domain", "Name", fallback=""))
    values = default_connector_config_values(normalized_path)
    values.update(
        {
            "ldap_server": parser.get("LDAP", "Server", fallback=""),
            "ldap_domain": ldap_domain,
            "ldap_username": parser.get("LDAP", "Username", fallback=""),
            "ldap_password": parser.get("LDAP", "Password", fallback=""),
            "ldap_use_ssl": use_ssl,
            "ldap_port": parser.getint("LDAP", "Port", fallback=636 if use_ssl else 389),
            "ldap_validate_cert": parser.getboolean("LDAP", "ValidateCert", fallback=True),
            "ldap_ca_cert_path": parser.get("LDAP", "CACertPath", fallback=""),
            "default_password": parser.get("Account", "DefaultPassword", fallback="").strip(),
            "force_change_password": parser.getboolean("Account", "ForceChangePassword", fallback=True),
            "password_complexity": parser.get("Account", "PasswordComplexity", fallback="strong").strip() or "strong",
        }
    )
    return values


def normalize_connector_config_values(
    values: Dict[str, Any],
    *,
    existing: Optional[Dict[str, Any]] = None,
    config_path: str = "",
) -> Dict[str, Any]:
    normalized = default_connector_config_values(config_path)
    if existing:
        normalized.update({key: existing.get(key) for key in normalized.keys() if key in existing})
    normalized_path = str(values.get("config_path") or normalized.get("config_path") or config_path or "").strip()
    normalized["config_path"] = os.path.abspath(normalized_path) if normalized_path else ""
    normalized["ldap_server"] = str(values.get("ldap_server") or "").strip()
    normalized["ldap_domain"] = str(values.get("ldap_domain") or "").strip()
    normalized["ldap_username"] = str(values.get("ldap_username") or "").strip()
    normalized["ldap_password"] = (
        str(values.get("ldap_password") or "").strip()
        if str(values.get("ldap_password") or "").strip()
        else str((existing or {}).get("ldap_password") or "")
    )
    raw_use_ssl = values.get("ldap_use_ssl", (existing or {}).get("ldap_use_ssl"))
    normalized["ldap_use_ssl"] = None if raw_use_ssl in ("", None) else to_bool_value(raw_use_ssl, True)
    raw_port = values.get("ldap_port", (existing or {}).get("ldap_port"))
    normalized["ldap_port"] = None if raw_port in ("", None) else to_int_value(raw_port, 636)
    raw_validate = values.get("ldap_validate_cert", (existing or {}).get("ldap_validate_cert"))
    normalized["ldap_validate_cert"] = None if raw_validate in ("", None) else to_bool_value(raw_validate, True)
    normalized["ldap_ca_cert_path"] = str(values.get("ldap_ca_cert_path") or "").strip()
    normalized["default_password"] = (
        str(values.get("default_password") or "").strip()
        if str(values.get("default_password") or "").strip()
        else str((existing or {}).get("default_password") or "")
    )
    raw_force_change = values.get("force_change_password", (existing or {}).get("force_change_password"))
    normalized["force_change_password"] = None if raw_force_change in ("", None) else to_bool_value(raw_force_change, True)
    normalized["password_complexity"] = str(values.get("password_complexity") or "").strip()
    return normalized


def record_has_connector_overrides(record: SyncConnectorRecord) -> bool:
    return any(
        [
            bool(record.ldap_server),
            bool(record.ldap_domain),
            bool(record.ldap_username),
            bool(record.ldap_password),
            record.ldap_use_ssl is not None,
            record.ldap_port is not None,
            record.ldap_validate_cert is not None,
            bool(record.ldap_ca_cert_path),
            bool(record.default_password),
            record.force_change_password is not None,
            bool(record.password_complexity),
        ]
    )


def build_app_config_from_connector_record(
    record: SyncConnectorRecord,
    *,
    base_config: AppConfig,
    config_source: str,
) -> AppConfig:
    ldap_config = LDAPConfig(
        server=record.ldap_server or base_config.ldap.server,
        domain=record.ldap_domain or base_config.ldap.domain,
        username=record.ldap_username or base_config.ldap.username,
        password=record.ldap_password or base_config.ldap.password,
        use_ssl=base_config.ldap.use_ssl if record.ldap_use_ssl is None else bool(record.ldap_use_ssl),
        port=base_config.ldap.port if record.ldap_port is None else int(record.ldap_port),
        validate_cert=(
            base_config.ldap.validate_cert
            if record.ldap_validate_cert is None
            else bool(record.ldap_validate_cert)
        ),
        ca_cert_path=record.ldap_ca_cert_path or base_config.ldap.ca_cert_path,
    )
    account_config = AccountConfig(
        default_password=record.default_password or base_config.account.default_password,
        force_change_password=(
            base_config.account.force_change_password
            if record.force_change_password is None
            else bool(record.force_change_password)
        ),
        password_complexity=record.password_complexity or base_config.account.password_complexity,
    )
    return AppConfig(
        source_connector=SourceConnectorConfig(
            corpid=base_config.source_connector.corpid,
            corpsecret=base_config.source_connector.corpsecret,
            agentid=base_config.source_connector.agentid,
        ),
        ldap=ldap_config,
        domain=ldap_config.domain,
        source_provider=base_config.source_provider,
        account=account_config,
        exclude_departments=list(base_config.exclude_departments),
        exclude_accounts=list(base_config.exclude_accounts),
        webhook_url=base_config.webhook_url,
        config_path=config_source,
    )
