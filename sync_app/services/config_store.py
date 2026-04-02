import configparser
import os
from typing import Any, Dict, Iterable

from sync_app.core.directory_protection import (
    DEFAULT_PROTECTED_AD_ACCOUNTS,
    filter_custom_protected_ad_accounts,
    merge_protected_ad_accounts,
)
from sync_app.core.models import AccountConfig, AppConfig, LDAPConfig, SourceConnectorConfig
from sync_app.providers.source.base import normalize_source_provider


DEFAULT_SECTIONS = [
    "Source",
    "WeChat",
    "WeChatBot",
    "Domain",
    "LDAP",
    "ExcludeUsers",
    "ExcludeDepartments",
    "Sync",
    "Account",
    "Schedule",
    "Logging",
]

SECRET_FIELDS = {"corpsecret", "webhook_url", "ldap_password", "default_password"}

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


def _normalize_path(config_path: str = "config.ini") -> str:
    return os.path.abspath(str(config_path or "").strip() or "config.ini")


def _to_bool(value: Any, default: bool = False) -> bool:
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


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, (list, tuple, set)):
        return [str(item).strip() for item in values if str(item).strip()]
    return [item.strip() for item in str(values).split(",") if item.strip()]


def _default_raw_config_values(config_path: str = "config.ini") -> Dict[str, Any]:
    return {
        "config_path": _normalize_path(config_path),
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


def _get_secret_status(parser: configparser.ConfigParser, section: str, option: str) -> bool:
    return bool(parser.get(section, option, fallback="").strip())


def _resolve_secret_value(
    parser: configparser.ConfigParser,
    section: str,
    option: str,
    submitted_value: Any,
) -> str:
    value = str(submitted_value or "")
    if value:
        return value
    return parser.get(section, option, fallback="")


def load_raw_config_values(config_path: str = "config.ini") -> Dict[str, Any]:
    normalized_path = _normalize_path(config_path)
    parser = configparser.ConfigParser()
    parser.read(normalized_path, encoding="utf-8")

    use_ssl = parser.getboolean("LDAP", "UseSSL", fallback=True)
    port = parser.getint("LDAP", "Port", fallback=636 if use_ssl else 389)
    ldap_domain = parser.get("LDAP", "Domain", fallback=parser.get("Domain", "Name", fallback=""))

    values = _default_raw_config_values(normalized_path)
    values.update(
        {
            "source_provider": normalize_source_provider(parser.get("Source", "Provider", fallback="wecom")),
            "corpid": parser.get("WeChat", "CorpID", fallback=""),
            "agentid": parser.get("WeChat", "AgentID", fallback=""),
            "corpsecret": parser.get("WeChat", "CorpSecret", fallback=""),
            "webhook_url": parser.get("WeChatBot", "WebhookUrl", fallback=""),
            "ldap_server": parser.get("LDAP", "Server", fallback=""),
            "ldap_domain": ldap_domain,
            "ldap_username": parser.get("LDAP", "Username", fallback=""),
            "ldap_password": parser.get("LDAP", "Password", fallback=""),
            "ldap_use_ssl": use_ssl,
            "ldap_port": port,
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


def normalize_editable_config_values(
    values: Dict[str, Any],
    *,
    existing: Dict[str, Any] | None = None,
    config_path: str = "config.ini",
) -> Dict[str, Any]:
    normalized_path = _normalize_path(str(values.get("config_path") or (existing or {}).get("config_path") or config_path))
    normalized = _default_raw_config_values(normalized_path)
    if existing:
        normalized.update({key: existing.get(key) for key in normalized.keys() if key in existing})
    normalized["config_path"] = normalized_path
    normalized["source_provider"] = normalize_source_provider(
        values.get("source_provider") or (existing or {}).get("source_provider") or "wecom"
    )

    normalized["corpid"] = str(values.get("corpid") or "").strip()
    normalized["agentid"] = str(values.get("agentid") or "").strip()
    normalized["webhook_url"] = (
        str(values.get("webhook_url") or "").strip()
        if str(values.get("webhook_url") or "").strip()
        else str((existing or {}).get("webhook_url") or "")
    )
    normalized["corpsecret"] = (
        str(values.get("corpsecret") or "").strip()
        if str(values.get("corpsecret") or "").strip()
        else str((existing or {}).get("corpsecret") or "")
    )
    normalized["ldap_server"] = str(values.get("ldap_server") or "").strip()
    normalized["ldap_domain"] = str(values.get("ldap_domain") or "").strip()
    normalized["ldap_username"] = str(values.get("ldap_username") or "").strip()
    normalized["ldap_password"] = (
        str(values.get("ldap_password") or "").strip()
        if str(values.get("ldap_password") or "").strip()
        else str((existing or {}).get("ldap_password") or "")
    )
    normalized["ldap_use_ssl"] = _to_bool(values.get("ldap_use_ssl"), bool((existing or {}).get("ldap_use_ssl", True)))
    normalized["ldap_port"] = _to_int(values.get("ldap_port"), _to_int((existing or {}).get("ldap_port"), 636))
    normalized["ldap_validate_cert"] = _to_bool(
        values.get("ldap_validate_cert"),
        bool((existing or {}).get("ldap_validate_cert", True)),
    )
    normalized["ldap_ca_cert_path"] = str(values.get("ldap_ca_cert_path") or "").strip()
    normalized["default_password"] = (
        str(values.get("default_password") or "").strip()
        if str(values.get("default_password") or "").strip()
        else str((existing or {}).get("default_password") or "")
    )
    normalized["force_change_password"] = _to_bool(
        values.get("force_change_password"),
        bool((existing or {}).get("force_change_password", True)),
    )
    normalized["password_complexity"] = str(values.get("password_complexity") or "strong").strip() or "strong"
    normalized["schedule_time"] = str(values.get("schedule_time") or "03:00").strip() or "03:00"
    normalized["retry_interval"] = _to_int(values.get("retry_interval"), _to_int((existing or {}).get("retry_interval"), 60))
    normalized["max_retries"] = _to_int(values.get("max_retries"), _to_int((existing or {}).get("max_retries"), 3))
    normalized["exclude_accounts"] = merge_protected_ad_accounts(
        _normalize_list(values.get("exclude_accounts", (existing or {}).get("exclude_accounts", [])))
    )
    normalized["exclude_departments"] = _normalize_list(
        values.get("exclude_departments", (existing or {}).get("exclude_departments", []))
    )
    return normalized


def build_editable_config(values: Dict[str, Any], *, config_source: str = "database") -> Dict[str, Any]:
    raw_values = normalize_editable_config_values(values, existing=values, config_path=str(values.get("config_path") or "config.ini"))
    return {
        "config_path": raw_values["config_path"],
        "config_source": config_source,
        "source_provider": raw_values["source_provider"],
        "corpid": raw_values["corpid"],
        "agentid": raw_values["agentid"],
        "corpsecret": "",
        "corpsecret_configured": bool(raw_values["corpsecret"]),
        "webhook_url": "",
        "webhook_url_configured": bool(raw_values["webhook_url"]),
        "ldap_server": raw_values["ldap_server"],
        "ldap_domain": raw_values["ldap_domain"],
        "ldap_username": raw_values["ldap_username"],
        "ldap_password": "",
        "ldap_password_configured": bool(raw_values["ldap_password"]),
        "ldap_use_ssl": raw_values["ldap_use_ssl"],
        "ldap_port": raw_values["ldap_port"],
        "ldap_validate_cert": raw_values["ldap_validate_cert"],
        "ldap_ca_cert_path": raw_values["ldap_ca_cert_path"],
        "default_password": "",
        "default_password_configured": bool(raw_values["default_password"]),
        "force_change_password": raw_values["force_change_password"],
        "password_complexity": raw_values["password_complexity"],
        "schedule_time": raw_values["schedule_time"],
        "retry_interval": raw_values["retry_interval"],
        "max_retries": raw_values["max_retries"],
    }


def build_app_config_from_values(values: Dict[str, Any], *, config_source: str = "database") -> AppConfig:
    raw_values = normalize_editable_config_values(values, existing=values, config_path=str(values.get("config_path") or "config.ini"))
    domain_name = raw_values["ldap_domain"]
    return AppConfig(
        wecom=SourceConnectorConfig(
            corpid=raw_values["corpid"],
            corpsecret=raw_values["corpsecret"],
            agentid=raw_values["agentid"] or None,
        ),
        ldap=LDAPConfig(
            server=raw_values["ldap_server"],
            domain=domain_name,
            username=raw_values["ldap_username"],
            password=raw_values["ldap_password"],
            use_ssl=bool(raw_values["ldap_use_ssl"]),
            port=int(raw_values["ldap_port"]) if raw_values["ldap_port"] else None,
            validate_cert=bool(raw_values["ldap_validate_cert"]),
            ca_cert_path=raw_values["ldap_ca_cert_path"],
        ),
        domain=domain_name,
        source_provider=raw_values["source_provider"],
        account=AccountConfig(
            default_password=raw_values["default_password"],
            force_change_password=bool(raw_values["force_change_password"]),
            password_complexity=raw_values["password_complexity"],
        ),
        exclude_departments=list(raw_values["exclude_departments"]),
        exclude_accounts=list(raw_values["exclude_accounts"]),
        webhook_url=raw_values["webhook_url"],
        config_path=str(config_source or raw_values["config_path"]),
    )


def load_editable_config(config_path: str = "config.ini") -> Dict[str, Any]:
    normalized_path = _normalize_path(config_path)
    return build_editable_config(load_raw_config_values(normalized_path), config_source=normalized_path)


def save_editable_config(values: Dict[str, Any], config_path: str = "config.ini") -> str:
    normalized_path = _normalize_path(config_path)
    existing = load_raw_config_values(normalized_path) if os.path.exists(normalized_path) else _default_raw_config_values(normalized_path)
    raw_values = normalize_editable_config_values(values, existing=existing, config_path=normalized_path)

    parser = configparser.ConfigParser()
    if os.path.exists(normalized_path):
        parser.read(normalized_path, encoding="utf-8")

    for section in DEFAULT_SECTIONS:
        if not parser.has_section(section):
            parser.add_section(section)

    parser.set("WeChat", "CorpID", raw_values["corpid"])
    parser.set("Source", "Provider", raw_values["source_provider"])
    parser.set("WeChat", "AgentID", raw_values["agentid"])
    parser.set("WeChat", "CorpSecret", raw_values["corpsecret"])
    parser.set("WeChatBot", "WebhookUrl", raw_values["webhook_url"])

    parser.set("LDAP", "Server", raw_values["ldap_server"])
    parser.set("LDAP", "Domain", raw_values["ldap_domain"])
    parser.set("LDAP", "Username", raw_values["ldap_username"])
    parser.set("LDAP", "Password", raw_values["ldap_password"])
    parser.set("LDAP", "UseSSL", str(bool(raw_values["ldap_use_ssl"])).lower())
    parser.set("LDAP", "Port", str(int(raw_values["ldap_port"] or 636)))
    parser.set("LDAP", "ValidateCert", str(bool(raw_values["ldap_validate_cert"])).lower())
    parser.set("LDAP", "CACertPath", raw_values["ldap_ca_cert_path"])

    parser.set("Domain", "Name", raw_values["ldap_domain"])
    parser.set("Schedule", "Time", raw_values["schedule_time"])
    parser.set("Schedule", "RetryInterval", str(int(raw_values["retry_interval"] or 60)))
    parser.set("Schedule", "MaxRetries", str(int(raw_values["max_retries"] or 3)))

    parser.set("ExcludeUsers", "SystemAccounts", ",".join(DEFAULT_PROTECTED_AD_ACCOUNTS))
    parser.set(
        "ExcludeUsers",
        "CustomAccounts",
        ",".join(filter_custom_protected_ad_accounts(raw_values["exclude_accounts"])),
    )
    parser.set("ExcludeDepartments", "Names", ",".join(raw_values["exclude_departments"]))

    if "ForceFullSync" not in parser["Sync"]:
        parser.set("Sync", "ForceFullSync", "false")
    if "SyncMode" not in parser["Sync"]:
        parser.set("Sync", "SyncMode", "full")
    if "KeepHistoryDays" not in parser["Sync"]:
        parser.set("Sync", "KeepHistoryDays", "30")

    parser.set("Account", "DefaultPassword", raw_values["default_password"])
    parser.set("Account", "ForceChangePassword", str(bool(raw_values["force_change_password"])).lower())
    parser.set("Account", "PasswordComplexity", raw_values["password_complexity"])

    if "Level" not in parser["Logging"]:
        parser.set("Logging", "Level", "INFO")
    if "DetailedLogging" not in parser["Logging"]:
        parser.set("Logging", "DetailedLogging", "true")
    if "KeepLogsDays" not in parser["Logging"]:
        parser.set("Logging", "KeepLogsDays", "30")

    with open(normalized_path, "w", encoding="utf-8") as handle:
        parser.write(handle)

    return normalized_path
