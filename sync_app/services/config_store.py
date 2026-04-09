import configparser
import os
from typing import Any, Dict, Iterable

from sync_app.core.directory_protection import (
    DEFAULT_PROTECTED_AD_ACCOUNTS,
    filter_custom_protected_ad_accounts,
)
from sync_app.storage.config_codec import (
    ORGANIZATION_CONFIG_VALUE_TYPES,
    build_app_config_from_org_values,
    build_editable_org_config,
    default_org_config_values,
    load_org_config_values_from_file,
    normalize_config_path,
    normalize_org_config_values,
)


DEFAULT_SECTIONS = [
    "Source",
    "SourceConnector",
    "Notification",
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

def load_raw_config_values(config_path: str = "config.ini") -> Dict[str, Any]:
    return load_org_config_values_from_file(config_path)


def normalize_editable_config_values(
    values: Dict[str, Any],
    *,
    existing: Dict[str, Any] | None = None,
    config_path: str = "config.ini",
) -> Dict[str, Any]:
    return normalize_org_config_values(
        values,
        existing=existing,
        config_path=config_path,
    )


def build_editable_config(values: Dict[str, Any], *, config_source: str = "database") -> Dict[str, Any]:
    return build_editable_org_config(values, config_source=config_source)


def build_app_config_from_values(values: Dict[str, Any], *, config_source: str = "database"):
    return build_app_config_from_org_values(values, config_source=config_source)


def load_editable_config(config_path: str = "config.ini") -> Dict[str, Any]:
    normalized_path = normalize_config_path(config_path)
    return build_editable_config(load_raw_config_values(normalized_path), config_source=normalized_path)


def save_editable_config(values: Dict[str, Any], config_path: str = "config.ini") -> str:
    normalized_path = normalize_config_path(config_path)
    existing = (
        load_raw_config_values(normalized_path)
        if os.path.exists(normalized_path)
        else default_org_config_values(normalized_path)
    )
    raw_values = normalize_editable_config_values(values, existing=existing, config_path=normalized_path)

    parser = configparser.ConfigParser()
    if os.path.exists(normalized_path):
        parser.read(normalized_path, encoding="utf-8")

    for section in DEFAULT_SECTIONS:
        if not parser.has_section(section):
            parser.add_section(section)

    parser.set("SourceConnector", "CorpID", raw_values["corpid"])
    parser.set("SourceConnector", "AgentID", raw_values["agentid"])
    parser.set("SourceConnector", "CorpSecret", raw_values["corpsecret"])
    parser.set("Notification", "WebhookUrl", raw_values["webhook_url"])

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
