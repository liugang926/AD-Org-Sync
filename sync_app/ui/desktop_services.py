from __future__ import annotations

import configparser
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from sync_app.services.typed_settings import DirectoryUiSettings
from sync_app.storage.local_db import (
    DatabaseManager,
    GroupExclusionRuleRepository,
    SettingsRepository,
)


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


def _ensure_config_sections(parser: configparser.ConfigParser, sections: tuple[str, ...]) -> None:
    for section in sections:
        if not parser.has_section(section):
            parser.add_section(section)


def _safe_get_int(
    parser: configparser.ConfigParser,
    section: str,
    option: str,
    *,
    fallback: int,
) -> int:
    try:
        return parser.getint(section, option, fallback=fallback)
    except (TypeError, ValueError):
        return fallback


def _normalize_schedule_time(raw_value: str) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return "03:00"
    try:
        hour_text, minute_text = value.split(":", 1)
        hour = max(0, min(int(hour_text), 23))
        minute = max(0, min(int(minute_text), 59))
    except (TypeError, ValueError):
        return "03:00"
    return f"{hour:02d}:{minute:02d}"


DEFAULT_DESKTOP_CONFIG = """[Source]
Provider = wecom

[SourceConnector]
CorpID =
AgentID =
CorpSecret =

[Notification]
WebhookUrl =

[WeChat]
CorpID = 
AgentID =
CorpSecret = 

[WeChatBot]
WebhookUrl = 

[Domain]
Name = 

[LDAP]
# LDAP服务器地址（域控制器地址）
Server = dc.example.com
# 域名
Domain = example.com
# 管理员用户名（格式：DOMAIN\\username 或 username@domain）
Username = DOMAIN\\administrator
# 管理员密码
Password = 
# 是否使用SSL/TLS加密连接
UseSSL = true
# LDAP端口（默认：636用于LDAPS，389用于LDAP）
Port = 636

[ExcludeUsers]
SystemAccounts = admin,administrator,guest,krbtgt
CustomAccounts = 

[ExcludeDepartments]
Names = 

[Sync]
ForceFullSync = false
SyncMode = full
KeepHistoryDays = 30

[Account]
DefaultPassword =
ForceChangePassword = true
PasswordComplexity = strong

[Schedule]
Time = 03:00
RetryInterval = 60
MaxRetries = 3

[Logging]
Level = INFO
DetailedLogging = true
KeepLogsDays = 30
"""


@dataclass(frozen=True)
class DesktopConfigValues:
    corp_id: str = ""
    corp_secret: str = ""
    webhook_url: str = ""
    ldap_server: str = ""
    ldap_domain: str = ""
    ldap_username: str = ""
    ldap_password: str = ""
    ldap_use_ssl: bool = True
    ldap_port: int = 636
    schedule_time: str = "03:00"
    retry_interval: int = 60
    max_retries: int = 3


@dataclass(frozen=True)
class DesktopLocalStrategyValues:
    group_display_separator: str = "-"
    group_recursive_enabled: bool = True
    managed_relation_cleanup_enabled: bool = False
    schedule_execution_mode: str = "apply"
    protected_rules: list[dict[str, Any]] = field(default_factory=list)
    soft_excluded_rules: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class DesktopLocalStorageState:
    db_manager: Optional[DatabaseManager] = None
    settings_repo: Optional[SettingsRepository] = None
    rule_repo: Optional[GroupExclusionRuleRepository] = None
    error: Optional[str] = None
    init_result: dict[str, Any] = field(default_factory=dict)


class DesktopConfigService:
    def __init__(self, app_path: str):
        self.app_path = os.path.abspath(app_path)
        self.config_path = os.path.join(self.app_path, "config.ini")

    def ensure_config_file(self) -> bool:
        if os.path.exists(self.config_path):
            return False

        with open(self.config_path, "w", encoding="utf-8") as config_file:
            config_file.write(DEFAULT_DESKTOP_CONFIG)
        return True

    def load(self) -> DesktopConfigValues:
        config = configparser.ConfigParser()
        config.read(self.config_path, encoding="utf-8")

        ldap_use_ssl = config.getboolean("LDAP", "UseSSL", fallback=True)
        ldap_port = _safe_get_int(
            config,
            "LDAP",
            "Port",
            fallback=636 if ldap_use_ssl else 389,
        )

        return DesktopConfigValues(
            corp_id=_get_config_value(config, ("SourceConnector", "WeChat"), "CorpID", fallback=""),
            corp_secret=_get_config_value(config, ("SourceConnector", "WeChat"), "CorpSecret", fallback=""),
            webhook_url=_get_config_value(config, ("Notification", "WeChatBot"), "WebhookUrl", fallback=""),
            ldap_server=config.get("LDAP", "Server", fallback=""),
            ldap_domain=config.get("LDAP", "Domain", fallback=config.get("Domain", "Name", fallback="")),
            ldap_username=config.get("LDAP", "Username", fallback=""),
            ldap_password=config.get("LDAP", "Password", fallback=""),
            ldap_use_ssl=ldap_use_ssl,
            ldap_port=ldap_port,
            schedule_time=_normalize_schedule_time(config.get("Schedule", "Time", fallback="03:00")),
            retry_interval=_safe_get_int(config, "Schedule", "RetryInterval", fallback=60),
            max_retries=_safe_get_int(config, "Schedule", "MaxRetries", fallback=3),
        )

    def save(self, values: DesktopConfigValues) -> str:
        config = configparser.ConfigParser()
        if os.path.exists(self.config_path):
            config.read(self.config_path, encoding="utf-8")

        _ensure_config_sections(
            config,
            (
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
            ),
        )

        source_provider = _get_config_value(config, ("Source",), "Provider", fallback="wecom").strip() or "wecom"
        agent_id = _get_config_value(config, ("SourceConnector", "WeChat"), "AgentID", fallback="")

        config.set("Source", "Provider", source_provider)
        config.set("SourceConnector", "CorpID", values.corp_id)
        config.set("SourceConnector", "CorpSecret", values.corp_secret)
        config.set("SourceConnector", "AgentID", agent_id)
        config.set("Notification", "WebhookUrl", values.webhook_url)

        config.set("WeChat", "CorpID", values.corp_id)
        config.set("WeChat", "CorpSecret", values.corp_secret)
        config.set("WeChat", "AgentID", agent_id)
        config.set("WeChatBot", "WebhookUrl", values.webhook_url)

        config.set("LDAP", "Server", values.ldap_server)
        config.set("LDAP", "Domain", values.ldap_domain)
        config.set("LDAP", "Username", values.ldap_username)
        config.set("LDAP", "Password", values.ldap_password)
        config.set("LDAP", "UseSSL", str(bool(values.ldap_use_ssl)).lower())
        config.set("LDAP", "Port", str(max(int(values.ldap_port or 0), 1)))

        config.set("Domain", "Name", values.ldap_domain)

        if "SystemAccounts" not in config["ExcludeUsers"]:
            config.set("ExcludeUsers", "SystemAccounts", "admin,administrator,guest,krbtgt")
        if "CustomAccounts" not in config["ExcludeUsers"]:
            config.set("ExcludeUsers", "CustomAccounts", "")
        if "Names" not in config["ExcludeDepartments"]:
            config.set("ExcludeDepartments", "Names", "")

        if "ForceFullSync" not in config["Sync"]:
            config.set("Sync", "ForceFullSync", "false")
        if "SyncMode" not in config["Sync"]:
            config.set("Sync", "SyncMode", "full")
        if "KeepHistoryDays" not in config["Sync"]:
            config.set("Sync", "KeepHistoryDays", "30")

        if "DefaultPassword" not in config["Account"]:
            config.set("Account", "DefaultPassword", "")
        if "ForceChangePassword" not in config["Account"]:
            config.set("Account", "ForceChangePassword", "true")
        if "PasswordComplexity" not in config["Account"]:
            config.set("Account", "PasswordComplexity", "strong")

        config.set("Schedule", "Time", _normalize_schedule_time(values.schedule_time))
        config.set("Schedule", "RetryInterval", str(max(int(values.retry_interval or 0), 0)))
        config.set("Schedule", "MaxRetries", str(max(int(values.max_retries or 0), 0)))

        if "Level" not in config["Logging"]:
            config.set("Logging", "Level", "INFO")
        if "DetailedLogging" not in config["Logging"]:
            config.set("Logging", "DetailedLogging", "true")
        if "KeepLogsDays" not in config["Logging"]:
            config.set("Logging", "KeepLogsDays", "30")

        with open(self.config_path, "w", encoding="utf-8") as config_file:
            config.write(config_file)

        return self.config_path


class DesktopLocalStrategyService:
    def __init__(
        self,
        *,
        db_factory: Callable[[], DatabaseManager] = DatabaseManager,
        logger: Optional[logging.Logger] = None,
    ):
        self._db_factory = db_factory
        self._logger = logger or logging.getLogger(__name__)

    def initialize(self) -> DesktopLocalStorageState:
        state = DesktopLocalStorageState()
        try:
            state.db_manager = self._db_factory()
            state.init_result = state.db_manager.initialize() or {}
            state.settings_repo = SettingsRepository(state.db_manager)
            state.rule_repo = GroupExclusionRuleRepository(state.db_manager)
        except Exception as exc:
            state.error = str(exc)
            self._logger.error("本地配置库初始化失败: %s", exc)
        return state

    def load(self, state: DesktopLocalStorageState) -> DesktopLocalStrategyValues:
        if state.error:
            raise RuntimeError(state.error)
        if not state.settings_repo or not state.rule_repo:
            return DesktopLocalStrategyValues()

        directory_ui_settings = DirectoryUiSettings.load(state.settings_repo)
        return DesktopLocalStrategyValues(
            group_display_separator=directory_ui_settings.group_display_separator,
            group_recursive_enabled=directory_ui_settings.group_recursive_enabled,
            managed_relation_cleanup_enabled=directory_ui_settings.managed_relation_cleanup_enabled,
            schedule_execution_mode=directory_ui_settings.schedule_execution_mode,
            protected_rules=[
                dict(row)
                for row in state.rule_repo.list_rules(rule_type="protect", protection_level="hard")
                if row["is_enabled"]
            ],
            soft_excluded_rules=[dict(row) for row in state.rule_repo.list_soft_excluded_rules()],
        )

    def save(self, state: DesktopLocalStorageState, values: DesktopLocalStrategyValues) -> None:
        if state.error:
            raise RuntimeError(state.error)
        if not state.settings_repo or not state.rule_repo:
            return

        DirectoryUiSettings(
            group_display_separator=values.group_display_separator,
            group_recursive_enabled=bool(values.group_recursive_enabled),
            managed_relation_cleanup_enabled=bool(values.managed_relation_cleanup_enabled),
            schedule_execution_mode=values.schedule_execution_mode,
        ).persist(state.settings_repo)
        state.rule_repo.replace_soft_excluded_rules(list(values.soft_excluded_rules))

    def build_summary(self, state: DesktopLocalStorageState) -> str:
        if state.error:
            return f"本地策略存储不可用: {state.error}"
        if not state.rule_repo or not state.db_manager:
            return "本地策略存储尚未初始化"

        enabled_rules = state.rule_repo.list_enabled_rules()
        hard_protected = [
            row
            for row in state.rule_repo.list_rules(rule_type="protect", protection_level="hard")
            if row["is_enabled"]
        ]
        soft_excluded = [
            row
            for row in state.rule_repo.list_rules(rule_type="exclude", protection_level="soft")
            if row["is_enabled"]
        ]

        integrity_info = (state.init_result or {}).get("integrity_check") or {}
        integrity_text = ""
        if integrity_info:
            integrity_text = (
                f" | 完整性检查 {'通过' if integrity_info.get('ok') else integrity_info.get('result', '未知')}"
            )

        extra_notes = []
        if (state.init_result or {}).get("migration_source_path"):
            extra_notes.append("已迁移旧库")
        if (state.init_result or {}).get("startup_snapshot_path"):
            extra_notes.append("已创建启动快照")
        extra_text = f" | {' | '.join(extra_notes)}" if extra_notes else ""

        return (
            f"SQLite: {state.db_manager.db_path} | 备份目录 {state.db_manager.backup_dir} | "
            f"启用规则 {len(enabled_rules)} 条 | 硬保护组 {len(hard_protected)} | 软排除组 {len(soft_excluded)}"
            f"{integrity_text}{extra_text}"
        )

    def run_integrity_check(self, state: DesktopLocalStorageState) -> dict[str, Any]:
        if not state.db_manager:
            raise RuntimeError("本地 SQLite 数据库尚未初始化。")

        result = state.db_manager.run_integrity_check()
        state.init_result = dict(state.init_result or {})
        state.init_result["integrity_check"] = result
        return result

    def create_backup(self, state: DesktopLocalStorageState, *, label: str = "manual_ui") -> str:
        if not state.db_manager:
            raise RuntimeError("本地 SQLite 数据库尚未初始化。")
        return state.db_manager.backup_database(label=label)
