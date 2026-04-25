from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional


@dataclass(slots=True)
class SourceConnectorConfig:
    corpid: str
    corpsecret: str
    agentid: Optional[str] = None

    def to_dict(self, *, include_secrets: bool = True) -> Dict[str, Any]:
        data = asdict(self)
        if not include_secrets:
            data["corpsecret"] = "***"
        return data

SourceConfig = SourceConnectorConfig
WeComConfig = SourceConnectorConfig


@dataclass(slots=True)
class LDAPConfig:
    server: str
    domain: str
    username: str
    password: str
    use_ssl: bool = True
    port: Optional[int] = None
    validate_cert: bool = True
    ca_cert_path: str = ""

    def to_dict(self, *, include_secrets: bool = True) -> Dict[str, Any]:
        data = asdict(self)
        if not include_secrets:
            data["password"] = "***"
        return data

@dataclass(slots=True)
class AccountConfig:
    default_password: str = ""
    force_change_password: bool = True
    password_complexity: str = "strong"

    def to_dict(self, *, include_secrets: bool = True) -> Dict[str, Any]:
        data = asdict(self)
        if not include_secrets:
            data["default_password"] = "***" if self.default_password else ""
        return data

@dataclass(slots=True, init=False)
class AppConfig:
    source_connector: SourceConnectorConfig
    ldap: LDAPConfig
    domain: str
    source_provider: str = "wecom"
    account: AccountConfig = field(default_factory=AccountConfig)
    exclude_departments: list[str] = field(default_factory=list)
    exclude_accounts: list[str] = field(default_factory=list)
    webhook_url: str = ""
    config_path: str = "config.ini"

    def __init__(
        self,
        source_connector: SourceConnectorConfig | None = None,
        *,
        wecom: SourceConnectorConfig | None = None,
        ldap: LDAPConfig,
        domain: str,
        source_provider: str = "wecom",
        account: AccountConfig | None = None,
        exclude_departments: list[str] | None = None,
        exclude_accounts: list[str] | None = None,
        webhook_url: str = "",
        config_path: str = "config.ini",
    ) -> None:
        resolved_source_connector = source_connector or wecom
        if resolved_source_connector is None:
            raise TypeError("source_connector is required")
        self.source_connector = resolved_source_connector
        self.ldap = ldap
        self.domain = str(domain or "")
        self.source_provider = str(source_provider or "wecom").strip() or "wecom"
        self.account = account if account is not None else AccountConfig()
        self.exclude_departments = list(exclude_departments or [])
        self.exclude_accounts = list(exclude_accounts or [])
        self.webhook_url = str(webhook_url or "")
        self.config_path = str(config_path or "config.ini") or "config.ini"

    def to_dict(self, *, include_secrets: bool = True) -> Dict[str, Any]:
        source_connector = self.source_connector.to_dict(include_secrets=include_secrets)
        return {
            "source_connector": source_connector,
            "wecom": source_connector,
            "ldap": self.ldap.to_dict(include_secrets=include_secrets),
            "domain": self.domain,
            "source_provider": self.source_provider,
            "account": self.account.to_dict(include_secrets=include_secrets),
            "exclude_departments": list(self.exclude_departments),
            "exclude_accounts": list(self.exclude_accounts),
            "webhook_url": self.webhook_url if include_secrets else ("***" if self.webhook_url else ""),
            "config_path": self.config_path,
        }

    def to_hash_payload(self) -> Dict[str, Any]:
        return self.to_dict(include_secrets=True)

    def to_public_dict(self) -> Dict[str, Any]:
        return self.to_dict(include_secrets=False)

    def to_json(self, *, include_secrets: bool = True) -> str:
        return json.dumps(self.to_dict(include_secrets=include_secrets), ensure_ascii=False, sort_keys=True)

    @property
    def wecom(self) -> SourceConnectorConfig:
        return self.source_connector

    @wecom.setter
    def wecom(self, value: SourceConnectorConfig) -> None:
        self.source_connector = value
