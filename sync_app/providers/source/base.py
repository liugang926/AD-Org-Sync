from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from sync_app.core.models import DepartmentNode, SourceDirectoryUser

DEFAULT_SOURCE_PROVIDER = "wecom"


@dataclass(frozen=True)
class SourceProviderFieldDefinition:
    name: str
    label: str
    input_type: str = "text"
    help_text: str = ""
    placeholder: str = ""
    required: bool = False
    secret: bool = False
    width: str = "half"
    autocomplete: str = ""


@dataclass(frozen=True)
class SourceProviderSchema:
    provider_id: str
    display_name: str
    description: str = ""
    implemented: bool = False
    implementation_status: str = ""
    connection_fields: tuple[SourceProviderFieldDefinition, ...] = ()
    notification_fields: tuple[SourceProviderFieldDefinition, ...] = ()


SOURCE_PROVIDER_SCHEMAS = {
    "wecom": SourceProviderSchema(
        provider_id="wecom",
        display_name="WeCom",
        description="Use a WeCom self-built application to read departments and users.",
        implemented=True,
        connection_fields=(
            SourceProviderFieldDefinition(
                "corpid",
                "CorpID",
                help_text="Your WeCom Corporate ID",
                placeholder="ww1234567890abcdef",
                required=True,
            ),
            SourceProviderFieldDefinition(
                "agentid",
                "AgentID",
                help_text="Application AgentID",
                placeholder="1000002",
            ),
            SourceProviderFieldDefinition(
                "corpsecret",
                "CorpSecret",
                input_type="password",
                help_text="The secret for the self-built application.",
                placeholder="Enter CorpSecret",
                required=True,
                secret=True,
                width="full",
                autocomplete="new-password",
            ),
        ),
        notification_fields=(
            SourceProviderFieldDefinition(
                "webhook_url",
                "WeCom Webhook",
                input_type="password",
                help_text="Optional markdown robot webhook for operational notifications.",
                placeholder="Enter webhook URL",
                secret=True,
                width="full",
                autocomplete="off",
            ),
        ),
    ),
    "dingtalk": SourceProviderSchema(
        provider_id="dingtalk",
        display_name="DingTalk",
        description="Use a DingTalk internal application to read departments and users.",
        implemented=True,
        connection_fields=(
            SourceProviderFieldDefinition(
                "corpid",
                "AppKey / Client ID",
                help_text="The DingTalk application key or client ID.",
                placeholder="Enter AppKey",
                required=True,
            ),
            SourceProviderFieldDefinition(
                "agentid",
                "Agent ID",
                help_text="Optional application or suite identifier.",
                placeholder="Enter Agent ID",
            ),
            SourceProviderFieldDefinition(
                "corpsecret",
                "AppSecret / Client Secret",
                input_type="password",
                help_text="The DingTalk application secret.",
                placeholder="Enter AppSecret",
                required=True,
                secret=True,
                width="full",
                autocomplete="new-password",
            ),
        ),
        notification_fields=(
            SourceProviderFieldDefinition(
                "webhook_url",
                "DingTalk Bot Webhook",
                input_type="password",
                help_text="Optional bot webhook for notification delivery.",
                placeholder="Enter webhook URL",
                secret=True,
                width="full",
                autocomplete="off",
            ),
        ),
    ),
    "feishu": SourceProviderSchema(
        provider_id="feishu",
        display_name="Feishu",
        description="Reserve the connector contract for a Feishu source adapter.",
        implementation_status="Feishu provider schema is available, but the runtime adapter is not implemented in this build.",
        connection_fields=(
            SourceProviderFieldDefinition(
                "corpid",
                "App ID",
                help_text="The Feishu application ID.",
                placeholder="cli_xxxxxxxxxxxxxxxx",
                required=True,
            ),
            SourceProviderFieldDefinition(
                "agentid",
                "App Token / Tenant Key",
                help_text="Optional tenant-scoped application token.",
                placeholder="Enter App Token",
            ),
            SourceProviderFieldDefinition(
                "corpsecret",
                "App Secret",
                input_type="password",
                help_text="The Feishu application secret.",
                placeholder="Enter App Secret",
                required=True,
                secret=True,
                width="full",
                autocomplete="new-password",
            ),
        ),
        notification_fields=(
            SourceProviderFieldDefinition(
                "webhook_url",
                "Feishu Bot Webhook",
                input_type="password",
                help_text="Optional bot webhook for notification delivery.",
                placeholder="Enter webhook URL",
                secret=True,
                width="full",
                autocomplete="off",
            ),
        ),
    ),
}
SOURCE_PROVIDER_DISPLAY_NAMES = {
    provider_id: schema.display_name for provider_id, schema in SOURCE_PROVIDER_SCHEMAS.items()
}


def normalize_source_provider(value: str | None, *, default: str = DEFAULT_SOURCE_PROVIDER) -> str:
    normalized = str(value or "").strip().lower()
    return normalized or default


def get_source_provider_display_name(value: str | None) -> str:
    normalized = normalize_source_provider(value)
    return SOURCE_PROVIDER_DISPLAY_NAMES.get(normalized, normalized or DEFAULT_SOURCE_PROVIDER)


def get_source_provider_schema(value: str | None) -> SourceProviderSchema:
    normalized = normalize_source_provider(value)
    return SOURCE_PROVIDER_SCHEMAS.get(normalized, SOURCE_PROVIDER_SCHEMAS[DEFAULT_SOURCE_PROVIDER])


def list_source_provider_schemas(*, include_unimplemented: bool = True) -> list[SourceProviderSchema]:
    schemas = list(SOURCE_PROVIDER_SCHEMAS.values())
    if include_unimplemented:
        return schemas
    return [schema for schema in schemas if schema.implemented]


def list_source_provider_options(*, include_unimplemented: bool = True) -> list[tuple[str, str]]:
    options: list[tuple[str, str]] = []
    for schema in list_source_provider_schemas(include_unimplemented=include_unimplemented):
        label = schema.display_name
        if include_unimplemented and not schema.implemented:
            label = f"{label} (planned)"
        options.append((schema.provider_id, label))
    return options


def get_source_provider_secret_field_names(value: str | None) -> set[str]:
    schema = get_source_provider_schema(value)
    return {
        field.name
        for field in (*schema.connection_fields, *schema.notification_fields)
        if field.secret
    }


class SourceDirectoryProvider(ABC):
    provider_id = DEFAULT_SOURCE_PROVIDER
    display_name = "Source Provider"

    @abstractmethod
    def list_departments(self) -> list[DepartmentNode]:
        raise NotImplementedError

    @abstractmethod
    def list_department_users(self, department_id: int) -> list[SourceDirectoryUser]:
        raise NotImplementedError

    @abstractmethod
    def get_user_detail(self, user_id: str) -> dict[str, Any]:
        raise NotImplementedError

    def search_users(self, query: str, *, limit: int = 20) -> list[SourceDirectoryUser]:
        normalized_query = str(query or "").strip().lower()
        if not normalized_query:
            return []
        seen_users: dict[str, SourceDirectoryUser] = {}
        departments = self.list_departments()
        for department in departments:
            try:
                department_users = self.list_department_users(int(department.department_id))
            except Exception:
                continue
            for user in department_users:
                user_id = str(user.source_user_id or "").strip()
                if not user_id:
                    continue
                existing = seen_users.get(user_id)
                if existing is None:
                    seen_users[user_id] = user
                else:
                    existing.merge_payload(user.raw_payload)
                    merged_departments = {int(value) for value in existing.departments if str(value).strip()}
                    merged_departments.update(int(value) for value in user.departments if str(value).strip())
                    existing.departments = sorted(merged_departments)
        matches: list[SourceDirectoryUser] = []
        for user in seen_users.values():
            haystack = " ".join(
                [
                    str(user.source_user_id or ""),
                    str(user.userid or ""),
                    str(user.name or ""),
                    str(user.email or ""),
                ]
            ).lower()
            if normalized_query in haystack:
                matches.append(user)
        matches.sort(key=lambda item: (str(item.name or "").lower(), str(item.source_user_id or "").lower()))
        return matches[: max(int(limit or 20), 1)]

    def update_user(self, user_id: str, updates: dict[str, Any]) -> bool:
        raise NotImplementedError(f"{self.provider_id} does not support user updates")

    def list_tag_records(self) -> list[dict[str, Any]]:
        return []

    def get_tag_users(self, tag_id: str | int) -> dict[str, Any]:
        return {"userlist": []}

    def list_external_group_chats(self, *, status_filter: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        return []

    def get_external_group_chat(self, chat_id: str) -> dict[str, Any]:
        return {}

    def close(self) -> None:
        return None

    def __enter__(self) -> "SourceDirectoryProvider":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
