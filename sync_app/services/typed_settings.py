from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from sync_app.core.sync_policies import MANAGED_GROUP_TYPES
from sync_app.storage.local_db import normalize_org_id

SECURE_COOKIE_MODES = {"auto", "always", "never"}
SCHEDULE_EXECUTION_MODES = {"apply", "dry_run"}


def normalize_secure_cookie_mode(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in SECURE_COOKIE_MODES else "auto"


def normalize_schedule_execution_mode(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in SCHEDULE_EXECUTION_MODES else "apply"


def _clean_public_base_url(value: Any) -> str:
    return str(value or "").strip().rstrip("/")


def _clean_text(value: Any, default: str = "") -> str:
    normalized = str(value or "").strip()
    return normalized or default


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _coerce_int(value: Any, default: int, *, minimum: int | None = None) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        normalized = int(default)
    if minimum is not None:
        normalized = max(normalized, int(minimum))
    return normalized


def _coerce_float(value: Any, default: float, *, minimum: float | None = None) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        normalized = float(default)
    if minimum is not None:
        normalized = max(normalized, float(minimum))
    return normalized


def _coerce_raw_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    normalized = str(value)
    if normalized == "":
        return default
    return normalized


def _normalize_managed_group_type(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in MANAGED_GROUP_TYPES else "security"


def _bool_setting(value: bool) -> str:
    return "true" if bool(value) else "false"


@dataclass(frozen=True, slots=True)
class WebRuntimeSettings:
    bind_host: str = "127.0.0.1"
    bind_port: int = 8000
    public_base_url: str = ""
    session_cookie_secure_mode: str = "auto"
    trust_proxy_headers: bool = False
    forwarded_allow_ips: str = "127.0.0.1"

    @classmethod
    def load(
        cls,
        settings_repo: Any,
        *,
        bind_host: str | None = None,
        bind_port: int | None = None,
        public_base_url: str | None = None,
        session_cookie_secure_mode: str | None = None,
        trust_proxy_headers: bool | None = None,
        forwarded_allow_ips: str | None = None,
    ) -> "WebRuntimeSettings":
        return cls(
            bind_host=_clean_text(
                bind_host if bind_host is not None else settings_repo.get_value("web_bind_host", "127.0.0.1"),
                "127.0.0.1",
            ),
            bind_port=_coerce_int(
                bind_port if bind_port is not None else settings_repo.get_int("web_bind_port", 8000),
                8000,
                minimum=1,
            ),
            public_base_url=_clean_public_base_url(
                public_base_url if public_base_url is not None else settings_repo.get_value("web_public_base_url", "")
            ),
            session_cookie_secure_mode=normalize_secure_cookie_mode(
                session_cookie_secure_mode
                if session_cookie_secure_mode is not None
                else settings_repo.get_value("web_session_cookie_secure_mode", "auto")
            ),
            trust_proxy_headers=(
                bool(trust_proxy_headers)
                if trust_proxy_headers is not None
                else settings_repo.get_bool("web_trust_proxy_headers", False)
            ),
            forwarded_allow_ips=_clean_text(
                forwarded_allow_ips
                if forwarded_allow_ips is not None
                else settings_repo.get_value("web_forwarded_allow_ips", "127.0.0.1"),
                "127.0.0.1",
            ),
        )

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "WebRuntimeSettings":
        data = dict(values or {})
        return cls(
            bind_host=_clean_text(data.get("web_bind_host"), "127.0.0.1"),
            bind_port=_coerce_int(data.get("web_bind_port"), 8000, minimum=1),
            public_base_url=_clean_public_base_url(data.get("web_public_base_url")),
            session_cookie_secure_mode=normalize_secure_cookie_mode(data.get("web_session_cookie_secure_mode")),
            trust_proxy_headers=_coerce_bool(data.get("web_trust_proxy_headers"), False),
            forwarded_allow_ips=_clean_text(data.get("web_forwarded_allow_ips"), "127.0.0.1"),
        )

    def to_dict(self) -> dict[str, Any]:
        return dict(asdict(self))

    def persist(self, settings_repo: Any) -> None:
        settings_repo.set_value("web_bind_host", self.bind_host, "string")
        settings_repo.set_value("web_bind_port", str(self.bind_port), "int")
        settings_repo.set_value("web_public_base_url", self.public_base_url, "string")
        settings_repo.set_value("web_session_cookie_secure_mode", self.session_cookie_secure_mode, "string")
        settings_repo.set_value("web_trust_proxy_headers", _bool_setting(self.trust_proxy_headers), "bool")
        settings_repo.set_value("web_forwarded_allow_ips", self.forwarded_allow_ips, "string")


@dataclass(frozen=True, slots=True)
class WebSecuritySettings:
    session_idle_minutes: int = 30
    login_max_attempts: int = 5
    login_window_seconds: int = 300
    login_lockout_seconds: int = 300
    admin_password_min_length: int = 8

    @classmethod
    def load(cls, settings_repo: Any) -> "WebSecuritySettings":
        return cls(
            session_idle_minutes=_coerce_int(
                settings_repo.get_int("web_session_idle_minutes", 30),
                30,
                minimum=1,
            ),
            login_max_attempts=_coerce_int(
                settings_repo.get_int("web_login_max_attempts", 5),
                5,
                minimum=1,
            ),
            login_window_seconds=_coerce_int(
                settings_repo.get_int("web_login_window_seconds", 300),
                300,
                minimum=1,
            ),
            login_lockout_seconds=_coerce_int(
                settings_repo.get_int("web_login_lockout_seconds", 300),
                300,
                minimum=1,
            ),
            admin_password_min_length=_coerce_int(
                settings_repo.get_int("web_admin_password_min_length", 8),
                8,
                minimum=1,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return dict(asdict(self))


@dataclass(frozen=True, slots=True)
class BrandingSettings:
    brand_display_name: str = "AD Org Sync"
    brand_mark_text: str = "AD"
    brand_attribution: str = ""

    @classmethod
    def load(
        cls,
        settings_repo: Any,
        *,
        default_display_name: str,
        default_mark_text: str,
        default_attribution: str,
    ) -> "BrandingSettings":
        return cls(
            brand_display_name=_clean_text(
                settings_repo.get_value("brand_display_name", default_display_name),
                default_display_name,
            ),
            brand_mark_text=_clean_text(
                settings_repo.get_value("brand_mark_text", default_mark_text),
                default_mark_text,
            ),
            brand_attribution=_clean_text(
                settings_repo.get_value("brand_attribution", default_attribution),
                default_attribution,
            ),
        )

    @classmethod
    def from_mapping(
        cls,
        values: Mapping[str, Any],
        *,
        default_display_name: str,
        default_mark_text: str,
        default_attribution: str,
    ) -> "BrandingSettings":
        data = dict(values or {})
        return cls(
            brand_display_name=_clean_text(data.get("brand_display_name"), default_display_name),
            brand_mark_text=_clean_text(data.get("brand_mark_text"), default_mark_text),
            brand_attribution=_clean_text(data.get("brand_attribution"), default_attribution),
        )

    def to_dict(self) -> dict[str, Any]:
        return dict(asdict(self))

    def persist(self, settings_repo: Any) -> None:
        settings_repo.set_value("brand_display_name", self.brand_display_name, "string")
        settings_repo.set_value("brand_mark_text", self.brand_mark_text, "string")
        settings_repo.set_value("brand_attribution", self.brand_attribution, "string")


@dataclass(frozen=True, slots=True)
class DirectoryUiSettings:
    group_display_separator: str = "-"
    group_recursive_enabled: bool = True
    managed_relation_cleanup_enabled: bool = False
    schedule_execution_mode: str = "apply"
    user_ou_placement_strategy: str = "source_primary_department"
    source_root_unit_ids: str = ""
    source_root_unit_display_text: str = ""
    directory_root_ou_path: str = ""
    disabled_users_ou_path: str = "Disabled Users"
    custom_group_ou_path: str = "Managed Groups"

    @classmethod
    def load(
        cls,
        settings_repo: Any,
        *,
        org_id: str | None = None,
    ) -> "DirectoryUiSettings":
        return cls(
            group_display_separator=_coerce_raw_text(
                settings_repo.get_value("group_display_separator", "-", org_id=org_id),
                "-",
            ),
            group_recursive_enabled=settings_repo.get_bool("group_recursive_enabled", True, org_id=org_id),
            managed_relation_cleanup_enabled=settings_repo.get_bool(
                "managed_relation_cleanup_enabled",
                False,
                org_id=org_id,
            ),
            schedule_execution_mode=normalize_schedule_execution_mode(
                settings_repo.get_value("schedule_execution_mode", "apply", org_id=org_id)
            ),
            user_ou_placement_strategy=_clean_text(
                settings_repo.get_value("user_ou_placement_strategy", "source_primary_department", org_id=org_id),
                "source_primary_department",
            ),
            source_root_unit_ids=_clean_text(
                settings_repo.get_value("source_root_unit_ids", "", org_id=org_id),
                "",
            ),
            source_root_unit_display_text=_clean_text(
                settings_repo.get_value("source_root_unit_display_text", "", org_id=org_id),
                "",
            ),
            directory_root_ou_path=_clean_text(
                settings_repo.get_value("directory_root_ou_path", "", org_id=org_id),
                "",
            ),
            disabled_users_ou_path=_clean_text(
                settings_repo.get_value("disabled_users_ou_path", "Disabled Users", org_id=org_id),
                "Disabled Users",
            ),
            custom_group_ou_path=_clean_text(
                settings_repo.get_value("custom_group_ou_path", "Managed Groups", org_id=org_id),
                "Managed Groups",
            ),
        )

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "DirectoryUiSettings":
        data = dict(values or {})
        return cls(
            group_display_separator=_coerce_raw_text(data.get("group_display_separator"), "-"),
            group_recursive_enabled=_coerce_bool(data.get("group_recursive_enabled"), True),
            managed_relation_cleanup_enabled=_coerce_bool(data.get("managed_relation_cleanup_enabled"), False),
            schedule_execution_mode=normalize_schedule_execution_mode(data.get("schedule_execution_mode")),
            user_ou_placement_strategy=_clean_text(
                data.get("user_ou_placement_strategy"),
                "source_primary_department",
            ),
            source_root_unit_ids=_clean_text(data.get("source_root_unit_ids"), ""),
            source_root_unit_display_text=_clean_text(data.get("source_root_unit_display_text"), ""),
            directory_root_ou_path=_clean_text(data.get("directory_root_ou_path"), ""),
            disabled_users_ou_path=_clean_text(data.get("disabled_users_ou_path"), "Disabled Users"),
            custom_group_ou_path=_clean_text(data.get("custom_group_ou_path"), "Managed Groups"),
        )

    def to_dict(self) -> dict[str, Any]:
        return dict(asdict(self))

    def persist(self, settings_repo: Any, *, org_id: str | None = None) -> None:
        normalized_org_id = normalize_org_id(org_id) if org_id is not None else None
        settings_repo.set_value("group_display_separator", self.group_display_separator, "string", org_id=normalized_org_id)
        settings_repo.set_value(
            "group_recursive_enabled",
            _bool_setting(self.group_recursive_enabled),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value("group_recursive_enabled_user_override", "true", "bool", org_id=normalized_org_id)
        settings_repo.set_value(
            "managed_relation_cleanup_enabled",
            _bool_setting(self.managed_relation_cleanup_enabled),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "schedule_execution_mode",
            self.schedule_execution_mode,
            "string",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "user_ou_placement_strategy",
            self.user_ou_placement_strategy,
            "string",
            org_id=normalized_org_id,
        )
        settings_repo.set_value("source_root_unit_ids", self.source_root_unit_ids, "string", org_id=normalized_org_id)
        settings_repo.set_value(
            "source_root_unit_display_text",
            self.source_root_unit_display_text,
            "string",
            org_id=normalized_org_id,
        )
        settings_repo.set_value("directory_root_ou_path", self.directory_root_ou_path, "string", org_id=normalized_org_id)
        settings_repo.set_value("disabled_users_ou_path", self.disabled_users_ou_path, "string", org_id=normalized_org_id)
        settings_repo.set_value("custom_group_ou_path", self.custom_group_ou_path, "string", org_id=normalized_org_id)


@dataclass(frozen=True, slots=True)
class NotificationAutomationPolicySettings:
    schedule_execution_mode: str = "apply"
    notify_dry_run_failure_enabled: bool = False
    notify_conflict_backlog_enabled: bool = False
    notify_conflict_backlog_threshold: int = 5
    notify_review_pending_enabled: bool = False
    notify_rule_governance_enabled: bool = False
    scheduled_apply_gate_enabled: bool = True
    scheduled_apply_max_dry_run_age_hours: int = 24
    scheduled_apply_requires_zero_conflicts: bool = True
    scheduled_apply_requires_review_approval: bool = True

    @classmethod
    def load(cls, settings_repo: Any, *, org_id: str) -> "NotificationAutomationPolicySettings":
        normalized_org_id = normalize_org_id(org_id, fallback="default") or "default"
        return cls(
            schedule_execution_mode=normalize_schedule_execution_mode(
                settings_repo.get_value("schedule_execution_mode", "apply", org_id=normalized_org_id)
            ),
            notify_dry_run_failure_enabled=settings_repo.get_bool(
                "ops_notify_dry_run_failure_enabled",
                False,
                org_id=normalized_org_id,
            ),
            notify_conflict_backlog_enabled=settings_repo.get_bool(
                "ops_notify_conflict_backlog_enabled",
                False,
                org_id=normalized_org_id,
            ),
            notify_conflict_backlog_threshold=_coerce_int(
                settings_repo.get_int("ops_notify_conflict_backlog_threshold", 5, org_id=normalized_org_id),
                5,
                minimum=1,
            ),
            notify_review_pending_enabled=settings_repo.get_bool(
                "ops_notify_review_pending_enabled",
                False,
                org_id=normalized_org_id,
            ),
            notify_rule_governance_enabled=settings_repo.get_bool(
                "ops_notify_rule_governance_enabled",
                False,
                org_id=normalized_org_id,
            ),
            scheduled_apply_gate_enabled=settings_repo.get_bool(
                "ops_scheduled_apply_gate_enabled",
                True,
                org_id=normalized_org_id,
            ),
            scheduled_apply_max_dry_run_age_hours=_coerce_int(
                settings_repo.get_int("ops_scheduled_apply_max_dry_run_age_hours", 24, org_id=normalized_org_id),
                24,
                minimum=1,
            ),
            scheduled_apply_requires_zero_conflicts=settings_repo.get_bool(
                "ops_scheduled_apply_requires_zero_conflicts",
                True,
                org_id=normalized_org_id,
            ),
            scheduled_apply_requires_review_approval=settings_repo.get_bool(
                "ops_scheduled_apply_requires_review_approval",
                True,
                org_id=normalized_org_id,
            ),
        )

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "NotificationAutomationPolicySettings":
        data = dict(values or {})
        return cls(
            schedule_execution_mode=normalize_schedule_execution_mode(data.get("schedule_execution_mode")),
            notify_dry_run_failure_enabled=_coerce_bool(data.get("notify_dry_run_failure_enabled"), False),
            notify_conflict_backlog_enabled=_coerce_bool(data.get("notify_conflict_backlog_enabled"), False),
            notify_conflict_backlog_threshold=_coerce_int(
                data.get("notify_conflict_backlog_threshold"),
                5,
                minimum=1,
            ),
            notify_review_pending_enabled=_coerce_bool(data.get("notify_review_pending_enabled"), False),
            notify_rule_governance_enabled=_coerce_bool(data.get("notify_rule_governance_enabled"), False),
            scheduled_apply_gate_enabled=_coerce_bool(data.get("scheduled_apply_gate_enabled"), True),
            scheduled_apply_max_dry_run_age_hours=_coerce_int(
                data.get("scheduled_apply_max_dry_run_age_hours"),
                24,
                minimum=1,
            ),
            scheduled_apply_requires_zero_conflicts=_coerce_bool(
                data.get("scheduled_apply_requires_zero_conflicts"),
                True,
            ),
            scheduled_apply_requires_review_approval=_coerce_bool(
                data.get("scheduled_apply_requires_review_approval"),
                True,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return dict(asdict(self))

    def persist(self, settings_repo: Any, *, org_id: str) -> None:
        normalized_org_id = normalize_org_id(org_id, fallback="default") or "default"
        settings_repo.set_value("schedule_execution_mode", self.schedule_execution_mode, "string", org_id=normalized_org_id)
        settings_repo.set_value(
            "ops_notify_dry_run_failure_enabled",
            _bool_setting(self.notify_dry_run_failure_enabled),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "ops_notify_conflict_backlog_enabled",
            _bool_setting(self.notify_conflict_backlog_enabled),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "ops_notify_conflict_backlog_threshold",
            str(self.notify_conflict_backlog_threshold),
            "int",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "ops_notify_review_pending_enabled",
            _bool_setting(self.notify_review_pending_enabled),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "ops_notify_rule_governance_enabled",
            _bool_setting(self.notify_rule_governance_enabled),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "ops_scheduled_apply_gate_enabled",
            _bool_setting(self.scheduled_apply_gate_enabled),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "ops_scheduled_apply_max_dry_run_age_hours",
            str(self.scheduled_apply_max_dry_run_age_hours),
            "int",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "ops_scheduled_apply_requires_zero_conflicts",
            _bool_setting(self.scheduled_apply_requires_zero_conflicts),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "ops_scheduled_apply_requires_review_approval",
            _bool_setting(self.scheduled_apply_requires_review_approval),
            "bool",
            org_id=normalized_org_id,
        )


@dataclass(frozen=True, slots=True)
class AdvancedSyncPolicySettings:
    offboarding_grace_days: int = 0
    offboarding_notify_managers: bool = False
    advanced_connector_routing_enabled: bool = False
    attribute_mapping_enabled: bool = False
    write_back_enabled: bool = False
    custom_group_sync_enabled: bool = False
    offboarding_lifecycle_enabled: bool = False
    rehire_restore_enabled: bool = False
    automatic_replay_enabled: bool = False
    future_onboarding_enabled: bool = False
    future_onboarding_start_field: str = "hire_date"
    contractor_lifecycle_enabled: bool = False
    lifecycle_employment_type_field: str = "employment_type"
    contractor_end_field: str = "contract_end_date"
    lifecycle_sponsor_field: str = "sponsor_userid"
    contractor_type_values: str = "contractor,intern,vendor,temp"
    disable_circuit_breaker_enabled: bool = False
    disable_circuit_breaker_percent: float = 5.0
    disable_circuit_breaker_min_count: int = 10
    disable_circuit_breaker_requires_approval: bool = True
    managed_group_type: str = "security"
    managed_group_mail_domain: str = ""
    custom_group_ou_path: str = "Managed Groups"

    @classmethod
    def load(cls, settings_repo: Any, *, org_id: str) -> "AdvancedSyncPolicySettings":
        normalized_org_id = normalize_org_id(org_id, fallback="default") or "default"
        return cls(
            offboarding_grace_days=_coerce_int(
                settings_repo.get_int("offboarding_grace_days", 0, org_id=normalized_org_id),
                0,
                minimum=0,
            ),
            offboarding_notify_managers=settings_repo.get_bool(
                "offboarding_notify_managers",
                False,
                org_id=normalized_org_id,
            ),
            advanced_connector_routing_enabled=settings_repo.get_bool(
                "advanced_connector_routing_enabled",
                False,
                org_id=normalized_org_id,
            ),
            attribute_mapping_enabled=settings_repo.get_bool(
                "attribute_mapping_enabled",
                False,
                org_id=normalized_org_id,
            ),
            write_back_enabled=settings_repo.get_bool(
                "write_back_enabled",
                False,
                org_id=normalized_org_id,
            ),
            custom_group_sync_enabled=settings_repo.get_bool(
                "custom_group_sync_enabled",
                False,
                org_id=normalized_org_id,
            ),
            offboarding_lifecycle_enabled=settings_repo.get_bool(
                "offboarding_lifecycle_enabled",
                False,
                org_id=normalized_org_id,
            ),
            rehire_restore_enabled=settings_repo.get_bool(
                "rehire_restore_enabled",
                False,
                org_id=normalized_org_id,
            ),
            automatic_replay_enabled=settings_repo.get_bool(
                "automatic_replay_enabled",
                False,
                org_id=normalized_org_id,
            ),
            future_onboarding_enabled=settings_repo.get_bool(
                "future_onboarding_enabled",
                False,
                org_id=normalized_org_id,
            ),
            future_onboarding_start_field=_clean_text(
                settings_repo.get_value("future_onboarding_start_field", "hire_date", org_id=normalized_org_id),
                "hire_date",
            ),
            contractor_lifecycle_enabled=settings_repo.get_bool(
                "contractor_lifecycle_enabled",
                False,
                org_id=normalized_org_id,
            ),
            lifecycle_employment_type_field=_clean_text(
                settings_repo.get_value("lifecycle_employment_type_field", "employment_type", org_id=normalized_org_id),
                "employment_type",
            ),
            contractor_end_field=_clean_text(
                settings_repo.get_value("contractor_end_field", "contract_end_date", org_id=normalized_org_id),
                "contract_end_date",
            ),
            lifecycle_sponsor_field=_clean_text(
                settings_repo.get_value("lifecycle_sponsor_field", "sponsor_userid", org_id=normalized_org_id),
                "sponsor_userid",
            ),
            contractor_type_values=_clean_text(
                settings_repo.get_value("contractor_type_values", "contractor,intern,vendor,temp", org_id=normalized_org_id),
                "contractor,intern,vendor,temp",
            ),
            disable_circuit_breaker_enabled=settings_repo.get_bool(
                "disable_circuit_breaker_enabled",
                False,
                org_id=normalized_org_id,
            ),
            disable_circuit_breaker_percent=_coerce_float(
                settings_repo.get_float("disable_circuit_breaker_percent", 5.0, org_id=normalized_org_id),
                5.0,
                minimum=0.0,
            ),
            disable_circuit_breaker_min_count=_coerce_int(
                settings_repo.get_int("disable_circuit_breaker_min_count", 10, org_id=normalized_org_id),
                10,
                minimum=0,
            ),
            disable_circuit_breaker_requires_approval=settings_repo.get_bool(
                "disable_circuit_breaker_requires_approval",
                True,
                org_id=normalized_org_id,
            ),
            managed_group_type=_normalize_managed_group_type(
                settings_repo.get_value("managed_group_type", "security", org_id=normalized_org_id)
            ),
            managed_group_mail_domain=_clean_text(
                settings_repo.get_value("managed_group_mail_domain", "", org_id=normalized_org_id),
                "",
            ),
            custom_group_ou_path=_clean_text(
                settings_repo.get_value("custom_group_ou_path", "Managed Groups", org_id=normalized_org_id),
                "Managed Groups",
            ),
        )

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "AdvancedSyncPolicySettings":
        data = dict(values or {})
        return cls(
            offboarding_grace_days=_coerce_int(data.get("offboarding_grace_days"), 0, minimum=0),
            offboarding_notify_managers=_coerce_bool(data.get("offboarding_notify_managers"), False),
            advanced_connector_routing_enabled=_coerce_bool(data.get("advanced_connector_routing_enabled"), False),
            attribute_mapping_enabled=_coerce_bool(data.get("attribute_mapping_enabled"), False),
            write_back_enabled=_coerce_bool(data.get("write_back_enabled"), False),
            custom_group_sync_enabled=_coerce_bool(data.get("custom_group_sync_enabled"), False),
            offboarding_lifecycle_enabled=_coerce_bool(data.get("offboarding_lifecycle_enabled"), False),
            rehire_restore_enabled=_coerce_bool(data.get("rehire_restore_enabled"), False),
            automatic_replay_enabled=_coerce_bool(data.get("automatic_replay_enabled"), False),
            future_onboarding_enabled=_coerce_bool(data.get("future_onboarding_enabled"), False),
            future_onboarding_start_field=_clean_text(data.get("future_onboarding_start_field"), "hire_date"),
            contractor_lifecycle_enabled=_coerce_bool(data.get("contractor_lifecycle_enabled"), False),
            lifecycle_employment_type_field=_clean_text(
                data.get("lifecycle_employment_type_field"),
                "employment_type",
            ),
            contractor_end_field=_clean_text(data.get("contractor_end_field"), "contract_end_date"),
            lifecycle_sponsor_field=_clean_text(data.get("lifecycle_sponsor_field"), "sponsor_userid"),
            contractor_type_values=_clean_text(
                data.get("contractor_type_values"),
                "contractor,intern,vendor,temp",
            ),
            disable_circuit_breaker_enabled=_coerce_bool(data.get("disable_circuit_breaker_enabled"), False),
            disable_circuit_breaker_percent=_coerce_float(
                data.get("disable_circuit_breaker_percent"),
                5.0,
                minimum=0.0,
            ),
            disable_circuit_breaker_min_count=_coerce_int(
                data.get("disable_circuit_breaker_min_count"),
                10,
                minimum=0,
            ),
            disable_circuit_breaker_requires_approval=_coerce_bool(
                data.get("disable_circuit_breaker_requires_approval"),
                True,
            ),
            managed_group_type=_normalize_managed_group_type(data.get("managed_group_type")),
            managed_group_mail_domain=_clean_text(data.get("managed_group_mail_domain"), ""),
            custom_group_ou_path=_clean_text(data.get("custom_group_ou_path"), "Managed Groups"),
        )

    def to_dict(self) -> dict[str, Any]:
        return dict(asdict(self))

    def persist(self, settings_repo: Any, *, org_id: str) -> None:
        normalized_org_id = normalize_org_id(org_id, fallback="default") or "default"
        settings_repo.set_value("offboarding_grace_days", str(self.offboarding_grace_days), "int", org_id=normalized_org_id)
        settings_repo.set_value(
            "offboarding_notify_managers",
            _bool_setting(self.offboarding_notify_managers),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "advanced_connector_routing_enabled",
            _bool_setting(self.advanced_connector_routing_enabled),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value("attribute_mapping_enabled", _bool_setting(self.attribute_mapping_enabled), "bool", org_id=normalized_org_id)
        settings_repo.set_value("write_back_enabled", _bool_setting(self.write_back_enabled), "bool", org_id=normalized_org_id)
        settings_repo.set_value(
            "custom_group_sync_enabled",
            _bool_setting(self.custom_group_sync_enabled),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "offboarding_lifecycle_enabled",
            _bool_setting(self.offboarding_lifecycle_enabled),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value("rehire_restore_enabled", _bool_setting(self.rehire_restore_enabled), "bool", org_id=normalized_org_id)
        settings_repo.set_value("automatic_replay_enabled", _bool_setting(self.automatic_replay_enabled), "bool", org_id=normalized_org_id)
        settings_repo.set_value("future_onboarding_enabled", _bool_setting(self.future_onboarding_enabled), "bool", org_id=normalized_org_id)
        settings_repo.set_value("future_onboarding_start_field", self.future_onboarding_start_field, "string", org_id=normalized_org_id)
        settings_repo.set_value(
            "contractor_lifecycle_enabled",
            _bool_setting(self.contractor_lifecycle_enabled),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "lifecycle_employment_type_field",
            self.lifecycle_employment_type_field,
            "string",
            org_id=normalized_org_id,
        )
        settings_repo.set_value("contractor_end_field", self.contractor_end_field, "string", org_id=normalized_org_id)
        settings_repo.set_value("lifecycle_sponsor_field", self.lifecycle_sponsor_field, "string", org_id=normalized_org_id)
        settings_repo.set_value("contractor_type_values", self.contractor_type_values, "string", org_id=normalized_org_id)
        settings_repo.set_value(
            "disable_circuit_breaker_enabled",
            _bool_setting(self.disable_circuit_breaker_enabled),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "disable_circuit_breaker_percent",
            str(self.disable_circuit_breaker_percent),
            "float",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "disable_circuit_breaker_min_count",
            str(self.disable_circuit_breaker_min_count),
            "int",
            org_id=normalized_org_id,
        )
        settings_repo.set_value(
            "disable_circuit_breaker_requires_approval",
            _bool_setting(self.disable_circuit_breaker_requires_approval),
            "bool",
            org_id=normalized_org_id,
        )
        settings_repo.set_value("managed_group_type", self.managed_group_type, "string", org_id=normalized_org_id)
        settings_repo.set_value("managed_group_mail_domain", self.managed_group_mail_domain, "string", org_id=normalized_org_id)
        settings_repo.set_value("custom_group_ou_path", self.custom_group_ou_path, "string", org_id=normalized_org_id)
