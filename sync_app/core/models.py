from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional


class MappingLikeModel:
    __slots__ = ()

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def items(self):
        return self.to_dict().items()

    def keys(self):
        return self.to_dict().keys()

    def values(self):
        return self.to_dict().values()

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _normalize_mapping_direction_value(value: Any, default: str = "source_to_ad") -> str:
    normalized = str(value or "").strip().lower()
    aliases = {
        "wecom_to_ad": "source_to_ad",
        "ad_to_wecom": "ad_to_source",
        "source_to_ad": "source_to_ad",
        "ad_to_source": "ad_to_source",
    }
    return aliases.get(normalized, aliases.get(str(default or "").strip().lower(), "source_to_ad"))


def _append_unique_int(target: list[int], raw_value: Any) -> None:
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return
    if parsed not in target:
        target.append(parsed)


def _coerce_int_list(value: Any) -> list[int]:
    normalized: list[int] = []
    if value in (None, ""):
        return normalized

    if isinstance(value, dict):
        candidate_keys = ("dept_id", "deptId", "department_id", "departmentId")
        for key in candidate_keys:
            if key in value:
                for item in _coerce_int_list(value.get(key)):
                    _append_unique_int(normalized, item)
        if normalized:
            return normalized

        numeric_keys = []
        for key in value.keys():
            key_text = str(key).strip()
            if key_text.lstrip("-").isdigit():
                numeric_keys.append(key_text)
        if numeric_keys:
            for key in numeric_keys:
                _append_unique_int(normalized, key)
            return normalized

        for nested_value in value.values():
            for item in _coerce_int_list(nested_value):
                _append_unique_int(normalized, item)
        return normalized

    if isinstance(value, (list, tuple, set)):
        for item in value:
            for nested_item in _coerce_int_list(item):
                _append_unique_int(normalized, nested_item)
        return normalized

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return normalized
        if text.startswith("[") and text.endswith("]"):
            try:
                return _coerce_int_list(json.loads(text))
            except json.JSONDecodeError:
                pass
        for token in re.findall(r"-?\d+", text):
            _append_unique_int(normalized, token)
        return normalized

    _append_unique_int(normalized, value)
    return normalized


def _extract_department_ids(payload: Dict[str, Any]) -> list[int]:
    department_ids: list[int] = []
    candidate_keys = (
        "department",
        "departments",
        "dept_id_list",
        "deptIdList",
        "dept_ids",
        "deptIds",
        "dept_id",
        "deptId",
        "department_id",
        "departmentId",
        "dept_order_list",
    )
    for key in candidate_keys:
        for department_id in _coerce_int_list(payload.get(key)):
            _append_unique_int(department_ids, department_id)
    return department_ids


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


@dataclass(slots=True)
class DepartmentNode:
    department_id: int
    name: str
    parent_id: int
    path: list[str] = field(default_factory=list)
    path_ids: list[int] = field(default_factory=list)
    users: list["SourceDirectoryUser"] = field(default_factory=list)

    @classmethod
    def from_source_payload(cls, payload: Dict[str, Any]) -> "DepartmentNode":
        payload_copy = dict(payload)
        department_id = (
            payload_copy.get("id")
            or payload_copy.get("dept_id")
            or payload_copy.get("deptId")
            or payload_copy.get("department_id")
            or payload_copy.get("departmentId")
            or 0
        )
        parent_id = (
            payload_copy.get("parentid")
            or payload_copy.get("parent_id")
            or payload_copy.get("parentId")
            or payload_copy.get("parent_department_id")
            or payload_copy.get("parentDepartmentId")
            or 0
        )
        return cls(
            department_id=int(department_id or 0),
            name=str(payload_copy.get("name") or payload_copy.get("dept_name") or payload_copy.get("displayName") or ""),
            parent_id=int(parent_id or 0),
        )

    @classmethod
    def from_wecom_payload(cls, payload: Dict[str, Any]) -> "DepartmentNode":
        return cls.from_source_payload(payload)

    def set_hierarchy(self, path: list[str], path_ids: list[int]) -> None:
        self.path = list(path)
        self.path_ids = list(path_ids)

    def to_hash_payload(self) -> Dict[str, Any]:
        return {
            "id": self.department_id,
            "name": self.name,
            "parentid": self.parent_id,
        }


@dataclass(slots=True)
class SourceDirectoryUser:
    userid: str
    name: str
    email: str = ""
    departments: list[int] = field(default_factory=list)
    raw_payload: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_wecom_payload(cls, payload: Dict[str, Any]) -> "SourceDirectoryUser":
        payload_copy = dict(payload)
        return cls(
            userid=str(payload_copy.get("userid") or ""),
            name=str(payload_copy.get("name") or ""),
            email=str(payload_copy.get("email") or ""),
            departments=_coerce_int_list(payload_copy.get("department", [])),
            raw_payload=payload_copy,
        )

    def merge_payload(self, payload: Dict[str, Any]) -> None:
        if payload.get("name"):
            self.name = str(payload["name"])
        if payload.get("email"):
            self.email = str(payload["email"])
        departments = payload.get("department")
        normalized_departments = _coerce_int_list(departments)
        if normalized_departments:
            self.departments = normalized_departments
        self.raw_payload.update(payload)

    def to_state_payload(self) -> Dict[str, Any]:
        payload = dict(self.raw_payload)
        payload.update(
            {
                "userid": self.userid,
                "name": self.name,
                "email": self.email,
                "department": list(self.departments),
            }
        )
        return payload

    def declared_primary_department_id(self) -> Optional[int]:
        candidate_keys = (
            "main_department",
            "mainDepartment",
            "main_department_id",
            "mainDepartmentId",
            "primary_department_id",
            "primaryDepartmentId",
        )
        for key in candidate_keys:
            value = self.raw_payload.get(key)
            if value in (None, ""):
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
        return None

    @property
    def source_user_id(self) -> str:
        return self.userid

    @source_user_id.setter
    def source_user_id(self, value: str) -> None:
        self.userid = str(value or "").strip()

    @classmethod
    def from_source_payload(cls, payload: Dict[str, Any]) -> "SourceDirectoryUser":
        payload_copy = dict(payload)
        userid = (
            payload_copy.get("userid")
            or payload_copy.get("userId")
            or payload_copy.get("staffid")
            or payload_copy.get("staffId")
            or payload_copy.get("unionid")
            or payload_copy.get("unionId")
            or payload_copy.get("emplId")
            or ""
        )
        email = (
            payload_copy.get("email")
            or payload_copy.get("org_email")
            or payload_copy.get("orgEmail")
            or payload_copy.get("work_email")
            or payload_copy.get("workEmail")
            or ""
        )
        return cls(
            userid=str(userid or ""),
            name=str(payload_copy.get("name") or payload_copy.get("nick") or payload_copy.get("displayName") or ""),
            email=str(email or ""),
            departments=_extract_department_ids(payload_copy),
            raw_payload=payload_copy,
        )


SourceUser = SourceDirectoryUser
WeComUser = SourceDirectoryUser


@dataclass(slots=True)
class UserDepartmentBundle:
    user: SourceDirectoryUser
    departments: list[DepartmentNode] = field(default_factory=list)

    def add_department(self, department: DepartmentNode) -> None:
        self.departments.append(department)


@dataclass(slots=True)
class SkipOperationSummary(MappingLikeModel):
    total: int = 0
    by_action: Dict[str, int] = field(default_factory=dict)
    samples: list[Dict[str, Any]] = field(default_factory=list)
    details: list[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_mapping(cls, value: Any) -> "SkipOperationSummary":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            return cls()
        return cls(
            total=int(value.get("total") or 0),
            by_action=dict(value.get("by_action") or {}),
            samples=list(value.get("samples") or []),
            details=list(value.get("details") or []),
        )


@dataclass(slots=True)
class SyncErrorBuckets(MappingLikeModel):
    department_errors: list[Dict[str, Any]] = field(default_factory=list)
    user_create_errors: list[Dict[str, Any]] = field(default_factory=list)
    user_update_errors: list[Dict[str, Any]] = field(default_factory=list)
    group_add_errors: list[Dict[str, Any]] = field(default_factory=list)
    group_hierarchy_errors: list[Dict[str, Any]] = field(default_factory=list)
    group_relation_cleanup_errors: list[Dict[str, Any]] = field(default_factory=list)
    user_disable_errors: list[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_mapping(cls, value: Any) -> "SyncErrorBuckets":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            return cls()
        return cls(
            department_errors=list(value.get("department_errors") or []),
            user_create_errors=list(value.get("user_create_errors") or []),
            user_update_errors=list(value.get("user_update_errors") or []),
            group_add_errors=list(value.get("group_add_errors") or []),
            group_hierarchy_errors=list(value.get("group_hierarchy_errors") or []),
            group_relation_cleanup_errors=list(value.get("group_relation_cleanup_errors") or []),
            user_disable_errors=list(value.get("user_disable_errors") or []),
        )


@dataclass(slots=True)
class SyncOperationCounters(MappingLikeModel):
    departments_created: int = 0
    departments_existed: int = 0
    users_created: int = 0
    users_updated: int = 0
    users_disabled: int = 0
    groups_assigned: int = 0
    groups_nested: int = 0
    group_relations_removed: int = 0

    @classmethod
    def from_mapping(cls, value: Any) -> "SyncOperationCounters":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            return cls()
        return cls(
            departments_created=int(value.get("departments_created") or 0),
            departments_existed=int(value.get("departments_existed") or 0),
            users_created=int(value.get("users_created") or 0),
            users_updated=int(value.get("users_updated") or 0),
            users_disabled=int(value.get("users_disabled") or 0),
            groups_assigned=int(value.get("groups_assigned") or 0),
            groups_nested=int(value.get("groups_nested") or 0),
            group_relations_removed=int(value.get("group_relations_removed") or 0),
        )


@dataclass(slots=True)
class SyncRunStats(MappingLikeModel):
    execution_mode: str = "apply"
    org_id: str = "default"
    organization_name: str = ""
    organization_config_path: str = ""
    total_users: int = 0
    processed_users: int = 0
    disabled_users: list[str] = field(default_factory=list)
    error_count: int = 0
    log_file: str = ""
    skip_detail_report: str = ""
    db_path: str = ""
    db_backup_dir: str = ""
    db_startup_snapshot_path: str = ""
    db_migration_source_path: str = ""
    db_integrity_check: Dict[str, Any] = field(default_factory=dict)
    job_id: str = ""
    planned_operation_count: int = 0
    executed_operation_count: int = 0
    high_risk_operation_count: int = 0
    conflict_count: int = 0
    review_required: bool = False
    automatic_replay_request_count: int = 0
    automatic_replay_request_ids: list[int] = field(default_factory=list)
    skipped_operations: SkipOperationSummary = field(default_factory=SkipOperationSummary)
    errors: SyncErrorBuckets = field(default_factory=SyncErrorBuckets)
    operations: SyncOperationCounters = field(default_factory=SyncOperationCounters)
    field_ownership_policy: Dict[str, str] = field(default_factory=dict)
    summary: Optional[Dict[str, Any]] = None
    job_summary: Optional[Dict[str, Any]] = None

    @classmethod
    def from_mapping(cls, value: Any) -> "SyncRunStats":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            return cls()
        return cls(
            execution_mode=str(value.get("execution_mode") or "apply"),
            org_id=str(value.get("org_id") or "default"),
            organization_name=str(value.get("organization_name") or ""),
            organization_config_path=str(value.get("organization_config_path") or ""),
            total_users=int(value.get("total_users") or 0),
            processed_users=int(value.get("processed_users") or value.get("users_processed") or 0),
            disabled_users=list(value.get("disabled_users") or []),
            error_count=int(value.get("error_count") or 0),
            log_file=str(value.get("log_file") or ""),
            skip_detail_report=str(value.get("skip_detail_report") or ""),
            db_path=str(value.get("db_path") or ""),
            db_backup_dir=str(value.get("db_backup_dir") or ""),
            db_startup_snapshot_path=str(value.get("db_startup_snapshot_path") or ""),
            db_migration_source_path=str(value.get("db_migration_source_path") or ""),
            db_integrity_check=dict(value.get("db_integrity_check") or {}),
            job_id=str(value.get("job_id") or ""),
            planned_operation_count=int(value.get("planned_operation_count") or 0),
            executed_operation_count=int(value.get("executed_operation_count") or 0),
            high_risk_operation_count=int(value.get("high_risk_operation_count") or 0),
            conflict_count=int(value.get("conflict_count") or 0),
            review_required=bool(value.get("review_required") or False),
            automatic_replay_request_count=int(value.get("automatic_replay_request_count") or 0),
            automatic_replay_request_ids=[
                int(item) for item in list(value.get("automatic_replay_request_ids") or []) if str(item).isdigit()
            ],
            skipped_operations=SkipOperationSummary.from_mapping(value.get("skipped_operations")),
            errors=SyncErrorBuckets.from_mapping(value.get("errors")),
            operations=SyncOperationCounters.from_mapping(value.get("operations")),
            field_ownership_policy=dict(value.get("field_ownership_policy") or {}),
            summary=dict(value.get("summary")) if isinstance(value.get("summary"), dict) else value.get("summary"),
            job_summary=dict(value.get("job_summary")) if isinstance(value.get("job_summary"), dict) else value.get("job_summary"),
        )


@dataclass(slots=True)
class SyncJobSummary:
    job_id: str
    mode: str
    error_count: int
    planned_operation_count: int
    executed_operation_count: int
    high_risk_operation_count: int = 0
    conflict_count: int = 0
    review_required: bool = False
    log_file: str = ""
    db_path: str = ""
    db_backup_dir: str = ""
    db_startup_snapshot_path: str = ""
    db_migration_source_path: str = ""
    summary: Optional[Dict[str, Any]] = None

    @classmethod
    def from_sync_stats(cls, sync_stats: Dict[str, Any]) -> "SyncJobSummary":
        return cls(
            job_id=str(sync_stats.get("job_id") or ""),
            mode=str(sync_stats.get("execution_mode") or ""),
            error_count=int(sync_stats.get("error_count") or 0),
            planned_operation_count=int(sync_stats.get("planned_operation_count") or 0),
            executed_operation_count=int(sync_stats.get("executed_operation_count") or 0),
            high_risk_operation_count=int(sync_stats.get("high_risk_operation_count") or 0),
            conflict_count=int(sync_stats.get("conflict_count") or 0),
            review_required=bool(sync_stats.get("review_required") or False),
            log_file=str(sync_stats.get("log_file") or ""),
            db_path=str(sync_stats.get("db_path") or ""),
            db_backup_dir=str(sync_stats.get("db_backup_dir") or ""),
            db_startup_snapshot_path=str(sync_stats.get("db_startup_snapshot_path") or ""),
            db_migration_source_path=str(sync_stats.get("db_migration_source_path") or ""),
            summary=sync_stats.get("summary"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class GroupPolicyEvaluation:
    is_hard_protected: bool = False
    is_excluded: bool = False
    matched_rules: list[Dict[str, Any]] = field(default_factory=list)

    def matched_rule_labels(self) -> list[str]:
        return [
            rule.get("display_name") or rule.get("match_value")
            for rule in self.matched_rules
            if isinstance(rule, dict)
        ]


@dataclass(slots=True)
class DepartmentGroupInfo:
    exists: bool
    group_sam: str
    group_cn: str
    group_dn: str
    display_name: str
    description: str
    binding_source: str
    created: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DirectoryUserRecord:
    username: str
    dn: str
    display_name: str = ""
    email: str = ""
    user_principal_name: str = ""
    raw_entry: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_ldap_json(cls, payload: Dict[str, Any]) -> "DirectoryUserRecord":
        attributes = payload.get("attributes", {})
        return cls(
            username=str(attributes.get("sAMAccountName") or payload.get("dn") or ""),
            dn=str(payload.get("dn") or ""),
            display_name=str(attributes.get("displayName") or ""),
            email=str(attributes.get("mail") or ""),
            user_principal_name=str(attributes.get("userPrincipalName") or ""),
            raw_entry=payload,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DirectoryGroupRecord:
    dn: str
    cn: str
    group_sam: str
    display_name: str = ""
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ExclusionRuleRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    rule_type: str = ""
    protection_level: str = ""
    match_type: str = ""
    match_value: str = ""
    display_name: str = ""
    is_enabled: bool = True
    source: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "ExclusionRuleRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            rule_type=str(row["rule_type"] or ""),
            protection_level=str(row["protection_level"] or ""),
            match_type=str(row["match_type"] or ""),
            match_value=str(row["match_value"] or ""),
            display_name=str(row["display_name"] or ""),
            is_enabled=bool(row["is_enabled"]),
            source=str(row["source"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )


@dataclass(slots=True)
class ManagedGroupBindingRecord(MappingLikeModel):
    org_id: str = "default"
    department_id: str = ""
    parent_department_id: Optional[str] = None
    group_sam: str = ""
    group_dn: str = ""
    group_cn: str = ""
    display_name: str = ""
    path_text: str = ""
    status: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "ManagedGroupBindingRecord":
        return cls(
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            department_id=str(row["department_id"] or ""),
            parent_department_id=str(row["parent_department_id"]) if row["parent_department_id"] is not None else None,
            group_sam=str(row["group_sam"] or ""),
            group_dn=str(row["group_dn"] or ""),
            group_cn=str(row["group_cn"] or ""),
            display_name=str(row["display_name"] or ""),
            path_text=str(row["path_text"] or ""),
            status=str(row["status"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )


@dataclass(slots=True)
class SyncJobRecord(MappingLikeModel):
    job_id: str = ""
    org_id: str = "default"
    trigger_type: str = ""
    execution_mode: str = ""
    status: str = ""
    plan_source_job_id: str = ""
    app_version: str = ""
    config_snapshot_hash: str = ""
    planned_operation_count: int = 0
    executed_operation_count: int = 0
    error_count: int = 0
    summary: Optional[Dict[str, Any]] = None
    started_at: str = ""
    ended_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "SyncJobRecord":
        summary = None
        if "summary" in row.keys():
            summary = row["summary"]
        elif "summary_json" in row.keys():
            summary = row["summary_json"]
        if isinstance(summary, str) and summary:
            try:
                summary = json.loads(summary)
            except json.JSONDecodeError:
                pass
        return cls(
            job_id=str(row["job_id"] or ""),
            org_id=str(row["org_id"] or "default"),
            trigger_type=str(row["trigger_type"] or ""),
            execution_mode=str(row["execution_mode"] or ""),
            status=str(row["status"] or ""),
            plan_source_job_id=str(row["plan_source_job_id"] or ""),
            app_version=str(row["app_version"] or ""),
            config_snapshot_hash=str(row["config_snapshot_hash"] or ""),
            planned_operation_count=int(row["planned_operation_count"] or 0),
            executed_operation_count=int(row["executed_operation_count"] or 0),
            error_count=int(row["error_count"] or 0),
            summary=summary if isinstance(summary, dict) or summary is None else {"raw": summary},
            started_at=str(row["started_at"] or ""),
            ended_at=str(row["ended_at"] or ""),
        )


@dataclass(slots=True)
class WebAdminUserRecord(MappingLikeModel):
    id: Optional[int] = None
    username: str = ""
    password_hash: str = ""
    role: str = "super_admin"
    is_enabled: bool = True
    must_change_password: bool = False
    created_at: str = ""
    updated_at: str = ""
    last_login_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "WebAdminUserRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            username=str(row["username"] or ""),
            password_hash=str(row["password_hash"] or ""),
            role=str(row["role"] or "super_admin"),
            is_enabled=bool(row["is_enabled"]),
            must_change_password=bool(row["must_change_password"]),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
            last_login_at=str(row["last_login_at"] or ""),
        )


@dataclass(slots=True)
class WebAuditLogRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = ""
    actor_username: str = ""
    action_type: str = ""
    target_type: str = ""
    target_id: str = ""
    result: str = ""
    message: str = ""
    payload: Optional[Dict[str, Any]] = None
    created_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "WebAuditLogRecord":
        payload = row["payload_json"] if "payload_json" in row.keys() else None
        if isinstance(payload, str) and payload:
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {"raw": payload}
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or ""),
            actor_username=str(row["actor_username"] or ""),
            action_type=str(row["action_type"] or ""),
            target_type=str(row["target_type"] or ""),
            target_id=str(row["target_id"] or ""),
            result=str(row["result"] or ""),
            message=str(row["message"] or ""),
            payload=payload if isinstance(payload, dict) or payload is None else {"raw": payload},
            created_at=str(row["created_at"] or ""),
        )


@dataclass(slots=True)
class SyncOperationRecord(MappingLikeModel):
    id: Optional[int] = None
    job_id: str = ""
    stage_name: str = ""
    object_type: str = ""
    operation_type: str = ""
    source_id: str = ""
    department_id: str = ""
    target_id: str = ""
    target_dn: str = ""
    risk_level: str = "normal"
    status: str = ""
    message: str = ""
    rule_source: str = ""
    reason_code: str = ""
    details: Optional[Dict[str, Any]] = None
    created_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "SyncOperationRecord":
        details = row["details_json"] if "details_json" in row.keys() else None
        if isinstance(details, str) and details:
            try:
                details = json.loads(details)
            except json.JSONDecodeError:
                details = {"raw": details}
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            job_id=str(row["job_id"] or ""),
            stage_name=str(row["stage_name"] or ""),
            object_type=str(row["object_type"] or ""),
            operation_type=str(row["operation_type"] or ""),
            source_id=str(row["source_id"] or ""),
            department_id=str(row["department_id"] or ""),
            target_id=str(row["target_id"] or ""),
            target_dn=str(row["target_dn"] or ""),
            risk_level=str(row["risk_level"] or "normal"),
            status=str(row["status"] or ""),
            message=str(row["message"] or ""),
            rule_source=str(row["rule_source"] or ""),
            reason_code=str(row["reason_code"] or ""),
            details=details if isinstance(details, dict) or details is None else {"raw": details},
            created_at=str(row["created_at"] or ""),
        )


@dataclass(slots=True)
class SyncConflictRecord(MappingLikeModel):
    id: Optional[int] = None
    job_id: str = ""
    conflict_type: str = ""
    severity: str = "warning"
    status: str = "open"
    source_id: str = ""
    target_key: str = ""
    message: str = ""
    resolution_hint: str = ""
    details: Optional[Dict[str, Any]] = None
    resolution_payload: Optional[Dict[str, Any]] = None
    created_at: str = ""
    resolved_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "SyncConflictRecord":
        details = row["details_json"] if "details_json" in row.keys() else None
        if isinstance(details, str) and details:
            try:
                details = json.loads(details)
            except json.JSONDecodeError:
                details = {"raw": details}
        resolution_payload = row["resolution_payload_json"] if "resolution_payload_json" in row.keys() else None
        if isinstance(resolution_payload, str) and resolution_payload:
            try:
                resolution_payload = json.loads(resolution_payload)
            except json.JSONDecodeError:
                resolution_payload = {"raw": resolution_payload}
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            job_id=str(row["job_id"] or ""),
            conflict_type=str(row["conflict_type"] or ""),
            severity=str(row["severity"] or "warning"),
            status=str(row["status"] or "open"),
            source_id=str(row["source_id"] or ""),
            target_key=str(row["target_key"] or ""),
            message=str(row["message"] or ""),
            resolution_hint=str(row["resolution_hint"] or ""),
            details=details if isinstance(details, dict) or details is None else {"raw": details},
            resolution_payload=(
                resolution_payload
                if isinstance(resolution_payload, dict) or resolution_payload is None
                else {"raw": resolution_payload}
            ),
            created_at=str(row["created_at"] or ""),
            resolved_at=str(row["resolved_at"] or ""),
        )


@dataclass(slots=True)
class SyncPlanReviewRecord(MappingLikeModel):
    id: Optional[int] = None
    job_id: str = ""
    plan_fingerprint: str = ""
    config_snapshot_hash: str = ""
    high_risk_operation_count: int = 0
    status: str = "pending"
    reviewer_username: str = ""
    review_notes: str = ""
    created_at: str = ""
    reviewed_at: str = ""
    expires_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "SyncPlanReviewRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            job_id=str(row["job_id"] or ""),
            plan_fingerprint=str(row["plan_fingerprint"] or ""),
            config_snapshot_hash=str(row["config_snapshot_hash"] or ""),
            high_risk_operation_count=int(row["high_risk_operation_count"] or 0),
            status=str(row["status"] or "pending"),
            reviewer_username=str(row["reviewer_username"] or ""),
            review_notes=str(row["review_notes"] or ""),
            created_at=str(row["created_at"] or ""),
            reviewed_at=str(row["reviewed_at"] or ""),
            expires_at=str(row["expires_at"] or ""),
        )


@dataclass(slots=True)
class SyncExceptionRuleRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    rule_type: str = ""
    match_type: str = ""
    match_value: str = ""
    notes: str = ""
    is_enabled: bool = True
    expires_at: str = ""
    is_once: bool = False
    last_matched_at: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "SyncExceptionRuleRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            rule_type=str(row["rule_type"] or ""),
            match_type=str(row["match_type"] or ""),
            match_value=str(row["match_value"] or ""),
            notes=str(row["notes"] or ""),
            is_enabled=bool(row["is_enabled"]),
            expires_at=str(row["expires_at"] or ""),
            is_once=bool(row["is_once"]) if "is_once" in row.keys() else False,
            last_matched_at=str(row["last_matched_at"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )


@dataclass(slots=True)
class UserIdentityBindingRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    source_user_id: str = ""
    connector_id: str = "default"
    ad_username: str = ""
    source: str = ""
    notes: str = ""
    is_enabled: bool = True
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "UserIdentityBindingRecord":
        source_user_id = (
            row["source_user_id"]
            if "source_user_id" in row.keys()
            else row["wecom_userid"]
        )
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            source_user_id=str(source_user_id or ""),
            connector_id=str(row["connector_id"] or "default"),
            ad_username=str(row["ad_username"] or ""),
            source=str(row["source"] or ""),
            notes=str(row["notes"] or ""),
            is_enabled=bool(row["is_enabled"]),
            updated_at=str(row["updated_at"] or ""),
        )

    @property
    def wecom_userid(self) -> str:
        return self.source_user_id

    @wecom_userid.setter
    def wecom_userid(self, value: str) -> None:
        self.source_user_id = str(value or "").strip()

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["wecom_userid"] = self.source_user_id
        return data


@dataclass(slots=True)
class UserDepartmentOverrideRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    source_user_id: str = ""
    primary_department_id: str = ""
    notes: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "UserDepartmentOverrideRecord":
        source_user_id = (
            row["source_user_id"]
            if "source_user_id" in row.keys()
            else row["wecom_userid"]
        )
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            source_user_id=str(source_user_id or ""),
            primary_department_id=str(row["primary_department_id"] or ""),
            notes=str(row["notes"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

    @property
    def wecom_userid(self) -> str:
        return self.source_user_id

    @wecom_userid.setter
    def wecom_userid(self, value: str) -> None:
        self.source_user_id = str(value or "").strip()

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["wecom_userid"] = self.source_user_id
        return data


@dataclass(slots=True)
class AttributeMappingRuleRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    connector_id: str = ""
    direction: str = "source_to_ad"
    source_field: str = ""
    target_field: str = ""
    transform_template: str = ""
    sync_mode: str = "replace"
    is_enabled: bool = True
    notes: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "AttributeMappingRuleRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            connector_id=str(row["connector_id"] or ""),
            direction=_normalize_mapping_direction_value(row["direction"] if "direction" in row.keys() else "source_to_ad"),
            source_field=str(row["source_field"] or ""),
            target_field=str(row["target_field"] or ""),
            transform_template=str(row["transform_template"] or ""),
            sync_mode=str(row["sync_mode"] or "replace"),
            is_enabled=bool(row["is_enabled"]),
            notes=str(row["notes"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )


@dataclass(slots=True)
class OrganizationRecord(MappingLikeModel):
    org_id: str = "default"
    name: str = ""
    config_path: str = ""
    description: str = ""
    is_enabled: bool = True
    is_default: bool = False
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "OrganizationRecord":
        return cls(
            org_id=str(row["org_id"] or "default"),
            name=str(row["name"] or ""),
            config_path=str(row["config_path"] or ""),
            description=str(row["description"] or ""),
            is_enabled=bool(row["is_enabled"]),
            is_default=bool(row["is_default"]) if "is_default" in row.keys() else False,
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )


@dataclass(slots=True)
class SyncConnectorRecord(MappingLikeModel):
    connector_id: str = ""
    org_id: str = "default"
    name: str = ""
    config_path: str = ""
    ldap_server: str = ""
    ldap_domain: str = ""
    ldap_username: str = ""
    ldap_password: str = ""
    ldap_use_ssl: Optional[bool] = None
    ldap_port: Optional[int] = None
    ldap_validate_cert: Optional[bool] = None
    ldap_ca_cert_path: str = ""
    default_password: str = ""
    force_change_password: Optional[bool] = None
    password_complexity: str = ""
    root_department_ids: list[int] = field(default_factory=list)
    username_template: str = ""
    disabled_users_ou: str = ""
    group_type: str = "security"
    group_mail_domain: str = ""
    custom_group_ou_path: str = ""
    managed_tag_ids: list[str] = field(default_factory=list)
    managed_external_chat_ids: list[str] = field(default_factory=list)
    is_enabled: bool = True
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "SyncConnectorRecord":
        root_department_ids = row["root_department_ids_json"] if "root_department_ids_json" in row.keys() else None
        managed_tag_ids = row["managed_tag_ids_json"] if "managed_tag_ids_json" in row.keys() else None
        managed_external_chat_ids = (
            row["managed_external_chat_ids_json"]
            if "managed_external_chat_ids_json" in row.keys()
            else None
        )
        try:
            root_department_ids = json.loads(root_department_ids) if root_department_ids else []
        except json.JSONDecodeError:
            root_department_ids = []
        if isinstance(root_department_ids, dict):
            root_department_ids = root_department_ids.get("values") or []
        try:
            managed_tag_ids = json.loads(managed_tag_ids) if managed_tag_ids else []
        except json.JSONDecodeError:
            managed_tag_ids = []
        if isinstance(managed_tag_ids, dict):
            managed_tag_ids = managed_tag_ids.get("values") or []
        try:
            managed_external_chat_ids = json.loads(managed_external_chat_ids) if managed_external_chat_ids else []
        except json.JSONDecodeError:
            managed_external_chat_ids = []
        if isinstance(managed_external_chat_ids, dict):
            managed_external_chat_ids = managed_external_chat_ids.get("values") or []
        return cls(
            connector_id=str(row["connector_id"] or ""),
            org_id=str(row["org_id"] or "default"),
            name=str(row["name"] or ""),
            config_path=str(row["config_path"] or ""),
            ldap_server=str(row["ldap_server"] or "") if "ldap_server" in row.keys() else "",
            ldap_domain=str(row["ldap_domain"] or "") if "ldap_domain" in row.keys() else "",
            ldap_username=str(row["ldap_username"] or "") if "ldap_username" in row.keys() else "",
            ldap_password=str(row["ldap_password"] or "") if "ldap_password" in row.keys() else "",
            ldap_use_ssl=bool(row["ldap_use_ssl"]) if "ldap_use_ssl" in row.keys() and row["ldap_use_ssl"] is not None else None,
            ldap_port=int(row["ldap_port"]) if "ldap_port" in row.keys() and row["ldap_port"] is not None else None,
            ldap_validate_cert=bool(row["ldap_validate_cert"]) if "ldap_validate_cert" in row.keys() and row["ldap_validate_cert"] is not None else None,
            ldap_ca_cert_path=str(row["ldap_ca_cert_path"] or "") if "ldap_ca_cert_path" in row.keys() else "",
            default_password=str(row["default_password"] or "") if "default_password" in row.keys() else "",
            force_change_password=bool(row["force_change_password"]) if "force_change_password" in row.keys() and row["force_change_password"] is not None else None,
            password_complexity=str(row["password_complexity"] or "") if "password_complexity" in row.keys() else "",
            root_department_ids=[int(value) for value in root_department_ids if str(value).strip()],
            username_template=str(row["username_template"] or ""),
            disabled_users_ou=str(row["disabled_users_ou"] or ""),
            group_type=str(row["group_type"] or "security"),
            group_mail_domain=str(row["group_mail_domain"] or ""),
            custom_group_ou_path=str(row["custom_group_ou_path"] or ""),
            managed_tag_ids=[str(value) for value in managed_tag_ids if str(value).strip()],
            managed_external_chat_ids=[
                str(value) for value in managed_external_chat_ids if str(value).strip()
            ],
            is_enabled=bool(row["is_enabled"]),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )


@dataclass(slots=True)
class OffboardingRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    connector_id: str = "default"
    source_user_id: str = ""
    ad_username: str = ""
    status: str = "pending"
    reason: str = ""
    manager_userids: list[str] = field(default_factory=list)
    first_missing_at: str = ""
    due_at: str = ""
    notified_at: str = ""
    last_job_id: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "OffboardingRecord":
        source_user_id = (
            row["source_user_id"]
            if "source_user_id" in row.keys()
            else row["wecom_userid"]
        )
        manager_userids = row["manager_userids_json"] if "manager_userids_json" in row.keys() else None
        if isinstance(manager_userids, str) and manager_userids:
            try:
                manager_userids = json.loads(manager_userids)
            except json.JSONDecodeError:
                manager_userids = [manager_userids]
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            connector_id=str(row["connector_id"] or "default"),
            source_user_id=str(source_user_id or ""),
            ad_username=str(row["ad_username"] or ""),
            status=str(row["status"] or "pending"),
            reason=str(row["reason"] or ""),
            manager_userids=[str(value) for value in (manager_userids or []) if str(value).strip()],
            first_missing_at=str(row["first_missing_at"] or ""),
            due_at=str(row["due_at"] or ""),
            notified_at=str(row["notified_at"] or ""),
            last_job_id=str(row["last_job_id"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

    @property
    def wecom_userid(self) -> str:
        return self.source_user_id

    @wecom_userid.setter
    def wecom_userid(self, value: str) -> None:
        self.source_user_id = str(value or "").strip()

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["wecom_userid"] = self.source_user_id
        return data


@dataclass(slots=True)
class UserLifecycleRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    lifecycle_type: str = ""
    connector_id: str = "default"
    source_user_id: str = ""
    ad_username: str = ""
    status: str = "pending"
    reason: str = ""
    employment_type: str = ""
    sponsor_userid: str = ""
    manager_userids: list[str] = field(default_factory=list)
    effective_at: str = ""
    notified_at: str = ""
    completed_at: str = ""
    last_job_id: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "UserLifecycleRecord":
        source_user_id = (
            row["source_user_id"]
            if "source_user_id" in row.keys()
            else row["wecom_userid"]
        )
        manager_userids = row["manager_userids_json"] if "manager_userids_json" in row.keys() else None
        payload = row["payload_json"] if "payload_json" in row.keys() else None
        if isinstance(manager_userids, str) and manager_userids:
            try:
                manager_userids = json.loads(manager_userids)
            except json.JSONDecodeError:
                manager_userids = [manager_userids]
        if isinstance(payload, str) and payload:
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {"raw": payload}
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            lifecycle_type=str(row["lifecycle_type"] or ""),
            connector_id=str(row["connector_id"] or "default"),
            source_user_id=str(source_user_id or ""),
            ad_username=str(row["ad_username"] or ""),
            status=str(row["status"] or "pending"),
            reason=str(row["reason"] or ""),
            employment_type=str(row["employment_type"] or ""),
            sponsor_userid=str(row["sponsor_userid"] or ""),
            manager_userids=[str(value) for value in (manager_userids or []) if str(value).strip()],
            effective_at=str(row["effective_at"] or ""),
            notified_at=str(row["notified_at"] or ""),
            completed_at=str(row["completed_at"] or ""),
            last_job_id=str(row["last_job_id"] or ""),
            payload=dict(payload or {}),
            updated_at=str(row["updated_at"] or ""),
        )

    @property
    def wecom_userid(self) -> str:
        return self.source_user_id

    @wecom_userid.setter
    def wecom_userid(self, value: str) -> None:
        self.source_user_id = str(value or "").strip()

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["wecom_userid"] = self.source_user_id
        return data


@dataclass(slots=True)
class CustomManagedGroupBindingRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    connector_id: str = "default"
    source_type: str = ""
    source_key: str = ""
    group_sam: str = ""
    group_dn: str = ""
    group_cn: str = ""
    display_name: str = ""
    status: str = "active"
    last_seen_at: str = ""
    archived_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: Any) -> "CustomManagedGroupBindingRecord":
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            connector_id=str(row["connector_id"] or "default"),
            source_type=str(row["source_type"] or ""),
            source_key=str(row["source_key"] or ""),
            group_sam=str(row["group_sam"] or ""),
            group_dn=str(row["group_dn"] or ""),
            group_cn=str(row["group_cn"] or ""),
            display_name=str(row["display_name"] or ""),
            status=str(row["status"] or "active"),
            last_seen_at=str(row["last_seen_at"] or ""),
            archived_at=str(row["archived_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )


@dataclass(slots=True)
class SyncReplayRequestRecord(MappingLikeModel):
    id: Optional[int] = None
    org_id: str = "default"
    request_type: str = ""
    execution_mode: str = ""
    status: str = "pending"
    requested_by: str = ""
    target_scope: str = ""
    target_id: str = ""
    trigger_reason: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    started_at: str = ""
    finished_at: str = ""
    last_job_id: str = ""
    result_summary: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: Any) -> "SyncReplayRequestRecord":
        payload = row["payload_json"] if "payload_json" in row.keys() else None
        result_summary = row["result_summary_json"] if "result_summary_json" in row.keys() else None
        try:
            payload = json.loads(payload) if payload else {}
        except json.JSONDecodeError:
            payload = {}
        try:
            result_summary = json.loads(result_summary) if result_summary else {}
        except json.JSONDecodeError:
            result_summary = {}
        return cls(
            id=int(row["id"]) if row["id"] is not None else None,
            org_id=str(row["org_id"] or "default") if "org_id" in row.keys() else "default",
            request_type=str(row["request_type"] or ""),
            execution_mode=str(row["execution_mode"] or ""),
            status=str(row["status"] or "pending"),
            requested_by=str(row["requested_by"] or ""),
            target_scope=str(row["target_scope"] or ""),
            target_id=str(row["target_id"] or ""),
            trigger_reason=str(row["trigger_reason"] or ""),
            payload=dict(payload or {}),
            created_at=str(row["created_at"] or ""),
            started_at=str(row["started_at"] or ""),
            finished_at=str(row["finished_at"] or ""),
            last_job_id=str(row["last_job_id"] or ""),
            result_summary=dict(result_summary or {}),
        )


@dataclass(slots=True)
class ManagedGroupTarget:
    exists: bool
    group_sam: str
    group_cn: str
    group_dn: str
    display_name: str
    description: str
    binding_source: str
    created: bool
    binding_exists: bool
    department_id: int
    parent_department_id: Optional[int]
    ou_name: str
    ou_dn: str
    full_path: list[str] = field(default_factory=list)
    policy: GroupPolicyEvaluation = field(default_factory=GroupPolicyEvaluation)

    def apply_mapping(self, data: Dict[str, Any] | DepartmentGroupInfo) -> None:
        if hasattr(data, "to_dict"):
            data = data.to_dict()
        for field_name in (
            "exists",
            "group_sam",
            "group_cn",
            "group_dn",
            "display_name",
            "description",
            "binding_source",
            "created",
            "binding_exists",
        ):
            if field_name in data and data[field_name] is not None:
                setattr(self, field_name, data[field_name])

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DepartmentAction:
    connector_id: str
    department_id: int
    parent_department_id: Optional[int]
    ou_name: str
    parent_dn: str
    ou_dn: str
    full_path: list[str]
    group_target: ManagedGroupTarget
    should_manage_group: bool


@dataclass(slots=True)
class UserAction:
    connector_id: str
    operation_type: str
    username: str
    display_name: str
    email: str
    ou_dn: str
    ou_path: list[str]
    target_department_id: int
    placement_reason: str
    user: SourceDirectoryUser
    lifecycle_profile: Dict[str, Any] = field(default_factory=dict)

    @property
    def source_user_id(self) -> str:
        return self.user.source_user_id


@dataclass(slots=True)
class GroupMembershipAction:
    connector_id: str
    source_user_id: str
    username: str
    group_sam: str
    group_dn: str
    group_display_name: str
    department_id: int
    operation_type: str = "add_user_to_group"

    @property
    def wecom_userid(self) -> str:
        return self.source_user_id

    @wecom_userid.setter
    def wecom_userid(self, value: str) -> None:
        self.source_user_id = str(value or "").strip()


@dataclass(slots=True)
class GroupHierarchyAction:
    connector_id: str
    child_department_id: int
    parent_department_id: int
    child_group_sam: str
    child_group_dn: str
    child_display_name: str
    parent_group_sam: str
    parent_group_dn: str
    parent_display_name: str


@dataclass(slots=True)
class GroupCleanupAction:
    connector_id: str
    child_department_id: int
    child_group_sam: str
    child_group_dn: str
    parent_group_sam: str
    parent_group_dn: str
    expected_parent_group_sam: Optional[str]


@dataclass(slots=True)
class DisableUserAction:
    connector_id: str
    username: str
    source_user_id: str = ""
    reason: str = ""
    employment_type: str = ""
    sponsor_userid: str = ""
    effective_at: str = ""

    @property
    def wecom_userid(self) -> str:
        return self.source_user_id

    @wecom_userid.setter
    def wecom_userid(self, value: str) -> None:
        self.source_user_id = str(value or "").strip()
