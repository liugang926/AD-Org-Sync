from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional

from sync_app.core.models.directory import (
    DepartmentGroupInfo,
    GroupPolicyEvaluation,
    SourceDirectoryUser,
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
