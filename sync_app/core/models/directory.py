from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional

from sync_app.core.models.utils import _coerce_int_list, _extract_department_ids


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
