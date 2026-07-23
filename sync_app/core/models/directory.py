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
    provider_id: str = ""
    employee_id: str = ""
    mobile: str = ""
    position: str = ""
    department_names: list[str] = field(default_factory=list)
    primary_department_id: Optional[int] = None
    account_status: str = "active"
    is_active: bool = True
    platform_union_id: str = ""
    platform_open_id: str = ""
    employee_number: str = ""
    given_name: str = ""
    family_name: str = ""
    enterprise_email: str = ""
    telephone: str = ""
    job_title: str = ""
    employee_type: str = ""
    employment_status: str = ""
    manager_account_id: str = ""
    work_station: str = ""
    city: str = ""
    hire_date: str = ""
    leave_date: str = ""

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
        normalized = self.from_source_payload(payload)
        for field_name in (
            "provider_id", "employee_id", "mobile", "position", "account_status",
            "platform_union_id", "platform_open_id", "employee_number", "given_name",
            "family_name", "enterprise_email", "telephone", "job_title", "employee_type",
            "employment_status", "manager_account_id", "work_station", "city", "hire_date",
            "leave_date",
        ):
            value = getattr(normalized, field_name)
            if value not in (None, ""):
                setattr(self, field_name, value)
        if normalized.department_names:
            self.department_names = list(normalized.department_names)
        if normalized.primary_department_id is not None:
            self.primary_department_id = normalized.primary_department_id
        self.is_active = normalized.is_active
        self.raw_payload.update(payload)

    def to_state_payload(self) -> Dict[str, Any]:
        payload = dict(self.raw_payload)
        payload.update(
            {
                "userid": self.userid,
                "name": self.name,
                "email": self.email,
                "department": list(self.departments),
                "provider_id": self.provider_id,
                "employee_id": self.employee_id,
                "mobile": self.mobile,
                "position": self.position,
                "department_names": list(self.department_names),
                "primary_department_id": self.primary_department_id,
                "account_status": self.account_status,
                "is_active": self.is_active,
                "platform_union_id": self.platform_union_id,
                "platform_open_id": self.platform_open_id,
                "employee_number": self.employee_number,
                "given_name": self.given_name,
                "family_name": self.family_name,
                "enterprise_email": self.enterprise_email,
                "telephone": self.telephone,
                "job_title": self.job_title,
                "employee_type": self.employee_type,
                "employment_status": self.employment_status,
                "manager_account_id": self.manager_account_id,
                "work_station": self.work_station,
                "city": self.city,
                "hire_date": self.hire_date,
                "leave_date": self.leave_date,
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
            or payload_copy.get("user_id")
            or payload_copy.get("open_id")
            or payload_copy.get("staffid")
            or payload_copy.get("staffId")
            or payload_copy.get("unionid")
            or payload_copy.get("unionId")
            or payload_copy.get("emplId")
            or ""
        )
        email = (
            payload_copy.get("email")
            or payload_copy.get("enterprise_email")
            or payload_copy.get("org_email")
            or payload_copy.get("orgEmail")
            or payload_copy.get("work_email")
            or payload_copy.get("workEmail")
            or ""
        )
        employee_id = ""
        for key in (
            "employee_id", "employeeid", "employee_no", "employee_number",
            "job_number", "jobnumber", "staff_no", "staffno", "staff_id",
            "workcode", "work_code", "employeeNo", "employeeNumber", "jobNumber",
        ):
            if payload_copy.get(key) not in (None, ""):
                employee_id = str(payload_copy[key]).strip()
                break
        status_payload = payload_copy.get("status")
        if isinstance(status_payload, dict):
            is_active = not bool(
                status_payload.get("is_resigned")
                or status_payload.get("is_frozen")
                or status_payload.get("is_unjoin")
            ) and bool(status_payload.get("is_activated", True))
            account_status = "active" if is_active else "inactive"
        else:
            raw_active = payload_copy.get("is_active", payload_copy.get("active", True))
            is_active = raw_active if isinstance(raw_active, bool) else str(raw_active).strip().lower() not in {
                "0", "false", "inactive", "disabled", "resigned", "terminated"
            }
            account_status = str(status_payload or ("active" if is_active else "inactive"))
        department_names_raw = payload_copy.get("department_names") or payload_copy.get("departmentNames") or []
        if isinstance(department_names_raw, str):
            department_names = [item.strip() for item in department_names_raw.split(",") if item.strip()]
        else:
            department_names = [str(item).strip() for item in department_names_raw if str(item).strip()]
        primary_department_id = None
        for key in ("primary_department_id", "main_department", "mainDepartment", "primaryDepartmentId"):
            value = payload_copy.get(key)
            if value in (None, ""):
                continue
            try:
                primary_department_id = int(value)
                break
            except (TypeError, ValueError):
                continue
        return cls(
            userid=str(userid or ""),
            name=str(payload_copy.get("name") or payload_copy.get("nick") or payload_copy.get("displayName") or ""),
            email=str(email or ""),
            departments=_extract_department_ids(payload_copy),
            raw_payload=payload_copy,
            provider_id=str(payload_copy.get("provider_id") or ""),
            employee_id=employee_id,
            mobile=str(payload_copy.get("mobile") or payload_copy.get("phone") or ""),
            position=str(payload_copy.get("position") or payload_copy.get("title") or ""),
            department_names=department_names,
            primary_department_id=primary_department_id,
            account_status=account_status,
            is_active=is_active,
            platform_union_id=str(payload_copy.get("union_id") or payload_copy.get("unionid") or ""),
            platform_open_id=str(payload_copy.get("open_id") or payload_copy.get("openId") or ""),
            employee_number=str(payload_copy.get("employee_number") or payload_copy.get("employeeNumber") or ""),
            given_name=str(payload_copy.get("given_name") or payload_copy.get("givenName") or ""),
            family_name=str(payload_copy.get("family_name") or payload_copy.get("familyName") or ""),
            enterprise_email=str(
                payload_copy.get("enterprise_email")
                or payload_copy.get("biz_mail")
                or payload_copy.get("org_email")
                or ""
            ),
            telephone=str(payload_copy.get("telephone") or payload_copy.get("telephoneNumber") or ""),
            job_title=str(payload_copy.get("job_title") or payload_copy.get("title") or payload_copy.get("position") or ""),
            employee_type=str(payload_copy.get("employee_type") or payload_copy.get("employeeType") or ""),
            employment_status=str(payload_copy.get("employment_status") or payload_copy.get("employmentStatus") or account_status),
            manager_account_id=str(
                payload_copy.get("manager_account_id")
                or payload_copy.get("manager_userid")
                or payload_copy.get("managerUserId")
                or payload_copy.get("leader_user_id")
                or ""
            ),
            work_station=str(payload_copy.get("work_station") or payload_copy.get("workStation") or payload_copy.get("office") or ""),
            city=str(payload_copy.get("city") or ""),
            hire_date=str(payload_copy.get("hire_date") or payload_copy.get("hireDate") or payload_copy.get("join_time") or ""),
            leave_date=str(payload_copy.get("leave_date") or payload_copy.get("leaveDate") or payload_copy.get("resign_time") or ""),
        )

SourceUser = SourceDirectoryUser
WeComUser = SourceDirectoryUser
CanonicalUserDTO = SourceDirectoryUser


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
