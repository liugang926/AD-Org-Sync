from __future__ import annotations

from typing import Optional

from sync_app.core.models import DepartmentNode, SourceDirectoryUser, UserDepartmentBundle
from sync_app.core.sync_policies import build_identity_candidates as build_identity_candidates_from_policy


def build_identity_candidates(user: SourceDirectoryUser, *, username_template: str = "") -> list[dict[str, str]]:
    return build_identity_candidates_from_policy(user, username_template=username_template)


def resolve_target_department(
    bundle: UserDepartmentBundle,
    *,
    placement_strategy: str,
    is_department_excluded,
    override_department_id: Optional[int] = None,
) -> tuple[Optional[DepartmentNode], str]:
    valid_departments = [
        department
        for department in bundle.departments
        if department.path and not is_department_excluded(department)
    ]
    if not valid_departments:
        return None, "all_departments_excluded"

    departments_by_id = {
        department.department_id: department for department in valid_departments
    }

    if override_department_id is not None and override_department_id in departments_by_id:
        return departments_by_id[override_department_id], "manual_override"

    strategy = (placement_strategy or "source_primary_department").strip().lower()
    if strategy in {"source_primary_department", "wecom_primary_department"}:
        declared_primary_id = bundle.user.declared_primary_department_id()
        if declared_primary_id is not None and declared_primary_id in departments_by_id:
            return departments_by_id[declared_primary_id], "source_primary_department"

    if strategy == "lowest_department_id":
        department = min(valid_departments, key=lambda item: (item.department_id, len(item.path_ids), item.name))
        return department, "lowest_department_id"

    if strategy == "shortest_path":
        department = min(valid_departments, key=lambda item: (len(item.path_ids), item.department_id, item.name))
        return department, "shortest_path"

    department = sorted(valid_departments, key=lambda item: (item.department_id, len(item.path_ids), item.name))[0]
    return department, "first_non_excluded_department"
