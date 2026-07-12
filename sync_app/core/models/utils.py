from __future__ import annotations

from typing import Any, Dict

from sync_app.core.value_coercion import coerce_int_list


def _normalize_mapping_direction_value(value: Any, default: str = "source_to_ad") -> str:
    normalized = str(value or "").strip().lower()
    aliases = {
        "wecom_to_ad": "source_to_ad",
        "ad_to_wecom": "ad_to_source",
        "source_to_ad": "source_to_ad",
        "ad_to_source": "ad_to_source",
    }
    return aliases.get(normalized, aliases.get(str(default or "").strip().lower(), "source_to_ad"))

def _coerce_int_list(value: Any) -> list[int]:
    """Compatibility wrapper for the pre-package private helper."""
    return coerce_int_list(value)

def _extract_department_ids(payload: Dict[str, Any]) -> list[int]:
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
    return coerce_int_list([payload.get(key) for key in candidate_keys])
