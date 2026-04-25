from __future__ import annotations

import json
import re
from typing import Any, Dict


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
