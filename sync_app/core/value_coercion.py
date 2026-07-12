from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any


DEPARTMENT_ID_KEYS = ("dept_id", "deptId", "department_id", "departmentId")


def _unique_ints(values: Iterable[Any]) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for value in values:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed in seen:
            continue
        seen.add(parsed)
        normalized.append(parsed)
    return normalized


def _coerce_mapping_ints(value: Mapping[Any, Any]) -> list[int]:
    explicit_values = coerce_int_list(
        [value[key] for key in DEPARTMENT_ID_KEYS if key in value]
    )
    if explicit_values:
        return explicit_values

    numeric_keys = [
        key
        for key in value
        if str(key).strip().lstrip("-").isdigit()
    ]
    if numeric_keys:
        return _unique_ints(numeric_keys)

    return coerce_int_list(list(value.values()))


def _coerce_text_ints(value: str) -> list[int]:
    text = value.strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            return coerce_int_list(json.loads(text))
        except json.JSONDecodeError:
            pass
    return _unique_ints(re.findall(r"-?\d+", text))


def coerce_int_list(value: Any) -> list[int]:
    """Normalize nested directory payload values into ordered unique integers."""
    if value in (None, ""):
        return []
    if isinstance(value, Mapping):
        return _coerce_mapping_ints(value)
    if isinstance(value, (list, tuple, set)):
        return _unique_ints(
            item
            for nested_value in value
            for item in coerce_int_list(nested_value)
        )
    if isinstance(value, str):
        return _coerce_text_ints(value)
    return _unique_ints([value])


__all__ = ["coerce_int_list"]
