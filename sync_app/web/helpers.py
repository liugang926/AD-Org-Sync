from __future__ import annotations

import csv
import math
from typing import Iterable, Sequence, TypeVar

T = TypeVar("T")


def parse_bulk_bindings(text: str) -> tuple[list[dict[str, str]], list[str]]:
    rows: list[dict[str, str]] = []
    errors: list[str] = []
    raw_lines = [line for line in (text or "").splitlines() if line.strip()]
    if not raw_lines:
        return rows, errors

    for index, raw_line in enumerate(raw_lines, start=1):
        parts = next(csv.reader([raw_line], skipinitialspace=True), [])
        trimmed_parts = [str(part or "").strip() for part in parts]
        if index == 1 and trimmed_parts[:2] == ["source_user_id", "ad_username"]:
            continue
        if len(parts) < 2:
            errors.append(f"Line {index}: expected at least source_user_id,ad_username")
            continue
        source_user_id = trimmed_parts[0]
        ad_username = trimmed_parts[1]
        rule_owner = ""
        effective_reason = ""
        next_review_at = ""
        notes = ""
        if len(trimmed_parts) >= 5:
            rule_owner = trimmed_parts[2]
            effective_reason = trimmed_parts[3]
            next_review_at = trimmed_parts[4]
            notes = trimmed_parts[5] if len(trimmed_parts) > 5 else ""
        elif len(trimmed_parts) == 4:
            rule_owner = trimmed_parts[2]
            notes = trimmed_parts[3]
        elif len(trimmed_parts) > 2:
            notes = trimmed_parts[2]
        if not source_user_id or not ad_username:
            errors.append(f"Line {index}: source user ID and AD username are required")
            continue
        rows.append(
            {
                "source_user_id": source_user_id,
                "ad_username": ad_username,
                "rule_owner": rule_owner,
                "effective_reason": effective_reason,
                "next_review_at": next_review_at,
                "notes": notes,
            }
        )
    return rows, errors


def paginate_records(records: Sequence[T] | Iterable[T], page: int, page_size: int) -> dict[str, object]:
    items = list(records)
    page_size = max(int(page_size or 20), 1)
    total = len(items)
    total_pages = max(math.ceil(total / page_size), 1)
    page = min(max(int(page or 1), 1), total_pages)
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "items": items[start:end],
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "prev_page": page - 1 if page > 1 else 1,
        "next_page": page + 1 if page < total_pages else total_pages,
    }
