from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


FINGERPRINT_VERSION = "sha256:v2"


def _canonicalize(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _canonicalize(asdict(value))
    if isinstance(value, Mapping):
        return {
            str(key): _canonicalize(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized_items = [_canonicalize(item) for item in value]
        return sorted(
            normalized_items,
            key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        )
    if isinstance(value, Enum):
        return _canonicalize(value.value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "to_dict"):
        return _canonicalize(value.to_dict())
    return str(value)


def canonical_json(value: Any) -> str:
    return json.dumps(
        _canonicalize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def fingerprint_json(value: Any, *, namespace: str) -> str:
    envelope = {
        "namespace": str(namespace or "generic").strip().lower() or "generic",
        "payload": _canonicalize(value),
        "version": FINGERPRINT_VERSION,
    }
    digest = hashlib.sha256(canonical_json(envelope).encode("utf-8")).hexdigest()
    return f"{FINGERPRINT_VERSION}:{digest}"


__all__ = ["FINGERPRINT_VERSION", "canonical_json", "fingerprint_json"]
