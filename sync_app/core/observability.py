from __future__ import annotations

import logging
import re
import threading
import uuid
from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator, Mapping


_CORRELATION_ID = ContextVar("correlation_id", default="-")
_ORG_ID = ContextVar("observability_org_id", default="-")
_JOB_ID = ContextVar("observability_job_id", default="-")

_BEARER_PATTERN = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")
_SENSITIVE_QUOTED_PATTERN = re.compile(
    r"(?i)([\"']?(?:password|passwd|corpsecret|client_secret|api_token|access_token|refresh_token|authorization|secret|token)[\"']?\s*[:=]\s*)([\"'])(.*?)(\2)"
)
_SENSITIVE_UNQUOTED_PATTERN = re.compile(
    r"(?i)\b(password|passwd|corpsecret|client_secret|api_token|access_token|refresh_token|authorization|secret|token)\s*[:=]\s*([^\s,;&]+)"
)
_BASIC_AUTH_URL_PATTERN = re.compile(r"(?i)(https?://[^:/\s]+:)([^@/\s]+)(@)")
_EMAIL_PATTERN = re.compile(r"(?<![\w.+-])[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}(?![\w.-])", re.IGNORECASE)
_PHONE_PATTERN = re.compile(r"(?<!\d)(?:\+?86[- ]?)?1[3-9]\d{9}(?!\d)")


def new_correlation_id() -> str:
    return uuid.uuid4().hex


def normalize_correlation_id(value: str | None) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]", "", str(value or "").strip())[:64]
    return normalized or new_correlation_id()


def redact_sensitive_text(value: Any) -> str:
    text = str(value or "")
    text = _BEARER_PATTERN.sub("Bearer [REDACTED]", text)
    text = _SENSITIVE_QUOTED_PATTERN.sub(r"\1\2[REDACTED]\2", text)
    text = _SENSITIVE_UNQUOTED_PATTERN.sub(lambda match: f"{match.group(1)}=[REDACTED]", text)
    text = _BASIC_AUTH_URL_PATTERN.sub(r"\1[REDACTED]\3", text)
    text = _EMAIL_PATTERN.sub("[REDACTED_EMAIL]", text)
    return _PHONE_PATTERN.sub("[REDACTED_PHONE]", text)


@contextmanager
def bind_observability_context(
    *,
    correlation_id: str | None = None,
    org_id: str | None = None,
    job_id: str | None = None,
) -> Iterator[None]:
    tokens = []
    if correlation_id is not None:
        tokens.append((_CORRELATION_ID, _CORRELATION_ID.set(normalize_correlation_id(correlation_id))))
    if org_id is not None:
        tokens.append((_ORG_ID, _ORG_ID.set(str(org_id or "-").strip() or "-")))
    if job_id is not None:
        tokens.append((_JOB_ID, _JOB_ID.set(str(job_id or "-").strip() or "-")))
    try:
        yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)


class ObservabilityContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = _CORRELATION_ID.get()
        record.org_id = _ORG_ID.get()
        record.job_id = _JOB_ID.get()
        return True


class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return redact_sensitive_text(super().format(record))


@dataclass(frozen=True, slots=True)
class _MetricKey:
    name: str
    labels: tuple[tuple[str, str], ...]


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[_MetricKey, float] = defaultdict(float)
        self._observations: dict[_MetricKey, dict[str, float]] = {}

    @staticmethod
    def _key(name: str, labels: Mapping[str, Any] | None = None) -> _MetricKey:
        normalized_name = re.sub(r"[^a-zA-Z0-9_:]", "_", str(name or "metric"))
        normalized_labels = tuple(
            sorted((str(key), str(value)) for key, value in dict(labels or {}).items())
        )
        return _MetricKey(normalized_name, normalized_labels)

    def increment(self, name: str, value: float = 1.0, *, labels: Mapping[str, Any] | None = None) -> None:
        key = self._key(name, labels)
        with self._lock:
            self._counters[key] += float(value)

    def observe(self, name: str, value: float, *, labels: Mapping[str, Any] | None = None) -> None:
        key = self._key(name, labels)
        numeric_value = float(value)
        with self._lock:
            current = self._observations.setdefault(key, {"count": 0.0, "sum": 0.0, "max": 0.0})
            current["count"] += 1.0
            current["sum"] += numeric_value
            current["max"] = max(current["max"], numeric_value)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            counters = dict(self._counters)
            observations = {key: dict(value) for key, value in self._observations.items()}
        return {
            "counters": [
                {"name": key.name, "labels": dict(key.labels), "value": counter_value}
                for key, counter_value in sorted(counters.items(), key=lambda item: (item[0].name, item[0].labels))
            ],
            "observations": [
                {"name": key.name, "labels": dict(key.labels), **observation_value}
                for key, observation_value in sorted(
                    observations.items(), key=lambda item: (item[0].name, item[0].labels)
                )
            ],
        }

    @staticmethod
    def _render_labels(labels: tuple[tuple[str, str], ...]) -> str:
        if not labels:
            return ""
        rendered = ",".join(
            f'{key}="{str(value).replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"'
            for key, value in labels
        )
        return "{" + rendered + "}"

    def render_prometheus(self) -> str:
        with self._lock:
            counters = dict(self._counters)
            observations = {key: dict(value) for key, value in self._observations.items()}
        lines: list[str] = []
        for key, counter_value in sorted(counters.items(), key=lambda item: (item[0].name, item[0].labels)):
            lines.append(f"{key.name}{self._render_labels(key.labels)} {counter_value:g}")
        for key, observation_value in sorted(
            observations.items(), key=lambda item: (item[0].name, item[0].labels)
        ):
            labels = self._render_labels(key.labels)
            lines.append(f"{key.name}_count{labels} {observation_value['count']:g}")
            lines.append(f"{key.name}_sum{labels} {observation_value['sum']:g}")
            lines.append(f"{key.name}_max{labels} {observation_value['max']:g}")
        return "\n".join(lines) + ("\n" if lines else "")

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._observations.clear()


METRICS = MetricsRegistry()


__all__ = [
    "METRICS",
    "MetricsRegistry",
    "ObservabilityContextFilter",
    "RedactingFormatter",
    "bind_observability_context",
    "new_correlation_id",
    "normalize_correlation_id",
    "redact_sensitive_text",
]
