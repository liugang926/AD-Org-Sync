from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Callable


@dataclass(frozen=True, slots=True)
class SSPRRateLimitDecision:
    limited: bool
    retry_after_seconds: int = 0


class SSPRRateLimiter:
    def __init__(
        self,
        *,
        max_attempts: int = 5,
        window_seconds: int = 300,
        lockout_seconds: int = 300,
        now_factory: Callable[[], datetime] | None = None,
    ) -> None:
        self.max_attempts = max(int(max_attempts or 1), 1)
        self.window = timedelta(seconds=max(int(window_seconds or 1), 1))
        self.lockout = timedelta(seconds=max(int(lockout_seconds or 1), 1))
        self.now_factory = now_factory or (lambda: datetime.now(timezone.utc))
        self._state: dict[tuple[str, str, str], dict[str, object]] = {}

    def check(self, *, org_id: str, source_user_id: str, request_ip: str) -> SSPRRateLimitDecision:
        key = self._key(org_id, source_user_id, request_ip)
        now = self.now_factory()
        state = self._state.get(key)
        if not state:
            return SSPRRateLimitDecision(limited=False)
        locked_until = state.get("locked_until")
        if isinstance(locked_until, datetime) and locked_until > now:
            return SSPRRateLimitDecision(
                limited=True,
                retry_after_seconds=max(ceil((locked_until - now).total_seconds()), 1),
            )
        if isinstance(locked_until, datetime):
            state["locked_until"] = None
        state["failures"] = self._active_failures(state, now)
        return SSPRRateLimitDecision(limited=False)

    def record_failure(self, *, org_id: str, source_user_id: str, request_ip: str) -> SSPRRateLimitDecision:
        decision = self.check(org_id=org_id, source_user_id=source_user_id, request_ip=request_ip)
        if decision.limited:
            return decision
        key = self._key(org_id, source_user_id, request_ip)
        now = self.now_factory()
        state = self._state.setdefault(key, {"failures": [], "locked_until": None})
        failures = [*self._active_failures(state, now), now]
        state["failures"] = failures
        if len(failures) >= self.max_attempts:
            locked_until = now + self.lockout
            state["locked_until"] = locked_until
            return SSPRRateLimitDecision(
                limited=True,
                retry_after_seconds=max(ceil((locked_until - now).total_seconds()), 1),
            )
        return SSPRRateLimitDecision(limited=False)

    def clear(self, *, org_id: str, source_user_id: str, request_ip: str) -> None:
        self._state.pop(self._key(org_id, source_user_id, request_ip), None)

    def _active_failures(self, state: dict[str, object], now: datetime) -> list[datetime]:
        failures = state.get("failures")
        if not isinstance(failures, list):
            return []
        cutoff = now - self.window
        return [item for item in failures if isinstance(item, datetime) and item >= cutoff]

    def _key(self, org_id: str, source_user_id: str, request_ip: str) -> tuple[str, str, str]:
        return (
            str(org_id or "").strip().lower() or "default",
            str(source_user_id or "").strip().lower() or "anonymous",
            str(request_ip or "").strip() or "unknown",
        )

