from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Any, Callable


@dataclass(frozen=True, slots=True)
class SSPRRateLimitDecision:
    limited: bool
    retry_after_seconds: int = 0


class SSPRRateLimiter:
    """Rate limit SSPR actions by a privacy-preserving composite bucket.

    A SQLite backend is used by the Web application so limits are shared by
    workers and survive restarts.  The in-memory fallback remains useful for
    isolated unit tests and non-Web consumers.
    """

    def __init__(
        self,
        *,
        max_attempts: int = 5,
        window_seconds: int = 300,
        lockout_seconds: int = 300,
        now_factory: Callable[[], datetime] | None = None,
        store: Any | None = None,
    ) -> None:
        self.max_attempts = max(int(max_attempts or 1), 1)
        self.window = timedelta(seconds=max(int(window_seconds or 1), 1))
        self.lockout = timedelta(seconds=max(int(lockout_seconds or 1), 1))
        self.now_factory = now_factory or (lambda: datetime.now(timezone.utc))
        self.store = store
        self._state: dict[str, dict[str, object]] = {}

    @staticmethod
    def _bucket_hash(
        *,
        org_id: str,
        source_user_id: str,
        request_ip: str,
        provider_id: str,
        action: str,
    ) -> str:
        parts = (
            str(org_id or "").strip().lower() or "default",
            str(provider_id or "").strip().lower() or "unknown",
            str(source_user_id or "").strip().casefold() or "anonymous",
            str(request_ip or "").strip().lower() or "unknown",
            str(action or "").strip().lower() or "verify",
        )
        return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()

    def check(
        self,
        *,
        org_id: str,
        source_user_id: str = "",
        request_ip: str,
        provider_id: str = "",
        action: str = "verify",
    ) -> SSPRRateLimitDecision:
        key = self._bucket_hash(
            org_id=org_id,
            source_user_id=source_user_id,
            request_ip=request_ip,
            provider_id=provider_id,
            action=action,
        )
        now = self.now_factory()
        if self.store is not None:
            limited, retry_after = self.store.evaluate(
                key,
                now=now,
                max_attempts=self.max_attempts,
                window_seconds=max(int(self.window.total_seconds()), 1),
                lockout_seconds=max(int(self.lockout.total_seconds()), 1),
                record_failure=False,
            )
            return SSPRRateLimitDecision(limited=limited, retry_after_seconds=retry_after)
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

    def record_failure(
        self,
        *,
        org_id: str,
        source_user_id: str = "",
        request_ip: str,
        provider_id: str = "",
        action: str = "verify",
    ) -> SSPRRateLimitDecision:
        key = self._bucket_hash(
            org_id=org_id,
            source_user_id=source_user_id,
            request_ip=request_ip,
            provider_id=provider_id,
            action=action,
        )
        now = self.now_factory()
        if self.store is not None:
            limited, retry_after = self.store.evaluate(
                key,
                now=now,
                max_attempts=self.max_attempts,
                window_seconds=max(int(self.window.total_seconds()), 1),
                lockout_seconds=max(int(self.lockout.total_seconds()), 1),
                record_failure=True,
            )
            return SSPRRateLimitDecision(limited=limited, retry_after_seconds=retry_after)
        decision = self.check(
            org_id=org_id,
            source_user_id=source_user_id,
            request_ip=request_ip,
            provider_id=provider_id,
            action=action,
        )
        if decision.limited:
            return decision
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

    def clear(
        self,
        *,
        org_id: str,
        source_user_id: str = "",
        request_ip: str,
        provider_id: str = "",
        action: str = "verify",
    ) -> None:
        key = self._bucket_hash(
            org_id=org_id,
            source_user_id=source_user_id,
            request_ip=request_ip,
            provider_id=provider_id,
            action=action,
        )
        if self.store is not None:
            self.store.delete_bucket(key)
            return
        self._state.pop(key, None)

    def _active_failures(self, state: dict[str, object], now: datetime) -> list[datetime]:
        failures = state.get("failures")
        if not isinstance(failures, list):
            return []
        cutoff = now - self.window
        return [item for item in failures if isinstance(item, datetime) and item >= cutoff]
