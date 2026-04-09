from __future__ import annotations

import logging
import threading
import time
from typing import Any, Optional

from sync_app.services.runtime import run_sync_job
from sync_app.storage.local_db import SettingsRepository, WebAuditLogRepository

LOGGER = logging.getLogger(__name__)

LOCAL_WEB_BIND_HOSTS = {"127.0.0.1", "localhost", "::1"}
SECURE_COOKIE_MODES = {"auto", "always", "never"}
WEB_RUNTIME_RESTART_KEYS = (
    "bind_host",
    "bind_port",
    "public_base_url",
    "session_cookie_secure_mode",
    "session_cookie_secure",
    "trust_proxy_headers",
    "forwarded_allow_ips",
)


def _to_text(value: Any, default: str = "") -> str:
    if isinstance(value, str):
        return value.strip()
    return default


def _clean_public_base_url(value: Optional[str]) -> str:
    return str(value or "").strip().rstrip("/")


def normalize_secure_cookie_mode(value: Optional[str]) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in SECURE_COOKIE_MODES else "auto"


def resolve_web_runtime_settings(
    settings_repo: SettingsRepository,
    *,
    bind_host: str | None = None,
    bind_port: int | None = None,
    public_base_url: str | None = None,
    session_cookie_secure_mode: str | None = None,
    trust_proxy_headers: bool | None = None,
    forwarded_allow_ips: str | None = None,
) -> dict[str, Any]:
    resolved_bind_host = _to_text(
        bind_host,
        settings_repo.get_value("web_bind_host", "127.0.0.1") or "127.0.0.1",
    ) or "127.0.0.1"
    resolved_bind_port = max(
        int(bind_port or settings_repo.get_int("web_bind_port", 8000) or 8000),
        1,
    )
    resolved_public_base_url = _clean_public_base_url(
        public_base_url if public_base_url is not None else settings_repo.get_value("web_public_base_url", "")
    )
    resolved_secure_mode = normalize_secure_cookie_mode(
        session_cookie_secure_mode
        if session_cookie_secure_mode is not None
        else settings_repo.get_value("web_session_cookie_secure_mode", "auto")
    )
    resolved_trust_proxy_headers = (
        settings_repo.get_bool("web_trust_proxy_headers", False)
        if trust_proxy_headers is None
        else bool(trust_proxy_headers)
    )
    resolved_forwarded_allow_ips = _to_text(
        forwarded_allow_ips,
        settings_repo.get_value("web_forwarded_allow_ips", "127.0.0.1") or "127.0.0.1",
    ) or "127.0.0.1"
    bind_is_local = resolved_bind_host.lower() in LOCAL_WEB_BIND_HOSTS
    public_url_is_https = resolved_public_base_url.lower().startswith("https://")
    session_cookie_secure = resolved_secure_mode == "always" or (
        resolved_secure_mode == "auto" and (public_url_is_https or not bind_is_local)
    )

    warnings: list[str] = []
    if resolved_secure_mode == "never":
        warnings.append("Secure session cookies are disabled.")
    if resolved_public_base_url and not public_url_is_https:
        warnings.append("Public base URL does not use HTTPS.")
    if resolved_trust_proxy_headers and resolved_forwarded_allow_ips in {"*", "0.0.0.0", "0.0.0.0/0", "::/0"}:
        warnings.append("Forwarded proxy headers are trusted from every IP address.")

    return {
        "bind_host": resolved_bind_host,
        "bind_port": resolved_bind_port,
        "public_base_url": resolved_public_base_url,
        "session_cookie_secure_mode": resolved_secure_mode,
        "session_cookie_secure": session_cookie_secure,
        "trust_proxy_headers": resolved_trust_proxy_headers,
        "forwarded_allow_ips": resolved_forwarded_allow_ips,
        "warnings": warnings,
    }


def web_runtime_requires_restart(
    current_runtime_settings: dict[str, Any],
    persisted_runtime_settings: dict[str, Any],
) -> bool:
    return any(
        current_runtime_settings.get(key) != persisted_runtime_settings.get(key)
        for key in WEB_RUNTIME_RESTART_KEYS
    )


class WebSyncRunner:
    def __init__(self, *, db_path: str, audit_repo: WebAuditLogRepository):
        self.db_path = db_path
        self.audit_repo = audit_repo
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self.last_error = ""

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def launch(
        self,
        *,
        mode: str,
        actor_username: str,
        org_id: str,
        config_path: str,
    ) -> tuple[bool, str]:
        with self._lock:
            if self.is_running():
                return False, "A synchronization job is already running in the background"
            self.last_error = ""
            self._thread = threading.Thread(
                target=self._run_job,
                kwargs={
                    "mode": mode,
                    "actor_username": actor_username,
                    "org_id": org_id,
                    "config_path": config_path,
                },
                daemon=True,
                name=f"web-sync-{org_id}-{mode}",
            )
            self._thread.start()
        return True, "Synchronization job started"

    def _run_job(self, *, mode: str, actor_username: str, org_id: str, config_path: str) -> None:
        try:
            result = run_sync_job(
                execution_mode=mode,
                trigger_type="web",
                db_path=self.db_path,
                config_path=config_path,
                org_id=org_id,
            )
            self.audit_repo.add_log(
                org_id=org_id,
                actor_username=actor_username,
                action_type="job.run",
                target_type="sync_job",
                target_id=result.get("job_id"),
                result="success",
                message=f"Started {mode} synchronization job",
                payload={
                    "job_id": result.get("job_id"),
                    "mode": mode,
                    "org_id": org_id,
                    "error_count": result.get("error_count"),
                },
            )
        except Exception as exc:
            self.last_error = str(exc)
            LOGGER.exception("web sync job failed")
            self.audit_repo.add_log(
                org_id=org_id,
                actor_username=actor_username,
                action_type="job.run",
                target_type="sync_job",
                target_id="",
                result="error",
                message=f"Failed to start synchronization job: {exc}",
                payload={"mode": mode, "org_id": org_id},
            )


class LoginRateLimiter:
    def __init__(self, *, max_attempts: int, window_seconds: int, lockout_seconds: int):
        self.max_attempts = max(int(max_attempts or 1), 1)
        self.window_seconds = max(int(window_seconds or 1), 1)
        self.lockout_seconds = max(int(lockout_seconds or 1), 1)
        self._lock = threading.Lock()
        self._failed_attempts: dict[str, list[float]] = {}
        self._locked_until: dict[str, float] = {}

    def _build_key(self, username: str, client_ip: str) -> str:
        normalized_username = str(username or "").strip().lower() or "-"
        normalized_ip = str(client_ip or "").strip().lower() or "unknown"
        return f"{normalized_ip}::{normalized_username}"

    def _prune(self, key: str, *, now: float) -> None:
        cutoff = now - self.window_seconds
        failures = [timestamp for timestamp in self._failed_attempts.get(key, []) if timestamp >= cutoff]
        if failures:
            self._failed_attempts[key] = failures
        else:
            self._failed_attempts.pop(key, None)

        locked_until = self._locked_until.get(key, 0.0)
        if locked_until and locked_until <= now:
            self._locked_until.pop(key, None)

    def check(self, username: str, client_ip: str) -> tuple[bool, int]:
        key = self._build_key(username, client_ip)
        now = time.time()
        with self._lock:
            self._prune(key, now=now)
            locked_until = self._locked_until.get(key, 0.0)
            if not locked_until or locked_until <= now:
                return False, 0
            return True, max(int(locked_until - now), 1)

    def record_failure(self, username: str, client_ip: str) -> tuple[bool, int]:
        key = self._build_key(username, client_ip)
        now = time.time()
        with self._lock:
            self._prune(key, now=now)
            failures = self._failed_attempts.setdefault(key, [])
            failures.append(now)
            if len(failures) < self.max_attempts:
                return False, 0
            self._locked_until[key] = now + self.lockout_seconds
            self._failed_attempts.pop(key, None)
            return True, self.lockout_seconds

    def clear(self, username: str, client_ip: str) -> None:
        key = self._build_key(username, client_ip)
        with self._lock:
            self._failed_attempts.pop(key, None)
            self._locked_until.pop(key, None)
