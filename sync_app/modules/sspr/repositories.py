from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from sync_app.modules.sspr.domain import (
    SSPROAuthTransaction,
    SSPRResetReceipt,
    SSPRVerificationSession,
    SSPRVerifiedIdentity,
)
from sync_app.storage.local_db import DatabaseManager


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _as_utc(value).isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _as_utc(parsed)


def hash_capability(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def hash_user_agent(value: str) -> str:
    normalized = " ".join(str(value or "").strip().split()).lower()
    return hash_capability(normalized) if normalized else ""


def _normalize_org_id(value: str | None) -> str:
    return str(value or "").strip().lower() or "default"


def _normalize_provider_id(value: str | None) -> str:
    return str(value or "").strip().lower() or "dingtalk"


class SQLiteSSPROAuthTransactionStore:
    def __init__(
        self,
        db: DatabaseManager,
        *,
        now_factory: Callable[[], datetime] | None = None,
        token_factory: Callable[[], str] | None = None,
    ) -> None:
        self.db = db
        self.now_factory = now_factory or _utcnow
        self.token_factory = token_factory or (lambda: secrets.token_urlsafe(32))

    def create_transaction(
        self,
        *,
        org_id: str,
        provider_id: str,
        connector_id: str = "",
        corp_id: str,
        return_path: str = "/sspr/account",
        ttl_seconds: int = 300,
        request_ip: str = "",
        user_agent: str = "",
    ) -> SSPROAuthTransaction:
        safe_return_path = str(return_path or "").strip()
        if not safe_return_path.startswith("/sspr") or safe_return_path.startswith("//"):
            safe_return_path = "/sspr/account"
        now = _as_utc(self.now_factory())
        expires_at = now + timedelta(seconds=max(int(ttl_seconds or 1), 1))
        state = self.token_factory()
        with self.db.transaction() as connection:
            connection.execute(
                """
                INSERT INTO sspr_oauth_transactions (
                  state_hash, org_id, provider_id, connector_id, corp_id,
                  return_path, issued_at, expires_at, request_ip,
                  user_agent_hash, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    hash_capability(state),
                    _normalize_org_id(org_id),
                    _normalize_provider_id(provider_id),
                    str(connector_id or "").strip(),
                    str(corp_id or "").strip(),
                    safe_return_path,
                    _iso(now),
                    _iso(expires_at),
                    str(request_ip or "").strip(),
                    hash_user_agent(user_agent),
                    _iso(now),
                    _iso(now),
                ),
            )
        self.cleanup_expired()
        return SSPROAuthTransaction(
            state=state,
            org_id=_normalize_org_id(org_id),
            provider_id=_normalize_provider_id(provider_id),
            connector_id=str(connector_id or "").strip(),
            corp_id=str(corp_id or "").strip(),
            return_path=safe_return_path,
            issued_at=now,
            expires_at=expires_at,
            request_ip=str(request_ip or "").strip(),
            user_agent_hash=hash_user_agent(user_agent),
        )

    def consume_transaction(
        self,
        state: str,
        *,
        user_agent: str = "",
    ) -> SSPROAuthTransaction | None:
        state_hash = hash_capability(state)
        now = _as_utc(self.now_factory())
        current_ua_hash = hash_user_agent(user_agent)
        with self.db.transaction() as connection:
            row = connection.execute(
                """
                SELECT * FROM sspr_oauth_transactions
                WHERE state_hash = ?
                LIMIT 1
                """,
                (state_hash,),
            ).fetchone()
            if not row:
                return None
            expires_at = _parse_datetime(row["expires_at"])
            if (
                row["consumed_at"]
                or not expires_at
                or expires_at <= now
                or (
                    str(row["user_agent_hash"] or "")
                    and str(row["user_agent_hash"]) != current_ua_hash
                )
            ):
                return None
            updated = connection.execute(
                """
                UPDATE sspr_oauth_transactions
                SET consumed_at = ?, updated_at = ?
                WHERE state_hash = ?
                  AND consumed_at IS NULL
                  AND expires_at > ?
                """,
                (_iso(now), _iso(now), state_hash, _iso(now)),
            )
            if updated.rowcount != 1:
                return None
            return SSPROAuthTransaction(
                state=str(state or ""),
                org_id=str(row["org_id"] or "default"),
                provider_id=str(row["provider_id"] or "dingtalk"),
                connector_id=str(row["connector_id"] or ""),
                corp_id=str(row["corp_id"] or ""),
                return_path=str(row["return_path"] or "/sspr/account"),
                issued_at=_parse_datetime(row["issued_at"]) or now,
                expires_at=expires_at,
                request_ip=str(row["request_ip"] or ""),
                user_agent_hash=str(row["user_agent_hash"] or ""),
                consumed_at=now,
            )

    def cleanup_expired(self, *, retention_seconds: int = 86400) -> int:
        cutoff = _as_utc(self.now_factory()) - timedelta(seconds=max(int(retention_seconds or 0), 0))
        with self.db.transaction() as connection:
            result = connection.execute(
                """
                DELETE FROM sspr_oauth_transactions
                WHERE expires_at < ?
                   OR (consumed_at IS NOT NULL AND consumed_at < ?)
                """,
                (_iso(cutoff), _iso(cutoff)),
            )
            return int(result.rowcount)


class SQLiteSSPRSessionStore:
    def __init__(
        self,
        db: DatabaseManager,
        *,
        now_factory: Callable[[], datetime] | None = None,
        token_factory: Callable[[], str] | None = None,
        csrf_token_factory: Callable[[], str] | None = None,
    ) -> None:
        self.db = db
        self.now_factory = now_factory or _utcnow
        self.token_factory = token_factory or (lambda: secrets.token_urlsafe(32))
        self.csrf_token_factory = csrf_token_factory or (lambda: secrets.token_urlsafe(32))

    def create_session(
        self,
        identity: SSPRVerifiedIdentity,
        *,
        request_ip: str = "",
        user_agent: str = "",
        ttl_seconds: int = 600,
    ) -> SSPRVerificationSession:
        now = _as_utc(self.now_factory())
        expires_at = now + timedelta(seconds=max(int(ttl_seconds or 1), 1))
        session_token = self.token_factory()
        csrf_token = self.csrf_token_factory()
        normalized_org_id = _normalize_org_id(identity.org_id)
        normalized_provider = _normalize_provider_id(identity.provider_id)
        normalized_source_user_id = str(identity.source_user_id or "").strip()
        if not normalized_source_user_id:
            raise ValueError("verified source user id is required")
        with self.db.transaction() as connection:
            connection.execute(
                """
                INSERT INTO sspr_verification_sessions (
                  token_hash, csrf_token_hash, org_id, provider_id, connector_id,
                  source_user_id, display_name, issued_at, expires_at,
                  request_ip, user_agent_hash, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    hash_capability(session_token),
                    hash_capability(csrf_token),
                    normalized_org_id,
                    normalized_provider,
                    str(identity.connector_id or "").strip(),
                    normalized_source_user_id,
                    str(identity.display_name or "").strip(),
                    _iso(now),
                    _iso(expires_at),
                    str(request_ip or "").strip(),
                    hash_user_agent(user_agent),
                    _iso(now),
                    _iso(now),
                ),
            )
        self.cleanup_expired()
        return SSPRVerificationSession(
            session_id=session_token,
            org_id=normalized_org_id,
            source_user_id=normalized_source_user_id,
            provider_id=normalized_provider,
            connector_id=str(identity.connector_id or "").strip(),
            display_name=str(identity.display_name or "").strip(),
            issued_at=now,
            expires_at=expires_at,
            request_ip=str(request_ip or "").strip(),
            user_agent_hash=hash_user_agent(user_agent),
            csrf_token=csrf_token,
        )

    @staticmethod
    def _session_from_row(row: Any, *, session_id: str, csrf_token: str = "") -> SSPRVerificationSession:
        now = _utcnow()
        return SSPRVerificationSession(
            session_id=session_id,
            org_id=str(row["org_id"] or "default"),
            source_user_id=str(row["source_user_id"] or ""),
            provider_id=str(row["provider_id"] or "dingtalk"),
            connector_id=str(row["connector_id"] or ""),
            display_name=str(row["display_name"] or ""),
            issued_at=_parse_datetime(row["issued_at"]) or now,
            expires_at=_parse_datetime(row["expires_at"]) or now,
            request_ip=str(row["request_ip"] or ""),
            user_agent_hash=str(row["user_agent_hash"] or ""),
            csrf_token=csrf_token,
            consumed_at=_parse_datetime(row["consumed_at"]),
            revoked_at=_parse_datetime(row["revoked_at"]),
            claimed_at=_parse_datetime(row["claimed_at"]),
        )

    def validate_session(
        self,
        session_id: str,
        *,
        org_id: str = "",
        source_user_id: str = "",
        provider_id: str = "",
        connector_id: str = "",
        request_ip: str = "",
        user_agent: str = "",
    ) -> SSPRVerificationSession | None:
        del request_ip  # Mobile network changes are audit context, not a hard identity boundary.
        token_hash = hash_capability(session_id)
        with self.db.connection() as connection:
            row = connection.execute(
                "SELECT * FROM sspr_verification_sessions WHERE token_hash = ? LIMIT 1",
                (token_hash,),
            ).fetchone()
        if not row:
            return None
        session = self._session_from_row(row, session_id=str(session_id or ""))
        now = _as_utc(self.now_factory())
        if not session.is_active(now):
            return None
        expected_ua = str(row["user_agent_hash"] or "")
        current_ua = hash_user_agent(user_agent)
        if expected_ua and expected_ua != current_ua:
            return None
        if org_id and session.org_id != _normalize_org_id(org_id):
            return None
        if provider_id and session.provider_id != _normalize_provider_id(provider_id):
            return None
        if connector_id and session.connector_id != str(connector_id or "").strip():
            return None
        if source_user_id and session.source_user_id.casefold() != str(source_user_id or "").strip().casefold():
            return None
        return session

    def get_session_record(self, session_id: str) -> SSPRVerificationSession | None:
        """Load an inactive session for safe lifecycle auditing, never authorization."""

        if not str(session_id or "").strip():
            return None
        with self.db.connection() as connection:
            row = connection.execute(
                "SELECT * FROM sspr_verification_sessions WHERE token_hash = ? LIMIT 1",
                (hash_capability(session_id),),
            ).fetchone()
        return self._session_from_row(row, session_id="") if row else None

    def issue_csrf_token(self, session_id: str) -> str:
        session = self.validate_session(session_id)
        if session is None:
            return ""
        csrf_token = self.csrf_token_factory()
        now = _as_utc(self.now_factory())
        with self.db.transaction() as connection:
            updated = connection.execute(
                """
                UPDATE sspr_verification_sessions
                SET csrf_token_hash = ?, updated_at = ?
                WHERE token_hash = ?
                  AND consumed_at IS NULL
                  AND revoked_at IS NULL
                  AND expires_at > ?
                """,
                (hash_capability(csrf_token), _iso(now), hash_capability(session_id), _iso(now)),
            )
        return csrf_token if updated.rowcount == 1 else ""

    def validate_csrf_token(self, session_id: str, submitted_token: str) -> bool:
        if not session_id or not submitted_token:
            return False
        with self.db.connection() as connection:
            row = connection.execute(
                """
                SELECT csrf_token_hash
                FROM sspr_verification_sessions
                WHERE token_hash = ?
                  AND consumed_at IS NULL
                  AND revoked_at IS NULL
                  AND expires_at > ?
                LIMIT 1
                """,
                (hash_capability(session_id), _iso(_as_utc(self.now_factory()))),
            ).fetchone()
        return bool(row and secrets.compare_digest(str(row["csrf_token_hash"] or ""), hash_capability(submitted_token)))

    def claim_session(
        self,
        session_id: str,
        *,
        org_id: str,
        source_user_id: str,
        provider_id: str,
        connector_id: str,
        user_agent: str = "",
    ) -> tuple[SSPRVerificationSession, str] | None:
        session = self.validate_session(
            session_id,
            org_id=org_id,
            source_user_id=source_user_id,
            provider_id=provider_id,
            connector_id=connector_id,
            user_agent=user_agent,
        )
        if session is None:
            return None
        now = _as_utc(self.now_factory())
        claim_token = secrets.token_urlsafe(24)
        with self.db.transaction() as connection:
            updated = connection.execute(
                """
                UPDATE sspr_verification_sessions
                SET claimed_at = ?, claim_token_hash = ?, updated_at = ?
                WHERE token_hash = ?
                  AND consumed_at IS NULL
                  AND revoked_at IS NULL
                  AND expires_at > ?
                  AND claimed_at IS NULL
                """,
                (
                    _iso(now),
                    hash_capability(claim_token),
                    _iso(now),
                    hash_capability(session_id),
                    _iso(now),
                ),
            )
        if updated.rowcount != 1:
            return None
        return session, claim_token

    def release_claim(self, session_id: str, claim_token: str) -> bool:
        now = _as_utc(self.now_factory())
        with self.db.transaction() as connection:
            updated = connection.execute(
                """
                UPDATE sspr_verification_sessions
                SET claimed_at = NULL, claim_token_hash = NULL, updated_at = ?
                WHERE token_hash = ?
                  AND claim_token_hash = ?
                  AND consumed_at IS NULL
                  AND revoked_at IS NULL
                """,
                (_iso(now), hash_capability(session_id), hash_capability(claim_token)),
            )
            return updated.rowcount == 1

    def consume_claim(self, session_id: str, claim_token: str) -> bool:
        now = _as_utc(self.now_factory())
        with self.db.transaction() as connection:
            updated = connection.execute(
                """
                UPDATE sspr_verification_sessions
                SET consumed_at = ?, claimed_at = NULL, claim_token_hash = NULL, updated_at = ?
                WHERE token_hash = ?
                  AND claim_token_hash = ?
                  AND consumed_at IS NULL
                  AND revoked_at IS NULL
                  AND expires_at > ?
                """,
                (
                    _iso(now),
                    _iso(now),
                    hash_capability(session_id),
                    hash_capability(claim_token),
                    _iso(now),
                ),
            )
            return updated.rowcount == 1

    def invalidate(self, session_id: str) -> None:
        now = _as_utc(self.now_factory())
        with self.db.transaction() as connection:
            connection.execute(
                """
                UPDATE sspr_verification_sessions
                SET revoked_at = COALESCE(revoked_at, ?),
                    claimed_at = NULL,
                    claim_token_hash = NULL,
                    updated_at = ?
                WHERE token_hash = ? AND consumed_at IS NULL
                """,
                (_iso(now), _iso(now), hash_capability(session_id)),
            )

    revoke = invalidate

    def cleanup_expired(self, *, retention_seconds: int = 7 * 86400) -> int:
        cutoff = _as_utc(self.now_factory()) - timedelta(seconds=max(int(retention_seconds or 0), 0))
        with self.db.transaction() as connection:
            result = connection.execute(
                """
                DELETE FROM sspr_verification_sessions
                WHERE expires_at < ?
                  AND COALESCE(consumed_at, revoked_at, expires_at) < ?
                """,
                (_iso(cutoff), _iso(cutoff)),
            )
            return int(result.rowcount)


class SQLiteSSPRResetReceiptStore:
    def __init__(
        self,
        db: DatabaseManager,
        *,
        now_factory: Callable[[], datetime] | None = None,
        token_factory: Callable[[], str] | None = None,
    ) -> None:
        self.db = db
        self.now_factory = now_factory or _utcnow
        self.token_factory = token_factory or (lambda: secrets.token_urlsafe(24))

    def create_receipt(
        self,
        *,
        org_id: str,
        ad_username: str,
        unlock_requested: bool,
        unlock_succeeded: bool,
        ttl_seconds: int = 300,
    ) -> SSPRResetReceipt:
        now = _as_utc(self.now_factory())
        expires_at = now + timedelta(seconds=max(int(ttl_seconds or 1), 1))
        token = self.token_factory()
        with self.db.transaction() as connection:
            connection.execute(
                """
                INSERT INTO sspr_reset_receipts (
                  token_hash, org_id, ad_username, completed_at, expires_at,
                  unlock_requested, unlock_succeeded, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    hash_capability(token),
                    _normalize_org_id(org_id),
                    str(ad_username or "").strip(),
                    _iso(now),
                    _iso(expires_at),
                    1 if unlock_requested else 0,
                    1 if unlock_succeeded else 0,
                    _iso(now),
                    _iso(now),
                ),
            )
        self.cleanup_expired()
        return SSPRResetReceipt(
            token=token,
            org_id=_normalize_org_id(org_id),
            ad_username=str(ad_username or "").strip(),
            completed_at=now,
            expires_at=expires_at,
            unlock_requested=bool(unlock_requested),
            unlock_succeeded=bool(unlock_succeeded),
        )

    def consume_receipt(self, token: str) -> SSPRResetReceipt | None:
        now = _as_utc(self.now_factory())
        token_hash = hash_capability(token)
        with self.db.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM sspr_reset_receipts WHERE token_hash = ? LIMIT 1",
                (token_hash,),
            ).fetchone()
            if not row or row["consumed_at"] or str(row["expires_at"] or "") <= _iso(now):
                return None
            updated = connection.execute(
                """
                UPDATE sspr_reset_receipts
                SET consumed_at = ?, updated_at = ?
                WHERE token_hash = ? AND consumed_at IS NULL AND expires_at > ?
                """,
                (_iso(now), _iso(now), token_hash, _iso(now)),
            )
            if updated.rowcount != 1:
                return None
            return SSPRResetReceipt(
                token=str(token or ""),
                org_id=str(row["org_id"] or "default"),
                ad_username=str(row["ad_username"] or ""),
                completed_at=_parse_datetime(row["completed_at"]) or now,
                expires_at=_parse_datetime(row["expires_at"]) or now,
                unlock_requested=bool(row["unlock_requested"]),
                unlock_succeeded=bool(row["unlock_succeeded"]),
                consumed_at=now,
            )

    def get_receipt(self, token: str) -> SSPRResetReceipt | None:
        """Read a valid receipt without changing database state.

        The web result page clears its HttpOnly capability cookie after this read,
        so refreshing the GET cannot replay the result while GET remains read-only.
        """
        now = _as_utc(self.now_factory())
        token_hash = hash_capability(token)
        with self.db.connection() as connection:
            row = connection.execute(
                "SELECT * FROM sspr_reset_receipts WHERE token_hash = ? LIMIT 1",
                (token_hash,),
            ).fetchone()
        if not row or row["consumed_at"] or str(row["expires_at"] or "") <= _iso(now):
            return None
        return SSPRResetReceipt(
            token=str(token or ""),
            org_id=str(row["org_id"] or "default"),
            ad_username=str(row["ad_username"] or ""),
            completed_at=_parse_datetime(row["completed_at"]) or now,
            expires_at=_parse_datetime(row["expires_at"]) or now,
            unlock_requested=bool(row["unlock_requested"]),
            unlock_succeeded=bool(row["unlock_succeeded"]),
        )

    def cleanup_expired(self, *, retention_seconds: int = 86400) -> int:
        cutoff = _as_utc(self.now_factory()) - timedelta(seconds=max(int(retention_seconds or 0), 0))
        with self.db.transaction() as connection:
            result = connection.execute(
                "DELETE FROM sspr_reset_receipts WHERE expires_at < ?",
                (_iso(cutoff),),
            )
            return int(result.rowcount)


class SQLiteSSPRRateLimitStore:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def get_bucket(self, bucket_hash: str) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                "SELECT * FROM sspr_rate_limit_buckets WHERE bucket_hash = ? LIMIT 1",
                (str(bucket_hash or ""),),
            ).fetchone()
        return dict(row) if row else None

    def save_bucket(
        self,
        bucket_hash: str,
        *,
        attempts: int,
        window_started_at: datetime,
        locked_until: datetime | None,
        updated_at: datetime,
    ) -> None:
        with self.db.transaction() as connection:
            connection.execute(
                """
                INSERT INTO sspr_rate_limit_buckets (
                  bucket_hash, attempts, window_started_at, locked_until, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(bucket_hash) DO UPDATE SET
                  attempts = excluded.attempts,
                  window_started_at = excluded.window_started_at,
                  locked_until = excluded.locked_until,
                  updated_at = excluded.updated_at
                """,
                (
                    str(bucket_hash or ""),
                    max(int(attempts or 0), 0),
                    _iso(window_started_at),
                    _iso(locked_until) if locked_until else None,
                    _iso(updated_at),
                ),
            )

    def delete_bucket(self, bucket_hash: str) -> None:
        with self.db.transaction() as connection:
            connection.execute(
                "DELETE FROM sspr_rate_limit_buckets WHERE bucket_hash = ?",
                (str(bucket_hash or ""),),
            )

    def evaluate(
        self,
        bucket_hash: str,
        *,
        now: datetime,
        max_attempts: int,
        window_seconds: int,
        lockout_seconds: int,
        record_failure: bool,
    ) -> tuple[bool, int]:
        normalized_hash = str(bucket_hash or "")
        current = _as_utc(now)
        window = timedelta(seconds=max(int(window_seconds or 1), 1))
        lockout = timedelta(seconds=max(int(lockout_seconds or 1), 1))
        with self.db.transaction() as connection:
            connection.execute(
                "DELETE FROM sspr_rate_limit_buckets WHERE updated_at < ?",
                (_iso(current - timedelta(days=7)),),
            )
            row = connection.execute(
                "SELECT * FROM sspr_rate_limit_buckets WHERE bucket_hash = ? LIMIT 1",
                (normalized_hash,),
            ).fetchone()
            attempts = int(row["attempts"] or 0) if row else 0
            window_started = _parse_datetime(row["window_started_at"]) if row else None
            locked_until = _parse_datetime(row["locked_until"]) if row else None
            if locked_until and locked_until > current:
                return True, max(int((locked_until - current).total_seconds()), 1)
            if not window_started or current - window_started >= window:
                attempts = 0
                window_started = current
            if not record_failure:
                if row and locked_until:
                    connection.execute(
                        """
                        UPDATE sspr_rate_limit_buckets
                        SET locked_until = NULL, attempts = ?, window_started_at = ?, updated_at = ?
                        WHERE bucket_hash = ?
                        """,
                        (attempts, _iso(window_started), _iso(current), normalized_hash),
                    )
                return False, 0
            attempts += 1
            locked_until = current + lockout if attempts >= max(int(max_attempts or 1), 1) else None
            connection.execute(
                """
                INSERT INTO sspr_rate_limit_buckets (
                  bucket_hash, attempts, window_started_at, locked_until, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(bucket_hash) DO UPDATE SET
                  attempts = excluded.attempts,
                  window_started_at = excluded.window_started_at,
                  locked_until = excluded.locked_until,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_hash,
                    attempts,
                    _iso(window_started),
                    _iso(locked_until) if locked_until else None,
                    _iso(current),
                ),
            )
            if locked_until:
                return True, max(int((locked_until - current).total_seconds()), 1)
            return False, 0

    def cleanup(self, *, older_than: datetime) -> int:
        with self.db.transaction() as connection:
            result = connection.execute(
                "DELETE FROM sspr_rate_limit_buckets WHERE updated_at < ?",
                (_iso(older_than),),
            )
            return int(result.rowcount)


__all__ = [
    "SQLiteSSPROAuthTransactionStore",
    "SQLiteSSPRRateLimitStore",
    "SQLiteSSPRResetReceiptStore",
    "SQLiteSSPRSessionStore",
    "hash_capability",
    "hash_user_agent",
]
