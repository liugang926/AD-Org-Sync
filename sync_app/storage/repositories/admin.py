from __future__ import annotations

from typing import Optional

from sync_app.core.models import WebAdminUserRecord
from sync_app.storage.local_db import BaseRepository, utcnow_iso


class WebAdminUserRepository(BaseRepository):
    def has_any_user(self) -> bool:
        row = self._fetchone("SELECT COUNT(*) AS total FROM web_admin_users")
        return bool(row and int(row["total"]) > 0)

    def count_users(self) -> int:
        row = self._fetchone("SELECT COUNT(*) AS total FROM web_admin_users")
        return int(row["total"]) if row else 0

    def get_by_username(self, username: str):
        return self._fetchone(
            """
            SELECT * FROM web_admin_users
            WHERE LOWER(username) = LOWER(?)
            LIMIT 1
            """,
            (username,),
        )

    def get_user_record_by_username(self, username: str) -> Optional[WebAdminUserRecord]:
        row = self.get_by_username(username)
        if not row:
            return None
        return WebAdminUserRecord.from_row(row)

    def get_by_id(self, user_id: int):
        return self._fetchone(
            """
            SELECT * FROM web_admin_users
            WHERE id = ?
            LIMIT 1
            """,
            (int(user_id),),
        )

    def get_user_record_by_id(self, user_id: int) -> Optional[WebAdminUserRecord]:
        row = self.get_by_id(user_id)
        if not row:
            return None
        return WebAdminUserRecord.from_row(row)

    def list_user_records(self) -> list[WebAdminUserRecord]:
        rows = self._fetchall(
            """
            SELECT * FROM web_admin_users
            ORDER BY is_enabled DESC, created_at ASC, username ASC
            """
        )
        return [WebAdminUserRecord.from_row(row) for row in rows]

    def create_user(
        self,
        username: str,
        password_hash: str,
        *,
        role: str = "super_admin",
        is_enabled: bool = True,
        must_change_password: bool = False,
    ) -> int:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO web_admin_users (
                  username, password_hash, role, is_enabled, must_change_password,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    username.strip(),
                    password_hash,
                    role,
                    1 if is_enabled else 0,
                    1 if must_change_password else 0,
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def update_last_login(self, username: str) -> None:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE web_admin_users
                SET last_login_at = ?, updated_at = ?
                WHERE LOWER(username) = LOWER(?)
                """,
                (now, now, username),
            )

    def set_password(
        self,
        username: str,
        password_hash: str,
        *,
        must_change_password: bool = False,
    ) -> None:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE web_admin_users
                SET password_hash = ?,
                    must_change_password = ?,
                    updated_at = ?
                WHERE LOWER(username) = LOWER(?)
                """,
                (password_hash, 1 if must_change_password else 0, now, username),
            )

    def set_enabled(self, user_id: int, enabled: bool) -> None:
        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE web_admin_users
                SET is_enabled = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (1 if enabled else 0, now, int(user_id)),
            )
