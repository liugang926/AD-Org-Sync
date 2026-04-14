from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, Optional

from sync_app.core.models import ExclusionRuleRecord
from sync_app.storage.local_db import BaseRepository, utcnow_iso
from sync_app.storage.schema import DEFAULT_HARD_PROTECTED_GROUPS, DEFAULT_SOFT_EXCLUDED_GROUPS


class GroupExclusionRuleRepository(BaseRepository):
    def seed_defaults(self):
        try:
            org_rows = self._fetchall("SELECT org_id FROM organizations ORDER BY org_id ASC")
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc).lower():
                raise
            org_rows = []
        org_ids = {str(row["org_id"] or "").strip().lower() or "default" for row in org_rows}
        org_ids.add("default")
        for org_id in sorted(org_ids):
            self.ensure_defaults_for_org(org_id)

    def ensure_defaults_for_org(self, org_id: Optional[str] = None) -> None:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        now = utcnow_iso()
        with self.db.transaction() as conn:
            for group_name in DEFAULT_HARD_PROTECTED_GROUPS:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO group_exclusion_rules (
                      org_id, rule_type, protection_level, match_type, match_value, display_name,
                      is_enabled, source, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_org_id,
                        "protect",
                        "hard",
                        "samaccountname",
                        group_name,
                        group_name,
                        1,
                        "system_seed",
                        now,
                        now,
                    ),
                )

            for group_name in DEFAULT_SOFT_EXCLUDED_GROUPS:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO group_exclusion_rules (
                      org_id, rule_type, protection_level, match_type, match_value, display_name,
                      is_enabled, source, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_org_id,
                        "exclude",
                        "soft",
                        "samaccountname",
                        group_name,
                        group_name,
                        1,
                        "system_seed",
                        now,
                        now,
                    ),
                )

    def list_enabled_rules(self, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            return self._fetchall(
                """
                SELECT * FROM group_exclusion_rules
                WHERE org_id = ?
                  AND is_enabled = 1
                ORDER BY protection_level DESC, source, display_name
                """,
                (normalized_org_id,),
            )
        return self._fetchall(
            """
            SELECT * FROM group_exclusion_rules
            WHERE is_enabled = 1
            ORDER BY org_id ASC, protection_level DESC, source, display_name
            """
        )

    def list_enabled_rule_records(self, *, org_id: Optional[str] = None) -> list[ExclusionRuleRecord]:
        return [ExclusionRuleRecord.from_row(row) for row in self.list_enabled_rules(org_id=org_id)]

    def list_rules(
        self,
        *,
        rule_type: Optional[str] = None,
        protection_level: Optional[str] = None,
        org_id: Optional[str] = None,
    ):
        clauses = []
        params: list[Any] = []
        normalized_org_id = self._resolve_org_id(org_id)
        if normalized_org_id:
            clauses.append("org_id = ?")
            params.append(normalized_org_id)
        if rule_type:
            clauses.append("rule_type = ?")
            params.append(rule_type)
        if protection_level:
            clauses.append("protection_level = ?")
            params.append(protection_level)

        sql = "SELECT * FROM group_exclusion_rules"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY is_enabled DESC, protection_level DESC, source, display_name"
        return self._fetchall(sql, tuple(params))

    def list_rule_records(
        self,
        *,
        rule_type: Optional[str] = None,
        protection_level: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> list[ExclusionRuleRecord]:
        return [
            ExclusionRuleRecord.from_row(row)
            for row in self.list_rules(
                rule_type=rule_type,
                protection_level=protection_level,
                org_id=org_id,
            )
        ]

    def list_soft_excluded_group_names(self, *, enabled_only: bool = True, org_id: Optional[str] = None) -> list[str]:
        normalized_org_id = self._resolve_org_id(org_id)
        sql = """
            SELECT match_value
            FROM group_exclusion_rules
            WHERE rule_type = 'exclude'
              AND protection_level = 'soft'
              AND match_type = 'samaccountname'
        """
        params: list[Any] = []
        if normalized_org_id:
            sql += " AND org_id = ?"
            params.append(normalized_org_id)
        if enabled_only:
            sql += " AND is_enabled = 1"
        sql += " ORDER BY LOWER(match_value)"
        rows = self._fetchall(sql, tuple(params))
        return [row["match_value"] for row in rows]

    def list_soft_excluded_rules(self, *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id)
        sql = """
            SELECT *
            FROM group_exclusion_rules
            WHERE rule_type = 'exclude'
              AND protection_level = 'soft'
              AND match_type = 'samaccountname'
        """
        params: list[Any] = []
        if normalized_org_id:
            sql += " AND org_id = ?"
            params.append(normalized_org_id)
        sql += " ORDER BY is_enabled DESC, LOWER(match_value), source"
        rows = self._fetchall(sql, tuple(params))
        return [dict(row) for row in rows]

    def replace_soft_excluded_rules(self, rules: Iterable[Dict[str, Any]], *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        self.ensure_defaults_for_org(normalized_org_id)
        normalized_rules = []
        seen = set()
        for rule in rules:
            match_value = str(rule.get("match_value", "")).strip()
            if not match_value:
                continue
            lowered = match_value.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            normalized_rules.append(
                {
                    "match_value": match_value,
                    "display_name": str(rule.get("display_name") or match_value).strip() or match_value,
                    "is_enabled": 1 if rule.get("is_enabled", True) else 0,
                    "source": str(rule.get("source") or "user_ui").strip() or "user_ui",
                }
            )

        existing_rules = self._fetchall(
            """
            SELECT *
            FROM group_exclusion_rules
            WHERE org_id = ?
              AND rule_type = 'exclude'
              AND protection_level = 'soft'
              AND match_type = 'samaccountname'
            """,
            (normalized_org_id,),
        )
        existing_by_lower = {row["match_value"].lower(): row for row in existing_rules}
        now = utcnow_iso()

        with self.db.transaction() as conn:
            for rule in normalized_rules:
                lowered = rule["match_value"].lower()
                existing = existing_by_lower.get(lowered)
                if existing:
                    conn.execute(
                        """
                        UPDATE group_exclusion_rules
                        SET display_name = ?,
                            is_enabled = ?,
                            source = ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            rule["display_name"],
                            rule["is_enabled"],
                            rule["source"],
                            now,
                            existing["id"],
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO group_exclusion_rules (
                          org_id, rule_type, protection_level, match_type, match_value, display_name,
                          is_enabled, source, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            normalized_org_id,
                            "exclude",
                            "soft",
                            "samaccountname",
                            rule["match_value"],
                            rule["display_name"],
                            rule["is_enabled"],
                            rule["source"],
                            now,
                            now,
                        ),
                    )

            desired_set = {rule["match_value"].lower() for rule in normalized_rules}
            for lowered, existing in existing_by_lower.items():
                if lowered in desired_set:
                    continue
                conn.execute(
                    """
                    UPDATE group_exclusion_rules
                    SET is_enabled = 0,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, existing["id"]),
                )

    def sync_soft_excluded_groups(self, group_names: Iterable[str], *, org_id: Optional[str] = None):
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        self.ensure_defaults_for_org(normalized_org_id)
        normalized_names = []
        seen = set()
        for group_name in group_names:
            if group_name is None:
                continue
            normalized = str(group_name).strip()
            if not normalized:
                continue
            lowered = normalized.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            normalized_names.append(normalized)

        existing_rules = self._fetchall(
            """
            SELECT *
            FROM group_exclusion_rules
            WHERE org_id = ?
              AND rule_type = 'exclude'
              AND protection_level = 'soft'
              AND match_type = 'samaccountname'
            """,
            (normalized_org_id,),
        )
        existing_by_lower = {row["match_value"].lower(): row for row in existing_rules}
        now = utcnow_iso()

        with self.db.transaction() as conn:
            for group_name in normalized_names:
                lowered = group_name.lower()
                existing = existing_by_lower.get(lowered)
                if existing:
                    conn.execute(
                        """
                        UPDATE group_exclusion_rules
                        SET display_name = ?,
                            is_enabled = 1,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (group_name, now, existing["id"]),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO group_exclusion_rules (
                          org_id, rule_type, protection_level, match_type, match_value, display_name,
                          is_enabled, source, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            normalized_org_id,
                            "exclude",
                            "soft",
                            "samaccountname",
                            group_name,
                            group_name,
                            1,
                            "user_ui",
                            now,
                            now,
                        ),
                    )

            desired_set = {name.lower() for name in normalized_names}
            for lowered, existing in existing_by_lower.items():
                if lowered in desired_set:
                    continue
                conn.execute(
                    """
                    UPDATE group_exclusion_rules
                    SET is_enabled = 0,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, existing["id"]),
                )

    def upsert_rule(
        self,
        *,
        rule_type: str,
        protection_level: str,
        match_type: str,
        match_value: str,
        display_name: str = "",
        is_enabled: bool = True,
        source: str = "import",
        org_id: Optional[str] = None,
    ) -> None:
        normalized_org_id = self._resolve_org_id(org_id, default="default") or "default"
        normalized_rule_type = str(rule_type or "").strip().lower()
        normalized_level = str(protection_level or "").strip().lower()
        normalized_match_type = str(match_type or "").strip().lower()
        normalized_match_value = str(match_value or "").strip()
        normalized_display_name = str(display_name or normalized_match_value).strip() or normalized_match_value
        normalized_source = str(source or "import").strip() or "import"
        if not normalized_rule_type or not normalized_level or not normalized_match_type or not normalized_match_value:
            raise ValueError("group exclusion rule fields are required")

        now = utcnow_iso()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO group_exclusion_rules (
                  org_id, rule_type, protection_level, match_type, match_value, display_name,
                  is_enabled, source, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(org_id, rule_type, protection_level, match_type, match_value) DO UPDATE SET
                  display_name = excluded.display_name,
                  is_enabled = excluded.is_enabled,
                  source = excluded.source,
                  updated_at = excluded.updated_at
                """,
                (
                    normalized_org_id,
                    normalized_rule_type,
                    normalized_level,
                    normalized_match_type,
                    normalized_match_value,
                    normalized_display_name,
                    1 if is_enabled else 0,
                    normalized_source,
                    now,
                    now,
                ),
            )

    def evaluate_group(
        self,
        *,
        group_sam: Optional[str] = None,
        group_dn: Optional[str] = None,
        display_name: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        matched_rules = []
        for row in self.list_enabled_rules(org_id=org_id):
            match_type = row["match_type"]
            match_value = row["match_value"]
            is_match = False

            if match_type == "samaccountname" and group_sam:
                is_match = group_sam.lower() == match_value.lower()
            elif match_type == "dn" and group_dn:
                is_match = group_dn.lower() == match_value.lower()
            elif match_type == "display_name" and display_name:
                is_match = display_name.lower() == match_value.lower()

            if is_match:
                matched_rules.append(dict(row))

        is_hard_protected = any(
            rule["rule_type"] == "protect" and rule["protection_level"] == "hard"
            for rule in matched_rules
        )
        is_excluded = any(rule["rule_type"] == "exclude" for rule in matched_rules) or is_hard_protected
        return {
            "is_hard_protected": is_hard_protected,
            "is_excluded": is_excluded,
            "matched_rules": matched_rules,
        }

    def delete_rules_for_org(self, org_id: str) -> None:
        normalized_org_id = self._resolve_org_id(org_id, default="default")
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM group_exclusion_rules WHERE org_id = ?", (normalized_org_id,))
