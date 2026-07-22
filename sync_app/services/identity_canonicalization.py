from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable

from sync_app.core.fingerprints import fingerprint_json
from sync_app.core.models import (
    EnterpriseIdentityRecord,
    FieldAuthorityRuleRecord,
    PlatformAccountRecord,
)


CANONICAL_IDENTITY_FIELDS = (
    "employee_id",
    "display_name",
    "email",
    "mobile",
    "primary_department_id",
    "manager_account_id",
    "account_status",
)


def _comparable_value(field_name: str, value: Any) -> str:
    normalized = str(value or "").strip()
    if field_name == "mobile":
        digits = re.sub(r"\D", "", normalized)
        if digits.startswith("0086") and len(digits) > 11:
            digits = digits[4:]
        elif digits.startswith("86") and len(digits) > 11:
            digits = digits[2:]
        return digits
    return normalized.casefold()


@dataclass(slots=True)
class CanonicalIdentityPreview:
    identity_id: str
    identity_revision: int
    selected_fields: dict[str, Any] = field(default_factory=dict)
    field_sources: dict[str, Any] = field(default_factory=dict)
    conflicts: dict[str, Any] = field(default_factory=dict)
    missing_fields: list[str] = field(default_factory=list)
    account_ids: list[int] = field(default_factory=list)
    preview_fingerprint: str = ""

    @property
    def requires_confirmation(self) -> bool:
        return bool(self.conflicts)


def build_canonical_identity_preview(
    identity: EnterpriseIdentityRecord,
    platform_accounts: Iterable[PlatformAccountRecord],
    rules: Iterable[FieldAuthorityRuleRecord],
) -> CanonicalIdentityPreview:
    accounts = [item for item in platform_accounts if item.id is not None]
    enabled_rules = [item for item in rules if item.is_enabled]
    rules_by_field: dict[str, list[FieldAuthorityRuleRecord]] = defaultdict(list)
    for rule in enabled_rules:
        rules_by_field[rule.field_name].append(rule)
    for field_rules in rules_by_field.values():
        field_rules.sort(key=lambda item: (item.source_priority, item.id or 0))

    selected_fields: dict[str, Any] = {}
    field_sources: dict[str, Any] = {}
    conflicts: dict[str, Any] = {}
    missing_fields: list[str] = []
    for field_name in CANONICAL_IDENTITY_FIELDS:
        candidates: list[dict[str, Any]] = []
        for account in accounts:
            value = getattr(account, field_name, account.custom_fields.get(field_name, ""))
            if value in (None, ""):
                continue
            matching_rule = next(
                (
                    rule
                    for rule in rules_by_field.get(field_name, [])
                    if rule.source_provider == account.provider_id
                ),
                next(
                    (
                        rule
                        for rule in rules_by_field.get(field_name, [])
                        if rule.source_provider == "*"
                    ),
                    None,
                ),
            )
            priority = matching_rule.source_priority if matching_rule else 1000
            candidates.append(
                {
                    "value": value,
                    "comparable_value": _comparable_value(field_name, value),
                    "provider_id": account.provider_id,
                    "connector_id": account.connector_id,
                    "platform_account_id": account.platform_account_id,
                    "account_id": int(account.id or 0),
                    "priority": priority,
                    "rule_id": matching_rule.id if matching_rule else None,
                    "sync_direction": (
                        matching_rule.sync_direction
                        if matching_rule
                        else "compare_only"
                    ),
                    "prevent_loop": (
                        matching_rule.prevent_loop if matching_rule else True
                    ),
                }
            )
        if not candidates:
            missing_fields.append(field_name)
            continue
        candidates.sort(
            key=lambda item: (
                int(item["priority"]),
                str(item["provider_id"]),
                int(item["account_id"]),
            )
        )
        selected = candidates[0]
        selected_fields[field_name] = selected["value"]
        field_sources[field_name] = {
            key: selected[key]
            for key in (
                "provider_id",
                "connector_id",
                "platform_account_id",
                "account_id",
                "priority",
                "rule_id",
                "sync_direction",
                "prevent_loop",
            )
        }
        distinct_values = {
            item["comparable_value"] for item in candidates if item["comparable_value"]
        }
        if len(distinct_values) > 1:
            conflicts[field_name] = {
                "reason": "platform_authority_value_conflict",
                "selected": selected["value"],
                "selected_provider": selected["provider_id"],
                "alternatives": [
                    {
                        key: item[key]
                        for key in (
                            "value",
                            "provider_id",
                            "connector_id",
                            "platform_account_id",
                            "priority",
                        )
                    }
                    for item in candidates[1:]
                    if item["comparable_value"] != selected["comparable_value"]
                ],
            }

    fingerprint_payload = {
        "identity_id": identity.identity_id,
        "identity_revision": identity.identity_revision,
        "selected_fields": selected_fields,
        "field_sources": field_sources,
        "conflicts": conflicts,
        "account_ids": sorted(int(item.id or 0) for item in accounts),
        "rule_revisions": sorted(
            (int(rule.id or 0), rule.rule_revision) for rule in enabled_rules
        ),
    }
    return CanonicalIdentityPreview(
        identity_id=identity.identity_id,
        identity_revision=identity.identity_revision,
        selected_fields=selected_fields,
        field_sources=field_sources,
        conflicts=conflicts,
        missing_fields=missing_fields,
        account_ids=sorted(int(item.id or 0) for item in accounts),
        preview_fingerprint=fingerprint_json(
            fingerprint_payload,
            namespace="canonical-enterprise-identity-preview",
        ),
    )


class IdentityCanonicalizationService:
    def __init__(
        self,
        *,
        identity_repo: Any,
        platform_account_repo: Any,
        field_authority_repo: Any,
    ) -> None:
        self.identity_repo = identity_repo
        self.platform_account_repo = platform_account_repo
        self.field_authority_repo = field_authority_repo

    def preview(self, *, org_id: str, identity_id: str) -> CanonicalIdentityPreview:
        identity = self.identity_repo.get_identity(identity_id, org_id=org_id)
        if identity is None:
            raise ValueError("enterprise identity does not exist in this organization")
        self.field_authority_repo.seed_defaults(org_id=org_id)
        rules = self.field_authority_repo.list_enabled_rules(org_id=org_id)
        links = self.identity_repo.list_links(
            org_id=org_id,
            identity_id=identity_id,
        )
        linked_account_ids = {
            int(link.platform_account_id)
            for link in links
            if link.account_kind == "platform" and link.platform_account_id is not None
        }
        accounts = [
            account
            for account in self.platform_account_repo.list_accounts(org_id=org_id)
            if int(account.id or 0) in linked_account_ids
        ]
        return build_canonical_identity_preview(identity, accounts, rules)

    def apply_preview(
        self,
        *,
        org_id: str,
        identity_id: str,
        expected_preview_fingerprint: str,
        confirm_conflicts: bool = False,
    ) -> EnterpriseIdentityRecord:
        preview = self.preview(org_id=org_id, identity_id=identity_id)
        if preview.preview_fingerprint != str(expected_preview_fingerprint or ""):
            raise ValueError("canonical identity preview is stale")
        if preview.requires_confirmation and not confirm_conflicts:
            raise ValueError("canonical identity field conflicts require confirmation")
        fields = dict(preview.selected_fields)
        identity = self.identity_repo.update_canonical_fields(
            identity_id,
            org_id=org_id,
            expected_revision=preview.identity_revision,
            display_name=str(fields.get("display_name") or ""),
            canonical_employee_id=str(fields.get("employee_id") or ""),
            primary_department_id=str(fields.get("primary_department_id") or ""),
            canonical_fields=fields,
            field_sources=preview.field_sources,
        )
        return identity


__all__ = [
    "CANONICAL_IDENTITY_FIELDS",
    "CanonicalIdentityPreview",
    "IdentityCanonicalizationService",
    "build_canonical_identity_preview",
]
