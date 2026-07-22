from __future__ import annotations

import csv
import io
from collections import defaultdict
from typing import Any

from sync_app.core.fingerprints import fingerprint_json


TAKEOVER_HEADER_ALIASES = {
    "provider_id": ("provider_id", "source_provider", "provider"),
    "connector_id": ("connector_id", "connector"),
    "platform_account_id": (
        "platform_account_id",
        "source_user_id",
        "platform_user_id",
    ),
    "ad_account_key": (
        "ad_account_key",
        "object_guid",
        "ad_username",
        "sam_account_name",
    ),
}


def _value(row: dict[str, Any], field_name: str) -> str:
    for alias in TAKEOVER_HEADER_ALIASES[field_name]:
        value = str(row.get(alias) or "").strip()
        if value:
            return value
    return ""


def parse_takeover_csv(text: str) -> list[dict[str, Any]]:
    normalized_text = str(text or "").lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(normalized_text))
    if not reader.fieldnames:
        raise ValueError("takeover CSV requires a header row")
    normalized_headers = {str(value or "").strip().lower() for value in reader.fieldnames}
    required_groups = ("provider_id", "platform_account_id", "ad_account_key")
    for field_name in required_groups:
        if not normalized_headers.intersection(TAKEOVER_HEADER_ALIASES[field_name]):
            raise ValueError(f"takeover CSV is missing the {field_name} column")
    rows: list[dict[str, Any]] = []
    for row_number, raw in enumerate(reader, start=2):
        normalized_raw = {
            str(key or "").strip().lower(): str(value or "").strip()
            for key, value in dict(raw or {}).items()
        }
        if not any(normalized_raw.values()):
            continue
        rows.append(
            {
                "row_number": row_number,
                "provider_id": _value(normalized_raw, "provider_id").lower(),
                "connector_id": _value(normalized_raw, "connector_id") or "default",
                "platform_account_id": _value(
                    normalized_raw, "platform_account_id"
                ),
                "ad_account_key": _value(normalized_raw, "ad_account_key"),
            }
        )
    if not rows:
        raise ValueError("takeover CSV contains no data rows")
    if len(rows) > 10000:
        raise ValueError("takeover CSV exceeds the 10000 row safety limit")
    return rows


class AccountTakeoverService:
    def __init__(
        self,
        *,
        takeover_repo: Any,
        platform_account_repo: Any,
        ad_account_repo: Any,
        identity_repo: Any,
    ) -> None:
        self.takeover_repo = takeover_repo
        self.platform_account_repo = platform_account_repo
        self.ad_account_repo = ad_account_repo
        self.identity_repo = identity_repo

    def preview(
        self,
        *,
        org_id: str,
        csv_text: str,
        original_filename: str = "takeover.csv",
        created_by: str,
    ) -> dict[str, Any]:
        parsed_rows = parse_takeover_csv(csv_text)
        source_targets: dict[tuple[str, str, str], set[str]] = defaultdict(set)
        target_sources: dict[tuple[str, str], set[tuple[str, str, str]]] = defaultdict(set)
        for item in parsed_rows:
            source_key = (
                item["provider_id"],
                item["connector_id"],
                item["platform_account_id"],
            )
            target_key = (item["connector_id"], item["ad_account_key"].casefold())
            source_targets[source_key].add(item["ad_account_key"].casefold())
            target_sources[target_key].add(source_key)

        links = self.identity_repo.list_links(org_id=org_id)
        platform_links = {
            int(item.platform_account_id): item
            for item in links
            if item.status == "active"
            and item.account_kind == "platform"
            and item.platform_account_id is not None
        }
        ad_links = {
            int(item.ad_account_id): item
            for item in links
            if item.status == "active"
            and item.account_kind == "ad"
            and item.ad_account_id is not None
        }
        primary_ad_by_identity = {
            item.identity_id: int(item.ad_account_id)
            for item in links
            if item.status == "active"
            and item.account_kind == "ad"
            and item.account_role == "primary_ad"
            and item.ad_account_id is not None
        }

        validated_rows: list[dict[str, Any]] = []
        for item in parsed_rows:
            conflicts: list[str] = []
            if not item["provider_id"]:
                conflicts.append("provider_required")
            if not item["platform_account_id"]:
                conflicts.append("platform_account_id_required")
            if not item["ad_account_key"]:
                conflicts.append("ad_account_key_required")
            source_key = (
                item["provider_id"],
                item["connector_id"],
                item["platform_account_id"],
            )
            target_key = (item["connector_id"], item["ad_account_key"].casefold())
            if len(source_targets[source_key]) > 1:
                conflicts.append("one_person_multiple_primary_ad_accounts")
            if len(target_sources[target_key]) > 1:
                conflicts.append("one_ad_account_multiple_people")

            platform = self.platform_account_repo.get_account_by_stable_id(
                org_id=org_id,
                provider_id=item["provider_id"],
                connector_id=item["connector_id"],
                platform_account_id=item["platform_account_id"],
            )
            try:
                ad_account = self.ad_account_repo.get_account_by_stable_key(
                    org_id=org_id,
                    connector_id=item["connector_id"],
                    account_key=item["ad_account_key"],
                )
            except ValueError:
                ad_account = None
                conflicts.append("ad_account_key_ambiguous")
            if platform is None:
                conflicts.append("platform_account_not_found")
            if ad_account is None and "ad_account_key_ambiguous" not in conflicts:
                conflicts.append("ad_account_not_found")
            if platform is not None and (
                platform.is_excluded
                or platform.account_type in {"service", "shared", "test"}
            ):
                conflicts.append("platform_account_not_person")
            if ad_account is not None and (
                ad_account.is_protected
                or ad_account.account_type in {"service", "shared", "test"}
            ):
                conflicts.append("ad_account_protected_or_not_person")

            existing_identity_id = ""
            proposed_action = "create_permanent_link"
            if platform is not None and ad_account is not None:
                platform_link = platform_links.get(int(platform.id or 0))
                ad_link = ad_links.get(int(ad_account.id or 0))
                identity_ids = {
                    link.identity_id for link in (platform_link, ad_link) if link is not None
                }
                if len(identity_ids) > 1:
                    conflicts.append("active_relationship_ownership_conflict")
                    proposed_action = "would_overwrite"
                elif identity_ids:
                    existing_identity_id = next(iter(identity_ids))
                    current_primary_ad = primary_ad_by_identity.get(existing_identity_id)
                    if current_primary_ad not in (None, int(ad_account.id or 0)):
                        conflicts.append("identity_already_has_another_primary_ad")
                        proposed_action = "would_overwrite"
                    elif platform_link and ad_link:
                        proposed_action = "no_change"
                    else:
                        proposed_action = "complete_permanent_link"
                legacy = self.takeover_repo.get_legacy_binding(
                    org_id=org_id,
                    provider_id=item["provider_id"],
                    connector_id=item["connector_id"],
                    platform_account_id=item["platform_account_id"],
                )
                if legacy and str(legacy.get("ad_username") or "").casefold() != str(
                    ad_account.sam_account_name or ""
                ).casefold():
                    conflicts.append("existing_legacy_binding_would_be_overwritten")
                    proposed_action = "would_overwrite"

            conflict_codes = sorted(set(conflicts))
            normalized_payload = {
                "platform_db_id": int(platform.id or 0) if platform else None,
                "ad_db_id": int(ad_account.id or 0) if ad_account else None,
                "platform_stable_id": platform.platform_account_id if platform else "",
                "ad_object_guid": ad_account.object_guid if ad_account else "",
                "ad_object_sid": ad_account.object_sid if ad_account else "",
                "ad_sam_account_name": ad_account.sam_account_name if ad_account else "",
                "source_employee_id": platform.employee_id if platform else "",
                "ad_employee_id": ad_account.employee_id if ad_account else "",
            }
            validated_rows.append(
                {
                    **item,
                    "validation_status": "conflict" if conflict_codes else "valid",
                    "proposed_action": proposed_action,
                    "existing_identity_id": existing_identity_id,
                    "conflict_codes": conflict_codes,
                    "normalized_payload": normalized_payload,
                }
            )

        preview_fingerprint = fingerprint_json(
            validated_rows,
            namespace="account-takeover-preview",
        )
        batch = self.takeover_repo.save_preview(
            org_id=org_id,
            original_filename=original_filename,
            file_fingerprint=fingerprint_json(
                {"content": str(csv_text or "")},
                namespace="account-takeover-file",
            ),
            preview_fingerprint=preview_fingerprint,
            rows=validated_rows,
            created_by=created_by,
        )
        return {"batch": batch, "rows": validated_rows}

    def approve(
        self,
        *,
        org_id: str,
        batch_id: str,
        preview_fingerprint: str,
        approved_by: str,
    ) -> dict[str, Any]:
        return self.takeover_repo.approve(
            batch_id,
            org_id=org_id,
            expected_preview_fingerprint=preview_fingerprint,
            approved_by=approved_by,
        )

    def apply(
        self,
        *,
        org_id: str,
        batch_id: str,
        preview_fingerprint: str,
        applied_by: str,
    ) -> dict[str, Any]:
        return self.takeover_repo.apply(
            batch_id,
            org_id=org_id,
            expected_preview_fingerprint=preview_fingerprint,
            applied_by=applied_by,
        )


__all__ = ["AccountTakeoverService", "parse_takeover_csv"]
