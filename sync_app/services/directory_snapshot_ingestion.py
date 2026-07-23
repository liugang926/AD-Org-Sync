from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

from sync_app.core.fingerprints import fingerprint_json
from sync_app.services.ad_capabilities import build_ad_capability_report
from sync_app.storage.repositories.field_registry import ADTargetAttributeRegistryRepository


def _duplicate_value_count(values: Iterable[Any]) -> int:
    counts = Counter(
        str(value or "").strip().casefold()
        for value in values
        if str(value or "").strip()
    )
    return sum(count for count in counts.values() if count > 1)


def _source_account_type(raw_payload: dict[str, Any]) -> str:
    explicit = str(
        raw_payload.get("account_type")
        or raw_payload.get("accountType")
        or ""
    ).strip().lower()
    if explicit in {"person", "service", "shared", "test"}:
        return explicit
    flag_map = {
        "is_service_account": "service",
        "is_shared_account": "shared",
        "is_test_account": "test",
    }
    for key, account_type in flag_map.items():
        if bool(raw_payload.get(key)):
            return account_type
    return "person"


class PlatformAccountIngestionService:
    def __init__(self, *, source_directory_repo: Any, platform_account_repo: Any) -> None:
        self.source_directory_repo = source_directory_repo
        self.platform_account_repo = platform_account_repo

    def ingest_snapshot(
        self,
        *,
        org_id: str,
        provider_id: str,
        connector_id: str,
        snapshot_id: int,
    ) -> dict[str, Any]:
        snapshot = self.source_directory_repo.get_snapshot(snapshot_id, org_id=org_id)
        if not snapshot or str(snapshot["status"] or "") not in {"success", "succeeded"}:
            raise ValueError("source snapshot must exist and be successful before ingestion")
        if str(snapshot["provider_id"] or "") != str(provider_id or ""):
            raise ValueError("source snapshot provider does not match ingestion provider")
        offset = 0
        ingested = []
        while True:
            page = self.source_directory_repo.list_users(
                snapshot_id,
                org_id=org_id,
                provider_id=provider_id,
                limit=200,
                offset=offset,
            )
            rows = list(page.get("items") or [])
            for row in rows:
                raw_payload = dict(row.get("raw_payload") or {})
                account = self.platform_account_repo.upsert_account(
                    org_id=org_id,
                    provider_id=provider_id,
                    connector_id=connector_id,
                    platform_account_id=str(row.get("source_user_id") or ""),
                    display_name=str(row.get("display_name") or ""),
                    employee_id=str(row.get("employee_id") or ""),
                    email=str(row.get("email") or ""),
                    mobile=str(
                        raw_payload.get("mobile")
                        or raw_payload.get("phone")
                        or ""
                    ),
                    account_status=str(row.get("account_status") or "active"),
                    account_type=_source_account_type(raw_payload),
                    primary_department_id=str(row.get("primary_department_id") or ""),
                    department_ids=row.get("department_ids") or [],
                    manager_account_id=str(
                        raw_payload.get("manager_userid")
                        or raw_payload.get("managerUserId")
                        or raw_payload.get("leader_user_id")
                        or ""
                    ),
                    custom_fields=raw_payload,
                    source_snapshot_id=snapshot_id,
                    raw_payload=raw_payload,
                    is_excluded=bool(raw_payload.get("identity_matching_excluded", False)),
                )
                ingested.append(account)
            offset += len(rows)
            if not rows or offset >= int(page.get("total") or 0):
                break
        return {
            "snapshot_id": int(snapshot_id),
            "provider_id": provider_id,
            "connector_id": connector_id,
            "account_count": len(ingested),
            "missing_employee_id_count": sum(
                1 for item in ingested if not str(item.employee_id or "").strip()
            ),
            "duplicate_employee_id_count": _duplicate_value_count(
                item.employee_id for item in ingested
            ),
        }


class ADDirectorySnapshotService:
    def __init__(
        self,
        *,
        snapshot_repo: Any,
        ad_account_repo: Any,
        ad_target_attribute_repo: Any | None = None,
    ) -> None:
        self.snapshot_repo = snapshot_repo
        self.ad_account_repo = ad_account_repo
        self.ad_target_attribute_repo = ad_target_attribute_repo

    def refresh(
        self,
        *,
        org_id: str,
        connector_id: str,
        provider: Any,
        created_by: str = "",
        search_base: str = "",
        ou_search_base: str = "",
        capability_report: dict[str, Any] | None = None,
        directory_mode: str = "writable",
    ) -> dict[str, Any]:
        snapshot_id = self.snapshot_repo.start_snapshot(
            org_id=org_id,
            connector_id=connector_id,
            created_by=created_by,
        )
        try:
            directory_users = list(
                provider.list_directory_users(
                    search_base=search_base,
                    page_size=500,
                )
            )
            try:
                organizational_units = list(
                    provider.list_organizational_units(search_base=ou_search_base)
                )
            except TypeError as exc:
                if "search_base" not in str(exc):
                    raise
                organizational_units = list(provider.list_organizational_units())
            resolved_capability_report = capability_report or build_ad_capability_report(
                connected=True,
                user_read_succeeded=True,
                ou_read_succeeded=True,
                directory_mode=directory_mode,
                use_ssl=bool(getattr(getattr(provider, "client", provider), "use_ssl", True)),
                validate_cert=bool(
                    getattr(getattr(provider, "client", provider), "validate_cert", True)
                ),
            )
            normalized_rows: list[dict[str, Any]] = []
            for item in directory_users:
                row = dict(item or {})
                if not str(row.get("object_guid") or row.get("sam_account_name") or "").strip():
                    raise ValueError("AD snapshot contains an account without a stable or directory key")
                account = self.ad_account_repo.upsert_account(
                    org_id=org_id,
                    connector_id=connector_id,
                    latest_snapshot_id=snapshot_id,
                    **{
                        key: row.get(key)
                        for key in (
                            "object_guid",
                            "object_sid",
                            "distinguished_name",
                            "sam_account_name",
                            "user_principal_name",
                            "employee_id",
                            "employee_number",
                            "mail",
                            "telephone_number",
                            "mobile",
                            "display_name",
                            "account_enabled",
                            "manager_dn",
                            "group_membership",
                            "ou_path",
                            "extension_attributes",
                            "when_created",
                            "when_changed",
                            "account_type",
                            "is_protected",
                        )
                    },
                )
                normalized_rows.append(
                    {
                        "object_guid": account.object_guid,
                        "object_sid": account.object_sid,
                        "distinguished_name": account.distinguished_name,
                        "sam_account_name": account.sam_account_name,
                        "user_principal_name": account.user_principal_name,
                        "employee_id": account.employee_id,
                        "employee_number": account.employee_number,
                        "mail": account.mail,
                        "telephone_number": account.telephone_number,
                        "mobile": account.mobile,
                        "display_name": account.display_name,
                        "account_enabled": account.account_enabled,
                        "manager_dn": account.manager_dn,
                        "group_membership": account.group_membership,
                        "ou_path": account.ou_path,
                        "extension_attributes": account.extension_attributes,
                        "when_created": account.when_created,
                        "when_changed": account.when_changed,
                        "account_type": account.account_type,
                        "is_protected": account.is_protected,
                    }
                )
            self.snapshot_repo.replace_ous(
                snapshot_id,
                org_id=org_id,
                connector_id=connector_id,
                organizational_units=organizational_units,
            )
            snapshot_fingerprint = fingerprint_json(
                {
                    "org_id": org_id,
                    "connector_id": connector_id,
                    "users": sorted(
                        normalized_rows,
                        key=lambda item: (
                            str(item.get("object_guid") or ""),
                            str(item.get("sam_account_name") or "").casefold(),
                        ),
                    ),
                    "organizational_units": sorted(
                        [dict(item) for item in organizational_units],
                        key=lambda item: str(
                            item.get("guid") or item.get("dn") or ""
                        ).casefold(),
                    ),
                },
                namespace="ad-directory-snapshot",
            )
            duplicate_employee_ids = _duplicate_value_count(
                item.get("employee_id") for item in normalized_rows
            )
            duplicate_employee_numbers = _duplicate_value_count(
                item.get("employee_number") for item in normalized_rows
            )
            attribute_name_map = {
                "object_guid": "objectGUID",
                "object_sid": "objectSid",
                "distinguished_name": "distinguishedName",
                "sam_account_name": "sAMAccountName",
                "user_principal_name": "userPrincipalName",
                "employee_id": "employeeID",
                "employee_number": "employeeNumber",
                "mail": "mail",
                "telephone_number": "telephoneNumber",
                "mobile": "mobile",
                "display_name": "displayName",
                "manager_dn": "manager",
                "group_membership": "memberOf",
                "when_created": "whenCreated",
                "when_changed": "whenChanged",
            }
            discovered_attributes = {
                attribute_name_map[key]
                for row in normalized_rows
                for key in attribute_name_map
                if row.get(key) not in (None, "", [], {})
            }
            for row in normalized_rows:
                discovered_attributes.update(
                    str(key).strip()
                    for key in dict(row.get("extension_attributes") or {})
                    if str(key).strip()
                )
            attribute_repo = self.ad_target_attribute_repo
            if attribute_repo is None and getattr(self.snapshot_repo, "db", None) is not None:
                attribute_repo = ADTargetAttributeRegistryRepository(self.snapshot_repo.db)
            if attribute_repo is not None:
                attribute_repo.sync_snapshot_catalog(
                    org_id=org_id,
                    ad_connector_id=connector_id,
                    snapshot_id=snapshot_id,
                    capability_report=resolved_capability_report,
                    discovered_attributes=discovered_attributes,
                )
            self.snapshot_repo.complete_snapshot(
                snapshot_id,
                org_id=org_id,
                user_count=len(normalized_rows),
                ou_count=len(organizational_units),
                duplicate_employee_id_count=duplicate_employee_ids,
                duplicate_employee_number_count=duplicate_employee_numbers,
                snapshot_fingerprint=snapshot_fingerprint,
                capability_report=resolved_capability_report,
                metadata={
                    "user_search_base": search_base,
                    "ou_search_base": ou_search_base,
                },
            )
            snapshot = self.snapshot_repo.get_snapshot(snapshot_id, org_id=org_id)
            return dict(snapshot) if snapshot else {"id": snapshot_id, "status": "success"}
        except Exception as exc:
            self.snapshot_repo.fail_snapshot(
                snapshot_id,
                org_id=org_id,
                error_summary=f"{type(exc).__name__}: AD snapshot refresh failed",
            )
            raise


__all__ = [
    "PlatformAccountIngestionService",
    "ADDirectorySnapshotService",
]
