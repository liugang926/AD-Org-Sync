from __future__ import annotations

import copy
import secrets
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

from sync_app.services.identity_relationships import IdentityRelationshipPreview


BINDING_RECONCILIATION_CATEGORIES = (
    {
        "code": "safe_cleanup",
        "label": "Safe to clean",
        "level": "success",
    },
    {
        "code": "ad_exists",
        "label": "Still exists in AD",
        "level": "info",
    },
    {
        "code": "directory_unavailable",
        "label": "Directory unavailable",
        "level": "error",
    },
    {
        "code": "verification_unknown",
        "label": "Verification unknown",
        "level": "warning",
    },
    {
        "code": "protected_account",
        "label": "Protected account",
        "level": "warning",
    },
    {
        "code": "connector_ambiguous",
        "label": "Multiple connector ambiguity",
        "level": "error",
    },
    {
        "code": "binding_changed",
        "label": "Binding changed concurrently",
        "level": "error",
    },
    {
        "code": "manual_review",
        "label": "Manual review required",
        "level": "warning",
    },
)
CATEGORY_BY_CODE = {
    str(item["code"]): dict(item) for item in BINDING_RECONCILIATION_CATEGORIES
}
UNAVAILABLE_AD_STATUSES = {
    "connection_failed",
    "error",
    "failed",
    "timeout",
    "unavailable",
}
UNKNOWN_AD_STATUSES = {
    "",
    "historical",
    "not_checked",
    "unknown",
}
EXISTING_AD_STATUSES = {
    "disabled",
    "enabled",
    "exists",
    "locked",
}
AMBIGUOUS_RISKS = {
    "connector_conflict",
    "connector_migration_required",
}
MULTI_BINDING_RISKS = {
    "multiple_bindings",
}
CREATE_BLOCKING_RISKS = {
    "connector_conflict",
    "connector_migration_required",
    "multiple_ad_candidates",
    "multiple_bindings",
    "normalized_username_collision",
    "protected_account",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    return (value or _utcnow()).astimezone(timezone.utc).isoformat(
        timespec="seconds"
    )


def _parse_time(value: Any) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_fresh_verification(verified_at: str, started_at: str) -> bool:
    verified = _parse_time(verified_at)
    started = _parse_time(started_at)
    return bool(verified and started and verified >= started)


@dataclass(slots=True)
class BindingReconciliationItem:
    organization_id: str
    source_provider: str
    connector_id: str
    source_user_id: str
    source_display_name: str
    employee_id: str
    saved_binding: str
    current_saved_binding: str
    candidate_account: str
    candidate_ad_status: str
    candidate_ad_exists: bool | None
    candidate_verified_at: str
    ad_status: str
    ad_exists: bool | None
    verified_at: str
    cleanup_reason: str
    reason_code: str
    category: str
    category_label: str
    category_level: str
    cleanup_allowed: bool
    binding_revision: int
    binding_updated_at: str
    checked_account: str
    binding_source: str
    risks: list[str] = field(default_factory=list)
    can_create: bool = False
    creation_status: str = ""
    creation_reason: str = ""
    execution_result: str = ""
    execution_reason_code: str = ""

    @property
    def identity_key(self) -> tuple[str, str, str, str]:
        return (
            self.organization_id.casefold(),
            self.source_provider.casefold(),
            self.connector_id,
            self.source_user_id,
        )

    def cleanup_target(self) -> dict[str, Any] | None:
        if not self.cleanup_allowed:
            return None
        return {
            "org_id": self.organization_id,
            "source_provider": self.source_provider,
            "connector_id": self.connector_id,
            "source_user_id": self.source_user_id,
            "source_display_name": self.source_display_name,
            "employee_id": self.employee_id,
            "ad_username": self.saved_binding,
            "binding_source": self.binding_source,
            "binding_revision": self.binding_revision,
            "binding_updated_at": self.binding_updated_at,
            "candidate_ad_username": self.candidate_account,
            "verified_at": self.verified_at,
        }

    def set_category(self, code: str, reason: str, reason_code: str) -> None:
        definition = CATEGORY_BY_CODE[code]
        self.category = code
        self.category_label = str(definition["label"])
        self.category_level = str(definition["level"])
        self.cleanup_reason = reason
        self.reason_code = reason_code
        self.cleanup_allowed = code == "safe_cleanup"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class BindingReconciliationReport:
    scan_id: str
    organization_id: str
    organization_name: str
    environment_label: str
    source_provider: str
    snapshot_id: int
    snapshot_fingerprint: str
    snapshot_version: str
    scan_started_at: str
    scanned_at: str
    items: list[BindingReconciliationItem]
    status: str = "scanned"
    completed_at: str = ""
    execution_deleted_count: int = 0
    execution_skipped_count: int = 0
    execution_reason_code: str = ""

    @property
    def category_counts(self) -> dict[str, int]:
        counts = {str(item["code"]): 0 for item in BINDING_RECONCILIATION_CATEGORIES}
        for item in self.items:
            counts[item.category] = counts.get(item.category, 0) + 1
        return counts

    @property
    def cleanup_targets(self) -> list[dict[str, Any]]:
        return [
            target
            for item in self.items
            if (target := item.cleanup_target()) is not None
        ]

    @property
    def cleanup_count(self) -> int:
        return len(self.cleanup_targets)

    @property
    def skip_count(self) -> int:
        return max(len(self.items) - self.cleanup_count, 0)

    @property
    def exact_account_summary(self) -> str:
        accounts = [
            (
                f"{target['source_provider']}/{target['connector_id']}/"
                f"{target['source_user_id']} → {target['ad_username']}"
            )
            for target in self.cleanup_targets
        ]
        if len(accounts) <= 10:
            return "; ".join(accounts) or "-"
        return "; ".join(accounts[:10]) + f"; +{len(accounts) - 10} more"

    @property
    def has_concurrent_changes(self) -> bool:
        return any(item.category == "binding_changed" for item in self.items)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["category_counts"] = self.category_counts
        value["cleanup_count"] = self.cleanup_count
        value["skip_count"] = self.skip_count
        value["exact_account_summary"] = self.exact_account_summary
        return value


class BindingReconciliationScanStore:
    """Short-lived, process-local scan storage.

    Scan reports deliberately do not enter the application database. The
    signed session stores only the opaque scan id; reports expire after the
    same bounded window as the high-risk confirmation.
    """

    def __init__(self, *, max_age_seconds: int = 900) -> None:
        self.max_age_seconds = max(int(max_age_seconds), 1)
        self._reports: dict[str, BindingReconciliationReport] = {}
        self._lock = threading.RLock()

    def put(self, report: BindingReconciliationReport) -> None:
        with self._lock:
            self.prune()
            self._reports[report.scan_id] = copy.deepcopy(report)

    def get(self, scan_id: str) -> BindingReconciliationReport | None:
        with self._lock:
            self.prune()
            report = self._reports.get(str(scan_id or "").strip())
            return copy.deepcopy(report) if report is not None else None

    def replace(self, report: BindingReconciliationReport) -> None:
        with self._lock:
            self.prune()
            self._reports[report.scan_id] = copy.deepcopy(report)

    def prune(self, *, now: datetime | None = None) -> None:
        with self._lock:
            current = (now or _utcnow()).astimezone(timezone.utc)
            expired = []
            for scan_id, report in self._reports.items():
                scanned_at = _parse_time(report.scanned_at)
                if (
                    scanned_at is None
                    or scanned_at
                    < current - timedelta(seconds=self.max_age_seconds)
                ):
                    expired.append(scan_id)
            for scan_id in expired:
                self._reports.pop(scan_id, None)


class BindingReconciliationService:
    def __init__(self, *, user_binding_repo: Any, audit_repo: Any | None = None):
        self.user_binding_repo = user_binding_repo
        self.audit_repo = audit_repo

    @staticmethod
    def _classify(
        relationship: IdentityRelationshipPreview,
        *,
        scan_started_at: str,
    ) -> BindingReconciliationItem:
        before = dict(relationship.before_state or {})
        state = dict(before.get("ad_account_state") or {})
        candidate_state = dict(relationship.candidate_ad_state or {})
        risks = sorted({str(value) for value in relationship.risks})
        binding = str(before.get("bound_ad_username") or "").strip()
        candidate = str(
            relationship.candidate_mapping.get("ad_username") or ""
        ).strip()
        checked_account = str(before.get("checked_ad_username") or "").strip()
        verified_at = str(before.get("verified_at") or "").strip()
        ad_status = str(state.get("status") or "unknown").strip().lower()
        category = "manual_review"
        reason_code = "binding_reconciliation.reason.manual_review"
        reason = "This identity requires a reviewed decision."
        cleanup_allowed = False

        if relationship.connector_id == "__conflict__" or AMBIGUOUS_RISKS.intersection(
            risks
        ):
            category = "connector_ambiguous"
            reason_code = "binding_reconciliation.reason.connector_ambiguous"
            reason = (
                "Connector assignment is ambiguous or has migrated; no binding can be "
                "cleaned automatically."
            )
        elif MULTI_BINDING_RISKS.intersection(risks):
            reason_code = "binding_reconciliation.reason.multiple_bindings"
            reason = "Multiple persisted bindings require manual review."
        elif not binding:
            reason_code = "binding_reconciliation.reason.no_saved_binding"
            reason = "No saved binding exists, so there is nothing to clean."
        elif bool(state.get("protected")) or ad_status == "protected":
            category = "protected_account"
            reason_code = "binding_reconciliation.reason.protected"
            reason = "The saved target is protected and cannot be cleaned."
        elif not _is_fresh_verification(verified_at, scan_started_at):
            category = "verification_unknown"
            reason_code = "binding_reconciliation.reason.historical_verification"
            reason = "Only historical or stale verification evidence is available."
        elif checked_account.casefold() != binding.casefold():
            category = "verification_unknown"
            reason_code = "binding_reconciliation.reason.checked_target_mismatch"
            reason = "The live verification did not check the exact saved target."
        elif ad_status in UNAVAILABLE_AD_STATUSES:
            category = "directory_unavailable"
            reason_code = "binding_reconciliation.reason.directory_unavailable"
            reason = "The target directory was unavailable; cleanup fails closed."
        elif ad_status in UNKNOWN_AD_STATUSES or state.get("exists") is None:
            category = "verification_unknown"
            reason_code = "binding_reconciliation.reason.verification_unknown"
            reason = "AD did not return a definitive exists result."
        elif state.get("exists") is True or ad_status in EXISTING_AD_STATUSES:
            category = "ad_exists"
            reason_code = "binding_reconciliation.reason.ad_exists"
            reason = "The exact saved target still exists in AD."
        elif ad_status == "missing" and state.get("exists") is False:
            category = "safe_cleanup"
            reason_code = "binding_reconciliation.reason.safe_cleanup"
            reason = "Fresh live AD verification confirmed the exact saved target is missing."
            cleanup_allowed = True
        else:
            category = "verification_unknown"
            reason_code = "binding_reconciliation.reason.verification_unknown"
            reason = "AD did not return a definitive missing result."

        definition = CATEGORY_BY_CODE[category]
        creation = dict(relationship.creation_eligibility or {})
        return BindingReconciliationItem(
            organization_id=relationship.org_id,
            source_provider=relationship.source_provider,
            connector_id=relationship.connector_id,
            source_user_id=relationship.source_user_id,
            source_display_name=relationship.source_display_name,
            employee_id=relationship.employee_id,
            saved_binding=binding,
            current_saved_binding=binding,
            candidate_account=candidate,
            candidate_ad_status=str(
                candidate_state.get("status") or "unknown"
            ).strip().lower(),
            candidate_ad_exists=candidate_state.get("exists"),
            candidate_verified_at=str(candidate_state.get("verified_at") or ""),
            ad_status=ad_status,
            ad_exists=state.get("exists"),
            verified_at=verified_at,
            cleanup_reason=reason,
            reason_code=reason_code,
            category=category,
            category_label=str(definition["label"]),
            category_level=str(definition["level"]),
            cleanup_allowed=cleanup_allowed,
            binding_revision=int(before.get("binding_revision") or 0),
            binding_updated_at=str(before.get("binding_updated_at") or ""),
            checked_account=checked_account,
            binding_source=str(before.get("binding_source") or ""),
            risks=risks,
            can_create=bool(creation.get("eligible")),
            creation_status=str(creation.get("status") or ""),
            creation_reason=str(creation.get("reason") or ""),
        )

    def scan(
        self,
        relationships: Iterable[IdentityRelationshipPreview],
        *,
        organization_id: str,
        organization_name: str,
        environment_label: str,
        source_provider: str,
        snapshot_id: int,
        snapshot_fingerprint: str,
        scan_started_at: str,
        scanned_at: str | None = None,
        scan_id: str | None = None,
    ) -> BindingReconciliationReport:
        completed_at = scanned_at or _iso()
        items = [
            self._classify(item, scan_started_at=scan_started_at)
            for item in relationships
        ]
        return BindingReconciliationReport(
            scan_id=scan_id or secrets.token_urlsafe(18),
            organization_id=str(organization_id or "").strip().casefold(),
            organization_name=str(organization_name or "").strip(),
            environment_label=str(environment_label or "").strip(),
            source_provider=str(source_provider or "").strip().casefold(),
            snapshot_id=max(int(snapshot_id or 0), 0),
            snapshot_fingerprint=str(snapshot_fingerprint or ""),
            snapshot_version=f"#{int(snapshot_id)}" if snapshot_id else "Not available",
            scan_started_at=scan_started_at,
            scanned_at=completed_at,
            items=items,
        )

    def refresh_concurrency(
        self, report: BindingReconciliationReport
    ) -> BindingReconciliationReport:
        refreshed = copy.deepcopy(report)
        if refreshed.status != "scanned":
            return refreshed
        for item in refreshed.items:
            if not item.cleanup_allowed:
                continue
            records = self.user_binding_repo.list_binding_records_for_source_identity(
                item.source_user_id,
                org_id=item.organization_id,
                source_provider=item.source_provider,
            )
            exact = [
                record
                for record in records
                if record.connector_id == item.connector_id
            ]
            record = exact[0] if len(records) == 1 and len(exact) == 1 else None
            if (
                record is None
                or str(record.ad_username or "").casefold()
                != item.saved_binding.casefold()
                or int(record.binding_revision or 0) != item.binding_revision
            ):
                item.current_saved_binding = (
                    str(record.ad_username or "") if record is not None else ""
                )
                item.set_category(
                    "binding_changed",
                    "The saved binding changed after this scan. Scan again before cleanup.",
                    "binding_reconciliation.reason.binding_changed",
                )
        return refreshed

    @staticmethod
    def _can_create_after_cleanup(
        item: BindingReconciliationItem,
    ) -> tuple[bool, str]:
        if not item.candidate_account:
            return False, "No field candidate account is available."
        if item.creation_status not in {
            "binding_disabled",
            "binding_review_required",
            "eligible",
        }:
            return (
                False,
                item.creation_reason
                or "The candidate does not meet the account-creation conditions.",
            )
        if CREATE_BLOCKING_RISKS.intersection(item.risks):
            return False, "Candidate creation is blocked by identity or connector risk."
        candidate_missing = (
            item.candidate_ad_status == "missing"
            and item.candidate_ad_exists is False
            and bool(item.candidate_verified_at)
        )
        if not candidate_missing:
            return False, "The candidate account is not freshly verified as missing."
        return (
            True,
            "The field candidate is freshly verified as missing and can be prepared in the identity matching workbench.",
        )

    def _audit_item(
        self,
        *,
        actor_username: str,
        item: BindingReconciliationItem,
        result: str,
        message: str,
        audit_context: Mapping[str, Any],
        reason_code: str,
    ) -> None:
        if self.audit_repo is None:
            return
        self.audit_repo.add_log(
            org_id=item.organization_id,
            actor_username=actor_username,
            action_type="mapping.binding_reconciliation.item",
            target_type="user_identity_binding",
            target_id=item.source_user_id,
            result=result,
            message=message,
            payload={
                **dict(audit_context),
                "source_provider": item.source_provider,
                "connector_id": item.connector_id,
                "source_user_id": item.source_user_id,
                "source_display_name": item.source_display_name,
                "employee_id": item.employee_id,
                "scanned_ad_username": item.saved_binding,
                "candidate_ad_username": item.candidate_account,
                "binding_revision": item.binding_revision,
                "verified_at": item.verified_at,
                "classification": item.category,
                "reason_code": reason_code,
                "cleanup_allowed": item.cleanup_allowed,
            },
        )

    def block_execution(
        self,
        report: BindingReconciliationReport,
        *,
        actor_username: str,
        audit_context: Mapping[str, Any],
        reason_code: str,
    ) -> BindingReconciliationReport:
        blocked = copy.deepcopy(report)
        blocked.status = "blocked"
        blocked.completed_at = _iso()
        blocked.execution_reason_code = reason_code
        blocked.execution_deleted_count = 0
        blocked.execution_skipped_count = len(blocked.items)
        for item in blocked.items:
            item.execution_result = "skipped"
            item.execution_reason_code = reason_code
            self._audit_item(
                actor_username=actor_username,
                item=item,
                result="blocked",
                message="Binding reconciliation item failed closed before cleanup",
                audit_context=audit_context,
                reason_code=reason_code,
            )
        return blocked

    def execute(
        self,
        report: BindingReconciliationReport,
        current_report: BindingReconciliationReport,
        *,
        actor_username: str,
        audit_context: Mapping[str, Any],
    ) -> BindingReconciliationReport:
        executed = copy.deepcopy(report)
        current_by_key = {
            item.identity_key: item for item in current_report.items
        }
        deleted_count = 0
        skipped_count = 0
        for item in executed.items:
            if not item.cleanup_allowed:
                skipped_count += 1
                item.execution_result = "skipped"
                item.execution_reason_code = item.reason_code
                self._audit_item(
                    actor_username=actor_username,
                    item=item,
                    result="skipped",
                    message="Binding reconciliation item was not eligible for cleanup",
                    audit_context=audit_context,
                    reason_code=item.reason_code,
                )
                continue

            current = current_by_key.get(item.identity_key)
            reason_code = ""
            if current is None:
                reason_code = "binding_reconciliation.reason.connector_or_identity_changed"
            elif not current.cleanup_allowed:
                reason_code = current.reason_code
            elif current.saved_binding.casefold() != item.saved_binding.casefold():
                reason_code = "binding_reconciliation.reason.target_changed"
            elif current.binding_revision != item.binding_revision:
                reason_code = "binding_reconciliation.reason.binding_changed"

            if reason_code:
                skipped_count += 1
                item.execution_result = "skipped"
                item.execution_reason_code = reason_code
                item.current_saved_binding = (
                    current.current_saved_binding if current is not None else ""
                )
                if reason_code in {
                    "binding_reconciliation.reason.binding_changed",
                    "binding_reconciliation.reason.target_changed",
                    "binding_reconciliation.reason.connector_or_identity_changed",
                }:
                    item.set_category(
                        "binding_changed",
                        "The binding or its identity boundary changed before cleanup.",
                        reason_code,
                    )
                self._audit_item(
                    actor_username=actor_username,
                    item=item,
                    result="blocked",
                    message="Binding reconciliation item failed closed during live reverification",
                    audit_context=audit_context,
                    reason_code=reason_code,
                )
                continue

            assert current is not None
            removed = self.user_binding_repo.delete_binding_if_target_matches(
                item.source_user_id,
                item.saved_binding,
                org_id=item.organization_id,
                source_provider=item.source_provider,
                connector_id=item.connector_id,
                binding_revision=item.binding_revision,
            )
            if not removed:
                skipped_count += 1
                item.execution_result = "skipped"
                item.execution_reason_code = (
                    "binding_reconciliation.reason.binding_changed"
                )
                item.set_category(
                    "binding_changed",
                    "The binding changed during cleanup and was not deleted.",
                    item.execution_reason_code,
                )
                self._audit_item(
                    actor_username=actor_username,
                    item=item,
                    result="blocked",
                    message="Compare-and-delete rejected a concurrent binding change",
                    audit_context=audit_context,
                    reason_code=item.execution_reason_code,
                )
                continue

            deleted_count += 1
            item.current_saved_binding = ""
            item.execution_result = "deleted"
            item.execution_reason_code = ""
            item.can_create, item.creation_reason = self._can_create_after_cleanup(
                current
            )
            self._audit_item(
                actor_username=actor_username,
                item=item,
                result="success",
                message="Removed the exact saved binding after a second live AD missing result",
                audit_context=audit_context,
                reason_code="binding_reconciliation.result.deleted",
            )

        executed.status = (
            "completed"
            if deleted_count == report.cleanup_count
            else "completed_with_skips"
        )
        executed.completed_at = _iso()
        executed.execution_deleted_count = deleted_count
        executed.execution_skipped_count = skipped_count
        return executed
