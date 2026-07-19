from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping

from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.core.fingerprints import fingerprint_json
from sync_app.services.source_directory import SourceDirectoryService


STANDARD_FIELD_LABELS = {
    "source_user_id": "Platform User ID",
    "employee_id": "Employee ID",
    "email_localpart": "Email local part",
    "pinyin_initials_employee_id": "Pinyin initials + employee ID",
    "pinyin_full_employee_id": "Full pinyin + employee ID",
    "family_name_pinyin_given_initials": "Family pinyin + given initials",
    "family_name_pinyin_given_name_pinyin": "Full romanized name",
    "custom_template": "Custom template",
}


@dataclass(slots=True)
class IdentityRelationshipPreview:
    org_id: str
    source_provider: str
    connector_id: str
    source_user_id: str
    source_display_name: str
    employee_id: str
    source_user: dict[str, Any]
    mapping_input: dict[str, Any]
    candidate_mapping: dict[str, Any]
    before_state: dict[str, Any]
    planned_after_state: dict[str, Any]
    applied_after_state: dict[str, Any]
    effective_ad_username: str
    effective_resolution_source: str
    resolution_reason: str
    rule_hits: list[str]
    difference: dict[str, Any]
    candidate_ad_state: dict[str, Any] = field(default_factory=dict)
    creation_eligibility: dict[str, Any] = field(default_factory=dict)
    risks: list[str] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)
    updated_at: str = ""
    ad_query_usernames: list[str] = field(default_factory=list, repr=False)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("ad_query_usernames", None)
        return payload


def assess_identity_match(item: IdentityRelationshipPreview) -> dict[str, str]:
    """Summarize matching evidence without changing binding or AD state."""

    candidate = str(item.candidate_mapping.get("ad_username") or "").strip()
    binding = str(item.before_state.get("bound_ad_username") or "").strip()
    difference = str(item.difference.get("status") or "")
    blocking_risks = {
        "connector_conflict",
        "multiple_bindings",
        "multiple_ad_candidates",
        "normalized_username_collision",
        "connector_migration_required",
    }
    if item.effective_resolution_source == "manual_binding":
        return {
            "status": "manual_override",
            "confidence": "not_applicable",
            "level": "info",
            "reason": item.resolution_reason or "A reviewed manual binding takes precedence",
            "next_action": "Review Manual Override",
        }
    if item.effective_resolution_source == "conflict" or blocking_risks.intersection(item.risks):
        return {
            "status": "conflict",
            "confidence": "low",
            "level": "error",
            "reason": item.resolution_reason or "Conflicting evidence requires a human decision",
            "next_action": "Open Conflict Queue",
        }
    if not candidate:
        return {
            "status": "blocked",
            "confidence": "low",
            "level": "error",
            "reason": "No candidate username can be generated from the current source data",
            "next_action": "Repair Source Data",
        }
    if binding and item.before_state.get("binding_enabled") is False:
        return {
            "status": "blocked",
            "confidence": "low",
            "level": "error",
            "reason": "The current binding is disabled and cannot resolve this identity",
            "next_action": "Review Binding",
        }
    if binding and candidate.casefold() == binding.casefold():
        return {
            "status": "confirmed",
            "confidence": "high",
            "level": "success",
            "reason": "The generated candidate matches the current binding",
            "next_action": "Review Evidence",
        }
    if binding or difference == "candidate_differs_from_binding":
        return {
            "status": "review",
            "confidence": "low",
            "level": "warning",
            "reason": "The generated candidate differs from the current binding",
            "next_action": "Review Identity",
        }
    method = str(item.mapping_input.get("method") or "")
    if method in {"employee_id", "userid"}:
        return {
            "status": "ready",
            "confidence": "high",
            "level": "success",
            "reason": "The candidate comes from a stable unique source identifier",
            "next_action": "Review Evidence",
        }
    return {
        "status": "review",
        "confidence": "medium",
        "level": "warning",
        "reason": item.resolution_reason or "Review the generated candidate before execution",
        "next_action": "Review Identity",
    }


IDENTITY_WORKBENCH_QUEUES = (
    "pending",
    "creatable",
    "unbound",
    "bound",
    "conflict",
    "all",
)

BUSINESS_IDENTITY_STATUSES = {
    "pending": {
        "label_code": "business_status.pending",
        "level": "warning",
    },
    "bound": {
        "label_code": "business_status.bound",
        "level": "success",
    },
    "unbound": {
        "label_code": "business_status.unbound",
        "level": "info",
    },
    "creatable": {
        "label_code": "business_status.creatable",
        "level": "success",
    },
    "conflict": {
        "label_code": "business_status.conflict",
        "level": "danger",
    },
    "validation_unknown": {
        "label_code": "business_status.validation_unknown",
        "level": "unchecked",
    },
    "planned": {
        "label_code": "business_status.planned",
        "level": "planned",
    },
    "applied": {
        "label_code": "business_status.applied",
        "level": "success",
    },
    "apply_failed": {
        "label_code": "business_status.apply_failed",
        "level": "danger",
    },
}


def resolve_identity_business_status(
    item: IdentityRelationshipPreview,
    *,
    relationship_code: str,
    ad_status: str,
) -> dict[str, str]:
    """Resolve the single shared business status shown across identity surfaces."""

    applied_result = str(item.applied_after_state.get("result") or "").strip().lower()
    has_current_plan = bool(item.planned_after_state.get("job_id")) and not bool(
        item.planned_after_state.get("is_stale")
    )
    if applied_result == "failed":
        status = "apply_failed"
    elif applied_result == "succeeded":
        status = "applied"
    elif has_current_plan:
        status = "planned"
    elif relationship_code in {
        "multiple_user_candidate_conflict",
        "connector_unavailable",
        "saved_binding_expired",
    }:
        status = (
            "validation_unknown"
            if relationship_code == "connector_unavailable"
            else ("conflict" if relationship_code == "multiple_user_candidate_conflict" else "pending")
        )
    elif ad_status == "unknown":
        status = "validation_unknown"
    elif relationship_code == "bound_account_exists":
        status = "bound"
    elif relationship_code == "creatable":
        status = "creatable"
    elif relationship_code in {
        "unbound_candidate_exists",
        "unbound_candidate_missing",
        "candidate_unavailable",
    }:
        status = "unbound"
    else:
        status = "pending"
    return {"status": status, **BUSINESS_IDENTITY_STATUSES[status]}


def classify_identity_relationship(
    item: IdentityRelationshipPreview,
    *,
    deferred: bool = False,
) -> dict[str, Any]:
    """Return one business-facing conclusion for the identity workbench."""

    candidate = str(item.candidate_mapping.get("ad_username") or "").strip()
    binding = str(item.before_state.get("bound_ad_username") or "").strip()
    candidate_state = dict(item.candidate_ad_state or {})
    binding_state = dict(item.before_state.get("ad_account_state") or {})
    if binding:
        ad_state = binding_state
        ad_subject = binding
    elif item.effective_resolution_source == "existing_ad_match":
        ad_state = binding_state
        ad_subject = str(item.before_state.get("checked_ad_username") or candidate)
    elif str(candidate_state.get("status") or "") not in {"", "not_checked"}:
        ad_state = candidate_state
        ad_subject = candidate
    else:
        ad_state = binding_state or candidate_state
        ad_subject = candidate
    ad_status = str(ad_state.get("status") or "not_checked").strip().lower()
    if ad_status in {"", "not_checked"}:
        ad_status = "unknown"

    conflict_risks = {
        "connector_conflict",
        "connector_migration_required",
        "multiple_ad_candidates",
        "multiple_bindings",
        "normalized_username_collision",
    }
    is_conflict = bool(
        item.effective_resolution_source == "conflict"
        or conflict_risks.intersection(item.risks)
        or "conflict" in str(item.difference.get("status") or "")
        or any(
            str(record.get("status") or "").strip().lower() == "open"
            for record in item.evidence.get("conflict_records") or []
        )
    )
    candidate_differs = bool(
        binding and candidate and binding.casefold() != candidate.casefold()
    )
    ad_exists = bool(ad_state.get("exists") is True) or ad_status in {
        "exists",
        "enabled",
        "disabled",
        "locked",
        "protected",
    }
    ad_missing = bool(ad_state.get("exists") is False) or ad_status == "missing"

    if is_conflict:
        code = "multiple_user_candidate_conflict"
        conclusion = (
            "Multiple users share the same candidate AD account"
            if "normalized_username_collision" in item.risks
            else "Identity evidence conflicts and requires review"
        )
        level = "error"
        suggested_action = "View conflict"
        action_code = "conflict"
    elif ad_status == "unavailable":
        code = "connector_unavailable"
        conclusion = "Connector unavailable"
        level = "error"
        suggested_action = "View identity details"
        action_code = "details"
    elif binding and ad_missing:
        code = "saved_binding_expired"
        conclusion = "Saved binding has expired"
        level = "error"
        suggested_action = "View or modify binding"
        action_code = "binding"
    elif binding and candidate_differs:
        code = "candidate_binding_mismatch"
        conclusion = "Candidate AD account differs from saved binding"
        level = "warning"
        suggested_action = "View or modify binding"
        action_code = "binding"
    elif binding and ad_status == "unknown":
        code = "ad_status_unknown"
        conclusion = "AD status unknown"
        level = "warning"
        suggested_action = "View identity details"
        action_code = "details"
    elif binding and ad_exists:
        code = "bound_account_exists"
        conclusion = "Bound and AD account exists"
        level = "success"
        suggested_action = "View or modify binding"
        action_code = "binding"
    elif not binding and ad_exists:
        code = "unbound_candidate_exists"
        conclusion = "No binding; candidate AD account exists"
        level = "warning"
        suggested_action = "Bind existing account"
        action_code = "bind"
    elif (
        not binding
        and ad_missing
        and bool(item.creation_eligibility.get("eligible"))
    ):
        code = "creatable"
        conclusion = "No binding; candidate AD account does not exist and can be created"
        level = "success"
        suggested_action = "Select for creation"
        action_code = "create"
    elif ad_status == "unknown":
        code = "ad_status_unknown"
        conclusion = "AD status unknown"
        level = "warning"
        suggested_action = "View identity details"
        action_code = "details"
    elif not candidate:
        code = "candidate_unavailable"
        conclusion = "No candidate AD account can be calculated"
        level = "error"
        suggested_action = "View identity details"
        action_code = "details"
    else:
        code = "unbound_candidate_missing"
        conclusion = "No binding; candidate AD account does not exist"
        level = "warning"
        suggested_action = "View identity details"
        action_code = "details"

    queues = {"all"}
    if binding:
        queues.add("bound")
    else:
        queues.add("unbound")
    if code == "creatable":
        queues.add("creatable")
    if is_conflict:
        queues.add("conflict")
    if code != "bound_account_exists" and not deferred:
        queues.add("pending")

    business_status = resolve_identity_business_status(
        item,
        relationship_code=code,
        ad_status=ad_status,
    )
    return {
        "code": code,
        "conclusion": conclusion,
        "level": level,
        "business_status": business_status["status"],
        "business_status_label_code": business_status["label_code"],
        "business_status_level": business_status["level"],
        "suggested_action": suggested_action,
        "action_code": action_code,
        "queues": sorted(queues),
        "identity_status": code,
        "ad_status": ad_status,
        "ad_state": ad_state,
        "ad_subject": ad_subject,
        "has_binding": bool(binding),
        "is_conflict": is_conflict,
        "deferred": bool(deferred),
    }


def filter_identity_workbench_rows(
    rows: Iterable[dict[str, Any]],
    *,
    queue: str = "all",
    identity_status: str = "",
    ad_status: str = "",
) -> list[dict[str, Any]]:
    normalized_queue = str(queue or "all").strip().lower()
    if normalized_queue not in IDENTITY_WORKBENCH_QUEUES:
        normalized_queue = "pending"
    normalized_identity_status = str(identity_status or "").strip().lower()
    normalized_ad_status = str(ad_status or "").strip().lower()
    return [
        row
        for row in rows
        if (
            (not normalized_identity_status or row["workbench"]["identity_status"] == normalized_identity_status)
            and (not normalized_ad_status or row["workbench"]["ad_status"] == normalized_ad_status)
            and normalized_queue in row["workbench"]["queues"]
        )
    ]


def summarize_identity_workbench_rows(
    rows: Iterable[dict[str, Any]],
) -> dict[str, int]:
    materialized = list(rows)
    return {
        queue: (
            len(materialized)
            if queue == "all"
            else sum(1 for row in materialized if queue in row["workbench"]["queues"])
        )
        for queue in IDENTITY_WORKBENCH_QUEUES
    }


def build_identity_preview_fingerprint(
    *,
    org_id: str,
    source_provider: str,
    connector_id: str,
    source_user_id: str,
    source_snapshot_fingerprint: str,
    selection_fingerprint: str,
    config_fingerprint: str = "",
    mapping_input: dict[str, Any],
    candidate_mapping: dict[str, Any],
    binding_signature: Iterable[dict[str, Any]] = (),
    ad_state: dict[str, Any] | None = None,
) -> str:
    normalized_ad_state = dict(ad_state or {})
    if normalized_ad_state.get("status") in {"", "not_checked", "unavailable"}:
        normalized_ad_state = {}
    else:
        normalized_ad_state = {
            key: normalized_ad_state.get(key)
            for key in ("status", "exists", "enabled", "locked", "protected")
        }
    return fingerprint_json(
        {
            "org_id": org_id,
            "source_provider": source_provider,
            "connector_id": connector_id,
            "source_user_id": source_user_id,
            "source_snapshot_fingerprint": source_snapshot_fingerprint,
            "selection_fingerprint": selection_fingerprint,
            "config_fingerprint": config_fingerprint,
            "mapping_input": mapping_input,
            "candidate_mapping": {
                "ad_username": candidate_mapping.get("ad_username") or "",
                "source": candidate_mapping.get("source") or "",
            },
            "bindings": list(binding_signature),
            "ad_state": normalized_ad_state,
        },
        namespace="identity-relationship-preview",
    )


def build_runtime_identity_evidence(
    *,
    user: Any,
    org_id: str,
    source_provider: str,
    connector_id: str,
    connector_spec: dict[str, Any],
    source_scope: dict[str, Any] | None,
    config_fingerprint: str = "",
    binding_records: Iterable[Any] = (),
    before_ad_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the structured evidence shared by runtime and web previews."""

    scope = dict(source_scope or {})
    source_field = str(scope.get("source_field") or connector_spec.get("username_strategy") or "source_user_id")
    row = {
        "source_user_id": str(getattr(user, "source_user_id", "") or getattr(user, "userid", "")),
        "display_name": str(getattr(user, "name", "") or ""),
        "employee_id": str(getattr(user, "employee_id", "") or ""),
        "email": str(getattr(user, "email", "") or ""),
        "position": str(getattr(user, "position", "") or ""),
        "department_ids": list(getattr(user, "departments", []) or []),
        "raw_payload": dict(getattr(user, "raw_payload", {}) or {}),
    }
    preview = SourceDirectoryService.preview_username(
        row,
        username_strategy=str(connector_spec.get("username_strategy") or "custom_template"),
        username_template=str(connector_spec.get("username_template") or ""),
        source_field=source_field,
        username_collision_policy=str(
            connector_spec.get("username_collision_policy") or "append_employee_id"
        ),
        username_collision_template=str(
            connector_spec.get("username_collision_template") or ""
        ),
        field_label=STANDARD_FIELD_LABELS.get(source_field, source_field),
    )
    binding_signature = [
        {
            "source_provider": str(getattr(item, "source_provider", "") or ""),
            "connector_id": str(getattr(item, "connector_id", "") or ""),
            "ad_username": str(getattr(item, "ad_username", "") or ""),
            "source": str(getattr(item, "source", "") or ""),
            "is_enabled": bool(getattr(item, "is_enabled", False)),
            "updated_at": str(getattr(item, "updated_at", "") or ""),
        }
        for item in binding_records
    ]
    evidence: dict[str, Any] = {
        "org_id": org_id,
        "source_provider": source_provider,
        "connector_id": connector_id,
        "source_user_id": row["source_user_id"],
        "source_display_name": row["display_name"],
        "mapping_input": preview["mapping_input"],
        "candidate_mapping": preview["candidate_mapping"],
        "source_snapshot_fingerprint": str(scope.get("source_snapshot_fingerprint") or ""),
        "selection_fingerprint": str(scope.get("selection_fingerprint") or ""),
        "config_fingerprint": str(config_fingerprint or ""),
        "before_ad_state": dict(before_ad_state or {}),
    }
    evidence["preview_fingerprint"] = build_identity_preview_fingerprint(
        org_id=org_id,
        source_provider=source_provider,
        connector_id=connector_id,
        source_user_id=str(row["source_user_id"]),
        source_snapshot_fingerprint=str(evidence["source_snapshot_fingerprint"]),
        selection_fingerprint=str(evidence["selection_fingerprint"]),
        config_fingerprint=str(evidence["config_fingerprint"]),
        mapping_input=dict(evidence["mapping_input"]),
        candidate_mapping=dict(evidence["candidate_mapping"]),
        binding_signature=binding_signature,
        ad_state=before_ad_state,
    )
    return evidence


class IdentityRelationshipPreviewService:
    def __init__(
        self,
        *,
        source_directory_repo: Any,
        user_binding_repo: Any,
        operation_log_repo: Any,
        planned_operation_repo: Any,
    ) -> None:
        self.source_directory_repo = source_directory_repo
        self.user_binding_repo = user_binding_repo
        self.operation_log_repo = operation_log_repo
        self.planned_operation_repo = planned_operation_repo

    @staticmethod
    def _ad_state_from_record(record: Any, *, protected: bool = False) -> dict[str, Any]:
        if record is None:
            return {
                "status": "protected" if protected else "missing",
                "exists": None if protected else False,
                "enabled": None,
                "locked": None,
                "protected": protected,
            }
        raw_entry = dict(getattr(record, "raw_entry", {}) or {})
        attributes = dict(raw_entry.get("attributes") or {})
        try:
            user_account_control = int(attributes.get("userAccountControl") or 0)
        except (TypeError, ValueError):
            user_account_control = 0
        try:
            lockout_time = int(attributes.get("lockoutTime") or 0)
        except (TypeError, ValueError):
            lockout_time = 0
        enabled = None if "userAccountControl" not in attributes else not bool(user_account_control & 2)
        locked = None if "lockoutTime" not in attributes else lockout_time > 0
        return {
            "status": (
                "protected"
                if protected
                else (
                    "locked"
                    if locked
                    else (
                        "disabled"
                        if enabled is False
                        else ("enabled" if enabled is True else "exists")
                    )
                )
            ),
            "exists": True,
            "enabled": enabled,
            "locked": locked,
            "protected": protected,
        }

    @staticmethod
    def load_ad_states(
        target_provider_factory: Callable[[str], Any],
        usernames_by_connector: Mapping[str, Iterable[str]],
        *,
        protected_accounts_by_connector: Mapping[str, Iterable[str]] | None = None,
        batch_size: int = 100,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        """Query each connector once and never expose directory exceptions."""

        result: dict[tuple[str, str], dict[str, Any]] = {}
        protected_map = {
            connector_id: list(values)
            for connector_id, values in dict(protected_accounts_by_connector or {}).items()
        }
        for connector_id, raw_usernames in usernames_by_connector.items():
            usernames = sorted(
                {str(value or "").strip() for value in raw_usernames if str(value or "").strip()},
                key=str.lower,
            )
            if not usernames:
                continue
            provider = None
            verified_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            query_usernames: list[str] = []
            for username in usernames:
                if is_protected_ad_account_name(
                    username,
                    protected_map.get(connector_id, []),
                ):
                    result[(connector_id, username.lower())] = {
                        "status": "protected",
                        "exists": None,
                        "enabled": None,
                        "locked": None,
                        "protected": True,
                        "verified_at": verified_at,
                    }
                else:
                    query_usernames.append(username)
            if not query_usernames:
                continue
            try:
                provider = target_provider_factory(connector_id)
                bounded_batch_size = min(max(int(batch_size or 100), 1), 500)
                for batch_start in range(0, len(query_usernames), bounded_batch_size):
                    batch = query_usernames[
                        batch_start : batch_start + bounded_batch_size
                    ]
                    records = provider.get_users_batch(batch)
                    if bool(getattr(provider, "last_batch_lookup_failed", False)):
                        raise RuntimeError("target directory batch lookup unavailable")
                    records_lower = {
                        str(key).lower(): value
                        for key, value in dict(records or {}).items()
                    }
                    for username in batch:
                        state = IdentityRelationshipPreviewService._ad_state_from_record(
                            records_lower.get(username.lower()),
                            protected=False,
                        )
                        state["verified_at"] = verified_at
                        result[(connector_id, username.lower())] = state
            except Exception:
                for username in query_usernames:
                    result[(connector_id, username.lower())] = {
                        "status": "unavailable",
                        "exists": None,
                        "enabled": None,
                        "locked": None,
                        "protected": False,
                        "verified_at": verified_at,
                    }
            finally:
                if provider is not None:
                    close_fn = getattr(provider, "close", None)
                    if callable(close_fn):
                        try:
                            close_fn()
                        except Exception:
                            pass
        return result

    @staticmethod
    def _latest_by_identity(
        rows: Iterable[dict[str, Any]],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            source_user_id = str(row.get("source_id") or "").strip()
            connector_id = str(
                row.get("resolved_connector_id")
                or row.get("scope_connector_id")
                or "default"
            ).strip()
            key = (source_user_id, connector_id)
            if source_user_id and key not in latest:
                latest[key] = row
        return latest

    @staticmethod
    def _binding_signature(records: Iterable[Any]) -> list[dict[str, Any]]:
        return [
            {
                "source_provider": item.source_provider,
                "connector_id": item.connector_id,
                "ad_username": item.ad_username,
                "source": item.source,
                "is_enabled": item.is_enabled,
                "updated_at": item.updated_at,
            }
            for item in records
        ]

    def build_relationships(
        self,
        users: Iterable[dict[str, Any]],
        *,
        org_id: str,
        source_provider: str,
        snapshot: Any,
        scope: dict[str, Any] | None,
        connector_specs_by_id: dict[str, dict[str, Any]],
        connector_ids_by_source_user: dict[str, str],
        field_labels: dict[str, str] | None = None,
        ad_states: dict[tuple[str, str], dict[str, Any]] | None = None,
        config_fingerprint: str = "",
        candidate_collision_source_ids: Iterable[str] = (),
    ) -> list[IdentityRelationshipPreview]:
        user_rows = [dict(item) for item in users]
        source_user_ids = [str(item.get("source_user_id") or "") for item in user_rows]
        connector_ids = sorted(
            {
                value
                for value in connector_ids_by_source_user.values()
                if value and value != "__conflict__"
            }
        )
        bindings = self.user_binding_repo.list_binding_records_for_source_identities(
            source_user_ids,
            org_id=org_id,
            source_provider=source_provider,
            connector_ids=(),
        )
        bindings_by_user: dict[str, list[Any]] = {}
        for item in bindings:
            bindings_by_user.setdefault(item.source_user_id, []).append(item)

        dry_evidence = self.operation_log_repo.list_latest_identity_resolution_evidence(
            source_user_ids,
            org_id=org_id,
            source_provider=source_provider,
            connector_ids=connector_ids,
            execution_mode="dry_run",
            successful_only=True,
        )
        apply_evidence = self.operation_log_repo.list_latest_identity_resolution_evidence(
            source_user_ids,
            org_id=org_id,
            source_provider=source_provider,
            connector_ids=connector_ids,
            execution_mode="apply",
            successful_only=False,
        )
        latest_dry = self._latest_by_identity(dry_evidence)
        latest_apply_resolution = self._latest_by_identity(apply_evidence)
        relevant_job_ids = {
            str(item.get("job_id") or "")
            for item in [*latest_dry.values(), *latest_apply_resolution.values()]
            if str(item.get("job_id") or "")
        }
        planned_ops = self.planned_operation_repo.list_user_operations_for_jobs(
            relevant_job_ids,
            source_user_ids=source_user_ids,
        )
        planned_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
        for item in planned_ops:
            desired_state = dict(item.get("desired_state") or {})
            key = (
                str(item.get("job_id") or ""),
                str(item.get("source_id") or ""),
                str(desired_state.get("connector_id") or "default"),
            )
            if item.get("operation_type") not in {
                "propose_identity_binding",
                "create_user",
                "update_user",
                "reactivate_user",
                "disable_user",
            }:
                continue
            if key not in planned_by_key or item.get("operation_type") != "propose_identity_binding":
                planned_by_key[key] = item
        apply_ops = self.operation_log_repo.list_user_operation_evidence_for_jobs(
            relevant_job_ids,
            org_id=org_id,
            source_user_ids=source_user_ids,
        )
        apply_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
        for item in apply_ops:
            key = (
                str(item.get("job_id") or ""),
                str(item.get("source_id") or ""),
                str((item.get("details") or {}).get("connector_id") or "default"),
            )
            if key not in apply_by_key:
                apply_by_key[key] = item

        snapshot_fingerprint = str(snapshot["snapshot_fingerprint"] or "") if snapshot else ""
        selection_fingerprint = str((scope or {}).get("selection_fingerprint") or "")
        labels = dict(STANDARD_FIELD_LABELS)
        labels.update(field_labels or {})
        states = dict(ad_states or {})
        known_candidate_collisions = {
            str(value or "").strip()
            for value in candidate_collision_source_ids
            if str(value or "").strip()
        }
        relationships: list[IdentityRelationshipPreview] = []
        for row in user_rows:
            source_user_id = str(row.get("source_user_id") or "")
            connector_id = connector_ids_by_source_user.get(source_user_id, "default") or "default"
            connector_conflict = connector_id == "__conflict__"
            spec = connector_specs_by_id.get(connector_id) or connector_specs_by_id.get("default") or {}
            source_field = str((scope or {}).get("source_field") or spec.get("username_strategy") or "source_user_id")
            preview = SourceDirectoryService.preview_username(
                row,
                username_strategy=str(spec.get("username_strategy") or (scope or {}).get("username_strategy") or "userid"),
                username_template=str(spec.get("username_template") or (scope or {}).get("username_template") or ""),
                source_field=source_field,
                username_collision_policy=str(spec.get("username_collision_policy") or "append_employee_id"),
                username_collision_template=str(spec.get("username_collision_template") or ""),
                field_label=labels.get(source_field, source_field),
            )
            ad_query_usernames = sorted(
                {
                    str(item.get("username") or "").strip()
                    for item in preview.get("resolution_candidates") or []
                    if str(item.get("username") or "").strip()
                },
                key=str.lower,
            )
            user_bindings = bindings_by_user.get(source_user_id, [])
            exact_bindings = [item for item in user_bindings if item.connector_id == connector_id]
            binding_conflict = len(exact_bindings) > 1
            connector_migration_required = bool(
                not connector_conflict
                and not exact_bindings
                and user_bindings
            )
            binding = exact_bindings[0] if len(exact_bindings) == 1 and not binding_conflict else None
            before_username = str(binding.ad_username if binding else "")
            checked_username = before_username or str(preview["username"] or "")
            before_ad_state: dict[str, Any] = dict(
                states.get((connector_id, checked_username.lower())) or {}
                if checked_username
                else {}
            )
            if not before_ad_state:
                before_ad_state = {
                    "status": "not_checked",
                    "exists": None,
                    "enabled": None,
                    "locked": None,
                    "protected": False,
                    "verified_at": "",
                }
            before_state = {
                "bound_ad_username": before_username,
                "binding_source": str(binding.source if binding else ""),
                "binding_enabled": bool(binding.is_enabled) if binding else False,
                "binding_revision": int(binding.binding_revision) if binding else 0,
                "binding_updated_at": str(binding.updated_at if binding else ""),
                "connector_id": str(binding.connector_id if binding else connector_id),
                "checked_ad_username": checked_username,
                "ad_account_state": before_ad_state,
                "verified_at": str(before_ad_state.get("verified_at") or ""),
            }

            dry = latest_dry.get((source_user_id, connector_id))
            dry_details = dict((dry or {}).get("details") or {})
            historical_before_state = dict(dry_details.get("before_state") or {})
            historical_ad_state = dict(
                historical_before_state.get("ad_account_state") or {}
            )
            if (
                before_ad_state.get("status") == "not_checked"
                and historical_ad_state
            ):
                before_ad_state = historical_ad_state
                before_state["ad_account_state"] = before_ad_state
                before_state["verified_at"] = str(
                    historical_before_state.get("verified_at")
                    or (dry or {}).get("created_at")
                    or ""
                )
            dry_job_id = str((dry or {}).get("job_id") or "")
            dry_plan = planned_by_key.get(
                (dry_job_id, source_user_id, connector_id), {}
            )
            dry_desired = dict(dry_plan.get("desired_state") or {})
            planned_username = str(
                dry_desired.get("ad_username")
                or dry_details.get("ad_username")
                or dry_details.get("candidate_mapping", {}).get("ad_username")
                or ""
            )
            if not before_username and planned_username:
                planned_ad_state = states.get(
                    (connector_id, planned_username.lower())
                )
                if planned_ad_state is not None:
                    before_ad_state = planned_ad_state
                    before_state["checked_ad_username"] = planned_username
                    before_state["ad_account_state"] = before_ad_state
                    before_state["verified_at"] = str(
                        before_ad_state.get("verified_at") or ""
                    )
            current_fingerprint = build_identity_preview_fingerprint(
                org_id=org_id,
                source_provider=source_provider,
                connector_id=connector_id,
                source_user_id=source_user_id,
                source_snapshot_fingerprint=snapshot_fingerprint,
                selection_fingerprint=selection_fingerprint,
                config_fingerprint=config_fingerprint,
                mapping_input=preview["mapping_input"],
                candidate_mapping=preview["candidate_mapping"],
                binding_signature=self._binding_signature(
                    user_bindings
                    if connector_conflict or connector_migration_required
                    else exact_bindings
                ),
                ad_state=before_ad_state if before_ad_state.get("status") != "not_checked" else None,
            )
            evidence_fingerprint = str(dry_details.get("preview_fingerprint") or "")
            planned_is_stale = bool(dry and (not evidence_fingerprint or evidence_fingerprint != current_fingerprint))
            planned_after_state = {
                "ad_username": planned_username,
                "binding_source": str(dry_details.get("source") or dry_details.get("rule_source") or ""),
                "operation": str(dry_plan.get("operation_type") or ""),
                "account_state": str(dry_desired.get("planned_account_state") or ""),
                "risks": list(dry_details.get("risks") or []),
                "job_id": dry_job_id,
                "plan_fingerprint": str((dry or {}).get("job_summary", {}).get("plan_fingerprint") or ""),
                "planned_at": str((dry or {}).get("job_ended_at") or (dry or {}).get("created_at") or ""),
                "is_stale": planned_is_stale,
                "result_status": str((dry or {}).get("status") or "not_planned"),
            }

            apply_resolution = latest_apply_resolution.get(
                (source_user_id, connector_id)
            )
            apply_job_id = str((apply_resolution or {}).get("job_id") or "")
            apply_operation = apply_by_key.get(
                (apply_job_id, source_user_id, connector_id), {}
            )
            apply_succeeded = bool(
                apply_resolution
                and str((apply_resolution or {}).get("job_status") or "") == "COMPLETED"
                and str(apply_operation.get("status") or "") == "succeeded"
                and binding
                and binding.is_enabled
                and str(binding.ad_username or "").lower()
                == str(apply_operation.get("target_id") or "").lower()
            )
            post_state = dict((apply_operation.get("details") or {}).get("post_apply_ad_account_state") or {})
            if not post_state:
                post_state = {
                    "status": "exists" if apply_succeeded else "not_checked",
                    "exists": True if apply_succeeded else None,
                    "enabled": True if apply_succeeded else None,
                    "locked": None,
                    "protected": False,
                }
            applied_after_state: dict[str, Any] = {
                "ad_username": str(apply_operation.get("target_id") or "") if apply_succeeded else "",
                "binding_source": str((apply_operation.get("details") or {}).get("binding_resolution", {}).get("source") or "") if apply_succeeded else "",
                "operation": str(apply_operation.get("operation_type") or "") if apply_succeeded else "",
                "result": "succeeded" if apply_succeeded else (
                    "failed" if apply_operation and str(apply_operation.get("status") or "") != "succeeded" else "not_applied"
                ),
                "job_id": apply_job_id if apply_operation else "",
                "applied_at": str(apply_operation.get("created_at") or "") if apply_succeeded else "",
                "post_apply_ad_account_state": post_state,
            }

            risks = list(preview["risks"])
            safe_existing_candidates: dict[str, dict[str, Any]] = {}
            protected_existing_candidates: list[dict[str, Any]] = []
            for candidate in preview.get("resolution_candidates") or []:
                if not candidate.get("allow_existing_match"):
                    continue
                username = str(candidate.get("username") or "").strip()
                state = states.get((connector_id, username.lower())) if username else None
                if not state or state.get("exists") is not True:
                    continue
                if state.get("protected"):
                    protected_existing_candidates.append(candidate)
                    continue
                safe_existing_candidates[username.lower()] = candidate
            existing_match_is_eligible = bool(
                not binding
                and not binding_conflict
                and not connector_conflict
                and not connector_migration_required
            )
            multiple_ad_candidates = bool(
                existing_match_is_eligible and len(safe_existing_candidates) > 1
            )
            safe_existing_match = (
                next(iter(safe_existing_candidates.values()))
                if existing_match_is_eligible and len(safe_existing_candidates) == 1
                else None
            )
            if protected_existing_candidates and existing_match_is_eligible:
                risks.append("protected_account")
            if safe_existing_match and not planned_username:
                matched_username = str(safe_existing_match.get("username") or "")
                matched_state = dict(
                    states.get((connector_id, matched_username.lower()))
                    or before_ad_state
                )
                before_ad_state = matched_state
                before_state["checked_ad_username"] = matched_username
                before_state["ad_account_state"] = matched_state
                before_state["verified_at"] = str(
                    matched_state.get("verified_at") or ""
                )
            effective_username = ""
            resolution_source = "unresolved"
            reason = "No enabled binding or executable candidate is available"
            rule_hits: list[str] = []
            if connector_conflict:
                risks.append("connector_conflict")
                resolution_source = "conflict"
                reason = "The source user belongs to multiple connector roots"
            elif binding_conflict:
                risks.append("multiple_bindings")
                resolution_source = "conflict"
                reason = "Multiple persisted bindings exist for this source identity"
            elif connector_migration_required:
                risks.append("connector_migration_required")
                resolution_source = "conflict"
                reason = "A persisted binding exists under a different connector and requires reviewed migration"
            elif binding and binding.is_enabled:
                effective_username = binding.ad_username
                resolution_source = "manual_binding" if binding.source == "manual" else "existing_binding"
                reason = (
                    "Manual binding overrides the field-generated candidate"
                    if binding.source == "manual"
                    else "Using the enabled persisted automatic binding"
                )
                rule_hits.append(resolution_source)
            elif binding and not binding.is_enabled:
                risks.append("binding_disabled")
                reason = "The persisted binding is disabled and is not effective"
            elif planned_username and not planned_is_stale:
                effective_username = planned_username
                resolution_source = str(dry_details.get("source") or "generated_username")
                reason = str(dry_details.get("explanation") or "Latest Dry Run selected this planned identity")
                rule_hits = list(dry_details.get("rule_hits") or [])
            elif multiple_ad_candidates:
                risks.append("multiple_ad_candidates")
                resolution_source = "conflict"
                reason = "Multiple safe existing AD accounts match this source identity"
            elif safe_existing_match:
                effective_username = str(safe_existing_match.get("username") or "")
                resolution_source = "existing_ad_match"
                reason = "A server-computed identity candidate matches an existing AD account"
                rule_hits = [str(safe_existing_match.get("rule") or "existing_ad_match")]
            elif preview["username"]:
                effective_username = preview["username"]
                candidate_source = str(preview["candidate_mapping"].get("source") or "")
                resolution_source = {
                    "employee_id": "employee_id",
                    "userid": "source_user_id",
                    "email_localpart": "email_localpart",
                    "custom_template": "custom_template",
                }.get(str(preview["mapping_input"].get("method") or ""), "generated_username")
                reason = "Field mapping candidate only; no binding has been applied"
                rule_hits = [candidate_source] if candidate_source else []

            candidate_username = str(preview["username"] or "")
            candidate_ad_state = (
                dict(states.get((connector_id, candidate_username.lower())) or {})
                if candidate_username and ad_states is not None
                else {}
            )
            if not candidate_ad_state:
                candidate_ad_state = {
                    "status": "not_checked",
                    "exists": None,
                    "enabled": None,
                    "locked": None,
                    "protected": False,
                    "verified_at": "",
                }
            if connector_conflict:
                difference_status = "connector_conflict"
            elif binding_conflict:
                difference_status = "multiple_binding_conflict"
            elif connector_migration_required:
                difference_status = "connector_migration_required"
            elif multiple_ad_candidates:
                difference_status = "multiple_ad_candidate_conflict"
            elif before_ad_state.get("protected"):
                difference_status = "protected_account"
                risks.append("protected_account")
            elif binding and binding.source == "manual" and candidate_username.lower() != before_username.lower():
                difference_status = "manual_binding_overrides_candidate"
            elif applied_after_state["result"] == "succeeded":
                difference_status = "applied_" + str(
                    applied_after_state["operation"] or "success"
                )
            elif applied_after_state["result"] == "failed":
                difference_status = "apply_failed"
            elif (
                binding
                and candidate_username
                and candidate_username.lower() != before_username.lower()
            ):
                difference_status = "candidate_differs_from_binding"
            elif planned_is_stale:
                difference_status = "planned_stale"
                risks.append("planned_stale")
            elif planned_username and not before_username:
                difference_status = "planned_create"
            elif planned_username and planned_username.lower() != before_username.lower():
                difference_status = "planned_rebind"
            elif not dry:
                difference_status = "not_dry_run"
            elif not apply_operation:
                difference_status = "not_applied"
            else:
                difference_status = "no_change"
            if before_ad_state.get("status") == "missing" and before_username:
                risks.append("ad_account_missing")
            elif before_ad_state.get("status") in {"unavailable", "not_checked"}:
                risks.append("ad_state_unknown")
            if candidate_username and before_username and candidate_username.lower() != before_username.lower():
                risks.append("candidate_differs_from_binding")

            relationships.append(
                IdentityRelationshipPreview(
                    org_id=org_id,
                    source_provider=source_provider,
                    connector_id=connector_id,
                    source_user_id=source_user_id,
                    source_display_name=str(row.get("display_name") or ""),
                    employee_id=str(row.get("employee_id") or ""),
                    source_user={
                        "source_user_id": source_user_id,
                        "display_name": str(row.get("display_name") or ""),
                        "employee_id": str(row.get("employee_id") or ""),
                        "provider": source_provider,
                        "connector_id": connector_id,
                        "account_status": str(row.get("account_status") or ""),
                    },
                    mapping_input=dict(preview["mapping_input"]),
                    candidate_mapping=dict(preview["candidate_mapping"]),
                    before_state=before_state,
                    planned_after_state=planned_after_state,
                    applied_after_state=applied_after_state,
                    effective_ad_username=effective_username,
                    effective_resolution_source=resolution_source,
                    resolution_reason=reason,
                    rule_hits=rule_hits,
                    difference={
                        "status": difference_status,
                        "changed": difference_status not in {"no_change", "not_dry_run", "not_applied"},
                    },
                    candidate_ad_state=candidate_ad_state,
                    risks=sorted(set(risks)),
                    evidence={
                        "preview_fingerprint": current_fingerprint,
                        "source_snapshot_fingerprint": snapshot_fingerprint,
                        "selection_fingerprint": selection_fingerprint,
                        "config_fingerprint": config_fingerprint,
                        "dry_run_evidence_at": str((dry or {}).get("created_at") or ""),
                        "apply_evidence_at": str(apply_operation.get("created_at") or ""),
                    },
                    updated_at=max(
                        [
                            str(getattr(item, "updated_at", "") or "")
                            for item in user_bindings
                        ]
                        + [str((dry or {}).get("created_at") or ""), str(apply_operation.get("created_at") or "")]
                    ),
                    ad_query_usernames=ad_query_usernames,
                )
            )
        candidate_users: dict[str, list[IdentityRelationshipPreview]] = {}
        for item in relationships:
            candidate = str(item.candidate_mapping.get("ad_username") or "").strip().lower()
            if candidate:
                candidate_users.setdefault(candidate, []).append(item)
        for _candidate, matching_items in candidate_users.items():
            if len(matching_items) <= 1:
                continue
            for item in matching_items:
                if "normalized_username_collision" not in item.risks:
                    item.risks.append("normalized_username_collision")
                    item.risks.sort()
                if (
                    not item.before_state.get("bound_ad_username")
                    and item.effective_resolution_source != "conflict"
                ):
                    item.effective_ad_username = ""
                    item.effective_resolution_source = "conflict"
                    item.resolution_reason = (
                        "The field-generated candidate is shared by multiple source users"
                    )
                    item.difference = {
                        "status": "multiple_candidate_conflict",
                        "changed": True,
                    }
        for item in relationships:
            if item.source_user_id not in known_candidate_collisions:
                continue
            if "normalized_username_collision" not in item.risks:
                item.risks.append("normalized_username_collision")
                item.risks.sort()
            if (
                not item.before_state.get("bound_ad_username")
                and item.effective_resolution_source != "conflict"
            ):
                item.effective_ad_username = ""
                item.effective_resolution_source = "conflict"
                item.resolution_reason = (
                    "The field-generated candidate is shared by multiple source users"
                )
                item.difference = {
                    "status": "multiple_candidate_conflict",
                    "changed": True,
                }
        for item in relationships:
            item.creation_eligibility = self._build_creation_eligibility(item)
        return relationships

    @staticmethod
    def _build_creation_eligibility(
        item: IdentityRelationshipPreview,
    ) -> dict[str, Any]:
        candidate = str(item.candidate_mapping.get("ad_username") or "").strip()
        state = dict(item.candidate_ad_state or {})
        status = str(state.get("status") or "not_checked")
        before = item.before_state
        bound_username = str(before.get("bound_ad_username") or "").strip()
        target = candidate

        def result(code: str, reason: str, *, eligible: bool = False) -> dict[str, Any]:
            return {
                "eligible": eligible,
                "status": code,
                "reason": reason,
                "target_ad_username": target,
            }

        if not candidate:
            return result(
                "mapping_required",
                "Save a valid candidate mapping before selecting account creation.",
            )
        if status == "not_checked":
            return result(
                "verification_required",
                "Verify the candidate AD account before selecting account creation.",
            )
        if status == "unavailable":
            return result(
                "verification_unavailable",
                "The candidate AD account could not be verified.",
            )
        if status == "protected" or bool(state.get("protected")):
            return result(
                "protected_account",
                "The candidate AD account is protected and cannot be selected for creation.",
            )
        if state.get("exists") is True:
            return result(
                "candidate_exists",
                "The candidate AD account already exists.",
            )
        if status != "missing" or state.get("exists") is not False:
            return result(
                "verification_required",
                "Verify the candidate AD account before selecting account creation.",
            )
        if "normalized_username_collision" in item.risks:
            return result(
                "candidate_collision",
                "Resolve the candidate username collision before selecting account creation.",
            )
        if (
            item.effective_resolution_source == "conflict"
            or any("conflict" in risk for risk in item.risks)
        ):
            return result(
                "identity_conflict",
                "Resolve the identity or connector conflict before selecting account creation.",
            )
        checked_username = str(before.get("checked_ad_username") or "").strip()
        if (
            item.effective_resolution_source == "existing_ad_match"
            or (
                checked_username
                and checked_username.lower() != candidate.lower()
                and before.get("ad_account_state", {}).get("exists") is True
            )
        ):
            return result(
                "existing_identity_match",
                "Review the existing AD identity match before creating another account.",
            )
        if bound_username and not bool(before.get("binding_enabled")):
            return result(
                "binding_disabled",
                "Enable or remove the disabled saved binding before selecting account creation.",
            )
        if bound_username and bound_username.lower() != candidate.lower():
            return result(
                "binding_review_required",
                "Review or update the saved binding before creating the candidate account.",
            )
        if str(item.source_user.get("account_status") or "").strip().lower() == "inactive":
            return result(
                "source_inactive",
                "The source user is inactive and cannot be selected for account creation.",
            )
        return result(
            "eligible",
            "The candidate AD account was verified as missing and can be prepared for Dry Run.",
            eligible=True,
        )

    @staticmethod
    def matches_filter(item: IdentityRelationshipPreview, status: str) -> bool:
        normalized = str(status or "all").strip().lower()
        before = item.before_state
        planned = item.planned_after_state
        applied = item.applied_after_state
        ad_status = str(before.get("ad_account_state", {}).get("status") or "")
        if normalized in {"", "all"}:
            return True
        predicates = {
            "bound": bool(before.get("bound_ad_username")),
            "unbound": not bool(before.get("bound_ad_username")),
            "candidate_only": bool(item.candidate_mapping.get("ad_username")) and not bool(before.get("bound_ad_username")),
            "manual": before.get("binding_source") == "manual",
            "automatic": bool(before.get("binding_source")) and before.get("binding_source") != "manual",
            "disabled": bool(before.get("bound_ad_username")) and not bool(before.get("binding_enabled")),
            "ad_exists": ad_status in {"exists", "enabled", "disabled", "locked", "protected"},
            "ad_missing": ad_status == "missing",
            "ad_unknown": ad_status in {"unavailable", "not_checked", ""},
            "planned_change": bool(planned.get("ad_username")) and item.difference.get("changed", False),
            "not_applied": applied.get("result") == "not_applied",
            "apply_success": applied.get("result") == "succeeded",
            "apply_failed": applied.get("result") == "failed",
            "conflict": item.effective_resolution_source == "conflict" or any("conflict" in risk for risk in item.risks),
        }
        return bool(predicates.get(normalized, True))

    def build_job_identity_resolutions(
        self,
        job_id: str,
        *,
        org_id: str,
    ) -> list[dict[str, Any]]:
        resolution_rows = self.operation_log_repo.list_identity_resolution_evidence_for_job(
            job_id,
            org_id=org_id,
        )
        planned_rows = self.planned_operation_repo.list_user_operations_for_jobs(
            [job_id]
        )
        apply_rows = self.operation_log_repo.list_user_operation_evidence_for_jobs(
            [job_id],
            org_id=org_id,
        )
        planned_by_source: dict[str, dict[str, Any]] = {}
        for item in planned_rows:
            source_user_id = str(item.get("source_id") or "")
            if not source_user_id:
                continue
            if item.get("operation_type") in {
                "create_user",
                "update_user",
                "reactivate_user",
                "disable_user",
            } or source_user_id not in planned_by_source:
                planned_by_source[source_user_id] = item
        apply_by_source: dict[str, dict[str, Any]] = {}
        for item in apply_rows:
            source_user_id = str(item.get("source_id") or "")
            if source_user_id and source_user_id not in apply_by_source:
                apply_by_source[source_user_id] = item

        output: list[dict[str, Any]] = []
        for row in resolution_rows:
            source_user_id = str(row.get("source_id") or "")
            details = dict(row.get("details") or {})
            planned = planned_by_source.get(source_user_id, {})
            desired = dict(planned.get("desired_state") or {})
            apply_item = apply_by_source.get(source_user_id, {})
            binding_resolution = dict(
                (apply_item.get("details") or {}).get("binding_resolution") or {}
            )
            mapping_input = dict(details.get("mapping_input") or {})
            candidate_mapping = dict(details.get("candidate_mapping") or {})
            before_state = dict(details.get("before_state") or {})
            if not before_state:
                before_state = {
                    "bound_ad_username": "",
                    "binding_source": "",
                    "binding_enabled": False,
                    "ad_account_state": details.get("before_ad_state")
                    or {"status": "not_checked"},
                }
            applied_success = bool(
                str(row.get("execution_mode") or "") == "apply"
                and str(row.get("job_status") or "") == "COMPLETED"
                and str(apply_item.get("status") or "") == "succeeded"
            )
            output.append(
                {
                    "source_display_name": str(
                        details.get("source_display_name") or ""
                    ),
                    "source_user_id": source_user_id,
                    "source_provider": str(
                        details.get("source_provider")
                        or row.get("provider_id")
                        or ""
                    ),
                    "connector_id": str(
                        details.get("connector_id")
                        or row.get("scope_connector_id")
                        or "default"
                    ),
                    "mapping_input": mapping_input,
                    "candidate_mapping": candidate_mapping,
                    "before_state": before_state,
                    "expected_ad_username": str(
                        desired.get("ad_username")
                        or details.get("ad_username")
                        or row.get("target_id")
                        or ""
                    ),
                    "resolution_source": str(
                        details.get("source") or row.get("rule_source") or ""
                    ),
                    "planned_operation": str(planned.get("operation_type") or ""),
                    "result_status": str(row.get("status") or ""),
                    "reason_code": str(row.get("reason_code") or ""),
                    "risks": sorted(
                        set(
                            list(details.get("risks") or [])
                            + ([str(row.get("reason_code"))] if row.get("reason_code") else [])
                        )
                    ),
                    "actual_ad_username": str(apply_item.get("target_id") or "")
                    if applied_success
                    else "",
                    "actual_operation": str(apply_item.get("operation_type") or "")
                    if applied_success
                    else "",
                    "apply_result": "succeeded"
                    if applied_success
                    else (
                        "failed"
                        if apply_item and str(apply_item.get("status") or "") != "succeeded"
                        else "not_applied"
                    ),
                    "post_apply_ad_account_state": dict(
                        (apply_item.get("details") or {}).get(
                            "post_apply_ad_account_state"
                        )
                        or {}
                    ),
                    "completed_at": str(apply_item.get("created_at") or "")
                    if applied_success
                    else "",
                    "binding_resolution": binding_resolution or details,
                }
            )
        return output


__all__ = [
    "IDENTITY_WORKBENCH_QUEUES",
    "IdentityRelationshipPreview",
    "IdentityRelationshipPreviewService",
    "classify_identity_relationship",
    "filter_identity_workbench_rows",
    "build_identity_preview_fingerprint",
    "build_runtime_identity_evidence",
    "summarize_identity_workbench_rows",
]
