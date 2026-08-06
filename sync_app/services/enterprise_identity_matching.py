from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Optional

from sync_app.core.fingerprints import fingerprint_json
from sync_app.core.models import (
    ADAccountRecord,
    IdentityAccountLinkRecord,
    IdentityMatchCandidateRecord,
    IdentityMatchRuleRecord,
    PlatformAccountRecord,
)


RESULT_AUTOMATIC = "automatic_link"
RESULT_SUGGESTED = "suggested_link"
RESULT_MANUAL = "manual_confirmation"
RESULT_BLOCKED = "blocked"

RESULT_LEVELS = (
    RESULT_AUTOMATIC,
    RESULT_SUGGESTED,
    RESULT_MANUAL,
    RESULT_BLOCKED,
)

AUTO_LINK_SOURCE_FIELDS = frozenset({"employee_id", "employee_number"})
NON_PERSON_ACCOUNT_TYPES = frozenset({"service", "shared", "test"})
ACTIVE_ACCOUNT_STATUSES = frozenset({"active", "enabled", "employed", "onboarding"})


def identity_candidate_requires_decision(candidate: Any) -> bool:
    """Return whether a pending match candidate must block rollout.

    A source identity with a present, unique required identifier and no AD hit
    is an expected new-account plan, not an ambiguous identity decision.  It
    remains visible in Dry Run, while duplicates, suggestions, protected
    accounts, and other manual cases stay fail-closed.
    """

    def value(name: str) -> str:
        if isinstance(candidate, dict):
            return str(candidate.get(name) or "").strip().lower()
        return str(getattr(candidate, name, "") or "").strip().lower()

    if value("status") != "pending":
        return False
    result_level = value("result_level")
    if result_level == RESULT_MANUAL:
        return value("recommended_action") != "create_new_ad_account"
    return result_level in {RESULT_BLOCKED, RESULT_SUGGESTED}


def _plain_text(value: Any) -> str:
    return str(value or "").strip()


def _email_localpart(value: Any) -> str:
    normalized = _plain_text(value)
    return normalized.split("@", 1)[0] if "@" in normalized else ""


def _strip_phone_country_code(value: str) -> str:
    digits = re.sub(r"\D", "", value)
    if digits.startswith("0086") and len(digits) > 11:
        digits = digits[4:]
    elif digits.startswith("86") and len(digits) > 11:
        digits = digits[2:]
    return digits


def normalize_rule_value(value: Any, rule: IdentityMatchRuleRecord) -> str:
    normalized = str(value or "")
    if rule.trim_whitespace:
        normalized = "".join(normalized.split())
    else:
        normalized = normalized.strip()
    if rule.strip_phone_country_code:
        normalized = _strip_phone_country_code(normalized)
    if rule.lowercase_email or not rule.case_sensitive:
        normalized = normalized.casefold()
    return normalized


def platform_field(account: PlatformAccountRecord, field_name: str) -> Any:
    if field_name == "email_localpart":
        return _email_localpart(account.email)
    if hasattr(account, field_name):
        return getattr(account, field_name)
    direct = account.custom_fields.get(field_name, account.raw_payload.get(field_name, ""))
    if direct not in (None, "") or "." not in field_name:
        return direct
    current: Any = account.raw_payload
    for segment in field_name.split("."):
        if not isinstance(current, dict) or segment not in current:
            return ""
        current = current[segment]
    return current


def ad_field(account: ADAccountRecord, field_name: str) -> Any:
    if field_name.startswith("extensionAttribute"):
        return account.extension_attributes.get(field_name, "")
    if hasattr(account, field_name):
        return getattr(account, field_name)
    return ""


@dataclass(slots=True)
class IdentityMatchAssessment:
    platform_account_id: int
    ad_account_id: Optional[int]
    proposed_identity_id: str
    result_level: str
    confidence: int
    matched_rule_ids: list[int] = field(default_factory=list)
    matched_fields: dict[str, Any] = field(default_factory=dict)
    unmatched_fields: dict[str, Any] = field(default_factory=dict)
    conflict_fields: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    recommended_action: str = ""

    def to_candidate(self, *, run_id: str, org_id: str) -> IdentityMatchCandidateRecord:
        payload = asdict(self)
        candidate_fingerprint = fingerprint_json(
            {
                "run_id": run_id,
                "org_id": org_id,
                **payload,
            },
            namespace="identity-match-candidate",
        )
        return IdentityMatchCandidateRecord(
            run_id=run_id,
            org_id=org_id,
            platform_account_id=self.platform_account_id,
            ad_account_id=self.ad_account_id,
            proposed_identity_id=self.proposed_identity_id,
            result_level=self.result_level,
            confidence=self.confidence,
            matched_rule_ids=list(self.matched_rule_ids),
            matched_fields=dict(self.matched_fields),
            unmatched_fields=dict(self.unmatched_fields),
            conflict_fields=dict(self.conflict_fields),
            evidence=dict(self.evidence),
            recommended_action=self.recommended_action,
            candidate_fingerprint=candidate_fingerprint,
        )


def _active_platform_links_by_account(
    links: Iterable[IdentityAccountLinkRecord],
) -> dict[int, IdentityAccountLinkRecord]:
    return {
        int(link.platform_account_id): link
        for link in links
        if link.status == "active"
        and link.account_kind == "platform"
        and link.platform_account_id is not None
    }


def _active_ad_links_by_account(
    links: Iterable[IdentityAccountLinkRecord],
) -> dict[int, IdentityAccountLinkRecord]:
    return {
        int(link.ad_account_id): link
        for link in links
        if link.status == "active"
        and link.account_kind == "ad"
        and link.ad_account_id is not None
    }


def _primary_ad_links_by_identity(
    links: Iterable[IdentityAccountLinkRecord],
) -> dict[str, list[IdentityAccountLinkRecord]]:
    output: dict[str, list[IdentityAccountLinkRecord]] = defaultdict(list)
    for link in links:
        if (
            link.status == "active"
            and link.account_kind == "ad"
            and link.account_role == "primary_ad"
            and link.ad_account_id is not None
        ):
            output[link.identity_id].append(link)
    return output


def _source_uniqueness_counts(
    accounts: Iterable[PlatformAccountRecord],
    rules: Iterable[IdentityMatchRuleRecord],
) -> dict[tuple[int, str], int]:
    result: dict[tuple[int, str], int] = {}
    account_rows = list(accounts)
    for rule in rules:
        if rule.id is None:
            continue
        counts: Counter[tuple[str, str, str]] = Counter()
        normalized_by_id: dict[int, str] = {}
        for account in account_rows:
            if rule.source_provider not in {"*", account.provider_id}:
                continue
            normalized = normalize_rule_value(platform_field(account, rule.source_field), rule)
            if not normalized or account.id is None:
                continue
            normalized_by_id[int(account.id)] = normalized
            counts[(account.provider_id, account.connector_id, normalized)] += 1
        for account in account_rows:
            if account.id is None:
                continue
            normalized = normalized_by_id.get(int(account.id), "")
            result[(int(account.id), str(rule.id))] = (
                counts[(account.provider_id, account.connector_id, normalized)]
                if normalized
                else 0
            )
    return result


def _ad_rule_index(
    accounts: Iterable[ADAccountRecord],
    rules: Iterable[IdentityMatchRuleRecord],
) -> dict[tuple[str, str, str], list[ADAccountRecord]]:
    result: dict[tuple[str, str, str], list[ADAccountRecord]] = defaultdict(list)
    for rule in rules:
        if rule.id is None:
            continue
        for account in accounts:
            normalized = normalize_rule_value(ad_field(account, rule.ad_field), rule)
            if normalized:
                result[(account.connector_id, str(rule.id), normalized)].append(account)
    return result


def _multi_platform_group_conflicts(
    account: PlatformAccountRecord,
    accounts: Iterable[PlatformAccountRecord],
) -> dict[str, Any]:
    employee_id = _plain_text(account.employee_id).casefold()
    if not employee_id:
        return {}
    peers = [
        item
        for item in accounts
        if item.id != account.id
        and _plain_text(item.employee_id).casefold() == employee_id
        and item.provider_id != account.provider_id
    ]
    conflicts: dict[str, Any] = {}
    for field_name in ("display_name", "email", "mobile", "account_status"):
        values = {
            _plain_text(getattr(item, field_name)).casefold()
            for item in [account, *peers]
            if _plain_text(getattr(item, field_name))
        }
        if len(values) > 1:
            conflicts[field_name] = {
                "reason": "multi_platform_field_conflict",
                "values": sorted(values),
            }
    return conflicts


def assess_identity_matches(
    platform_accounts: Iterable[PlatformAccountRecord],
    ad_accounts: Iterable[ADAccountRecord],
    rules: Iterable[IdentityMatchRuleRecord],
    links: Iterable[IdentityAccountLinkRecord] = (),
) -> list[IdentityMatchAssessment]:
    """Build fail-closed, explainable match conclusions without writing links."""

    source_accounts = [item for item in platform_accounts if item.id is not None]
    directory_accounts = [item for item in ad_accounts if item.id is not None]
    enabled_rules = sorted(
        [item for item in rules if item.is_enabled and item.id is not None],
        key=lambda item: (item.rule_order, int(item.id or 0)),
    )
    active_links = [item for item in links if item.status == "active"]
    platform_links = _active_platform_links_by_account(active_links)
    ad_links = _active_ad_links_by_account(active_links)
    primary_links = _primary_ad_links_by_identity(active_links)
    source_by_id = {
        int(item.id): item for item in source_accounts if item.id is not None
    }
    source_accounts_by_identity: dict[str, list[PlatformAccountRecord]] = defaultdict(list)
    for linked_account_id, link in platform_links.items():
        linked_account = source_by_id.get(linked_account_id)
        if linked_account is not None:
            source_accounts_by_identity[link.identity_id].append(linked_account)
    ad_by_id = {int(item.id): item for item in directory_accounts if item.id is not None}
    source_counts = _source_uniqueness_counts(source_accounts, enabled_rules)
    target_index = _ad_rule_index(directory_accounts, enabled_rules)
    assessments: list[IdentityMatchAssessment] = []

    for source_account in source_accounts:
        source_id = int(source_account.id or 0)
        existing_link = platform_links.get(source_id)
        pending_creation_identity_id = ""
        multi_platform_conflicts = _multi_platform_group_conflicts(
            source_account, source_accounts
        )
        base_evidence = {
            "source": {
                "provider_id": source_account.provider_id,
                "connector_id": source_account.connector_id,
                "platform_account_id": source_account.platform_account_id,
                "employee_id": source_account.employee_id,
                "account_status": source_account.account_status,
                "account_type": source_account.account_type,
            },
            "stable_source_id": source_account.platform_account_id,
        }

        if source_account.is_excluded or source_account.account_type in NON_PERSON_ACCOUNT_TYPES:
            assessments.append(
                IdentityMatchAssessment(
                    platform_account_id=source_id,
                    ad_account_id=None,
                    proposed_identity_id=existing_link.identity_id if existing_link else "",
                    result_level=RESULT_BLOCKED,
                    confidence=100,
                    conflict_fields={
                        "account_type": {
                            "reason": "non_person_account_requires_explicit_maintenance",
                            "value": source_account.account_type,
                        }
                    },
                    evidence=base_evidence,
                    recommended_action="maintain_explicitly_or_exclude",
                )
            )
            continue

        if existing_link:
            target_links = primary_links.get(existing_link.identity_id, [])
            is_pending_account_creation = bool(
                not target_links
                and existing_link.source == "identity_match_decision"
                and str(existing_link.evidence.get("decision") or "").strip()
                == "create_new_ad_account"
            )
            if is_pending_account_creation:
                pending_creation_identity_id = existing_link.identity_id
                base_evidence["pending_creation_platform_link"] = (
                    existing_link.to_dict()
                )
            elif len(target_links) != 1:
                assessments.append(
                    IdentityMatchAssessment(
                        platform_account_id=source_id,
                        ad_account_id=(
                            int(target_links[0].ad_account_id or 0)
                            if len(target_links) == 1
                            else None
                        ),
                        proposed_identity_id=existing_link.identity_id,
                        result_level=RESULT_BLOCKED,
                        confidence=100,
                        conflict_fields={
                            "primary_ad": {
                                "reason": "permanent_identity_requires_exactly_one_primary_ad",
                                "count": len(target_links),
                            }
                        },
                        evidence={
                            **base_evidence,
                            "permanent_platform_link": existing_link.to_dict(),
                        },
                        recommended_action="repair_permanent_identity_link",
                    )
                )
                continue
            else:
                target_link = target_links[0]
                target_id = int(target_link.ad_account_id or 0)
                target = ad_by_id.get(target_id)
                if target is None or target.is_protected:
                    assessments.append(
                        IdentityMatchAssessment(
                            platform_account_id=source_id,
                            ad_account_id=target_id or None,
                            proposed_identity_id=existing_link.identity_id,
                            result_level=RESULT_BLOCKED,
                            confidence=100,
                            conflict_fields={
                                "primary_ad": {
                                    "reason": "permanent_target_missing_or_protected"
                                }
                            },
                            evidence={
                                **base_evidence,
                                "permanent_platform_link": existing_link.to_dict(),
                                "permanent_ad_link": target_link.to_dict(),
                            },
                            recommended_action="repair_permanent_identity_link",
                        )
                    )
                    continue
                assessments.append(
                    IdentityMatchAssessment(
                        platform_account_id=source_id,
                        ad_account_id=target_id,
                        proposed_identity_id=existing_link.identity_id,
                        result_level=RESULT_AUTOMATIC,
                        confidence=100,
                        matched_fields={
                            "permanent_link": {
                                "source": existing_link.source,
                                "association_type": existing_link.association_type,
                            }
                        },
                        evidence={
                            **base_evidence,
                            "permanent_platform_link": existing_link.to_dict(),
                            "permanent_ad_link": target_link.to_dict(),
                            "stable_target_id": target.object_guid,
                        },
                        recommended_action="reuse_permanent_link",
                    )
                )
                continue

        required_missing: dict[str, Any] = {}
        duplicate_source_fields: dict[str, Any] = {}
        target_hits: dict[int, list[tuple[IdentityMatchRuleRecord, str]]] = defaultdict(list)
        unmatched_fields: dict[str, Any] = {}
        ambiguous_targets: dict[str, Any] = {}

        for rule in enabled_rules:
            if rule.source_provider not in {"*", source_account.provider_id}:
                continue
            source_value = normalize_rule_value(
                platform_field(source_account, rule.source_field), rule
            )
            if not source_value:
                unmatched_fields[rule.source_field] = {"reason": "source_value_missing"}
                if rule.is_required:
                    required_missing[rule.source_field] = {
                        "reason": "required_match_field_missing",
                        "rule_id": rule.id,
                    }
                continue
            source_occurrences = source_counts.get((source_id, str(rule.id)), 0)
            if source_occurrences > 1:
                duplicate_source_fields[rule.source_field] = {
                    "reason": "duplicate_source_value",
                    "count": source_occurrences,
                    "rule_id": rule.id,
                }
                if rule.stop_on_conflict:
                    continue
            matches = target_index.get(
                (source_account.connector_id, str(rule.id), source_value), []
            )
            if len(matches) > 1:
                ambiguous_targets[rule.ad_field] = {
                    "reason": "duplicate_ad_value",
                    "count": len(matches),
                    "rule_id": rule.id,
                    "candidate_account_ids": [int(item.id or 0) for item in matches],
                }
                if rule.stop_on_conflict:
                    continue
            if len(matches) == 1:
                target_hits[int(matches[0].id or 0)].append((rule, source_value))
            else:
                unmatched_fields[rule.source_field] = {
                    "reason": "no_unique_ad_match",
                    "rule_id": rule.id,
                }

        hard_conflicts = {
            **required_missing,
            **duplicate_source_fields,
            **ambiguous_targets,
        }
        if hard_conflicts:
            assessments.append(
                IdentityMatchAssessment(
                    platform_account_id=source_id,
                    ad_account_id=None,
                    proposed_identity_id="",
                    result_level=RESULT_BLOCKED,
                    confidence=0,
                    unmatched_fields=unmatched_fields,
                    conflict_fields=hard_conflicts,
                    evidence=base_evidence,
                    recommended_action="repair_source_or_ad_duplicates",
                )
            )
            continue
        if len(target_hits) > 1:
            assessments.append(
                IdentityMatchAssessment(
                    platform_account_id=source_id,
                    ad_account_id=None,
                    proposed_identity_id="",
                    result_level=RESULT_BLOCKED,
                    confidence=0,
                    unmatched_fields=unmatched_fields,
                    conflict_fields={
                        "rule_targets": {
                            "reason": "different_rules_resolved_different_ad_accounts",
                            "candidate_account_ids": sorted(target_hits),
                        }
                    },
                    evidence=base_evidence,
                    recommended_action="confirm_target_manually",
                )
            )
            continue
        if not target_hits:
            assessments.append(
                IdentityMatchAssessment(
                    platform_account_id=source_id,
                    ad_account_id=None,
                    proposed_identity_id=pending_creation_identity_id,
                    result_level=RESULT_MANUAL,
                    confidence=0,
                    unmatched_fields=unmatched_fields,
                    conflict_fields=multi_platform_conflicts,
                    evidence=base_evidence,
                    recommended_action=(
                        "resolve_multi_platform_fields"
                        if multi_platform_conflicts
                        else "create_new_ad_account"
                    ),
                )
            )
            continue

        target_id, rule_hits = next(iter(target_hits.items()))
        target = ad_by_id[target_id]
        target_link = ad_links.get(target_id)
        proposed_identity_id = (
            target_link.identity_id
            if target_link
            else pending_creation_identity_id
        )
        matched_fields = {
            rule.source_field: {
                "rule_id": rule.id,
                "rule_name": rule.rule_name,
                "ad_field": rule.ad_field,
                "normalized_value": value,
            }
            for rule, value in rule_hits
        }
        if target.is_protected or target.account_type in NON_PERSON_ACCOUNT_TYPES:
            assessments.append(
                IdentityMatchAssessment(
                    platform_account_id=source_id,
                    ad_account_id=target_id,
                    proposed_identity_id=proposed_identity_id,
                    result_level=RESULT_BLOCKED,
                    confidence=0,
                    matched_rule_ids=[int(rule.id or 0) for rule, _ in rule_hits],
                    matched_fields=matched_fields,
                    unmatched_fields=unmatched_fields,
                    conflict_fields={
                        "ad_account_type": {
                            "reason": "protected_or_non_person_ad_account",
                            "value": target.account_type,
                        }
                    },
                    evidence={**base_evidence, "stable_target_id": target.object_guid},
                    recommended_action="select_another_ad_account",
                )
            )
            continue
        corroborated_existing_identity = False
        if target_link:
            linked_source_accounts = source_accounts_by_identity.get(
                target_link.identity_id, []
            )
            source_employee_id = _plain_text(source_account.employee_id).casefold()
            corroborated_existing_identity = bool(
                source_employee_id
                and linked_source_accounts
                and all(
                    _plain_text(item.employee_id).casefold() == source_employee_id
                    for item in linked_source_accounts
                )
            )
        confidence = max(rule.confidence_score for rule, _ in rule_hits)
        auto_rules = [
            rule
            for rule, _ in rule_hits
            if rule.allow_auto_link
            and rule.source_field in AUTO_LINK_SOURCE_FIELDS
            and source_counts.get((source_id, str(rule.id)), 0) == 1
        ]
        is_active_source = source_account.account_status.casefold() in ACTIVE_ACCOUNT_STATUSES
        status_conflict = bool(is_active_source and target.account_enabled is False)
        result_level = RESULT_AUTOMATIC if auto_rules else RESULT_SUGGESTED
        recommended_action = "establish_permanent_link" if auto_rules else "confirm_suggested_link"
        conflict_fields = dict(multi_platform_conflicts)
        if status_conflict:
            conflict_fields["account_status"] = {
                "reason": "active_source_matches_disabled_ad_account",
                "source": source_account.account_status,
                "target_enabled": False,
            }
            result_level = RESULT_MANUAL
            recommended_action = "verify_rehire_or_historical_account"
        if multi_platform_conflicts:
            result_level = RESULT_MANUAL
            recommended_action = "resolve_multi_platform_fields"
        if target_link and not corroborated_existing_identity:
            result_level = RESULT_BLOCKED
            conflict_fields["ad_ownership"] = {
                "reason": "ad_account_already_linked_to_another_identity",
                "identity_id": target_link.identity_id,
            }
            recommended_action = "select_another_ad_account_or_review_existing_identity"
        elif target_link and corroborated_existing_identity:
            proposed_identity_id = target_link.identity_id
            recommended_action = (
                "merge_platform_account_into_existing_identity"
                if result_level == RESULT_AUTOMATIC
                else "confirm_merge_into_existing_identity"
            )
            base_evidence["corroborating_platform_account_ids"] = [
                int(item.id or 0)
                for item in source_accounts_by_identity.get(target_link.identity_id, [])
            ]
        assessments.append(
            IdentityMatchAssessment(
                platform_account_id=source_id,
                ad_account_id=target_id,
                proposed_identity_id=proposed_identity_id,
                result_level=result_level,
                confidence=confidence,
                matched_rule_ids=[int(rule.id or 0) for rule, _ in rule_hits],
                matched_fields=matched_fields,
                unmatched_fields=unmatched_fields,
                conflict_fields=conflict_fields,
                evidence={
                    **base_evidence,
                    "stable_target_id": target.object_guid,
                    "target_sam_account_name": target.sam_account_name,
                    "target_ou_path": target.ou_path,
                    "target_account_enabled": target.account_enabled,
                },
                recommended_action=recommended_action,
            )
        )

    return assessments


class EnterpriseIdentityMatchingService:
    def __init__(
        self,
        *,
        platform_account_repo: Any,
        ad_account_repo: Any,
        identity_repo: Any,
        rule_repo: Any,
        run_repo: Any,
    ) -> None:
        self.platform_account_repo = platform_account_repo
        self.ad_account_repo = ad_account_repo
        self.identity_repo = identity_repo
        self.rule_repo = rule_repo
        self.run_repo = run_repo

    def run(
        self,
        *,
        org_id: str,
        source_snapshot_ids: Iterable[int] = (),
        ad_snapshot_id: Optional[int] = None,
        config_fingerprint: str = "",
        created_by: str = "",
    ) -> dict[str, Any]:
        self.rule_repo.seed_defaults(org_id=org_id, created_by=created_by or "system")
        rules = self.rule_repo.list_enabled_rules(org_id=org_id)
        rules_fingerprint = fingerprint_json(
            [rule.to_dict() for rule in rules],
            namespace="identity-match-rules",
        )
        run_id = self.run_repo.create_run(
            org_id=org_id,
            source_snapshot_ids=source_snapshot_ids,
            ad_snapshot_id=ad_snapshot_id,
            rules_fingerprint=rules_fingerprint,
            config_fingerprint=config_fingerprint,
            created_by=created_by,
        )
        assessments = assess_identity_matches(
            self.platform_account_repo.list_accounts(org_id=org_id),
            self.ad_account_repo.list_accounts(org_id=org_id),
            rules,
            self.identity_repo.list_links(org_id=org_id),
        )
        counts = Counter(item.result_level for item in assessments)
        summary = {
            "total": len(assessments),
            "automatic_link": counts[RESULT_AUTOMATIC],
            "suggested_link": counts[RESULT_SUGGESTED],
            "manual_confirmation": counts[RESULT_MANUAL],
            "blocked": counts[RESULT_BLOCKED],
            "rules_fingerprint": rules_fingerprint,
        }
        candidates = self.run_repo.replace_candidates(
            run_id,
            org_id=org_id,
            candidates=[
                assessment.to_candidate(run_id=run_id, org_id=org_id)
                for assessment in assessments
            ],
            summary=summary,
        )
        return {
            "run_id": run_id,
            "summary": summary,
            "candidates": [candidate.to_dict() for candidate in candidates],
        }


class EnterpriseIdentityDecisionService:
    """Revalidate immutable evidence, then commit one reviewed decision atomically."""

    def __init__(
        self,
        *,
        platform_account_repo: Any,
        ad_account_repo: Any,
        identity_repo: Any,
        rule_repo: Any,
        decision_repo: Any,
    ) -> None:
        self.platform_account_repo = platform_account_repo
        self.ad_account_repo = ad_account_repo
        self.identity_repo = identity_repo
        self.rule_repo = rule_repo
        self.decision_repo = decision_repo

    def decide(
        self,
        *,
        org_id: str,
        candidate_id: int,
        candidate_fingerprint: str,
        decision: str,
        request_id: str,
        decided_by: str,
        reason: str = "",
    ) -> dict[str, Any]:
        existing = self.decision_repo.get_decision_by_request_id(
            request_id, org_id=org_id
        )
        if existing is not None:
            persisted = self.decision_repo.apply_decision(
                org_id=org_id,
                candidate_id=candidate_id,
                expected_fingerprint=candidate_fingerprint,
                decision=decision,
                request_id=request_id,
                decided_by=decided_by,
                reason=reason,
            )
            return persisted.to_dict()

        candidate = self.decision_repo.get_candidate(candidate_id, org_id=org_id)
        if candidate is None:
            raise ValueError("match candidate does not exist in this organization")
        if candidate.candidate_fingerprint != str(candidate_fingerprint or "").strip():
            raise ValueError("match candidate evidence changed; run matching again")

        rules = self.rule_repo.list_enabled_rules(org_id=org_id)
        current_assessments = assess_identity_matches(
            self.platform_account_repo.list_accounts(org_id=org_id),
            self.ad_account_repo.list_accounts(org_id=org_id),
            rules,
            self.identity_repo.list_links(org_id=org_id),
        )
        current = next(
            (
                item
                for item in current_assessments
                if item.platform_account_id == candidate.platform_account_id
            ),
            None,
        )
        if current is None:
            raise ValueError("platform account is no longer eligible for matching")
        current_candidate = current.to_candidate(run_id=candidate.run_id, org_id=org_id)
        if current_candidate.candidate_fingerprint != candidate.candidate_fingerprint:
            raise ValueError("match evidence is stale; run matching again before deciding")

        persisted = self.decision_repo.apply_decision(
            org_id=org_id,
            candidate_id=candidate_id,
            expected_fingerprint=candidate_fingerprint,
            decision=decision,
            request_id=request_id,
            decided_by=decided_by,
            reason=reason,
            decision_payload={
                "result_level": candidate.result_level,
                "recommended_action": candidate.recommended_action,
                "confidence": candidate.confidence,
                "run_id": candidate.run_id,
            },
        )
        return persisted.to_dict()


__all__ = [
    "RESULT_AUTOMATIC",
    "RESULT_SUGGESTED",
    "RESULT_MANUAL",
    "RESULT_BLOCKED",
    "RESULT_LEVELS",
    "IdentityMatchAssessment",
    "EnterpriseIdentityMatchingService",
    "EnterpriseIdentityDecisionService",
    "assess_identity_matches",
    "identity_candidate_requires_decision",
    "normalize_rule_value",
]
