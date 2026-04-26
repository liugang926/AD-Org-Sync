from __future__ import annotations

from typing import Any, Optional


IDENTITY_RULE_PRIORITY = (
    "manual_binding",
    "existing_binding",
    "existing_ad_userid",
    "existing_ad_email_localpart",
    "derived_default_userid",
)

IDENTITY_RULE_PRIORITY_INDEX = {
    rule_name: index
    for index, rule_name in enumerate(IDENTITY_RULE_PRIORITY)
}

CONFIDENCE_ORDER = ("low", "medium", "high")
CONFIDENCE_INDEX = {
    level: index
    for index, level in enumerate(CONFIDENCE_ORDER)
}
DEFAULT_DIRECT_APPLY_MIN_CONFIDENCE = "high"


def _pick_best_candidate(candidates: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    normalized_candidates = [candidate for candidate in candidates if candidate.get("username")]
    if not normalized_candidates:
        return None
    return sorted(
        normalized_candidates,
        key=lambda candidate: (
            IDENTITY_RULE_PRIORITY_INDEX.get(str(candidate.get("rule") or ""), 999),
            str(candidate.get("username") or "").lower(),
        ),
    )[0]


def normalize_recommendation_confidence(confidence: str | None) -> str:
    normalized = str(confidence or "").strip().lower()
    if normalized in CONFIDENCE_INDEX:
        return normalized
    return "medium"


def recommendation_requires_confirmation(
    recommendation: Optional[dict[str, Any]],
    *,
    min_confidence: str = DEFAULT_DIRECT_APPLY_MIN_CONFIDENCE,
) -> bool:
    if not recommendation:
        return False
    recommendation_confidence = normalize_recommendation_confidence(recommendation.get("confidence"))
    threshold_confidence = normalize_recommendation_confidence(min_confidence)
    return CONFIDENCE_INDEX[recommendation_confidence] < CONFIDENCE_INDEX[threshold_confidence]


def _finalize_recommendation(recommendation: dict[str, Any]) -> dict[str, Any]:
    normalized_confidence = normalize_recommendation_confidence(recommendation.get("confidence"))
    finalized = dict(recommendation)
    finalized["confidence"] = normalized_confidence
    finalized["requires_confirmation"] = recommendation_requires_confirmation(
        {"confidence": normalized_confidence}
    )
    return finalized


def recommend_conflict_resolution(conflict: Any) -> Optional[dict[str, Any]]:
    conflict_type = str(getattr(conflict, "conflict_type", "") or "").strip().lower()
    source_id = str(getattr(conflict, "source_id", "") or "").strip()
    target_key = str(getattr(conflict, "target_key", "") or "").strip()
    details = getattr(conflict, "details", None) or {}
    if not isinstance(details, dict):
        details = {}

    if conflict_type == "multiple_ad_candidates":
        candidates = list(details.get("candidates") or [])
        best_candidate = _pick_best_candidate(candidates)
        if not best_candidate:
            return _finalize_recommendation({
                "action": "skip_user_sync",
                "label": "Add skip_user_sync",
                "reason": f"No stable AD candidate is available for {source_id}; skip this user until identity is clarified.",
                "confidence": "medium",
            })

        candidate_rule = str(best_candidate.get("rule") or "")
        candidate_username = str(best_candidate.get("username") or "")
        if candidate_rule == "existing_ad_userid":
            reason = f"Prefer {candidate_username} because it matches the source user ID directly."
            confidence = "high"
        elif candidate_rule == "existing_ad_email_localpart":
            reason = f"Prefer {candidate_username} because it matches the source email local part."
            confidence = "medium"
        else:
            reason = f"Prefer {candidate_username} because it is the strongest remaining identity match."
            confidence = "medium"
        return _finalize_recommendation({
            "action": "manual_binding",
            "label": "Create manual binding",
            "reason": reason,
            "confidence": confidence,
            "ad_username": candidate_username,
        })

    if conflict_type == "existing_ad_identity_claim_review":
        candidate = details.get("candidate") if isinstance(details.get("candidate"), dict) else {}
        candidate_username = str(candidate.get("username") or target_key or "").strip()
        candidate_rule = str(candidate.get("rule") or "").strip()
        if not candidate_username:
            return _finalize_recommendation({
                "action": "skip_user_sync",
                "label": "Add skip_user_sync",
                "reason": f"No reviewable AD account is available for {source_id}; skip this user until identity is clarified.",
                "confidence": "medium",
            })
        if candidate_rule == "existing_ad_userid":
            reason = f"Bind {source_id} to {candidate_username} because it matches the source user ID directly."
            confidence = "high"
        else:
            reason = f"Bind {source_id} to {candidate_username} after review because it is the configured existing AD match."
            confidence = "medium"
        return _finalize_recommendation({
            "action": "manual_binding",
            "label": "Approve existing AD account claim",
            "reason": reason,
            "confidence": confidence,
            "ad_username": candidate_username,
        })

    if conflict_type == "shared_ad_account":
        related_userids = list(details.get("source_user_ids") or details.get("wecom_userids") or [])
        reason = (
            f"AD account {target_key or '-'} is shared by multiple source users"
            f"{': ' + ', '.join(related_userids) if related_userids else ''}. "
            f"Safest default is to skip syncing {source_id} until a unique AD identity is assigned."
        )
        return _finalize_recommendation({
            "action": "skip_user_sync",
            "label": "Add skip_user_sync",
            "reason": reason,
            "confidence": "high",
        })

    return None
