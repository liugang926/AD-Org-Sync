from __future__ import annotations

from typing import Any


def _localized_message(code: str, **params: Any) -> dict[str, Any]:
    return {"code": code, "params": params}


def build_binding_decision_summary(
    *,
    conflict_type: str,
    source_user_id: str,
    selected_target_username: str,
    target_exists: bool,
    target_enabled: bool | None,
    current_binding_owner: str = "",
    is_protected_account: bool = False,
    shared_source_user_ids: list[str] | None = None,
    rehire_restore_enabled: bool = False,
) -> dict[str, Any]:
    normalized_conflict_type = str(conflict_type or "").strip().lower()
    normalized_source_user_id = str(source_user_id or "").strip()
    normalized_target_username = str(selected_target_username or "").strip()
    normalized_binding_owner = str(current_binding_owner or "").strip()
    related_source_users = [
        str(item or "").strip()
        for item in list(shared_source_user_ids or [])
        if str(item or "").strip()
    ]
    other_shared_users = [
        user_id
        for user_id in related_source_users
        if user_id != normalized_source_user_id
    ]

    bind_now: dict[str, Any]
    if not normalized_target_username:
        bind_now = {
            "status": "error",
            "action": "target_not_selected",
            "label": "Pick a target AD account first",
            "label_code": "conflicts.decision.label.target_not_selected",
            "summary": "No AD account is selected yet, so the binding decision cannot be evaluated.",
            "summary_code": "conflicts.decision.summary.target_not_selected",
            "summary_params": {},
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [
                "Choose one existing AD account before approving a binding decision.",
            ],
            "note_messages": [
                _localized_message("conflicts.decision.note.choose_existing_account"),
            ],
        }
    elif is_protected_account:
        bind_now = {
            "status": "error",
            "action": "protected_account",
            "label": "Protected AD account",
            "label_code": "conflicts.decision.label.protected_account",
            "summary": (
                f"{normalized_target_username} is marked as a protected directory account and should not be managed "
                "through synchronization."
            ),
            "summary_code": "conflicts.decision.summary.protected_account",
            "summary_params": {"target_username": normalized_target_username},
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [
                "Pick a user-managed AD account instead of a protected system identity.",
            ],
            "note_messages": [
                _localized_message(
                    "conflicts.decision.note.choose_unprotected_account"
                ),
            ],
        }
    elif (
        normalized_binding_owner
        and normalized_binding_owner != normalized_source_user_id
    ):
        bind_now = {
            "status": "warning",
            "action": "already_bound_elsewhere",
            "label": "Already bound to another source user",
            "label_code": "conflicts.decision.label.already_bound_elsewhere",
            "summary": (
                f"{normalized_target_username} is already bound to {normalized_binding_owner}, "
                "so binding it here would keep the identity conflict unresolved."
            ),
            "summary_code": "conflicts.decision.summary.already_bound_elsewhere",
            "summary_params": {
                "target_username": normalized_target_username,
                "binding_owner": normalized_binding_owner,
            },
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [
                "Resolve the existing binding first, or choose a different AD account.",
            ],
            "note_messages": [
                _localized_message("conflicts.decision.note.resolve_existing_binding"),
            ],
        }
    else:
        if target_exists:
            if target_enabled is False and rehire_restore_enabled:
                action = "reactivate_user"
                label = "Reactivate and update existing AD account"
                label_code = "conflicts.decision.label.reactivate_user"
            else:
                action = "update_user"
                label = "Update existing AD account"
                label_code = "conflicts.decision.label.update_user"
        else:
            action = "create_user"
            label = "Create new managed AD account"
            label_code = "conflicts.decision.label.create_user"

        bind_now = {
            "status": "success",
            "action": action,
            "label": label,
            "label_code": label_code,
            "summary": (
                f"The next sync should {label.lower()} {normalized_target_username} "
                "under the current field ownership and OU placement rules."
            ),
            "summary_code": f"conflicts.decision.summary.{action}",
            "summary_params": {"target_username": normalized_target_username},
            "will_create_new_account": not target_exists,
            "will_conflict_continue": False,
            "notes": [],
            "note_messages": [],
        }
        if target_exists and target_enabled is False and not rehire_restore_enabled:
            bind_now["status"] = "warning"
            bind_now["notes"].append(
                "The account is currently disabled, and automatic reactivation is off, so this will stay an update plan."
            )
            bind_now["note_messages"].append(
                _localized_message(
                    "conflicts.decision.note.disabled_without_reactivation"
                )
            )
        if normalized_conflict_type == "shared_ad_account" and other_shared_users:
            bind_now["status"] = "warning"
            bind_now["will_conflict_continue"] = True
            bind_now["notes"].append(
                "This AD account is still shared with "
                + ", ".join(sorted(other_shared_users))
                + ", so binding it here does not remove the shared-account risk."
            )
            bind_now["note_messages"].append(
                _localized_message(
                    "conflicts.decision.note.shared_account_risk",
                    shared_users=", ".join(sorted(other_shared_users)),
                )
            )
        elif normalized_conflict_type == "multiple_ad_candidates":
            bind_now["notes"].append(
                "Choosing one concrete AD account should clear this user's candidate ambiguity on the next sync run."
            )
            bind_now["note_messages"].append(
                _localized_message("conflicts.decision.note.clear_candidate_ambiguity")
            )
        elif normalized_conflict_type == "existing_ad_identity_claim_review":
            bind_now["notes"].append(
                "Approving this claim writes a manual binding, so the next sync can update the existing AD account "
                "instead of creating a duplicate managed account."
            )
            bind_now["note_messages"].append(
                _localized_message("conflicts.decision.note.approve_existing_claim")
            )
        else:
            bind_now["notes"].append(
                "This binding should let the next sync proceed with one stable AD identity for the source user."
            )
            bind_now["note_messages"].append(
                _localized_message("conflicts.decision.note.stable_identity")
            )

    if normalized_conflict_type == "multiple_ad_candidates":
        without_binding = {
            "status": "warning",
            "summary": (
                "If you do not bind one account, the multiple-candidate conflict should remain open and the next sync "
                "should not create a new managed account automatically for this user."
            ),
            "summary_code": "conflicts.decision.without_binding.multiple_ad_candidates",
            "summary_params": {},
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [
                "The system still sees more than one existing AD match for this source identity.",
            ],
            "note_messages": [
                _localized_message("conflicts.decision.note.multiple_matches_remain"),
            ],
        }
    elif normalized_conflict_type == "shared_ad_account":
        without_binding = {
            "status": "warning",
            "summary": (
                "If you do not change the identity decision, the shared-account conflict should remain open and this "
                "decision alone should not safely create a separate managed account."
            ),
            "summary_code": "conflicts.decision.without_binding.shared_ad_account",
            "summary_params": {},
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [
                "A unique AD identity still needs to be chosen for each affected source user.",
            ],
            "note_messages": [
                _localized_message("conflicts.decision.note.unique_identity_required"),
            ],
        }
    elif normalized_conflict_type == "existing_ad_identity_claim_review":
        without_binding = {
            "status": "warning",
            "summary": (
                "If you do not approve the claim, the existing AD account stays unbound and the review-mode policy "
                "should keep this user in the conflict queue on the next sync run."
            ),
            "summary_code": "conflicts.decision.without_binding.existing_claim_review",
            "summary_params": {},
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [
                "Switching the policy back to auto-safe would allow unique, unprotected existing-account claims to bind automatically.",
            ],
            "note_messages": [
                _localized_message("conflicts.decision.note.auto_safe_policy"),
            ],
        }
    else:
        without_binding = {
            "status": "info",
            "summary": "No automatic identity decision is applied yet, so the conflict stays pending for review.",
            "summary_code": "conflicts.decision.without_binding.generic",
            "summary_params": {},
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [],
            "note_messages": [],
        }

    return {
        "bind_now": bind_now,
        "without_binding": without_binding,
    }
