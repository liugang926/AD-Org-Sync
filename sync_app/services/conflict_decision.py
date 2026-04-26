from __future__ import annotations

from typing import Any


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
            "summary": "No AD account is selected yet, so the binding decision cannot be evaluated.",
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [
                "Choose one existing AD account before approving a binding decision.",
            ],
        }
    elif is_protected_account:
        bind_now = {
            "status": "error",
            "action": "protected_account",
            "label": "Protected AD account",
            "summary": (
                f"{normalized_target_username} is marked as a protected directory account and should not be managed "
                "through synchronization."
            ),
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [
                "Pick a user-managed AD account instead of a protected system identity.",
            ],
        }
    elif normalized_binding_owner and normalized_binding_owner != normalized_source_user_id:
        bind_now = {
            "status": "warning",
            "action": "already_bound_elsewhere",
            "label": "Already bound to another source user",
            "summary": (
                f"{normalized_target_username} is already bound to {normalized_binding_owner}, "
                "so binding it here would keep the identity conflict unresolved."
            ),
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [
                "Resolve the existing binding first, or choose a different AD account.",
            ],
        }
    else:
        if target_exists:
            if target_enabled is False and rehire_restore_enabled:
                action = "reactivate_user"
                label = "Reactivate and update existing AD account"
            else:
                action = "update_user"
                label = "Update existing AD account"
        else:
            action = "create_user"
            label = "Create new managed AD account"

        bind_now = {
            "status": "success",
            "action": action,
            "label": label,
            "summary": (
                f"The next sync should {label.lower()} {normalized_target_username} "
                "under the current field ownership and OU placement rules."
            ),
            "will_create_new_account": not target_exists,
            "will_conflict_continue": False,
            "notes": [],
        }
        if target_exists and target_enabled is False and not rehire_restore_enabled:
            bind_now["status"] = "warning"
            bind_now["notes"].append(
                "The account is currently disabled, and automatic reactivation is off, so this will stay an update plan."
            )
        if normalized_conflict_type == "shared_ad_account" and other_shared_users:
            bind_now["status"] = "warning"
            bind_now["will_conflict_continue"] = True
            bind_now["notes"].append(
                "This AD account is still shared with "
                + ", ".join(sorted(other_shared_users))
                + ", so binding it here does not remove the shared-account risk."
            )
        elif normalized_conflict_type == "multiple_ad_candidates":
            bind_now["notes"].append(
                "Choosing one concrete AD account should clear this user's candidate ambiguity on the next sync run."
            )
        elif normalized_conflict_type == "existing_ad_identity_claim_review":
            bind_now["notes"].append(
                "Approving this claim writes a manual binding, so the next sync can update the existing AD account "
                "instead of creating a duplicate managed account."
            )
        else:
            bind_now["notes"].append(
                "This binding should let the next sync proceed with one stable AD identity for the source user."
            )

    if normalized_conflict_type == "multiple_ad_candidates":
        without_binding = {
            "status": "warning",
            "summary": (
                "If you do not bind one account, the multiple-candidate conflict should remain open and the next sync "
                "should not create a new managed account automatically for this user."
            ),
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [
                "The system still sees more than one existing AD match for this source identity.",
            ],
        }
    elif normalized_conflict_type == "shared_ad_account":
        without_binding = {
            "status": "warning",
            "summary": (
                "If you do not change the identity decision, the shared-account conflict should remain open and this "
                "decision alone should not safely create a separate managed account."
            ),
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [
                "A unique AD identity still needs to be chosen for each affected source user.",
            ],
        }
    elif normalized_conflict_type == "existing_ad_identity_claim_review":
        without_binding = {
            "status": "warning",
            "summary": (
                "If you do not approve the claim, the existing AD account stays unbound and the review-mode policy "
                "should keep this user in the conflict queue on the next sync run."
            ),
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [
                "Switching the policy back to auto-safe would allow unique, unprotected existing-account claims to bind automatically.",
            ],
        }
    else:
        without_binding = {
            "status": "info",
            "summary": "No automatic identity decision is applied yet, so the conflict stays pending for review.",
            "will_create_new_account": False,
            "will_conflict_continue": True,
            "notes": [],
        }

    return {
        "bind_now": bind_now,
        "without_binding": without_binding,
    }
