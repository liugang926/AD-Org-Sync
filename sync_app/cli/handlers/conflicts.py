from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from sync_app.cli.common import _open_db_manager
from sync_app.core.conflict_recommendations import (
    recommend_conflict_resolution,
    recommendation_requires_confirmation,
)
from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.storage.local_db import (
    OrganizationConfigRepository,
    SyncConflictRepository,
    SyncExceptionRuleRepository,
    SyncJobRepository,
    UserIdentityBindingRepository,
)


def _validate_binding_target(
    user_binding_repo: UserIdentityBindingRepository,
    source_user_id: str,
    ad_username: str,
    *,
    org_id: str,
) -> str | None:
    protected_accounts = OrganizationConfigRepository(user_binding_repo.db).get_app_config(
        org_id,
        config_path="",
    ).exclude_accounts
    if is_protected_ad_account_name(ad_username, protected_accounts):
        return f"AD account {ad_username} is system-protected and cannot be managed by sync"
    existing_by_ad = user_binding_repo.get_binding_record_by_ad_username(ad_username, org_id=org_id)
    if existing_by_ad and existing_by_ad.source_user_id != source_user_id:
        return (
            f"AD account {ad_username} is already bound to source user {existing_by_ad.source_user_id}"
        )
    return None

def _resolve_conflict_org_id(job_repo: SyncJobRepository, conflict: Any) -> str:
    if not conflict or not getattr(conflict, "job_id", ""):
        return "default"
    job_record = job_repo.get_job_record(str(conflict.job_id))
    if not job_record or not job_record.org_id:
        return "default"
    return str(job_record.org_id)

def _resolve_conflicts_for_source(
    conflict_repo: SyncConflictRepository,
    *,
    job_id: str,
    source_id: str,
    resolution_payload: dict[str, Any],
) -> int:
    resolved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return conflict_repo.resolve_open_conflicts_for_source(
        job_id=job_id,
        source_id=source_id,
        resolution_payload=resolution_payload,
        resolved_at=resolved_at,
    )

def _serialize_conflict(item: Any) -> dict[str, Any]:
    return {
        "id": item.id,
        "job_id": item.job_id,
        "conflict_type": item.conflict_type,
        "severity": item.severity,
        "status": item.status,
        "source_id": item.source_id,
        "target_key": item.target_key,
        "message": item.message,
        "resolution_hint": item.resolution_hint,
        "details": item.details,
        "resolution_payload": item.resolution_payload,
        "recommendation": recommend_conflict_resolution(item),
        "created_at": item.created_at,
        "resolved_at": item.resolved_at,
    }

def _apply_conflict_manual_binding(
    conflict_repo: SyncConflictRepository,
    user_binding_repo: UserIdentityBindingRepository,
    job_repo: SyncJobRepository,
    *,
    conflict: Any,
    ad_username: str,
    actor_username: str,
    notes: str = "",
) -> tuple[bool, str, int]:
    normalized_ad_username = str(ad_username or "").strip()
    if not conflict.source_id or not normalized_ad_username:
        return False, "conflict does not support manual binding", 0

    org_id = _resolve_conflict_org_id(job_repo, conflict)
    conflict_message = _validate_binding_target(
        user_binding_repo,
        conflict.source_id,
        normalized_ad_username,
        org_id=org_id,
    )
    if conflict_message:
        return False, conflict_message, 0

    binding_notes = str(notes or "").strip() or f"resolved from conflict {conflict.id}"
    user_binding_repo.upsert_binding_for_source_user(
        conflict.source_id,
        normalized_ad_username,
        org_id=org_id,
        source="manual",
        notes=binding_notes,
        preserve_manual=False,
    )
    resolved_count = _resolve_conflicts_for_source(
        conflict_repo,
        job_id=conflict.job_id,
        source_id=conflict.source_id,
        resolution_payload={
            "action": "manual_binding",
            "ad_username": normalized_ad_username,
            "notes": binding_notes,
            "source_conflict_id": conflict.id,
            "actor_username": actor_username,
        },
    )
    return True, normalized_ad_username, resolved_count

def _apply_conflict_skip_user_sync(
    conflict_repo: SyncConflictRepository,
    exception_rule_repo: SyncExceptionRuleRepository,
    job_repo: SyncJobRepository,
    *,
    conflict: Any,
    actor_username: str,
    notes: str = "",
) -> tuple[bool, str, int]:
    if not conflict.source_id:
        return False, "conflict does not have a source user", 0

    org_id = _resolve_conflict_org_id(job_repo, conflict)
    rule_notes = str(notes or "").strip() or f"resolved from conflict {conflict.id}"
    exception_rule_repo.upsert_rule(
        rule_type="skip_user_sync",
        match_value=conflict.source_id,
        org_id=org_id,
        notes=rule_notes,
        is_enabled=True,
    )
    resolved_count = _resolve_conflicts_for_source(
        conflict_repo,
        job_id=conflict.job_id,
        source_id=conflict.source_id,
        resolution_payload={
            "action": "skip_user_sync",
            "notes": rule_notes,
            "source_conflict_id": conflict.id,
            "actor_username": actor_username,
        },
    )
    return True, rule_notes, resolved_count

def _apply_conflict_recommendation(
    conflict_repo: SyncConflictRepository,
    exception_rule_repo: SyncExceptionRuleRepository,
    user_binding_repo: UserIdentityBindingRepository,
    job_repo: SyncJobRepository,
    *,
    conflict: Any,
    actor_username: str,
    confirmation_reason: str = "",
) -> tuple[bool, str, int, dict[str, Any] | None]:
    recommendation = recommend_conflict_resolution(conflict)
    if not recommendation:
        return False, "no recommendation is available for this conflict", 0, None

    action = str(recommendation.get("action") or "").strip().lower()
    normalized_confirmation_reason = str(confirmation_reason or "").strip()
    if recommendation_requires_confirmation(recommendation) and not normalized_confirmation_reason:
        return False, "low-confidence recommendations require --reason", 0, recommendation

    notes = normalized_confirmation_reason or str(recommendation.get("reason") or "").strip() or f"recommended resolution from conflict {conflict.id}"
    if action == "manual_binding":
        ok, detail, resolved_count = _apply_conflict_manual_binding(
            conflict_repo,
            user_binding_repo,
            job_repo,
            conflict=conflict,
            ad_username=str(recommendation.get("ad_username") or ""),
            actor_username=actor_username,
            notes=notes,
        )
        return ok, detail, resolved_count, recommendation
    if action == "skip_user_sync":
        ok, detail, resolved_count = _apply_conflict_skip_user_sync(
            conflict_repo,
            exception_rule_repo,
            job_repo,
            conflict=conflict,
            actor_username=actor_username,
            notes=notes,
        )
        return ok, detail, resolved_count, recommendation
    return False, f"unsupported recommendation action: {action or '-'}", 0, recommendation

def _handle_conflicts_list(args: argparse.Namespace) -> int:
    db_manager = _open_db_manager(args.db_path)
    conflict_repo = SyncConflictRepository(db_manager)
    status_filter = None if args.status == "all" else args.status
    conflicts = conflict_repo.list_conflict_records(
        job_id=args.job_id,
        status=status_filter,
        limit=max(int(args.limit), 1),
    )
    if args.json:
        print(json.dumps([_serialize_conflict(item) for item in conflicts], ensure_ascii=False, indent=2))
        return 0

    if not conflicts:
        print("no conflicts found")
        return 0

    for item in conflicts:
        print(f"id: {item.id}")
        print(f"job_id: {item.job_id}")
        print(f"type: {item.conflict_type}")
        print(f"status: {item.status}")
        print(f"source_id: {item.source_id}")
        print(f"target_key: {item.target_key or '-'}")
        print(f"message: {item.message}")
        recommendation = recommend_conflict_resolution(item)
        if recommendation:
            print(f"recommendation: {json.dumps(recommendation, ensure_ascii=False, sort_keys=True)}")
        if item.resolution_payload:
            print(f"resolution: {json.dumps(item.resolution_payload, ensure_ascii=False, sort_keys=True)}")
        print("---")
    return 0

def _handle_conflicts_resolve_binding(args: argparse.Namespace) -> int:
    db_manager = _open_db_manager(args.db_path)
    conflict_repo = SyncConflictRepository(db_manager)
    user_binding_repo = UserIdentityBindingRepository(db_manager)
    job_repo = SyncJobRepository(db_manager)

    conflict = conflict_repo.get_conflict_record(args.conflict_id)
    if not conflict:
        print(f"conflict not found: {args.conflict_id}", file=sys.stderr)
        return 1
    if conflict.status != "open":
        print(f"conflict is not open: {conflict.status}", file=sys.stderr)
        return 1

    ok, ad_username, resolved_count = _apply_conflict_manual_binding(
        conflict_repo,
        user_binding_repo,
        job_repo,
        conflict=conflict,
        ad_username=str(args.ad_username or ""),
        actor_username=os.getenv("USERNAME") or os.getenv("USER") or "cli",
    )
    if not ok:
        print(ad_username, file=sys.stderr)
        return 1
    print(f"resolved conflict: {conflict.id}")
    print(f"source_user_id: {conflict.source_id}")
    print(f"ad_username: {ad_username}")
    print(f"resolved_conflicts: {resolved_count}")
    return 0

def _handle_conflicts_skip_user(args: argparse.Namespace) -> int:
    db_manager = _open_db_manager(args.db_path)
    conflict_repo = SyncConflictRepository(db_manager)
    exception_rule_repo = SyncExceptionRuleRepository(db_manager)
    job_repo = SyncJobRepository(db_manager)

    conflict = conflict_repo.get_conflict_record(args.conflict_id)
    if not conflict:
        print(f"conflict not found: {args.conflict_id}", file=sys.stderr)
        return 1
    if conflict.status != "open":
        print(f"conflict is not open: {conflict.status}", file=sys.stderr)
        return 1
    if not conflict.source_id:
        print("conflict does not have a source user", file=sys.stderr)
        return 1

    ok, notes, resolved_count = _apply_conflict_skip_user_sync(
        conflict_repo,
        exception_rule_repo,
        job_repo,
        conflict=conflict,
        actor_username=os.getenv("USERNAME") or os.getenv("USER") or "cli",
        notes=str(args.notes or ""),
    )
    if not ok:
        print(notes, file=sys.stderr)
        return 1
    print(f"resolved conflict: {conflict.id}")
    print(f"skip_user_sync: {conflict.source_id}")
    print(f"resolved_conflicts: {resolved_count}")
    return 0

def _handle_conflicts_dismiss(args: argparse.Namespace) -> int:
    db_manager = _open_db_manager(args.db_path)
    conflict_repo = SyncConflictRepository(db_manager)

    conflict = conflict_repo.get_conflict_record(args.conflict_id)
    if not conflict:
        print(f"conflict not found: {args.conflict_id}", file=sys.stderr)
        return 1

    notes = str(args.notes or "").strip()
    conflict_repo.update_conflict_status(
        conflict.id,
        status="dismissed",
        resolution_payload={
            "action": "dismissed",
            "notes": notes,
            "actor_username": os.getenv("USERNAME") or os.getenv("USER") or "cli",
        },
        resolved_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )
    print(f"dismissed conflict: {conflict.id}")
    return 0

def _handle_conflicts_reopen(args: argparse.Namespace) -> int:
    db_manager = _open_db_manager(args.db_path)
    conflict_repo = SyncConflictRepository(db_manager)

    conflict = conflict_repo.get_conflict_record(args.conflict_id)
    if not conflict:
        print(f"conflict not found: {args.conflict_id}", file=sys.stderr)
        return 1
    if conflict.status == "open":
        print("conflict is already open", file=sys.stderr)
        return 1

    conflict_repo.update_conflict_status(
        conflict.id,
        status="open",
        resolution_payload=None,
        resolved_at=None,
    )
    print(f"reopened conflict: {conflict.id}")
    return 0

def _handle_conflicts_apply_recommendation(args: argparse.Namespace) -> int:
    db_manager = _open_db_manager(args.db_path)
    conflict_repo = SyncConflictRepository(db_manager)
    exception_rule_repo = SyncExceptionRuleRepository(db_manager)
    user_binding_repo = UserIdentityBindingRepository(db_manager)
    job_repo = SyncJobRepository(db_manager)

    conflict = conflict_repo.get_conflict_record(args.conflict_id)
    if not conflict:
        print(f"conflict not found: {args.conflict_id}", file=sys.stderr)
        return 1
    if conflict.status != "open":
        print(f"conflict is not open: {conflict.status}", file=sys.stderr)
        return 1

    ok, detail, resolved_count, recommendation = _apply_conflict_recommendation(
        conflict_repo,
        exception_rule_repo,
        user_binding_repo,
        job_repo,
        conflict=conflict,
        actor_username=os.getenv("USERNAME") or os.getenv("USER") or "cli",
        confirmation_reason=str(args.reason or ""),
    )
    if not ok:
        print(detail, file=sys.stderr)
        return 1

    print(f"resolved conflict: {conflict.id}")
    print(f"recommended_action: {recommendation.get('action')}")
    if recommendation.get("ad_username"):
        print(f"ad_username: {recommendation.get('ad_username')}")
    print(f"resolved_conflicts: {resolved_count}")
    return 0

def _handle_conflicts_bulk(args: argparse.Namespace) -> int:
    db_manager = _open_db_manager(args.db_path)
    conflict_repo = SyncConflictRepository(db_manager)
    exception_rule_repo = SyncExceptionRuleRepository(db_manager)
    user_binding_repo = UserIdentityBindingRepository(db_manager)
    job_repo = SyncJobRepository(db_manager)

    normalized_action = str(args.action or "").strip().lower()
    updated_count = 0
    skipped_count = 0
    actor_username = os.getenv("USERNAME") or os.getenv("USER") or "cli"
    notes = str(args.notes or "").strip()

    for conflict_id in args.conflict_ids:
        conflict = conflict_repo.get_conflict_record(int(conflict_id))
        if not conflict:
            skipped_count += 1
            continue

        if normalized_action == "reopen":
            if conflict.status == "open":
                skipped_count += 1
                continue
            conflict_repo.update_conflict_status(
                conflict.id,
                status="open",
                resolution_payload=None,
                resolved_at=None,
            )
            updated_count += 1
            continue

        if conflict.status != "open":
            skipped_count += 1
            continue

        if normalized_action == "apply-recommendation" and not notes:
            recommendation = recommend_conflict_resolution(conflict)
            if recommendation_requires_confirmation(recommendation):
                print("low-confidence bulk recommendation apply requires --notes", file=sys.stderr)
                return 1

        if normalized_action == "dismiss":
            conflict_repo.update_conflict_status(
                conflict.id,
                status="dismissed",
                resolution_payload={
                    "action": "dismissed",
                    "notes": notes,
                    "actor_username": actor_username,
                    "bulk": True,
                },
                resolved_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )
            updated_count += 1
            continue

        if normalized_action == "apply-recommendation":
            ok, _detail, resolved_count, _recommendation = _apply_conflict_recommendation(
                conflict_repo,
                exception_rule_repo,
                user_binding_repo,
                job_repo,
                conflict=conflict,
                actor_username=actor_username,
                confirmation_reason=notes,
            )
            if ok and resolved_count:
                updated_count += 1
            else:
                skipped_count += 1
            continue

        if normalized_action == "skip-user-sync":
            ok, _rule_notes, resolved_count = _apply_conflict_skip_user_sync(
                conflict_repo,
                exception_rule_repo,
                job_repo,
                conflict=conflict,
                actor_username=actor_username,
                notes=notes or f"bulk resolved from conflict {conflict.id}",
            )
            if ok and resolved_count:
                updated_count += 1
            else:
                skipped_count += 1
            continue

        print(f"unsupported bulk action: {normalized_action}", file=sys.stderr)
        return 1

    print(f"action: {normalized_action}")
    print(f"updated: {updated_count}")
    print(f"skipped: {skipped_count}")
    return 0 if updated_count > 0 or skipped_count == 0 else 1
