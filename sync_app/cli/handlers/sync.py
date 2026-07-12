from __future__ import annotations

import argparse
import json
import os
import sys

from sync_app.application import TenantContext
from sync_app.cli.common import _get_cli_dependency, _print_summary, _resolve_cli_org_context
from sync_app.core.common import APP_VERSION
from sync_app.core.models import SyncJobSummary
from sync_app.services.external_integrations import build_approve_plan_use_case
from sync_app.storage.local_db import DatabaseManager


def _handle_version(_args: argparse.Namespace) -> int:
    print(APP_VERSION)
    return 0

def _handle_sync(args: argparse.Namespace) -> int:
    execution_mode = "dry_run" if args.mode == "dry-run" else "apply"
    try:
        _, organization, resolved_config_path = _resolve_cli_org_context(
            db_path=args.db_path,
            org_id=args.org_id,
            config_path=args.config,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    result = _get_cli_dependency("run_sync")(
        execution_mode=execution_mode,
        trigger_type="cli",
        db_path=args.db_path,
        config_path=resolved_config_path,
        org_id=organization.org_id,
        requested_by=os.getenv("USERNAME") or os.getenv("USER") or "cli",
    )
    summary_model = SyncJobSummary.from_sync_stats(result)
    summary = summary_model.to_dict()
    summary["org_id"] = result.get("org_id") or organization.org_id
    summary["organization_config_path"] = result.get("organization_config_path") or resolved_config_path
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        _print_summary(summary)
    if summary_model.review_required and execution_mode == "apply":
        return 3
    return 0 if summary_model.error_count == 0 else 2

def _handle_approve_plan(args: argparse.Namespace) -> int:
    db_manager = DatabaseManager(db_path=args.db_path)
    db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    reviewer = args.reviewer or os.getenv("USERNAME") or os.getenv("USER") or "cli"
    try:
        tenant = TenantContext.create(
            org_id=args.org_id,
            actor_username=reviewer,
            channel="cli",
        )
        result = build_approve_plan_use_case(db_manager).execute(
            tenant,
            job_id=args.job_id,
            review_notes=args.notes,
            ttl_minutes=args.ttl_minutes,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"approved plan: {args.job_id}")
    print(f"reviewer: {reviewer}")
    print(f"organization: {tenant.org_id}")
    print(f"expires_at: {result.expires_at_iso}")
    if result.replay_request_id is not None:
        print(f"replay_request_id: {result.replay_request_id}")
    if not result.fresh_approval:
        print("approval_status: already_approved")
    return 0
