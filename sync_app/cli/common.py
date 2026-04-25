from __future__ import annotations

import os
from typing import Any

from sync_app.core.config import load_sync_config
from sync_app.storage.local_db import (
    DatabaseManager,
    OrganizationConfigRepository,
    OrganizationRepository,
)


def _get_cli_dependency(name: str) -> Any:
    import sync_app.cli as public_cli

    return getattr(public_cli, name)


def _normalize_cli_org_id(raw_org_id: Any) -> str:
    return str(raw_org_id or "").strip().lower() or "default"

def _resolve_cli_org_context(
    *,
    db_path: str | None,
    org_id: str,
    config_path: str | None = None,
):
    db_manager = DatabaseManager(db_path=db_path)
    db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    organization_repo = OrganizationRepository(db_manager)
    existing_default = organization_repo.get_organization_record("default")
    bootstrap_path = (
        str(config_path or "").strip()
        or (existing_default.config_path if existing_default else "")
        or "config.ini"
    )
    organization_repo.ensure_default(config_path=bootstrap_path)
    normalized_org_id = _normalize_cli_org_id(org_id)
    organization = organization_repo.get_organization_record(normalized_org_id)
    if not organization:
        raise ValueError(f"organization not found: {normalized_org_id}")
    if not organization.is_enabled:
        raise ValueError(f"organization is disabled: {normalized_org_id}")
    resolved_config_path = str(config_path or "").strip() or organization.config_path or bootstrap_path
    return db_manager, organization, resolved_config_path

def _load_cli_db_config(
    *,
    db_path: str | None,
    org_id: str,
    config_path: str | None = None,
):
    db_manager, organization, resolved_config_path = _resolve_cli_org_context(
        db_path=db_path,
        org_id=org_id,
        config_path=config_path,
    )
    config = OrganizationConfigRepository(db_manager).get_app_config(
        organization.org_id,
        config_path=resolved_config_path,
    )
    return db_manager, organization, config

def _load_cli_effective_config(args: argparse.Namespace):
    raw_config_path = str(getattr(args, "config", "") or "").strip()
    if raw_config_path:
        config = load_sync_config(raw_config_path)
        return None, None, config, os.path.abspath(raw_config_path)
    db_manager, organization, config = _load_cli_db_config(
        db_path=getattr(args, "db_path", None),
        org_id=getattr(args, "org_id", "default"),
        config_path=None,
    )
    return db_manager, organization, config, config.config_path

def _open_db_manager(db_path: str | None) -> DatabaseManager:
    db_manager = DatabaseManager(db_path=db_path)
    db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    return db_manager

def _print_summary(summary: dict[str, Any]) -> None:
    print(f"job_id: {summary.get('job_id')}")
    if summary.get("org_id"):
        print(f"org_id: {summary.get('org_id')}")
    if summary.get("organization_config_path"):
        print(f"organization_config_path: {summary.get('organization_config_path')}")
    print(f"mode: {summary.get('mode')}")
    print(f"errors: {summary.get('error_count')}")
    print(f"planned_operations: {summary.get('planned_operation_count')}")
    print(f"executed_operations: {summary.get('executed_operation_count')}")
    print(f"high_risk_operations: {summary.get('high_risk_operation_count', 0)}")
    print(f"conflicts: {summary.get('conflict_count', 0)}")
    if summary.get("review_required"):
        print("review_required: true")
    if summary.get("log_file"):
        print(f"log_file: {summary['log_file']}")
    if summary.get("db_path"):
        print(f"db_path: {summary['db_path']}")
    if summary.get("db_backup_dir"):
        print(f"db_backup_dir: {summary['db_backup_dir']}")
    if summary.get("db_startup_snapshot_path"):
        print(f"db_startup_snapshot_path: {summary['db_startup_snapshot_path']}")
    if summary.get("db_migration_source_path"):
        print(f"db_migration_source_path: {summary['db_migration_source_path']}")
