import asyncio
import argparse
import getpass
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from sync_app.core.common import APP_VERSION
from sync_app.core.config import (
    load_sync_config,
    run_config_security_self_check,
    test_ldap_connection,
    test_source_connection,
    test_wecom_connection,
    validate_config,
)
from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.core.conflict_recommendations import (
    recommend_conflict_resolution,
    recommendation_requires_confirmation,
)
from sync_app.core.models import SyncJobSummary
from sync_app.services.config_bundle import export_organization_bundle, import_organization_bundle
from sync_app.services.entry import main as run_sync
from sync_app.storage.local_db import (
    DatabaseManager,
    OrganizationConfigRepository,
    OrganizationRepository,
    SettingsRepository,
    SyncConflictRepository,
    SyncExceptionRuleRepository,
    SyncJobRepository,
    SyncPlanReviewRepository,
    UserIdentityBindingRepository,
    WebAdminUserRepository,
)
from sync_app.web.security import hash_password, validate_admin_password_strength


def windows_selector_loop_factory():
    return asyncio.SelectorEventLoop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ad-org-sync",
        description="Source directory to Active Directory synchronization tool.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    version_parser = subparsers.add_parser("version", help="Print version information")
    version_parser.set_defaults(handler=_handle_version)

    init_web_parser = subparsers.add_parser(
        "init-web",
        help="Initialize the local SQLite database and the default web organization",
    )
    init_web_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    init_web_parser.add_argument(
        "--config",
        default=None,
        help="Optional legacy config file path to associate with the default organization",
    )
    init_web_parser.add_argument(
        "--no-startup-snapshot",
        action="store_true",
        help="Skip creating a startup snapshot when initializing an existing database",
    )
    init_web_parser.add_argument("--json", action="store_true", help="Print machine-readable output")
    init_web_parser.set_defaults(handler=_handle_init_web)

    bootstrap_admin_parser = subparsers.add_parser(
        "bootstrap-admin",
        help="Create or reset the initial local administrator account",
    )
    bootstrap_admin_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    bootstrap_admin_parser.add_argument("--username", default="admin", help="Administrator username")
    bootstrap_admin_parser.add_argument("--password", default="", help="Administrator password")
    bootstrap_admin_parser.add_argument(
        "--password-env",
        default="",
        help="Environment variable name that stores the administrator password",
    )
    bootstrap_admin_parser.add_argument(
        "--role",
        choices=["super_admin", "operator", "auditor"],
        default="super_admin",
        help="Role for a newly created account",
    )
    bootstrap_admin_parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset the password when the administrator already exists",
    )
    bootstrap_admin_parser.add_argument(
        "--enable",
        action="store_true",
        help="Re-enable an existing administrator account after resetting the password",
    )
    bootstrap_admin_parser.set_defaults(handler=_handle_bootstrap_admin)

    validate_parser = subparsers.add_parser("validate-config", help="Validate database-backed org config or a legacy config file")
    validate_parser.add_argument("--org-id", default="default", help="Organization ID to validate when using database-backed configuration")
    validate_parser.add_argument("--config", default=None, help="Optional legacy config file path to validate directly or import from")
    validate_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    validate_parser.set_defaults(handler=_handle_validate_config)

    test_source_parser = subparsers.add_parser(
        "test-source",
        help="Test source connector credentials from org config or a legacy config file",
    )
    test_source_parser.add_argument("--org-id", default="default", help="Organization ID to test when using database-backed configuration")
    test_source_parser.add_argument("--config", default=None, help="Optional legacy config file path to test directly or import from")
    test_source_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    test_source_parser.set_defaults(handler=_handle_test_source)

    test_wecom_parser = subparsers.add_parser(
        "test-wecom",
        help="Legacy alias for test-source when the organization uses the WeCom provider",
    )
    test_wecom_parser.add_argument("--org-id", default="default", help="Organization ID to test when using database-backed configuration")
    test_wecom_parser.add_argument("--config", default=None, help="Optional legacy config file path to test directly or import from")
    test_wecom_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    test_wecom_parser.set_defaults(handler=_handle_test_source)

    test_ldap_parser = subparsers.add_parser("test-ldap", help="Test LDAP/LDAPS connectivity from org config or a legacy config file")
    test_ldap_parser.add_argument("--org-id", default="default", help="Organization ID to test when using database-backed configuration")
    test_ldap_parser.add_argument("--config", default=None, help="Optional legacy config file path to test directly or import from")
    test_ldap_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    test_ldap_parser.set_defaults(handler=_handle_test_ldap)

    sync_parser = subparsers.add_parser("sync", help="Run synchronization")
    sync_parser.add_argument("--mode", choices=["apply", "dry-run"], default="dry-run")
    sync_parser.add_argument("--org-id", default="default", help="Organization ID to synchronize")
    sync_parser.add_argument("--config", default=None, help="Optional legacy config file path used for import or compatibility")
    sync_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    sync_parser.add_argument("--json", action="store_true", help="Print machine-readable summary")
    sync_parser.set_defaults(handler=_handle_sync)

    export_parser = subparsers.add_parser("config-export", help="Export a database-backed organization configuration bundle")
    export_parser.add_argument("--org-id", default="default", help="Organization ID to export")
    export_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    export_parser.add_argument("--out", default="", help="Optional output file path; prints to stdout when omitted")
    export_parser.set_defaults(handler=_handle_config_export)

    import_parser = subparsers.add_parser("config-import", help="Import an organization configuration bundle into the database")
    import_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    import_parser.add_argument("--file", required=True, help="Path to a JSON bundle file")
    import_parser.add_argument("--target-org-id", default="", help="Optional target organization ID override")
    import_parser.add_argument("--replace", action="store_true", help="Replace existing connectors, rules, and org-scoped settings before import")
    import_parser.set_defaults(handler=_handle_config_import)

    approve_parser = subparsers.add_parser("approve-plan", help="Approve a dry-run high-risk plan for apply execution")
    approve_parser.add_argument("job_id", help="Dry-run job ID to approve")
    approve_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    approve_parser.add_argument("--reviewer", default=None, help="Reviewer name, defaults to current OS user")
    approve_parser.add_argument("--notes", default="", help="Optional review notes")
    approve_parser.add_argument("--ttl-minutes", type=int, default=240, help="Approval validity window in minutes")
    approve_parser.set_defaults(handler=_handle_approve_plan)

    conflicts_parser = subparsers.add_parser("conflicts", help="Inspect or manage sync conflicts")
    conflict_subparsers = conflicts_parser.add_subparsers(dest="conflict_command", required=True)

    conflict_list_parser = conflict_subparsers.add_parser("list", help="List recorded sync conflicts")
    conflict_list_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    conflict_list_parser.add_argument("--job-id", default=None, help="Filter by job ID")
    conflict_list_parser.add_argument(
        "--status",
        choices=["open", "resolved", "dismissed", "all"],
        default="open",
        help="Filter by conflict status",
    )
    conflict_list_parser.add_argument("--limit", type=int, default=50, help="Maximum conflicts to print")
    conflict_list_parser.add_argument("--json", action="store_true", help="Print machine-readable output")
    conflict_list_parser.set_defaults(handler=_handle_conflicts_list)

    conflict_resolve_binding_parser = conflict_subparsers.add_parser(
        "resolve-binding",
        help="Resolve a conflict by creating a manual source -> AD binding",
    )
    conflict_resolve_binding_parser.add_argument("conflict_id", type=int, help="Conflict ID")
    conflict_resolve_binding_parser.add_argument("--ad-username", required=True, help="AD username to bind")
    conflict_resolve_binding_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    conflict_resolve_binding_parser.set_defaults(handler=_handle_conflicts_resolve_binding)

    conflict_skip_user_parser = conflict_subparsers.add_parser(
        "skip-user",
        help="Resolve a conflict by adding skip_user_sync for the source user",
    )
    conflict_skip_user_parser.add_argument("conflict_id", type=int, help="Conflict ID")
    conflict_skip_user_parser.add_argument("--notes", default="", help="Optional exception rule notes")
    conflict_skip_user_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    conflict_skip_user_parser.set_defaults(handler=_handle_conflicts_skip_user)

    conflict_dismiss_parser = conflict_subparsers.add_parser("dismiss", help="Mark a conflict as dismissed")
    conflict_dismiss_parser.add_argument("conflict_id", type=int, help="Conflict ID")
    conflict_dismiss_parser.add_argument("--notes", default="", help="Optional dismiss notes")
    conflict_dismiss_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    conflict_dismiss_parser.set_defaults(handler=_handle_conflicts_dismiss)

    conflict_reopen_parser = conflict_subparsers.add_parser("reopen", help="Reopen a resolved or dismissed conflict")
    conflict_reopen_parser.add_argument("conflict_id", type=int, help="Conflict ID")
    conflict_reopen_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    conflict_reopen_parser.set_defaults(handler=_handle_conflicts_reopen)

    conflict_apply_recommendation_parser = conflict_subparsers.add_parser(
        "apply-recommendation",
        help="Resolve a conflict by applying the recommended action",
    )
    conflict_apply_recommendation_parser.add_argument("conflict_id", type=int, help="Conflict ID")
    conflict_apply_recommendation_parser.add_argument("--reason", default="", help="Confirmation reason for low-confidence recommendations")
    conflict_apply_recommendation_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    conflict_apply_recommendation_parser.set_defaults(handler=_handle_conflicts_apply_recommendation)

    conflict_bulk_parser = conflict_subparsers.add_parser("bulk", help="Run a bulk action on selected conflicts")
    conflict_bulk_parser.add_argument(
        "--action",
        choices=["apply-recommendation", "skip-user-sync", "dismiss", "reopen"],
        required=True,
        help="Bulk action to execute",
    )
    conflict_bulk_parser.add_argument("conflict_ids", nargs="+", type=int, help="Conflict IDs")
    conflict_bulk_parser.add_argument("--notes", default="", help="Optional notes for bulk action")
    conflict_bulk_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    conflict_bulk_parser.set_defaults(handler=_handle_conflicts_bulk)

    db_check_parser = subparsers.add_parser("db-check", help="Run SQLite integrity check")
    db_check_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    db_check_parser.add_argument("--json", action="store_true", help="Print machine-readable output")
    db_check_parser.set_defaults(handler=_handle_db_check)

    db_backup_parser = subparsers.add_parser("db-backup", help="Create a SQLite backup snapshot")
    db_backup_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    db_backup_parser.add_argument("--label", default="manual", help="Backup label")
    db_backup_parser.add_argument("--json", action="store_true", help="Print machine-readable output")
    db_backup_parser.set_defaults(handler=_handle_db_backup)

    web_parser = subparsers.add_parser("web", help="Launch the web control plane")
    web_parser.add_argument("--host", default=None, help="Bind host, defaults to SQLite app setting or 127.0.0.1")
    web_parser.add_argument("--port", type=int, default=None, help="Bind port, defaults to SQLite app setting or 8000")
    web_parser.add_argument(
        "--public-base-url",
        default=None,
        help="Public HTTPS URL used when the app is deployed behind a reverse proxy",
    )
    web_parser.add_argument(
        "--secure-cookies",
        choices=["auto", "always", "never"],
        default=None,
        help="Secure session cookie policy for the current run",
    )
    proxy_group = web_parser.add_mutually_exclusive_group()
    proxy_group.add_argument(
        "--trust-proxy-headers",
        action="store_true",
        help="Trust X-Forwarded-* headers from the configured proxy IP allowlist",
    )
    proxy_group.add_argument(
        "--no-trust-proxy-headers",
        action="store_true",
        help="Disable trusted proxy headers for the current run",
    )
    web_parser.add_argument(
        "--forwarded-allow-ips",
        default=None,
        help="Comma-separated IPs or CIDRs that are allowed to send trusted forwarded headers",
    )
    web_parser.add_argument("--config", default=None, help="Optional legacy config file path for the default organization")
    web_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    web_parser.set_defaults(handler=_handle_web)

    gui_parser = subparsers.add_parser("gui", help="Launch the desktop UI")
    gui_parser.set_defaults(handler=_handle_gui)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.handler(args))


def _handle_version(_args: argparse.Namespace) -> int:
    print(APP_VERSION)
    return 0


def _resolve_admin_password_input(args: argparse.Namespace) -> str:
    direct_password = str(getattr(args, "password", "") or "")
    if direct_password:
        return direct_password

    password_env = str(getattr(args, "password_env", "") or "").strip()
    if password_env:
        env_value = str(os.getenv(password_env) or "")
        if not env_value:
            raise ValueError(f"environment variable is empty or missing: {password_env}")
        return env_value

    password = getpass.getpass("Administrator password: ")
    confirm_password = getpass.getpass("Confirm administrator password: ")
    if password != confirm_password:
        raise ValueError("administrator passwords do not match")
    return password


def _handle_init_web(args: argparse.Namespace) -> int:
    db_manager = DatabaseManager(db_path=args.db_path)
    init_result = db_manager.initialize(
        create_startup_snapshot=not bool(getattr(args, "no_startup_snapshot", False)),
        verify_integrity=True,
    )
    organization_repo = OrganizationRepository(db_manager)
    org_config_repo = OrganizationConfigRepository(db_manager)
    existing_default = organization_repo.get_organization_record("default")
    effective_config_path = (
        str(getattr(args, "config", "") or "").strip()
        or (existing_default.config_path if existing_default else "")
        or "config.ini"
    )
    organization_repo.ensure_default(config_path=effective_config_path)
    org_config_repo.ensure_loaded("default", config_path=effective_config_path)

    result = {
        "db_path": db_manager.db_path,
        "backup_dir": db_manager.backup_dir,
        "config_path": effective_config_path,
        "organization_id": "default",
        "created_new_database": bool(init_result.get("created_new_database")),
        "migration_source_path": init_result.get("migration_source_path") or "",
        "startup_snapshot_path": init_result.get("startup_snapshot_path") or "",
        "integrity_result": str((init_result.get("integrity_check") or {}).get("result") or ""),
    }
    if getattr(args, "json", False):
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {result['db_path']}")
        print(f"backup_dir: {result['backup_dir']}")
        print(f"organization_id: {result['organization_id']}")
        print(f"config_path: {result['config_path']}")
        print(f"created_new_database: {str(result['created_new_database']).lower()}")
        if result["migration_source_path"]:
            print(f"migration_source_path: {result['migration_source_path']}")
        if result["startup_snapshot_path"]:
            print(f"startup_snapshot_path: {result['startup_snapshot_path']}")
        if result["integrity_result"]:
            print(f"integrity: {result['integrity_result']}")
    return 0


def _handle_bootstrap_admin(args: argparse.Namespace) -> int:
    db_manager = _open_db_manager(args.db_path)
    user_repo = WebAdminUserRepository(db_manager)
    settings_repo = SettingsRepository(db_manager)

    username = str(getattr(args, "username", "") or "").strip()
    if not username:
        print("administrator username is required", file=sys.stderr)
        return 1

    try:
        password = _resolve_admin_password_input(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    password_error = validate_admin_password_strength(
        password,
        min_length=settings_repo.get_int("web_admin_password_min_length", 8),
    )
    if password_error:
        print(password_error, file=sys.stderr)
        return 1

    existing_user = user_repo.get_user_record_by_username(username)
    if existing_user:
        if not getattr(args, "reset", False):
            print(
                f"administrator already exists: {username}. Use --reset to rotate the password.",
                file=sys.stderr,
            )
            return 1
        user_repo.set_password(username, hash_password(password))
        if getattr(args, "enable", False) and not existing_user.is_enabled:
            user_repo.set_enabled(existing_user.id, True)
        print(f"administrator password updated: {username}")
        if getattr(args, "enable", False):
            print("administrator account enabled")
        return 0

    user_repo.create_user(
        username=username,
        password_hash=hash_password(password),
        role=str(getattr(args, "role", "super_admin") or "super_admin"),
        is_enabled=True,
    )
    print(f"administrator created: {username}")
    print(f"role: {str(getattr(args, 'role', 'super_admin') or 'super_admin')}")
    return 0


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


def _handle_validate_config(args: argparse.Namespace) -> int:
    try:
        _, organization, config, config_source = _load_cli_effective_config(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    is_valid, errors = validate_config(config)
    if is_valid:
        print(f"config valid: {config_source}")
        if organization:
            print(f"organization: {organization.org_id}")
        security_warnings = run_config_security_self_check(config)
        for warning in security_warnings:
            print(f"warning: {warning}")
        return 0

    print(f"config invalid: {config_source}", file=sys.stderr)
    if organization:
        print(f"organization: {organization.org_id}", file=sys.stderr)
    for error in errors:
        print(f"- {error}", file=sys.stderr)
    return 1


def _handle_test_source(args: argparse.Namespace) -> int:
    try:
        _, _, config, _ = _load_cli_effective_config(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    success, message = test_source_connection(
        config.wecom.corpid,
        config.wecom.corpsecret,
        config.wecom.agentid,
        source_provider=getattr(config, "source_provider", "wecom"),
    )
    stream = sys.stdout if success else sys.stderr
    print(message, file=stream)
    return 0 if success else 1


def _handle_test_wecom(args: argparse.Namespace) -> int:
    return _handle_test_source(args)


def _handle_test_ldap(args: argparse.Namespace) -> int:
    try:
        _, _, config, _ = _load_cli_effective_config(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    success, message = test_ldap_connection(
        config.ldap.server,
        config.ldap.domain,
        config.ldap.username,
        config.ldap.password,
        config.ldap.use_ssl,
        config.ldap.port,
        config.ldap.validate_cert,
        config.ldap.ca_cert_path,
    )
    stream = sys.stdout if success else sys.stderr
    print(message, file=stream)
    return 0 if success else 1


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
    result = run_sync(
        execution_mode=execution_mode,
        trigger_type="cli",
        db_path=args.db_path,
        config_path=resolved_config_path,
        org_id=organization.org_id,
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


def _handle_config_export(args: argparse.Namespace) -> int:
    db_manager = DatabaseManager(db_path=args.db_path)
    db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    try:
        bundle = export_organization_bundle(db_manager, _normalize_cli_org_id(args.org_id))
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    payload = json.dumps(bundle, ensure_ascii=False, indent=2)
    output_path = str(getattr(args, "out", "") or "").strip()
    if output_path:
        with open(output_path, "w", encoding="utf-8") as handle:
            handle.write(payload)
        print(f"exported organization bundle: {os.path.abspath(output_path)}")
    else:
        print(payload)
    return 0


def _handle_config_import(args: argparse.Namespace) -> int:
    bundle_path = str(getattr(args, "file", "") or "").strip()
    if not bundle_path:
        print("bundle file is required", file=sys.stderr)
        return 1
    if not os.path.exists(bundle_path):
        print(f"bundle file not found: {bundle_path}", file=sys.stderr)
        return 1
    with open(bundle_path, "r", encoding="utf-8") as handle:
        try:
            bundle = json.load(handle)
        except json.JSONDecodeError as exc:
            print(f"invalid bundle JSON: {exc}", file=sys.stderr)
            return 1

    db_manager = DatabaseManager(db_path=args.db_path)
    db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    try:
        summary = import_organization_bundle(
            db_manager,
            bundle,
            target_org_id=str(getattr(args, "target_org_id", "") or "").strip() or None,
            replace_existing=bool(getattr(args, "replace", False)),
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"imported organization bundle into: {summary['org_id']}")
    print(f"source_org_id: {summary['source_org_id']}")
    print(f"replace_existing: {'true' if summary['replace_existing'] else 'false'}")
    print(f"imported_settings: {summary['imported_settings']}")
    print(f"imported_connectors: {summary['imported_connectors']}")
    print(f"imported_mappings: {summary['imported_mappings']}")
    print(f"imported_group_rules: {summary['imported_group_rules']}")
    return 0


def _handle_approve_plan(args: argparse.Namespace) -> int:
    db_manager = DatabaseManager(db_path=args.db_path)
    db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    review_repo = SyncPlanReviewRepository(db_manager)
    review_record = review_repo.get_review_record_by_job_id(args.job_id)
    if not review_record:
        print(f"review record not found for job: {args.job_id}", file=sys.stderr)
        return 1

    reviewer = args.reviewer or os.getenv("USERNAME") or os.getenv("USER") or "cli"
    expires_at = None
    if args.ttl_minutes and args.ttl_minutes > 0:
        from datetime import datetime, timedelta, timezone

        expires_at = (
            datetime.now(timezone.utc) + timedelta(minutes=int(args.ttl_minutes))
        ).isoformat(timespec="seconds")

    review_repo.approve_review(
        args.job_id,
        reviewer_username=reviewer,
        review_notes=args.notes,
        expires_at=expires_at,
    )
    print(f"approved plan: {args.job_id}")
    print(f"reviewer: {reviewer}")
    if expires_at:
        print(f"expires_at: {expires_at}")
    return 0


def _open_db_manager(db_path: str | None) -> DatabaseManager:
    db_manager = DatabaseManager(db_path=db_path)
    db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    return db_manager


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


def _handle_db_check(args: argparse.Namespace) -> int:
    db_manager = DatabaseManager(db_path=args.db_path)
    init_result = db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    result = {
        "db_path": db_manager.db_path,
        "backup_dir": db_manager.backup_dir,
        "migration_source_path": init_result.get("migration_source_path"),
        "startup_snapshot_path": init_result.get("startup_snapshot_path"),
        "integrity_check": db_manager.run_integrity_check(),
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {result['db_path']}")
        print(f"backup_dir: {result['backup_dir']}")
        if result.get("migration_source_path"):
            print(f"migration_source_path: {result['migration_source_path']}")
        if result.get("startup_snapshot_path"):
            print(f"startup_snapshot: {result['startup_snapshot_path']}")
        print(f"integrity: {result['integrity_check']['result']}")
    return 0 if result["integrity_check"]["ok"] else 2


def _handle_db_backup(args: argparse.Namespace) -> int:
    db_manager = DatabaseManager(db_path=args.db_path)
    db_manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    backup_path = db_manager.backup_database(label=args.label)
    result = {
        "db_path": db_manager.db_path,
        "backup_dir": db_manager.backup_dir,
        "backup_path": backup_path,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {result['db_path']}")
        print(f"backup_dir: {result['backup_dir']}")
        print(f"backup_path: {result['backup_path']}")
    return 0


def _handle_gui(_args: argparse.Namespace) -> int:
    from sync_app.ui.desktop import main as ui_main

    ui_main()
    return 0


def _handle_web(args: argparse.Namespace) -> int:
    try:
        import uvicorn
    except ImportError as exc:
        print(f"web dependencies not installed: {exc}", file=sys.stderr)
        print("install the web dependency set first, then retry `ad-org-sync web`", file=sys.stderr)
        return 1

    from sync_app.storage.local_db import SettingsRepository
    from sync_app.web.app import create_app, resolve_web_runtime_settings

    db_manager = DatabaseManager(db_path=args.db_path)
    db_manager.initialize()
    organization_repo = OrganizationRepository(db_manager)
    existing_default = organization_repo.get_organization_record("default")
    effective_config_path = (
        str(args.config or "").strip()
        or (existing_default.config_path if existing_default else "")
        or "config.ini"
    )
    settings_repo = SettingsRepository(db_manager)
    trust_proxy_headers = None
    if getattr(args, "trust_proxy_headers", False):
        trust_proxy_headers = True
    elif getattr(args, "no_trust_proxy_headers", False):
        trust_proxy_headers = False
    web_runtime_settings = resolve_web_runtime_settings(
        settings_repo,
        bind_host=args.host,
        bind_port=args.port,
        public_base_url=args.public_base_url,
        session_cookie_secure_mode=args.secure_cookies,
        trust_proxy_headers=trust_proxy_headers,
        forwarded_allow_ips=args.forwarded_allow_ips,
    )
    host = web_runtime_settings["bind_host"]
    port = web_runtime_settings["bind_port"]

    app = create_app(
        db_path=db_manager.db_path,
        config_path=effective_config_path,
        bind_host=host,
        bind_port=port,
        public_base_url=web_runtime_settings["public_base_url"],
        session_cookie_secure_mode=web_runtime_settings["session_cookie_secure_mode"],
        trust_proxy_headers=web_runtime_settings["trust_proxy_headers"],
        forwarded_allow_ips=web_runtime_settings["forwarded_allow_ips"],
    )
    print(f"web control plane bind address: http://{host}:{port}")
    if web_runtime_settings["public_base_url"]:
        print(f"public base URL: {web_runtime_settings['public_base_url']}")
    print(
        "secure session cookies: "
        + ("enabled" if web_runtime_settings["session_cookie_secure"] else "disabled")
        + f" ({web_runtime_settings['session_cookie_secure_mode']})"
    )
    print(
        "trusted proxy headers: "
        + ("enabled" if web_runtime_settings["trust_proxy_headers"] else "disabled")
    )
    if web_runtime_settings["trust_proxy_headers"]:
        print(f"forwarded allow IPs: {web_runtime_settings['forwarded_allow_ips']}")
    uvicorn.run(
        app,
        host=host,
        port=port,
        loop="sync_app.cli:windows_selector_loop_factory" if sys.platform.startswith("win") else "auto",
        proxy_headers=web_runtime_settings["trust_proxy_headers"],
        forwarded_allow_ips=(
            web_runtime_settings["forwarded_allow_ips"]
            if web_runtime_settings["trust_proxy_headers"]
            else None
        ),
    )
    return 0


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


if __name__ == "__main__":
    raise SystemExit(main())
