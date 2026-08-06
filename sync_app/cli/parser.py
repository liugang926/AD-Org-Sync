from __future__ import annotations

import argparse

from sync_app.core.admin_roles import WEB_ADMIN_ROLES

from sync_app.cli.handlers.config import (
    _handle_config_export,
    _handle_config_import,
    _handle_test_ldap,
    _handle_test_source,
    _handle_validate_config,
)
from sync_app.cli.handlers.conflicts import (
    _handle_conflicts_apply_recommendation,
    _handle_conflicts_bulk,
    _handle_conflicts_archive,
    _handle_conflicts_dismiss,
    _handle_conflicts_list,
    _handle_conflicts_reopen,
    _handle_conflicts_resolve_binding,
    _handle_conflicts_skip_user,
)
from sync_app.cli.handlers.database import _handle_db_backup, _handle_db_check, _handle_db_restore_check
from sync_app.cli.handlers.sync import _handle_approve_plan, _handle_sync, _handle_version
from sync_app.cli.handlers.web import (
    _handle_bootstrap_admin,
    _handle_gui,
    _handle_init_web,
    _handle_web,
)


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
    password_source_group = bootstrap_admin_parser.add_mutually_exclusive_group()
    password_source_group.add_argument("--password", default="", help="Administrator password")
    password_source_group.add_argument(
        "--password-env",
        default="",
        help="Environment variable name that stores the administrator password",
    )
    password_source_group.add_argument(
        "--password-file",
        default="",
        help="UTF-8 file that stores the administrator password, suitable for container secrets",
    )
    bootstrap_admin_parser.add_argument(
        "--role",
        choices=WEB_ADMIN_ROLES,
        default="super_admin",
        help="Role for a newly created account",
    )
    existing_account_group = bootstrap_admin_parser.add_mutually_exclusive_group()
    existing_account_group.add_argument(
        "--reset",
        action="store_true",
        help="Reset the password when the administrator already exists",
    )
    existing_account_group.add_argument(
        "--if-missing",
        action="store_true",
        help="Create the administrator only when it does not already exist",
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
    approve_parser.add_argument("--org-id", default="default", help="Organization that owns the dry-run job")
    approve_parser.add_argument("--reviewer", default=None, help="Reviewer name, defaults to current OS user")
    approve_parser.add_argument("--notes", default="", help="Optional review notes")
    approve_parser.add_argument(
        "--ttl-minutes",
        type=int,
        default=None,
        help="Approval validity window; defaults to the configured high-risk review TTL",
    )
    approve_parser.set_defaults(handler=_handle_approve_plan)

    conflicts_parser = subparsers.add_parser("conflicts", help="Inspect or manage sync conflicts")
    conflict_subparsers = conflicts_parser.add_subparsers(dest="conflict_command", required=True)

    conflict_list_parser = conflict_subparsers.add_parser("list", help="List recorded sync conflicts")
    conflict_list_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    conflict_list_parser.add_argument("--job-id", default=None, help="Filter by job ID")
    conflict_list_parser.add_argument("--plan-id", default=None, help="Filter by plan ID")
    conflict_list_parser.add_argument(
        "--status",
        choices=["open", "resolved", "ignored", "archived", "dismissed", "all"],
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

    conflict_dismiss_parser = conflict_subparsers.add_parser("dismiss", help="Legacy alias for ignoring a conflict")
    conflict_dismiss_parser.add_argument("conflict_id", type=int, help="Conflict ID")
    conflict_dismiss_parser.add_argument("--notes", default="", help="Optional dismiss notes")
    conflict_dismiss_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    conflict_dismiss_parser.set_defaults(handler=_handle_conflicts_dismiss)

    conflict_ignore_parser = conflict_subparsers.add_parser("ignore", help="Ignore an open conflict with an auditable reason")
    conflict_ignore_parser.add_argument("conflict_id", type=int, help="Conflict ID")
    conflict_ignore_parser.add_argument("--notes", default="", help="Optional ignore notes")
    conflict_ignore_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    conflict_ignore_parser.set_defaults(handler=_handle_conflicts_dismiss)

    conflict_archive_parser = conflict_subparsers.add_parser("archive", help="Archive conflict evidence")
    conflict_archive_parser.add_argument("conflict_id", type=int, help="Conflict ID")
    conflict_archive_parser.add_argument("--notes", default="", help="Optional archive notes")
    conflict_archive_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    conflict_archive_parser.set_defaults(handler=_handle_conflicts_archive)

    conflict_reopen_parser = conflict_subparsers.add_parser("reopen", help="Reopen a resolved, ignored, or archived conflict")
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
        choices=["apply-recommendation", "skip-user-sync", "dismiss", "ignore", "archive", "reopen"],
        required=True,
        help="Bulk action to execute",
    )
    conflict_bulk_parser.add_argument("conflict_ids", nargs="+", type=int, help="Conflict IDs")
    conflict_bulk_parser.add_argument("--notes", default="", help="Optional notes for bulk action")
    conflict_bulk_parser.add_argument("--confirm", required=True, help="Type CONFIRM to authorize the bulk change")
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

    db_restore_check_parser = subparsers.add_parser(
        "db-restore-check",
        help="Restore a backup into an isolated database and verify recoverability",
    )
    db_restore_check_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
    db_restore_check_parser.add_argument(
        "--backup-path",
        default=None,
        help="Backup to verify; creates a fresh restore-drill backup when omitted",
    )
    db_restore_check_parser.add_argument("--json", action="store_true", help="Print machine-readable output")
    db_restore_check_parser.set_defaults(handler=_handle_db_restore_check)

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
