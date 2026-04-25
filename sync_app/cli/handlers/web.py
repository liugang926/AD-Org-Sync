from __future__ import annotations

import argparse
import getpass
import json
import os
import sys

from sync_app.cli.common import _open_db_manager
from sync_app.services.typed_settings import WebSecuritySettings
from sync_app.storage.local_db import (
    DatabaseManager,
    OrganizationConfigRepository,
    OrganizationRepository,
    SettingsRepository,
    WebAdminUserRepository,
)
from sync_app.web.security import hash_password, validate_admin_password_strength


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

    security_settings = WebSecuritySettings.load(settings_repo)
    password_error = validate_admin_password_strength(
        password,
        min_length=security_settings.admin_password_min_length,
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
