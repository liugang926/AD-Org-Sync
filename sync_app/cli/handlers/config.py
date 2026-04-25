from __future__ import annotations

import argparse
import json
import os
import sys

from sync_app.cli.common import (
    _get_cli_dependency,
    _load_cli_effective_config,
    _normalize_cli_org_id,
)
from sync_app.services.config_bundle import export_organization_bundle, import_organization_bundle
from sync_app.services.config_validation import (
    run_config_security_self_check,
    test_ldap_connection,
    validate_config,
)
from sync_app.storage.local_db import DatabaseManager


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
    success, message = _get_cli_dependency("test_source_connection")(
        config.source_connector.corpid,
        config.source_connector.corpsecret,
        config.source_connector.agentid,
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
