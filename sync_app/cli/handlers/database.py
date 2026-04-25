from __future__ import annotations

import argparse
import json

from sync_app.storage.local_db import DatabaseManager


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
