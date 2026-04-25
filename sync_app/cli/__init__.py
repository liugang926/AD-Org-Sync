from __future__ import annotations

from sync_app.cli.main import (
    _normalize_legacy_command_aliases,
    main,
    windows_selector_loop_factory,
)
from sync_app.cli.parser import build_parser
from sync_app.cli.handlers.config import (
    _handle_config_export,
    _handle_config_import,
    _handle_test_ldap,
    _handle_test_source,
    _handle_test_wecom,
    _handle_validate_config,
)
from sync_app.cli.handlers.conflicts import (
    _handle_conflicts_apply_recommendation,
    _handle_conflicts_bulk,
    _handle_conflicts_dismiss,
    _handle_conflicts_list,
    _handle_conflicts_reopen,
    _handle_conflicts_resolve_binding,
    _handle_conflicts_skip_user,
)
from sync_app.cli.handlers.database import _handle_db_backup, _handle_db_check
from sync_app.cli.handlers.sync import _handle_approve_plan, _handle_sync, _handle_version
from sync_app.cli.handlers.web import (
    _handle_bootstrap_admin,
    _handle_gui,
    _handle_init_web,
    _handle_web,
)
from sync_app.services.config_validation import test_source_connection
from sync_app.services.entry import main as run_sync

__all__ = [
    "build_parser",
    "main",
    "run_sync",
    "test_source_connection",
    "windows_selector_loop_factory",
]
