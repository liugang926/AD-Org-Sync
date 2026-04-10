from __future__ import annotations

from importlib import import_module
from typing import Any

_ATTRIBUTE_MODULES = {
    "runtime": "sync_app.services.runtime",
    "main": "sync_app.services.entry",
    "run_sync_job": "sync_app.services.runtime",
    "SyncStateManager": "sync_app.services.state",
    "ADSync": "sync_app.services.ad_sync",
    "ADSyncLDAPS": "sync_app.services.ad_sync",
    "build_group_cn": "sync_app.services.ad_sync",
    "build_group_display_name": "sync_app.services.ad_sync",
    "build_group_sam": "sync_app.services.ad_sync",
    "_generate_skip_detail_report": "sync_app.services.reports",
    "_generate_sync_operation_log": "sync_app.services.reports",
    "_generate_sync_validation_report": "sync_app.services.reports",
}

__all__ = sorted(_ATTRIBUTE_MODULES)


def __getattr__(name: str) -> Any:
    module_name = _ATTRIBUTE_MODULES.get(name)
    if not module_name:
        raise AttributeError(f"module 'sync_app.services' has no attribute {name!r}")
    module = import_module(module_name)
    value = module if name == "runtime" else getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
