from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable

from sync_app.core.config import load_sync_config
from sync_app.core.models import AppConfig
from sync_app.storage.local_db import OrganizationConfigRepository, normalize_org_id


CONFIG_SOURCE_PRECEDENCE = (
    "explicit_file_override",
    "organization_database",
    "registered_legacy_file_import",
    "fallback_loader",
)


@dataclass(frozen=True, slots=True)
class ResolvedOrganizationConfig:
    config: AppConfig
    source_kind: str
    source_reference: str
    resolved_file_path: str
    legacy_file_imported: bool = False


def resolve_organization_config(
    organization_config_repo: OrganizationConfigRepository | None,
    *,
    org_id: str,
    config_path: str = "config.ini",
    explicit_file_override: bool = False,
    file_loader: Callable[[str], AppConfig] = load_sync_config,
) -> ResolvedOrganizationConfig:
    """Resolve configuration through one documented, deterministic precedence chain."""

    normalized_org_id = normalize_org_id(org_id)
    if not normalized_org_id:
        raise ValueError("Organization ID is required to resolve configuration")
    resolved_file_path = os.path.abspath(str(config_path or "").strip() or "config.ini")

    if explicit_file_override:
        if not os.path.isfile(resolved_file_path):
            raise FileNotFoundError(f"Configuration override file not found: {resolved_file_path}")
        config = file_loader(resolved_file_path)
        return ResolvedOrganizationConfig(
            config=config,
            source_kind="explicit_file_override",
            source_reference=resolved_file_path,
            resolved_file_path=resolved_file_path,
        )

    if organization_config_repo is None:
        raise ValueError("Organization configuration repository is required without an explicit file override")

    if organization_config_repo.has_config(normalized_org_id):
        config = organization_config_repo.get_app_config(
            normalized_org_id,
            config_path=resolved_file_path,
        )
        return ResolvedOrganizationConfig(
            config=config,
            source_kind="organization_database",
            source_reference=f"db:org:{normalized_org_id}",
            resolved_file_path=resolved_file_path,
        )

    if os.path.isfile(resolved_file_path):
        imported = organization_config_repo.import_legacy_config(
            normalized_org_id,
            config_path=resolved_file_path,
        )
        if not imported:
            raise RuntimeError(f"Configuration file could not be imported: {resolved_file_path}")
        config = organization_config_repo.get_app_config(
            normalized_org_id,
            config_path=resolved_file_path,
        )
        return ResolvedOrganizationConfig(
            config=config,
            source_kind="registered_legacy_file_import",
            source_reference=f"db:org:{normalized_org_id}",
            resolved_file_path=resolved_file_path,
            legacy_file_imported=True,
        )

    config = file_loader(resolved_file_path)
    return ResolvedOrganizationConfig(
        config=config,
        source_kind="fallback_loader",
        source_reference=str(getattr(config, "config_path", "") or resolved_file_path),
        resolved_file_path=resolved_file_path,
    )
