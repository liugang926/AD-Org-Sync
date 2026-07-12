from __future__ import annotations

from dataclasses import dataclass

from sync_app.storage.local_db import normalize_org_id


@dataclass(frozen=True, slots=True)
class TenantContext:
    """Mandatory organization and actor identity for application use cases."""

    org_id: str
    actor_username: str
    channel: str = "system"

    @classmethod
    def create(
        cls,
        *,
        org_id: str,
        actor_username: str,
        channel: str = "system",
    ) -> "TenantContext":
        normalized_org_id = normalize_org_id(org_id)
        if not normalized_org_id:
            raise ValueError("Organization ID is required")
        normalized_actor = str(actor_username or "").strip()
        if not normalized_actor:
            raise ValueError("Actor username is required")
        normalized_channel = str(channel or "system").strip().lower() or "system"
        return cls(
            org_id=normalized_org_id,
            actor_username=normalized_actor,
            channel=normalized_channel,
        )
