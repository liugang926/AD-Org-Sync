from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from sync_app.providers.source import get_source_provider_display_name, normalize_source_provider


@dataclass(frozen=True, slots=True)
class SSPROAuthLaunch:
    provider_id: str
    provider_label: str
    authorization_url: str = ""
    callback_url: str = ""
    error: str = ""

    @property
    def available(self) -> bool:
        return bool(self.authorization_url and not self.error)


def build_sspr_oauth_launch(
    *,
    provider_id: str,
    client_id: str,
    callback_url: str,
    org_id: str,
    source_user_id: str,
) -> SSPROAuthLaunch:
    normalized_provider = normalize_source_provider(provider_id)
    provider_label = get_source_provider_display_name(normalized_provider)
    normalized_client_id = str(client_id or "").strip()
    normalized_callback_url = str(callback_url or "").strip()
    normalized_source_user_id = str(source_user_id or "").strip()
    if normalized_provider not in {"wecom", "dingtalk"}:
        return SSPROAuthLaunch(
            provider_id=normalized_provider,
            provider_label=provider_label,
            callback_url=normalized_callback_url,
            error="Provider OAuth verification is not available for this source provider yet.",
        )
    if not normalized_client_id:
        return SSPROAuthLaunch(
            provider_id=normalized_provider,
            provider_label=provider_label,
            callback_url=normalized_callback_url,
            error="Source provider client ID is not configured.",
        )
    if not normalized_callback_url:
        return SSPROAuthLaunch(
            provider_id=normalized_provider,
            provider_label=provider_label,
            error="SSPR callback URL is not configured.",
        )
    if not normalized_source_user_id:
        return SSPROAuthLaunch(
            provider_id=normalized_provider,
            provider_label=provider_label,
            callback_url=normalized_callback_url,
            error="Employee ID is required.",
        )

    state = encode_sspr_oauth_state(
        {
            "org_id": str(org_id or "").strip().lower() or "default",
            "source_user_id": normalized_source_user_id,
            "provider_id": normalized_provider,
        }
    )
    if normalized_provider == "wecom":
        params = {
            "appid": normalized_client_id,
            "redirect_uri": normalized_callback_url,
            "response_type": "code",
            "scope": "snsapi_base",
            "state": state,
        }
        return SSPROAuthLaunch(
            provider_id=normalized_provider,
            provider_label=provider_label,
            authorization_url=f"https://open.weixin.qq.com/connect/oauth2/authorize?{urlencode(params)}#wechat_redirect",
            callback_url=normalized_callback_url,
        )

    params = {
        "redirect_uri": normalized_callback_url,
        "response_type": "code",
        "client_id": normalized_client_id,
        "scope": "openid",
        "state": state,
        "prompt": "consent",
    }
    return SSPROAuthLaunch(
        provider_id=normalized_provider,
        provider_label=provider_label,
        authorization_url=f"https://login.dingtalk.com/oauth2/auth?{urlencode(params)}",
        callback_url=normalized_callback_url,
    )


def encode_sspr_oauth_state(values: dict[str, Any]) -> str:
    payload = {
        str(key): str(value)
        for key, value in values.items()
        if value not in (None, "")
    }
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("utf-8")
    return encoded.rstrip("=")
