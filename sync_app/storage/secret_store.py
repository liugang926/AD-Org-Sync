from __future__ import annotations

import base64
import logging
import os
from typing import Final

try:  # pragma: no cover - exercised on Windows deployments
    import win32crypt
except ImportError:  # pragma: no cover - keeps local tooling usable without pywin32
    win32crypt = None

LOGGER = logging.getLogger(__name__)
DPAPI_PREFIX: Final[str] = "dpapi:"
ORGANIZATION_SECRET_FIELDS: Final[set[str]] = {
    "corpsecret",
    "webhook_url",
    "ldap_password",
    "default_password",
}
CONNECTOR_SECRET_FIELDS: Final[set[str]] = {
    "ldap_password",
    "default_password",
}


def is_encrypted_secret(value: str | None) -> bool:
    return str(value or "").startswith(DPAPI_PREFIX)


def can_use_dpapi() -> bool:
    return os.name == "nt" and win32crypt is not None


def protect_secret(value: str | None) -> str:
    plaintext = str(value or "")
    if not plaintext:
        return ""
    if is_encrypted_secret(plaintext):
        return plaintext
    if not can_use_dpapi():
        return plaintext
    try:
        encrypted_bytes = win32crypt.CryptProtectData(plaintext.encode("utf-8"), None, None, None, None, 0)
        return DPAPI_PREFIX + base64.b64encode(encrypted_bytes).decode("ascii")
    except Exception as exc:  # pragma: no cover - depends on host DPAPI state
        LOGGER.warning("failed to protect secret with DPAPI, storing plaintext fallback: %s", exc)
        return plaintext


def unprotect_secret(value: str | None) -> str:
    encrypted_value = str(value or "")
    if not encrypted_value:
        return ""
    if not is_encrypted_secret(encrypted_value):
        return encrypted_value
    encoded_payload = encrypted_value[len(DPAPI_PREFIX):]
    if not encoded_payload:
        return ""
    if not can_use_dpapi():
        LOGGER.warning("DPAPI secret detected but pywin32 is unavailable; returning empty value")
        return ""
    try:
        encrypted_bytes = base64.b64decode(encoded_payload.encode("ascii"))
        decrypted_bytes = win32crypt.CryptUnprotectData(encrypted_bytes, None, None, None, 0)[1]
        return decrypted_bytes.decode("utf-8")
    except Exception as exc:  # pragma: no cover - depends on host DPAPI state
        LOGGER.warning("failed to unprotect DPAPI secret: %s", exc)
        return ""
