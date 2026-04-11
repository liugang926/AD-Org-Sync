from __future__ import annotations

import base64
import hashlib
import logging
import os
from typing import Final

try:  # pragma: no cover - exercised on Windows deployments
    import win32crypt
except ImportError:  # pragma: no cover - keeps local tooling usable without pywin32
    win32crypt = None

LOGGER = logging.getLogger(__name__)
DPAPI_PREFIX: Final[str] = "dpapi:"
LOCAL_PREFIX: Final[str] = "local:"
FALLBACK_SALT: Final[bytes] = b"ad-org-sync::secret-store"
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
    normalized = str(value or "")
    return normalized.startswith(DPAPI_PREFIX) or normalized.startswith(LOCAL_PREFIX)


def can_use_dpapi() -> bool:
    return os.name == "nt" and win32crypt is not None


def _fallback_key() -> bytes:
    seed = "|".join(
        [
            os.name,
            os.environ.get("COMPUTERNAME", ""),
            os.environ.get("USERNAME", ""),
            os.path.expanduser("~"),
        ]
    ).encode("utf-8")
    return hashlib.sha256(FALLBACK_SALT + seed).digest()


def _xor_bytes(payload: bytes, key: bytes) -> bytes:
    return bytes(byte ^ key[index % len(key)] for index, byte in enumerate(payload))


def _protect_local_secret(plaintext: str) -> str:
    ciphertext = _xor_bytes(plaintext.encode("utf-8"), _fallback_key())
    return LOCAL_PREFIX + base64.b64encode(ciphertext).decode("ascii")


def _unprotect_local_secret(value: str) -> str:
    encoded_payload = value[len(LOCAL_PREFIX):]
    if not encoded_payload:
        return ""
    try:
        ciphertext = base64.b64decode(encoded_payload.encode("ascii"))
        return _xor_bytes(ciphertext, _fallback_key()).decode("utf-8")
    except Exception as exc:  # pragma: no cover - defensive fallback
        LOGGER.warning("failed to unprotect local secret fallback: %s", exc)
        return ""


def protect_secret(value: str | None) -> str:
    plaintext = str(value or "")
    if not plaintext:
        return ""
    if is_encrypted_secret(plaintext):
        return plaintext
    if not can_use_dpapi():
        return _protect_local_secret(plaintext)
    try:
        encrypted_bytes = win32crypt.CryptProtectData(plaintext.encode("utf-8"), None, None, None, None, 0)
        return DPAPI_PREFIX + base64.b64encode(encrypted_bytes).decode("ascii")
    except Exception as exc:  # pragma: no cover - depends on host DPAPI state
        LOGGER.warning("failed to protect secret with DPAPI, using local fallback: %s", exc)
        return _protect_local_secret(plaintext)


def unprotect_secret(value: str | None) -> str:
    encrypted_value = str(value or "")
    if not encrypted_value:
        return ""
    if encrypted_value.startswith(LOCAL_PREFIX):
        return _unprotect_local_secret(encrypted_value)
    if not encrypted_value.startswith(DPAPI_PREFIX):
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
