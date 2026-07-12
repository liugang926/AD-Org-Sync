from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import urlencode, urlparse

import requests


class OIDCError(RuntimeError):
    """A safe-to-handle OIDC protocol or configuration failure."""


def _env_bool(name: str, default: bool = False) -> bool:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _csv_values(value: str | None) -> tuple[str, ...]:
    return tuple(item.strip() for item in str(value or "").split(",") if item.strip())


def _validated_external_url(value: str, *, field_name: str) -> str:
    candidate = str(value or "").strip()
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise OIDCError(f"invalid {field_name}")
    if parsed.fragment:
        raise OIDCError(f"invalid {field_name}")
    if parsed.scheme != "https" and parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise OIDCError(f"{field_name} must use HTTPS")
    return candidate


@dataclass(frozen=True, slots=True)
class OIDCSettings:
    enabled: bool
    discovery_url: str
    client_id: str
    client_secret: str
    display_name: str = "Corporate SSO"
    scopes: tuple[str, ...] = ("openid", "profile", "email")
    username_claim: str = "preferred_username"
    callback_url: str = ""
    password_reset_url: str = ""
    environment_label: str = ""
    token_auth_method: str = "client_secret_post"
    mfa_required: bool = False
    accepted_mfa_methods: tuple[str, ...] = ("mfa", "otp", "hwk", "sms")

    @classmethod
    def from_environment(cls, *, default_environment_label: str = "") -> "OIDCSettings":
        scopes = tuple(os.environ.get("AD_ORG_SYNC_OIDC_SCOPES", "openid profile email").split())
        accepted_mfa_methods = tuple(
            item.lower()
            for item in _csv_values(
                os.environ.get("AD_ORG_SYNC_OIDC_ACCEPTED_MFA_METHODS", "mfa,otp,hwk,sms")
            )
        )
        return cls(
            enabled=_env_bool("AD_ORG_SYNC_OIDC_ENABLED", False),
            discovery_url=os.environ.get("AD_ORG_SYNC_OIDC_DISCOVERY_URL", "").strip(),
            client_id=os.environ.get("AD_ORG_SYNC_OIDC_CLIENT_ID", "").strip(),
            client_secret=os.environ.get("AD_ORG_SYNC_OIDC_CLIENT_SECRET", "").strip(),
            display_name=os.environ.get("AD_ORG_SYNC_OIDC_DISPLAY_NAME", "Corporate SSO").strip()
            or "Corporate SSO",
            scopes=scopes or ("openid", "profile", "email"),
            username_claim=os.environ.get("AD_ORG_SYNC_OIDC_USERNAME_CLAIM", "preferred_username").strip()
            or "preferred_username",
            callback_url=os.environ.get("AD_ORG_SYNC_OIDC_CALLBACK_URL", "").strip(),
            password_reset_url=os.environ.get("AD_ORG_SYNC_PASSWORD_RESET_URL", "").strip(),
            environment_label=os.environ.get(
                "AD_ORG_SYNC_ENVIRONMENT_LABEL", default_environment_label
            ).strip(),
            token_auth_method=os.environ.get(
                "AD_ORG_SYNC_OIDC_TOKEN_AUTH_METHOD", "client_secret_post"
            ).strip(),
            mfa_required=_env_bool("AD_ORG_SYNC_OIDC_MFA_REQUIRED", False),
            accepted_mfa_methods=accepted_mfa_methods or ("mfa",),
        )

    @property
    def configured(self) -> bool:
        return bool(self.enabled and self.discovery_url and self.client_id and self.client_secret)

    @property
    def configuration_error(self) -> str:
        if not self.enabled:
            return ""
        missing = [
            name
            for name, value in (
                ("discovery URL", self.discovery_url),
                ("client ID", self.client_id),
                ("client secret", self.client_secret),
            )
            if not value
        ]
        return ", ".join(missing)


@dataclass(frozen=True, slots=True)
class OIDCIdentity:
    username: str
    subject: str
    issuer: str
    mfa_methods: tuple[str, ...]

    @property
    def mfa_satisfied(self) -> bool:
        return bool(self.mfa_methods)


class OIDCService:
    def __init__(
        self,
        settings: OIDCSettings,
        *,
        http_session: Any | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.settings = settings
        self.http = http_session or requests.Session()
        self.clock = clock
        self._discovery: dict[str, Any] = {}
        self._discovery_expires_at = 0.0
        self._jwks: dict[str, Any] = {}
        self._jwks_expires_at = 0.0

    def resolve_callback_url(self, default_url: str) -> str:
        return _validated_external_url(
            self.settings.callback_url or default_url,
            field_name="OIDC callback URL",
        )

    def password_reset_url(self) -> str:
        if not self.settings.password_reset_url:
            return ""
        return _validated_external_url(
            self.settings.password_reset_url,
            field_name="password reset URL",
        )

    def _require_configured(self) -> None:
        if not self.settings.configured:
            raise OIDCError("OIDC is not configured")
        _validated_external_url(self.settings.discovery_url, field_name="OIDC discovery URL")
        if self.settings.token_auth_method not in {"client_secret_post", "client_secret_basic"}:
            raise OIDCError("unsupported OIDC token authentication method")

    def discovery(self) -> dict[str, Any]:
        self._require_configured()
        if self._discovery and self._discovery_expires_at > self.clock():
            return dict(self._discovery)
        try:
            response = self.http.get(self.settings.discovery_url, timeout=8)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            raise OIDCError("OIDC discovery failed") from exc
        if not isinstance(payload, dict):
            raise OIDCError("OIDC discovery returned invalid data")
        for key in ("issuer", "authorization_endpoint", "token_endpoint", "userinfo_endpoint", "jwks_uri"):
            payload[key] = _validated_external_url(str(payload.get(key) or ""), field_name=key)
        self._discovery = dict(payload)
        self._discovery_expires_at = self.clock() + 300
        return dict(payload)

    def jwks(self, metadata: dict[str, Any]) -> dict[str, Any]:
        if self._jwks and self._jwks_expires_at > self.clock():
            return dict(self._jwks)
        try:
            response = self.http.get(str(metadata["jwks_uri"]), timeout=8)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            raise OIDCError("OIDC signing keys could not be loaded") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("keys"), list):
            raise OIDCError("OIDC signing keys are invalid")
        self._jwks = dict(payload)
        self._jwks_expires_at = self.clock() + 300
        return dict(payload)

    @staticmethod
    def _pkce_challenge(verifier: str) -> str:
        digest = hashlib.sha256(verifier.encode("ascii")).digest()
        return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")

    def begin(self, *, redirect_uri: str) -> tuple[str, dict[str, str]]:
        metadata = self.discovery()
        state = secrets.token_urlsafe(32)
        nonce = secrets.token_urlsafe(32)
        verifier = secrets.token_urlsafe(64)
        params = {
            "client_id": self.settings.client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "response_mode": "query",
            "scope": " ".join(self.settings.scopes),
            "state": state,
            "nonce": nonce,
            "code_challenge": self._pkce_challenge(verifier),
            "code_challenge_method": "S256",
        }
        return f"{metadata['authorization_endpoint']}?{urlencode(params)}", {
            "state": state,
            "nonce": nonce,
            "verifier": verifier,
            "redirect_uri": redirect_uri,
        }

    @staticmethod
    def _base64url_decode(value: str) -> bytes:
        padded = str(value or "") + "=" * (-len(str(value or "")) % 4)
        try:
            return base64.urlsafe_b64decode(padded.encode("ascii"))
        except Exception as exc:
            raise OIDCError("OIDC token encoding is invalid") from exc

    def _decode_and_verify_id_token(
        self,
        id_token: str,
        *,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        parts = str(id_token or "").split(".")
        if len(parts) != 3:
            raise OIDCError("OIDC provider did not return a valid ID token")
        try:
            header = json.loads(self._base64url_decode(parts[0]))
            payload = json.loads(self._base64url_decode(parts[1]))
        except Exception as exc:
            raise OIDCError("OIDC provider returned an invalid ID token") from exc
        if not isinstance(header, dict) or not isinstance(payload, dict):
            raise OIDCError("OIDC provider returned an invalid ID token")
        if str(header.get("alg") or "") != "RS256":
            raise OIDCError("OIDC ID token must use RS256")
        key_id = str(header.get("kid") or "")
        key = next(
            (
                item
                for item in self.jwks(metadata).get("keys", [])
                if isinstance(item, dict)
                and str(item.get("kid") or "") == key_id
                and str(item.get("kty") or "") == "RSA"
                and str(item.get("use") or "sig") == "sig"
            ),
            None,
        )
        if not isinstance(key, dict):
            raise OIDCError("OIDC signing key was not found")
        try:
            modulus = int.from_bytes(self._base64url_decode(str(key.get("n") or "")), "big")
            exponent = int.from_bytes(self._base64url_decode(str(key.get("e") or "")), "big")
            signature = self._base64url_decode(parts[2])
        except Exception as exc:
            raise OIDCError("OIDC signing key is invalid") from exc
        if modulus.bit_length() < 2048 or exponent < 3:
            raise OIDCError("OIDC signing key is too weak")
        encoded_length = (modulus.bit_length() + 7) // 8
        if len(signature) != encoded_length:
            raise OIDCError("OIDC ID token signature is invalid")
        signing_input = f"{parts[0]}.{parts[1]}".encode("ascii")
        digest_info = bytes.fromhex("3031300d060960864801650304020105000420") + hashlib.sha256(
            signing_input
        ).digest()
        padding_length = encoded_length - len(digest_info) - 3
        if padding_length < 8:
            raise OIDCError("OIDC signing key is too weak")
        expected_message = b"\x00\x01" + (b"\xff" * padding_length) + b"\x00" + digest_info
        actual_message = pow(int.from_bytes(signature, "big"), exponent, modulus).to_bytes(
            encoded_length,
            "big",
        )
        if not hmac.compare_digest(actual_message, expected_message):
            raise OIDCError("OIDC ID token signature is invalid")
        return payload

    def _validate_id_token_claims(
        self,
        claims: dict[str, Any],
        *,
        metadata: dict[str, Any],
        nonce: str,
    ) -> None:
        if str(claims.get("iss") or "") != str(metadata.get("issuer") or ""):
            raise OIDCError("OIDC issuer mismatch")
        audience = claims.get("aud")
        audiences = {str(item) for item in audience} if isinstance(audience, list) else {str(audience or "")}
        if self.settings.client_id not in audiences:
            raise OIDCError("OIDC audience mismatch")
        if not hmac.compare_digest(str(claims.get("nonce") or ""), nonce):
            raise OIDCError("OIDC nonce mismatch")
        try:
            expires_at = float(claims.get("exp") or 0)
        except (TypeError, ValueError) as exc:
            raise OIDCError("OIDC token expiry is invalid") from exc
        if expires_at <= self.clock() - 30:
            raise OIDCError("OIDC token has expired")

    def finish(
        self,
        *,
        query: dict[str, str],
        transaction: dict[str, str],
    ) -> OIDCIdentity:
        if query.get("error"):
            raise OIDCError("OIDC provider rejected sign-in")
        code = str(query.get("code") or "")
        state = str(query.get("state") or "")
        expected_state = str(transaction.get("state") or "")
        if not code or not state or not expected_state or not hmac.compare_digest(state, expected_state):
            raise OIDCError("OIDC state validation failed")
        metadata = self.discovery()
        token_data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": str(transaction.get("redirect_uri") or ""),
            "code_verifier": str(transaction.get("verifier") or ""),
        }
        request_kwargs: dict[str, Any] = {"data": token_data, "timeout": 8}
        if self.settings.token_auth_method == "client_secret_basic":
            request_kwargs["auth"] = (self.settings.client_id, self.settings.client_secret)
        else:
            token_data["client_id"] = self.settings.client_id
            token_data["client_secret"] = self.settings.client_secret
        try:
            token_response = self.http.post(metadata["token_endpoint"], **request_kwargs)
            token_response.raise_for_status()
            token_payload = token_response.json()
        except Exception as exc:
            raise OIDCError("OIDC token exchange failed") from exc
        if not isinstance(token_payload, dict):
            raise OIDCError("OIDC token exchange returned invalid data")
        access_token = str(token_payload.get("access_token") or "")
        claims = self._decode_and_verify_id_token(
            str(token_payload.get("id_token") or ""),
            metadata=metadata,
        )
        self._validate_id_token_claims(
            claims,
            metadata=metadata,
            nonce=str(transaction.get("nonce") or ""),
        )
        if not access_token:
            raise OIDCError("OIDC provider did not return an access token")
        try:
            userinfo_response = self.http.get(
                metadata["userinfo_endpoint"],
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=8,
            )
            userinfo_response.raise_for_status()
            userinfo = userinfo_response.json()
        except Exception as exc:
            raise OIDCError("OIDC user information request failed") from exc
        if not isinstance(userinfo, dict):
            raise OIDCError("OIDC user information is invalid")
        subject = str(userinfo.get("sub") or "")
        if not subject or subject != str(claims.get("sub") or ""):
            raise OIDCError("OIDC subject mismatch")
        username = str(userinfo.get(self.settings.username_claim) or claims.get(self.settings.username_claim) or "").strip()
        if not username:
            raise OIDCError("OIDC username claim is missing")
        raw_amr = claims.get("amr") or userinfo.get("amr") or []
        mfa_methods = tuple(str(item).lower() for item in raw_amr if str(item).strip()) if isinstance(raw_amr, list) else ()
        if self.settings.mfa_required and not set(mfa_methods).intersection(self.settings.accepted_mfa_methods):
            raise OIDCError("OIDC sign-in did not satisfy MFA policy")
        return OIDCIdentity(
            username=username,
            subject=subject,
            issuer=str(metadata["issuer"]),
            mfa_methods=mfa_methods,
        )
