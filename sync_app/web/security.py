import base64
import hashlib
import hmac
import os
import secrets
from collections.abc import MutableMapping


PASSWORD_SCHEME = "pbkdf2_sha256"
PASSWORD_ITERATIONS = 200_000
CSRF_SESSION_KEY = "_csrf_token"
ADMIN_PASSWORD_MIN_LENGTH = 12


def hash_password(password: str, *, iterations: int = PASSWORD_ITERATIONS) -> str:
    salt = base64.b64encode(os.urandom(16)).decode("ascii")
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        iterations,
    )
    encoded = base64.b64encode(derived).decode("ascii")
    return f"{PASSWORD_SCHEME}${iterations}${salt}${encoded}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        scheme, iterations_text, salt, expected = password_hash.split("$", 3)
    except ValueError:
        return False
    if scheme != PASSWORD_SCHEME:
        return False
    try:
        iterations = int(iterations_text)
    except ValueError:
        return False

    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        iterations,
    )
    encoded = base64.b64encode(derived).decode("ascii")
    return hmac.compare_digest(encoded, expected)


def ensure_csrf_token(session: MutableMapping[str, str]) -> str:
    token = str(session.get(CSRF_SESSION_KEY) or "")
    if token:
        return token
    token = secrets.token_urlsafe(32)
    session[CSRF_SESSION_KEY] = token
    return token


def rotate_csrf_token(session: MutableMapping[str, str]) -> str:
    token = secrets.token_urlsafe(32)
    session[CSRF_SESSION_KEY] = token
    return token


def validate_csrf_token(session: MutableMapping[str, str], submitted_token: str) -> bool:
    expected = str(session.get(CSRF_SESSION_KEY) or "")
    provided = str(submitted_token or "")
    if not expected or not provided:
        return False
    return hmac.compare_digest(expected, provided)


def validate_admin_password_strength(password: str, *, min_length: int = ADMIN_PASSWORD_MIN_LENGTH) -> str | None:
    normalized_password = str(password or "")
    minimum = max(int(min_length or ADMIN_PASSWORD_MIN_LENGTH), 8)
    if len(normalized_password) < minimum:
        return f"Password must be at least {minimum} characters long"
    if not any(char.islower() for char in normalized_password):
        return "Password must include a lowercase letter"
    if not any(char.isupper() for char in normalized_password):
        return "Password must include an uppercase letter"
    if not any(char.isdigit() for char in normalized_password):
        return "Password must include a digit"
    if not any(not char.isalnum() for char in normalized_password):
        return "Password must include a symbol"
    return None
