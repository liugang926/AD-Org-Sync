from typing import Any, Optional

LDAP_IMPORT_ERROR: Optional[ImportError] = None

try:
    from ldap3 import ALL, NTLM, SIMPLE, MODIFY_ADD, MODIFY_DELETE, MODIFY_REPLACE, Connection, Server, Tls
    from ldap3.core.exceptions import LDAPBindError, LDAPException
    from ldap3.utils.conv import escape_filter_chars
except ImportError as exc:
    LDAP_IMPORT_ERROR = exc
    ALL = NTLM = SIMPLE = MODIFY_ADD = MODIFY_DELETE = MODIFY_REPLACE = None
    Connection = Server = Tls = None

    class LDAPException(Exception):
        """Fallback LDAP exception when ldap3 is unavailable."""

    class LDAPBindError(LDAPException):
        """Fallback bind exception when ldap3 is unavailable."""

    def escape_filter_chars(value: str) -> str:
        return value


def ensure_ldap3_available() -> None:
    if LDAP_IMPORT_ERROR is not None:
        raise RuntimeError(f"ldap3 dependency unavailable: {LDAP_IMPORT_ERROR}")


__all__ = [
    "ALL",
    "Connection",
    "LDAPBindError",
    "LDAPException",
    "LDAP_IMPORT_ERROR",
    "MODIFY_ADD",
    "MODIFY_DELETE",
    "MODIFY_REPLACE",
    "NTLM",
    "SIMPLE",
    "Server",
    "Tls",
    "ensure_ldap3_available",
    "escape_filter_chars",
]
