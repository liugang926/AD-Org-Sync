from __future__ import annotations

from typing import Any, Mapping


AD_CAPABILITY_ORDER = (
    "network",
    "authentication",
    "read_users",
    "read_ous",
    "create_user",
    "update_user",
    "move_user",
    "disable_user",
    "password_operation",
    "ldaps_certificate",
)


def build_ad_capability_report(
    *,
    connected: bool,
    user_read_succeeded: bool,
    ou_read_succeeded: bool,
    directory_mode: str = "writable",
    use_ssl: bool = True,
    validate_cert: bool = True,
    write_probe_results: Mapping[str, bool] | None = None,
) -> dict[str, Any]:
    """Return an explicit, non-destructive AD capability matrix.

    Write permissions are never claimed merely because bind succeeded. They are
    marked unverified unless a dedicated disposable probe supplied a result.
    """

    normalized_mode = str(directory_mode or "writable").strip().lower()
    if normalized_mode not in {"read_only", "writable"}:
        raise ValueError("directory_mode must be read_only or writable")
    probes = dict(write_probe_results or {})

    def result(status: str, detail: str, *, verified: bool) -> dict[str, Any]:
        return {"status": status, "detail": detail, "verified": verified}

    capabilities = {
        "network": result(
            "success" if connected else "failed",
            "LDAP endpoint connection established" if connected else "LDAP endpoint connection failed",
            verified=True,
        ),
        "authentication": result(
            "success" if connected else "failed",
            "Directory bind authenticated" if connected else "Directory bind was not authenticated",
            verified=True,
        ),
        "read_users": result(
            "success" if user_read_succeeded else "failed",
            "User search completed" if user_read_succeeded else "User search failed",
            verified=True,
        ),
        "read_ous": result(
            "success" if ou_read_succeeded else "failed",
            "OU search completed" if ou_read_succeeded else "OU search failed",
            verified=True,
        ),
    }
    for key in (
        "create_user",
        "update_user",
        "move_user",
        "disable_user",
        "password_operation",
    ):
        if normalized_mode == "read_only":
            capabilities[key] = result(
                "blocked",
                "Blocked by configured read-only directory mode",
                verified=True,
            )
        elif key in probes:
            capabilities[key] = result(
                "success" if probes[key] else "failed",
                "Verified with a dedicated disposable probe object",
                verified=True,
            )
        else:
            capabilities[key] = result(
                "not_tested",
                "A dedicated disposable probe object is required for a safe write-permission test",
                verified=False,
            )
    if not use_ssl:
        capabilities["ldaps_certificate"] = result(
            "blocked",
            "LDAP transport is not using TLS",
            verified=True,
        )
    elif validate_cert:
        capabilities["ldaps_certificate"] = result(
            "success",
            "TLS certificate validation completed during connection",
            verified=True,
        )
    else:
        capabilities["ldaps_certificate"] = result(
            "warning",
            "TLS is enabled but certificate validation is disabled",
            verified=True,
        )
    return {
        "directory_mode": normalized_mode,
        "safe_probe_policy": "no_employee_object_mutation",
        "capabilities": {
            key: capabilities[key] for key in AD_CAPABILITY_ORDER
        },
    }


__all__ = ["AD_CAPABILITY_ORDER", "build_ad_capability_report"]
