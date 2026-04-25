from __future__ import annotations

DEFAULT_HARD_PROTECTED_GROUPS = [
    "Domain Admins",
    "Schema Admins",
    "Enterprise Admins",
    "Administrators",
    "Account Operators",
    "Server Operators",
    "Backup Operators",
    "Print Operators",
    "Domain Controllers",
    "Read-only Domain Controllers",
    "Protected Users",
    "Key Admins",
    "Enterprise Key Admins",
]


DEFAULT_SOFT_EXCLUDED_GROUPS = [
    "Domain Users",
    "Domain Guests",
    "Domain Computers",
    "Users",
    "Guests",
    "Replicator",
    "Group Policy Creator Owners",
]
