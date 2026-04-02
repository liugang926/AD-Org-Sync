from __future__ import annotations

from typing import Iterable


DEFAULT_PROTECTED_AD_ACCOUNTS = [
    "admin",
    "administrator",
    "guest",
    "krbtgt",
    "defaultaccount",
    "wdagutilityaccount",
]

_DEFAULT_PROTECTED_AD_ACCOUNT_SET = {
    str(account).strip().lower()
    for account in DEFAULT_PROTECTED_AD_ACCOUNTS
    if str(account).strip()
}


def merge_protected_ad_accounts(accounts: Iterable[str] | None = None) -> list[str]:
    merged = list(DEFAULT_PROTECTED_AD_ACCOUNTS)
    seen = set(_DEFAULT_PROTECTED_AD_ACCOUNT_SET)
    for account in accounts or ():
        normalized = str(account or "").strip()
        if not normalized:
            continue
        lowered = normalized.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        merged.append(normalized)
    return merged


def filter_custom_protected_ad_accounts(accounts: Iterable[str] | None = None) -> list[str]:
    custom: list[str] = []
    seen = set()
    for account in accounts or ():
        normalized = str(account or "").strip()
        if not normalized:
            continue
        lowered = normalized.lower()
        if lowered in _DEFAULT_PROTECTED_AD_ACCOUNT_SET or lowered in seen:
            continue
        seen.add(lowered)
        custom.append(normalized)
    return custom


def is_protected_ad_account_name(
    username: str | None,
    extra_accounts: Iterable[str] | None = None,
) -> bool:
    normalized = str(username or "").strip().lower()
    if not normalized:
        return False
    if normalized in _DEFAULT_PROTECTED_AD_ACCOUNT_SET:
        return True
    return normalized in {
        str(account or "").strip().lower()
        for account in (extra_accounts or ())
        if str(account or "").strip()
    }
