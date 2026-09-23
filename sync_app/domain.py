"""Pure identity rules extracted from the original synchronization semantics."""
import hashlib
import json
import re
from collections import Counter


class RuleError(Exception):
    """Only safe, actionable text may be attached to this exception."""


def fingerprint(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()


def candidate(user: dict, strategy: str) -> str:
    raw = str(user.get(strategy, "")).strip()
    if strategy == "email":
        raw = raw.split("@", 1)[0]
    return re.sub(r'["/\\\[\]:;|=,+*?<>\x00-\x1f]', "", raw)[:20].strip(" .")


def protected(account: dict) -> bool:
    return bool(account.get("protected")) or account.get("username", "").casefold() in {"admin", "administrator", "guest", "krbtgt", "defaultaccount", "wdagutilityaccount"}


def resolve(user: dict, binding: dict | None, accounts: list[dict], occupied: set[str], naming: str, employee_counts: Counter, name_counts: Counter) -> tuple[str, dict | None, str]:
    if binding:
        matches = [a for a in accounts if a["guid"] == binding["guid"]]
        if not binding["enabled"]:
            return "skip", None, "绑定已停用"
        if len(matches) != 1:
            return "conflict", None, "绑定目标不存在或无法唯一确认"
        target = matches[0]
        if protected(target) or not target["enabled"]:
            return "conflict", target, "目标受保护或已禁用，需人工处理"
        return "update", target, "使用已有绑定"
    employee = user.get("employee_id", "").casefold()
    if not employee or employee_counts[employee] != 1:
        return "conflict", None, "工号缺失或重复"
    matches = [a for a in accounts if a.get("employee_id", "").casefold() == employee]
    if len(matches) > 1:
        return "conflict", None, "多个 AD 账号命中同一工号"
    if matches:
        target = matches[0]
        if target["guid"] in occupied or protected(target) or not target["enabled"]:
            return "conflict", target, "AD 账号已占用、受保护或已禁用"
        return "bind", target, "唯一工号匹配"
    username = candidate(user, naming)
    if not username or name_counts[username.casefold()] != 1 or any(a["username"].casefold() == username.casefold() for a in accounts) or protected({"username": username}):
        return "conflict", None, "新账号命名为空、重复、被占用或受保护"
    return "create", None, "创建新账号"
