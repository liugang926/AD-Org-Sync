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


def resolve(user: dict, binding: dict | None, accounts: list[dict], occupied: set[str], naming: str, employee_counts: Counter, name_counts: Counter, match_field: str = "employee_id", protected_usernames=()) -> tuple[str, dict | None, str]:
    if binding:
        matches = [a for a in accounts if a["guid"] == binding["guid"]]
        if not binding["enabled"]:
            if len(matches) == 1 and matches[0]["enabled"]:
                return "conflict", matches[0], "停用绑定的 AD 账号已启用，请人工核验"
            return "skip", None, "绑定已停用"
        if len(matches) != 1:
            return "conflict", None, "绑定目标不存在或无法唯一确认"
        target = matches[0]
        if protected(target) or not target["enabled"]:
            return "conflict", target, "目标受保护或已禁用，需人工处理"
        return "update", target, "使用已有绑定"
    identifier = user.get(match_field, "").strip().casefold()
    if not identifier or employee_counts[identifier] != 1:
        return "conflict", None, "匹配字段缺失或重复"
    employee = user.get("employee_id", "").strip().casefold()
    if not employee:
        return "conflict", None, "工号缺失或重复"
    target_field = "username" if match_field == "source_id" else match_field
    matches = [a for a in accounts if a.get(target_field, "").strip().casefold() == identifier]
    if len(matches) > 1:
        return "conflict", None, "多个 AD 账号命中同一标识"
    if matches:
        target = matches[0]
        if target["guid"] in occupied or protected(target) or not target["enabled"]:
            return "conflict", target, "AD 账号已占用、受保护或已禁用"
        if match_field != "employee_id":
            return "conflict", target, "建议关联此账号，请在人员页面人工确认"
        return "bind", target, "唯一工号匹配"
    if any(a.get("employee_id", "").strip().casefold() == employee for a in accounts):
        return "conflict", None, "工号已存在于其他 AD 账号，请人工核验"
    username = candidate(user, naming)
    if not username:
        return "conflict", None, "新账号命名字段为空或规范化后为空，请修正来源信息"
    if name_counts[username.casefold()] != 1:
        return "conflict", None, "新账号名在来源内重复或截断碰撞，请修正来源信息"
    if protected({"username": username}) or username.casefold() in {value.casefold() for value in protected_usernames}:
        return "conflict", None, "新账号名受保护，不能自动创建"
    if any(a["username"].casefold() == username.casefold() for a in accounts):
        return "conflict", None, "新账号名已被 AD 占用，请核验后人工绑定"
    return "create", None, "创建新账号"
