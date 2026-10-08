"""Employee authorization depends on live LDAPS identity, never sync status."""
import secrets
from datetime import timedelta
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from .directory import DingTalk, ActiveDirectory
from .domain import ResetOutcomeUnknown, RuleError, fingerprint, protected
from .locking import lock
from .models import Audit, Configuration, EmployeeSession
from .security import audit, normalized_ip, rate_limit


def _close_clients(*clients):
    # Closing a connection cannot change an already confirmed directory result.
    for client in clients:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def _finish_attempt(attempt, result, success, state):
    attempt.result, attempt.success, attempt.state = result, success, state
    attempt.completed_at = max(timezone.now(), attempt.created_at)
    try:
        attempt.save(update_fields=["result", "success", "state", "completed_at"])
        return True
    except Exception:
        # The pre-write record remains available when this update fails.
        return False


def _confirm_attempt_identity(attempt, user, account):
    # The live match confirms updated profile/login labels for the same actor and GUID.
    identity = _audit_identity(user=user, account=account)
    fields = ["actor_name", "employee_id", "target_username"]
    for field in fields:
        setattr(attempt, field, identity[field])
    try:
        attempt.save(update_fields=fields)
    except Exception:
        raise RuleError("审计暂时不可用，密码未提交；请稍后再试") from None


def _audit_identity(*, item=None, user=None, account=None):
    """Use only identities obtained from a server session or live providers."""
    if item is not None:
        return {
            "actor": item.source_id, "actor_name": item.display_name,
            "employee_id": item.employee_id, "target": str(item.object_guid),
            "target_username": item.target_username,
        }
    user, account = user or {}, account or {}
    return {
        "actor": str(user.get("source_id") or "未验证访客")[:150],
        "actor_name": str(user.get("name") or "")[:200],
        "employee_id": str(user.get("employee_id") or "")[:100],
        "target": str(account.get("guid") or "")[:150],
        "target_username": str(account.get("username") or "")[:100],
    }


def _record_denial(action, message, ip, **identity):
    try:
        audit(action=action, result=message, success=False, client_ip=ip, **identity)
    except Exception:
        raise RuleError("审计暂时不可用，密码未提交；请稍后再试") from None


def config_signature(config):
    return fingerprint([settings.DINGTALK_CORP_ID, settings.DINGTALK_APP_KEY, settings.LDAP_HOST, settings.LDAP_BASE_DN, settings.LDAP_VERIFY_CERT, settings.LDAP_CA_FILE, sorted(settings.SSPR_ALLOWED_DINGTALK_USER_IDS), config.sspr_match, config.sspr_enabled, config.updated_at])


def match_employee(source, ad, config, source_id=None, code=None, *, _identity=None):
    user = source.employee(code) if code is not None else source.user(source_id)
    if _identity is not None:
        _identity["user"] = user
    allowed = settings.SSPR_ALLOWED_DINGTALK_USER_IDS
    if allowed and user["source_id"] not in allowed:
        raise RuleError("员工密码重置尚未对当前账号开放")
    match_fields = {
        "employee_id": ("employee_id", "employee_id"),
        "email": ("email", "email"),
        "source_id": ("source_id", "source_id"),
        "employee_username": ("employee_id", "source_id"),
    }
    fields = match_fields.get(config.sspr_match)
    if fields is None:
        raise RuleError("密码重置匹配方式无效，请联系管理员核对配置")
    source_field, ad_field = fields
    value = user.get(source_field, "")
    if config.sspr_match == "employee_username" and not str(value or "").strip():
        raise RuleError("钉钉工号为空，不能匹配 AD 账号")
    matches = ad.match(ad_field, value)
    match_count = len(matches)
    if match_count == 0:
        raise RuleError("未匹配到AD账号，请联系管理员核对钉钉身份字段和AD账号资料")
    if match_count > 1:
        raise RuleError("匹配到多个AD账号，请联系管理员核对重复身份字段和AD账号资料")
    account = matches[0]
    if _identity is not None:
        _identity["account"] = account
    if protected(account):
        if not account.get("enabled", True):
            raise RuleError("AD账号受保护且已禁用，不能自助重置；无需先同步或绑定，请联系AD管理员核查权限、保护与启用状态")
        raise RuleError("AD账号受保护，不能自助重置；无需先同步或绑定，请联系AD管理员核查权限与保护状态")
    if not account["enabled"]:
        raise RuleError("AD账号已禁用，不能自助重置；无需先同步或绑定，请联系AD管理员核查账号启用状态")
    if not str(account.get("username") or "").strip() or not str(account.get("guid") or "").strip():
        raise RuleError("匹配账号缺少登录名或对象标识，请联系管理员核对")
    return user, account


def verify(code, ip):
    source = ad = None
    identity = {}
    try:
        if not code or len(code) > 4096:
            raise RuleError("缺少有效钉钉授权码")
        rate_limit("sspr-auth-ip:" + ip, 20)
        config = Configuration.current()
        if not config.sspr_enabled:
            raise RuleError("员工密码重置尚未开启")
        if not settings.DINGTALK_CORP_ID:
            raise RuleError("请管理员先配置钉钉企业 ID，再使用员工身份验证")
        source = DingTalk()
        ad = ActiveDirectory()
        user, account = match_employee(source, ad, config, code=code, _identity=identity)
        rate_limit("sspr-user:" + user["source_id"], 5)
        token = secrets.token_urlsafe(32)
        with transaction.atomic():
            EmployeeSession.objects.create(
                digest=fingerprint(token), source_id=user["source_id"], display_name=user["name"],
                employee_id=user.get("employee_id", ""), target_username=account["username"],
                object_guid=account["guid"], config_fingerprint=config_signature(config),
                expires_at=timezone.now() + timedelta(minutes=5),
            )
            audit(action="sspr_verified", client_ip=ip, **_audit_identity(user=user, account=account))
        return token, account
    except RuleError as exc:
        _record_denial("sspr_auth_failed", str(exc), ip, **_audit_identity(**identity))
        raise
    except Exception:
        message = "员工身份核验暂不可用，请稍后再试或联系管理员"
        _record_denial("sspr_auth_failed", message, ip, **_audit_identity(**identity))
        raise RuleError(message) from None
    finally:
        _close_clients(source, ad)


def session_for(token):
    item = EmployeeSession.objects.filter(digest=fingerprint(token), used=False, expires_at__gt=timezone.now()).first()
    config = Configuration.current()
    if not item or not config.sspr_enabled or item.config_fingerprint != config_signature(config):
        raise RuleError("验证已失效，请重新通过钉钉验证")
    return item, config


def current_account(token):
    """Show a verified account only after a fresh source and AD identity check."""
    item, config = session_for(token)
    source = ad = None
    try:
        source = DingTalk()
        ad = ActiveDirectory()
        user, target = match_employee(source, ad, config, source_id=item.source_id)
        if user["source_id"] != item.source_id:
            raise RuleError("钉钉身份发生变化，请重新验证")
        if target["guid"] != str(item.object_guid):
            raise RuleError("AD 匹配对象发生变化，请重新验证")
        if item.config_fingerprint != config_signature(Configuration.current()):
            raise RuleError("配置发生变化，请重新验证")
        return {"username": target["username"], "name": user["name"]}
    finally:
        _close_clients(source, ad)


def reset(token, password, confirmation, ip):
    # Expired or consumed server sessions still identify their rejected submission.
    # An arbitrary cookie never supplies the actor or target account itself.
    item = EmployeeSession.objects.filter(digest=fingerprint(token)).first()
    attempt = None
    try:
        rate_limit("sspr-reset-ip:" + ip, 20)
        item, config = session_for(token)
        if password != confirmation:
            raise RuleError("两次输入的新密码不一致")
        if not config.minimum_password_length <= len(password) <= 128:
            raise RuleError(f"新密码长度须为 {config.minimum_password_length}–128 位")
        rate_limit("sspr-reset-user:" + item.source_id, 5)
        with lock("account:" + str(item.object_guid)):
            # Claim the session and persist an uncertain attempt before any external write.
            # A process exit after LDAPS changes the password must leave evidence to review.
            try:
                with transaction.atomic():
                    claimed = EmployeeSession.objects.filter(pk=item.pk, used=False, expires_at__gt=timezone.now()).update(used=True)
                    if not claimed:
                        raise RuleError("验证已被使用，请重新验证")
                    attempt = Audit.objects.create(
                        action="sspr_reset", **_audit_identity(item=item), client_ip=normalized_ip(ip),
                        result="密码重置请求处理中，结果待确认", success=False, state="pending",
                    )
            except RuleError:
                raise
            except Exception:
                raise RuleError("审计暂时不可用，密码未提交；请稍后再试") from None
            source = ad = None
            write_started = False
            try:
                source = DingTalk()
                ad = ActiveDirectory()
                user, account = match_employee(source, ad, config, source_id=item.source_id)
                if user["source_id"] != item.source_id:
                    raise RuleError("钉钉身份发生变化，请重新验证")
                if account["guid"] != str(item.object_guid):
                    raise RuleError("AD 匹配对象发生变化，请重新验证")
                if item.config_fingerprint != config_signature(Configuration.current()):
                    raise RuleError("配置发生变化，请重新验证")
                _confirm_attempt_identity(attempt, user, account)
                write_started = True
                outcome = ad.reset_password(item.object_guid, password, config.unlock_after_reset)
            except ResetOutcomeUnknown as exc:
                _finish_attempt(attempt, str(exc), False, "unknown")
                raise
            except RuleError as exc:
                _finish_attempt(attempt, str(exc), False, "failed")
                raise
            except Exception:
                message = (
                    "目录响应中断，密码修改结果不明；请先验证或联系管理员"
                    if write_started else "身份复核暂时失败，密码未提交；请重新验证"
                )
                _finish_attempt(attempt, message, False, "unknown" if write_started else "failed")
                if write_started:
                    raise ResetOutcomeUnknown(message) from None
                raise RuleError(message) from None
            else:
                if not _finish_attempt(attempt, outcome.message, outcome.complete, "success" if outcome.complete else "partial"):
                    raise ResetOutcomeUnknown("密码修改结果记录暂不可用，请先验证或联系管理员")
                return outcome.message
            finally:
                _close_clients(source, ad)
    except RuleError as exc:
        if attempt is None:
            _record_denial("sspr_reset", str(exc), ip, **_audit_identity(item=item))
        raise
