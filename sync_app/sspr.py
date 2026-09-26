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
from .security import audit, rate_limit


def _close_clients(*clients):
    # Closing a connection cannot change an already confirmed directory result.
    for client in clients:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def _finish_attempt(attempt, result, success):
    attempt.result, attempt.success = result, success
    try:
        attempt.save(update_fields=["result", "success"])
        return True
    except Exception:
        # The pre-write record remains available when this update fails.
        return False


def config_signature(config):
    return fingerprint([settings.DINGTALK_CORP_ID, settings.DINGTALK_APP_KEY, settings.LDAP_HOST, settings.LDAP_BASE_DN, settings.LDAP_VERIFY_CERT, settings.LDAP_CA_FILE, sorted(settings.SSPR_ALLOWED_DINGTALK_USER_IDS), config.sspr_match, config.sspr_enabled, config.updated_at])


def match_employee(source, ad, config, source_id=None, code=None):
    user = source.employee(code) if code is not None else source.user(source_id)
    allowed = settings.SSPR_ALLOWED_DINGTALK_USER_IDS
    if allowed and user["source_id"] not in allowed:
        raise RuleError("员工密码重置尚未对当前账号开放")
    matches = ad.match(config.sspr_match, user.get(config.sspr_match, ""))
    if len(matches) != 1:
        raise RuleError("未唯一匹配 AD 账号，请联系管理员核对身份字段")
    account = matches[0]
    if protected(account) or not account["enabled"]:
        raise RuleError("匹配账号受保护或已禁用，不能自助重置")
    return user, account


def verify(code, ip):
    rate_limit("sspr-auth-ip:" + ip, 20)
    config = Configuration.current()
    if not config.sspr_enabled:
        raise RuleError("员工密码重置尚未开启")
    if not settings.DINGTALK_CORP_ID:
        raise RuleError("请管理员先配置钉钉企业 ID，再使用员工身份验证")
    source = DingTalk()
    ad = None
    try:
        ad = ActiveDirectory()
        user, account = match_employee(source, ad, config, code=code)
        rate_limit("sspr-user:" + user["source_id"], 5)
        token = secrets.token_urlsafe(32)
        EmployeeSession.objects.create(digest=fingerprint(token), source_id=user["source_id"], display_name=user["name"], object_guid=account["guid"], config_fingerprint=config_signature(config), expires_at=timezone.now() + timedelta(minutes=5))
        audit(user["source_id"], "sspr_verified", account["guid"])
        return token, account
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
    rate_limit("sspr-reset-ip:" + ip, 20)
    item, config = session_for(token)
    if password != confirmation or not config.minimum_password_length <= len(password) <= 128:
        raise RuleError("密码长度不符合要求或两次输入不一致")
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
                    actor=item.source_id, action="sspr_reset", target=str(item.object_guid),
                    result="密码重置请求处理中，结果待确认", success=False,
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
            write_started = True
            outcome = ad.reset_password(item.object_guid, password, config.unlock_after_reset)
        except RuleError as exc:
            _finish_attempt(attempt, str(exc), False)
            raise
        except Exception:
            message = (
                "目录响应中断，密码修改结果不明；请先验证或联系管理员"
                if write_started else "身份复核暂时失败，密码未提交；请重新验证"
            )
            _finish_attempt(attempt, message, False)
            if write_started:
                raise ResetOutcomeUnknown(message) from None
            raise RuleError(message) from None
        else:
            if not _finish_attempt(attempt, outcome.message, outcome.complete):
                raise ResetOutcomeUnknown("密码修改结果记录暂不可用，请先验证或联系管理员")
            return outcome.message
        finally:
            _close_clients(source, ad)
