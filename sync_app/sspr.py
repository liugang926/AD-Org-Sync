"""Employee authorization depends on live LDAPS identity, never sync status."""
import secrets
from datetime import timedelta
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from .directory import DingTalk, ActiveDirectory
from .domain import RuleError, fingerprint, protected
from .locking import lock
from .models import Configuration, EmployeeSession
from .security import audit, rate_limit


def config_signature(config):
    return fingerprint([settings.DINGTALK_CORP_ID, settings.DINGTALK_APP_KEY, settings.LDAP_HOST, settings.LDAP_BASE_DN, config.sspr_match, config.sspr_enabled, config.updated_at])


def match_employee(source, ad, config, source_id=None, code=None):
    user = source.employee(code) if code is not None else source.user(source_id)
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
    source = DingTalk()
    ad = None
    try:
        ad = ActiveDirectory()
        user, account = match_employee(source, ad, config, code=code)
        rate_limit("sspr-user:" + user["source_id"], 5)
        token = secrets.token_urlsafe(32)
        EmployeeSession.objects.create(digest=fingerprint(token), source_id=user["source_id"], object_guid=account["guid"], config_fingerprint=config_signature(config), expires_at=timezone.now() + timedelta(minutes=5))
        audit(user["source_id"], "sspr_verified", account["guid"])
        return token, account
    finally:
        source.close()
        if ad:
            ad.close()


def session_for(token):
    item = EmployeeSession.objects.filter(digest=fingerprint(token), used=False, expires_at__gt=timezone.now()).first()
    config = Configuration.current()
    if not item or not config.sspr_enabled or item.config_fingerprint != config_signature(config):
        raise RuleError("验证已失效，请重新通过钉钉验证")
    return item, config


def reset(token, password, confirmation, ip):
    rate_limit("sspr-reset-ip:" + ip, 20)
    item, config = session_for(token)
    if password != confirmation or not config.minimum_password_length <= len(password) <= 128:
        raise RuleError("密码长度不符合要求或两次输入不一致")
    rate_limit("sspr-reset-user:" + item.source_id, 5)
    with lock("account:" + str(item.object_guid)):
        # Atomic claim BEFORE the external write: timeout/replay cannot repeat it.
        with transaction.atomic():
            claimed = EmployeeSession.objects.filter(pk=item.pk, used=False, expires_at__gt=timezone.now()).update(used=True)
            if not claimed:
                raise RuleError("验证已被使用，请重新验证")
        source = DingTalk()
        ad = None
        try:
            ad = ActiveDirectory()
            _, account = match_employee(source, ad, config, source_id=item.source_id)
            if account["guid"] != str(item.object_guid):
                raise RuleError("AD 匹配对象发生变化，请重新验证")
            if item.config_fingerprint != config_signature(Configuration.current()):
                raise RuleError("配置发生变化，请重新验证")
            result = ad.reset_password(item.object_guid, password, config.unlock_after_reset)
            audit(item.source_id, "sspr_reset", str(item.object_guid), result)
            return result
        except RuleError as exc:
            audit(item.source_id, "sspr_reset", str(item.object_guid), str(exc))
            raise
        finally:
            source.close()
            if ad:
                ad.close()
