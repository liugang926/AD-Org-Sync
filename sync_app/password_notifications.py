"""A dedicated, non-replaying outbox for confirmed SSPR password changes."""
import base64
import hashlib
import hmac
import os
import re
import stat
import time
import unicodedata
from datetime import timedelta
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit
from zoneinfo import ZoneInfo

import requests
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .domain import RuleError
from .locking import lock
from .models import Audit, PasswordResetNotification

ROBOT_URL = "https://oapi.dingtalk.com/robot/send"
MINIMUM_INTERVAL = timedelta(seconds=3.1)


class RobotConfigurationError(Exception):
    """Never includes a file path, webhook, token or signing secret."""


def _secret_file(setting_name, *, optional=False):
    value = getattr(settings, setting_name, "")
    if not value:
        return ""
    try:
        path = Path(value)
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or path.is_symlink() or metadata.st_size > 8192:
            raise RobotConfigurationError()
        # Windows files use ACLs rather than POSIX mode bits; deployment owns ACLs.
        if os.name != "nt" and metadata.st_mode & 0o077:
            raise RobotConfigurationError()
        secret = path.read_text(encoding="utf-8").strip()
        if optional and not secret:
            raise RobotConfigurationError()
        return secret
    except FileNotFoundError:
        if optional:
            raise RobotConfigurationError() from None
        return ""
    except RobotConfigurationError:
        raise
    except Exception:
        raise RobotConfigurationError() from None


def _webhook_token(webhook):
    try:
        if any(character.isspace() or unicodedata.category(character).startswith("C") for character in webhook):
            raise RobotConfigurationError()
        parsed = urlsplit(webhook)
        if (parsed.scheme != "https" or parsed.hostname != "oapi.dingtalk.com"
                or parsed.port not in (None, 443) or parsed.path != "/robot/send"
                or parsed.username is not None or parsed.password is not None
                or "#" in webhook):
            raise RobotConfigurationError()
        pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
        if len(pairs) != 1 or pairs[0][0] != "access_token" or not re.fullmatch(r"[A-Za-z0-9_-]{1,512}", pairs[0][1]):
            raise RobotConfigurationError()
        return pairs[0][1]
    except RobotConfigurationError:
        raise
    except Exception:
        raise RobotConfigurationError() from None


def _eligible(audit):
    return audit.action == "sspr_reset" and audit.completed_at is not None and (
        (audit.state == "success" and audit.success is True)
        or (audit.state == "partial" and audit.success is False)
    )


def enqueue_password_notification(attempt):
    """Called only by the newly completed reset; never scans past audit records."""
    with transaction.atomic():
        audit = Audit.objects.get(pk=attempt.pk)
        if not _eligible(audit):
            return None
        if not getattr(settings, "DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE", ""):
            return None
        defaults = {}
        try:
            if not _secret_file("DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE"):
                raise RobotConfigurationError()
        except RobotConfigurationError:
            defaults = {
                "state": PasswordResetNotification.State.FAILED,
                "completed_at": timezone.now(),
                "message": "机器人受限配置文件不可用，未发送通知",
            }
        # A faulty later configuration must not overwrite an earlier outcome.
        notice, _ = PasswordResetNotification.objects.get_or_create(audit=audit, defaults=defaults)
        return notice


def _field(value, limit):
    # Untrusted profile fields cannot introduce a new template line or bidi control.
    text = "".join(" " if character.isspace() or unicodedata.category(character).startswith("C") else character for character in str(value or ""))
    return " ".join(text.split())[:limit] or "未记录"


def _text(audit):
    confirmed = timezone.localtime(audit.completed_at, ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
    result = "密码已修改；解锁未完整完成，请联系管理员" if audit.state == "partial" else "密码已修改，本服务已确认请求完整完成"
    return "\n".join([
        "AD 密码修改通知（本服务）",
        "员工姓名：" + _field(audit.actor_name, 200),
        "工号：" + _field(audit.employee_id, 100),
        "AD 账号：" + _field(audit.target_username, 100),
        "本服务结果确认时间：" + confirmed + "（北京时间）",
        "结果：" + result,
    ])


def _send(token, signing_secret, audit):
    params = {"access_token": token}
    if signing_secret:
        timestamp = str(int(time.time() * 1000))
        digest = hmac.new(signing_secret.encode("utf-8"), (timestamp + "\n" + signing_secret).encode("utf-8"), hashlib.sha256).digest()
        # requests encodes the base64 sign exactly once in the query string.
        params.update(timestamp=timestamp, sign=base64.b64encode(digest).decode("ascii"))
    payload = {"msgtype": "text", "text": {"content": _text(audit)}, "at": {"isAtAll": False}}
    try:
        with requests.Session() as http:
            http.trust_env = False
            response = http.post(ROBOT_URL, params=params, json=payload, verify=True, allow_redirects=False, timeout=(5, 15))
            if 400 <= response.status_code < 500:
                return PasswordResetNotification.State.FAILED, f"机器人请求被拒绝（HTTP {response.status_code}）"
            if response.status_code != 200:
                return PasswordResetNotification.State.UNKNOWN, "机器人未明确确认通知结果，不自动重发"
            data = response.json()
            code = data.get("errcode") if isinstance(data, dict) else None
            if type(code) is not int or not -(2 ** 31) <= code < 2 ** 31:
                return PasswordResetNotification.State.UNKNOWN, "机器人响应格式无法确认，不自动重发"
            if code == 0:
                return PasswordResetNotification.State.SENT, "机器人已接受通知"
            return PasswordResetNotification.State.FAILED, f"机器人拒绝通知（错误码 {code}）"
    except Exception:
        return PasswordResetNotification.State.UNKNOWN, "机器人连接或响应结果不明，不自动重发"


def _complete(notice, state, message):
    notice.state, notice.message, notice.completed_at = state, message, timezone.now()
    notice.save(update_fields=["state", "message", "completed_at"])


def process_one_password_notification():
    """At most one attempt under the shared lock; ambiguous attempts never replay."""
    try:
        with lock("password-reset-robot"):
            now = timezone.now()
            with transaction.atomic():
                # Owning the process lock proves a previous sender no longer runs.
                interrupted = PasswordResetNotification.objects.filter(state=PasswordResetNotification.State.SENDING).update(
                    state=PasswordResetNotification.State.UNKNOWN, completed_at=now,
                    message="发送进程中断，通知结果不明，不自动重发",
                )
                if interrupted:
                    return True
                recent = PasswordResetNotification.objects.filter(started_at__isnull=False).order_by("-started_at", "-pk").first()
                if recent and now < max(recent.started_at, recent.completed_at or recent.started_at) + MINIMUM_INTERVAL:
                    return False
                notice = PasswordResetNotification.objects.select_related("audit").filter(state=PasswordResetNotification.State.PENDING).order_by("created_at", "pk").first()
                if notice is None:
                    return False
                if not _eligible(notice.audit):
                    _complete(notice, PasswordResetNotification.State.FAILED, "改密审计未确认密码修改，未发送通知")
                    return True
                try:
                    if not getattr(settings, "DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE", ""):
                        return False
                    webhook = _secret_file("DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE")
                    if not webhook:
                        raise RobotConfigurationError()
                    token = _webhook_token(webhook)
                    signing_secret = _secret_file("DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_FILE", optional=True)
                except RobotConfigurationError:
                    _complete(notice, PasswordResetNotification.State.FAILED, "机器人受限配置文件或地址无效，未发送通知")
                    return True
                notice.state, notice.started_at, notice.message = PasswordResetNotification.State.SENDING, now, "正在提交机器人通知"
                notice.save(update_fields=["state", "started_at", "message"])
            state, message = _send(token, signing_secret, notice.audit)
            _complete(notice, state, message)
            return True
    except RuleError:
        return False
