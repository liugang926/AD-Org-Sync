import re

import pytest
from django.test import Client

from sync_app import sspr
from sync_app.models import Audit, AuthPlatform, Configuration, EmployeePageSettings, EmployeeSession
from .fakes import Directory, Source, account


pytestmark = pytest.mark.django_db

STATUS_CASES = (
    (True, True, "AD账号受保护，不能自助重置；无需先同步或绑定，请联系AD管理员核查权限与保护状态"),
    (False, False, "AD账号已禁用，不能自助重置；无需先同步或绑定，请联系AD管理员核查账号启用状态"),
    (True, False, "AD账号已禁用，不能自助重置；无需先同步或绑定，请联系AD管理员核查账号启用状态"),
)


@pytest.fixture
def status_directory(monkeypatch):
    config = Configuration.current()
    config.sspr_enabled = True
    config.save()
    source, directory = Source(), Directory()
    monkeypatch.setattr(sspr, "DingTalk", lambda: source)
    monkeypatch.setattr(sspr, "ActiveDirectory", lambda: directory)
    return directory


def form_token(response, action):
    html = response.content.decode()
    form = re.search(r'<form[^>]*action="' + re.escape(action) + r'"[^>]*>(.*?)</form>', html, re.S)
    if action == "verify":
        form = re.search(r'<form id="verify"[^>]*>(.*?)</form>', html, re.S)
    assert form is not None
    tokens = re.findall(r'name="csrfmiddlewaretoken" value="([^"]+)"', form.group(1))
    assert len(tokens) == 1
    return tokens[0]


@pytest.mark.parametrize("is_protected,enabled,message", STATUS_CASES)
def test_real_auth_http_distinguishes_status_and_records_same_safe_reason(status_directory, is_protected, enabled, message):
    status_directory.items[0].update(protected=is_protected, enabled=enabled)
    client = Client(enforce_csrf_checks=True)
    token = form_token(client.get("/sspr"), "verify")
    response = client.post("/sspr/auth/dingtalk", {"code": "valid", "csrfmiddlewaretoken": token})

    assert response.status_code == 400 and response.json() == {"error": message}
    assert not EmployeeSession.objects.exists()
    assert "employee_verification" not in response.cookies
    assert status_directory.resets == 0
    denied = Audit.objects.get(action="sspr_auth_failed")
    assert denied.result == message and denied.state == "failed" and not denied.success
    assert denied.completed_at is not None
    assert denied.actor == "u1" and denied.target_username == "testuser"
    assert "testuser" not in response.content.decode()
    assert "Domain Admins" not in response.content.decode()
    assert "LDAP" not in response.content.decode()


@pytest.mark.parametrize("is_protected,enabled,message", STATUS_CASES)
def test_reset_rechecks_status_without_password_write_and_keeps_same_audit_reason(status_directory, is_protected, enabled, message):
    verification, _ = sspr.verify("valid", "198.51.100.20")
    client = Client(enforce_csrf_checks=True)
    client.cookies["employee_verification"] = verification
    token = form_token(client.get("/sspr"), "/sspr/reset")
    status_directory.items[0].update(protected=is_protected, enabled=enabled)
    response = client.post("/sspr/reset", {
        "csrfmiddlewaretoken": token, "password": "Ab1!xyza", "confirmation": "Ab1!xyza",
    })

    assert response.status_code == 400
    html = response.content.decode()
    assert message in html and "testuser" not in html and 'action="/sspr/reset"' not in html
    assert "钉钉核验后会显示本人当前AD账号；本服务无需先同步或绑定" in html
    assert "正在通过钉钉确认身份" not in html
    denied = Audit.objects.get(action="sspr_reset")
    assert denied.result == message and denied.state == "failed" and not denied.success
    assert denied.completed_at >= denied.created_at
    assert status_directory.resets == 0
    # The existing one-shot session is consumed on this claimed reset, as before.
    assert EmployeeSession.objects.count() == 1
    assert EmployeeSession.objects.get(digest=sspr.fingerprint(verification)).used
    assert "employee_verification" not in response.cookies


@pytest.mark.parametrize("is_protected,enabled,message", STATUS_CASES)
def test_fresh_account_denial_hides_account_and_platforms_without_loading_copy(status_directory, is_protected, enabled, message):
    page = EmployeePageSettings.current()
    page.save()
    AuthPlatform.objects.create(page_settings=page, name="仅验证成功才可见的平台")
    verification, _ = sspr.verify("valid", "198.51.100.20")
    client = Client()
    client.cookies["employee_verification"] = verification
    status_directory.items[0].update(protected=is_protected, enabled=enabled)
    response = client.get("/sspr")

    html = response.content.decode()
    assert response.status_code == 200 and message in html
    assert "testuser" not in html and "仅验证成功才可见的平台" not in html
    assert 'class="sspr-account"' not in html and 'action="/sspr/reset"' not in html
    assert "正在通过钉钉确认身份" not in html
    assert "钉钉核验后会显示本人当前AD账号；本服务无需先同步或绑定" in html
    assert not EmployeeSession.objects.get(digest=sspr.fingerprint(verification)).used
    assert status_directory.resets == 0


@pytest.mark.parametrize("mode,message", (
    ("missing", "未匹配到AD账号，请联系管理员核对钉钉身份字段和AD账号资料"),
    ("ambiguous", "匹配到多个AD账号，请联系管理员核对重复身份字段和AD账号资料"),
))
def test_not_unique_match_guides_identity_and_ad_data_review_without_binding_requirement(status_directory, mode, message):
    if mode == "missing":
        status_directory.items = []
    else:
        status_directory.items.append(account())
    client = Client(enforce_csrf_checks=True)
    token = form_token(client.get("/sspr"), "verify")
    response = client.post("/sspr/auth/dingtalk", {"code": "valid", "csrfmiddlewaretoken": token})
    assert response.status_code == 400 and response.json()["error"] == message
    denied = Audit.objects.get(action="sspr_auth_failed")
    assert denied.result == message and denied.state == "failed" and not denied.success
    assert not EmployeeSession.objects.exists()
    assert status_directory.resets == 0
    assert "同步" not in message and "绑定" not in message
