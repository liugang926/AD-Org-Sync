import pytest

from sync_app import sspr, synchronization
from sync_app.domain import ResetOutcomeUnknown, RuleError
from sync_app.models import AuthPlatform, Binding, Configuration, EmployeePageSettings, Job
from .fakes import Directory, Source


pytestmark = pytest.mark.django_db


@pytest.fixture
def employee_content():
    page = EmployeePageSettings.current()
    page.title = "企业账号密码服务"
    page.description = "同一个 AD 账号，按企业授权登录业务平台。"
    page.announcement = "服务公告：请先核对当前账号。"
    page.help_text = "遇到问题请联系服务台。"
    page.support_text = "支持分机：1234"
    page.save()
    AuthPlatform.objects.filter(page_settings=page).delete()
    return page


@pytest.fixture
def employee_directory(monkeypatch):
    config = Configuration.current()
    config.sspr_enabled = True
    config.root_ou = "OU=People,DC=example,DC=com"
    config.save()
    source, directory = Source(), Directory()
    monkeypatch.setattr(sspr, "DingTalk", lambda: source)
    monkeypatch.setattr(sspr, "ActiveDirectory", lambda: directory)
    return source, directory, config


def test_content_is_escaped_and_platforms_hidden_before_identity_check(client, employee_content):
    employee_content.title = '<script>alert("title")</script>'
    employee_content.announcement = '<img src=x onerror="alert(1)">'
    employee_content.save()
    AuthPlatform.objects.create(page_settings=employee_content, name="仅核验后可见的平台")
    for path in ("/sspr", "/sspr/callback/dingtalk", "/sspr/oauth/start"):
        response = client.get(path)
        html = response.content.decode()
        assert response.status_code == 200
        assert "&lt;script&gt;" in html and "&lt;img" in html
        assert '<script>alert("title")</script>' not in html
        assert '<img src=x onerror=' not in html
        assert "仅核验后可见的平台" not in html
        assert "服务尚未开启" in html
        assert not response.context["auth_platforms"]


def test_verified_account_shows_enabled_platforms_in_order_without_binding(client, employee_content, employee_directory):
    AuthPlatform.objects.create(page_settings=employee_content, name="Nextcloud", position=20)
    AuthPlatform.objects.create(page_settings=employee_content, name="已关闭平台", enabled=False)
    AuthPlatform.objects.create(
        page_settings=employee_content, name="VPN", position=10,
        login_url="https://vpn.example.com/", password_note="请退出后使用新密码重新登录。",
    )
    token, _ = sspr.verify("valid", "127.0.0.1")
    client.cookies["employee_verification"] = token
    html = client.get("/sspr").content.decode()
    assert not Binding.objects.exists()
    assert "当前 AD 账号" in html and "testuser" in html
    assert html.index("VPN") < html.index("Nextcloud")
    assert "已关闭平台" not in html
    assert 'href="https://vpn.example.com/"' in html
    assert 'rel="noopener noreferrer"' in html
    assert "具体访问权限以各平台授权为准" in html


def test_display_changes_preserve_real_session_and_sync_preview(client, employee_content, employee_directory):
    source, directory, config = employee_directory
    job = Job.objects.create()
    job.plan = synchronization.plan(job, source, directory)
    token, _ = sspr.verify("valid", "127.0.0.1")
    sync_signature = synchronization.configuration_signature(Configuration.current())
    session_signature = sspr.config_signature(Configuration.current())
    employee_content.title = "更新后的服务标题"
    employee_content.save()
    AuthPlatform.objects.create(page_settings=employee_content, name="AI 知识库")
    assert synchronization.configuration_signature(Configuration.current()) == sync_signature
    assert sspr.config_signature(Configuration.current()) == session_signature
    assert synchronization.apply(job, source, directory) == "success"
    client.cookies["employee_verification"] = token
    response = client.post("/sspr/reset", {"password": "Ab1!xyza", "confirmation": "Ab1!xyza"})
    html = response.content.decode()
    assert response.status_code == 200 and directory.resets == 1
    assert "更新后的服务标题" in html and "密码已成功重置" in html
    assert employee_content.help_text in html and employee_content.support_text in html
    assert "AI 知识库" not in html  # The successful reset consumed the verified session.
    config.refresh_from_db()
    config.minimum_password_length = 10
    config.save()
    assert sspr.config_signature(config) != session_signature


def test_retry_keeps_custom_copy_and_verified_platforms(client, employee_content, employee_directory):
    _, directory, _ = employee_directory
    AuthPlatform.objects.create(page_settings=employee_content, name="VPN")
    token, _ = sspr.verify("valid", "127.0.0.1")
    client.cookies["employee_verification"] = token
    response = client.post("/sspr/reset", {"password": "short", "confirmation": "short"})
    html = response.content.decode()
    assert response.status_code == 400 and directory.resets == 0
    assert employee_content.title in html and employee_content.announcement in html
    assert "VPN" in html and 'minlength="8"' in html


@pytest.mark.parametrize("error", [RuleError("验证已失效"), ResetOutcomeUnknown("结果待确认")])
def test_rejected_or_unknown_reset_keeps_copy_and_fixed_result_guidance(client, employee_content, monkeypatch, error):
    AuthPlatform.objects.create(page_settings=employee_content, name="保密平台")

    def reject(*args):
        raise error

    monkeypatch.setattr(sspr, "reset", reject)
    response = client.post("/sspr/reset", {"password": "Ab1!xyza", "confirmation": "Ab1!xyza"})
    html = response.content.decode()
    assert employee_content.title in html and employee_content.support_text in html
    assert str(error) in html and "保密平台" not in html
    if isinstance(error, ResetOutcomeUnknown):
        assert "不要立即重复提交" in html
    assert 'action="/sspr/reset"' not in html


def test_failed_fresh_account_check_hides_platforms(client, employee_content, employee_directory):
    _, directory, _ = employee_directory
    AuthPlatform.objects.create(page_settings=employee_content, name="核验成功才可见的平台")
    token, _ = sspr.verify("valid", "127.0.0.1")
    client.cookies["employee_verification"] = token
    directory.items[0]["enabled"] = False
    html = client.get("/sspr").content.decode()
    assert "核验成功才可见的平台" not in html and "testuser" not in html
    assert "AD账号已禁用，不能自助重置" in html


def test_csrf_rejection_uses_custom_copy_without_security_or_identity_bypass(employee_content):
    from django.test import Client

    AuthPlatform.objects.create(page_settings=employee_content, name="仅核验后显示")
    client = Client(enforce_csrf_checks=True)
    response = client.post("/sspr/reset", {"password": "Ab1!xyza", "confirmation": "Ab1!xyza"})
    html = response.content.decode()
    assert response.status_code == 403
    assert employee_content.title in html and employee_content.support_text in html
    assert "安全校验未通过，密码未提交" in html
    assert "重新打开密码服务并通过钉钉验证" in html
    assert "仅核验后显示" not in html and "服务尚未开启" not in html
    assert 'action="/sspr/reset"' not in html and 'id="verify"' not in html


def test_csrf_database_failure_still_returns_fixed_safe_rejection(monkeypatch):
    from django.db import OperationalError
    from django.test import Client
    from sync_app import views

    def unavailable(*args, **kwargs):
        raise OperationalError("private database diagnostic")

    monkeypatch.setattr(views, "audit", unavailable)
    monkeypatch.setattr(EmployeePageSettings, "current", unavailable)
    response = Client(enforce_csrf_checks=True).post("/sspr/reset", {"password": "Never-submit-this"})
    html = response.content.decode()
    assert response.status_code == 403
    assert "安全校验未通过，密码未提交" in html and "审计暂时不可用" in html
    assert "private database diagnostic" not in html and "Never-submit-this" not in html
    assert 'id="verify"' not in html and 'action="/sspr/reset"' not in html


def test_presentation_read_failure_cannot_hide_completed_reset(client, employee_directory, monkeypatch):
    from django.db import OperationalError

    _, directory, _ = employee_directory
    token, _ = sspr.verify("valid", "127.0.0.1")
    client.cookies["employee_verification"] = token

    def unavailable():
        raise OperationalError("private optional content diagnostic")

    monkeypatch.setattr(EmployeePageSettings, "current", unavailable)
    response = client.post("/sspr/reset", {"password": "Ab1!xyza", "confirmation": "Ab1!xyza"})
    html = response.content.decode()
    assert response.status_code == 200 and directory.resets == 1
    assert "密码已成功重置" in html and "重置我的 AD 密码" in html
    assert "private optional content diagnostic" not in html
