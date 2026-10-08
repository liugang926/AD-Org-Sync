import uuid

import pytest
from django.test import Client

from sync_app import sspr
from sync_app.domain import RuleError
from sync_app.models import Audit, AuthPlatform, Binding, Configuration, EmployeePageSettings, EmployeeSession, Job
from .fakes import Directory, Source, account
from .test_sspr_status import form_token


pytestmark = pytest.mark.django_db


@pytest.fixture
def domain_admin_runtime(monkeypatch):
    config = Configuration.current()
    config.sspr_enabled = True
    config.save()
    source, directory = Source(), Directory()
    directory.items[0].update(protected=True, domain_admin=True)
    monkeypatch.setattr(sspr, "DingTalk", lambda: source)
    monkeypatch.setattr(sspr, "ActiveDirectory", lambda: directory)
    return source, directory, config


@pytest.mark.parametrize("username", ["testuser", "Administrator"])
def test_domain_admin_real_csrf_http_can_verify_view_and_reset_without_binding(domain_admin_runtime, username):
    _, directory, _ = domain_admin_runtime
    directory.items[0]["username"] = username
    page = EmployeePageSettings.current()
    page.save()
    AuthPlatform.objects.create(page_settings=page, name="VPN")
    client = Client(enforce_csrf_checks=True)
    anonymous = client.get("/sspr")
    assert username not in anonymous.content.decode() and "VPN" not in anonymous.content.decode()
    response = client.post("/sspr/auth/dingtalk", {
        "code": "valid", "csrfmiddlewaretoken": form_token(anonymous, "verify"),
    })
    assert response.status_code == 200 and response.json() == {"next": "/sspr"}
    cookie = response.cookies["employee_verification"]
    assert cookie["secure"] and cookie["httponly"]
    assert EmployeeSession.objects.count() == 1
    assert not Binding.objects.exists() and not Job.objects.exists()
    verified = client.get("/sspr")
    html = verified.content.decode()
    assert username in html and "VPN" in html and 'action="/sspr/reset"' in html
    response = client.post("/sspr/reset", {
        "password": "Ab1!xyza", "confirmation": "Ab1!xyza",
        "csrfmiddlewaretoken": form_token(verified, "/sspr/reset"),
    })
    assert response.status_code == 200 and "密码已成功重置" in response.content.decode()
    assert directory.resets == 1 and EmployeeSession.objects.get().used
    record = Audit.objects.get(action="sspr_reset")
    assert record.success and record.state == "success" and record.target_username == username
    assert record.actor == "u1" and record.target == directory.items[0]["guid"]
    assert "Ab1!xyza" not in str(list(Audit.objects.values()))
    assert not Binding.objects.exists() and not Job.objects.exists()
    response = client.post("/sspr/reset", {
        "password": "Ab1!xyza", "confirmation": "Ab1!xyza",
        "csrfmiddlewaretoken": form_token(client.get("/sspr"), "verify"),
    })
    assert response.status_code == 400 and directory.resets == 1


@pytest.mark.parametrize("step", ["current_account", "reset"])
@pytest.mark.parametrize("change", ["membership", "disabled", "guid", "config"])
def test_domain_admin_still_rechecks_membership_enabled_guid_and_configuration(domain_admin_runtime, step, change):
    _, directory, config = domain_admin_runtime
    token, _ = sspr.verify("valid", "192.0.2.25")
    if change == "membership":
        directory.items[0]["domain_admin"] = False
    elif change == "disabled":
        directory.items[0]["enabled"] = False
    elif change == "guid":
        directory.items[0]["guid"] = str(uuid.uuid4())
    else:
        config.minimum_password_length = 9
        config.save()
    with pytest.raises(RuleError):
        if step == "current_account":
            sspr.current_account(token)
        else:
            sspr.reset(token, "Ab1!xyzab", "Ab1!xyzab", "192.0.2.25")
    assert directory.resets == 0
    if step == "reset":
        assert Audit.objects.get(action="sspr_reset").state == "failed"


def test_domain_admin_loss_between_match_and_adapter_write_refuses_password(domain_admin_runtime, monkeypatch):
    _, directory, _ = domain_admin_runtime
    token, _ = sspr.verify("valid", "192.0.2.25")
    original = directory.reset_password

    def remove_role_before_final_guard(guid, password, unlock=False):
        directory.items[0]["domain_admin"] = False
        return original(guid, password, unlock)

    monkeypatch.setattr(directory, "reset_password", remove_role_before_final_guard)
    with pytest.raises(RuleError, match="受保护"):
        sspr.reset(token, "Ab1!xyza", "Ab1!xyza", "192.0.2.25")
    assert directory.resets == 0
    assert EmployeeSession.objects.get().used
    assert Audit.objects.get(action="sspr_reset").state == "failed"


def test_domain_admin_role_query_failure_does_not_issue_session_or_leak_diagnostics(domain_admin_runtime, monkeypatch):
    _, directory, _ = domain_admin_runtime

    def fail_role_read(item):
        raise RuntimeError("private group DN and LDAP response")

    monkeypatch.setattr(directory, "password_reset_allowed", fail_role_read)
    with pytest.raises(RuleError, match="身份核验暂不可用") as failure:
        sspr.verify("valid", "192.0.2.25")
    assert "private" not in str(failure.value)
    assert not EmployeeSession.objects.exists() and directory.resets == 0
    assert "private" not in Audit.objects.get(action="sspr_auth_failed").result
