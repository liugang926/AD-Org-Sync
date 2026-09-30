from datetime import timedelta
from unittest.mock import Mock
from uuid import UUID

import pytest
from django.test import Client
from django.utils import timezone

from sync_app import sspr, views
from sync_app.domain import fingerprint
from sync_app.models import Audit, Configuration, EmployeeSession


@pytest.fixture
def rejected_reset_guards(monkeypatch):
    guards = []
    for owner, name in (
        (sspr, "DingTalk"), (sspr, "ActiveDirectory"),
        (sspr, "reset"), (sspr, "current_account"),
        (Configuration, "current"),
    ):
        guard = Mock(side_effect=AssertionError("CSRF rejection reached business code"))
        monkeypatch.setattr(owner, name, guard)
        guards.append(guard)
    return guards


@pytest.mark.django_db
@pytest.mark.parametrize("failure", ["missing_cookie", "mismatched_token", "foreign_origin"])
def test_csrf_rejected_reset_is_audited_without_trusting_identity(
    failure, rejected_reset_guards, admin_client,
):
    cookie = "server-verified-employee-cookie"
    session = EmployeeSession.objects.create(
        digest=fingerprint(cookie), source_id="verified-user", display_name="已验证员工",
        employee_id="verified-number", target_username="verified.account",
        object_guid=UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        config_fingerprint="configuration-snapshot", expires_at=timezone.now() + timedelta(minutes=5),
    )
    client = Client(enforce_csrf_checks=True)
    client.cookies["employee_verification"] = cookie
    data = {
        "password": "never-store-test-password", "confirmation": "never-store-test-password",
        "userId": "forged-user", "name": "forged-name", "employee_id": "forged-number",
        "account": "forged.account", "objectGUID": "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "code": "never-store-test-code",
    }
    headers = {"REMOTE_ADDR": "192.0.2.10", "HTTP_X_REAL_IP": "2001:0db8:0:0:0:0:0:17"}
    if failure != "missing_cookie":
        client.cookies["csrftoken"] = "a" * 32
        data["csrfmiddlewaretoken"] = "b" * 32 if failure == "mismatched_token" else "a" * 32
    if failure == "foreign_origin":
        headers["HTTP_ORIGIN"] = "https://untrusted.example"
    started = timezone.now()
    response = client.post("/sspr/reset", data, **headers)
    finished = timezone.now()

    assert response.status_code == 403
    assert "no-store" in response["Cache-Control"]
    assert "密码未提交" in response.content.decode()
    attempt = Audit.objects.get(action="sspr_reset")
    assert attempt.actor == "未验证访客"
    assert not any((attempt.actor_name, attempt.employee_id, attempt.target_username, attempt.target))
    assert attempt.client_ip == "2001:db8::17"
    assert attempt.state == "failed" and attempt.success is False
    assert started <= attempt.created_at <= attempt.completed_at <= finished
    serialized = str(list(Audit.objects.values())) + response.content.decode()
    for value in (*data.values(), cookie, "verified-user", "verified.account", "untrusted.example"):
        assert value not in serialized
    session.refresh_from_db()
    assert session.used is False
    assert "employee_verification" not in response.cookies
    for guard in rejected_reset_guards:
        guard.assert_not_called()

    # The failed submission is visible through the existing administrator filters.
    page = admin_client.get("/logs", {"action": "sspr_reset", "result": "failed"})
    assert [row.pk for row in page.context["page"]] == [attempt.pk]
    assert attempt.result in page.content.decode()
    assert "未验证访客" in page.content.decode()


@pytest.mark.django_db
def test_csrf_audit_failure_still_rejects_reset_without_private_diagnostics(
    monkeypatch, rejected_reset_guards,
):
    write_audit = Mock(side_effect=RuntimeError("private database diagnostic"))
    monkeypatch.setattr(views, "audit", write_audit)
    response = Client(enforce_csrf_checks=True).post(
        "/sspr/reset", {"password": "never-store-test-password"}, REMOTE_ADDR="192.0.2.10",
    )
    assert response.status_code == 403
    assert "审计暂时不可用" in response.content.decode()
    assert "private" not in response.content.decode()
    assert "never-store-test-password" not in response.content.decode()
    assert not Audit.objects.exists()
    write_audit.assert_called_once()
    for guard in rejected_reset_guards:
        guard.assert_not_called()


@pytest.mark.django_db
@pytest.mark.parametrize("path", ["/login", "/sspr/auth/dingtalk", "/connections/test"])
def test_csrf_failure_on_other_routes_does_not_create_reset_audit(path, rejected_reset_guards):
    response = Client(enforce_csrf_checks=True).post(path, {})
    assert response.status_code == 403
    assert not Audit.objects.exists()
    for guard in rejected_reset_guards:
        guard.assert_not_called()


@pytest.mark.django_db
def test_csrf_valid_reset_reaches_existing_single_denial_audit(monkeypatch):
    # A valid token still reaches the normal session checks instead of the failure hook.
    for name in ("DingTalk", "ActiveDirectory"):
        monkeypatch.setattr(sspr, name, Mock(side_effect=AssertionError("Invalid session reached provider")))
    client = Client(enforce_csrf_checks=True)
    client.cookies["csrftoken"] = "a" * 32
    response = client.post("/sspr/reset", {"csrfmiddlewaretoken": "a" * 32})
    assert response.status_code == 400
    assert "验证已失效" in response.content.decode()
    attempts = Audit.objects.filter(action="sspr_reset")
    assert attempts.count() == 1
    assert "验证已失效" in attempts.get().result
    assert attempts.get().actor == "未验证访客"
