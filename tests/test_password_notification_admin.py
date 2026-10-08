from datetime import datetime, timezone as dt_timezone

import pytest
from django.urls import reverse

from sync_app.models import Audit, PasswordResetNotification


@pytest.mark.django_db
def test_notification_failure_is_separate_from_completed_password_audit(admin_client):
    at = datetime(2026, 10, 8, 10, 30, tzinfo=dt_timezone.utc)
    audit = Audit.objects.create(
        actor="verified-user", actor_name="验证员工", employee_id="T0001919",
        action="sspr_reset", target_username="T0001919@example.com",
        result="密码已成功重置", state="success", success=True, completed_at=at,
    )
    notice = PasswordResetNotification.objects.create(
        audit=audit, state="failed", started_at=at, completed_at=at,
        message="机器人拒绝投递（错误码 310000）",
    )
    response = admin_client.get("/logs", {"q": "T0001919", "action": "sspr_reset"})
    assert response.status_code == 200
    html = response.content.decode()
    assert "密码已成功重置" in html and "钉钉通知：" in html and notice.message in html
    assert "2026-10-08 18:30:00" in html
    audit.refresh_from_db()
    assert audit.state == "success" and audit.success is True


@pytest.mark.django_db
def test_notification_admin_is_read_only_and_shows_trusted_identity(admin_client):
    audit = Audit.objects.create(
        actor="verified-user", actor_name="验证员工", employee_id="T0001919",
        action="sspr_reset", target_username="T0001919@example.com", result="密码已成功重置",
        state="success", success=True,
    )
    notice = PasswordResetNotification.objects.create(audit=audit, state="unknown", message="投递结果不明，不自动重发")
    path = reverse("admin:sync_app_passwordresetnotification_change", args=[notice.pk])
    response = admin_client.get(path)
    assert response.status_code == 200
    html = response.content.decode()
    assert "验证员工" in html and "T0001919@example.com" in html and notice.message in html
    assert 'name="_save"' not in html and 'name="_delete"' not in html
    assert admin_client.post(path, {"state": "sent"}).status_code == 403
    notice.refresh_from_db()
    assert notice.state == "unknown"
    assert admin_client.get(reverse("admin:sync_app_passwordresetnotification_add")).status_code == 403


@pytest.mark.django_db
def test_old_audit_without_notice_does_not_invent_delivery(admin_client):
    Audit.objects.create(actor="old-user", action="sspr_reset", state="success", result="历史密码重置成功")
    response = admin_client.get("/logs")
    assert response.status_code == 200 and "历史密码重置成功" in response.content.decode()
    assert "钉钉通知：" not in response.content.decode()
