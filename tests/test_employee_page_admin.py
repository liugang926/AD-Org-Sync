from datetime import timedelta
import os
from pathlib import Path
import subprocess
import sys
import uuid

import pytest
from django.contrib.admin import site
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.test import Client
from django.urls import reverse
from django.utils import timezone

from sync_app.admin import EmployeePageSettingsAdmin
from sync_app.models import Audit, AuthPlatform, Configuration, EmployeePageSettings, EmployeeSession
from sync_app.sspr import config_signature
from sync_app.synchronization import configuration_signature


@pytest.fixture
def page_settings(db):
    page = EmployeePageSettings.current()
    page.save()
    page.platforms.all().delete()
    AuthPlatform.objects.create(page_settings=page, name="VPN", authentication_note="使用企业 AD 账号认证")
    return page


def admin_payload(page):
    platform = page.platforms.get()
    return {
        "title": "企业账号密码服务", "description": "请使用自己的 AD 账号。",
        "announcement": "维护公告 <script>alert('plain text')</script>",
        "help_text": "在钉钉中验证身份后重置。", "support_text": "联系 IT 服务台。",
        "platforms-TOTAL_FORMS": "2", "platforms-INITIAL_FORMS": "1",
        "platforms-MIN_NUM_FORMS": "0", "platforms-MAX_NUM_FORMS": "1000",
        "platforms-0-id": str(platform.pk), "platforms-0-page_settings": "1",
        "platforms-0-name": "VPN", "platforms-0-authentication_note": "使用企业 AD 账号认证",
        "platforms-0-login_url": "https://vpn.example.com/login", "platforms-0-password_note": "下次登录时使用新密码。",
        "platforms-0-enabled": "on", "platforms-0-position": "10",
        "platforms-1-id": "", "platforms-1-page_settings": "1",
        "platforms-1-name": "Nextcloud", "platforms-1-authentication_note": "使用企业 AD 账号认证",
        "platforms-1-login_url": "", "platforms-1-password_note": "",
        "platforms-1-enabled": "on", "platforms-1-position": "20", "_save": "保存",
    }


@pytest.mark.django_db
def test_current_returns_defaults_without_creating_database_rows(django_assert_num_queries):
    EmployeePageSettings.objects.all().delete()
    with django_assert_num_queries(1):
        current = EmployeePageSettings.current()
    assert current.pk == 1 and current._state.adding
    assert current.title == "重置我的 AD 密码"
    assert current.description == "通过钉钉验证身份，查询并重置本人 AD 账号密码。"
    assert not EmployeePageSettings.objects.exists()
    assert not AuthPlatform.objects.exists()


@pytest.mark.django_db
def test_employee_page_is_singleton_and_text_has_length_limits(page_settings):
    with pytest.raises(ValidationError):
        EmployeePageSettings(id=2).full_clean()
    with pytest.raises(IntegrityError), transaction.atomic():
        EmployeePageSettings.objects.create(id=2)
    for field, limit in (("title", 100), ("description", 1000), ("announcement", 2000), ("help_text", 1000), ("support_text", 500)):
        candidate = EmployeePageSettings.objects.get(pk=1)
        setattr(candidate, field, "文" * (limit + 1))
        with pytest.raises(ValidationError) as exc:
            candidate.full_clean()
        assert field in exc.value.message_dict


@pytest.mark.django_db
@pytest.mark.parametrize("url", ["https://vpn.example.com/login", "http://10.106.1.122:8080/login", ""])
def test_auth_platform_accepts_http_https_or_unconfigured_address(page_settings, url):
    platform = AuthPlatform(page_settings=page_settings, name="测试平台", login_url=url)
    platform.full_clean()


@pytest.mark.django_db
@pytest.mark.parametrize("url", [
    "javascript:alert(1)", "ftp://vpn.example.com/login", "//vpn.example.com/login",
    "https://user:private-value@vpn.example.com/login", "https://user@vpn.example.com/login",
    "https://:private-value@vpn.example.com/login", "https://@vpn.example.com/login",
])
def test_auth_platform_rejects_unsafe_or_credential_bearing_addresses(page_settings, url):
    with pytest.raises(ValidationError) as exc:
        AuthPlatform(page_settings=page_settings, name="测试平台", login_url=url).full_clean()
    assert "login_url" in exc.value.message_dict


@pytest.mark.django_db
def test_auth_platform_order_is_bounded_and_stable(page_settings):
    original = page_settings.platforms.get()
    original.position = 2
    original.save()
    same_order = AuthPlatform.objects.create(page_settings=page_settings, name="Nextcloud", position=2)
    first = AuthPlatform.objects.create(page_settings=page_settings, name="AI知识库", position=1)
    assert list(page_settings.platforms.values_list("pk", flat=True)) == [first.pk, original.pk, same_order.pk]
    for position in (-1, 10000):
        with pytest.raises(ValidationError):
            AuthPlatform(page_settings=page_settings, name="测试平台", position=position).full_clean()


@pytest.mark.django_db
def test_admin_requires_staff_and_model_permissions_and_denies_singleton_deletion(client, django_user_model, rf, page_settings):
    path = reverse("admin:sync_app_employeepagesettings_change", args=[1])
    assert client.get(path).status_code == 302
    assert client.post(path, admin_payload(page_settings)).status_code == 302
    non_staff = django_user_model.objects.create_user("employee-page-reader")
    client.force_login(non_staff)
    assert client.get(path).status_code == 302
    staff = django_user_model.objects.create_user("employee-page-staff", is_staff=True)
    client.force_login(staff)
    assert client.get(path).status_code == 403
    assert client.post(path, admin_payload(page_settings)).status_code == 403
    request = rf.get(path)
    request.user = staff
    model_admin = EmployeePageSettingsAdmin(EmployeePageSettings, site)
    assert not model_admin.has_delete_permission(request, page_settings)
    assert not model_admin.has_add_permission(request)
    assert not Audit.objects.filter(action="employee_page_settings").exists()
    page_settings.refresh_from_db()
    assert page_settings.title == "重置我的 AD 密码"


@pytest.mark.django_db
def test_admin_csrf_prevents_presentation_edits(admin_user, page_settings):
    client = Client(enforce_csrf_checks=True)
    client.force_login(admin_user)
    path = reverse("admin:sync_app_employeepagesettings_change", args=[1])
    response = client.post(path, admin_payload(page_settings))
    assert response.status_code == 403
    page_settings.refresh_from_db()
    assert page_settings.title == "重置我的 AD 密码"
    assert page_settings.platforms.count() == 1
    assert not Audit.objects.filter(action="employee_page_settings").exists()


@pytest.mark.django_db
def test_admin_saves_page_and_platforms_with_one_sanitized_audit(admin_client, admin_user, page_settings):
    path = reverse("admin:sync_app_employeepagesettings_change", args=[1])
    response = admin_client.post(path, admin_payload(page_settings), REMOTE_ADDR="198.51.100.18")
    assert response.status_code == 302
    page_settings.refresh_from_db()
    assert page_settings.title == "企业账号密码服务"
    assert list(page_settings.platforms.values_list("name", flat=True)) == ["VPN", "Nextcloud"]
    assert page_settings.platforms.get(name="VPN").login_url == "https://vpn.example.com/login"
    record = Audit.objects.get(action="employee_page_settings")
    assert record.actor == admin_user.get_username() and record.target == "1"
    assert record.state == "success" and record.success
    assert record.client_ip == "198.51.100.18"
    assert record.completed_at >= record.created_at
    assert "title" in record.result and "新增 1、修改 1、删除 0" in record.result
    assert "企业账号密码服务" not in record.result
    assert "vpn.example.com" not in record.result
    assert "<script>" not in record.result


@pytest.mark.django_db
def test_invalid_inline_url_preserves_page_platforms_and_audit(admin_client, page_settings):
    path = reverse("admin:sync_app_employeepagesettings_change", args=[1])
    payload = admin_payload(page_settings)
    payload["platforms-0-login_url"] = "https://user:private-value@vpn.example.com/login"
    response = admin_client.post(path, payload)
    assert response.status_code == 200
    assert "登录地址不能包含用户名或密码" in response.content.decode()
    page_settings.refresh_from_db()
    assert page_settings.title == "重置我的 AD 密码"
    assert page_settings.platforms.count() == 1 and page_settings.platforms.get().login_url == ""
    assert not Audit.objects.filter(action="employee_page_settings").exists()


@pytest.mark.django_db
def test_admin_audit_failure_rolls_back_model_inlines_and_audit(admin_client, page_settings, monkeypatch):
    from sync_app import admin as admin_module

    original_audit = admin_module.audit

    def fail_after_audit(*args, **kwargs):
        original_audit(*args, **kwargs)
        raise RuntimeError("test audit failure")

    monkeypatch.setattr(admin_module, "audit", fail_after_audit)
    path = reverse("admin:sync_app_employeepagesettings_change", args=[1])
    with pytest.raises(RuntimeError, match="test audit failure"):
        admin_client.post(path, admin_payload(page_settings))
    page_settings.refresh_from_db()
    assert page_settings.title == "重置我的 AD 密码"
    platform = page_settings.platforms.get()
    assert platform.name == "VPN" and platform.login_url == "" and platform.position == 0
    assert not Audit.objects.filter(action="employee_page_settings").exists()


@pytest.mark.django_db
def test_presentation_admin_does_not_invalidate_sync_or_sspr_signature(admin_client, page_settings):
    config = Configuration.current()
    config.sspr_enabled = True
    config.save()
    before_sync = configuration_signature(config)
    before_sspr = config_signature(config)
    before_timestamp = config.updated_at
    session = EmployeeSession.objects.create(
        digest="existing-valid-session", source_id="employee-1", object_guid=uuid.uuid4(),
        config_fingerprint=before_sspr, expires_at=timezone.now() + timedelta(minutes=5),
    )
    path = reverse("admin:sync_app_employeepagesettings_change", args=[1])
    assert admin_client.post(path, admin_payload(page_settings)).status_code == 302
    config.refresh_from_db()
    session.refresh_from_db()
    assert config.updated_at == before_timestamp
    assert configuration_signature(config) == before_sync
    assert config_signature(config) == session.config_fingerprint == before_sspr
    assert not session.used and session.expires_at > timezone.now()


def test_migration_seeds_only_confirmed_platform_names_and_preserves_existing_policy(tmp_path):
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env.update(AD_ORG_SYNC_DATA_DIR=str(tmp_path), DJANGO_SETTINGS_MODULE="sync_app.settings")
    code = '''
import django
django.setup()
import uuid
from datetime import timedelta
from django.core.management import call_command
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.utils import timezone
from sync_app.domain import fingerprint
from sync_app.sspr import config_signature

def historical_apps(target):
    return MigrationExecutor(connection).loader.project_state([("sync_app", target)]).apps

old_target = "0009_sspr_audit_details"
new_target = "0010_employee_page_settings"
call_command("migrate", "sync_app", old_target, interactive=False, verbosity=0)
apps = historical_apps(old_target)
Configuration = apps.get_model("sync_app", "Configuration")
Person = apps.get_model("sync_app", "Person")
Binding = apps.get_model("sync_app", "Binding")
EmployeeSession = apps.get_model("sync_app", "EmployeeSession")
config, _ = Configuration.objects.get_or_create(pk=1)
config.minimum_password_length = 16
config.sspr_enabled = True
config.save()
before = (config.updated_at, fingerprint(Configuration.objects.values().get(pk=config.pk)), config_signature(config))
person = Person.objects.create(source_id="kept-user", name="Kept employee")
binding = Binding.objects.create(person=person, object_guid=uuid.uuid4(), username="kept-login", manual=True)
item = EmployeeSession.objects.create(digest="kept-session", source_id=person.source_id, object_guid=binding.object_guid,
    config_fingerprint=before[2], expires_at=timezone.now() + timedelta(minutes=5))
binding_before = Binding.objects.values().get(pk=binding.pk)
session_before = EmployeeSession.objects.values().get(pk=item.pk)
call_command("migrate", "sync_app", new_target, interactive=False, verbosity=0)
EmployeePageSettings = historical_apps(new_target).get_model("sync_app", "EmployeePageSettings")
page = EmployeePageSettings.objects.get(pk=1)
assert list(page.platforms.values_list("name", flat=True)) == ["VPN", "Nextcloud", "AI知识库"]
assert all(p.enabled and p.authentication_note == "使用企业 AD 账号认证" and not p.login_url and not p.password_note for p in page.platforms.all())
assert EmployeePageSettings.objects.count() == 1
for target in (new_target, old_target):
    if target == old_target:
        call_command("migrate", "sync_app", target, interactive=False, verbosity=0)
    apps = historical_apps(target)
    Configuration = apps.get_model("sync_app", "Configuration")
    config = Configuration.objects.get(pk=config.pk)
    assert config.minimum_password_length == 16 and config.sspr_enabled
    assert (config.updated_at, fingerprint(Configuration.objects.values().get(pk=config.pk)), config_signature(config)) == before
    assert apps.get_model("sync_app", "Binding").objects.values().get(pk=binding.pk) == binding_before
    assert apps.get_model("sync_app", "EmployeeSession").objects.values().get(pk=item.pk) == session_before
call_command("migrate", interactive=False, verbosity=0)
call_command("db_check", verbosity=0)
'''
    result = subprocess.run([sys.executable, "-c", code], cwd=root, env=env, capture_output=True, text=True, timeout=40)
    assert result.returncode == 0, result.stderr
