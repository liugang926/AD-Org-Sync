import os
import subprocess
import sys
from pathlib import Path

import pytest
from django.contrib.admin import site
from django.contrib.auth.models import Permission
from django.urls import reverse

from sync_app import sspr
from sync_app.admin import ConfigurationAdmin
from sync_app.domain import RuleError
from sync_app.models import Audit, Binding, Configuration, EmployeeSession, Job

from .fakes import Directory, Source, account, user


@pytest.fixture
def username_sspr(db, monkeypatch, settings):
    settings.SSPR_ALLOWED_DINGTALK_USER_IDS = frozenset()
    config = Configuration.current()
    config.sspr_enabled = True
    config.sspr_match = "employee_username"
    config.save()
    source = Source([user("dingtalk-2341", "T0002341")])
    ad = Directory([account(employee="", name="T0002341")])
    monkeypatch.setattr(sspr, "DingTalk", lambda: source)
    monkeypatch.setattr(sspr, "ActiveDirectory", lambda: ad)
    return source, ad, config


def test_employee_username_uses_job_number_instead_of_dingtalk_uid(username_sspr, monkeypatch):
    source, ad, config = username_sspr
    uid_account = account(employee="", name="dingtalk-2341")
    ad.items.append(uid_account)
    calls = []
    original_match = ad.match

    def record_match(field, value):
        calls.append((field, value))
        return original_match(field, value)

    monkeypatch.setattr(ad, "match", record_match)
    _, matched = sspr.match_employee(source, ad, config, code="valid")
    assert matched["username"] == "T0002341"
    assert matched["guid"] != uid_account["guid"]
    assert calls == [("source_id", "T0002341")]


@pytest.mark.parametrize("mode,source_field,ad_field", [
    ("employee_id", "employee_id", "employee_id"),
    ("email", "email", "email"),
    ("source_id", "source_id", "source_id"),
])
def test_existing_modes_keep_their_explicit_single_field_mapping(username_sspr, monkeypatch, mode, source_field, ad_field):
    source, ad, config = username_sspr
    config.sspr_match = mode
    calls = []

    def record_match(field, value):
        calls.append((field, value))
        return [ad.items[0]]

    monkeypatch.setattr(ad, "match", record_match)
    sspr.match_employee(source, ad, config, code="valid")
    assert calls == [(ad_field, source.users[0][source_field])]


@pytest.mark.parametrize("value", ["", " ", None])
def test_empty_job_number_is_rejected_without_ad_lookup(username_sspr, monkeypatch, value):
    source, ad, config = username_sspr
    source.users[0]["employee_id"] = value

    def unexpected_lookup(*args):
        pytest.fail("An empty job number must not be queried against AD")

    monkeypatch.setattr(ad, "match", unexpected_lookup)
    with pytest.raises(RuleError, match="钉钉工号为空"):
        sspr.match_employee(source, ad, config, code="valid")
    assert not EmployeeSession.objects.exists() and ad.resets == 0


def test_unknown_match_mode_fails_closed_without_ad_lookup(username_sspr, monkeypatch):
    source, ad, config = username_sspr
    config.sspr_match = "automatic"

    def unexpected_lookup(*args):
        pytest.fail("An unknown mode must not query AD or fall back")

    monkeypatch.setattr(ad, "match", unexpected_lookup)
    with pytest.raises(RuleError, match="匹配方式无效"):
        sspr.match_employee(source, ad, config, code="valid")
    assert ad.resets == 0


def test_default_employee_id_mode_does_not_fall_back_to_matching_username(username_sspr, monkeypatch):
    _, ad, config = username_sspr
    assert Configuration._meta.get_field("sspr_match").default == "employee_id"
    config.sspr_match = "employee_id"
    config.save()
    calls = []
    original_match = ad.match

    def record_match(field, value):
        calls.append((field, value))
        return original_match(field, value)

    monkeypatch.setattr(ad, "match", record_match)
    with pytest.raises(RuleError, match="未匹配到AD账号"):
        sspr.verify("valid", "192.0.2.10")
    assert calls == [("employee_id", "T0002341")]
    assert ad.items[0]["username"] == "T0002341" and ad.items[0]["employee_id"] == ""
    assert not EmployeeSession.objects.exists() and ad.resets == 0


@pytest.mark.parametrize("mode", ["ambiguous", "protected", "disabled"])
def test_username_mode_keeps_unique_match_and_account_safety_checks(username_sspr, mode):
    _, ad, _ = username_sspr
    if mode == "ambiguous":
        ad.items.append(account(employee="", name="t0002341"))
    elif mode == "protected":
        ad.items[0]["protected"] = True
    else:
        ad.items[0]["enabled"] = False
    with pytest.raises(RuleError, match="匹配到多个AD账号|AD账号受保护|AD账号已禁用"):
        sspr.verify("valid", "192.0.2.10")
    assert not EmployeeSession.objects.exists() and ad.resets == 0
    assert not Audit.objects.get(action="sspr_auth_failed").success


def test_username_mode_preserves_dingtalk_allowlist(username_sspr, settings):
    _, ad, _ = username_sspr
    settings.SSPR_ALLOWED_DINGTALK_USER_IDS = frozenset({"T0002341"})
    with pytest.raises(RuleError, match="尚未对当前账号开放"):
        sspr.verify("valid", "192.0.2.10")
    assert not EmployeeSession.objects.exists() and ad.resets == 0
    settings.SSPR_ALLOWED_DINGTALK_USER_IDS = frozenset({"dingtalk-2341"})
    _, matched = sspr.verify("valid", "192.0.2.10")
    assert matched["username"] == "T0002341"


def test_username_mode_verifies_displays_and_resets_without_sync_binding(username_sspr):
    _, ad, _ = username_sspr
    token, matched = sspr.verify("valid", "192.0.2.10")
    assert sspr.current_account(token) == {"username": "T0002341", "name": "测试员工"}
    item = EmployeeSession.objects.get(digest=sspr.fingerprint(token))
    assert item.source_id == "dingtalk-2341" and item.employee_id == "T0002341"
    assert str(item.object_guid) == matched["guid"] and item.target_username == "T0002341"
    assert not Binding.objects.exists() and not Job.objects.exists()
    assert sspr.reset(token, "Ab1!xyza", "Ab1!xyza", "192.0.2.10") == "密码已成功重置"
    record = Audit.objects.get(action="sspr_reset")
    assert ad.resets == 1 and record.state == "success"
    assert record.actor == "dingtalk-2341" and record.employee_id == "T0002341"
    assert record.target == matched["guid"] and record.target_username == "T0002341"
    assert "Ab1!xyza" not in str(list(Audit.objects.values()))


@pytest.mark.parametrize("step", ["current_account", "reset"])
@pytest.mark.parametrize("change", ["guid", "match_mode", "directory"])
def test_username_mode_rejects_replaced_guid_or_changed_configuration(username_sspr, settings, step, change):
    _, ad, config = username_sspr
    token, matched = sspr.verify("valid", "192.0.2.10")
    if change == "guid":
        ad.items = [account(employee="", name="T0002341")]
        assert ad.items[0]["guid"] != matched["guid"]
    elif change == "match_mode":
        config.sspr_match = "employee_id"
        config.save()
    else:
        settings.LDAP_HOST = "changed-directory.example.com"
    with pytest.raises(RuleError, match="对象发生变化|验证已失效"):
        if step == "current_account":
            sspr.current_account(token)
        else:
            sspr.reset(token, "Ab1!xyza", "Ab1!xyza", "192.0.2.10")
    assert ad.resets == 0


@pytest.mark.parametrize("step", ["current_account", "reset"])
def test_username_mode_rejects_configuration_change_during_live_lookup(username_sspr, monkeypatch, step):
    source, ad, config = username_sspr
    token, _ = sspr.verify("valid", "192.0.2.10")
    original_user = source.user

    def change_config(source_id):
        config.sspr_match = "employee_id"
        config.save()
        return original_user(source_id)

    monkeypatch.setattr(source, "user", change_config)
    with pytest.raises(RuleError, match="配置发生变化"):
        if step == "current_account":
            sspr.current_account(token)
        else:
            sspr.reset(token, "Ab1!xyza", "Ab1!xyza", "192.0.2.10")
    assert ad.resets == 0


def test_configuration_admin_directory_scope_is_read_only_and_escaped(admin_client, rf, admin_user, settings):
    config = Configuration.current()
    settings.LDAP_HOST = '<script>directory-host</script>'
    settings.LDAP_BASE_DN = 'DC=<img src=x onerror=alert(1)>,DC=example'
    settings.LDAP_BIND_DN = "private-bind-identity"
    settings.LDAP_PASSWORD = "private-bind-password"
    settings.SSPR_ALLOWED_DINGTALK_USER_IDS = frozenset({"private-uid-1", "private-uid-2"})
    path = reverse("admin:sync_app_configuration_change", args=[config.pk])
    response = admin_client.get(path)
    content = response.content.decode()
    assert response.status_code == 200
    assert "当前 LDAPS 目录" in content and "当前开放范围" in content
    assert "&lt;script&gt;directory-host&lt;/script&gt;" in content
    assert "DC=&lt;img src=x onerror=alert(1)&gt;,DC=example" in content
    assert settings.LDAP_HOST not in content and settings.LDAP_BASE_DN not in content
    assert "仅向 2 个已配置的钉钉账号开放" in content
    for private_value in (settings.LDAP_BIND_DN, settings.LDAP_PASSWORD, *settings.SSPR_ALLOWED_DINGTALK_USER_IDS):
        assert private_value not in content
    request = rf.get(path)
    request.user = admin_user
    form_class = ConfigurationAdmin(Configuration, site).get_form(request, config)
    assert "current_ldap_directory" not in form_class.base_fields
    assert "sspr_open_scope" not in form_class.base_fields
    assert 'name="current_ldap_directory"' not in content and 'name="sspr_open_scope"' not in content
    assert "employee_username" in content


def test_configuration_admin_unrestricted_scope_is_explicit(admin_client, settings):
    config = Configuration.current()
    settings.SSPR_ALLOWED_DINGTALK_USER_IDS = frozenset()
    settings.LDAP_HOST = settings.LDAP_BASE_DN = ""
    content = admin_client.get(reverse("admin:sync_app_configuration_change", args=[config.pk])).content.decode()
    assert "域控：未配置；目录：未配置" in content
    assert "未限制钉钉 userId" in content and "仍须通过 AD 唯一匹配与账号保护检查" in content


@pytest.mark.django_db
def test_configuration_admin_runtime_details_require_staff_and_model_permission(client, django_user_model):
    config = Configuration.current()
    path = reverse("admin:sync_app_configuration_change", args=[config.pk])
    assert client.get(path).status_code == 302
    non_staff = django_user_model.objects.create_user("configuration-employee")
    client.force_login(non_staff)
    assert client.get(path).status_code == 302
    staff = django_user_model.objects.create_user("configuration-staff", is_staff=True)
    client.force_login(staff)
    assert client.get(path).status_code == 403
    staff.user_permissions.add(Permission.objects.get(codename="view_configuration"))
    client.force_login(staff)
    response = client.get(path)
    assert response.status_code == 200 and "当前 LDAPS 目录" in response.content.decode()
    assert client.post(path, {"sspr_match": "employee_username"}).status_code == 403
    config.refresh_from_db()
    assert config.sspr_match == "employee_id"


def test_username_choice_migration_preserves_policy_and_verified_sessions(tmp_path):
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
from sync_app.sspr import config_signature

def historical_apps(target):
    return MigrationExecutor(connection).loader.project_state([("sync_app", target)]).apps

old_target = "0010_employee_page_settings"
new_target = "0011_sspr_employee_username"
call_command("migrate", "sync_app", old_target, interactive=False, verbosity=0)
apps = historical_apps(old_target)
Configuration = apps.get_model("sync_app", "Configuration")
EmployeeSession = apps.get_model("sync_app", "EmployeeSession")
Person = apps.get_model("sync_app", "Person")
Binding = apps.get_model("sync_app", "Binding")
config, _ = Configuration.objects.get_or_create(pk=1)
config.sspr_enabled = True
config.sspr_match = "email"
config.minimum_password_length = 16
config.protected_usernames = ["kept-protected-account"]
config.identity_anchor = "kept-directory-anchor"
config.save()
before = (config.updated_at, config_signature(config))
configuration_before = Configuration.objects.values().get(pk=config.pk)
person = Person.objects.create(source_id="kept-user", name="Kept employee")
binding = Binding.objects.create(person=person, object_guid=uuid.uuid4(), username="kept-login", manual=True)
binding_before = Binding.objects.values().get(pk=binding.pk)
item = EmployeeSession.objects.create(
    digest="kept-session", source_id="kept-user", object_guid=binding.object_guid,
    config_fingerprint=before[1], expires_at=timezone.now() + timedelta(minutes=5),
)
session_before = EmployeeSession.objects.values().get(pk=item.pk)
for target in (new_target, old_target, new_target):
    call_command("migrate", "sync_app", target, interactive=False, verbosity=0)
    apps = historical_apps(target)
    Configuration = apps.get_model("sync_app", "Configuration")
    config = Configuration.objects.get(pk=config.pk)
    item = apps.get_model("sync_app", "EmployeeSession").objects.get(pk=item.pk)
    assert Configuration.objects.values().get(pk=config.pk) == configuration_before
    assert (config.updated_at, config_signature(config)) == before
    assert config.sspr_match == "email" and config.sspr_enabled
    assert config.minimum_password_length == 16
    assert config.protected_usernames == ["kept-protected-account"]
    assert config.identity_anchor == "kept-directory-anchor"
    assert item.config_fingerprint == before[1] and not item.used
    assert apps.get_model("sync_app", "Binding").objects.values().get(pk=binding.pk) == binding_before
    assert apps.get_model("sync_app", "EmployeeSession").objects.values().get(pk=item.pk) == session_before
    choices = dict(Configuration._meta.get_field("sspr_match").choices)
    assert ("employee_username" in choices) == (target == new_target)
    assert Configuration().sspr_match == "employee_id"
call_command("migrate", interactive=False, verbosity=0)
call_command("db_check", verbosity=0)
'''
    result = subprocess.run([sys.executable, "-c", code], cwd=root, env=env, capture_output=True, text=True, timeout=40, check=False)
    assert result.returncode == 0, result.stderr
