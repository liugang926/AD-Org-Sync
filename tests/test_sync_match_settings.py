import os
import subprocess
import sys
from pathlib import Path

import pytest
from django.urls import reverse

from sync_app.models import Audit, Configuration


def configuration_payload(config):
    return {
        "root_department": config.root_department, "root_ou": config.root_ou,
        "match_field": config.match_field, "naming": config.naming,
        "attributes": config.attributes, "clear_attributes": config.clear_attributes,
        "protected_usernames": "\n".join(config.protected_usernames),
        "disable_limit": config.disable_limit, "disable_percent": config.disable_percent,
        "sspr_match": config.sspr_match, "minimum_password_length": config.minimum_password_length,
        "interval_minutes": config.interval_minutes,
        **{name: "on" for name in (
            "enable_new_accounts", "require_password_change", "disable_missing",
            "sspr_enabled", "unlock_after_reset", "schedule_enabled",
        ) if getattr(config, name)},
        "_save": "保存",
    }


@pytest.mark.django_db
def test_new_configuration_keeps_employee_id_matching_default():
    config = Configuration.current()
    config.refresh_from_db()
    assert config.match_field == "employee_id"
    assert config.naming == "employee_id"
    assert config.sspr_match == "employee_id"


@pytest.mark.django_db
def test_admin_can_explicitly_save_job_to_sam_without_changing_other_policy(admin_client):
    config = Configuration.current()
    config.root_ou = "OU=Sync,DC=example,DC=com"
    config.identity_anchor = "kept-directory-anchor"
    config.attributes = ["displayName", "mail"]
    config.clear_attributes = ["mail"]
    config.protected_usernames = ["kept-service-account"]
    config.sspr_enabled = True
    config.sspr_match = "email"
    config.minimum_password_length = 16
    config.save()
    before = Configuration.objects.values().get(pk=config.pk)
    path = reverse("admin:sync_app_configuration_change", args=[config.pk])
    response = admin_client.get(path)
    assert response.status_code == 200
    form = response.context["adminform"].form
    assert "employee_username" in dict(form.fields["match_field"].choices)
    content = response.content.decode()
    assert "钉钉工号 → AD employeeID" in content
    assert "钉钉工号 → AD sAMAccountName" in content
    assert "更改匹配方式后须重新生成预览" in content
    assert "AD 根 OU 外的现有账号不会自动纳入同步" in content
    payload = {**configuration_payload(config), "match_field": "employee_username"}
    response = admin_client.post(path, payload)
    assert response.status_code == 302
    after = Configuration.objects.values().get(pk=config.pk)
    assert after["match_field"] == "employee_username"
    assert {name: value for name, value in before.items() if name not in {"match_field", "updated_at"}} == {
        name: value for name, value in after.items() if name not in {"match_field", "updated_at"}
    }
    assert after["updated_at"] != before["updated_at"]
    assert Audit.objects.filter(action="settings", success=True).count() == 1


@pytest.mark.django_db
def test_admin_rejects_unknown_match_mode_without_saving(admin_client):
    config = Configuration.current()
    before = Configuration.objects.values().get(pk=config.pk)
    path = reverse("admin:sync_app_configuration_change", args=[config.pk])
    response = admin_client.post(path, {**configuration_payload(config), "match_field": "automatic"})
    assert response.status_code == 200
    assert "match_field" in response.context["adminform"].form.errors
    assert Configuration.objects.values().get(pk=config.pk) == before
    assert not Audit.objects.filter(action="settings").exists()


def test_sync_match_choice_migration_preserves_existing_configuration(tmp_path):
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env.update(AD_ORG_SYNC_DATA_DIR=str(tmp_path), DJANGO_SETTINGS_MODULE="sync_app.settings")
    code = '''
import django
django.setup()
from django.core.management import call_command
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from sync_app.models import Configuration
from sync_app.sspr import config_signature
from sync_app.synchronization import configuration_signature

old_target = "0011_sspr_employee_username"
new_target = "0012_sync_employee_username"
call_command("migrate", "sync_app", old_target, interactive=False, verbosity=0)
config = Configuration.current()
config.root_department = "kept-department"
config.root_ou = "OU=Kept,DC=example,DC=com"
config.naming = "source_id"
config.identity_anchor = "kept-directory-anchor"
config.attributes = ["displayName", "mail"]
config.clear_attributes = ["mail"]
config.protected_usernames = ["kept-service-account"]
config.sspr_enabled = True
config.sspr_match = "email"
config.minimum_password_length = 16
for mode in ("employee_id", "email", "source_id"):
    config.match_field = mode
    config.save()
    before = Configuration.objects.values().get(pk=config.pk)
    signatures = (configuration_signature(config), config_signature(config))
    for target in (new_target, old_target, new_target):
        call_command("migrate", "sync_app", target, interactive=False, verbosity=0)
        config.refresh_from_db()
        assert Configuration.objects.values().get(pk=config.pk) == before
        assert (configuration_signature(config), config_signature(config)) == signatures
        state = MigrationExecutor(connection).loader.project_state([("sync_app", target)])
        historical_model = state.apps.get_model("sync_app", "Configuration")
        choices = dict(historical_model._meta.get_field("match_field").choices)
        assert ("employee_username" in choices) == (target == new_target)
        assert historical_model().match_field == "employee_id"
call_command("migrate", interactive=False, verbosity=0)
call_command("db_check", verbosity=0)
'''
    result = subprocess.run([sys.executable, "-c", code], cwd=root, env=env, capture_output=True, text=True, timeout=60, check=False)
    assert result.returncode == 0, result.stderr
