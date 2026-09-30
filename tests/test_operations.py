import io
from importlib.metadata import distribution, version
import sqlite3
import os
import shutil
import subprocess
import sys
from pathlib import Path
from django.core.management import call_command
import pytest
import yaml
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


def test_installed_dependency_graph_matches_lock():
    # Inspect the resolved graph, including dependencies activated by nested extras.
    # This fails when an install path bypasses the lock or adds an unlocked dependency.
    root = Path(__file__).resolve().parents[1]
    pins = {}
    for line in (root / "constraints.txt").read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        requirement = Requirement(line)
        if requirement.marker and not requirement.marker.evaluate():
            continue
        name = canonicalize_name(requirement.name)
        assert name not in pins, f"Multiple active locks for {name}"
        specifiers = list(requirement.specifier)
        assert len(specifiers) == 1 and specifiers[0].operator == "=="
        assert "*" not in specifiers[0].version
        pins[name] = specifiers[0].version

    pending = [("ad-org-sync", frozenset({"test"})), ("pip", frozenset())]
    visited = set()
    while pending:
        name, extras = pending.pop()
        key = (name, extras)
        if key in visited:
            continue
        visited.add(key)
        if name != "ad-org-sync":
            assert name in pins, f"Installed dependency is not locked: {name}"
            assert version(name) == pins[name], f"Installed version differs from lock: {name}"
        for raw in distribution(name).requires or []:
            requirement = Requirement(raw)
            if requirement.marker and not any(requirement.marker.evaluate({"extra": extra}) for extra in {"", *extras}):
                continue
            assert version(requirement.name) in requirement.specifier
            pending.append((canonicalize_name(requirement.name), frozenset(requirement.extras)))


@pytest.mark.django_db(transaction=True)
def test_clean_migrations_and_database_check():
    call_command("migrate", interactive=False, verbosity=0)
    output = io.StringIO()
    call_command("db_check", stdout=output)
    assert '"database": "ok"' in output.getvalue()


def test_password_length_migration_updates_old_default_and_preserves_custom_value(tmp_path):
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env.update(AD_ORG_SYNC_DATA_DIR=str(tmp_path), DJANGO_SETTINGS_MODULE="sync_app.settings", LDAP_BASE_DN="DC=example,DC=com")
    code = '''
import django
django.setup()
from datetime import timedelta
from django.core.management import call_command
from django.utils import timezone
from sync_app.models import Configuration
call_command("migrate", "sync_app", "0007_audit_state", interactive=False, verbosity=0)
config = Configuration.current()
Configuration.objects.filter(pk=config.pk).update(minimum_password_length=12, updated_at=timezone.now() - timedelta(days=1))
before = Configuration.current().updated_at
call_command("migrate", "sync_app", "0008_sspr_minimum_eight", interactive=False, verbosity=0)
config.refresh_from_db()
assert config.minimum_password_length == 8
assert config.updated_at > before
call_command("migrate", "sync_app", "0007_audit_state", interactive=False, verbosity=0)
Configuration.objects.filter(pk=config.pk).update(minimum_password_length=16)
call_command("migrate", "sync_app", "0008_sspr_minimum_eight", interactive=False, verbosity=0)
config.refresh_from_db()
assert config.minimum_password_length == 16
'''
    result = subprocess.run([sys.executable, "-c", code], cwd=root, env=env, capture_output=True, text=True, timeout=40)
    assert result.returncode == 0, result.stderr


@pytest.mark.django_db
def test_sqlite_backup_restores_records(tmp_path, settings):
    source_path = tmp_path / "source.sqlite3"
    with sqlite3.connect(source_path) as source:
        source.execute("CREATE TABLE proof (value TEXT)")
        source.execute("INSERT INTO proof VALUES ('restore-evidence')")
    settings.DATA_DIR = tmp_path
    previous_name = settings.DATABASES["default"]["NAME"]
    settings.DATABASES["default"]["NAME"] = str(source_path)
    try:
        output = io.StringIO()
        call_command("db_backup", stdout=output)
        restored = Path(output.getvalue().strip())
        with sqlite3.connect(restored) as backup:
            assert backup.execute("SELECT value FROM proof").fetchone()[0] == "restore-evidence"
            assert backup.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        settings.DATABASES["default"]["NAME"] = previous_name


def test_ci_preserves_production_gates():
    root = Path(__file__).resolve().parents[1]
    workflow = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())
    jobs = workflow["jobs"]
    assert jobs["quality"]["strategy"]["matrix"]["python-version"] == ["3.10", "3.12"]
    deploy = jobs["deploy-production"]
    assert deploy["if"] == "github.event_name == 'push' && github.ref == 'refs/heads/main'"
    assert deploy["runs-on"] == ["self-hosted", "linux", "x64", "production"]
    assert deploy["environment"] == "production"
    assert deploy["concurrency"]["cancel-in-progress"] is False
    assert set(deploy["needs"]) == {"windows", "container", "package-supply-chain", "browser-regression"}
    assert deploy["steps"][-1]["env"]["AD_ORG_SYNC_IMAGE_TAG"] == "${{ github.sha }}"
    script = (root / "scripts/deploy-production.sh").read_text()
    assert script.index("db_backup") < script.index("build --pull")
    assert script.index('exec -T web python -m sync_app.cli db_check') < script.index('cp docker-compose.yml')
    assert script.index('exec -T web python -m sync_app.cli db_check') < script.index('bash scripts/install-scheduler.sh') < script.index('cp docker-compose.yml')
    assert "ROLLBACK FAILED" in script and "--no-build" in script


def test_no_legacy_or_cache_runtime():
    root = Path(__file__).resolve().parents[1]
    compose = yaml.safe_load((root / "docker-compose.yml").read_text())
    assert set(compose["services"]) == {"web", "worker", "nginx", "volume-permissions"}
    assert compose["services"]["web"]["image"] == compose["services"]["worker"]["image"]
    assert not list((root / "sync_app/web").glob("*.py"))
    assert not list((root / "sync_app/ui").glob("*.py"))


def test_application_backup_restore_preserves_config_and_requires_ad_recheck(tmp_path):
    root = Path(__file__).resolve().parents[1]
    live, restored = tmp_path / "live", tmp_path / "restored"
    env = os.environ.copy()
    env.update(AD_ORG_SYNC_DATA_DIR=str(live), DJANGO_SETTINGS_MODULE="sync_app.settings", LDAP_BASE_DN="DC=example,DC=com")
    def run(code, environment):
        result = subprocess.run([sys.executable, "-c", code], cwd=root, env=environment, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr
        return result.stdout
    run('''
import django
django.setup()
from django.core.management import call_command
from sync_app.models import Configuration, Person, Binding
from sync_app.synchronization import directory_identity_anchor
call_command('migrate', verbosity=0, interactive=False)
config=Configuration.current()
config.root_ou='OU=People,DC=example,DC=com'
config.attributes=['displayName']
config.identity_anchor=directory_identity_anchor()
config.save()
person=Person.objects.create(source_id='u1',name='Original employee')
Binding.objects.create(person=person,object_guid='12345678-1234-1234-1234-123456789abc',username='testuser',manual=True)
call_command('db_backup')
config.attributes=[]
config.save()
Binding.objects.all().delete()
''', env)
    backup = next((live / "backups").glob("*.sqlite3"))
    restored.mkdir()
    shutil.copy2(backup, restored / "django.sqlite3")
    env["AD_ORG_SYNC_DATA_DIR"] = str(restored)
    output = run('''
import django
django.setup()
from django.core.management import call_command
from sync_app.models import Configuration, Binding, Job
from sync_app.synchronization import plan
from tests.fakes import Source, Directory, account
call_command('db_check')
assert Configuration.current().attributes == ['displayName']
binding=Binding.objects.get()
assert binding.manual and binding.username == 'testuser'
# A restored binding remains unusable if the directory object has been replaced.
ad=Directory([account()])
job=Job.objects.create()
assert plan(job,Source(),ad)['operations'][0]['action'] == 'conflict'
assert ad.created == 0
# Only rechecking the same directory identity produces an update plan.
ad.items=[account(guid=str(binding.object_guid))]
assert plan(Job.objects.create(),Source(),ad)['operations'][0]['action'] == 'update'
assert ad.created == 0
print('restored_config_and_binding_verified')
''', env)
    assert "restored_config_and_binding_verified" in output
