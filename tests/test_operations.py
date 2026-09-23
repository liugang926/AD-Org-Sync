import io
import sqlite3
import os
import shutil
import subprocess
import sys
from pathlib import Path
from django.core.management import call_command
from django.db import connection
import pytest
import yaml


@pytest.mark.django_db(transaction=True)
def test_clean_migrations_and_database_check():
    call_command("migrate", interactive=False, verbosity=0)
    output = io.StringIO()
    call_command("db_check", stdout=output)
    assert '"database": "ok"' in output.getvalue()


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
call_command('migrate', verbosity=0, interactive=False)
config=Configuration.current()
config.root_ou='OU=People,DC=example,DC=com'
config.attributes=['displayName']
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
