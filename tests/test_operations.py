import io
import sqlite3
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
    assert "ROLLBACK FAILED" in script and "--no-build" in script


def test_no_legacy_or_cache_runtime():
    root = Path(__file__).resolve().parents[1]
    compose = yaml.safe_load((root / "docker-compose.yml").read_text())
    assert set(compose["services"]) == {"web", "worker", "nginx", "volume-permissions"}
    assert compose["services"]["web"]["image"] == compose["services"]["worker"]["image"]
    assert not list((root / "sync_app/web").glob("*.py"))
    assert not list((root / "sync_app/ui").glob("*.py"))
