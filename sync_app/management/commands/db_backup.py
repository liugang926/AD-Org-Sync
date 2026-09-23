import sqlite3
from datetime import datetime, timezone
from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Consistent SQLite online backup; does not copy a live database file."

    def handle(self, *args, **options):
        folder = settings.DATA_DIR / "backups"
        folder.mkdir(exist_ok=True)
        path = folder / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f") + ".sqlite3")
        with sqlite3.connect(settings.DATABASES["default"]["NAME"]) as source, sqlite3.connect(path) as target:
            source.backup(target)
            assert target.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        self.stdout.write(str(path))
