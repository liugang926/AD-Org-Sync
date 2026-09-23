import json
from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from django.db.migrations.executor import MigrationExecutor


class Command(BaseCommand):
    help = "Check database integrity and unapplied migrations."

    def handle(self, *args, **options):
        with connection.cursor() as cursor:
            cursor.execute("PRAGMA integrity_check")
            ok = cursor.fetchone()[0] == "ok"
            cursor.execute("PRAGMA foreign_key_check")
            ok = ok and not cursor.fetchall()
        executor = MigrationExecutor(connection)
        ok = ok and not executor.migration_plan(executor.loader.graph.leaf_nodes())
        if not ok:
            raise CommandError("Database integrity or migration check failed")
        self.stdout.write(json.dumps({"database": "ok", "migrations": "ok"}))
