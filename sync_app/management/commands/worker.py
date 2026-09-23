import time
import threading
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import close_old_connections
from sync_app.domain import RuleError
from sync_app.synchronization import run_next


class Command(BaseCommand):
    help = "Run the single synchronization worker; no external queue."

    def add_arguments(self, parser):
        parser.add_argument("--once", action="store_true")

    def handle(self, *args, **options):
        def heartbeat():
            while True:
                (settings.DATA_DIR / "worker-heartbeat").touch()
                time.sleep(10)
        threading.Thread(target=heartbeat, daemon=True).start()
        while True:
            close_old_connections()
            (settings.DATA_DIR / "worker-heartbeat").touch()
            try:
                run_next()
            except RuleError:
                pass
            if options["once"]:
                return
            time.sleep(2)
