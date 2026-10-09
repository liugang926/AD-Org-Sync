import time
import threading
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import close_old_connections
from sync_app.domain import RuleError
from sync_app.synchronization import run_next
from sync_app.password_notifications import process_one_password_notification
from sync_app.account_associations import enqueue_due_association


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
        next_association_check = 0.0
        while True:
            close_old_connections()
            (settings.DATA_DIR / "worker-heartbeat").touch()
            try:
                process_one_password_notification()
            except Exception:
                # Keep notification failures separate from sync and SSPR outcomes.
                pass
            try:
                if time.monotonic() >= next_association_check:
                    enqueue_due_association()
                    next_association_check = time.monotonic() + 30
                run_next()
            except RuleError:
                pass
            if options["once"]:
                return
            time.sleep(2)
