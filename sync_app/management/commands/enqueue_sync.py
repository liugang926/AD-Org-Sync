from datetime import timedelta
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.utils import timezone
from sync_app.domain import RuleError
from sync_app.models import Configuration, Job
from sync_app.synchronization import enqueue


class Command(BaseCommand):
    help = "System scheduler entry point. --due honors the configured interval."

    def add_arguments(self, parser):
        parser.add_argument("--due", action="store_true")

    def handle(self, *args, **options):
        # The host has one cron entry; retention must run even while sync is disabled.
        try:
            call_command("cleanup", due=True)
        except RuleError:
            # An active sync owns the lock; the next minute will retry cleanup.
            pass
        config = Configuration.current()
        if options["due"]:
            if not config.schedule_enabled:
                return
            latest = Job.objects.filter(actor="scheduler").order_by("-created_at").first()
            if latest and latest.created_at > timezone.now() - timedelta(minutes=config.interval_minutes):
                return
        try:
            job = enqueue(kind="scheduled")
            self.stdout.write(str(job.pk))
        except RuleError as exc:
            self.stdout.write(str(exc))
