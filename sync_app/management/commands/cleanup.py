from datetime import timedelta
from django.core.management.base import BaseCommand
from django.utils import timezone
from sync_app.models import EmployeeSession, RateWindow, Snapshot, Job, Audit, Operation, Binding
from sync_app.locking import lock


class Command(BaseCommand):
    help = "Bound retention: expired sessions, 7-day snapshots, 90-day jobs, 180-day audit."

    def handle(self, *args, **options):
        now = timezone.now()
        with lock("sync"):
            EmployeeSession.objects.filter(expires_at__lt=now).delete()
            RateWindow.objects.filter(starts_at__lt=now - timedelta(days=1)).delete()
            latest = Snapshot.objects.order_by("-pk").first()
            old = Snapshot.objects.filter(created_at__lt=now - timedelta(days=7))
            if latest:
                old = old.exclude(pk=latest.pk)
            old.delete()
            unresolved = Operation.objects.filter(action="create").exclude(source_id__in=Binding.objects.values("person__source_id")).values("job_id")
            Job.objects.filter(created_at__lt=now - timedelta(days=90)).exclude(status__in=["queued", "running", "needs_confirmation", "preview_ready"]).exclude(pk__in=unresolved).delete()
            Audit.objects.filter(created_at__lt=now - timedelta(days=180)).delete()
        self.stdout.write("Expired operational records removed; bindings retained.")
