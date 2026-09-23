import os
from pathlib import Path
from django.contrib.auth import get_user_model
from django.contrib.auth.password_validation import validate_password
from django.core.management.base import BaseCommand, CommandError
from sync_app.models import Configuration


class Command(BaseCommand):
    help = "Initialize one administrator from a protected password file, only if missing."

    def handle(self, *args, **options):
        Configuration.current()
        username = os.environ.get("AD_ORG_SYNC_ADMIN_USERNAME", "admin")
        if get_user_model().objects.filter(username=username).exists():
            self.stdout.write("Administrator already exists; password unchanged.")
            return
        path = os.environ.get("AD_ORG_SYNC_ADMIN_PASSWORD_FILE", "")
        if not path:
            raise CommandError("Set AD_ORG_SYNC_ADMIN_PASSWORD_FILE to a protected file.")
        password = Path(path).read_text().strip()
        validate_password(password)
        get_user_model().objects.create_superuser(username=username, password=password)
        self.stdout.write("Administrator created.")
