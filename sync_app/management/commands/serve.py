from django.core.management.base import BaseCommand
from sync_app.wsgi import application


class Command(BaseCommand):
    help = "Serve with Waitress on Linux or Windows."

    def add_arguments(self, parser):
        parser.add_argument("--host", default="127.0.0.1")
        parser.add_argument("--port", default=8010, type=int)

    def handle(self, *args, **options):
        from waitress import serve
        serve(application, host=options["host"], port=options["port"], threads=8)
