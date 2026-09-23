from datetime import timedelta
from django.db import transaction
from django.utils import timezone
from .domain import RuleError, fingerprint
from .models import Audit, RateWindow


def audit(actor, action, target="", result="成功"):
    Audit.objects.create(actor=actor, action=action, target=target, result=result)


def rate_limit(key, limit=10):
    now = timezone.now()
    with transaction.atomic():
        item, _ = RateWindow.objects.get_or_create(key=fingerprint(key), defaults={"starts_at": now})
        if item.starts_at < now - timedelta(minutes=10):
            item.starts_at, item.count = now, 0
        if item.count >= limit:
            raise RuleError("操作过于频繁，请稍后再试")
        item.count += 1
        item.save()
