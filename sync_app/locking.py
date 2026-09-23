from contextlib import contextmanager
from filelock import FileLock, Timeout
from django.conf import settings
from .domain import RuleError, fingerprint


@contextmanager
def lock(name):
    try:
        with FileLock(str(settings.DATA_DIR / (fingerprint(name) + ".lock")), timeout=0):
            yield
    except Timeout:
        raise RuleError("操作正在执行，请稍后重试") from None
