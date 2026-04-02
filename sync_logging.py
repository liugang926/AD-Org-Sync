from sync_app.core import logging_utils as _impl


def setup_logging():
    return _impl.setup_logging()


def __getattr__(name):
    return getattr(_impl, name)
