from typing import Optional

REQUESTS_IMPORT_ERROR: Optional[BaseException] = None

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except BaseException as exc:
    REQUESTS_IMPORT_ERROR = exc
    requests = None
    HTTPAdapter = Retry = None


def ensure_requests_available() -> None:
    if REQUESTS_IMPORT_ERROR is not None:
        raise RuntimeError(f"requests dependency unavailable: {REQUESTS_IMPORT_ERROR}")


__all__ = [
    "HTTPAdapter",
    "REQUESTS_IMPORT_ERROR",
    "Retry",
    "ensure_requests_available",
    "requests",
]
