import pytest
from django.conf import settings


@pytest.fixture(autouse=True)
def isolated_runtime(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR", tmp_path)
    monkeypatch.setattr(settings, "LDAP_BASE_DN", "DC=example,DC=com")
