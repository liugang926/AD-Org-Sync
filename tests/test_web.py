import pytest
from django.conf import settings
from sync_app.models import Configuration


@pytest.mark.django_db
def test_administrator_pages_require_login(client):
    for path in ["/dashboard", "/people", "/logs"]:
        assert client.get(path).status_code == 302
    assert client.get("/sspr").status_code == 200
    assert client.get("/sspr/callback/dingtalk").status_code == 200


@pytest.mark.django_db
def test_pages_and_readiness(admin_client):
    Configuration.current()
    (settings.DATA_DIR / "worker-heartbeat").touch()
    for path in ["/dashboard", "/people", "/logs", "/admin/sync_app/configuration/1/change/"]:
        assert admin_client.get(path).status_code == 200
    assert admin_client.get("/healthz").status_code == 200
    assert admin_client.get("/readyz").json()["checks"] == {"database": True, "schema": True, "worker": True}


@pytest.mark.django_db
def test_non_admin_cannot_mutate(client, django_user_model):
    person = django_user_model.objects.create_user("reader", password="randomlongpassword")
    client.force_login(person)
    assert client.post("/dashboard", {"scope": "full"}).status_code == 403


@pytest.mark.django_db
def test_admin_builtin_login_cannot_bypass_rate_limit(client):
    for _ in range(10):
        assert client.post("/login", {"username": "unknown", "password": "incorrect"}).status_code == 200
    assert client.post("/admin/login/", {"username": "unknown", "password": "incorrect"}).status_code == 429
