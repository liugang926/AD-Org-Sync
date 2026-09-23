import pytest
from django.conf import settings
from sync_app.models import Configuration
from sync_app.security import client_address


def test_client_address_uses_proxy_header_and_validates_it(rf):
    request = rf.get("/", REMOTE_ADDR="172.18.0.4", HTTP_X_REAL_IP="198.51.100.17")
    assert client_address(request) == "198.51.100.17"
    request = rf.get("/", REMOTE_ADDR="172.18.0.4", HTTP_X_REAL_IP="spoofed, 198.51.100.17")
    assert client_address(request) == "172.18.0.4"


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
        assert client.post("/login", {"username": "unknown", "password": "incorrect"}, HTTP_X_REAL_IP="198.51.100.17").status_code == 200
    assert client.post("/admin/login/", {"username": "unknown", "password": "incorrect"}, HTTP_X_REAL_IP="198.51.100.17").status_code == 429
    assert client.post("/login", {"username": "unknown", "password": "incorrect"}, HTTP_X_REAL_IP="198.51.100.18").status_code == 200


@pytest.mark.django_db
def test_binding_requires_review_before_mutation(admin_client, monkeypatch):
    from sync_app import synchronization as sync
    from sync_app.models import Person, Binding
    from .fakes import Source, Directory
    config = Configuration.current()
    config.root_ou = "OU=People,DC=example,DC=com"
    config.save()
    ad = Directory()
    monkeypatch.setattr(sync, "DingTalk", Source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    person = Person.objects.create(source_id="u1", name="测试员工")
    response = admin_client.post(f"/people/{person.pk}", {"action": "bind", "username": "testuser", "reason": "核对工号"})
    assert response.status_code == 200
    assert not Binding.objects.exists()
    confirmation = response.context["confirmation"]
    response = admin_client.post(f"/people/{person.pk}", {"action": "confirm_bind", "confirmation": confirmation, "reason": "核对工号"})
    assert response.status_code == 302
    assert Binding.objects.get().username == "testuser"


@pytest.mark.django_db
def test_held_disabled_account_can_be_reviewed_for_reactivation(admin_client, monkeypatch):
    from sync_app import synchronization as sync
    from sync_app.models import Person, Binding
    from .fakes import Directory, account

    target = account()
    target["enabled"] = False
    ad = Directory([target])
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    person = Person.objects.create(source_id="u1", name="测试员工")
    Binding.objects.create(person=person, object_guid=target["guid"], username=target["username"], enabled=False)
    response = admin_client.post(f"/people/{person.pk}", {"action": "verify"})
    assert response.status_code == 200
    assert "恢复 AD 账号及同步绑定" in response.content.decode()


@pytest.mark.django_db
def test_audit_filters_and_invalid_date(admin_client):
    from sync_app.models import Audit
    Audit.objects.create(actor="admin", action="sample_ok", success=True)
    Audit.objects.create(actor="admin", action="sample_failed", success=False)
    response = admin_client.get("/logs?result=failed")
    assert [entry.action for entry in response.context["page"]] == ["sample_failed"]
    response = admin_client.get("/logs?start=2026-99-99")
    assert response.status_code == 200
    assert list(response.context["page"]) == []
