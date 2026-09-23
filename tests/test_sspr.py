import pytest
from django.utils import timezone
from datetime import timedelta
from sync_app import sspr
from sync_app.directory import PasswordResetOutcome
from sync_app.domain import RuleError
from sync_app.models import Configuration, Binding, Job, EmployeeSession, Audit
from .fakes import Source, Directory, account


@pytest.fixture
def setup_sspr(monkeypatch):
    config = Configuration.current()
    config.sspr_enabled = True
    config.save()
    source, ad = Source(), Directory()
    monkeypatch.setattr(sspr, "DingTalk", lambda: source)
    monkeypatch.setattr(sspr, "ActiveDirectory", lambda: ad)
    return source, ad, config


@pytest.mark.django_db
def test_unsynced_employee_can_reset_and_cannot_replay(setup_sspr):
    _, ad, _ = setup_sspr
    assert not Binding.objects.exists() and not Job.objects.exists()
    token, matched = sspr.verify("valid", "ip")
    assert matched["guid"] == ad.items[0]["guid"]
    assert sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")
    assert ad.resets == 1
    with pytest.raises(RuleError):
        sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")
    assert ad.resets == 1
    assert "Example-password" not in str(list(Audit.objects.values()))


@pytest.mark.django_db
def test_pilot_limits_dingtalk_user_and_invalidates_session_when_scope_changes(setup_sspr, settings):
    _, ad, _ = setup_sspr
    settings.SSPR_ALLOWED_DINGTALK_USER_IDS = frozenset({"somebody-else"})
    with pytest.raises(RuleError, match="尚未对当前账号开放"):
        sspr.verify("valid", "ip")
    assert not EmployeeSession.objects.exists()
    assert ad.resets == 0

    settings.SSPR_ALLOWED_DINGTALK_USER_IDS = frozenset({"u1"})
    token, _ = sspr.verify("valid", "ip")
    assert sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")
    assert ad.resets == 1
    token, _ = sspr.verify("valid", "ip")
    settings.SSPR_ALLOWED_DINGTALK_USER_IDS = frozenset({"somebody-else"})
    with pytest.raises(RuleError, match="验证已失效"):
        sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")
    assert ad.resets == 1


@pytest.mark.django_db
def test_unlock_failure_is_audited_as_partial_and_session_is_consumed(setup_sspr, monkeypatch):
    _, ad, config = setup_sspr
    config.unlock_after_reset = True
    config.save()
    token, _ = sspr.verify("valid", "ip")

    def partial_reset(guid, password, unlock=False):
        assert unlock is True
        ad.resets += 1
        return PasswordResetOutcome("密码已重置，但解锁失败，请联系管理员", False)

    monkeypatch.setattr(ad, "reset_password", partial_reset)
    result = sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")
    assert "密码已重置，但解锁失败" in result
    assert Audit.objects.get(action="sspr_reset").success is False
    with pytest.raises(RuleError):
        sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")
    assert ad.resets == 1


@pytest.mark.django_db
@pytest.mark.parametrize("mode", ["ambiguous", "missing", "protected", "disabled"])
def test_unsafe_matching_is_denied(setup_sspr, mode):
    _, ad, _ = setup_sspr
    if mode == "ambiguous":
        ad.items.append(account())
    elif mode == "missing":
        ad.items = []
    elif mode == "protected":
        ad.items[0]["protected"] = True
    else:
        ad.items[0]["enabled"] = False
    with pytest.raises(RuleError):
        sspr.verify("valid", "ip")
    assert ad.resets == 0


@pytest.mark.django_db
def test_rechecks_identity_and_guid_at_submission(setup_sspr):
    _, ad, _ = setup_sspr
    token, _ = sspr.verify("valid", "ip")
    ad.items = [account()]
    with pytest.raises(RuleError):
        sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")
    assert ad.resets == 0


@pytest.mark.django_db
def test_expired_session_and_changed_config_denied(setup_sspr):
    _, _, config = setup_sspr
    token, _ = sspr.verify("valid", "ip")
    EmployeeSession.objects.update(expires_at=timezone.now() - timedelta(seconds=1))
    with pytest.raises(RuleError):
        sspr.session_for(token)
    token, _ = sspr.verify("valid", "ip")
    config.sspr_match = "email"
    config.save()
    with pytest.raises(RuleError):
        sspr.session_for(token)


@pytest.mark.django_db
def test_input_identity_is_not_trusted(client, setup_sspr):
    _, ad, _ = setup_sspr
    response = client.post("/sspr/auth/dingtalk", {"code": "valid", "username": "administrator", "userId": "somebody-else"})
    assert response.status_code == 200
    assert EmployeeSession.objects.get().object_guid == __import__("uuid").UUID(ad.items[0]["guid"])
    assert response.cookies["employee_verification"]["httponly"]
    assert response.cookies["employee_verification"]["secure"]


@pytest.mark.django_db
def test_csrf_is_required(setup_sspr):
    from django.test import Client
    assert Client(enforce_csrf_checks=True).post("/sspr/auth/dingtalk", {"code": "valid"}).status_code == 403


@pytest.mark.django_db
def test_missing_enterprise_id_blocks_employee_auth(setup_sspr, settings):
    settings.DINGTALK_CORP_ID = ""
    with pytest.raises(RuleError, match="企业 ID"):
        sspr.verify("valid", "ip")
    assert not EmployeeSession.objects.exists()
