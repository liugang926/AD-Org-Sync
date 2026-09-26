import pytest
from django.utils import timezone
from datetime import timedelta
from threading import Event, Thread
from sync_app import sspr
from sync_app.directory import PasswordResetOutcome
from sync_app.domain import ResetOutcomeUnknown, RuleError
from sync_app.locking import lock
from sync_app.models import Configuration, Binding, Job, EmployeeSession, Audit
from .fakes import Source, Directory, account, user


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
def test_reset_can_retry_after_sync_holds_same_account_lock(setup_sspr):
    _, ad, _ = setup_sspr
    token, matched = sspr.verify("valid", "ip")
    held, release = Event(), Event()
    failures = []

    def hold_account_lock():
        try:
            with lock("account:" + matched["guid"]):
                held.set()
                if not release.wait(10):
                    raise AssertionError("account lock was not released")
        except Exception as exc:
            failures.append(exc)
            held.set()

    worker = Thread(target=hold_account_lock)
    worker.start()
    try:
        assert held.wait(10)
        assert not failures
        with pytest.raises(RuleError, match="操作正在执行"):
            sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")
        assert not EmployeeSession.objects.get(digest=sspr.fingerprint(token)).used
        assert ad.resets == 0
    finally:
        release.set()
        worker.join(10)
    assert not worker.is_alive() and not failures
    assert sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")
    assert ad.resets == 1


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
def test_reset_audits_dingtalk_client_initialization_failure(setup_sspr, monkeypatch):
    _, ad, _ = setup_sspr
    token, matched = sspr.verify("valid", "ip")

    def unavailable_client():
        raise RuleError("钉钉客户端暂时不可用")

    monkeypatch.setattr(sspr, "DingTalk", unavailable_client)
    with pytest.raises(RuleError, match="钉钉客户端暂时不可用"):
        sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")

    attempt = Audit.objects.get(action="sspr_reset")
    assert attempt.target == matched["guid"] and not attempt.success
    assert "Example-password-42!" not in attempt.result
    assert EmployeeSession.objects.get(digest=sspr.fingerprint(token)).used
    assert ad.resets == 0


@pytest.mark.django_db
def test_reset_attempt_exists_before_directory_write_and_remains_uncertain_on_error(setup_sspr, monkeypatch):
    _, ad, _ = setup_sspr
    token, matched = sspr.verify("valid", "ip")

    def interrupted_reset(guid, password, unlock=False):
        attempt = Audit.objects.get(action="sspr_reset")
        assert attempt.target == matched["guid"]
        assert attempt.success is False
        assert "待确认" in attempt.result
        raise RuntimeError("private directory diagnostic")

    monkeypatch.setattr(ad, "reset_password", interrupted_reset)
    with pytest.raises(ResetOutcomeUnknown, match="结果不明"):
        sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")

    attempt = Audit.objects.get(action="sspr_reset")
    assert attempt.success is False
    assert "结果不明" in attempt.result
    assert "private" not in attempt.result
    assert "Example-password" not in attempt.result
    assert EmployeeSession.objects.get(digest=sspr.fingerprint(token)).used


@pytest.mark.django_db
def test_reset_never_writes_without_durable_attempt(setup_sspr, monkeypatch):
    _, ad, _ = setup_sspr
    token, _ = sspr.verify("valid", "ip")

    def unavailable_audit(**kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(sspr.Audit.objects, "create", unavailable_audit)
    with pytest.raises(RuleError, match="审计暂时不可用"):
        sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")
    assert not EmployeeSession.objects.get(digest=sspr.fingerprint(token)).used
    assert ad.resets == 0


@pytest.mark.django_db
def test_reset_keeps_pending_attempt_if_final_audit_update_fails(setup_sspr, monkeypatch):
    _, ad, _ = setup_sspr
    token, _ = sspr.verify("valid", "ip")
    original_save = Audit.save

    def fail_final_update(self, *args, **kwargs):
        if self.action == "sspr_reset" and kwargs.get("update_fields"):
            raise RuntimeError("database unavailable after directory write")
        return original_save(self, *args, **kwargs)

    monkeypatch.setattr(Audit, "save", fail_final_update)
    with pytest.raises(ResetOutcomeUnknown, match="结果记录暂不可用"):
        sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip")

    attempt = Audit.objects.get(action="sspr_reset")
    assert attempt.success is False
    assert "待确认" in attempt.result
    assert ad.resets == 1
    assert EmployeeSession.objects.get(digest=sspr.fingerprint(token)).used


@pytest.mark.django_db
def test_client_close_failure_cannot_mask_completed_reset(setup_sspr, monkeypatch):
    _, ad, _ = setup_sspr
    token, _ = sspr.verify("valid", "ip")

    def close_failed():
        raise RuntimeError("connection already closed")

    monkeypatch.setattr(ad, "close", close_failed)
    assert sspr.reset(token, "Example-password-42!", "Example-password-42!", "ip") == "密码已成功重置"
    assert Audit.objects.get(action="sspr_reset").success is True
    assert ad.resets == 1


@pytest.mark.django_db
def test_unknown_reset_result_stops_automatic_reverification(client, setup_sspr, monkeypatch):
    _, ad, _ = setup_sspr
    token, _ = sspr.verify("valid", "ip")
    client.cookies["employee_verification"] = token

    def interrupted_reset(guid, password, unlock=False):
        raise ResetOutcomeUnknown("目录响应中断，密码修改结果不明；请先验证或联系管理员")

    monkeypatch.setattr(ad, "reset_password", interrupted_reset)
    response = client.post("/sspr/reset", {
        "password": "Example-password-42!", "confirmation": "Example-password-42!",
    })

    assert response.status_code == 200
    content = response.content.decode()
    assert "结果不明" in content and "不要立即重复提交" in content
    assert 'id="verify"' not in content
    assert 'action="/sspr/reset"' not in content
    assert response.cookies["employee_verification"]["max-age"] == 0
    assert EmployeeSession.objects.get(digest=sspr.fingerprint(token)).used


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
def test_changed_dingtalk_user_id_cannot_reuse_verified_session(setup_sspr, monkeypatch):
    source, ad, _ = setup_sspr
    token, _ = sspr.verify("valid", "ip")
    monkeypatch.setattr(source, "user", lambda uid: user("different-user", "1001"))
    with pytest.raises(RuleError, match="钉钉身份发生变化"):
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
@pytest.mark.parametrize("replacement", [False, True])
def test_employee_page_hides_account_when_live_identity_no_longer_matches(client, setup_sspr, replacement):
    _, ad, _ = setup_sspr
    token, _ = sspr.verify("valid", "ip")
    client.cookies["employee_verification"] = token

    assert b"testuser" in client.get("/sspr").content
    ad.items[0]["employee_id"] = "someone-else"
    if replacement:
        ad.items.append(account(employee="1001", name="replacement"))

    response = client.get("/sspr")
    assert response.status_code == 200
    assert b"testuser" not in response.content
    assert b"replacement" not in response.content
    assert b'id="verify"' in response.content


@pytest.mark.django_db
def test_employee_page_hides_account_when_source_returns_different_user(client, setup_sspr, monkeypatch):
    source, _, _ = setup_sspr
    token, _ = sspr.verify("valid", "ip")
    client.cookies["employee_verification"] = token
    monkeypatch.setattr(source, "user", lambda _: user("different-user", "1001"))

    response = client.get("/sspr")
    assert response.status_code == 200
    assert b"testuser" not in response.content
    assert b'id="verify"' in response.content


@pytest.mark.django_db
def test_employee_page_keeps_verified_account_when_client_close_fails(client, setup_sspr, monkeypatch):
    _, ad, _ = setup_sspr
    token, _ = sspr.verify("valid", "ip")
    client.cookies["employee_verification"] = token

    def close_failed():
        raise RuntimeError("connection already closed")

    monkeypatch.setattr(ad, "close", close_failed)
    response = client.get("/sspr")
    assert response.status_code == 200
    assert b"testuser" in response.content


@pytest.mark.django_db
def test_employee_page_hides_account_when_live_lookup_fails(client, setup_sspr, monkeypatch):
    source, _, _ = setup_sspr
    token, _ = sspr.verify("valid", "ip")
    client.cookies["employee_verification"] = token

    def lookup_failed(_):
        raise RuntimeError("private directory diagnostic")

    monkeypatch.setattr(source, "user", lookup_failed)
    response = client.get("/sspr")
    assert response.status_code == 200
    assert b"testuser" not in response.content
    assert "当前账号暂时无法核验" in response.content.decode()
    assert b"private directory diagnostic" not in response.content
    assert b'id="verify"' in response.content


@pytest.mark.django_db
def test_employee_page_hides_account_if_config_changes_during_live_lookup(client, setup_sspr, monkeypatch):
    source, _, config = setup_sspr
    token, _ = sspr.verify("valid", "ip")
    client.cookies["employee_verification"] = token
    original_user = source.user

    def change_config(uid):
        config.unlock_after_reset = True
        config.save()
        return original_user(uid)

    monkeypatch.setattr(source, "user", change_config)
    response = client.get("/sspr")
    assert response.status_code == 200
    assert b"testuser" not in response.content
    assert "配置发生变化" in response.content.decode()
    assert b'id="verify"' in response.content


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
