from datetime import datetime, timedelta, timezone as datetime_timezone
import os
from types import SimpleNamespace

import pytest
import requests
from django.utils import timezone

from sync_app import password_notifications as notifications, sspr
from sync_app.directory import PasswordResetOutcome
from sync_app.domain import ResetOutcomeUnknown, RuleError
from sync_app.locking import lock
from sync_app.models import Audit, Configuration, PasswordResetNotification
from .fakes import Directory, Source

pytestmark = pytest.mark.django_db


@pytest.fixture
def robot_files(settings, tmp_path):
    webhook = tmp_path / "robot-webhook"
    webhook.write_text("https://oapi.dingtalk.com/robot/send?access_token=unit-access-token", encoding="utf-8")
    webhook.chmod(0o600)
    settings.DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE = str(webhook)
    settings.DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_FILE = ""
    return webhook


def completed_audit(state="success", **fields):
    return Audit.objects.create(
        actor="source-user", actor_name="测试员工", employee_id="T0001919",
        target_username="T0001919", target="test-guid", action="sspr_reset",
        state=state, success=state == "success", completed_at=timezone.now(),
        result="已确认的测试结果", **fields,
    )


@pytest.fixture(autouse=True)
def http(monkeypatch):
    state = SimpleNamespace(calls=[], status=200, data={"errcode": 0}, failure=None, clients=[])

    class Client:
        def __init__(self):
            self.trust_env = True
            state.clients.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def post(self, url, **kwargs):
            state.calls.append((url, kwargs))
            if state.failure:
                raise state.failure

            def body():
                if isinstance(state.data, Exception):
                    raise state.data
                return state.data

            return SimpleNamespace(status_code=state.status, json=body)

    monkeypatch.setattr(notifications.requests, "Session", Client)
    return state


@pytest.mark.parametrize("state", ["success", "partial"])
def test_only_confirmed_password_changes_queue_once(robot_files, state):
    audit = completed_audit(state)
    first = notifications.enqueue_password_notification(audit)
    second = notifications.enqueue_password_notification(audit)
    assert first.pk == second.pk
    assert PasswordResetNotification.objects.count() == 1
    assert first.state == "pending" and first.started_at is None
    audit.delete()
    assert not PasswordResetNotification.objects.exists()


@pytest.mark.parametrize("state", ["failed", "unknown", "pending"])
def test_unconfirmed_password_outcomes_never_queue(robot_files, state):
    assert notifications.enqueue_password_notification(completed_audit(state)) is None
    assert not PasswordResetNotification.objects.exists()


def test_other_audit_actions_and_incomplete_success_never_queue(robot_files):
    audit = completed_audit()
    Audit.objects.filter(pk=audit.pk).update(action="sspr_verified")
    assert notifications.enqueue_password_notification(audit) is None
    Audit.objects.filter(pk=audit.pk).update(action="sspr_reset", completed_at=None)
    assert notifications.enqueue_password_notification(audit) is None
    assert not PasswordResetNotification.objects.exists()


def test_feature_disabled_when_webhook_path_not_configured(settings):
    settings.DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE = ""
    assert notifications.enqueue_password_notification(completed_audit()) is None
    assert not PasswordResetNotification.objects.exists()


@pytest.mark.parametrize("file_state", ["missing", "empty", "unreadable", "directory", "invalid_utf8"])
def test_explicit_bad_webhook_file_has_independent_failed_notice(settings, tmp_path, monkeypatch, http, file_state):
    path = tmp_path / "configured-webhook"
    if file_state == "directory":
        path.mkdir()
    elif file_state != "missing":
        path.write_bytes(b"\xff" if file_state == "invalid_utf8" else b"")
        path.chmod(0o600)
    settings.DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE = str(path)
    if file_state == "unreadable":
        original = notifications.Path.read_text

        def inaccessible(selected, *args, **kwargs):
            if selected == path:
                raise PermissionError("private file path and token")
            return original(selected, *args, **kwargs)

        monkeypatch.setattr(notifications.Path, "read_text", inaccessible)
    audit = completed_audit()
    notice = notifications.enqueue_password_notification(audit)
    repeated = notifications.enqueue_password_notification(audit)
    assert notice.pk == repeated.pk and PasswordResetNotification.objects.count() == 1
    assert notice.state == "failed" and notice.completed_at and notice.started_at is None
    assert notice.message == "机器人受限配置文件不可用，未发送通知"
    audit.refresh_from_db()
    assert audit.state == "success" and audit.success
    assert not http.calls and str(path) not in notice.message


@pytest.mark.parametrize("state", ["failed", "unknown", "pending"])
def test_unconfirmed_audit_with_bad_file_never_creates_notice(settings, tmp_path, monkeypatch, state):
    settings.DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE = str(tmp_path / "missing")

    def must_not_read(*args, **kwargs):
        raise AssertionError("unconfirmed audit must be rejected before configuration read")

    monkeypatch.setattr(notifications, "_secret_file", must_not_read)
    assert notifications.enqueue_password_notification(completed_audit(state)) is None
    assert not PasswordResetNotification.objects.exists()


def test_bad_later_configuration_does_not_overwrite_sent_notice(robot_files):
    audit = completed_audit()
    previous = PasswordResetNotification.objects.create(
        audit=audit, state="sent", message="机器人已接受通知",
        started_at=timezone.now(), completed_at=timezone.now(),
    )
    before = dict(PasswordResetNotification.objects.values().get(pk=previous.pk))
    robot_files.unlink()
    returned = notifications.enqueue_password_notification(audit)
    assert returned.pk == previous.pk
    assert dict(PasswordResetNotification.objects.values().get(pk=previous.pk)) == before


def test_pending_notice_records_configured_file_loss_as_failed(robot_files, http):
    notice = notifications.enqueue_password_notification(completed_audit())
    robot_files.unlink()
    assert notifications.process_one_password_notification()
    notice.refresh_from_db()
    assert notice.state == "failed" and notice.completed_at and not http.calls


def test_worker_does_not_backfill_historical_reset_audits(robot_files, http):
    completed_audit()
    assert notifications.process_one_password_notification() is False
    assert not http.calls and not PasswordResetNotification.objects.exists()


def test_worker_sends_safe_fixed_text_with_verified_tls_and_no_all_mentions(robot_files, http):
    audit = completed_audit()
    Audit.objects.filter(pk=audit.pk).update(
        actor_name="员工\r\n结果：伪造\u2028\u202e", employee_id="T0001919\t", target_username="T0001919",
        completed_at=datetime(2026, 10, 8, 2, 3, 4, tzinfo=datetime_timezone.utc),
        result="private-original-diagnostic",
    )
    notice = notifications.enqueue_password_notification(audit)
    assert notifications.process_one_password_notification() is True
    notice.refresh_from_db()
    assert notice.state == "sent" and notice.message == "机器人已接受通知"
    assert notice.started_at and notice.completed_at
    assert len(http.calls) == 1 and http.clients[0].trust_env is False
    url, options = http.calls[0]
    assert url == "https://oapi.dingtalk.com/robot/send"
    assert options["verify"] is True and options["allow_redirects"] is False
    assert options["timeout"] == (5, 15)
    assert options["params"] == {"access_token": "unit-access-token"}
    payload = options["json"]
    assert payload["msgtype"] == "text" and payload["at"] == {"isAtAll": False}
    text = payload["text"]["content"]
    assert len(text.splitlines()) == 6 and "\u202e" not in text
    assert "AD 账号：T0001919" in text and "2026-10-08 10:03:04（北京时间）" in text
    assert "private-original-diagnostic" not in text
    assert "unit-access-token" not in str(list(PasswordResetNotification.objects.values()))


def test_partial_notification_confirms_password_but_does_not_claim_unlock_completed(robot_files, http):
    notifications.enqueue_password_notification(completed_audit("partial"))
    assert notifications.process_one_password_notification()
    assert "密码已修改；解锁未完整完成" in http.calls[0][1]["json"]["text"]["content"]


@pytest.mark.parametrize(
    ("status", "data", "failure", "expected"),
    [
        (200, {"errcode": 310000, "errmsg": "unit-access-token private URL"}, None, "failed"),
        (403, {"errmsg": "private"}, None, "failed"),
        (302, {}, None, "unknown"),
        (503, {}, None, "unknown"),
        (200, ValueError("private-json-token"), None, "unknown"),
        (200, {"errcode": False}, None, "unknown"),
        (200, {"errcode": "0"}, None, "unknown"),
        (200, [], None, "unknown"),
        (200, {}, requests.Timeout("unit-access-token private URL"), "unknown"),
    ],
)
def test_rejections_and_uncertain_responses_are_safe_and_never_retry(robot_files, http, status, data, failure, expected):
    notice = notifications.enqueue_password_notification(completed_audit())
    http.status, http.data, http.failure = status, data, failure
    assert notifications.process_one_password_notification()
    notice.refresh_from_db()
    assert notice.state == expected
    assert all(value not in notice.message for value in ("unit-access-token", "private", "http"))
    if isinstance(data, dict) and data.get("errcode") == 310000:
        assert "310000" in notice.message
    assert notifications.process_one_password_notification() is False
    assert len(http.calls) == 1


def test_interrupted_sender_becomes_unknown_without_replay(robot_files, http):
    audit = completed_audit()
    notice = PasswordResetNotification.objects.create(audit=audit, state="sending", started_at=timezone.now() - timedelta(minutes=1))
    assert notifications.process_one_password_notification()
    notice.refresh_from_db()
    assert notice.state == "unknown" and notice.completed_at
    assert "不自动重发" in notice.message
    assert not http.calls


def test_active_sender_lock_prevents_another_worker_claim_or_recovery(robot_files, http):
    notice = PasswordResetNotification.objects.create(audit=completed_audit(), state="sending", started_at=timezone.now())
    with lock("password-reset-robot"):
        assert notifications.process_one_password_notification() is False
    notice.refresh_from_db()
    assert notice.state == "sending" and not http.calls


def test_global_interval_and_single_notice_per_worker_cycle(robot_files, http, monkeypatch):
    first = notifications.enqueue_password_notification(completed_audit())
    second = notifications.enqueue_password_notification(completed_audit())
    clock = [timezone.now()]
    monkeypatch.setattr(notifications.timezone, "now", lambda: clock[0])
    assert notifications.process_one_password_notification()
    first.refresh_from_db();second.refresh_from_db()
    assert first.state == "sent" and second.state == "pending" and len(http.calls) == 1
    clock[0] += timedelta(seconds=3.099)
    assert notifications.process_one_password_notification() is False
    clock[0] += timedelta(milliseconds=1)
    assert notifications.process_one_password_notification()
    second.refresh_from_db()
    assert second.state == "sent" and len(http.calls) == 2


def test_lost_final_db_update_does_not_resend_accepted_request(robot_files, http, monkeypatch):
    notice = notifications.enqueue_password_notification(completed_audit())
    original = notifications._complete

    def unavailable(*_):
        raise RuntimeError("private storage error")

    monkeypatch.setattr(notifications, "_complete", unavailable)
    with pytest.raises(RuntimeError):
        notifications.process_one_password_notification()
    notice.refresh_from_db()
    assert notice.state == "sending" and len(http.calls) == 1
    monkeypatch.setattr(notifications, "_complete", original)
    assert notifications.process_one_password_notification()
    notice.refresh_from_db()
    assert notice.state == "unknown" and len(http.calls) == 1


@pytest.mark.parametrize("webhook", [
    "http://oapi.dingtalk.com/robot/send?access_token=a",
    "https://evil.example/robot/send?access_token=a",
    "https://name@oapi.dingtalk.com/robot/send?access_token=a",
    "https://oapi.dingtalk.com/robot/send?access_token=a#fragment",
    "https://oapi.dingtalk.com/robot/send?access_token=a&access_token=b",
    "https://oapi.dingtalk.com/robot/send?access_token=",
    "https://oapi.dingtalk.com/robot/send?access_token=a&sign=provided",
    "https://oapi.dingtalk.com:8443/robot/send?access_token=a",
    "https://oapi.dingtalk.com/robot/other?access_token=a",
])
def test_bad_webhook_never_reaches_network_or_exposes_url(robot_files, http, webhook):
    robot_files.write_text(webhook, encoding="utf-8")
    notice = notifications.enqueue_password_notification(completed_audit())
    assert notifications.process_one_password_notification()
    notice.refresh_from_db()
    assert notice.state == "failed" and not http.calls
    assert webhook not in notice.message and "https" not in notice.message


def test_configured_but_missing_sign_secret_fails_closed(robot_files, http, settings, tmp_path):
    settings.DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_FILE = str(tmp_path / "missing-sign")
    notice = notifications.enqueue_password_notification(completed_audit())
    assert notifications.process_one_password_notification()
    notice.refresh_from_db()
    assert notice.state == "failed" and not http.calls


def test_optional_signing_matches_fixed_hmac_vector_and_never_persists_secrets(robot_files, http, settings, tmp_path, monkeypatch):
    signing_file = tmp_path / "robot-sign"
    signing_file.write_text("SEC-unit-signing-value", encoding="utf-8")
    signing_file.chmod(0o600)
    settings.DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_FILE = str(signing_file)
    monkeypatch.setattr(notifications, "time", SimpleNamespace(time=lambda: 1700000000))
    notifications.enqueue_password_notification(completed_audit())
    assert notifications.process_one_password_notification()
    params = http.calls[0][1]["params"]
    assert params["timestamp"] == "1700000000000"
    assert params["sign"] == "5KE4YR3o3Ce/XRQcweY0Nt8xvZ22KYXJlq/F/1iDWWg="
    stored = str(list(PasswordResetNotification.objects.values()))
    assert "SEC-unit-signing-value" not in stored and "unit-access-token" not in stored


@pytest.mark.skipif(os.name == "nt", reason="Windows uses deployment ACLs instead of POSIX mode bits")
def test_group_or_world_readable_webhook_is_not_enabled(robot_files):
    robot_files.chmod(0o644)
    notice = notifications.enqueue_password_notification(completed_audit())
    assert notice.state == "failed" and notice.completed_at
    assert notice.message == "机器人受限配置文件不可用，未发送通知"


@pytest.mark.parametrize("outcome", ["success", "partial", "failed", "unknown"])
def test_sspr_hook_only_queues_confirmed_results_and_never_sends_inline(robot_files, http, monkeypatch, outcome):
    config = Configuration.current();config.sspr_enabled = True;config.save()
    source, ad = Source(), Directory()
    monkeypatch.setattr(sspr, "DingTalk", lambda: source)
    monkeypatch.setattr(sspr, "ActiveDirectory", lambda: ad)

    def reset_password(*_):
        if outcome == "failed":
            raise RuleError("密码未修改")
        if outcome == "unknown":
            raise ResetOutcomeUnknown("密码结果不明")
        return PasswordResetOutcome("密码已修改", outcome == "success")

    monkeypatch.setattr(ad, "reset_password", reset_password)
    token, _ = sspr.verify("valid", "127.0.0.1")
    if outcome in {"failed", "unknown"}:
        with pytest.raises(RuleError):
            sspr.reset(token, "Ab1!unit-new-password", "Ab1!unit-new-password", "127.0.0.1")
    else:
        assert sspr.reset(token, "Ab1!unit-new-password", "Ab1!unit-new-password", "127.0.0.1") == "密码已修改"
    audit = Audit.objects.get(action="sspr_reset")
    assert audit.state == outcome
    assert PasswordResetNotification.objects.count() == int(outcome in {"success", "partial"})
    assert not http.calls
    assert "Ab1!unit-new-password" not in str(list(Audit.objects.values())) + str(list(PasswordResetNotification.objects.values()))
    if outcome in {"success", "partial"}:
        http.failure = requests.Timeout("private webhook diagnostic")
        assert notifications.process_one_password_notification()
        audit.refresh_from_db()
        assert audit.state == outcome and audit.result == "密码已修改"
        assert PasswordResetNotification.objects.get(audit=audit).state == "unknown"


def test_queue_failure_cannot_turn_confirmed_reset_into_failure(robot_files, monkeypatch):
    config = Configuration.current();config.sspr_enabled = True;config.save()
    source, ad = Source(), Directory()
    monkeypatch.setattr(sspr, "DingTalk", lambda: source)
    monkeypatch.setattr(sspr, "ActiveDirectory", lambda: ad)

    def unavailable(*_):
        raise RuntimeError("private queue diagnostic")

    monkeypatch.setattr(sspr, "enqueue_password_notification", unavailable)
    token, _ = sspr.verify("valid", "127.0.0.1")
    assert sspr.reset(token, "Ab1!unit-new-password", "Ab1!unit-new-password", "127.0.0.1")
    audit = Audit.objects.get(action="sspr_reset")
    assert audit.state == "success" and audit.success and ad.resets == 1


@pytest.mark.parametrize("complete", [True, False])
def test_config_file_failure_preserves_sspr_result_with_separate_failed_notice(settings, tmp_path, monkeypatch, complete):
    settings.DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE = str(tmp_path / "configured-but-missing")
    config = Configuration.current();config.sspr_enabled = True;config.save()
    source, ad = Source(), Directory()
    monkeypatch.setattr(sspr, "DingTalk", lambda: source)
    monkeypatch.setattr(sspr, "ActiveDirectory", lambda: ad)
    monkeypatch.setattr(ad, "reset_password", lambda *_: PasswordResetOutcome("密码已修改", complete))
    token, _ = sspr.verify("valid", "127.0.0.1")
    assert sspr.reset(token, "Ab1!unit-new-password", "Ab1!unit-new-password", "127.0.0.1") == "密码已修改"
    audit = Audit.objects.get(action="sspr_reset")
    assert audit.state == ("success" if complete else "partial") and audit.success is complete
    assert PasswordResetNotification.objects.get(audit=audit).state == "failed"


def test_notification_failure_does_not_stop_worker_sync_cycle(monkeypatch):
    from sync_app.management.commands import worker

    called = []

    def unavailable():
        raise RuntimeError("private notification diagnostic")

    monkeypatch.setattr(worker, "process_one_password_notification", unavailable)
    monkeypatch.setattr(worker, "run_next", lambda: called.append("sync"))
    monkeypatch.setattr(worker.threading, "Thread", lambda **_: SimpleNamespace(start=lambda: None))
    worker.Command().handle(once=True)
    assert called == ["sync"]
