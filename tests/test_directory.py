import ssl
from unittest.mock import Mock
import pytest
from sync_app.directory import DingTalk, ActiveDirectory
from sync_app.domain import RuleError


def test_dingtalk_incomplete_pagination_never_becomes_snapshot():
    source = object.__new__(DingTalk)
    replies = [
        {"dept_id": 1, "name": "Root", "parent_id": 0}, [],
        {"list": [{"userid": "u"}], "has_more": True, "next_cursor": 0},
    ]
    source.call = Mock(side_effect=replies)
    source.user = Mock(return_value={"source_id": "u"})
    with pytest.raises(RuleError, match="分页"):
        source.collect("1")


def test_dingtalk_missing_page_list_fails_closed():
    source = object.__new__(DingTalk)
    source.call = Mock(side_effect=[{"dept_id": 1, "name": "Root"}, [], {"has_more": False}])
    with pytest.raises(RuleError):
        source.collect("1")


def test_ldap_match_escapes_untrusted_identity_values():
    directory = object.__new__(ActiveDirectory)
    directory.search = Mock(return_value=[])
    directory.match("employee_id", "*)(sAMAccountName=*)")
    query = directory.search.call_args.args[0]
    assert "\\2a" in query and "\\29" in query and "\\28" in query
    assert "employeeID=" in query
    with pytest.raises(RuleError):
        directory.match("arbitraryLDAPAttribute", "x")


def test_ldap_failures_never_return_partial_results():
    directory = object.__new__(ActiveDirectory)
    directory.conn = Mock()
    directory.conn.result = {"result": 4}
    directory.conn.response = [{"type": "searchResEntry", "dn": "CN=one", "attributes": {}}]
    with pytest.raises(RuleError):
        directory.search("(objectClass=user)")


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        ({"result": 19, "message": "problem 1005 (CONSTRAINT_ATT_TYPE), Att (employeeID):len 48"}, "工号不符合域控 employeeID 字段约束"),
        ({"result": 19, "message": "untrusted directory diagnostic: hidden-value"}, "目录字段约束不满足"),
        ({"result": 50, "message": "untrusted directory diagnostic: hidden-value"}, "LDAP 错误码 50"),
    ],
)
def test_ad_create_rejection_reports_safe_cause(result, expected):
    directory = object.__new__(ActiveDirectory)
    directory.match = Mock(return_value=[])
    directory.conn = Mock()
    directory.conn.add.return_value = False
    directory.conn.result = result
    with pytest.raises(RuleError, match=expected) as error:
        directory.create(
            {"name": "Test User", "employee_id": "test-id"},
            "test-user", "OU=Test,DC=example,DC=com", "OU=Test,DC=example,DC=com",
        )
    assert "hidden-value" not in str(error.value)


@pytest.mark.parametrize("unlock_result", [False, RuntimeError("connection interrupted")])
def test_password_reset_reports_unlock_failure_after_password_change(unlock_result):
    directory = object.__new__(ActiveDirectory)
    directory.check_account = Mock(return_value={"dn": "CN=person,OU=People,DC=example,DC=com"})
    directory.conn = Mock()
    directory.conn.extend.microsoft.modify_password.return_value = True
    if isinstance(unlock_result, Exception):
        directory.conn.modify.side_effect = unlock_result
    else:
        directory.conn.modify.return_value = unlock_result
    outcome = directory.reset_password("test-guid", "test-password", unlock=True)
    assert outcome.complete is False
    assert "密码已重置，但解锁失败" in outcome.message
    directory.conn.extend.microsoft.modify_password.assert_called_once()


def test_interrupted_password_change_remains_unknown():
    directory = object.__new__(ActiveDirectory)
    directory.check_account = Mock(return_value={"dn": "CN=person,OU=People,DC=example,DC=com"})
    directory.conn = Mock()
    directory.conn.extend.microsoft.modify_password.side_effect = RuntimeError("connection interrupted")
    with pytest.raises(RuleError, match="结果不明"):
        directory.reset_password("test-guid", "test-password", unlock=True)
    directory.conn.modify.assert_not_called()


@pytest.mark.django_db
@pytest.mark.parametrize("verify", [True, False])
def test_ldaps_certificate_policy_preserves_encryption(monkeypatch, settings, verify):
    settings.LDAP_HOST = "ad.example.com"
    settings.LDAP_BIND_DN = "CN=service,DC=example,DC=com"
    settings.LDAP_PASSWORD = "test-placeholder"
    settings.LDAP_CA_FILE = "/test/ca.pem"
    settings.LDAP_VERIFY_CERT = verify
    server, connection, tls = Mock(), Mock(), Mock()
    monkeypatch.setattr("sync_app.directory.Server", server)
    monkeypatch.setattr("sync_app.directory.Connection", connection)
    monkeypatch.setattr("sync_app.directory.Tls", tls)
    ActiveDirectory()
    assert tls.call_args.kwargs["validate"] == (ssl.CERT_REQUIRED if verify else ssl.CERT_NONE)
    assert tls.call_args.kwargs["ca_certs_file"] == ("/test/ca.pem" if verify else None)
    assert server.call_args.kwargs["use_ssl"] is True
    assert connection.call_args.kwargs["auto_referrals"] is False
