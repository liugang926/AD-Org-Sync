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
