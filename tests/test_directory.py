import ssl
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
import pytest
from sync_app.directory import DingTalk, ActiveDirectory
from sync_app.domain import ResetOutcomeUnknown, RuleError


def test_dingtalk_incomplete_pagination_never_becomes_snapshot():
    source = object.__new__(DingTalk)
    replies = [
        {"dept_id": 1, "name": "Root", "parent_id": 0}, [],
        {"list": [{"userid": "u"}], "has_more": True, "next_cursor": 0},
    ]
    source.call = Mock(side_effect=replies)
    source.user = Mock(return_value={"source_id": "u", "departments": ["1"]})
    with pytest.raises(RuleError, match="分页"):
        source.collect("1")


def test_dingtalk_missing_page_list_fails_closed():
    source = object.__new__(DingTalk)
    source.call = Mock(side_effect=[{"dept_id": 1, "name": "Root"}, [], {"has_more": False}])
    with pytest.raises(RuleError):
        source.collect("1")


def test_dingtalk_inconsistent_department_parent_fails_closed():
    source = object.__new__(DingTalk)
    source.call = Mock(side_effect=[
        {"dept_id": 1, "name": "Root", "parent_id": 0},
        [{"dept_id": 2}],
        {"list": [{"userid": "u"}], "has_more": False},
        {"dept_id": 2, "name": "Child", "parent_id": 999},
    ])
    source.user = Mock(return_value={"source_id": "u", "departments": ["1"]})
    with pytest.raises(RuleError, match="父级与子部门列表不一致"):
        source.collect("1")


def test_dingtalk_consistent_child_and_shared_member_are_collected_once():
    source = object.__new__(DingTalk)
    source.call = Mock(side_effect=[
        {"dept_id": 1, "name": "Root", "parent_id": 0},
        [{"dept_id": 2}],
        {"list": [{"userid": "u"}], "has_more": False},
        {"dept_id": 2, "name": "Child", "parent_id": 1},
        [],
        {"list": [{"userid": "u"}], "has_more": False},
    ])
    source.user = Mock(return_value={"source_id": "u", "departments": ["1", "2"]})
    users, departments = source.collect("1")
    assert len(users) == 1 and len(departments) == 2
    source.user.assert_called_once_with("u")


@pytest.mark.parametrize(("detail", "reason"), [
    ({"source_id": "other", "departments": ["1"]}, "身份不一致"),
    ({"source_id": "u", "departments": []}, "成员关系不一致"),
    ({"source_id": "u", "departments": ["2"]}, "成员关系不一致"),
    ({"source_id": "u", "departments": "1"}, "成员关系不一致"),
])
def test_dingtalk_page_and_live_user_detail_must_agree(detail, reason):
    source = object.__new__(DingTalk)
    source.call = Mock(side_effect=[
        {"dept_id": 1, "name": "Root", "parent_id": 0}, [],
        {"list": [{"userid": "u"}], "has_more": False},
    ])
    source.user = Mock(return_value=detail)
    with pytest.raises(RuleError, match=reason):
        source.collect("1")


def test_dingtalk_shared_member_must_match_each_listed_department():
    source = object.__new__(DingTalk)
    source.call = Mock(side_effect=[
        {"dept_id": 1, "name": "Root", "parent_id": 0}, [{"dept_id": 2}],
        {"list": [{"userid": "u"}], "has_more": False},
        {"dept_id": 2, "name": "Child", "parent_id": 1}, [],
        {"list": [{"userid": "u"}], "has_more": False},
    ])
    source.user = Mock(return_value={"source_id": "u", "departments": ["1"]})
    with pytest.raises(RuleError, match="成员关系不一致"):
        source.collect("1")
    source.user.assert_called_once_with("u")


def test_dingtalk_malformed_detail_departments_cannot_complete_snapshot():
    source = object.__new__(DingTalk)
    source.call = Mock(side_effect=[
        {"dept_id": 1, "name": "Root", "parent_id": 0}, [],
        {"list": [{"userid": "u"}], "has_more": False},
        {"userid": "u", "name": "Employee", "dept_id_list": "1"},
    ])
    with pytest.raises(RuleError, match="成员关系不一致"):
        source.collect("1")


def test_dingtalk_user_scope_checks_live_department_ancestry():
    source = object.__new__(DingTalk)
    source.call = Mock(side_effect=[
        {"dept_id": 3, "parent_id": 2},
        {"dept_id": 2, "parent_id": 1},
    ])
    assert source.user_in_scope({"departments": ["3"]}, "1")
    assert source.call.call_count == 2
    source.call.reset_mock()
    assert source.user_in_scope({"departments": ["1"]}, "1")
    source.call.assert_not_called()


def test_dingtalk_user_scope_rejects_outside_or_uncertain_membership():
    source = object.__new__(DingTalk)
    source.call = Mock(side_effect=[{"dept_id": 3, "parent_id": 0}])
    assert not source.user_in_scope({"departments": ["3"]}, "1")
    source.call = Mock(side_effect=[{"dept_id": 3, "parent_id": 3}])
    with pytest.raises(RuleError, match="循环"):
        source.user_in_scope({"departments": ["3"]}, "1")


def test_dingtalk_user_scope_accepts_another_verified_membership():
    source = object.__new__(DingTalk)
    source.call = Mock(side_effect=[
        RuleError("另一部门不可访问"),
        {"dept_id": 4, "parent_id": 1},
    ])
    assert source.user_in_scope({"departments": ["3", "4"]}, "1")


@pytest.mark.parametrize(
    ("sub_code", "expected"),
    [(60011, "子错误码 60011"), ("60012", "子错误码 60012"), ("60013\nprivate", "错误码 88")],
)
def test_dingtalk_error_reports_only_safe_numeric_sub_code(sub_code, expected):
    source = object.__new__(DingTalk)
    source.token = "test-token"
    response = Mock()
    response.json.return_value = {"errcode": 88, "sub_code": sub_code, "sub_msg": "private details"}
    source.http = Mock()
    source.http.post.return_value = response

    with pytest.raises(RuleError, match=expected) as failure:
        source.call("/topapi/v2/department/get", {"dept_id": 1})
    assert "private details" not in str(failure.value)
    assert "private" not in str(failure.value)


@pytest.mark.parametrize("page", [
    {"list": [{"userid": "u"}], "has_more": "false"},
    {"list": [{"userid": "u"}], "has_more": True, "next_cursor": "bad"},
    {"list": [None], "has_more": False},
])
def test_dingtalk_malformed_pagination_fails_closed(page):
    source = object.__new__(DingTalk)
    source.call = Mock(side_effect=[{"dept_id": 1, "name": "Root", "parent_id": 0}, [], page])
    source.user = Mock(return_value={"source_id": "u", "departments": ["1"]})
    with pytest.raises(RuleError, match="人员分页"):
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


def test_ad_search_uses_critical_domain_scope_on_every_page(settings):
    settings.LDAP_BASE_DN = "DC=example,DC=com"
    directory = object.__new__(ActiveDirectory)
    directory.conn = Mock()
    responses = [
        ({"result": 0, "controls": {"1.2.840.113556.1.4.319": {"value": {"cookie": b"next"}}}}, [{"type": "searchResEntry", "dn": "CN=first,DC=example,DC=com"}]),
        ({"result": 0}, [{"type": "searchResEntry", "dn": "CN=second,DC=example,DC=com"}]),
    ]

    def search(*args, **kwargs):
        directory.conn.result, directory.conn.response = responses.pop(0)
        return True

    directory.conn.search.side_effect = search
    rows = directory.search("(objectClass=user)")
    assert len(rows) == 2
    calls = directory.conn.search.call_args_list
    assert len(calls) == 2
    assert all(call.args[0] == settings.LDAP_BASE_DN for call in calls)
    assert all(call.kwargs["controls"] == [("1.2.840.113556.1.4.1339", True, None)] for call in calls)
    assert calls[0].kwargs["paged_cookie"] is None
    assert calls[1].kwargs["paged_cookie"] == b"next"


@pytest.mark.parametrize("result_code", [10, 12, 50, 81])
def test_ad_domain_scope_failure_does_not_return_partial_matches(result_code):
    directory = object.__new__(ActiveDirectory)
    directory.conn = Mock()
    directory.conn.result = {"result": result_code}
    directory.conn.response = [{"type": "searchResEntry", "dn": "CN=partial,DC=example,DC=com"}]
    with pytest.raises(RuleError, match="未完整成功"):
        directory.search("(objectClass=user)")
    directory.conn.search.assert_called_once()


def test_ad_search_still_rejects_unhandled_refs_and_stalled_pages():
    directory = object.__new__(ActiveDirectory)
    directory.conn = Mock()
    directory.conn.result = {"result": 0}
    directory.conn.response = [{"type": "searchResRef", "uri": ["ldaps://other.example.com"]}]
    with pytest.raises(RuleError, match="未处理的引用"):
        directory.search("(objectClass=user)")
    directory.conn.response = [{"type": "searchResEntry", "dn": "CN=partial,DC=example,DC=com"}]
    directory.conn.result = {"result": 0, "controls": {"1.2.840.113556.1.4.319": {"value": {"cookie": b"stalled"}}}}
    with pytest.raises(RuleError, match="分页未前进"):
        directory.search("(objectClass=user)")


def test_ad_account_fingerprint_includes_directory_change_revision():
    assert "uSNChanged" in ActiveDirectory.ATTRS
    guid = str(uuid.uuid4())
    entry = {"dn": "CN=person,OU=People,DC=example,DC=com", "attributes": {
        "objectGUID": guid, "sAMAccountName": "person", "employeeID": "1001",
        "userAccountControl": 512, "uSNChanged": 42,
    }}
    account = ActiveDirectory.account(entry)
    assert account["ad_revision"] == "42"
    entry["attributes"]["uSNChanged"] = 43
    from sync_app.domain import fingerprint
    assert fingerprint(ActiveDirectory.account(entry)) != fingerprint(account)


def native_state_entry():
    return {"dn": "CN=person,OU=People,DC=example,DC=com", "attributes": {
        "objectGUID": str(uuid.uuid4()), "sAMAccountName": "person", "employeeID": "1001",
        "objectSid": "S-1-5-21-100-200-300-1100", "userAccountControl": 512,
        "adminCount": [], "isCriticalSystemObject": [], "lockoutTime": [], "uSNChanged": 42,
    }}


@pytest.mark.parametrize("raw,locked", [(b"0", False), (b"1", True), (b"133700000000000000", True)])
def test_ad_lockout_schema_datetime_uses_original_filetime_without_rounding(raw, locked):
    from ldap3.protocol.formatters.formatters import format_ad_timestamp

    entry = native_state_entry()
    entry["attributes"]["lockoutTime"] = format_ad_timestamp(raw)
    assert isinstance(entry["attributes"]["lockoutTime"], datetime)
    entry["raw_attributes"] = {"lockoutTime": [raw]}
    result = ActiveDirectory.account(entry)
    assert result["locked"] is locked
    assert result["enabled"] and not result["protected"]
    assert result["ad_revision"] == "42" and result["username"] == "person"
    assert result["guid"] == entry["attributes"]["objectGUID"]


@pytest.mark.parametrize("stamp,locked", [
    (datetime(1601, 1, 1), False),
    (datetime(1601, 1, 1, tzinfo=timezone.utc), False),
    (datetime(1601, 1, 1, 8, tzinfo=timezone(timedelta(hours=8))), False),
    (datetime(2026, 1, 1), True),
    (datetime(2026, 1, 1, tzinfo=timezone.utc), True),
])
def test_ad_lockout_datetime_fallback_accepts_utc_and_naive_ad_timestamps(stamp, locked):
    entry = native_state_entry()
    entry["attributes"]["lockoutTime"] = [stamp]
    assert ActiveDirectory.account(entry)["locked"] is locked


@pytest.mark.parametrize("value,locked", [(0, False), ("0", False), (b"0", False), (1, True), ("1", True), (b"1", True)])
def test_ad_numeric_lockout_formats_remain_compatible(value, locked):
    entry = native_state_entry()
    entry["attributes"]["lockoutTime"] = value
    assert ActiveDirectory.account(entry)["locked"] is locked


@pytest.mark.parametrize("field,bad_value", [
    ("userAccountControl", None), ("userAccountControl", []),
    ("userAccountControl", [512, 514]), ("userAccountControl", -1),
    ("userAccountControl", True), ("userAccountControl", 512.5),
    ("userAccountControl", "+512"), ("userAccountControl", "5_12"),
    ("userAccountControl", "٥١٢"), ("userAccountControl", b" 512"),
    ("adminCount", None), ("adminCount", [0, 1]), ("adminCount", -1),
    ("adminCount", False), ("adminCount", "private-invalid-value"),
    ("isCriticalSystemObject", None), ("isCriticalSystemObject", [False, True]),
    ("isCriticalSystemObject", 0), ("isCriticalSystemObject", "private-invalid-value"),
    ("lockoutTime", None), ("lockoutTime", [0, 1]), ("lockoutTime", -1),
    ("lockoutTime", False), ("lockoutTime", 0.5),
    ("lockoutTime", "private-invalid-value"), ("lockoutTime", datetime(1600, 1, 1)),
])
def test_ad_invalid_security_states_fail_closed_without_echoing_values(field, bad_value):
    entry = native_state_entry()
    entry["attributes"][field] = bad_value
    with pytest.raises(RuleError, match="安全属性无法核验") as error:
        ActiveDirectory.account(entry)
    assert "private-invalid-value" not in str(error.value)


def test_ad_missing_required_uac_is_denied_but_optional_empty_flags_keep_ad_defaults():
    entry = native_state_entry()
    result = ActiveDirectory.account(entry)
    assert result["enabled"] and not result["protected"] and not result["locked"]
    del entry["attributes"]["userAccountControl"]
    with pytest.raises(RuleError, match="安全属性无法核验"):
        ActiveDirectory.account(entry)


@pytest.mark.parametrize("raw", [None, [b"0", b"1"], [b"-1"], [b"private-invalid-value"], [datetime(1601, 1, 1)]])
def test_ad_invalid_raw_lockout_does_not_fall_back_to_a_safe_formatted_value(raw):
    entry = native_state_entry()
    entry["attributes"]["lockoutTime"] = datetime(1601, 1, 1, tzinfo=timezone.utc)
    entry["raw_attributes"] = {"lockoutTime": raw}
    with pytest.raises(RuleError, match="安全属性无法核验") as error:
        ActiveDirectory.account(entry)
    assert "private-invalid-value" not in str(error.value)


@pytest.mark.parametrize("attribute,value", [
    ("adminCount", 1), ("isCriticalSystemObject", True),
    ("isCriticalSystemObject", "TRUE"), ("isCriticalSystemObject", b"TRUE"),
    ("userAccountControl", 512 | 2048), ("userAccountControl", 512 | 4096),
    ("userAccountControl", 512 | 8192),
])
def test_ad_native_lockout_fix_preserves_all_protection_flags(attribute, value):
    entry = native_state_entry()
    entry["attributes"]["lockoutTime"] = datetime(1601, 1, 1, tzinfo=timezone.utc)
    entry["attributes"][attribute] = value
    assert ActiveDirectory.account(entry)["protected"]


def test_ad_native_lockout_fix_keeps_disabled_extra_and_builtin_account_protection():
    entry = native_state_entry()
    entry["attributes"]["lockoutTime"] = datetime(1601, 1, 1)
    entry["attributes"]["userAccountControl"] = 514
    assert not ActiveDirectory.account(entry)["enabled"]
    assert ActiveDirectory.account(entry, {"person"})["protected"]
    for rid in (500, 501, 502):
        entry["attributes"]["objectSid"] = "S-1-5-21-100-200-300-" + str(rid)
        assert ActiveDirectory.account(entry)["protected"]


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
    directory.check_password_reset_account = Mock(return_value={"dn": "CN=person,OU=People,DC=example,DC=com"})
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
    directory.check_password_reset_account = Mock(return_value={"dn": "CN=person,OU=People,DC=example,DC=com"})
    directory.conn = Mock()
    directory.conn.extend.microsoft.modify_password.side_effect = RuntimeError("connection interrupted")
    with pytest.raises(ResetOutcomeUnknown, match="结果不明"):
        directory.reset_password("test-guid", "test-password", unlock=True)
    directory.conn.modify.assert_not_called()


@pytest.mark.parametrize("code, reason", [
    (19, "不满足 AD 约束"), (50, "操作权限不足"), (32, "对象不存在"),
    (51, "目录服务繁忙"), (52, "目录服务暂不可用"), (53, "AD 不允许此操作"),
    (None, "未提供可识别的拒绝原因"), ("private-diagnostic", "未提供可识别的拒绝原因"),
])
def test_password_rejection_reports_safe_directory_reason(code, reason):
    directory = object.__new__(ActiveDirectory)
    directory.check_password_reset_account = Mock(return_value={"dn": "CN=person,OU=People,DC=example,DC=com"})
    directory.conn = Mock()
    directory.conn.extend.microsoft.modify_password.return_value = False
    directory.conn.result = {"result": code, "message": "private password diagnostic", "dn": "private DN"}

    with pytest.raises(RuleError, match=reason) as error:
        directory.reset_password("test-guid", "Never-audit-this-password!")
    assert "private" not in str(error.value)
    assert "Never-audit-this-password!" not in str(error.value)
    directory.conn.modify.assert_not_called()


def test_unlock_rejection_keeps_confirmed_password_result_and_safe_reason():
    directory = object.__new__(ActiveDirectory)
    directory.check_password_reset_account = Mock(return_value={"dn": "CN=person,OU=People,DC=example,DC=com"})
    directory.conn = Mock()
    directory.conn.extend.microsoft.modify_password.return_value = True
    directory.conn.modify.return_value = False
    directory.conn.result = {"result": 50, "message": "private LDAP diagnostic"}

    outcome = directory.reset_password("test-guid", "Never-audit-this-password!", unlock=True)
    assert not outcome.complete
    assert "密码已重置，但解锁失败" in outcome.message and "操作权限不足" in outcome.message
    assert "private" not in outcome.message and "Never-audit-this-password!" not in outcome.message


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
