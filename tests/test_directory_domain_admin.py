import copy
import uuid
from unittest.mock import Mock

import pytest
from django.conf import settings
from ldap3 import BASE, MOCK_SYNC, OFFLINE_AD_2012_R2, Connection, Server
from ldap3.core.exceptions import LDAPAttributeError

from sync_app.directory import ActiveDirectory
from sync_app.domain import RuleError


DOMAIN = "DC=example,DC=com"
DOMAIN_SID = "S-1-5-21-11-22-33"
USER_GUID = "12345678-1234-1234-1234-123456789abc"
ADMIN_GUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
PRIMARY_GUID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
ADMIN_DN = "CN=Renamed Admin Group,CN=Users," + DOMAIN
PRIMARY_DN = "CN=Primary Group,CN=Users," + DOMAIN
USER_DN = "CN=Employee,OU=People," + DOMAIN


def entry(dn, **attributes):
    return {"type": "searchResEntry", "dn": dn, "attributes": attributes}


def account(**changes):
    return {"guid": USER_GUID, "dn": USER_DN, "uac": 512, "enabled": True,
            "protected": True, "username": "employee", "ad_revision": "7", **changes}


@pytest.fixture
def role_directory(monkeypatch):
    monkeypatch.setattr(settings, "LDAP_BASE_DN", "OU=People," + DOMAIN)
    directory = object.__new__(ActiveDirectory)
    directory.extra_protected = set()
    directory.conn = Mock()
    directory.conn.result = {"result": 0}
    directory.conn.response = [entry("", defaultNamingContext=DOMAIN)]
    domain = entry(DOMAIN, objectSid=DOMAIN_SID)
    user = entry(USER_DN, objectGUID=USER_GUID, objectSid=DOMAIN_SID + "-1100",
                 primaryGroupID=513, userAccountControl=512, uSNChanged="7")
    admin = entry(ADMIN_DN, objectGUID=ADMIN_GUID, objectSid=DOMAIN_SID + "-512")
    primary = entry(PRIMARY_DN, objectGUID=PRIMARY_GUID, objectSid=DOMAIN_SID + "-513")
    rows = {"domain": domain, "user": user, "admin": admin, "primary": primary}

    def query(query, base=None, attrs=None, scope=None):
        if query == "(objectClass=domainDNS)":
            return [copy.deepcopy(rows["domain"])]
        if "objectCategory=person" in query:
            return [copy.deepcopy(rows["user"])]
        if "member:1.2.840.113556.1.4.1941:" in query:
            return []
        if "\\00\\02\\00\\00" in query:
            return [copy.deepcopy(rows["admin"])]
        return [copy.deepcopy(rows["primary"])]

    directory.search = Mock(side_effect=query)
    return directory, rows


@pytest.mark.parametrize("relation", ["direct", "nested"])
def test_direct_and_nested_membership_use_actual_sid_not_group_name(role_directory, relation):
    directory, rows = role_directory
    original = directory.search.side_effect
    nested_dn = "CN=Nested Group,CN=Users," + DOMAIN
    members = {ADMIN_DN: [USER_DN]} if relation == "direct" else {ADMIN_DN: [nested_dn], nested_dn: [USER_DN]}

    def reaches_member(group_dn, target_dn):
        return any(member == target_dn or reaches_member(member, target_dn) for member in members.get(group_dn, []))

    def query(query, **kwargs):
        if "member:1.2.840.113556.1.4.1941:" in query:
            assert "(objectSid=\\01\\05\\00\\00\\00\\00\\00\\05" in query
            assert USER_DN in query and kwargs["base"] == ADMIN_DN and kwargs["scope"] == BASE
            return [copy.deepcopy(rows["admin"])] if reaches_member(ADMIN_DN, USER_DN) else []
        return original(query, **kwargs)

    directory.search.side_effect = query
    assert directory.is_domain_admin(account()) is True
    directory.conn.search.assert_called_once_with(
        "", "(objectClass=*)", BASE, attributes=["*", "+"],
        controls=[(directory.DOMAIN_SCOPE_OID, True, None)],
    )


def test_domain_admin_primary_group_is_positive_without_memberof(role_directory):
    directory, rows = role_directory
    rows["user"]["attributes"]["primaryGroupID"] = 512
    assert directory.is_domain_admin(account()) is True
    assert all("member:" not in call.args[0] for call in directory.search.call_args_list)


def test_binary_domain_user_and_group_sids_with_native_and_binary_guids(role_directory):
    directory, rows = role_directory
    # Actual Windows SID encoding: revision, count, authority, little-endian RIDs.
    domain_bytes = b"\x01\x04\x00\x00\x00\x00\x00\x05\x15\x00\x00\x00\x0b\x00\x00\x00\x16\x00\x00\x00\x21\x00\x00\x00"
    rows["domain"]["attributes"]["objectSid"] = domain_bytes
    rows["user"]["attributes"].update(
        objectSid=b"\x01\x05" + domain_bytes[2:] + b"\x4c\x04\x00\x00",
        objectGUID=uuid.UUID(USER_GUID), primaryGroupID=b"512", userAccountControl=b"512", uSNChanged=7,
    )
    rows["admin"]["attributes"].update(
        objectSid=b"\x01\x05" + domain_bytes[2:] + b"\x00\x02\x00\x00",
        objectGUID=uuid.UUID(ADMIN_GUID).bytes_le,
    )
    assert directory.is_domain_admin(account()) is True


def test_other_primary_group_nested_in_domain_admins_is_positive(role_directory):
    directory, rows = role_directory
    original = directory.search.side_effect

    def query(query, **kwargs):
        if "member:1.2.840.113556.1.4.1941:" in query and PRIMARY_DN in query:
            return [copy.deepcopy(rows["admin"])]
        return original(query, **kwargs)

    directory.search.side_effect = query
    assert directory.is_domain_admin(account()) is True


def test_stale_admincount_without_current_member_evidence_is_not_an_exception(role_directory):
    directory, _ = role_directory
    assert directory.is_domain_admin(account()) is False
    assert directory.password_reset_allowed(account()) is False


def test_same_group_name_with_wrong_sid_cannot_grant_exception(role_directory):
    directory, rows = role_directory
    rows["admin"] = entry("CN=Domain Admins,CN=Users," + DOMAIN,
                          objectGUID=ADMIN_GUID, objectSid=DOMAIN_SID + "-1512")
    with pytest.raises(RuleError):
        directory.is_domain_admin(account())


@pytest.mark.parametrize("attribute,value", [
    ("objectGUID", str(uuid.uuid4())), ("objectSid", "S-1-5-21-44-55-66-1100"),
    ("primaryGroupID", None), ("primaryGroupID", []), ("primaryGroupID", [512, 513]),
    ("primaryGroupID", True), ("primaryGroupID", "-1"), ("primaryGroupID", "invalid"),
    ("userAccountControl", 514), ("userAccountControl", None), ("userAccountControl", [512, 514]),
    ("uSNChanged", "8"), ("uSNChanged", None), ("uSNChanged", "0"),
])
def test_changed_or_unverifiable_role_user_fails_closed(role_directory, attribute, value):
    directory, rows = role_directory
    rows["user"]["attributes"][attribute] = value
    with pytest.raises(RuleError) as failure:
        directory.is_domain_admin(account())
    assert "invalid" not in str(failure.value)


@pytest.mark.parametrize("result,response", [
    ({"result": 12}, [entry("", defaultNamingContext=DOMAIN)]),
    ({"result": 0}, [{"type": "searchResRef", "uri": "ldap://untrusted"}]),
    ({"result": 0}, []),
    ({"result": 0}, [entry("", defaultNamingContext=DOMAIN), entry("", defaultNamingContext=DOMAIN)]),
    ({"result": 0}, [entry("", defaultNamingContext="DC=other,DC=com")]),
    ({"result": 0}, [entry("", defaultNamingContext=None)]),
])
def test_root_dse_errors_references_or_wrong_domain_cannot_grant_exception(role_directory, result, response):
    directory, _ = role_directory
    directory.conn.result, directory.conn.response = result, response
    with pytest.raises(RuleError):
        directory.is_domain_admin(account())


@pytest.mark.parametrize("request_fails", [False, True])
def test_root_dse_standard_selectors_work_with_real_schema_without_changing_name_checks(role_directory, request_fails):
    directory, rows = role_directory
    rows["user"]["attributes"]["primaryGroupID"] = 512
    server = Server("offline.invalid", get_info=OFFLINE_AD_2012_R2)
    connection = Connection(server, client_strategy=MOCK_SYNC, check_names=True)
    connection.bind()
    assert "defaultNamingContext" not in server.schema.attribute_types
    connection.send = Mock(return_value=1)

    def root_response(_message_id):
        connection.result = {"result": 0, "type": "searchResDone"}
        connection.response = [entry("", defaultNamingContext=DOMAIN)]
        return connection.response

    connection.post_send_search = Mock(side_effect=root_response)
    if request_fails:
        connection.send.side_effect = RuntimeError("Synthetic transport failure")
    directory.conn = connection
    try:
        if request_fails:
            with pytest.raises(RuleError):
                directory.is_domain_admin(account())
        else:
            assert directory.is_domain_admin(account()) is True
        connection.send.assert_called_once()
        operation, request, controls = connection.send.call_args.args
        assert operation == "searchRequest"
        assert str(request["baseObject"]) == "" and int(request["scope"]) == 0
        assert [str(selector) for selector in request["attributes"]] == ["*", "+"]
        assert controls == [(directory.DOMAIN_SCOPE_OID, True, None)]
        assert connection.check_names is True
        # Subsequent ordinary requests still fail client-side on invalid names.
        with pytest.raises(LDAPAttributeError):
            connection.search(DOMAIN, "(objectClass=domainDNS)", BASE, attributes=["syntheticMissingSchemaAttribute"])
        connection.send.assert_called_once()
    finally:
        connection.send.side_effect = None
        connection.unbind()


@pytest.mark.parametrize("bad_rows", [[], [entry(DOMAIN, objectSid=DOMAIN_SID)] * 2])
def test_ambiguous_or_missing_domain_identity_fails_closed(role_directory, bad_rows):
    directory, _ = role_directory
    original = directory.search.side_effect
    directory.search.side_effect = lambda query, **kwargs: bad_rows if query == "(objectClass=domainDNS)" else original(query, **kwargs)
    with pytest.raises(RuleError):
        directory.is_domain_admin(account())


def test_member_group_replacement_cannot_grant_exception(role_directory):
    directory, rows = role_directory
    original = directory.search.side_effect

    def query(query, **kwargs):
        if "member:1.2.840.113556.1.4.1941:" in query:
            return [entry(ADMIN_DN, objectGUID=str(uuid.uuid4()), objectSid=DOMAIN_SID + "-512")]
        return original(query, **kwargs)

    directory.search.side_effect = query
    with pytest.raises(RuleError):
        directory.is_domain_admin(account())


@pytest.mark.parametrize("username,protected_marker", [("administrator", True), ("explicit-service", True), ("employee", True)])
def test_current_domain_admin_can_reset_despite_all_protection_markers(username, protected_marker):
    directory = object.__new__(ActiveDirectory)
    directory.is_domain_admin = Mock(return_value=True)
    current = account(username=username, protected=protected_marker)
    assert directory.password_reset_allowed(current) is True
    directory.is_domain_admin.assert_called_once_with(current)


def test_disabled_accounts_never_get_a_domain_admin_exception():
    directory = object.__new__(ActiveDirectory)
    directory.is_domain_admin = Mock(return_value=True)
    assert directory.password_reset_allowed(account(enabled=False)) is False
    directory.is_domain_admin.assert_not_called()


@pytest.mark.parametrize("enabled,expected", [
    (False, "AD账号已禁用，不能自助重置；无需先同步或绑定，请联系AD管理员核查账号启用状态"),
    (True, "AD账号受保护，不能自助重置；无需先同步或绑定，请联系AD管理员核查权限与保护状态"),
])
def test_final_password_guard_has_the_same_specific_denial_as_employee_matching(enabled, expected):
    directory = object.__new__(ActiveDirectory)
    directory.by_guid = Mock(return_value=account(enabled=enabled))
    directory.is_domain_admin = Mock(return_value=False)
    with pytest.raises(RuleError) as denial:
        directory.check_password_reset_account(USER_GUID)
    assert str(denial.value) == expected


def test_unprotected_enabled_account_does_not_require_domain_admin_queries():
    directory = object.__new__(ActiveDirectory)
    directory.is_domain_admin = Mock(side_effect=AssertionError("Unexpected role lookup"))
    assert directory.password_reset_allowed(account(protected=False)) is True


@pytest.mark.parametrize("unknown", [None, "yes", 1])
def test_unknown_membership_result_is_not_positive_authorization(unknown):
    directory = object.__new__(ActiveDirectory)
    directory.is_domain_admin = Mock(return_value=unknown)
    assert directory.password_reset_allowed(account()) is False


def test_sync_check_stays_strict_but_sspr_write_rechecks_current_role():
    directory = object.__new__(ActiveDirectory)
    current = account()
    directory.by_guid = Mock(return_value=current)
    directory.is_domain_admin = Mock(return_value=True)
    directory.conn = Mock()
    directory.conn.extend.microsoft.modify_password.return_value = True
    with pytest.raises(RuleError):
        directory.check_account(USER_GUID)
    directory.is_domain_admin.assert_not_called()
    assert directory.reset_password(USER_GUID, "Synthetic-test-only-password").complete
    directory.is_domain_admin.assert_called_once_with(current)
    directory.is_domain_admin.return_value = False
    with pytest.raises(RuleError):
        directory.reset_password(USER_GUID, "Synthetic-test-only-password")
    directory.conn.extend.microsoft.modify_password.assert_called_once()
