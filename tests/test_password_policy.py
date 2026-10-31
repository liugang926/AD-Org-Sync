from datetime import timedelta
from unittest.mock import Mock

from django.conf import settings
from ldap3 import BASE

from sync_app.directory import ActiveDirectory, domain_dn_from_base_dn
from sync_app.domain import RuleError
from .fakes import account


DOMAIN = "DC=example,DC=com"


def entry(dn, **attributes):
    return {"type": "searchResEntry", "dn": dn, "attributes": attributes}


def policy_directory(user_rows, domain_rows, pso_rows=None):
    directory = object.__new__(ActiveDirectory)
    directory._default_naming_context = lambda: DOMAIN
    directory.search_calls = []

    def search(query, base=None, attrs=None, scope=None, *, domain_scope=True):
        directory.search_calls.append({
            "query": query,
            "base": base,
            "attrs": attrs,
            "scope": scope,
            "domain_scope": domain_scope,
        })
        if base == DOMAIN:
            return domain_rows
        if base == account()["dn"]:
            return user_rows
        if pso_rows is not None:
            return pso_rows
        raise AssertionError((query, base, attrs, scope))

    directory.search = search
    return directory


def test_effective_password_policy_uses_domain_defaults():
    directory = policy_directory(
        [entry(account()["dn"], **{"msDS-ResultantPSO": []})],
        [entry(
            DOMAIN,
            minPwdLength=[12],
            pwdProperties=[1],
            pwdHistoryLength=[24],
            pwdMinAge=[timedelta(hours=1)],
            pwdMaxAge=[timedelta(days=90)],
        )],
    )

    policy = directory.effective_password_policy(account())

    assert policy.minimum_length == 12
    assert policy.complexity_enabled is True
    assert policy.history_length == 24
    assert policy.minimum_age_label == "1 小时"
    assert policy.maximum_age_label == "90 天"
    assert policy.source == "domain"
    assert policy.source_label == "域默认密码策略"
    assert policy.fallback is False
    assert policy.missing_fields == ()
    assert directory.search_calls[0]["domain_scope"] is False


def test_effective_password_policy_prefers_user_pso():
    pso_dn = "CN=Strong,CN=Password Settings Container,CN=System," + DOMAIN
    directory = policy_directory(
        [entry(account()["dn"], **{"msDS-ResultantPSO": [pso_dn]})],
        [entry(
            DOMAIN,
            minPwdLength=[8],
            pwdProperties=[0],
            pwdHistoryLength=[24],
            pwdMinAge=[timedelta(hours=1)],
            pwdMaxAge=[timedelta(days=90)],
        )],
        [entry(
            pso_dn,
            **{
                "msDS-MinimumPasswordLength": [14],
                "msDS-PasswordComplexityEnabled": [True],
                "msDS-PasswordHistoryLength": [8],
                "msDS-MinimumPasswordAge": [timedelta(days=1)],
                "msDS-MaximumPasswordAge": [timedelta(days=30)],
            },
        )],
    )

    policy = directory.effective_password_policy(account())

    assert policy.minimum_length == 14
    assert policy.complexity_enabled is True
    assert policy.history_length == 8
    assert policy.minimum_age_label == "1 天"
    assert policy.maximum_age_label == "30 天"
    assert policy.source == "pso"
    assert policy.source_label == "用户适用的细粒度密码策略"
    assert policy.fallback is False


def test_effective_password_policy_pso_age_falls_back_to_domain_when_absent():
    pso_dn = "CN=Strong,CN=Password Settings Container,CN=System," + DOMAIN
    directory = policy_directory(
        [entry(account()["dn"], **{"msDS-ResultantPSO": [pso_dn]})],
        [entry(
            DOMAIN,
            minPwdLength=[8],
            pwdProperties=[0],
            pwdHistoryLength=[24],
            pwdMinAge=[timedelta(hours=1)],
            pwdMaxAge=[timedelta(days=90)],
        )],
        [entry(
            pso_dn,
            **{
                "msDS-MinimumPasswordLength": [14],
                "msDS-PasswordComplexityEnabled": [True],
                "msDS-PasswordHistoryLength": [8],
            },
        )],
    )

    policy = directory.effective_password_policy(account())

    assert policy.minimum_age_label == "1 小时"
    assert policy.maximum_age_label == "90 天"
    assert policy.source == "pso"


def test_effective_password_policy_marks_domain_fallback_when_pso_lookup_fails():
    directory = policy_directory([], [entry(DOMAIN, minPwdLength=[10], pwdProperties=[0], pwdHistoryLength=[5])])

    def unavailable(*args, **kwargs):
        raise RuleError("private directory diagnostic")

    original_search = directory.search
    directory.search = lambda query, base=None, attrs=None, scope=None, *, domain_scope=True: (
        unavailable() if base == account()["dn"] else original_search(
            query, base, attrs, scope, domain_scope=domain_scope
        )
    )

    policy = directory.effective_password_policy(account())

    assert policy.minimum_length == 10
    assert policy.complexity_enabled is False
    assert policy.history_length == 5
    assert policy.source == "domain"
    assert policy.fallback is True
    assert "未能确认" in policy.source_label
    assert "private directory diagnostic" not in policy.source_label


def test_effective_password_policy_keeps_partial_domain_policy():
    directory = policy_directory(
        [entry(account()["dn"], **{"msDS-ResultantPSO": []})],
        [entry(DOMAIN, minPwdLength=[10], pwdProperties=None)],
    )

    directory.conn = Mock()
    directory.conn.result = {"result": 0}
    policy = directory.effective_password_policy(account())

    assert policy.minimum_length == 10
    assert policy.complexity_enabled is None
    assert policy.history_length is None
    assert "history_length" in policy.missing_fields
    assert "密码长度至少 10 位" in policy.requirement_lines
    assert "部分策略属性暂时无法读取" in policy.requirement_lines[-1]


def test_default_naming_context_prefers_configured_base_dn(monkeypatch):
    monkeypatch.setattr(settings, "LDAP_BASE_DN", "OU=People," + DOMAIN)
    directory = object.__new__(ActiveDirectory)
    directory.conn = Mock()

    assert directory._default_naming_context() == DOMAIN
    directory.conn.search.assert_not_called()


def test_default_naming_context_root_dse_fallback_has_no_domain_scope(monkeypatch):
    monkeypatch.setattr(settings, "LDAP_BASE_DN", "OU=People," + DOMAIN)
    monkeypatch.setattr("sync_app.directory.domain_dn_from_base_dn", lambda _: "")
    directory = object.__new__(ActiveDirectory)
    directory.conn = Mock()
    directory.conn.result = {"result": 0}
    directory.conn.response = [entry("", defaultNamingContext=[DOMAIN])]

    assert directory._default_naming_context() == DOMAIN
    directory.conn.search.assert_called_once()
    assert directory.conn.search.call_args.kwargs["controls"] == []


def test_search_can_disable_domain_scope_for_policy_base_queries():
    directory = object.__new__(ActiveDirectory)
    directory.conn = Mock()
    directory.conn.result = {"result": 0}
    directory.conn.response = [entry(DOMAIN)]

    rows = directory.search(
        "(objectClass=domainDNS)",
        base=DOMAIN,
        attrs=["minPwdLength"],
        scope=BASE,
        domain_scope=False,
    )

    assert rows == [entry(DOMAIN)]
    assert directory.conn.search.call_args.kwargs["controls"] == []


def test_domain_root_from_base_dn_uses_only_trailing_dc_components():
    assert domain_dn_from_base_dn("OU=People," + DOMAIN) == DOMAIN
    assert domain_dn_from_base_dn("OU=People,OU=Group") == ""
