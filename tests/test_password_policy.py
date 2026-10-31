from datetime import timedelta
from unittest.mock import Mock

from django.conf import settings

from sync_app.directory import ActiveDirectory
from sync_app.domain import RuleError
from .fakes import account


DOMAIN = "DC=example,DC=com"


def entry(dn, **attributes):
    return {"type": "searchResEntry", "dn": dn, "attributes": attributes}


def policy_directory(user_rows, domain_rows, pso_rows=None):
    directory = object.__new__(ActiveDirectory)
    directory._default_naming_context = lambda: DOMAIN

    def search(query, base=None, attrs=None, scope=None):
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
    directory.search = lambda query, base=None, attrs=None, scope=None: (
        unavailable() if base == account()["dn"] else original_search(query, base, attrs, scope)
    )

    policy = directory.effective_password_policy(account())

    assert policy.minimum_length == 10
    assert policy.complexity_enabled is False
    assert policy.history_length == 5
    assert policy.source == "domain"
    assert policy.fallback is True
    assert "未能确认" in policy.source_label
    assert "private directory diagnostic" not in policy.source_label


def test_default_naming_context_reads_root_dse(monkeypatch):
    monkeypatch.setattr(settings, "LDAP_BASE_DN", "OU=People," + DOMAIN)
    directory = object.__new__(ActiveDirectory)
    directory.conn = Mock()
    directory.conn.result = {"result": 0}
    directory.conn.response = [entry("", defaultNamingContext=[DOMAIN])]

    assert directory._default_naming_context() == DOMAIN
    directory.conn.search.assert_called_once()
