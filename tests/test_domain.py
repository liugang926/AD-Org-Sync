from collections import Counter
import pytest
from sync_app.domain import candidate, resolve
from sync_app.directory import under
from .fakes import user, account


def test_manual_binding_survives_changed_employee_number():
    target = account()
    action, result, _ = resolve(user(employee="changed"), {"guid": target["guid"], "enabled": True}, [target], set(), "employee_id", Counter(), Counter())
    assert action == "update" and result == target


def test_duplicate_employee_never_auto_links():
    assert resolve(user(), None, [account()], set(), "employee_id", Counter({"1001": 2}), Counter())[0] == "conflict"


def test_duplicate_target_never_auto_links():
    assert resolve(user(), None, [account(), account()], set(), "employee_id", Counter({"1001": 1}), Counter())[0] == "conflict"


def test_recreated_same_name_does_not_reuse_binding():
    old, new = account(), account()
    assert resolve(user(), {"guid": old["guid"], "enabled": True}, [new], set(), "employee_id", Counter(), Counter())[0] == "conflict"


def test_normalization_and_scope_boundary():
    assert candidate({"employee_id": "ab/c"}, "employee_id") == "abc"
    assert under("CN=A,OU=People,DC=example,DC=com", "OU=People,DC=example,DC=com")
    assert not under("CN=A,OU=OtherPeople,DC=example,DC=com", "OU=People,DC=example,DC=com")


@pytest.mark.parametrize(
    ("naming", "candidate_value", "names", "accounts", "protected_names", "expected"),
    [
        ("email", "@example.com", Counter({"": 1}), [], (), "规范化后为空"),
        ("employee_id", "1001", Counter({"1001": 2}), [], (), "来源内重复"),
        ("employee_id", "1001", Counter({"1001": 1}), [account("other", "1001")], (), "AD 占用"),
        ("employee_id", "administrator", Counter({"administrator": 1}), [], (), "受保护"),
        ("employee_id", "1001", Counter({"1001": 1}), [], ("1001",), "受保护"),
    ],
)
def test_new_account_naming_conflict_explains_cause(naming, candidate_value, names, accounts, protected_names, expected):
    person = user(employee="1001")
    person[naming] = candidate_value
    action, target, reason = resolve(
        person, None, accounts, set(), naming,
        Counter({person["employee_id"].casefold(): 1}), names, "employee_id", protected_names,
    )
    assert action == "conflict" and target is None
    assert expected in reason
