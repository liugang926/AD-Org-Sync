from collections import Counter
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
