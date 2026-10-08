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


def resolve_job_sam(person, accounts, *, binding=None, occupied=(), source_counts=None, naming="employee_id", name_counts=None):
    return resolve(
        person, binding, accounts, set(occupied), naming,
        source_counts if source_counts is not None else Counter({person["employee_id"].strip().casefold(): 1}),
        name_counts if name_counts is not None else Counter({candidate(person, naming).casefold(): 1}),
        "employee_username",
    )


def test_job_sam_uses_original_employee_number_with_trim_and_casefold():
    person = user(employee="  T0001919  ")
    person["employee_username"] = "untrusted-alias"
    target = account(employee="", name=" t0001919 ")
    action, result, reason = resolve_job_sam(person, [target], naming="source_id")
    assert action == "bind" and result is target
    assert reason == "唯一工号与 AD 账号名匹配"


@pytest.mark.parametrize(
    ("employee", "source_counts"),
    [("", Counter({"": 1})), ("T0001919", Counter({"t0001919": 2}))],
)
def test_job_sam_missing_or_duplicate_source_number_cannot_bind(employee, source_counts):
    action, result, reason = resolve_job_sam(
        user(employee=employee), [account(employee="", name="T0001919")],
        source_counts=source_counts,
    )
    assert action == "conflict" and result is None
    assert reason == "匹配字段缺失或重复"


def test_job_sam_multiple_exact_directory_matches_cannot_bind():
    action, result, reason = resolve_job_sam(
        user(employee="T0001919"),
        [account(employee="", name="T0001919"), account(employee="", name="t0001919")],
    )
    assert action == "conflict" and result is None
    assert reason == "多个 AD 账号命中同一标识"


@pytest.mark.parametrize("state", ["occupied", "protected_domain_admin", "disabled", "builtin_name"])
def test_job_sam_exact_target_keeps_sync_protection_and_enabled_checks(state):
    job = "administrator" if state == "builtin_name" else "T0001919"
    target = account(employee="", name=job)
    if state == "protected_domain_admin":
        target.update(protected=True, domain_admin=True)
    if state == "disabled":
        target["enabled"] = False
    action, result, reason = resolve_job_sam(
        user(employee=job), [target],
        occupied={target["guid"]} if state == "occupied" else set(),
    )
    assert action == "conflict" and result is target
    assert reason == "AD 账号已占用、受保护或已禁用"


@pytest.mark.parametrize("raw_job", ["ab/c", "1001.", "A" * 20 + "suffix"])
def test_job_sam_never_matches_a_cleaned_or_truncated_candidate(raw_job):
    person = user(employee=raw_job)
    cleaned = candidate(person, "employee_id")
    assert cleaned != raw_job
    action, result, reason = resolve_job_sam(person, [account(employee="", name=cleaned)])
    assert action == "conflict" and result is None
    assert reason == "新账号名已被 AD 占用，请核验后人工绑定"


def test_job_sam_full_identifier_selects_exact_target_instead_of_truncated_name():
    raw_job = "A" * 20 + "suffix"
    exact = account(employee="", name=raw_job)
    truncated = account(employee="", name=raw_job[:20])
    action, result, _ = resolve_job_sam(
        user(employee=raw_job), [truncated, exact],
        name_counts=Counter({raw_job[:20].casefold(): 2}),
    )
    assert action == "bind" and result is exact


def test_job_sam_does_not_fall_back_to_employee_id_matching():
    action, result, reason = resolve_job_sam(
        user(employee="T0001919"), [account(employee="T0001919", name="different-login")],
    )
    assert action == "conflict" and result is None
    assert reason == "工号已存在于其他 AD 账号，请人工核验"


def test_job_sam_does_not_fall_back_to_source_id_or_email():
    person = user(uid="different-login", employee="T0001919")
    other = account(employee="", name=person["source_id"])
    other["email"] = person["email"]
    action, result, reason = resolve_job_sam(person, [other])
    assert action == "create" and result is None
    assert reason == "创建新账号"


def test_job_sam_new_account_keeps_the_selected_naming_strategy():
    person = user(uid="source-login", employee="T0001919")
    action, result, reason = resolve_job_sam(
        person, [account(employee="", name="source-login")], naming="source_id",
    )
    assert action == "conflict" and result is None
    assert reason == "新账号名已被 AD 占用，请核验后人工绑定"


def test_job_sam_existing_guid_binding_has_priority_over_new_sam_and_source_duplicates():
    bound = account(employee="old-job", name="old-login")
    matching = account(employee="", name="T0001919")
    action, result, reason = resolve_job_sam(
        user(employee="T0001919"), [matching, bound],
        binding={"guid": bound["guid"], "enabled": True},
        source_counts=Counter({"t0001919": 2}),
    )
    assert action == "update" and result is bound
    assert reason == "使用已有绑定"


def test_job_sam_recreated_sam_cannot_replace_bound_guid():
    old = account(employee="", name="T0001919")
    recreated = account(employee="", name="T0001919")
    action, result, reason = resolve_job_sam(
        user(employee="T0001919"), [recreated],
        binding={"guid": old["guid"], "enabled": True},
    )
    assert action == "conflict" and result is None
    assert reason == "绑定目标不存在或无法唯一确认"


@pytest.mark.parametrize("match_field", ["email", "source_id"])
def test_existing_nonemployee_matching_still_requires_manual_confirmation(match_field):
    person = user(employee="T0001919")
    target = account(employee="", name=person["source_id"])
    target["email"] = person["email"]
    action, result, reason = resolve(
        person, None, [target], set(), "employee_id",
        Counter({person[match_field].casefold(): 1}), Counter(), match_field,
    )
    assert action == "conflict" and result is target
    assert reason == "建议关联此账号，请在人员页面人工确认"


def test_original_employee_id_mode_does_not_inherit_job_sam_matching():
    person = user(employee="T0001919")
    action, result, reason = resolve(
        person, None, [account(employee="", name="T0001919")], set(), "employee_id",
        Counter({"t0001919": 1}), Counter({"t0001919": 1}), "employee_id",
    )
    assert action == "conflict" and result is None
    assert reason == "新账号名已被 AD 占用，请核验后人工绑定"


def test_original_employee_id_mode_still_binds_an_unrelated_sam():
    action, result, reason = resolve(
        user(employee="T0001919"), None, [target := account(employee="T0001919", name="unrelated-sam")],
        set(), "employee_id", Counter({"t0001919": 1}), Counter(), "employee_id",
    )
    assert action == "bind" and result is target
    assert reason == "唯一工号匹配"
