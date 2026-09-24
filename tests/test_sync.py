import pytest
from sync_app.models import Configuration, Binding, Person, Job
from sync_app.synchronization import plan, apply, enqueue, queue_apply
from sync_app.domain import RuleError
from .fakes import Source, Directory, user, account


@pytest.fixture
def configured():
    config = Configuration.current()
    config.root_ou = "OU=People,DC=example,DC=com"
    config.save()
    return config


@pytest.mark.django_db
def test_preview_read_only_and_idempotent_execution(configured):
    source, ad = Source(), Directory([])
    job = Job.objects.create()
    job.plan = plan(job, source, ad)
    assert not Binding.objects.exists() and ad.created == 0
    assert apply(job, source, ad) == "success"
    assert Binding.objects.count() == 1 and ad.created == 1
    next_job = Job.objects.create()
    next_job.plan = plan(next_job, source, ad)
    assert apply(next_job, source, ad) == "success"
    assert ad.created == 1


@pytest.mark.django_db
def test_changed_config_binding_source_or_ad_blocks_before_write(configured):
    source, ad = Source(), Directory()
    job = Job.objects.create()
    job.plan = plan(job, source, ad)
    source.users[0]["name"] = "更名"
    with pytest.raises(RuleError):
        apply(job, source, ad)
    source.users[0]["name"] = "测试员工"
    ad.items[0]["enabled"] = False
    with pytest.raises(RuleError):
        apply(job, source, ad)
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_partial_scope_never_disables_missing(configured):
    configured.disable_missing = True
    configured.save()
    target = account("2000", "departed")
    person = Person.objects.create(source_id="gone", name="已离职")
    Binding.objects.create(person=person, object_guid=target["guid"], username=target["username"])
    job = Job.objects.create(scope="users", selected=["u1"])
    result = plan(job, Source(), Directory([account(), target]))
    assert not any(o["action"] == "disable" for o in result["operations"])


@pytest.mark.django_db
def test_mass_disable_requires_confirmation(configured):
    configured.disable_missing = True
    configured.save()
    target = account("2000", "departed")
    person = Person.objects.create(source_id="gone", name="已离职")
    Binding.objects.create(person=person, object_guid=target["guid"], username=target["username"])
    source, ad = Source(), Directory([account(), target])
    job = Job.objects.create()
    job.plan = plan(job, source, ad)
    assert job.plan["high_risk"]
    with pytest.raises(RuleError):
        apply(job, source, ad)
    assert not ad.disabled
    job.confirmed = True
    assert apply(job, source, ad) == "success"
    assert ad.disabled == [target["guid"]]


@pytest.mark.django_db
def test_successful_person_binding_survives_later_failure(configured):
    one, two = account(), account("1002", "other")
    source, ad = Source([user(), user("u2", "1002")]), Directory([one, two])
    ad.fail_update.add(two["guid"])
    job = Job.objects.create()
    job.plan = plan(job, source, ad)
    assert apply(job, source, ad) == "partial_failed"
    assert Binding.objects.get().person.source_id == "u1"


@pytest.mark.django_db
def test_duplicate_enqueue_and_apply_replay_denied(configured):
    job = enqueue()
    with pytest.raises(RuleError):
        enqueue()
    with pytest.raises(RuleError):
        queue_apply(job.pk, "admin")


@pytest.mark.django_db
def test_conflicting_preview_cannot_be_queued_for_apply(configured):
    job = Job.objects.create(status="preview_ready", plan={
        "operations": [{"action": "conflict"}], "departments": [], "high_risk": False,
    })
    with pytest.raises(RuleError, match="处理计划冲突"):
        queue_apply(job.pk, "admin")
    job.refresh_from_db()
    assert job.kind == "preview" and job.status == "preview_ready"


@pytest.mark.django_db
def test_empty_source_and_changed_binding_block_writes(configured):
    ad = Directory()
    with pytest.raises(RuleError):
        plan(Job.objects.create(), Source([]), ad)
    configured.refresh_from_db()
    assert not configured.identity_anchor
    source = Source()
    job = Job.objects.create()
    job.plan = plan(job, source, ad)
    Person.objects.update(excluded=True)
    with pytest.raises(RuleError):
        apply(job, source, ad)
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_changed_enterprise_cannot_reuse_bindings(configured, settings):
    plan(Job.objects.create(), Source(), Directory())
    configured.refresh_from_db()
    assert configured.identity_anchor
    settings.DINGTALK_CORP_ID = "different-enterprise"
    with pytest.raises(RuleError, match="企业"):
        plan(Job.objects.create(), Source(), Directory())


@pytest.mark.django_db
def test_worker_marks_interrupted_job_and_never_replays_it(configured, monkeypatch):
    from sync_app.synchronization import run_next
    interrupted = Job.objects.create(status="running", kind="apply")
    assert run_next() is False
    interrupted.refresh_from_db()
    assert interrupted.status == "failed"


@pytest.mark.django_db
def test_empty_department_is_planned_and_created(configured):
    from sync_app.models import DepartmentBinding
    class WithEmptyDepartment(Source):
        def collect(self, root):
            users, departments = super().collect(root)
            return users, departments + [{"id": "2", "name": "空部门", "parent": "1"}]
    source, ad = WithEmptyDepartment(), Directory()
    job = Job.objects.create()
    job.plan = plan(job, source, ad)
    empty = next(d for d in job.plan["departments"] if d["source_id"] == "2")
    assert empty["guid"] is None
    assert apply(job, source, ad) == "success"
    assert DepartmentBinding.objects.get(source_id="2").dn.startswith("OU=空部门,")


@pytest.mark.django_db
def test_new_account_appearing_after_preview_blocks_all_writes(configured):
    source, ad = Source(), Directory([])
    job = Job.objects.create()
    job.plan = plan(job, source, ad)
    ad.items.append(account())
    with pytest.raises(RuleError):
        apply(job, source, ad)
    assert ad.created == 0
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_binding_confirmation_rejects_replaced_target_and_replay(configured, monkeypatch):
    from sync_app import synchronization as sync
    source, ad = Source(), Directory()
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    person = Person.objects.create(source_id="u1", name="测试员工")
    review = sync.binding_review(person.pk, "testuser")
    original = ad.items[0]
    ad.items[0] = account()
    with pytest.raises(RuleError, match="目标已变化"):
        sync.bind_person(person.pk, review["confirmation"], "admin", "确认身份")
    assert not Binding.objects.exists()
    ad.items[0] = original
    sync.bind_person(person.pk, review["confirmation"], "admin", "确认身份")
    assert Binding.objects.get().manual
    with pytest.raises(RuleError, match="绑定已变化"):
        sync.bind_person(person.pk, review["confirmation"], "admin", "重复提交")


@pytest.mark.django_db
@pytest.mark.parametrize("change", ["employee_id", "enabled", "dn"])
def test_manual_binding_requires_new_review_when_ad_target_state_changes(configured, monkeypatch, change):
    from sync_app import synchronization as sync

    source, ad = Source(), Directory()
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    person = Person.objects.create(source_id="u1", name="测试员工")
    review = sync.binding_review(person.pk, "testuser")
    if change == "employee_id":
        ad.items[0]["employee_id"] = "another-person"
    elif change == "enabled":
        ad.items[0]["enabled"] = False
    else:
        ad.items[0]["dn"] = "CN=testuser,OU=Other,OU=People,DC=example,DC=com"

    with pytest.raises(RuleError, match="目标状态已变化"):
        sync.bind_person(person.pk, review["confirmation"], "admin", "确认身份")
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_manual_binding_rechecks_protection_under_account_lock(configured, monkeypatch):
    from sync_app import synchronization as sync

    source, ad = Source(), Directory()
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    person = Person.objects.create(source_id="u1", name="测试员工")
    review = sync.binding_review(person.pk, "testuser")
    original_by_guid = ad.by_guid

    def protected_after_lookup(guid):
        target = original_by_guid(guid)
        target["protected"] = True
        return target

    monkeypatch.setattr(ad, "by_guid", protected_after_lookup)
    with pytest.raises(RuleError, match="目标受保护"):
        sync.bind_person(person.pk, review["confirmation"], "admin", "确认身份")
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_old_binding_confirmation_without_ad_state_requires_new_review(configured, monkeypatch):
    from django.core import signing
    from sync_app import synchronization as sync

    source, ad = Source(), Directory()
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    person = Person.objects.create(source_id="u1", name="测试员工")
    legacy_confirmation = signing.dumps({
        "person": person.pk, "username": ad.items[0]["username"],
        "guid": ad.items[0]["guid"], "revision": "",
    }, salt="binding-review")

    with pytest.raises(RuleError, match="目标状态已变化"):
        sync.bind_person(person.pk, legacy_confirmation, "admin", "确认身份")
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_manual_binding_to_disabled_ad_account_waits_for_explicit_reactivation(configured, monkeypatch):
    from sync_app import synchronization as sync

    source = Source()
    target = account()
    target["enabled"] = False
    ad = Directory([target])
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    person = Person.objects.create(source_id="u1", name="测试员工")

    review = sync.binding_review(person.pk, "testuser")
    sync.bind_person(person.pk, review["confirmation"], "admin", "人工确认已禁用账号")
    binding = Binding.objects.get(person=person)
    assert binding.manual and not binding.enabled
    assert not ad.items[0]["enabled"]
    assert plan(Job.objects.create(), source, ad)["operations"][0]["action"] == "skip"

    sync.reactivate_person(person.pk, "admin", "核验后恢复", True)
    binding.refresh_from_db()
    assert binding.enabled and ad.items[0]["enabled"]
    assert plan(Job.objects.create(), source, ad)["operations"][0]["action"] == "update"


@pytest.mark.django_db
def test_manual_binding_rechecks_live_source_scope(configured, monkeypatch):
    from sync_app import synchronization as sync

    source, ad = Source(), Directory()
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    person = Person.objects.create(source_id="u1", name="测试员工")
    source.user_in_scope = lambda employee, root: False
    with pytest.raises(RuleError, match="当前同步范围"):
        sync.binding_review(person.pk, "testuser")

    source.user_in_scope = lambda employee, root: True
    review = sync.binding_review(person.pk, "testuser")
    source.user_in_scope = lambda employee, root: False
    with pytest.raises(RuleError, match="当前同步范围"):
        sync.bind_person(person.pk, review["confirmation"], "admin", "确认身份")
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_reactivation_rechecks_live_source_scope(configured, monkeypatch):
    from sync_app import synchronization as sync

    source = Source()
    target = account()
    target["enabled"] = False
    ad = Directory([target])
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    person = Person.objects.create(source_id="u1", name="测试员工")
    Binding.objects.create(person=person, object_guid=target["guid"], username=target["username"])

    source.user_in_scope = lambda employee, root: False
    with pytest.raises(RuleError, match="当前同步范围"):
        sync.reactivate_person(person.pk, "admin", "人工确认", True)
    assert not ad.items[0]["enabled"]

    source.user_in_scope = lambda employee, root: True
    sync.reactivate_person(person.pk, "admin", "人工确认", True)
    assert ad.items[0]["enabled"]


@pytest.mark.django_db
def test_new_account_policy_can_keep_account_disabled(configured):
    configured.enable_new_accounts = False
    configured.require_password_change = False
    configured.save()
    source, ad = Source(), Directory([])
    job = Job.objects.create()
    job.plan = plan(job, source, ad)
    assert apply(job, source, ad) == "success"
    assert ad.items[0]["enabled"] is False
    assert ad.items[0]["require_change"] is False
    assert not Binding.objects.get().enabled

    repeat = Job.objects.create()
    repeat.plan = plan(repeat, source, ad)
    assert repeat.plan["operations"][0]["action"] == "skip"
    assert apply(repeat, source, ad) == "success"
    assert ad.created == 1


@pytest.mark.django_db
def test_disabled_binding_with_externally_enabled_ad_target_is_conflict(configured):
    configured.enable_new_accounts = False
    configured.save()
    source, ad = Source(), Directory([])
    first = Job.objects.create()
    first.plan = plan(first, source, ad)
    assert apply(first, source, ad) == "success"
    ad.items[0]["enabled"] = True
    operation = plan(Job.objects.create(), source, ad)["operations"][0]
    assert operation["action"] == "conflict"
    assert "停用绑定" in operation["reason"]


@pytest.mark.django_db
@pytest.mark.parametrize("enable_new_accounts", [True, False])
def test_created_account_stays_disabled_until_attributes_finish(configured, enable_new_accounts):
    configured.enable_new_accounts = enable_new_accounts
    configured.save()

    class FailingUpdate(Directory):
        fail_once = True

        def update(self, guid, attrs, ou, root, *, allow_disabled=False):
            assert allow_disabled and not self.by_guid(guid)["enabled"]
            if self.fail_once:
                self.fail_once = False
                raise RuleError("模拟属性更新失败")
            return super().update(guid, attrs, ou, root, allow_disabled=allow_disabled)

    source, ad = Source(), FailingUpdate([])
    job = Job.objects.create()
    job.plan = plan(job, source, ad)
    assert apply(job, source, ad) == "partial_failed"
    assert ad.created == 1 and not ad.items[0]["enabled"]
    assert not Binding.objects.exists()
    retry_job = Job.objects.create()
    retry_job.plan = plan(retry_job, source, ad)
    retry = retry_job.plan["operations"][0]
    assert retry["action"] == "resume_create"
    assert retry["target"]["guid"] == ad.items[0]["guid"]
    assert apply(retry_job, source, ad) == "success"
    assert Binding.objects.get().enabled is enable_new_accounts
    assert ad.items[0]["enabled"] is enable_new_accounts
    assert ad.created == 1


@pytest.mark.django_db
def test_ad_revision_change_invalidates_preview_before_write(configured):
    source, ad = Source(), Directory()
    job = Job.objects.create()
    job.plan = plan(job, source, ad)
    ad.items[0]["ad_revision"] = "2"  # e.g. password changed outside the app
    with pytest.raises(RuleError, match="AD 状态已变化"):
        apply(job, source, ad)
    assert not Binding.objects.exists()


@pytest.mark.django_db
@pytest.mark.parametrize("enable_failures", [1, 2])
def test_enable_failure_resumes_initialized_account_without_recreating(configured, enable_failures):
    from sync_app.domain import fingerprint
    from sync_app.models import Operation

    configured.attributes = ["displayName"]
    configured.save()

    class FailingEnable(Directory):
        remaining = enable_failures

        def enable(self, guid, root):
            if self.remaining:
                self.remaining -= 1
                raise RuleError("模拟启用失败")
            return super().enable(guid, root)

    source, ad = Source(), FailingEnable([])
    first = Job.objects.create()
    first.plan = plan(first, source, ad)
    assert apply(first, source, ad) == "partial_failed"
    assert ad.created == 1 and not ad.items[0]["enabled"]
    assert ad.items[0]["attrs"]["displayName"] == source.users[0]["name"]
    assert not Binding.objects.exists()
    creation = Operation.objects.get(job=first, action="create")
    assert creation.evidence["initialized_fingerprint"] == fingerprint(ad.items[0])

    while ad.remaining:
        retry = Job.objects.create()
        retry.plan = plan(retry, source, ad)
        assert retry.plan["operations"][0]["action"] == "resume_create"
        assert apply(retry, source, ad) == "partial_failed"
        creation.refresh_from_db()
        assert creation.evidence["initialized_fingerprint"] == fingerprint(ad.items[0])

    final = Job.objects.create()
    final.plan = plan(final, source, ad)
    assert final.plan["operations"][0]["action"] == "resume_create"
    assert apply(final, source, ad) == "success"
    assert ad.created == 1 and ad.items[0]["enabled"]
    assert str(Binding.objects.get().object_guid) == ad.items[0]["guid"]
    creation.refresh_from_db()
    assert creation.evidence["enabled_fingerprint"] == fingerprint(ad.items[0])


@pytest.mark.django_db
def test_initialized_account_changed_after_enable_failure_requires_manual_recovery(configured):
    configured.attributes = ["displayName"]
    configured.save()

    class FailingEnable(Directory):
        def enable(self, guid, root):
            raise RuleError("模拟启用失败")

    source, ad = Source(), FailingEnable([])
    first = Job.objects.create()
    first.plan = plan(first, source, ad)
    assert apply(first, source, ad) == "partial_failed"
    ad.items[0]["attrs"]["title"] = "外部修改"
    retry = plan(Job.objects.create(), source, ad)["operations"][0]
    assert retry["action"] == "conflict"
    assert retry["target"]["guid"] == ad.items[0]["guid"]
    assert not Binding.objects.exists()


@pytest.mark.django_db
@pytest.mark.parametrize("ad_revision", ["2", "", "0"])
def test_external_password_change_or_missing_revision_blocks_disabled_create_recovery(configured, ad_revision):
    source, ad = Source(), Directory([])
    def fail_update(*args, **kwargs):
        raise RuleError("模拟属性更新失败")

    ad.update = fail_update
    first = Job.objects.create()
    first.plan = plan(first, source, ad)
    assert apply(first, source, ad) == "partial_failed"
    assert ad.created == 1 and not ad.items[0]["enabled"]
    ad.items[0]["ad_revision"] = ad_revision
    retry = plan(Job.objects.create(), source, ad)["operations"][0]
    assert retry["action"] == "conflict"
    assert retry["target"]["guid"] == ad.items[0]["guid"]
    assert not Binding.objects.exists()


@pytest.mark.django_db
@pytest.mark.parametrize("change", ["ad", "configuration"])
def test_created_account_changed_externally_requires_manual_recovery(configured, change):
    source, ad = Source(), Directory([])
    original_update = ad.update

    def fail_update(*args, **kwargs):
        raise RuleError("模拟属性更新失败")

    ad.update = fail_update
    job = Job.objects.create()
    job.plan = plan(job, source, ad)
    assert apply(job, source, ad) == "partial_failed"
    assert ad.created == 1 and not ad.items[0]["enabled"]
    ad.update = original_update
    if change == "ad":
        ad.items[0]["attrs"]["title"] = "外部修改"
    else:
        configured.attributes = ["title"]
        configured.save()
    retry = plan(Job.objects.create(), source, ad)["operations"][0]
    assert retry["action"] == "conflict"
    assert retry["target"]["guid"] == ad.items[0]["guid"]
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_manual_reactivation_enables_held_account_and_binding(configured, monkeypatch):
    from sync_app import synchronization as sync

    configured.enable_new_accounts = False
    configured.save()
    source, ad = Source(), Directory([])
    first = Job.objects.create()
    first.plan = plan(first, source, ad)
    assert apply(first, source, ad) == "success"
    binding = Binding.objects.get()
    old_revision = binding.revision
    assert not binding.enabled and not ad.items[0]["enabled"]

    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    source.user_in_scope = lambda employee, root: False
    with pytest.raises(RuleError, match="当前同步范围"):
        sync.reactivate_person(binding.person.pk, "admin", "核验后启用", True)
    assert not ad.items[0]["enabled"]
    binding.refresh_from_db()
    assert not binding.enabled

    source.user_in_scope = lambda employee, root: True
    sync.reactivate_person(binding.person.pk, "admin", "核验后启用", True)
    binding.refresh_from_db()
    assert binding.enabled and binding.revision != old_revision
    assert ad.items[0]["enabled"]
    assert plan(Job.objects.create(), source, ad)["operations"][0]["action"] == "update"


@pytest.mark.django_db
def test_manual_reactivation_can_reconcile_external_enable(configured, monkeypatch):
    from sync_app import synchronization as sync

    configured.enable_new_accounts = False
    configured.save()
    source, ad = Source(), Directory([])
    first = Job.objects.create()
    first.plan = plan(first, source, ad)
    assert apply(first, source, ad) == "success"
    binding = Binding.objects.get()
    ad.items[0]["enabled"] = True

    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    sync.reactivate_person(binding.person.pk, "admin", "核验外部启用", True)
    binding.refresh_from_db()
    assert binding.enabled and ad.items[0]["enabled"]
    assert plan(Job.objects.create(), source, ad)["operations"][0]["action"] == "update"


@pytest.mark.django_db
def test_refresh_does_not_require_ad_or_mark_full_sync_success(configured, monkeypatch):
    from sync_app import synchronization as sync
    from sync_app.models import Snapshot, RuntimeState
    monkeypatch.setattr(sync, "DingTalk", Source)
    def unavailable():
        raise AssertionError("通讯录刷新不应连接 AD")
    monkeypatch.setattr(sync, "ActiveDirectory", unavailable)
    job = enqueue(kind="refresh", actor="admin")
    assert sync.run_next()
    job.refresh_from_db()
    assert job.status == "success"
    assert Snapshot.objects.count() == 1
    assert RuntimeState.current().last_full_success is None
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_connection_failure_preserves_independent_results(configured, monkeypatch):
    from sync_app import synchronization as sync
    from sync_app.models import RuntimeState
    monkeypatch.setattr(sync, "DingTalk", Source)
    def unavailable():
        raise RuleError("LDAPS 证书验证失败")
    monkeypatch.setattr(sync, "ActiveDirectory", unavailable)
    job = enqueue(kind="connections", actor="admin")
    assert sync.run_next()
    job.refresh_from_db()
    assert job.status == "failed"
    state = RuntimeState.current()
    assert state.connection_checks["钉钉通讯录"]["success"]
    assert not state.connection_checks["LDAPS"]["success"]
    assert state.connections_checked_at is not None


@pytest.mark.django_db
def test_empty_attribute_clearing_requires_explicit_policy(configured):
    configured.attributes = ["telephoneNumber"]
    configured.save()
    ad = Directory()
    ad.items[0]["attrs"]["telephoneNumber"] = "12345"
    first = plan(Job.objects.create(), Source(), ad)["operations"][0]
    assert "telephoneNumber" not in first["attrs"]
    configured.clear_attributes = ["telephoneNumber"]
    configured.save()
    second = plan(Job.objects.create(), Source(), ad)["operations"][0]
    assert second["attrs"]["telephoneNumber"] == ""
    assert {"field": "telephoneNumber", "before": "12345", "after": ""} in second["changes"]


@pytest.mark.django_db
def test_email_match_requires_manual_confirmation(configured):
    configured.match_field = "email"
    configured.save()
    operation = plan(Job.objects.create(), Source(), Directory())["operations"][0]
    assert operation["action"] == "conflict"
    assert "人工确认" in operation["reason"]
    assert operation["target"]["username"] == "testuser"
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_configured_protected_username_blocks_new_account(configured):
    configured.protected_usernames = ["1001"]
    configured.save()
    operation = plan(Job.objects.create(), Source(), Directory([]))["operations"][0]
    assert operation["action"] == "conflict"
    assert "受保护" in operation["reason"]


@pytest.mark.django_db
def test_department_transfer_preserves_bound_object(configured):
    class TransferredSource(Source):
        def collect(self, root):
            users, departments = super().collect(root)
            users[0]["departments"] = ["2"]
            users[0]["primary_department"] = "2"
            return users, departments + [{"id": "2", "name": "研发", "parent": "1"}]
    source, ad = Source(), Directory()
    initial = Job.objects.create()
    initial.plan = plan(initial, source, ad)
    assert apply(initial, source, ad) == "success"
    old_guid = Binding.objects.get().object_guid
    transfer = Job.objects.create()
    transfer.plan = plan(transfer, TransferredSource(), ad)
    operation = transfer.plan["operations"][0]
    assert operation["action"] == "move"
    assert apply(transfer, TransferredSource(), ad) == "success"
    assert Binding.objects.get().object_guid == old_guid
    assert ad.items[0]["dn"].startswith("CN=testuser,OU=研发,")
    assert ad.created == 0


@pytest.mark.django_db
def test_partial_success_does_not_replace_full_success_marker(configured, monkeypatch):
    from sync_app import synchronization as sync
    from sync_app.models import RuntimeState
    source, ad = Source(), Directory()
    monkeypatch.setattr(sync, "DingTalk", lambda: source)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: ad)
    full = enqueue(kind="scheduled")
    sync.run_next()
    full.refresh_from_db()
    assert full.status == "success"
    marker = RuntimeState.current().last_full_success
    assert marker is not None
    partial = enqueue(kind="scheduled", scope="users", selected=["u1"])
    sync.run_next()
    partial.refresh_from_db()
    assert partial.status == "success"
    assert RuntimeState.current().last_full_success == marker


@pytest.mark.django_db
def test_binding_commit_failure_recovers_by_guid_even_after_employee_change(configured, monkeypatch):
    from django.db import IntegrityError
    from sync_app.models import Operation
    source, ad = Source(), Directory([])
    job = Job.objects.create()
    job.plan = plan(job, source, ad)
    original_create = Binding.objects.create
    def fail_commit(**kwargs):
        raise IntegrityError("simulated binding commit failure")
    monkeypatch.setattr(Binding.objects, "create", fail_commit)
    assert apply(job, source, ad) == "partial_failed"
    assert not Binding.objects.exists()
    evidence = Operation.objects.get(job=job, action="create")
    assert evidence.target_guid is not None and evidence.status == "failed"
    monkeypatch.setattr(Binding.objects, "create", original_create)
    source.users[0]["employee_id"] = "changed-employee-id"
    retry = Job.objects.create()
    retry.plan = plan(retry, source, ad)
    assert retry.plan["operations"][0]["action"] == "update"
    assert apply(retry, source, ad) == "success"
    assert Binding.objects.get().object_guid == evidence.target_guid
    assert ad.created == 1


@pytest.mark.django_db
def test_enabled_unbound_account_changed_after_commit_failure_requires_manual_recovery(configured, monkeypatch):
    from django.db import IntegrityError
    from sync_app.models import Operation

    source, ad = Source(), Directory([])
    job = Job.objects.create()
    job.plan = plan(job, source, ad)

    def fail_commit(**kwargs):
        raise IntegrityError("simulated binding commit failure")

    monkeypatch.setattr(Binding.objects, "create", fail_commit)
    assert apply(job, source, ad) == "partial_failed"
    evidence = Operation.objects.get(job=job, action="create").evidence
    assert evidence["enabled_fingerprint"]
    assert ad.created == 1 and ad.items[0]["enabled"]
    ad.items[0]["attrs"]["title"] = "外部修改"

    retry = plan(Job.objects.create(), source, ad)["operations"][0]
    assert retry["action"] == "conflict"
    assert retry["target"]["guid"] == ad.items[0]["guid"]
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_external_password_change_blocks_enabled_unbound_recovery(configured, monkeypatch):
    from django.db import IntegrityError

    source, ad = Source(), Directory([])
    first = Job.objects.create()
    first.plan = plan(first, source, ad)

    def fail_commit(**kwargs):
        raise IntegrityError("simulated binding commit failure")

    monkeypatch.setattr(Binding.objects, "create", fail_commit)
    assert apply(first, source, ad) == "partial_failed"
    assert ad.items[0]["enabled"]
    ad.items[0]["ad_revision"] = str(int(ad.items[0]["ad_revision"]) + 1)

    retry = plan(Job.objects.create(), source, ad)["operations"][0]
    assert retry["action"] == "conflict"
    assert retry["target"]["guid"] == ad.items[0]["guid"]
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_uncertain_creation_cannot_be_automatically_repeated(configured):
    from sync_app.models import Operation
    previous = Job.objects.create(status="failed")
    Operation.objects.create(job=previous, source_id="u1", action="create", status="failed")
    result = plan(Job.objects.create(), Source(), Directory([]))
    assert result["operations"][0]["action"] == "conflict"
    assert "可靠对象证据" in result["operations"][0]["reason"]


@pytest.mark.django_db
def test_creation_response_lost_after_ad_write_cannot_create_again(configured, monkeypatch):
    from sync_app.models import Operation

    source, ad = Source(), Directory([])
    first = Job.objects.create()
    first.plan = plan(first, source, ad)
    original_create = ad.create

    def create_then_lose_response(*args, **kwargs):
        original_create(*args, **kwargs)
        raise RuleError("创建响应丢失，结果需要人工核验")

    monkeypatch.setattr(ad, "create", create_then_lose_response)
    assert apply(first, source, ad) == "partial_failed"
    record = Operation.objects.get(job=first, action="create")
    assert record.status == "failed" and record.target_guid is None
    assert ad.created == 1 and len(ad.items) == 1
    assert not Binding.objects.exists()

    retry = Job.objects.create()
    retry.plan = plan(retry, source, ad)
    assert retry.plan["operations"][0]["action"] == "conflict"
    with pytest.raises(RuleError, match="人员冲突"):
        apply(retry, source, ad)
    assert ad.created == 1 and len(ad.items) == 1
    assert not Binding.objects.exists()


@pytest.mark.django_db
def test_cleanup_preserves_unresolved_creation_evidence(configured):
    from datetime import timedelta
    from django.core.management import call_command
    from django.utils import timezone
    from sync_app.models import Operation
    previous = Job.objects.create(status="failed")
    Job.objects.filter(pk=previous.pk).update(created_at=timezone.now() - timedelta(days=100))
    Operation.objects.create(job=previous, source_id="u1", action="create", status="failed")
    call_command("cleanup")
    assert Job.objects.filter(pk=previous.pk).exists()
    assert plan(Job.objects.create(), Source(), Directory([]))["operations"][0]["action"] == "conflict"


@pytest.mark.django_db
def test_scheduler_runs_daily_retention_when_sync_is_disabled(configured):
    from datetime import timedelta
    from django.core.management import call_command
    from django.utils import timezone
    from sync_app.models import Audit, RuntimeState, Snapshot

    assert not configured.schedule_enabled
    old = timezone.now() - timedelta(days=200)
    snapshot = Snapshot.objects.create(fingerprint="old", root_department="1", users=[], departments=[])
    Snapshot.objects.create(fingerprint="latest", root_department="1", users=[], departments=[])
    Snapshot.objects.filter(pk=snapshot.pk).update(created_at=old)
    preview = Job.objects.create(status="preview_ready")
    Job.objects.filter(pk=preview.pk).update(created_at=old)
    audit = Audit.objects.create(actor="test", action="preview", result="success")
    Audit.objects.filter(pk=audit.pk).update(created_at=old)

    call_command("enqueue_sync", due=True)
    assert not Snapshot.objects.filter(pk=snapshot.pk).exists()
    assert not Job.objects.filter(pk=preview.pk).exists()
    assert not Audit.objects.filter(pk=audit.pk).exists()
    assert RuntimeState.current().last_cleanup_at is not None
    assert not Job.objects.exists()

    later = Job.objects.create(status="needs_confirmation")
    Job.objects.filter(pk=later.pk).update(created_at=old)
    call_command("enqueue_sync", due=True)
    assert Job.objects.filter(pk=later.pk).exists()
    RuntimeState.objects.update(last_cleanup_at=old)
    call_command("enqueue_sync", due=True)
    assert not Job.objects.filter(pk=later.pk).exists()
