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
def test_empty_source_and_changed_binding_block_writes(configured):
    ad = Directory()
    with pytest.raises(RuleError):
        plan(Job.objects.create(), Source([]), ad)
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
