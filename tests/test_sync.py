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
