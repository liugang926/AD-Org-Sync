from copy import deepcopy
from unittest.mock import Mock
import uuid

import pytest

from sync_app import synchronization as sync
from sync_app.domain import RuleError
from sync_app.models import DepartmentBinding, Job, Operation
from .test_ou_sync import ROOT, StrictDirectory, TreeSource


def preview(monkeypatch, directory):
    monkeypatch.setattr(sync, "DingTalk", TreeSource)
    monkeypatch.setattr(sync, "ActiveDirectory", lambda: directory)
    job = sync.enqueue(scope="organization")
    assert sync.run_next()
    job.refresh_from_db()
    assert job.status == "preview_ready"
    return job


@pytest.mark.django_db
def test_completed_ou_rows_use_this_jobs_evidence_and_preserve_preview(admin_client, monkeypatch):
    directory = StrictDirectory(ROOT)
    job = preview(monkeypatch, directory)
    original = deepcopy(job.plan)
    sync.queue_apply(job.pk, "admin")
    assert sync.run_next()
    job.refresh_from_db()
    assert job.status == "success" and len(directory.writes) == 3

    # Later binding changes and another task cannot rewrite a historical result.
    DepartmentBinding.objects.all().delete()
    another = Job.objects.create()
    Operation.objects.create(job=another, source_id="department:1", action="ensure_ou", status="failed")
    ldap = Mock(side_effect=AssertionError("Viewing results must not contact AD"))
    monkeypatch.setattr(sync, "ActiveDirectory", ldap)
    response = admin_client.get(f"/jobs/{job.pk}")
    rows = response.context["department_rows"]
    assert [r["label"] for r in rows].count("已创建") == 3
    assert [r["label"] for r in rows].count("已关联") == 1
    assert all(r["guid"] for r in rows)
    assert response.context["department_result_counts"] == {"success": 4, "failed": 0, "unverified": 0, "waiting": 0}
    html = response.content.decode()
    assert "待创建" not in html and "部门 OU 执行结果" in html
    assert "本次执行已确认根 OU" in html
    job.refresh_from_db()
    assert job.plan == original
    ldap.assert_not_called()


@pytest.mark.django_db
def test_partial_failure_distinguishes_completed_failed_and_unexecuted_ous(admin_client, monkeypatch):
    directory = StrictDirectory()
    job = preview(monkeypatch, directory)
    original = deepcopy(job.plan)
    ensure = directory.ensure_ou

    def fail_third(dn, root, **kwargs):
        if dn == "OU=研发," + ROOT:
            raise RuleError("创建 OU 权限不足")
        return ensure(dn, root, **kwargs)

    directory.ensure_ou = fail_third
    sync.queue_apply(job.pk, "admin")
    assert sync.run_next()
    job.refresh_from_db()
    assert job.status == "partial_failed"
    response = admin_client.get(f"/jobs/{job.pk}")
    labels = {r["item"]["source_id"]: r["label"] for r in response.context["department_rows"]}
    assert labels == {"1": "已创建", "3": "已创建", "9": "执行失败", "2": "未执行"}
    assert response.context["department_result_counts"] == {"success": 2, "failed": 1, "unverified": 0, "waiting": 1}
    assert "创建 OU 权限不足" in response.content.decode()
    assert "根 OU 待创建" not in response.content.decode()
    job.refresh_from_db()
    assert job.plan == original


@pytest.mark.django_db
@pytest.mark.parametrize("evidence_case", ["missing", "pending", "no_guid", "wrong_path", "wrong_guid"])
def test_job_success_does_not_replace_missing_or_inconsistent_ou_evidence(admin_client, evidence_case):
    prior_guid = str(uuid.uuid4()) if evidence_case == "wrong_guid" else None
    job = Job.objects.create(kind="apply", status="success", scope="organization", plan={
        "departments": [{"source_id": "1", "name": "公司", "dn": ROOT, "guid": prior_guid, "action": "ensure_ou"}],
    })
    if evidence_case != "missing":
        Operation.objects.create(job=job, source_id="department:1", action="ensure_ou",
                                 status="pending" if evidence_case == "pending" else "success",
                                 target_guid=None if evidence_case == "no_guid" else uuid.uuid4(),
                                 evidence={"dn": "OU=Other," + ROOT if evidence_case == "wrong_path" else ROOT})
    response = admin_client.get(f"/jobs/{job.pk}")
    row, = response.context["department_rows"]
    assert row["label"] == "结果待核验" and not row["guid"]
    assert response.context["department_result_counts"]["success"] == 0


@pytest.mark.django_db
def test_ou_result_rows_include_evidence_beyond_operation_pagination(admin_client):
    departments = [{"source_id": str(i), "name": f"部门 {i}", "dn": f"OU=Dept{i}," + ROOT,
                    "guid": None, "action": "ensure_ou"} for i in range(52)]
    job = Job.objects.create(kind="apply", status="success", scope="organization", plan={"departments": departments})
    Operation.objects.bulk_create([
        Operation(job=job, source_id="department:" + d["source_id"], action="ensure_ou",
                  status="success", target_guid=uuid.uuid4(), evidence={"dn": d["dn"]}) for d in departments
    ])
    response = admin_client.get(f"/jobs/{job.pk}?result_page=2")
    assert len(response.context["operation_page"]) == 2
    assert len(response.context["department_rows"]) == 52
    assert all(r["label"] == "已创建" for r in response.context["department_rows"])
    assert response.context["department_result_counts"]["success"] == 52


@pytest.mark.django_db
def test_preview_and_pending_apply_keep_distinct_ou_states(admin_client, monkeypatch):
    job = preview(monkeypatch, StrictDirectory())
    response = admin_client.get(f"/jobs/{job.pk}")
    assert not response.context["execution_view"]
    assert all(r["label"] == "待创建" for r in response.context["department_rows"])
    sync.queue_apply(job.pk, "admin")
    response = admin_client.get(f"/jobs/{job.pk}")
    assert all(r["label"] == "待执行" for r in response.context["department_rows"])
    operation = Operation.objects.create(job=job, source_id="department:1", action="ensure_ou", evidence={"dn": ROOT})
    Job.objects.filter(pk=job.pk).update(status="running")
    assert admin_client.get(f"/jobs/{job.pk}").context["department_rows"][0]["label"] == "执行中"
    Job.objects.filter(pk=job.pk).update(status="failed")
    response = admin_client.get(f"/jobs/{job.pk}")
    assert response.context["department_rows"][0]["label"] == "结果待核验"
    operation.refresh_from_db()
    assert operation.status == "pending"  # A view must never repair execution evidence.
