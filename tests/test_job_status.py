from datetime import timedelta
from unittest.mock import Mock
import uuid

import pytest
from django.utils import timezone

from sync_app.models import Job


@pytest.mark.django_db
def test_status_requires_administrator_and_get(client, admin_client, django_user_model):
    job = Job.objects.create(kind="associate")
    url = f"/jobs/status?id={job.pk}"
    assert client.get(url).status_code == 302
    user = django_user_model.objects.create_user("viewer", is_staff=True)
    client.force_login(user)
    assert client.get(url).status_code == 403
    assert admin_client.post(url).status_code == 405
    assert "no-store" in admin_client.get(url)["Cache-Control"]


@pytest.mark.django_db
@pytest.mark.parametrize("ids", [[], ["invalid"], [str(uuid.uuid4())] * 13])
def test_status_rejects_invalid_or_unbounded_queries(admin_client, ids):
    assert admin_client.get("/jobs/status", {"id": ids}).status_code == 400


@pytest.mark.django_db
def test_status_tracks_real_completion_without_directory_calls_or_writes(admin_client, monkeypatch):
    from sync_app import synchronization as sync

    now = timezone.now()
    monkeypatch.setattr(timezone, "now", lambda: now)
    source, directory = Mock(), Mock()
    monkeypatch.setattr(sync, "DingTalk", source)
    monkeypatch.setattr(sync, "ActiveDirectory", directory)
    job = Job.objects.create(kind="associate", status="running", started_at=now-timedelta(seconds=87),
                             plan={"operations": [{"source_id": "private-person"}]})
    Job.objects.create(kind="preview")
    before = list(Job.objects.values())
    response = admin_client.get("/jobs/status", {"id": str(job.pk)})
    state, = response.json()["jobs"]
    assert state["id"] == str(job.pk) and state["active"] is True
    assert state["label"] == "执行中" and state["elapsed"] == "已运行 1 分 27 秒"
    assert "不创建部门 OU" in state["message"]
    assert "private-person" not in response.content.decode()
    assert list(Job.objects.values()) == before
    source.assert_not_called()
    directory.assert_not_called()

    Job.objects.filter(pk=job.pk).update(status="success", message="核验完成", finished_at=now)
    state, = admin_client.get("/jobs/status", {"id": str(job.pk)}).json()["jobs"]
    assert state["active"] is False and state["label"] == "已完成"
    assert state["message"] == "核验完成" and state["elapsed"] == "耗时 1 分 27 秒"
    assert admin_client.get("/jobs/status", {"id": str(uuid.uuid4())}).json() == {"jobs": []}


@pytest.mark.django_db
@pytest.mark.parametrize("status,kind", [("queued", "apply"), ("running", "apply"), ("running", "preview")])
def test_active_pages_show_current_work_instead_of_stale_preview_message(admin_client, status, kind):
    job = Job.objects.create(kind=kind, status=status, message="旧的预览完成消息")
    for path in ("/dashboard", f"/jobs/{job.pk}"):
        html = admin_client.get(path).content.decode()
        assert "旧的预览完成消息" not in html
        assert "每 5 秒自动更新" in html and "task_status.js?v=" in html
        assert "data-job-elapsed" in html
    Job.objects.filter(pk=job.pk).update(status="failed", message="来源读取失败", finished_at=timezone.now())
    for path in ("/dashboard", f"/jobs/{job.pk}"):
        html = admin_client.get(path).content.decode()
        assert "来源读取失败" in html and "task_status.js?v=" not in html
