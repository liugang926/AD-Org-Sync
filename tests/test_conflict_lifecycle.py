from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from sync_app.storage.local_db import DatabaseManager, SyncConflictRepository, SyncJobRepository


def _create_job(
    job_repo: SyncJobRepository,
    job_id: str,
    *,
    mode: str = "dry_run",
    ended_at: str | None = None,
    plan_source_job_id: str = "",
) -> None:
    job_repo.create_job(
        job_id,
        trigger_type="unit_test",
        execution_mode=mode,
        status="COMPLETED",
        org_id="default",
        plan_source_job_id=plan_source_job_id or None,
    )
    if ended_at:
        with job_repo.db.transaction() as conn:
            conn.execute(
                "UPDATE sync_jobs SET ended_at = ? WHERE job_id = ?",
                (ended_at, job_id),
            )


@pytest.fixture
def lifecycle_db_path(request):
    test_root = Path.cwd() / "test_artifacts"
    test_root.mkdir(exist_ok=True)
    db_path = test_root / f"{request.node.name}.db"
    for suffix in ("", "-wal", "-shm"):
        Path(str(db_path) + suffix).unlink(missing_ok=True)
    yield db_path
    for suffix in ("", "-wal", "-shm"):
        try:
            Path(str(db_path) + suffix).unlink(missing_ok=True)
        except PermissionError:
            pass


def test_conflicts_are_isolated_by_plan_and_workflow(lifecycle_db_path: Path) -> None:
    manager = DatabaseManager(db_path=str(lifecycle_db_path))
    manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    jobs = SyncJobRepository(manager)
    conflicts = SyncConflictRepository(manager)
    _create_job(jobs, "plan-current")
    _create_job(
        jobs,
        "apply-current",
        mode="apply",
        plan_source_job_id="plan-current",
    )
    _create_job(jobs, "plan-history")

    current_id = conflicts.add_conflict(
        job_id="apply-current",
        plan_id="plan-current",
        workflow_id="apply-current",
        conflict_type="identity_ambiguous",
        source_id="alice",
        message="current plan conflict",
    )
    conflicts.add_conflict(
        job_id="plan-history",
        plan_id="plan-history",
        workflow_id="plan-history",
        conflict_type="identity_ambiguous",
        source_id="bob",
        message="historical conflict",
    )

    current = conflicts.list_conflict_records(plan_id="plan-current")
    assert [item.id for item in current] == [current_id]
    assert current[0].workflow_id == "apply-current"
    assert conflicts.count_unresolved_conflicts_for_plan("plan-current") == 1
    assert conflicts.count_unresolved_conflicts_for_plan("plan-history") == 1
    assert conflicts.count_unresolved_conflicts_for_plan("missing-plan") == 0


def test_every_conflict_status_transition_has_audit_history(lifecycle_db_path: Path) -> None:
    manager = DatabaseManager(db_path=str(lifecycle_db_path))
    manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    jobs = SyncJobRepository(manager)
    conflicts = SyncConflictRepository(manager)
    _create_job(jobs, "plan-audit")
    conflict_id = conflicts.add_conflict(
        job_id="plan-audit",
        conflict_type="identity_ambiguous",
        source_id="alice",
        message="audit every transition",
    )

    assert conflicts.update_conflict_status(
        conflict_id,
        status="ignored",
        actor_username="reviewer",
        reason="approved business exception",
    )
    assert conflicts.update_conflict_status(
        conflict_id,
        status="archived",
        actor_username="reviewer",
        reason="expired plan",
    )
    assert conflicts.update_conflict_status(
        conflict_id,
        status="open",
        actor_username="reviewer",
        reason="new evidence",
    )
    assert conflicts.update_conflict_status(
        conflict_id,
        status="resolved",
        actor_username="reviewer",
        reason="identity confirmed",
    )

    history = conflicts.list_status_history(conflict_id)
    assert [(item["from_status"], item["to_status"]) for item in history] == [
        ("open", "ignored"),
        ("ignored", "archived"),
        ("archived", "open"),
        ("open", "resolved"),
    ]
    assert {item["actor_username"] for item in history} == {"reviewer"}
    with manager.connection() as conn:
        audit_count = conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM web_audit_logs
            WHERE action_type = 'conflict.status_transition'
              AND target_id = ?
            """,
            (str(conflict_id),),
        ).fetchone()["total"]
    assert audit_count == 4


def test_retention_archives_expired_plan_evidence_before_deletion(lifecycle_db_path: Path) -> None:
    manager = DatabaseManager(db_path=str(lifecycle_db_path))
    manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    jobs = SyncJobRepository(manager)
    conflicts = SyncConflictRepository(manager)
    ended_45_days_ago = (
        datetime.now(timezone.utc) - timedelta(days=45)
    ).isoformat(timespec="seconds")
    _create_job(jobs, "plan-expired", ended_at=ended_45_days_ago)
    conflict_id = conflicts.add_conflict(
        job_id="plan-expired",
        conflict_type="identity_ambiguous",
        source_id="alice",
        message="retain historical evidence",
    )

    first_cleanup = manager.cleanup_history(
        job_retention_days=30,
        conflict_archive_after_days=30,
        conflict_retention_days=90,
        event_retention_days=30,
        audit_log_retention_days=90,
    )
    assert first_cleanup["archived_conflicts"] == 1
    assert first_cleanup["deleted_conflicts"] == 0
    assert first_cleanup["deleted_jobs"] == 0
    assert conflicts.get_conflict_record(conflict_id).status == "archived"
    assert conflicts.list_status_history(conflict_id)[0]["actor_username"] == "retention-policy"
    with manager.connection() as conn:
        retention_audit = conn.execute(
            """
            SELECT action_type, actor_username
            FROM web_audit_logs
            WHERE target_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(conflict_id),),
        ).fetchone()
    assert dict(retention_audit) == {
        "action_type": "conflict.status_transition",
        "actor_username": "retention-policy",
    }

    ended_100_days_ago = (
        datetime.now(timezone.utc) - timedelta(days=100)
    ).isoformat(timespec="seconds")
    with manager.transaction() as conn:
        conn.execute(
            "UPDATE sync_jobs SET ended_at = ? WHERE job_id = 'plan-expired'",
            (ended_100_days_ago,),
        )
    second_cleanup = manager.cleanup_history(
        job_retention_days=30,
        conflict_archive_after_days=30,
        conflict_retention_days=90,
        event_retention_days=30,
        audit_log_retention_days=90,
    )
    assert second_cleanup["deleted_conflicts"] == 1
    assert second_cleanup["deleted_jobs"] == 1
    assert conflicts.get_conflict_record(conflict_id) is None
