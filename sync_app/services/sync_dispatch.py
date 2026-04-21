from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

from sync_app.core.common import generate_job_id
from sync_app.core.models import SyncJobRecord
from sync_app.services.notification_automation_center import evaluate_scheduled_apply_readiness
from sync_app.storage.local_db import DatabaseManager, SyncJobRepository
from sync_app.storage.repositories.conflicts import SyncConflictRepository, SyncPlanReviewRepository
from sync_app.storage.repositories.system import SettingsRepository

LOGGER = logging.getLogger(__name__)

DEFAULT_JOB_LEASE_SECONDS = 300
DEFAULT_JOB_HEARTBEAT_SECONDS = 30


@dataclass(frozen=True)
class SyncDispatchResult:
    accepted: bool
    job: Optional[SyncJobRecord]
    message: str


def _open_job_repo(db_path: Optional[str]) -> tuple[DatabaseManager, SyncJobRepository]:
    db_manager = DatabaseManager(db_path=db_path)
    db_manager.initialize()
    return db_manager, SyncJobRepository(db_manager)


def _build_worker_id(prefix: str) -> str:
    return f"{prefix}-{os.getpid()}-{threading.get_ident()}"


def _should_guard_scheduled_apply(*, execution_mode: str, trigger_type: str) -> bool:
    return (
        str(execution_mode or "").strip().lower() == "apply"
        and str(trigger_type or "").strip().lower() == "schedule"
    )


def _build_scheduled_apply_block_message(readiness: dict[str, Any]) -> str:
    reasons = [str(item or "").strip() for item in list(readiness.get("reasons") or []) if str(item or "").strip()]
    if reasons:
        return "Scheduled apply blocked: " + " ".join(reasons)
    return str(readiness.get("summary") or "Scheduled apply blocked by automation policy.")


class _LeaseHeartbeat:
    def __init__(
        self,
        *,
        db_path: Optional[str],
        job_id: str,
        worker_id: str,
        lease_seconds: int,
        heartbeat_seconds: int,
    ):
        self.db_path = db_path
        self.job_id = str(job_id or "").strip()
        self.worker_id = str(worker_id or "").strip()
        self.lease_seconds = max(int(lease_seconds or 0), 1)
        self.heartbeat_seconds = max(int(heartbeat_seconds or 0), 1)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"lease-heartbeat-{self.job_id}",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=max(self.heartbeat_seconds, 1))

    def _run(self) -> None:
        while not self._stop_event.wait(self.heartbeat_seconds):
            try:
                _db_manager, job_repo = _open_job_repo(self.db_path)
                renewed = job_repo.renew_lease(
                    self.job_id,
                    worker_id=self.worker_id,
                    lease_seconds=self.lease_seconds,
                )
                if not renewed:
                    LOGGER.warning("failed to renew sync job lease for %s", self.job_id)
                    return
            except Exception:
                LOGGER.exception("unexpected error while renewing sync job lease")
                return


def enqueue_sync_job(
    *,
    db_path: Optional[str],
    execution_mode: str,
    trigger_type: str,
    org_id: str,
    config_path: str,
    requested_by: str = "",
) -> SyncDispatchResult:
    _db_manager, job_repo = _open_job_repo(db_path)
    normalized_org_id = str(org_id or "").strip().lower() or "default"
    normalized_execution_mode = str(execution_mode or "").strip().lower() or "apply"
    normalized_trigger_type = str(trigger_type or "").strip().lower() or "manual"
    existing_job = job_repo.get_active_job_record(org_id=normalized_org_id)
    if existing_job:
        return SyncDispatchResult(
            accepted=False,
            job=existing_job,
            message=f"Synchronization job {existing_job.job_id} is already queued or running",
        )

    if _should_guard_scheduled_apply(
        execution_mode=normalized_execution_mode,
        trigger_type=normalized_trigger_type,
    ):
        readiness = evaluate_scheduled_apply_readiness(
            settings_repo=SettingsRepository(_db_manager),
            job_repo=job_repo,
            conflict_repo=SyncConflictRepository(_db_manager),
            review_repo=SyncPlanReviewRepository(_db_manager),
            org_id=normalized_org_id,
        )
        if not readiness.get("allowed"):
            return SyncDispatchResult(
                accepted=False,
                job=None,
                message=_build_scheduled_apply_block_message(readiness),
            )

    job_id = generate_job_id()
    job_repo.create_job(
        job_id=job_id,
        org_id=normalized_org_id,
        trigger_type=normalized_trigger_type,
        execution_mode=normalized_execution_mode,
        status="QUEUED",
        requested_by=str(requested_by or "").strip(),
        requested_config_path=str(config_path or "").strip(),
    )
    job_record = job_repo.get_job_record(job_id)
    return SyncDispatchResult(
        accepted=bool(job_record),
        job=job_record,
        message=f"Synchronization job queued as {job_id}",
    )


def claim_next_sync_job(
    *,
    db_path: Optional[str],
    worker_id: str,
    lease_seconds: int = DEFAULT_JOB_LEASE_SECONDS,
) -> Optional[SyncJobRecord]:
    _db_manager, job_repo = _open_job_repo(db_path)
    return job_repo.claim_next_queued_job(
        worker_id=str(worker_id or "").strip(),
        lease_seconds=lease_seconds,
    )


def run_claimed_sync_job(
    *,
    db_path: Optional[str],
    job_id: str,
    worker_id: str,
    stats_callback=None,
    cancel_flag=None,
    lease_seconds: int = DEFAULT_JOB_LEASE_SECONDS,
    heartbeat_seconds: int = DEFAULT_JOB_HEARTBEAT_SECONDS,
) -> dict[str, Any]:
    _db_manager, job_repo = _open_job_repo(db_path)
    job_record = job_repo.get_job_record(job_id)
    if not job_record:
        raise ValueError(f"sync job not found: {job_id}")

    heartbeat = _LeaseHeartbeat(
        db_path=db_path,
        job_id=job_record.job_id,
        worker_id=worker_id,
        lease_seconds=lease_seconds,
        heartbeat_seconds=heartbeat_seconds,
    )
    heartbeat.start()
    try:
        from sync_app.services.runtime import run_sync_job

        return run_sync_job(
            stats_callback=stats_callback,
            cancel_flag=cancel_flag,
            execution_mode=job_record.execution_mode or "apply",
            trigger_type=job_record.trigger_type or "manual",
            db_path=db_path,
            config_path=job_record.requested_config_path or "config.ini",
            org_id=job_record.org_id or "default",
            job_id=job_record.job_id,
            active_job_guard_id=job_record.job_id,
            requested_by=job_record.requested_by,
        )
    except Exception as exc:
        _db_manager, job_repo = _open_job_repo(db_path)
        current_record = job_repo.get_job_record(job_id)
        if current_record and str(current_record.ended_at or "").strip() == "":
            job_repo.update_job(
                job_id,
                status="FAILED",
                ended=True,
                summary={
                    "mode": job_record.execution_mode or "apply",
                    "error": str(exc),
                },
                clear_lease=True,
            )
        raise
    finally:
        heartbeat.stop()


def run_sync_request(
    *,
    execution_mode: str,
    trigger_type: str,
    db_path: Optional[str],
    config_path: str,
    org_id: str,
    requested_by: str = "",
    stats_callback=None,
    cancel_flag=None,
) -> dict[str, Any]:
    _db_manager, job_repo = _open_job_repo(db_path)
    blocking_job = job_repo.get_active_job_record()
    if blocking_job:
        raise RuntimeError(f"active sync job already exists: {blocking_job.job_id}")

    enqueue_result = enqueue_sync_job(
        db_path=db_path,
        execution_mode=execution_mode,
        trigger_type=trigger_type,
        org_id=org_id,
        config_path=config_path,
        requested_by=requested_by,
    )
    if not enqueue_result.accepted or not enqueue_result.job:
        raise RuntimeError(enqueue_result.message or "failed to enqueue sync job")

    worker_id = _build_worker_id(f"{trigger_type or 'sync'}-inline")
    _db_manager, job_repo = _open_job_repo(db_path)
    claimed = job_repo.claim_job(
        enqueue_result.job.job_id,
        worker_id=worker_id,
        lease_seconds=DEFAULT_JOB_LEASE_SECONDS,
    )
    if not claimed:
        raise RuntimeError(f"failed to claim queued sync job: {enqueue_result.job.job_id}")

    return run_claimed_sync_job(
        db_path=db_path,
        job_id=claimed.job_id,
        worker_id=worker_id,
        stats_callback=stats_callback,
        cancel_flag=cancel_flag,
    )
