from types import SimpleNamespace

from sync_app.services.binding_reconciliation import BindingReconciliationService
from sync_app.services.identity_relationships import IdentityRelationshipPreview
from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories import UserIdentityBindingRepository


SCAN_STARTED_AT = "2026-07-19T01:00:00+00:00"
VERIFIED_AT = "2026-07-19T01:00:01+00:00"


def _relationship(
    source_user_id: str,
    *,
    binding: str = "legacy.user",
    status: str = "missing",
    exists=False,
    protected: bool = False,
    connector_id: str = "default",
    risks=(),
    verified_at: str = VERIFIED_AT,
    checked_account: str | None = None,
    creation_status: str | None = None,
):
    candidate = f"{source_user_id}.candidate"
    checked = binding or candidate if checked_account is None else checked_account
    state = {
        "status": status,
        "exists": exists,
        "enabled": None,
        "locked": None,
        "protected": protected,
        "verified_at": verified_at,
    }
    return IdentityRelationshipPreview(
        org_id="default",
        source_provider="wecom",
        connector_id=connector_id,
        source_user_id=source_user_id,
        source_display_name=source_user_id.title(),
        employee_id=f"E-{source_user_id}",
        source_user={"account_status": "active"},
        mapping_input={"method": "employee_id"},
        candidate_mapping={"ad_username": candidate},
        before_state={
            "bound_ad_username": binding,
            "binding_source": "manual",
            "binding_enabled": bool(binding),
            "binding_revision": 3 if binding else 0,
            "binding_updated_at": "2026-07-19T00:59:00+00:00",
            "connector_id": connector_id,
            "checked_ad_username": checked,
            "ad_account_state": state,
            "verified_at": verified_at,
        },
        planned_after_state={},
        applied_after_state={},
        effective_ad_username=binding,
        effective_resolution_source=(
            "conflict" if connector_id == "__conflict__" else "manual_binding"
        ),
        resolution_reason="test",
        rule_hits=[],
        difference={"status": "test", "changed": False},
        candidate_ad_state={
            "status": "missing",
            "exists": False,
            "protected": False,
            "verified_at": verified_at,
        },
        creation_eligibility={
            "eligible": not binding,
            "status": creation_status
            or ("binding_review_required" if binding else "eligible"),
            "reason": "candidate can be created",
        },
        risks=list(risks),
    )


class _BindingRepo:
    def __init__(self):
        self.records = {}
        self.deleted = []

    def set_record(
        self,
        source_user_id,
        *,
        username="legacy.user",
        connector_id="default",
        revision=3,
    ):
        self.records[source_user_id] = [
            SimpleNamespace(
                connector_id=connector_id,
                ad_username=username,
                binding_revision=revision,
            )
        ]

    def list_binding_records_for_source_identity(
        self, source_user_id, *, org_id, source_provider
    ):
        return list(self.records.get(source_user_id, []))

    def delete_binding_if_target_matches(
        self,
        source_user_id,
        ad_username,
        *,
        org_id,
        source_provider,
        connector_id,
        binding_revision,
    ):
        records = self.records.get(source_user_id, [])
        if (
            len(records) != 1
            or records[0].connector_id != connector_id
            or records[0].ad_username.casefold() != ad_username.casefold()
            or records[0].binding_revision != binding_revision
        ):
            return False
        self.deleted.append(
            (
                org_id,
                source_provider,
                connector_id,
                source_user_id,
                ad_username,
                binding_revision,
            )
        )
        self.records[source_user_id] = []
        return True


class _AuditRepo:
    def __init__(self):
        self.logs = []

    def add_log(self, **payload):
        self.logs.append(payload)


def _scan(service, relationships):
    return service.scan(
        relationships,
        organization_id="default",
        organization_name="Default Organization",
        environment_label="Staging",
        source_provider="wecom",
        snapshot_id=42,
        snapshot_fingerprint="snapshot-42",
        scan_started_at=SCAN_STARTED_AT,
        scanned_at="2026-07-19T01:00:02+00:00",
    )


def test_scan_classifies_every_fail_closed_result_without_repository_writes():
    repo = _BindingRepo()
    audit = _AuditRepo()
    service = BindingReconciliationService(
        user_binding_repo=repo,
        audit_repo=audit,
    )
    report = _scan(
        service,
        [
            _relationship("safe"),
            _relationship("exists", status="exists", exists=True),
            _relationship("down", status="unavailable", exists=None),
            _relationship("unknown", status="unknown", exists=None),
            _relationship(
                "protected",
                status="protected",
                exists=None,
                protected=True,
            ),
            _relationship(
                "ambiguous",
                connector_id="__conflict__",
                risks=("connector_conflict",),
            ),
            _relationship("multiple", risks=("multiple_bindings",)),
            _relationship("unbound", binding=""),
        ],
    )

    categories = {
        item.source_user_id: item.category for item in report.items
    }
    assert categories == {
        "safe": "safe_cleanup",
        "exists": "ad_exists",
        "down": "directory_unavailable",
        "unknown": "verification_unknown",
        "protected": "protected_account",
        "ambiguous": "connector_ambiguous",
        "multiple": "manual_review",
        "unbound": "manual_review",
    }
    assert report.cleanup_count == 1
    assert report.skip_count == 7
    assert report.category_counts["binding_changed"] == 0
    assert audit.logs == []
    assert repo.deleted == []


def test_read_only_concurrency_refresh_disables_changed_target():
    repo = _BindingRepo()
    repo.set_record("safe", username="changed.user", revision=4)
    service = BindingReconciliationService(user_binding_repo=repo)
    report = _scan(service, [_relationship("safe")])

    refreshed = service.refresh_concurrency(report)

    assert report.items[0].category == "safe_cleanup"
    assert refreshed.items[0].category == "binding_changed"
    assert refreshed.items[0].current_saved_binding == "changed.user"
    assert refreshed.cleanup_count == 0
    assert repo.deleted == []


def test_execution_reverifies_each_item_and_records_per_item_audit():
    repo = _BindingRepo()
    repo.set_record("safe")
    audit = _AuditRepo()
    service = BindingReconciliationService(
        user_binding_repo=repo,
        audit_repo=audit,
    )
    report = _scan(
        service,
        [
            _relationship("safe"),
            _relationship("exists", status="exists", exists=True),
        ],
    )
    current = _scan(
        service,
        [
            _relationship("safe"),
            _relationship("exists", status="exists", exists=True),
        ],
    )

    executed = service.execute(
        report,
        current,
        actor_username="admin",
        audit_context={"scan_id": report.scan_id},
    )

    assert executed.execution_deleted_count == 1
    assert executed.execution_skipped_count == 1
    assert len(repo.deleted) == 1
    safe = next(item for item in executed.items if item.source_user_id == "safe")
    assert safe.current_saved_binding == ""
    assert safe.can_create is True
    assert len(audit.logs) == 2
    assert {item["result"] for item in audit.logs} == {"success", "skipped"}


def test_cleanup_only_removes_binding_related_candidate_creation_blocker():
    repo = _BindingRepo()
    repo.set_record("inactive")
    service = BindingReconciliationService(user_binding_repo=repo)
    report = _scan(
        service,
        [_relationship("inactive", creation_status="source_inactive")],
    )
    current = _scan(
        service,
        [_relationship("inactive", creation_status="source_inactive")],
    )

    executed = service.execute(
        report,
        current,
        actor_username="admin",
        audit_context={},
    )

    assert executed.execution_deleted_count == 1
    assert executed.items[0].can_create is False


def test_execution_fails_closed_when_latest_live_result_is_unavailable():
    repo = _BindingRepo()
    repo.set_record("safe")
    audit = _AuditRepo()
    service = BindingReconciliationService(
        user_binding_repo=repo,
        audit_repo=audit,
    )
    report = _scan(service, [_relationship("safe")])
    current = _scan(
        service,
        [_relationship("safe", status="unavailable", exists=None)],
    )

    executed = service.execute(
        report,
        current,
        actor_username="admin",
        audit_context={},
    )

    assert executed.execution_deleted_count == 0
    assert executed.execution_skipped_count == 1
    assert repo.deleted == []
    assert audit.logs[0]["result"] == "blocked"
    assert (
        audit.logs[0]["payload"]["reason_code"]
        == "binding_reconciliation.reason.directory_unavailable"
    )


def test_repository_compare_and_delete_requires_exact_binding_revision(tmp_path):
    manager = DatabaseManager(db_path=str(tmp_path / "bindings.db"))
    manager.initialize()
    repo = UserIdentityBindingRepository(manager)
    repo.upsert_binding(
        "alice",
        "alice.old",
        org_id="default",
        source_provider="wecom",
        connector_id="default",
        source="managed_generated",
    )
    scanned = repo.get_binding_record_by_source_user_id(
        "alice",
        org_id="default",
        source_provider="wecom",
        connector_id="default",
    )
    repo.upsert_binding(
        "alice",
        "alice.old",
        org_id="default",
        source_provider="wecom",
        connector_id="default",
        source="managed_generated",
        preserve_manual=False,
    )
    current = repo.get_binding_record_by_source_user_id(
        "alice",
        org_id="default",
        source_provider="wecom",
        connector_id="default",
    )

    assert current.binding_revision == scanned.binding_revision + 1
    assert (
        repo.delete_binding_if_target_matches(
            "alice",
            "alice.old",
            org_id="default",
            source_provider="wecom",
            connector_id="default",
            binding_revision=scanned.binding_revision,
        )
        is False
    )
    assert (
        repo.delete_binding_if_target_matches(
            "alice",
            "alice.old",
            org_id="default",
            source_provider="wecom",
            connector_id="default",
            binding_revision=current.binding_revision,
        )
        is True
    )
