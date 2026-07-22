from sync_app.web.authz import WEB_ADMIN_ROLES, has_capability


def test_enterprise_role_matrix_exposes_six_separated_business_roles() -> None:
    required_roles = {
        "super_admin",
        "connector_admin",
        "ad_admin",
        "mapping_reviewer",
        "sync_executor",
        "auditor",
    }

    assert required_roles.issubset(set(WEB_ADMIN_ROLES))


def test_mapping_reviewer_can_approve_but_cannot_execute() -> None:
    assert has_capability("mapping_reviewer", "mappings.write")
    assert has_capability("mapping_reviewer", "jobs.review")
    assert not has_capability("mapping_reviewer", "jobs.run")
    assert not has_capability("mapping_reviewer", "config.write")


def test_sync_executor_can_execute_but_cannot_approve_or_edit_mapping() -> None:
    assert has_capability("sync_executor", "jobs.run")
    assert not has_capability("sync_executor", "jobs.review")
    assert not has_capability("sync_executor", "mappings.write")


def test_connector_and_ad_administrators_have_distinct_sensitive_capabilities() -> None:
    assert has_capability("connector_admin", "connectors.write")
    assert not has_capability("connector_admin", "ad.write")
    assert has_capability("ad_admin", "ad.write")
    assert not has_capability("ad_admin", "connectors.write")


def test_auditor_remains_strictly_read_only() -> None:
    for capability in (
        "config.write",
        "connectors.write",
        "ad.write",
        "mappings.write",
        "jobs.review",
        "jobs.run",
    ):
        assert not has_capability("auditor", capability)
