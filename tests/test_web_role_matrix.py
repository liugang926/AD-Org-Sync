from pathlib import Path

from sync_app.cli.parser import build_parser
from sync_app.core.admin_roles import WEB_ADMIN_ROLES as CORE_ADMIN_ROLES
from sync_app.web.authz import WEB_ADMIN_ROLES, has_capability


def test_enterprise_role_matrix_exposes_complete_role_set() -> None:
    required_roles = {
        "super_admin",
        "connector_admin",
        "ad_admin",
        "mapping_reviewer",
        "sync_executor",
        "operator",
        "auditor",
    }

    assert required_roles.issubset(set(WEB_ADMIN_ROLES))


def test_cli_and_web_roles_share_the_core_role_catalog() -> None:
    parser = build_parser()
    bootstrap_parser = next(
        action.choices["bootstrap-admin"]
        for action in parser._actions
        if getattr(action, "choices", None) and "bootstrap-admin" in action.choices
    )
    role_action = next(
        action for action in bootstrap_parser._actions if action.dest == "role"
    )

    assert WEB_ADMIN_ROLES is CORE_ADMIN_ROLES
    assert tuple(role_action.choices) == CORE_ADMIN_ROLES


def test_web_administrator_form_is_populated_from_shared_role_catalog() -> None:
    routes_source = Path("sync_app/web/routes_admin.py").read_text(encoding="utf-8")
    template_source = Path("sync_app/web/templates/users.html").read_text(encoding="utf-8")

    assert "administrator_roles=WEB_ADMIN_ROLES" in routes_source
    assert "for role in administrator_roles" in template_source
    assert '[("super_admin"' not in template_source


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
