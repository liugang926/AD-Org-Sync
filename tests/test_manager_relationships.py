from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from sync_app.core.models import (
    ManagerRelationshipAction,
    SourceDirectoryUser,
    UserAction,
    UserDepartmentBundle,
)
from sync_app.infra.ldap_compat import MODIFY_REPLACE
from sync_app.services.ad_sync import ADSyncLDAPS
from sync_app.services.runtime_apply_phase import apply_manager_relationship_actions
from sync_app.services.runtime_user_phase import plan_manager_relationship_actions


def _user_action(
    source_user_id: str,
    username: str,
    display_name: str,
) -> UserAction:
    return UserAction(
        connector_id="default",
        operation_type="update_user",
        username=username,
        display_name=display_name,
        email=f"{username}@example.com",
        ou_dn="OU=People,DC=example,DC=com",
        ou_path=["People"],
        target_department_id=1,
        placement_reason="test",
        user=SourceDirectoryUser(
            userid=source_user_id,
            name=display_name,
            manager_account_id="manager-source"
            if source_user_id == "employee-source"
            else "",
        ),
    )


def test_manager_relationship_is_planned_from_stable_binding() -> None:
    employee_action = _user_action("employee-source", "employee", "Employee")
    manager_action = _user_action("manager-source", "manager", "Manager")
    planned_operations: list[dict[str, object]] = []
    conflicts: list[dict[str, object]] = []
    ctx = SimpleNamespace(
        actions=SimpleNamespace(
            user_actions=[employee_action, manager_action],
            manager_relationship_actions=[],
        ),
        identity=SimpleNamespace(
            user_departments={
                "employee-source": UserDepartmentBundle(employee_action.user),
                "manager-source": UserDepartmentBundle(manager_action.user),
            },
            binding_resolution_details={
                "employee-source": {
                    "connector_id": "default",
                    "ad_username": "employee",
                },
                "manager-source": {
                    "connector_id": "default",
                    "ad_username": "manager",
                },
            },
            existing_users_map_by_connector={"default": {}},
        ),
        hooks=SimpleNamespace(
            add_planned_operation=lambda **values: planned_operations.append(values),
            record_conflict=lambda **values: conflicts.append(values),
        ),
    )

    plan_manager_relationship_actions(ctx)

    assert conflicts == []
    assert len(ctx.actions.manager_relationship_actions) == 1
    action = ctx.actions.manager_relationship_actions[0]
    assert action.username == "employee"
    assert action.manager_username == "manager"
    assert action.manager_dn == "CN=Manager,OU=People,DC=example,DC=com"
    assert planned_operations[0]["operation_type"] == "set_manager"


def test_manager_relationship_apply_uses_dedicated_provider_method() -> None:
    operation_records: list[dict[str, object]] = []
    provider = SimpleNamespace(update_manager=Mock(return_value=True))
    ctx = SimpleNamespace(
        actions=SimpleNamespace(
            manager_relationship_actions=[
                ManagerRelationshipAction(
                    connector_id="default",
                    source_user_id="employee-source",
                    username="employee",
                    manager_source_user_id="manager-source",
                    manager_username="manager",
                    manager_dn="CN=Manager,OU=People,DC=example,DC=com",
                )
            ]
        ),
        hooks=SimpleNamespace(
            record_operation=lambda **values: operation_records.append(values)
        ),
        sync_stats={"error_count": 0, "executed_operation_count": 0},
        executed_count=0,
    )

    apply_manager_relationship_actions(ctx, get_ad_sync=lambda _connector: provider)

    provider.update_manager.assert_called_once_with(
        "employee", "CN=Manager,OU=People,DC=example,DC=com"
    )
    assert operation_records[0]["status"] == "succeeded"
    assert ctx.sync_stats["error_count"] == 0
    assert ctx.executed_count == 1


def test_ad_manager_handler_writes_manager_attribute_directly() -> None:
    client = object.__new__(ADSyncLDAPS)
    client.logger = Mock()
    client.connection = SimpleNamespace(
        modify=Mock(return_value=True),
        result={},
    )
    client.get_user = Mock(return_value={"dn": "CN=Employee,OU=People,DC=example,DC=com"})
    client._is_protected_account = Mock(return_value=False)

    assert client.update_manager(
        "employee", "CN=Manager,OU=People,DC=example,DC=com"
    )
    client.connection.modify.assert_called_once_with(
        "CN=Employee,OU=People,DC=example,DC=com",
        {
            "manager": [
                (MODIFY_REPLACE, ["CN=Manager,OU=People,DC=example,DC=com"])
            ]
        },
    )
