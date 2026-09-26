import json
import logging
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path.cwd()))

from sync_app.core.models import (
    AccountConfig,
    AppConfig,
    DirectoryUserRecord,
    LDAPConfig,
    WeComConfig,
)
from sync_app.services import runtime
from sync_app.storage.local_db import (
    DatabaseManager,
    PlannedOperationRepository,
    SyncConflictRepository,
    UserIdentityBindingRepository,
)
from tests.helpers.runtime_fakes import FakeADSyncPolicy, FakeWeComProgrammableAPI

config = AppConfig(
    wecom=WeComConfig(corpid="corp", corpsecret="secret", agentid="1001"),
    ldap=LDAPConfig(server="ldap.example.com", domain="example.com", username="EXAMPLE\\tester", password="unused", use_ssl=True, port=636),
    domain="example.com",
    account=AccountConfig(default_password="unused"),
    config_path="ignored.ini",
)
FakeWeComProgrammableAPI.reset()
FakeWeComProgrammableAPI.department_list = [{"id": 1, "name": "Company", "parentid": 0}, {"id": 2, "name": "Engineering", "parentid": 1}]
FakeWeComProgrammableAPI.department_users = {
    1: [{"userid": "alice", "name": "Alice"}, {"userid": "bob", "name": "Bob"}, {"userid": "charlie", "name": "Charlie Updated"}, {"userid": "eric", "name": "Eric"}],
    2: [{"userid": "dave", "name": "Dave"}],
}
FakeWeComProgrammableAPI.user_details = {
    "alice": {"userid": "alice", "name": "Alice", "email": "alice@example.com", "employee_id": "1001", "department": [1]},
    "bob": {"userid": "bob", "name": "Bob", "email": "bob@example.com", "employee_id": "1002", "department": [1]},
    "charlie": {"userid": "charlie", "name": "Charlie Updated", "email": "charlie@example.com", "employee_id": "1003", "department": [1]},
    "dave": {"userid": "dave", "name": "Dave", "email": "dave@example.com", "employee_id": "1004", "department": [2]},
    "eric": {"userid": "eric", "name": "Eric", "email": "eric@example.com", "employee_id": "1006", "department": [1]},
}
FakeADSyncPolicy.reset()
FakeADSyncPolicy.existing_users_by_domain = {"example.com": {
    "1001": DirectoryUserRecord(username="1001", dn="CN=Alice,OU=Company,DC=example,DC=com", display_name="Alice", email="alice@example.com", raw_entry={"attributes": {"employeeID": "1001"}}),
    "charlie1003": DirectoryUserRecord(username="charlie1003", dn="CN=Charlie,OU=Company,DC=example,DC=com", display_name="Charlie", email="charlie@example.com", raw_entry={"attributes": {"employeeID": "1003"}}),
    "dave1004": DirectoryUserRecord(username="dave1004", dn="CN=Dave,OU=Company,DC=example,DC=com", display_name="Dave", email="dave@example.com", raw_entry={"attributes": {"employeeID": "1004"}}),
    "gone1005": DirectoryUserRecord(username="gone1005", dn="CN=Gone,OU=Company,DC=example,DC=com", display_name="Gone", email="gone@example.com", raw_entry={"attributes": {"employeeID": "1005"}}),
    "1006": DirectoryUserRecord(username="1006", dn="CN=EricOne,OU=Company,DC=example,DC=com", display_name="Eric One", email="eric.one@example.com", raw_entry={"attributes": {"employeeID": "1006"}}),
    "eric1006": DirectoryUserRecord(username="eric1006", dn="CN=EricTwo,OU=Company,DC=example,DC=com", display_name="Eric Two", email="eric.two@example.com", raw_entry={"attributes": {"employeeID": "1006"}}),
}}
FakeADSyncPolicy.enabled_users_by_domain = {"example.com": ["1001", "charlie1003", "dave1004", "gone1005", "1006", "eric1006"]}
with tempfile.TemporaryDirectory() as tmp:
    db_path = str(Path(tmp) / "old.sqlite")
    manager = DatabaseManager(db_path=db_path)
    manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    for uid, username in (("charlie", "charlie1003"), ("dave", "dave1004"), ("gone", "gone1005")):
        UserIdentityBindingRepository(manager).upsert_binding(uid, username, source="manual", notes="synthetic preview fixture", preserve_manual=False)
    with patch.object(runtime, "load_sync_config", return_value=config), \
         patch.object(runtime, "validate_config", return_value=(True, [])), \
         patch.object(runtime, "test_source_connection", return_value=(True, "ok")), \
         patch.object(runtime, "test_ldap_connection", return_value=(True, "ok")), \
         patch.object(runtime, "run_config_security_self_check", return_value=[]), \
         patch("sync_app.providers.source.wecom.WeComAPI", FakeWeComProgrammableAPI), \
         patch.object(runtime, "ADSyncLDAPS", FakeADSyncPolicy), \
         patch.object(runtime.sync_logging, "setup_logging", return_value=logging.getLogger("preview-probe")), \
         patch.object(runtime.sync_logging, "log_filename", str(Path(tmp) / "sync.log")), \
         patch.object(runtime, "_generate_skip_detail_report", return_value=str(Path(tmp) / "skips.csv")):
        result = runtime.run_sync_job(execution_mode="dry_run", trigger_type="unit_test", db_path=db_path, config_path="ignored.ini")
    manager = DatabaseManager(db_path=db_path)
    manager.initialize(create_startup_snapshot=False, verify_integrity=True)
    operations = PlannedOperationRepository(manager).list_operations_for_job(result["job_id"], limit=100)
    conflicts = SyncConflictRepository(manager).list_conflicts_for_job(result["job_id"])
    user_actions = {op["source_id"]: op["operation_type"] for op in operations if op["operation_type"] in {"create_user", "update_user", "move_user", "disable_user"}}
    assert user_actions == {"alice": "update_user", "bob": "create_user", "charlie": "update_user", "dave": "move_user", "eric": "update_user", "gone": "disable_user"}
    assert result["executed_operation_count"] == 0
    assert not FakeADSyncPolicy.created_users and not FakeADSyncPolicy.updated_users and not FakeADSyncPolicy.disabled_users
    bindings = UserIdentityBindingRepository(manager)
    assert all(bindings.get_binding_record_by_source_user_id(uid) is None for uid in ("alice", "bob", "eric"))
    assert all(bindings.get_binding_record_by_source_user_id(uid) is not None for uid in ("charlie", "dave", "gone"))
    print(json.dumps({
        "summary": {k: result.get(k) for k in ("error_count", "conflict_count", "planned_operation_count", "executed_operation_count")},
        "operations": [{"source_id": op["source_id"], "type": op["operation_type"], "target_dn": op["target_dn"], "username": (op.get("desired_state") or {}).get("ad_username")} for op in operations],
        "conflicts": [{"source_id": c.source_id, "type": c.conflict_type} for c in conflicts],
    }, ensure_ascii=False, indent=2))
