import ast
import unittest
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

from sync_app.web import create_app
from sync_app.web.routes_config import (
    CONFIG_SUBMISSION_FIELD_NAMES,
    _collect_config_submission_values,
)


class StructureGuardTests(unittest.TestCase):
    def test_ad_sync_class_does_not_define_duplicate_methods(self):
        module_path = Path("sync_app/services/ad_sync.py")
        module = ast.parse(module_path.read_text(encoding="utf-8"))
        class_node = next(
            node for node in module.body if isinstance(node, ast.ClassDef) and node.name == "ADSyncLDAPS"
        )

        method_names = [
            node.name
            for node in class_node.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        duplicate_names = {
            method_name
            for method_name, count in Counter(method_names).items()
            if count > 1
        }

        self.assertEqual(duplicate_names, set())

    def test_config_submission_value_collection_filters_unknown_fields(self):
        payload = {
            "csrf_token": "token",
            "source_provider": "wecom",
            "ldap_server": "dc01.example.local",
            "unknown": "ignored",
        }

        values = _collect_config_submission_values(payload)

        self.assertEqual(
            values,
            {
                "source_provider": "wecom",
                "ldap_server": "dc01.example.local",
            },
        )
        self.assertTrue(set(values).issubset(set(CONFIG_SUBMISSION_FIELD_NAMES)))

    def test_create_app_binds_web_app_state_container_without_breaking_legacy_aliases(self):
        with TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            app = create_app(
                db_path=str(temp_root / "structure.db"),
                config_path=str(temp_root / "structure.ini"),
            )

        self.assertTrue(hasattr(app.state, "web_app_state"))
        self.assertIs(app.state.web_app_state.repositories.settings_repo, app.state.settings_repo)
        self.assertIs(app.state.web_app_state.repositories.organization_repo, app.state.organization_repo)
        self.assertIs(app.state.web_app_state.runtime.login_rate_limiter, app.state.login_rate_limiter)
        self.assertIs(app.state.web_app_state.runtime.sync_runner, app.state.sync_runner)

    def test_desktop_runtime_threads_live_in_dedicated_module(self):
        desktop_module = ast.parse(Path("sync_app/ui/desktop.py").read_text(encoding="utf-8"))
        runtime_module = ast.parse(Path("sync_app/ui/runtime_threads.py").read_text(encoding="utf-8"))

        desktop_classes = {
            node.name for node in desktop_module.body if isinstance(node, ast.ClassDef)
        }
        runtime_classes = {
            node.name for node in runtime_module.body if isinstance(node, ast.ClassDef)
        }

        self.assertTrue({"SyncThread", "ScheduleThread"}.isdisjoint(desktop_classes))
        self.assertTrue({"SyncThread", "ScheduleThread"}.issubset(runtime_classes))

    def test_desktop_config_and_storage_services_live_in_dedicated_module(self):
        desktop_module = ast.parse(Path("sync_app/ui/desktop.py").read_text(encoding="utf-8"))
        services_module = ast.parse(Path("sync_app/ui/desktop_services.py").read_text(encoding="utf-8"))

        desktop_function_names = {
            node.name for node in desktop_module.body if isinstance(node, ast.FunctionDef)
        }
        services_classes = {
            node.name for node in services_module.body if isinstance(node, ast.ClassDef)
        }

        self.assertTrue({"_get_config_value", "_ensure_config_sections"}.isdisjoint(desktop_function_names))
        self.assertTrue({"DesktopConfigService", "DesktopLocalStrategyService"}.issubset(services_classes))

    def test_shared_sync_dispatch_entrypoint_lives_in_dedicated_service_module(self):
        entry_module = ast.parse(Path("sync_app/services/entry.py").read_text(encoding="utf-8"))
        dispatch_module = ast.parse(Path("sync_app/services/sync_dispatch.py").read_text(encoding="utf-8"))

        imported_from_entry = {
            alias.name
            for node in ast.walk(entry_module)
            if isinstance(node, ast.ImportFrom) and node.module == "sync_app.services.sync_dispatch"
            for alias in node.names
        }
        dispatch_functions = {
            node.name for node in dispatch_module.body if isinstance(node, ast.FunctionDef)
        }

        self.assertIn("run_sync_request", dispatch_functions)
        self.assertIn("run_sync_request", imported_from_entry)


if __name__ == "__main__":
    unittest.main()
