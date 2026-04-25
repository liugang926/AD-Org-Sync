import ast
import unittest
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

from sync_app.web import create_app
from sync_app.web.app_state import (
    get_web_app_state,
    get_web_repositories,
    get_web_runtime_state,
    get_web_services,
)
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
        self.assertIs(get_web_app_state(app), app.state.web_app_state)
        self.assertIs(get_web_repositories(app), app.state.web_app_state.repositories)
        self.assertIs(get_web_runtime_state(app), app.state.web_app_state.runtime)
        self.assertIs(get_web_services(app), app.state.web_app_state.services)

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

    def test_services_layer_does_not_import_web_layer(self):
        offenders = []
        for module_path in Path("sync_app/services").glob("*.py"):
            module = ast.parse(module_path.read_text(encoding="utf-8"))
            for node in ast.walk(module):
                if isinstance(node, ast.ImportFrom):
                    imported_module = node.module or ""
                    if imported_module == "sync_app.web" or imported_module.startswith("sync_app.web."):
                        offenders.append(f"{module_path}:{node.lineno} imports {imported_module}")
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        imported_module = alias.name
                        if imported_module == "sync_app.web" or imported_module.startswith("sync_app.web."):
                            offenders.append(f"{module_path}:{node.lineno} imports {imported_module}")

        self.assertEqual(offenders, [])

    def test_core_layer_does_not_import_provider_layer(self):
        offenders = []
        for module_path in Path("sync_app/core").glob("*.py"):
            module = ast.parse(module_path.read_text(encoding="utf-8"))
            for node in ast.walk(module):
                if isinstance(node, ast.ImportFrom):
                    imported_module = node.module or ""
                    if imported_module == "sync_app.providers" or imported_module.startswith("sync_app.providers."):
                        offenders.append(f"{module_path}:{node.lineno} imports {imported_module}")
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        imported_module = alias.name
                        if imported_module == "sync_app.providers" or imported_module.startswith("sync_app.providers."):
                            offenders.append(f"{module_path}:{node.lineno} imports {imported_module}")

        self.assertEqual(offenders, [])

    def test_route_hotspots_use_web_app_state_accessors_for_repo_lookup(self):
        hotspot_expectations = {
            Path("sync_app/web/routes_jobs.py"): ("get_web_repositories", "get_web_services"),
            Path("sync_app/web/routes_conflicts.py"): ("get_web_services",),
            Path("sync_app/web/routes_organizations.py"): ("get_web_repositories",),
            Path("sync_app/web/routes_mappings.py"): ("get_web_repositories",),
            Path("sync_app/web/routes_exceptions.py"): ("get_web_repositories",),
            Path("sync_app/web/routes_advanced_sync.py"): ("get_web_repositories",),
        }

        for module_path, required_accessors in hotspot_expectations.items():
            source = module_path.read_text(encoding="utf-8")
            for accessor in required_accessors:
                self.assertIn(accessor, source, msg=f"{module_path} should use {accessor}")
            self.assertNotIn(
                "request.app.state.job_repo",
                source,
                msg=f"{module_path} should not reach into app.state.job_repo directly",
            )
            self.assertNotIn(
                "request.app.state.conflict_repo",
                source,
                msg=f"{module_path} should not reach into app.state.conflict_repo directly",
            )
            self.assertNotIn(
                "request.app.state.organization_repo",
                source,
                msg=f"{module_path} should not reach into app.state.organization_repo directly",
            )

    def test_job_and_conflict_routes_use_web_service_accessors_for_domain_workflows(self):
        route_expectations = {
            Path("sync_app/web/routes_jobs.py"): {
                "required": ("get_web_services",),
                "forbidden": (
                    "def build_job_comparison_sections(",
                    "def build_job_center_summary(",
                    "approve_job_review_action(",
                    'action_type="job.review_approve"',
                ),
            },
            Path("sync_app/web/routes_conflicts.py"): {
                "required": ("get_web_services",),
                "forbidden": (
                    "recommend_conflict_resolution(",
                    "recommendation_requires_confirmation(",
                    "apply_conflict_manual_binding(",
                    "apply_conflict_skip_user_sync(",
                    "apply_conflict_recommendation(",
                    'action_type="conflict.resolve_manual_binding"',
                    'action_type="conflict.resolve_skip_user"',
                    'action_type="conflict.apply_recommendation"',
                    'action_type="conflict.bulk_action"',
                ),
            },
        }

        for module_path, expectations in route_expectations.items():
            source = module_path.read_text(encoding="utf-8")
            for required in expectations["required"]:
                self.assertIn(required, source, msg=f"{module_path} should use {required}")
            for forbidden in expectations["forbidden"]:
                self.assertNotIn(
                    forbidden,
                    source,
                    msg=f"{module_path} should move {forbidden} behind a service facade",
                )

    def test_web_service_facades_live_in_dedicated_modules(self):
        compatibility_module = ast.parse(Path("sync_app/web/service_facades.py").read_text(encoding="utf-8"))
        compatibility_classes = {
            node.name for node in compatibility_module.body if isinstance(node, ast.ClassDef)
        }
        self.assertEqual(compatibility_classes, set())

        module_expectations = {
            Path("sync_app/web/services/jobs.py"): "WebJobService",
            Path("sync_app/web/services/conflicts.py"): "WebConflictService",
            Path("sync_app/web/services/config.py"): "WebConfigService",
            Path("sync_app/web/services/integrations.py"): "WebIntegrationService",
            Path("sync_app/web/services/state.py"): "WebServiceState",
        }
        for module_path, expected_class in module_expectations.items():
            module = ast.parse(module_path.read_text(encoding="utf-8"))
            classes = {
                node.name for node in module.body if isinstance(node, ast.ClassDef)
            }
            self.assertIn(expected_class, classes, msg=f"{module_path} should define {expected_class}")

    def test_storage_schema_lives_in_dedicated_modules(self):
        self.assertFalse(Path("sync_app/storage/schema.py").exists())

        schema_dir = Path("sync_app/storage/schema")
        module_expectations = {
            schema_dir / "defaults.py": {"DEFAULT_APP_SETTINGS", "ORG_SCOPED_APP_SETTINGS"},
            schema_dir / "protected_groups.py": {
                "DEFAULT_HARD_PROTECTED_GROUPS",
                "DEFAULT_SOFT_EXCLUDED_GROUPS",
            },
            schema_dir / "migrations.py": {"MIGRATIONS"},
        }

        for module_path, expected_names in module_expectations.items():
            module = ast.parse(module_path.read_text(encoding="utf-8"))
            assigned_names = {
                target.id
                for node in module.body
                if isinstance(node, ast.Assign)
                for target in node.targets
                if isinstance(target, ast.Name)
            }
            self.assertTrue(
                expected_names.issubset(assigned_names),
                msg=f"{module_path} should define {sorted(expected_names)}",
            )

    def test_cli_commands_live_in_dedicated_package_modules(self):
        self.assertFalse(Path("sync_app/cli.py").exists())

        module_expectations = {
            Path("sync_app/cli/parser.py"): {"build_parser"},
            Path("sync_app/cli/main.py"): {"main", "windows_selector_loop_factory"},
            Path("sync_app/cli/handlers/config.py"): {
                "_handle_config_export",
                "_handle_config_import",
                "_handle_validate_config",
            },
            Path("sync_app/cli/handlers/conflicts.py"): {
                "_handle_conflicts_list",
                "_handle_conflicts_resolve_binding",
                "_handle_conflicts_bulk",
            },
            Path("sync_app/cli/handlers/database.py"): {"_handle_db_backup", "_handle_db_check"},
            Path("sync_app/cli/handlers/sync.py"): {"_handle_sync", "_handle_approve_plan"},
            Path("sync_app/cli/handlers/web.py"): {
                "_handle_bootstrap_admin",
                "_handle_init_web",
                "_handle_web",
            },
        }

        for module_path, expected_functions in module_expectations.items():
            module = ast.parse(module_path.read_text(encoding="utf-8"))
            functions = {
                node.name for node in module.body if isinstance(node, ast.FunctionDef)
            }
            self.assertTrue(
                expected_functions.issubset(functions),
                msg=f"{module_path} should define {sorted(expected_functions)}",
            )

    def test_core_models_live_in_dedicated_package_modules(self):
        self.assertFalse(Path("sync_app/core/models.py").exists())

        module_expectations = {
            Path("sync_app/core/models/config.py"): {"AppConfig", "LDAPConfig", "SourceConnectorConfig"},
            Path("sync_app/core/models/directory.py"): {
                "DepartmentNode",
                "DirectoryUserRecord",
                "SourceDirectoryUser",
            },
            Path("sync_app/core/models/sync_job.py"): {
                "SyncJobRecord",
                "SyncJobSummary",
                "SyncRunStats",
            },
            Path("sync_app/core/models/actions.py"): {
                "DepartmentAction",
                "ManagedGroupTarget",
                "UserAction",
            },
            Path("sync_app/core/models/conflicts.py"): {
                "SyncConflictRecord",
                "SyncExceptionRuleRecord",
                "UserIdentityBindingRecord",
            },
            Path("sync_app/core/models/config_records.py"): {
                "AttributeMappingRuleRecord",
                "OrganizationRecord",
            },
            Path("sync_app/core/models/integrations.py"): {
                "IntegrationWebhookOutboxRecord",
                "IntegrationWebhookSubscriptionRecord",
                "SyncConnectorRecord",
            },
            Path("sync_app/core/models/lifecycle.py"): {
                "OffboardingRecord",
                "UserLifecycleRecord",
            },
            Path("sync_app/core/models/web_admin.py"): {
                "WebAdminUserRecord",
                "WebAuditLogRecord",
            },
        }

        for module_path, expected_classes in module_expectations.items():
            module = ast.parse(module_path.read_text(encoding="utf-8"))
            classes = {
                node.name for node in module.body if isinstance(node, ast.ClassDef)
            }
            self.assertTrue(
                expected_classes.issubset(classes),
                msg=f"{module_path} should define {sorted(expected_classes)}",
            )

    def test_foundation_routes_use_web_app_state_accessors_for_repo_lookup(self):
        route_expectations = {
            Path("sync_app/web/routes_public.py"): {
                "required": ("get_web_repositories",),
                "forbidden": (
                    "request.app.state.db_manager",
                    "request.app.state.organization_repo",
                    "request.app.state.user_repo",
                ),
            },
            Path("sync_app/web/routes_auth.py"): {
                "required": ("get_web_repositories", "get_web_runtime_state"),
                "forbidden": (
                    "request.app.state.user_repo",
                    "request.app.state.audit_repo",
                    "request.app.state.login_rate_limiter",
                ),
            },
            Path("sync_app/web/routes_admin.py"): {
                "required": ("get_web_repositories",),
                "forbidden": (
                    "request.app.state.user_repo",
                    "request.app.state.audit_repo",
                ),
            },
        }

        for module_path, expectations in route_expectations.items():
            source = module_path.read_text(encoding="utf-8")
            for required in expectations["required"]:
                self.assertIn(required, source, msg=f"{module_path} should use {required}")
            for forbidden in expectations["forbidden"]:
                self.assertNotIn(
                    forbidden,
                    source,
                    msg=f"{module_path} should not reach into {forbidden} directly",
                )

    def test_config_workflow_modules_use_web_app_state_accessors_for_repo_lookup(self):
        module_expectations = {
            Path("sync_app/web/config_submission.py"): {
                "required": ("get_web_repositories", "get_web_runtime_state"),
                "forbidden": (
                    "request.app.state.org_config_repo",
                    "request.app.state.settings_repo",
                    "request.app.state.exclusion_repo",
                    "request.app.state.config_path",
                ),
            },
            Path("sync_app/web/config_persistence.py"): {
                "required": ("get_web_repositories",),
                "forbidden": (
                    "request.app.state.org_config_repo",
                    "request.app.state.settings_repo",
                    "request.app.state.exclusion_repo",
                    "request.app.state.audit_repo",
                ),
            },
            Path("sync_app/web/routes_config.py"): {
                "required": ("get_web_services", "get_web_runtime_state"),
                "forbidden": (
                    "get_web_repositories(",
                    "request.app.state.settings_repo",
                    "request.app.state.audit_repo",
                    "request.app.state.config_release_snapshot_repo",
                    "request.app.state.web_runtime_settings",
                ),
            },
        }

        for module_path, expectations in module_expectations.items():
            source = module_path.read_text(encoding="utf-8")
            for required in expectations["required"]:
                self.assertIn(required, source, msg=f"{module_path} should use {required}")
            for forbidden in expectations["forbidden"]:
                self.assertNotIn(
                    forbidden,
                    source,
                    msg=f"{module_path} should not reach into {forbidden} directly",
                )

    def test_operational_routes_use_web_app_state_accessors_for_repo_lookup(self):
        module_expectations = {
            Path("sync_app/web/routes_integrations.py"): {
                "required": ("get_web_services", "get_web_runtime_state"),
                "forbidden": (
                    "request.app.state.db_manager",
                    "request.app.state.settings_repo",
                    "request.app.state.audit_repo",
                    "request.app.state.integration_webhook_subscription_repo",
                    "request.app.state.job_repo",
                    "request.app.state.review_repo",
                    "request.app.state.conflict_repo",
                    "request.app.state.integration_outbox_worker",
                ),
            },
            Path("sync_app/web/routes_automation_center.py"): {
                "required": ("get_web_repositories", "get_web_runtime_state"),
                "forbidden": (
                    "request.app.state.db_manager",
                    "request.app.state.config_path",
                    "request.app.state.settings_repo",
                    "request.app.state.audit_repo",
                ),
            },
            Path("sync_app/web/routes_lifecycle.py"): {
                "required": ("get_web_repositories",),
                "forbidden": (
                    "request.app.state.db_manager",
                    "request.app.state.audit_repo",
                ),
            },
            Path("sync_app/web/routes_data_quality.py"): {
                "required": ("get_web_repositories",),
                "forbidden": (
                    "request.app.state.db_manager",
                    "request.app.state.audit_repo",
                    "request.app.state.data_quality_snapshot_repo",
                ),
            },
        }

        for module_path, expectations in module_expectations.items():
            source = module_path.read_text(encoding="utf-8")
            for required in expectations["required"]:
                self.assertIn(required, source, msg=f"{module_path} should use {required}")
            for forbidden in expectations["forbidden"]:
                self.assertNotIn(
                    forbidden,
                    source,
                    msg=f"{module_path} should not reach into {forbidden} directly",
                )

    def test_integration_routes_use_web_service_accessors_for_domain_workflows(self):
        source = Path("sync_app/web/routes_integrations.py").read_text(encoding="utf-8")

        self.assertIn("get_web_services", source)
        for forbidden in (
            "generate_integration_api_token(",
            "validate_integration_subscription_payload(",
            "retry_outbox_delivery(",
            "retry_failed_outbox_deliveries(",
            "serialize_job_record(",
            "serialize_job_records(",
            "serialize_conflict_record(",
            "approve_job_review(",
            'action_type="integration.token_rotate"',
            'action_type="integration.token_clear"',
            'action_type="integration.subscription_save"',
            'action_type="integration.subscription_delete"',
            'action_type="integration.delivery_retry"',
            'action_type="integration.delivery_retry_bulk"',
            'action_type="integration.review_approve"',
        ):
            self.assertNotIn(forbidden, source, msg=f"routes_integrations.py should keep {forbidden} in facade")

    def test_config_routes_use_web_service_accessors_for_release_workflows(self):
        source = Path("sync_app/web/routes_config.py").read_text(encoding="utf-8")

        self.assertIn("get_web_services", source)
        for forbidden in (
            "build_config_release_center_context(",
            "publish_config_release_snapshot(",
            "rollback_config_release_snapshot(",
            "config_release_snapshot_repo",
            "resolve_web_runtime_settings(",
            "web_runtime_requires_restart(",
            'action_type="config.release_publish"',
            'action_type="config.release_rollback"',
            "json.dumps(snapshot.bundle",
        ):
            self.assertNotIn(forbidden, source, msg=f"routes_config.py should keep {forbidden} in facade")

    def test_support_modules_use_web_app_state_accessors_for_repo_lookup(self):
        module_expectations = {
            Path("sync_app/web/sync_directory_support.py"): {
                "required": ("get_web_repositories", "get_web_runtime_state"),
                "forbidden": (
                    "request.app.state.settings_repo",
                    "request.app.state.connector_repo",
                    "request.app.state.department_ou_mapping_repo",
                    "request.app.state.exception_rule_repo",
                    "request.app.state.org_config_repo",
                    "request.app.state.user_binding_repo",
                    "request.app.state.department_override_repo",
                    "request.app.state.config_path",
                ),
            },
            Path("sync_app/web/sync_conflict_support.py"): {
                "required": ("get_web_repositories",),
                "forbidden": (
                    "request.app.state.mapping_rule_repo",
                    "request.app.state.user_binding_repo",
                    "request.app.state.settings_repo",
                    "app.state.conflict_repo",
                    "app.state.org_config_repo",
                    "app.state.user_binding_repo",
                    "app.state.exception_rule_repo",
                ),
            },
            Path("sync_app/web/sync_support.py"): {
                "required": ("get_web_repositories",),
                "forbidden": (
                    "app.state.settings_repo",
                    "app.state.replay_request_repo",
                ),
            },
        }

        for module_path, expectations in module_expectations.items():
            source = module_path.read_text(encoding="utf-8")
            for required in expectations["required"]:
                self.assertIn(required, source, msg=f"{module_path} should use {required}")
            for forbidden in expectations["forbidden"]:
                self.assertNotIn(
                    forbidden,
                    source,
                    msg=f"{module_path} should not reach into {forbidden} directly",
                )

    def test_shared_request_and_config_support_use_web_app_state_accessors(self):
        module_expectations = {
            Path("sync_app/web/request_support.py"): {
                "required": ("get_web_repositories", "get_web_runtime_state"),
                "forbidden": (
                    "request.app.state",
                    "app.state.",
                ),
            },
            Path("sync_app/web/config_support.py"): {
                "required": ("get_web_repositories",),
                "forbidden": (
                    "request.app.state.db_manager",
                    "app.state.db_manager",
                ),
            },
        }

        for module_path, expectations in module_expectations.items():
            source = module_path.read_text(encoding="utf-8")
            for required in expectations["required"]:
                self.assertIn(required, source, msg=f"{module_path} should use {required}")
            for forbidden in expectations["forbidden"]:
                self.assertNotIn(
                    forbidden,
                    source,
                    msg=f"{module_path} should not reach into {forbidden} directly",
                )


if __name__ == "__main__":
    unittest.main()
