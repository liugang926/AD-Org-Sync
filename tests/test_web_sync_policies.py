import re
from unittest.mock import patch

from sync_app.services.runtime_bootstrap import resolve_runtime_config_fingerprint
from sync_app.services.typed_settings import AdvancedSyncPolicySettings, DirectoryUiSettings
from sync_app.web.navigation import PHASE7_LEGACY_GET_REDIRECTS
from tests.helpers.execution_plans import create_eligible_execution_plan
from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class WebSyncPolicyTests(WebAuthzBaseTestCase):
    def _seed_ad_ou_snapshot(self, *, connector_id: str = "asia") -> int:
        repository = self.app.state.ad_directory_snapshot_repo
        snapshot_id = repository.start_snapshot(
            org_id="default",
            connector_id=connector_id,
            created_by="superadmin",
        )
        repository.replace_ous(
            snapshot_id,
            org_id="default",
            connector_id=connector_id,
            organizational_units=[
                {
                    "guid": "ou-guid-china",
                    "dn": "OU=China,OU=Managed Users,DC=example,DC=local",
                    "name": "China",
                    "parent_dn": "OU=Managed Users,DC=example,DC=local",
                    "path": ["Managed Users", "China"],
                }
            ],
        )
        repository.complete_snapshot(
            snapshot_id,
            org_id="default",
            user_count=0,
            ou_count=1,
            duplicate_employee_id_count=0,
            duplicate_employee_number_count=0,
            snapshot_fingerprint="ad-ou-tree-fixture",
        )
        return snapshot_id

    def _seed_policy_fixture(self):
        snapshot_id = self.app.state.source_directory_repo.start_refresh(
            org_id="default",
            provider_id="wecom",
            created_by="superadmin",
        )
        self.app.state.source_directory_repo.replace_snapshot(
            snapshot_id,
            departments=[
                {
                    "source_department_id": "2",
                    "name": "Asia",
                    "parent_department_id": "0",
                    "path_ids": ["2"],
                    "path_names": ["Asia"],
                }
            ],
            users=[
                {
                    "source_user_id": "alice",
                    "display_name": "Alice",
                    "employee_id": "E001",
                    "department_ids": ["2"],
                    "department_names": ["Asia"],
                    "is_active": True,
                    "raw_payload": {"userid": "alice", "employee_id": "E001"},
                    "search_text": "Alice E001",
                }
            ],
            fields=[],
            fingerprint="phase5-policy-fixture",
        )
        self.app.state.source_directory_repo.save_scope_selection(
            org_id="default",
            provider_id="wecom",
            scope_type="full",
            username_strategy="employee_id",
            username_template="{employee_id}",
            source_field="employee_id",
            snapshot_id=snapshot_id,
            requested_by="superadmin",
        )
        self.app.state.connector_repo.upsert_connector(
            connector_id="asia",
            org_id="default",
            name="Asia Domain",
            config_path="",
            ldap_server="dc01.asia.example.local",
            ldap_domain="asia.example.local",
            ldap_username="svc-sync",
            ldap_password="connector-secret",
            default_password="bootstrap-secret",
            root_department_ids=[2],
            username_strategy="userid",
            username_collision_policy="append_employee_id",
            username_template="{userid}",
            disabled_users_ou="Disabled/Asia",
            group_mail_domain="groups.example.com",
            managed_tag_ids=["1001"],
            is_enabled=True,
        )
        return snapshot_id

    def test_canonical_policy_pages_are_task_focused_and_legacy_page_redirects(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        self._seed_policy_fixture()
        paths = (
            "/sync-policies/scope",
            "/sync-policies/account-naming",
            "/sync-policies/attribute-mappings",
            "/sync-policies/department-ou-routing",
            "/sync-policies/group-rules",
            "/sync-policies/lifecycle",
            "/sync-policies/security",
        )

        for path in paths:
            with self.subTest(path=path):
                response = self._route(path, "GET")(self._request(path))
                self.assertEqual(response.status_code, 200)
                body = self._text(response)
                self.assertEqual(len(re.findall(r'class="button"(?:\s|>)', body)), 1)
                self.assertIn("Current status", body)
                self.assertIn("Blocking reason", body)
                self.assertIn("Next step", body)
                for table in re.findall(r"<table[^>]*>(.*?)</table>", body, re.S):
                    self.assertLessEqual(len(re.findall(r"<th(?:\s|>)", table)), 8)

        landing = self._route("/sync-policies", "GET")(
            self._request("/sync-policies")
        )
        self.assertEqual(landing.status_code, 307)
        self.assertEqual(landing.headers["location"], "/sync-policies/scope")
        legacy = self._route("/advanced-sync", "GET")(
            self._request("/advanced-sync")
        )
        self.assertEqual(legacy.status_code, 200)
        self.assertNotIn("Pending Lifecycle Queue", self._text(legacy))
        self.assertEqual(
            PHASE7_LEGACY_GET_REDIRECTS["/advanced-sync"],
            "/sync-policies/scope",
        )

    def test_ad_attribute_capability_catalog_is_a_direct_snapshot_bound_view(self):
        self._login("superadmin")
        snapshot_id = self._seed_ad_ou_snapshot(connector_id="default")
        self.app.state.ad_target_attribute_registry_repo.sync_snapshot_catalog(
            org_id="default",
            ad_connector_id="default",
            snapshot_id=snapshot_id,
            capability_report={
                "capabilities": {
                    "update_user": {"status": "success", "verified": True}
                },
                "schema_attributes": ["displayName", "manager", "objectGUID"],
            },
            discovered_attributes=["displayName", "manager", "objectGUID"],
        )

        response = self._route("/sync-policies/attribute-mappings", "GET")(
            self._request(
                "/sync-policies/attribute-mappings",
                query={"view": "ad-capabilities"},
            )
        )

        self.assertEqual(response.status_code, 200)
        body = self._text(response)
        self.assertIn("AD Attribute Capability Catalog", body)
        self.assertIn('aria-label="AD attribute capability registry"', body)
        self.assertIn("displayName", body)
        self.assertIn("manager_dn", body)
        self.assertIn("objectGUID", body)
        self.assertIn("read_only", body)
        self.assertNotIn("Attribute Mapping Policy", body)

    def test_department_routing_renders_source_and_ad_ou_snapshot_trees(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        self._seed_policy_fixture()
        snapshot_id = self._seed_ad_ou_snapshot()

        response = self._route("/sync-policies/department-ou-routing", "GET")(
            self._request(
                "/sync-policies/department-ou-routing",
                query={"connector_id": "asia"},
            )
        )

        self.assertEqual(response.status_code, 200)
        body = self._text(response)
        self.assertIn("data-department-tree", body)
        self.assertIn('value="2" data-department-name="Asia"', body)
        self.assertIn("data-ou-tree", body)
        self.assertIn('data-target-ou-path="Managed Users/China"', body)
        self.assertIn(
            "OU=China,OU=Managed Users,DC=example,DC=local",
            body,
        )
        self.assertIn(f"AD snapshot #{snapshot_id}", body)

    def test_sync_scope_never_invokes_ad_verification_from_legacy_query(self):
        self._login("superadmin")
        self._seed_policy_fixture()
        path = "/sync-policies/scope"

        with patch(
            "sync_app.web.app.build_target_provider",
            side_effect=AssertionError("sync scope must not build an AD provider"),
        ) as build_target_provider:
            response = self._route(path, "GET")(
                self._request(path, query={"verify_ad": "true"})
            )

        self.assertEqual(response.status_code, 200)
        build_target_provider.assert_not_called()

    def test_legacy_entry_redirects_and_scope_forms_are_separated(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        self._seed_policy_fixture()

        legacy = self._route("/advanced-sync", "GET")(
            self._request("/advanced-sync")
        )
        self.assertEqual(legacy.status_code, 200)
        legacy_text = self._text(legacy)
        self.assertNotIn('action="/advanced-sync/policies"', legacy_text)
        self.assertNotIn('action="/advanced-sync/mappings"', legacy_text)
        self.assertNotIn("Pending Lifecycle Queue", legacy_text)

        scope = self._text(
            self._route("/sync-policies/scope", "GET")(
                self._request("/sync-policies/scope")
            )
        )
        policy_form = re.search(
            r'<form[^>]+id="sync-scope-policy-form".*?</form>',
            scope,
            re.S,
        )
        selection_form = re.search(
            r'<form[^>]+id="sync-scope-selection-form".*?</form>',
            scope,
            re.S,
        )
        self.assertIsNotNone(policy_form)
        self.assertIsNotNone(selection_form)
        self.assertNotIn("selected_source_user_ids", policy_form.group(0))
        self.assertIn("selected_source_user_ids", selection_form.group(0))
        self.assertIn('data-policy-change-form', policy_form.group(0))
        self.assertIn('{policy_old_values}', policy_form.group(0))
        self.assertIn('{policy_new_values}', policy_form.group(0))

        security = self._text(
            self._route("/sync-policies/security", "GET")(
                self._request("/sync-policies/security")
            )
        )
        self.assertNotIn('action="/sync-policies/security"', security)
        self.assertIn('href="/sync-policies/lifecycle#security"', security)

        legacy_config = self._route("/config", "GET")(
            self._request("/config")
        )
        self.assertEqual(legacy_config.status_code, 200)
        self.assertEqual(
            PHASE7_LEGACY_GET_REDIRECTS["/config"],
            "/data-sources/connectors",
        )
        routing = self._text(
            self._route("/sync-policies/department-ou-routing", "GET")(
                self._request("/sync-policies/department-ou-routing")
            )
        )
        groups = self._text(
            self._route("/sync-policies/group-rules", "GET")(
                self._request("/sync-policies/group-rules")
            )
        )
        self.assertIn('name="directory_root_ou_path"', routing)
        self.assertIn('name="soft_excluded_groups"', groups)
        source_directory = self._text(
            self._route("/data-sources/source-directory", "GET")(
                self._request("/data-sources/source-directory")
            )
        )
        self.assertIn('href="/sync-policies/scope"', source_directory)
        self.assertNotIn('action="/source-directory/scope"', source_directory)

    def test_policy_save_forms_include_old_new_scope_and_impact_review(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        self._seed_policy_fixture()
        for path in (
            "/sync-policies/scope",
            "/sync-policies/account-naming",
            "/sync-policies/attribute-mappings",
            "/sync-policies/department-ou-routing",
            "/sync-policies/group-rules",
            "/sync-policies/lifecycle",
        ):
            with self.subTest(path=path):
                body = self._text(self._route(path, "GET")(self._request(path)))
                forms = re.findall(
                    r"<form[^>]+data-policy-change-form.*?</form>",
                    body,
                    re.S,
                )
                self.assertTrue(forms)
                for form in forms:
                    self.assertIn("{policy_old_values}", form)
                    self.assertIn("{policy_new_values}", form)
                    self.assertIn("{policy_scope}", form)
                    self.assertIn("{policy_impact}", form)
                self.assertIn("Configuration version", body)
                self.assertIn("Last modified by", body)
                self.assertIn("Estimated impact", body)

    def test_field_authority_rule_is_audited_and_changes_runtime_fingerprint(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        self._seed_policy_fixture()
        path = "/sync-policies/field-authority"
        page = self._text(self._route(path, "GET")(self._request(path)))
        self.assertIn('action="/sync-policies/field-authority"', page)
        self.assertIn("Add or update field authority", page)
        fingerprint_before = resolve_runtime_config_fingerprint(
            db_manager=self.app.state.db_manager,
            org_id="default",
            config_path=str(self.config_path),
        )

        save_path = path
        response = self._route(save_path, "POST")(
            self._request(save_path, "POST"),
            csrf_token=self.session["_csrf_token"],
            field_name="email",
            source_provider="wecom",
            source_priority=5,
            sync_direction="bidirectional",
            sync_mode="fill_if_empty",
            prevent_loop="1",
            is_enabled="1",
            notes="Email is mastered in WeCom and generated by AD when empty.",
        )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], path)
        legacy_path = "/sync-policies/attribute-mappings/field-authority"
        legacy_response = self._route(legacy_path, "POST")(
            self._request(legacy_path, "POST"),
            csrf_token=self.session["_csrf_token"],
            field_name="email",
            source_provider="wecom",
            source_priority=5,
            sync_direction="bidirectional",
            sync_mode="fill_if_empty",
            prevent_loop="1",
            is_enabled="1",
            notes="Compatibility route remains supported.",
        )
        self.assertEqual(legacy_response.status_code, 303)
        self.assertEqual(
            legacy_response.headers["location"],
            "/sync-policies/attribute-mappings",
        )
        rules = self.app.state.field_authority_rule_repo.list_rules(org_id="default")
        saved = next(
            rule
            for rule in rules
            if rule.field_name == "email" and rule.source_provider == "wecom"
        )
        self.assertEqual(saved.source_priority, 5)
        self.assertEqual(saved.sync_direction, "bidirectional")
        self.assertTrue(saved.prevent_loop)
        self.assertNotEqual(
            fingerprint_before,
            resolve_runtime_config_fingerprint(
                db_manager=self.app.state.db_manager,
                org_id="default",
                config_path=str(self.config_path),
            ),
        )
        self.assertTrue(
            any(
                row.action_type == "sync_policy.field_authority.update"
                for row in self.app.state.audit_repo.list_recent_logs(20)
            )
        )

    def test_field_authority_options_use_business_labels_in_chinese_basic_mode(self):
        self._login("superadmin")
        self.session["ui_language"] = "zh-CN"
        path = "/sync-policies/field-authority"

        basic_body = self._text(self._route(path, "GET")(self._request(path)))
        field_select = re.search(
            r'<select name="field_name"[^>]*>(.*?)</select>',
            basic_body,
            re.S,
        )

        self.assertIsNotNone(field_select)
        self.assertIn('<optgroup label="联系方式">', field_select.group(1))
        self.assertIn(
            '<option value="alternate_email">备用邮箱</option>',
            field_select.group(1),
        )
        self.assertNotIn("备用邮箱 · alternate_email", field_select.group(1))
        self.assertIn(
            'value="PROVIDER_PRIORITY" selected>按平台优先顺序取第一个有值的来源</option>',
            basic_body,
        )
        self.assertIn(
            'value="REJECT_ON_CONFLICT">值冲突时停止并要求处理</option>',
            basic_body,
        )
        self.assertIn(
            'value="PRESERVE_TARGET" selected>来源为空时保留目标现值</option>',
            basic_body,
        )
        self.assertIn('<option value="dingtalk">钉钉</option>', basic_body)
        self.assertIn(
            '<option value="">仅使用权威来源（推荐）</option>',
            basic_body,
        )
        self.assertIn(
            '<option value="dingtalk,wecom,feishu">钉钉 → 企业微信 → 飞书</option>',
            basic_body,
        )
        self.assertNotIn('placeholder="dingtalk,wecom,feishu"', basic_body)

        self.session["ui_mode"] = "advanced"
        advanced_body = self._text(
            self._route(path, "GET")(self._request(path))
        )
        self.assertIn(
            '<option value="alternate_email">备用邮箱 · alternate_email</option>',
            advanced_body,
        )
        self.assertIn(
            "按平台优先顺序取第一个有值的来源 · PROVIDER_PRIORITY",
            advanced_body,
        )
        self.assertIn('placeholder="dingtalk,wecom,feishu"', advanced_body)

    def test_routing_and_group_pages_persist_existing_settings_keys(self):
        self._login("superadmin")
        self._seed_policy_fixture()
        DirectoryUiSettings(
            custom_group_ou_path="Groups/Keep",
            group_display_separator="_",
        ).persist(self.app.state.settings_repo, org_id="default")

        routing_path = "/sync-policies/department-ou-routing/defaults"
        routed = self._route(routing_path, "POST")(
            self._request(routing_path, "POST"),
            csrf_token=self.session["_csrf_token"],
            advanced_connector_routing_enabled="1",
            user_ou_placement_strategy="shortest_path",
            source_root_unit_ids="2,8",
            source_root_unit_display_text="Asia / Europe",
            directory_root_ou_path="Managed Users/Regional",
            disabled_users_ou_path="Disabled/Regional",
        )
        self.assertEqual(routed.status_code, 303)
        directory = DirectoryUiSettings.load(
            self.app.state.settings_repo,
            org_id="default",
        )
        self.assertEqual(directory.directory_root_ou_path, "Managed Users/Regional")
        self.assertEqual(directory.disabled_users_ou_path, "Disabled/Regional")
        self.assertEqual(directory.source_root_unit_ids, "2,8")
        self.assertEqual(directory.custom_group_ou_path, "Groups/Keep")
        self.assertEqual(directory.group_display_separator, "_")

        group_path = "/sync-policies/group-rules"
        grouped = self._route(group_path, "POST")(
            self._request(group_path, "POST"),
            csrf_token=self.session["_csrf_token"],
            custom_group_sync_enabled="1",
            managed_group_type="security",
            managed_group_mail_domain="groups.example.com",
            custom_group_ou_path="Managed Groups/New",
            connector_id="",
            connector_group_type="security",
            connector_group_mail_domain="",
            connector_custom_group_ou_path="",
            managed_tag_ids="",
            managed_external_chat_ids="",
            group_display_separator="-",
            group_recursive_enabled="",
            managed_relation_cleanup_enabled="true",
            soft_excluded_groups="Legacy Ignore\nTemporary Ignore",
        )
        self.assertEqual(grouped.status_code, 303)
        directory = DirectoryUiSettings.load(
            self.app.state.settings_repo,
            org_id="default",
        )
        self.assertFalse(directory.group_recursive_enabled)
        self.assertTrue(directory.managed_relation_cleanup_enabled)
        self.assertEqual(directory.custom_group_ou_path, "Managed Groups/New")
        exclusions = [
            record
            for record in self.app.state.exclusion_repo.list_enabled_rule_records(
                org_id="default",
            )
            if record.rule_type == "exclude"
            and record.protection_level == "soft"
        ]
        self.assertEqual(
            {record.match_value for record in exclusions},
            {"Legacy Ignore", "Temporary Ignore"},
        )

    def test_policy_change_invalidates_dry_run_and_blocks_apply(self):
        self._login("superadmin")
        created = create_eligible_execution_plan(
            self.app.state.db_manager,
            job_id="policy-before-change",
            environment_label=self.app.state.environment_label,
            approved=True,
        )
        security_path = "/sync-policies/security"
        saved = self._route(security_path, "POST")(
            self._request(security_path, "POST"),
            csrf_token=self.session["_csrf_token"],
            advanced_connector_routing_enabled="",
            disable_circuit_breaker_enabled="1",
            disable_circuit_breaker_percent=3.0,
            disable_circuit_breaker_min_count=2,
            disable_circuit_breaker_requires_approval="1",
            first_sync_identity_claim_mode="review",
            connector_id="",
            force_change_password="",
            password_complexity="",
            return_to="lifecycle",
        )
        self.assertEqual(saved.headers["location"], "/sync-policies/lifecycle#security")

        lifecycle = self._text(
            self._route("/sync-policies/lifecycle", "GET")(
                self._request("/sync-policies/lifecycle")
            )
        )
        self.assertIn("The previous Dry Run is invalid.", lifecycle)
        self.assertIn("Apply is blocked", lifecycle)

        apply_page = self._text(
            self._route("/execution-center/apply", "GET")(
                self._request(
                    "/execution-center/apply",
                    query={"plan_id": created["job"].job_id},
                ),
                plan_id=created["job"].job_id,
            )
        )
        self.assertNotIn("Apply 1 Changes", apply_page)
        self.assertIn("Run a new Dry Run", apply_page)

    def test_new_policy_write_requires_config_write_permission(self):
        self._login("operator1")
        path = "/sync-policies/department-ou-routing/defaults"
        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            advanced_connector_routing_enabled="1",
            user_ou_placement_strategy="shortest_path",
            source_root_unit_ids="2",
            source_root_unit_display_text="Asia",
            directory_root_ou_path="Managed Users/Denied",
            disabled_users_ou_path="Disabled/Denied",
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")
        settings = DirectoryUiSettings.load(
            self.app.state.settings_repo,
            org_id="default",
        )
        self.assertNotEqual(settings.directory_root_ou_path, "Managed Users/Denied")
    def test_sync_scope_hides_large_selectors_until_snapshot_exists(self):
        self._login("superadmin")

        response = self._route("/sync-policies/scope", "GET")(
            self._request("/sync-policies/scope")
        )
        body = response.body.decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn("A successful source snapshot is required.", body)
        self.assertNotIn("data-scope-table", body)
        self.assertNotIn("Connector root scopes", body)

    def test_sync_scope_uses_complete_chinese_scope_labels(self):
        self._login("superadmin")
        self.session["ui_language"] = "zh-CN"
        self._seed_policy_fixture()

        response = self._route("/sync-policies/scope", "GET")(
            self._request("/sync-policies/scope")
        )
        body = response.body.decode("utf-8")
        scope_options = re.search(
            r'<select name="scope_type"[^>]*>(.*?)</select>', body, re.S
        )

        self.assertIsNotNone(scope_options)
        option_text = scope_options.group(1)
        for label in (
            "全部在职用户",
            "所选部门及其下级部门",
            "仅已勾选用户",
            "单用户重放",
        ):
            self.assertIn(label, option_text)
        for english_label in (
            "All active users",
            "Selected departments and descendants",
            "Checked users only",
            "Single user replay",
        ):
            self.assertNotIn(english_label, option_text)

    def test_attribute_section_update_preserves_lifecycle_and_is_audited(self):
        self._login("superadmin")
        AdvancedSyncPolicySettings(
            offboarding_grace_days=21,
            future_onboarding_enabled=True,
            disable_circuit_breaker_percent=8.5,
        ).persist(self.app.state.settings_repo, org_id="default")
        path = "/sync-policies/attribute-mappings"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            attribute_mapping_enabled="1",
            write_back_enabled="1",
            connector_id="",
            direction="source_to_ad",
            source_field="",
            target_field="",
            transform_template="",
            sync_mode="replace",
            notes="",
            is_enabled=None,
        )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], path)
        saved = AdvancedSyncPolicySettings.load(
            self.app.state.settings_repo,
            org_id="default",
        )
        self.assertTrue(saved.attribute_mapping_enabled)
        self.assertTrue(saved.write_back_enabled)
        self.assertEqual(saved.offboarding_grace_days, 21)
        self.assertTrue(saved.future_onboarding_enabled)
        self.assertEqual(saved.disable_circuit_breaker_percent, 8.5)
        self.assertTrue(
            any(
                row.action_type == "sync_policy.attribute_mappings.update"
                for row in self.app.state.audit_repo.list_recent_logs(20)
            )
        )

    def test_invalid_csrf_does_not_update_policy(self):
        self._login("superadmin")
        path = "/sync-policies/security"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token="wrong",
            advanced_connector_routing_enabled="1",
            disable_circuit_breaker_enabled="1",
            disable_circuit_breaker_percent=1.0,
            disable_circuit_breaker_min_count=1,
            disable_circuit_breaker_requires_approval="1",
            first_sync_identity_claim_mode="review",
            connector_id="",
            force_change_password="",
            password_complexity="",
        )

        self.assertEqual(response.status_code, 303)
        saved = AdvancedSyncPolicySettings.load(
            self.app.state.settings_repo,
            org_id="default",
        )
        self.assertFalse(saved.disable_circuit_breaker_enabled)
        self.assertFalse(saved.advanced_connector_routing_enabled)

    def test_policy_pages_and_writes_require_config_capabilities(self):
        self._login("operator1")
        for path in (
            "/sync-policies/scope",
            "/sync-policies/account-naming",
            "/sync-policies/attribute-mappings",
            "/sync-policies/department-ou-routing",
            "/sync-policies/group-rules",
            "/sync-policies/lifecycle",
            "/sync-policies/security",
            "/sync-policies/releases",
        ):
            with self.subTest(path=path):
                response = self._route(path, "GET")(self._request(path))
                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers["location"], "/dashboard")

        security = self._route("/sync-policies/security", "POST")(
            self._request("/sync-policies/security", "POST"),
            csrf_token=self.session["_csrf_token"],
            advanced_connector_routing_enabled="1",
            disable_circuit_breaker_enabled="1",
            disable_circuit_breaker_percent=1.0,
            disable_circuit_breaker_min_count=1,
            disable_circuit_breaker_requires_approval="1",
            first_sync_identity_claim_mode="review",
            connector_id="",
            force_change_password="",
            password_complexity="",
        )
        self.assertEqual(security.status_code, 303)
        self.assertEqual(security.headers["location"], "/dashboard")
        saved = AdvancedSyncPolicySettings.load(
            self.app.state.settings_repo,
            org_id="default",
        )
        self.assertFalse(saved.disable_circuit_breaker_enabled)

    def test_connector_naming_update_preserves_connection_scope_group_and_lifecycle(self):
        self._login("superadmin")
        self._seed_policy_fixture()
        path = "/sync-policies/account-naming"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            policy_target="connector",
            connector_id="asia",
            username_strategy="employee_id",
            username_template="{employee_id}",
            username_collision_policy="append_userid",
            username_collision_template="",
            employee_id_attribute="",
        )

        self.assertEqual(response.status_code, 303)
        record = self.app.state.connector_repo.get_connector_record(
            "asia",
            org_id="default",
        )
        self.assertEqual(record.username_strategy, "employee_id")
        self.assertEqual(record.username_collision_policy, "append_userid")
        self.assertEqual(record.ldap_server, "dc01.asia.example.local")
        self.assertEqual(record.ldap_password, "connector-secret")
        self.assertEqual(record.default_password, "bootstrap-secret")
        self.assertEqual(record.root_department_ids, [2])
        self.assertEqual(record.managed_tag_ids, ["1001"])
        self.assertEqual(record.disabled_users_ou, "Disabled/Asia")

    def test_source_dynamic_field_naming_preserves_scope_and_is_audited(self):
        self._login("superadmin")
        self._seed_policy_fixture()
        path = "/sync-policies/account-naming"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            policy_target="source",
            connector_id="",
            username_strategy="userid",
            username_template="ignored for dynamic field",
            username_collision_policy="append_employee_id",
            username_collision_template="",
            employee_id_attribute="employee_number",
            source_field="tenant_staff_code",
        )

        self.assertEqual(response.status_code, 303)
        scope = self.app.state.source_directory_repo.get_scope_selection(
            org_id="default",
            provider_id="wecom",
        )
        self.assertEqual(scope["scope_type"], "full")
        self.assertEqual(scope["source_field"], "tenant_staff_code")
        self.assertEqual(scope["username_strategy"], "custom_template")
        self.assertEqual(scope["username_template"], "{tenant_staff_code}")
        self.assertEqual(
            self.app.state.settings_repo.get_value(
                "source_employee_id_attribute", "", org_id="default"
            ),
            "employee_number",
        )
        self.assertTrue(
            any(
                row.action_type == "sync_policy.account_naming.update"
                for row in self.app.state.audit_repo.list_recent_logs(20)
            )
        )

    def test_connector_policy_sections_preserve_secrets_and_emit_audit(self):
        self._login("superadmin")
        self._seed_policy_fixture()

        group_path = "/sync-policies/group-rules"
        group = self._route(group_path, "POST")(
            self._request(group_path, "POST"),
            csrf_token=self.session["_csrf_token"],
            custom_group_sync_enabled="1",
            managed_group_type="security",
            managed_group_mail_domain="org.example.com",
            custom_group_ou_path="Managed Groups",
            connector_id="asia",
            connector_group_type="distribution",
            connector_group_mail_domain="asia.example.com",
            connector_custom_group_ou_path="Groups/Asia",
            managed_tag_ids="1001,1002",
            managed_external_chat_ids="chat-1",
        )
        self.assertEqual(group.status_code, 303)

        lifecycle_path = "/sync-policies/lifecycle"
        lifecycle = self._route(lifecycle_path, "POST")(
            self._request(lifecycle_path, "POST"),
            csrf_token=self.session["_csrf_token"],
            offboarding_grace_days=14,
            offboarding_notify_managers="1",
            offboarding_lifecycle_enabled="1",
            rehire_restore_enabled="1",
            automatic_replay_enabled="1",
            future_onboarding_enabled="1",
            future_onboarding_start_field="hire_date",
            contractor_lifecycle_enabled="1",
            lifecycle_employment_type_field="employment_type",
            contractor_end_field="contract_end_date",
            lifecycle_sponsor_field="sponsor_userid",
            contractor_type_values="contractor,vendor",
            connector_id="asia",
            disabled_users_ou="Disabled/Reviewed",
        )
        self.assertEqual(lifecycle.status_code, 303)

        security_path = "/sync-policies/security"
        security = self._route(security_path, "POST")(
            self._request(security_path, "POST"),
            csrf_token=self.session["_csrf_token"],
            advanced_connector_routing_enabled="1",
            disable_circuit_breaker_enabled="1",
            disable_circuit_breaker_percent=7.5,
            disable_circuit_breaker_min_count=12,
            disable_circuit_breaker_requires_approval="1",
            first_sync_identity_claim_mode="review",
            connector_id="asia",
            force_change_password="false",
            password_complexity="reviewed-strong",
        )
        self.assertEqual(security.status_code, 303)

        record = self.app.state.connector_repo.get_connector_record(
            "asia", org_id="default"
        )
        self.assertEqual(record.ldap_password, "connector-secret")
        self.assertEqual(record.default_password, "bootstrap-secret")
        self.assertEqual(record.root_department_ids, [2])
        self.assertEqual(record.username_strategy, "userid")
        self.assertEqual(record.group_type, "distribution")
        self.assertEqual(record.managed_tag_ids, ["1001", "1002"])
        self.assertEqual(record.disabled_users_ou, "Disabled/Reviewed")
        self.assertFalse(record.force_change_password)
        self.assertEqual(record.password_complexity, "reviewed-strong")
        audit_actions = {
            row.action_type for row in self.app.state.audit_repo.list_recent_logs(30)
        }
        self.assertTrue(
            {
                "sync_policy.group_rules.update",
                "sync_policy.lifecycle.update",
                "sync_policy.security.update",
            }.issubset(audit_actions)
        )

    def test_foreign_connector_cannot_be_updated_from_selected_organization(self):
        self._login("superadmin")
        self.app.state.organization_repo.upsert_organization(
            org_id="other",
            name="Other Organization",
            config_path="other.ini",
            is_enabled=True,
        )
        self.app.state.connector_repo.upsert_connector(
            connector_id="foreign",
            org_id="other",
            name="Foreign Connector",
            config_path="",
            ldap_server="foreign.example.local",
            ldap_password="foreign-secret",
            is_enabled=True,
        )
        path = "/sync-policies/account-naming"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            policy_target="connector",
            connector_id="foreign",
            username_strategy="employee_id",
            username_template="{employee_id}",
            username_collision_policy="append_userid",
            username_collision_template="",
            employee_id_attribute="",
            source_field="",
        )

        self.assertEqual(response.status_code, 303)
        foreign = self.app.state.connector_repo.get_connector_record(
            "foreign", org_id="other"
        )
        self.assertEqual(foreign.username_strategy, "custom_template")
        self.assertEqual(foreign.ldap_password, "foreign-secret")

    def test_scope_update_preserves_naming_and_can_update_connector_roots(self):
        self._login("superadmin")
        self._seed_policy_fixture()
        path = "/sync-policies/scope"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            scope_type="full",
            selected_department_ids=[],
            selected_source_user_ids=[],
            source_field="",
            username_template=None,
            employee_id_attribute=None,
            connector_id="asia",
            root_department_ids="2, 8",
            selection_mode="explicit",
            selection_search="",
            selection_department_id="",
            selection_status="",
            selection_employee_id_state="",
        )

        self.assertEqual(response.status_code, 303)
        scope = self.app.state.source_directory_repo.get_scope_selection(
            org_id="default",
            provider_id="wecom",
        )
        self.assertEqual(scope["username_strategy"], "employee_id")
        self.assertEqual(scope["username_template"], "{employee_id}")
        connector = self.app.state.connector_repo.get_connector_record(
            "asia",
            org_id="default",
        )
        self.assertEqual(connector.root_department_ids, [2, 8])
        self.assertEqual(connector.username_strategy, "userid")
        self.assertEqual(connector.ldap_password, "connector-secret")

    def test_canonical_routing_write_and_delete_keep_csrf_org_scope_and_audit(self):
        self._login("superadmin")
        self._seed_policy_fixture()
        path = "/sync-policies/department-ou-routing"

        saved = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            connector_id="asia",
            source_department_id="2",
            source_department_name="Asia",
            target_ou_path="Managed Users/Asia",
            apply_mode="subtree",
            notes="regional route",
            is_enabled="1",
        )
        self.assertEqual(saved.status_code, 303)
        self.assertEqual(saved.headers["location"], path)
        records = self.app.state.department_ou_mapping_repo.list_mapping_records(
            org_id="default"
        )
        self.assertEqual(len(records), 1)
        self.assertTrue(
            any(
                row.action_type == "sync_policy.department_ou_routing.update"
                for row in self.app.state.audit_repo.list_recent_logs(20)
            )
        )

        delete_path = path + "/{mapping_id}/delete"
        deleted = self._route(delete_path, "POST")(
            self._request(path + f"/{records[0].id}/delete", "POST"),
            mapping_id=records[0].id,
            csrf_token=self.session["_csrf_token"],
        )
        self.assertEqual(deleted.status_code, 303)
        self.assertEqual(deleted.headers["location"], path)
        self.assertEqual(
            self.app.state.department_ou_mapping_repo.list_mapping_records(
                org_id="default"
            ),
            [],
        )
        self.assertTrue(
            any(
                row.action_type == "sync_policy.department_ou_routing.delete"
                for row in self.app.state.audit_repo.list_recent_logs(20)
            )
        )

    def test_release_alias_uses_canonical_actions_and_blocks_incomplete_readiness(self):
        self._login("superadmin")
        path = "/sync-policies/releases"
        page = self._route(path, "GET")(self._request(path))
        body = self._text(page)
        self.assertEqual(page.status_code, 200)
        self.assertIn('action="/sync-policies/releases/publish"', body)
        self.assertNotIn('action="/config/releases/publish"', body)
        self.assertIn('disabled aria-disabled="true"', body)

        published = self._route(path + "/publish", "POST")(
            self._request(path + "/publish", "POST"),
            csrf_token=self.session["_csrf_token"],
            snapshot_name="Phase 5 baseline",
        )
        self.assertEqual(published.status_code, 303)
        self.assertEqual(published.headers["location"], path)
        snapshots = self.app.state.config_release_snapshot_repo.list_snapshot_records(
            org_id="default"
        )
        self.assertEqual(snapshots, [])
        blocked_page = self._text(self._route(path, "GET")(self._request(path)))
        self.assertIn("Policy release blocked", blocked_page)


if __name__ == "__main__":
    import unittest

    unittest.main()
