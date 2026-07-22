import re
from html.parser import HTMLParser

from sync_app.services.data_quality_center import persist_data_quality_snapshot
from sync_app.web.ui_mode import (
    ADVANCED_UI_MODE,
    BASIC_UI_MODE,
    get_ui_mode_presentation,
)
from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class _VisibleTextParser(HTMLParser):
    _VOID_TAGS = {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }

    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._hidden_depth = 0
        self._tag_stack: list[tuple[str, bool]] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        attributes = dict(attrs)
        suppresses_text = "hidden" in attributes or tag in {"script", "style"}
        if suppresses_text:
            self._hidden_depth += 1
        if tag not in self._VOID_TAGS:
            self._tag_stack.append((tag, suppresses_text))

    def handle_endtag(self, tag: str) -> None:
        while self._tag_stack:
            open_tag, suppresses_text = self._tag_stack.pop()
            if suppresses_text:
                self._hidden_depth -= 1
            if open_tag == tag:
                break

    def handle_data(self, data: str) -> None:
        if not self._hidden_depth and data.strip():
            self.parts.append(data.strip())

    @property
    def text(self) -> str:
        return " ".join(self.parts)


def _visible_text(markup: str) -> str:
    parser = _VisibleTextParser()
    parser.feed(markup)
    return parser.text


def _table_column_counts(markup: str) -> list[int]:
    return [
        len(re.findall(r"<th\b", table, re.I))
        for table in re.findall(r"<table\b[^>]*>(.*?)</table>", markup, re.I | re.S)
    ]


class WebUIModeTests(WebAuthzBaseTestCase):
    def _seed_directory(self) -> None:
        snapshot_id = self.app.state.source_directory_repo.start_refresh(
            org_id="default",
            provider_id="wecom",
            created_by="mode-test",
        )
        self.app.state.source_directory_repo.replace_snapshot(
            snapshot_id,
            departments=[
                {
                    "source_department_id": "dept-internal-1",
                    "name": "Finance",
                    "parent_department_id": "0",
                    "path_ids": ["dept-internal-1"],
                    "path_names": ["Finance"],
                }
            ],
            users=[
                {
                    "source_user_id": "platform-user-internal-1",
                    "display_name": "Alice Example",
                    "employee_id": "E-100",
                    "department_ids": ["dept-internal-1"],
                    "department_names": ["Finance"],
                    "email": "alice@example.test",
                    "is_active": True,
                    "search_text": "Alice Example E-100",
                }
            ],
            fields=[
                {
                    "name": "tenant_staff_code",
                    "label": "Tenant Staff Code",
                    "coverage": 1,
                    "samples": ["T-100"],
                }
            ],
            fingerprint="sha256:mode-density",
        )

    def test_mode_policy_defines_density_without_security_capabilities(self):
        basic = get_ui_mode_presentation(BASIC_UI_MODE)
        advanced = get_ui_mode_presentation(ADVANCED_UI_MODE)

        self.assertEqual(basic.max_primary_columns, 8)
        self.assertEqual(basic.density, "business")
        self.assertFalse(basic.show_internal_identifiers)
        self.assertFalse(basic.show_bulk_tools)
        self.assertFalse(basic.show_routing_configuration)
        self.assertTrue(basic.show_system_guidance)
        self.assertEqual(basic.high_risk_interaction, "wizard")
        self.assertTrue(advanced.show_internal_identifiers)
        self.assertTrue(advanced.show_provider_connector_details)
        self.assertTrue(advanced.show_field_codes)
        self.assertTrue(advanced.show_audit_evidence)
        self.assertTrue(advanced.show_routing_configuration)
        self.assertNotIn("capability", basic.__dataclass_fields__)
        self.assertNotIn("role", basic.__dataclass_fields__)

    def test_switch_preserves_full_business_context_and_saved_state(self):
        self._login("superadmin")
        self.app.state.settings_repo.set_value(
            "mode_test_business_state",
            "unchanged",
            "string",
            org_id="default",
        )
        return_url = (
            "/identity-governance/exception-rules"
            "?q=finance&rule_type=skip_user_disable&status=enabled#rule-list"
        )

        response = self._route("/ui-mode", "POST")(
            self._request("/ui-mode", "POST"),
            csrf_token=self.session["_csrf_token"],
            ui_mode="advanced",
            return_url=return_url,
        )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], return_url)
        self.assertEqual(self.session["ui_mode"], "advanced")
        self.assertEqual(
            self.app.state.settings_repo.get_value(
                "mode_test_business_state",
                org_id="default",
            ),
            "unchanged",
        )

    def test_required_ou_routing_stays_reachable_in_both_modes(self):
        self._login("superadmin")
        path = "/sync-policies/department-ou-routing"

        basic = self._route(path, "GET")(self._request(path))
        basic_body = self._text(basic)
        self.assertNotIn('data-mode-gate="advanced-page"', basic_body)
        self.assertNotIn("This is advanced configuration.", basic_body)
        self.assertIn("Connector Route", _visible_text(basic_body))

        self.session["ui_mode"] = "advanced"
        advanced = self._route(path, "GET")(self._request(path))
        advanced_body = self._text(advanced)
        self.assertNotIn('data-mode-gate="advanced-page"', advanced_body)
        self.assertNotIn("This is advanced configuration.", advanced_body)
        self.assertIn("Connector Route", _visible_text(advanced_body))

    def test_bulk_import_workspace_is_advanced_only(self):
        self._login("superadmin")
        path = "/identity-governance/manual-overrides"

        basic_body = self._text(self._route(path, "GET")(self._request(path)))
        self.assertNotIn('data-mode-gate="advanced-page"', basic_body)
        self.assertIn("This is advanced configuration.", basic_body)
        self.assertNotIn("bulk import manual bindings", _visible_text(basic_body))

        self.session["ui_mode"] = "advanced"
        advanced_body = self._text(self._route(path, "GET")(self._request(path)))
        self.assertNotIn('data-mode-gate="advanced-page"', advanced_body)
        self.assertIn("bulk import manual bindings", _visible_text(advanced_body))

    def test_basic_source_list_is_business_dense_and_state_matches_advanced(self):
        self._login("superadmin")
        self._seed_directory()
        path = "/data-sources/source-directory"
        request_query = {"view": "users"}

        basic = self._route(path, "GET")(
            self._request(path, query=request_query),
            view="users",
        )
        basic_body = self._text(basic)
        basic_table = re.search(
            r'<table>(.*?)</table>',
            basic_body[basic_body.index("source-directory-business-table") :],
            re.S,
        ).group(1)
        self.assertLessEqual(basic_table.count("<th"), 8)
        self.assertNotIn("Platform User ID", _visible_text(basic_table))
        self.assertNotIn("platform-user-internal-1", _visible_text(basic_table))
        self.assertIn("Alice Example", _visible_text(basic_table))
        self.assertIn('data-mode-guidance="system-recommendation"', basic_body)

        self.session["ui_mode"] = "advanced"
        advanced = self._route(path, "GET")(
            self._request(path, query=request_query),
            view="users",
        )
        advanced_table = re.search(
            r'<table>(.*?)</table>',
            self._text(advanced)[
                self._text(advanced).index("source-directory-business-table") :
            ],
            re.S,
        ).group(1)
        self.assertIn("Platform User ID", _visible_text(advanced_table))
        self.assertIn("platform-user-internal-1", _visible_text(advanced_table))
        self.assertIn("Alice Example", _visible_text(advanced_table))

    def test_connector_management_and_identifiers_are_advanced_only(self):
        self._login("superadmin")
        self.app.state.connector_repo.upsert_connector(
            connector_id="mode-connector-secret",
            org_id="default",
            name="Regional Directory",
            config_path="",
            ldap_server="regional-dc.example.test",
            ldap_domain="example.test",
            ldap_username="sync-user",
            ldap_password="",
            ldap_use_ssl=True,
            ldap_port=636,
            ldap_validate_cert=True,
            ldap_ca_cert_path="",
            is_enabled=True,
        )
        path = "/data-sources/connectors"

        basic_body = self._text(self._route(path, "GET")(self._request(path)))
        basic_visible = _visible_text(basic_body)
        self.assertNotIn("mode-connector-secret", basic_visible)
        self.assertNotIn("Add Target Connector", basic_visible)
        self.assertIn("Save Connection Settings", basic_visible)
        self.assertIn('data-mode-guidance="system-recommendation"', basic_body)

        self.session["ui_mode"] = "advanced"
        advanced_body = self._text(self._route(path, "GET")(self._request(path)))
        advanced_visible = _visible_text(advanced_body)
        self.assertIn("mode-connector-secret", advanced_visible)
        self.assertIn("Add Target Connector", advanced_visible)
        self.assertIn("Regional Directory", advanced_visible)

    def test_field_codes_and_template_editor_are_advanced_only(self):
        self._login("superadmin")
        self._seed_directory()
        path = "/sync-policies/account-naming"

        basic_body = self._text(self._route(path, "GET")(self._request(path)))
        basic_visible = _visible_text(basic_body)
        self.assertIn("Directory username", basic_visible)
        self.assertNotIn("Platform User ID", basic_visible)
        self.assertNotIn("tenant_staff_code", basic_visible)
        self.assertIn('type="hidden" name="username_template"', basic_body)

        self.session["ui_mode"] = "advanced"
        advanced_body = self._text(self._route(path, "GET")(self._request(path)))
        advanced_visible = _visible_text(advanced_body)
        self.assertIn("Platform User ID", advanced_visible)
        self.assertIn("tenant_staff_code", advanced_visible)
        self.assertIn("Field code", advanced_visible)
        self.assertRegex(
            advanced_body,
            r'<input type="text" name="username_template"[^>]+>',
        )

    def test_internal_job_id_is_hidden_without_hiding_the_job_state(self):
        self._login("superadmin")
        job_id = "job-mode-internal-42"
        self.app.state.job_repo.create_job(
            job_id,
            trigger_type="mode_test",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="default",
            started_at="2026-07-19T10:00:00+00:00",
        )
        self.app.state.job_repo.update_job(
            job_id,
            planned_operation_count=3,
            executed_operation_count=0,
            error_count=0,
            summary={"conflict_count": 0, "high_risk_operation_count": 0},
        )
        route = "/jobs/{job_id}"
        request_path = f"/jobs/{job_id}"

        basic_body = self._text(
            self._route(route, "GET")(
                self._request(request_path),
                job_id=job_id,
            )
        )
        basic_visible = _visible_text(basic_body)
        self.assertNotIn(job_id, basic_visible)
        self.assertIn("Job Details", basic_visible)
        self.assertIn("COMPLETED", basic_visible)

        self.session["ui_mode"] = "advanced"
        advanced_body = self._text(
            self._route(route, "GET")(
                self._request(request_path),
                job_id=job_id,
            )
        )
        advanced_visible = _visible_text(advanced_body)
        self.assertIn(job_id, advanced_visible)
        self.assertIn("Job Details", advanced_visible)

    def test_notification_policy_stays_basic_while_technical_integrations_do_not(self):
        self._login("superadmin")
        path = "/operations-center/notifications"

        basic_body = self._text(self._route(path, "GET")(self._request(path)))
        basic_visible = _visible_text(basic_body)
        self.assertIn("Notification Policy", basic_visible)
        self.assertNotIn("API Endpoints", basic_visible)
        self.assertNotIn("Webhook Subscriptions", basic_visible)

        self.session["ui_mode"] = "advanced"
        advanced_body = self._text(self._route(path, "GET")(self._request(path)))
        advanced_visible = _visible_text(advanced_body)
        self.assertIn("Notification Policy", advanced_visible)
        self.assertIn("API Endpoints", advanced_visible)
        self.assertIn("Webhook Subscriptions", advanced_visible)

    def test_explicit_scope_bulk_selection_is_advanced_only(self):
        self._login("superadmin")
        self._seed_directory()
        path = "/sync-policies/scope"

        basic_body = self._text(self._route(path, "GET")(self._request(path)))
        basic_visible = _visible_text(basic_body)
        self.assertIn("Synchronization Boundary", basic_visible)
        self.assertNotIn("Select Current Page", basic_visible)
        self.assertNotIn("Select All Filtered Results", basic_visible)
        self.assertNotIn("Source User ID", basic_visible)
        self.assertIn('data-mode-guidance="system-recommendation"', basic_body)

        self.session["ui_mode"] = "advanced"
        advanced_body = self._text(self._route(path, "GET")(self._request(path)))
        advanced_visible = _visible_text(advanced_body)
        self.assertIn("Identity Selection", advanced_visible)
        self.assertIn("Source User ID", advanced_visible)
        self.assertIn("platform-user-internal-1", advanced_visible)

    def test_basic_data_quality_lists_never_exceed_eight_columns(self):
        self._login("superadmin")
        persist_data_quality_snapshot(
            self.app.state.db_manager,
            "default",
            created_by="mode-test",
            snapshot={
                "generated_at": "2026-07-19T10:00:00+00:00",
                "analysis_notes": [],
                "summary": {
                    "total_users": 1,
                    "users_missing_email": 0,
                    "users_missing_employee_id": 0,
                    "department_anomaly_count": 0,
                    "naming_risk_count": 0,
                    "duplicate_email_count": 0,
                    "duplicate_employee_id_count": 0,
                    "error_issue_count": 0,
                    "warning_issue_count": 0,
                },
                "connector_breakdown": [
                    {
                        "connector_id": "mode-connector-secret",
                        "name": "Regional Directory",
                        "user_count": 1,
                    }
                ],
                "issues": [],
                "repair_items": [],
                "high_risk_items": [],
            },
        )
        path = "/data-sources/data-quality"

        basic_body = self._text(self._route(path, "GET")(self._request(path)))
        basic_counts = _table_column_counts(basic_body)
        self.assertTrue(basic_counts)
        self.assertLessEqual(max(basic_counts), 8)
        self.assertNotIn("mode-connector-secret", _visible_text(basic_body))

        self.session["ui_mode"] = "advanced"
        advanced_body = self._text(self._route(path, "GET")(self._request(path)))
        advanced_counts = _table_column_counts(advanced_body)
        self.assertGreater(max(advanced_counts), 8)
        self.assertIn("mode-connector-secret", _visible_text(advanced_body))

    def test_mode_never_bypasses_permission_checks(self):
        protected_path = "/system-management/organizations"
        self._login("operator1")

        for mode in ("basic", "advanced"):
            with self.subTest(mode=mode):
                self.session["ui_mode"] = mode
                response = self._route(protected_path, "GET")(
                    self._request(protected_path)
                )
                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers["location"], "/dashboard")

    def test_mode_is_not_a_security_boundary_for_authorized_writes(self):
        self._login("superadmin")
        self.assertEqual(self.session.get("ui_mode", "basic"), "basic")
        path = "/identity-governance/exception-rules"

        response = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            rule_type="skip_user_disable",
            match_value="mode-boundary-user",
            rule_owner="iam@example.test",
            effective_reason="Authorized write remains permission controlled",
            next_review_at="",
            notes="",
            expires_at="",
            is_once=None,
        )

        self.assertEqual(response.status_code, 303)
        self.assertTrue(
            any(
                rule.match_value == "mode-boundary-user"
                for rule in self.app.state.exception_rule_repo.list_rule_records()
            )
        )

    def test_basic_high_risk_confirmation_uses_wizard_but_keeps_csrf(self):
        self._login("superadmin")

        dashboard = self._route("/dashboard", "GET")(
            self._request("/dashboard")
        )
        body = self._text(dashboard)
        self.assertIn('data-confirm-flow="wizard"', body)
        self.assertIn("data-confirm-next", body)
        self.assertIn("data-confirm-back", body)
        self.assertIn('name="csrf_token"', body)

        self.session["ui_mode"] = "advanced"
        advanced_dashboard = self._route("/dashboard", "GET")(
            self._request("/dashboard")
        )
        advanced_body = self._text(advanced_dashboard)
        self.assertIn('data-confirm-flow="confirmation"', advanced_body)
        self.assertNotIn('class="confirm-dialog__wizard-steps"', advanced_body)

        self.session["ui_mode"] = "basic"
        invalid = self._route("/ui-mode", "POST")(
            self._request("/ui-mode", "POST"),
            csrf_token="invalid",
            ui_mode="advanced",
            return_url="/dashboard",
        )
        self.assertEqual(invalid.status_code, 303)
        self.assertNotEqual(self.session.get("ui_mode"), "advanced")
