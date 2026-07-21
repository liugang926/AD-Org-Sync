import re
from unittest.mock import patch

from sync_app.core.models import DirectoryUserRecord
from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class _IdentityWorkbenchTargetProvider:
    def __init__(self, existing_usernames=()):
        self.existing_usernames = {
            str(username or "").strip().casefold()
            for username in existing_usernames
            if str(username or "").strip()
        }

    def get_users_batch(self, usernames):
        return {
            username: DirectoryUserRecord(
                username=username,
                dn="redacted",
                raw_entry={
                    "attributes": {
                        "userAccountControl": 512,
                        "lockoutTime": 0,
                    }
                },
            )
            for username in usernames
            if str(username or "").strip().casefold() in self.existing_usernames
        }

    def close(self):
        return None


class WebIdentityWorkbenchTests(WebAuthzBaseTestCase):
    def _seed_users(self, *, total=55, active_count=52):
        snapshot_id = self.app.state.source_directory_repo.start_refresh(
            org_id="default",
            provider_id="wecom",
            created_by="superadmin",
        )
        users = []
        for index in range(1, total + 1):
            is_active = index <= active_count
            department_id = "1" if is_active else "2"
            department_name = "Headquarters" if is_active else "Former Employees"
            users.append(
                {
                    "source_user_id": f"user-{index:03d}",
                    "display_name": f"Identity User {index:03d}",
                    "employee_id": f"WB{index:03d}",
                    "department_ids": [department_id],
                    "department_names": [department_name],
                    "is_active": is_active,
                    "account_status": "active" if is_active else "inactive",
                    "raw_payload": {
                        "userid": f"user-{index:03d}",
                        "employee_id": f"WB{index:03d}",
                    },
                    "search_text": (
                        f"Identity User {index:03d} user-{index:03d} WB{index:03d}"
                    ),
                }
            )
        self.app.state.source_directory_repo.replace_snapshot(
            snapshot_id,
            departments=[
                {
                    "source_department_id": "1",
                    "name": "Headquarters",
                    "parent_department_id": "0",
                    "path_ids": ["1"],
                    "path_names": ["Headquarters"],
                },
                {
                    "source_department_id": "2",
                    "name": "Former Employees",
                    "parent_department_id": "0",
                    "path_ids": ["2"],
                    "path_names": ["Former Employees"],
                },
            ],
            users=users,
            fields=[
                {
                    "name": "employee_id",
                    "label": "Employee ID",
                    "coverage": total,
                    "samples": ["WB001", "WB002"],
                }
            ],
            fingerprint=f"identity-workbench-{total}-{active_count}",
        )
        self.app.state.source_directory_repo.save_scope_selection(
            org_id="default",
            provider_id="wecom",
            scope_type="full",
            username_strategy="employee_id",
            source_field="employee_id",
            snapshot_id=snapshot_id,
            requested_by="superadmin",
        )
        return snapshot_id

    @staticmethod
    def _table(body):
        match = re.search(
            r"<table data-identity-matching-table>(.*?)</table>",
            body,
            re.S,
        )
        if match is None:
            raise AssertionError("identity workbench table was not rendered")
        return match.group(1)

    @staticmethod
    def _queue_count(body, queue):
        match = re.search(
            rf'data-identity-queue="{re.escape(queue)}"[^>]*>.*?<strong>(\d+)</strong>',
            body,
            re.S,
        )
        if match is None:
            raise AssertionError(f"queue tab was not rendered: {queue}")
        return int(match.group(1))

    def test_filters_counts_and_server_pagination_share_one_result_set(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        self._seed_users()
        path = "/identity-governance/identity-matching"
        query = {
            "page_number": "2",
            "search": "Identity User",
            "queue": "all",
            "department_id": "1",
            "employee_status": "active",
            "identity_status": "ad_status_unknown",
            "ad_status": "unknown",
            "mode": "basic",
        }

        response = self._route(path, "GET")(
            self._request(path, query=query),
            page_number=2,
            search="Identity User",
            queue="all",
            department_id="1",
            employee_status="active",
            identity_status="ad_status_unknown",
            ad_status="unknown",
            mode="basic",
        )

        self.assertEqual(response.status_code, 200)
        body = self._text(response)
        table = self._table(body)
        self.assertEqual(table.count("<th>"), 8)
        self.assertEqual(table.count("data-identity-row="), 2)
        self.assertIn("user-051", table)
        self.assertIn("user-052", table)
        self.assertNotIn("user-050", table)
        self.assertIn("52 matching users", body)
        self.assertIn("2 / 2", body)
        self.assertEqual(body.count("data-active-filter="), 5)
        self.assertEqual(self._queue_count(body, "pending"), 52)
        self.assertEqual(self._queue_count(body, "unbound"), 52)
        self.assertEqual(self._queue_count(body, "all"), 52)
        self.assertEqual(self._queue_count(body, "bound"), 0)
        self.assertNotIn("Evidence fingerprint", table)
        self.assertNotIn("Mapping field", table)

        advanced = self._route(path, "GET")(
            self._request(path, query={"queue": "all", "mode": "advanced"}),
            queue="all",
            mode="advanced",
        )
        advanced_table = self._table(self._text(advanced))
        self.assertIn("advanced-evidence", advanced_table)
        self.assertIn("employee_id", advanced_table)

    def test_live_identity_and_ad_filters_return_the_business_conclusion(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        self._seed_users(total=2, active_count=2)
        self.app.state.user_binding_repo.upsert_binding(
            "user-001",
            "legacy.user",
            org_id="default",
            source_provider="wecom",
            connector_id="default",
            source="manual",
            source_display_name="Identity User 001",
        )
        path = "/identity-governance/identity-matching"

        with patch(
            "sync_app.web.app.build_target_provider",
            return_value=_IdentityWorkbenchTargetProvider({"WB002"}),
        ):
            response = self._route(path, "GET")(
                self._request(
                    path,
                    query={
                        "queue": "all",
                        "identity_status": "saved_binding_expired",
                        "ad_status": "missing",
                        "verify_ad": "true",
                    },
                ),
                queue="all",
                identity_status="saved_binding_expired",
                ad_status="missing",
                verify_ad=True,
            )

        body = self._text(response)
        table = self._table(body)
        self.assertEqual(table.count("data-identity-row="), 1)
        self.assertIn("user-001", table)
        self.assertNotIn("user-002", table)
        self.assertIn("Saved binding has expired", table)
        self.assertEqual(body.count("data-active-filter="), 2)
        self.assertEqual(self._queue_count(body, "all"), 1)
        self.assertEqual(self._queue_count(body, "bound"), 1)
        self.assertEqual(self._queue_count(body, "unbound"), 0)

    def test_defer_requires_write_permission_csrf_and_current_snapshot_membership(self):
        snapshot_id = self._seed_users(total=2, active_count=2)
        path = "/identity-governance/identity-matching/defer"

        self._login("operator1")
        blocked = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            selected_source_user_ids=["user-001"],
            return_query="queue=pending",
        )
        self.assertEqual(blocked.status_code, 303)
        self.assertEqual(blocked.headers["location"], "/dashboard")
        self.assertNotIn("_identity_workbench_deferred", self.session)

        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        rejected_csrf = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token="wrong",
            selected_source_user_ids=["user-001"],
            return_query="queue=pending",
        )
        self.assertEqual(rejected_csrf.status_code, 303)
        self.assertNotIn("_identity_workbench_deferred", self.session)

        rejected_scope = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            selected_source_user_ids=["other-org-user"],
            return_query="queue=pending",
        )
        self.assertEqual(rejected_scope.status_code, 303)
        self.assertNotIn("_identity_workbench_deferred", self.session)

        saved = self._route(path, "POST")(
            self._request(path, "POST"),
            csrf_token=self.session["_csrf_token"],
            selected_source_user_ids=["user-001"],
            return_query="queue=pending&mode=basic&external=https%3A%2F%2Fexample.invalid",
        )
        self.assertEqual(saved.status_code, 303)
        self.assertEqual(
            saved.headers["location"],
            "/identity-governance/identity-matching?queue=pending&mode=basic",
        )
        deferred = self.session["_identity_workbench_deferred"]
        self.assertEqual(deferred["org_id"], "default")
        self.assertEqual(deferred["provider_id"], "wecom")
        self.assertEqual(deferred["snapshot_id"], snapshot_id)
        self.assertEqual(deferred["source_user_ids"], ["user-001"])
        audit = next(
            row
            for row in self.app.state.audit_repo.list_recent_logs(20)
            if row.action_type == "identity_workbench.defer"
        )
        self.assertEqual(audit.org_id, "default")
        self.assertEqual(audit.payload["source_user_ids"], ["user-001"])

        page_endpoint = self._route(
            "/identity-governance/identity-matching",
            "GET",
        )
        pending = page_endpoint(
            self._request("/identity-governance/identity-matching"),
            queue="pending",
        )
        pending_table = self._table(self._text(pending))
        self.assertNotIn("user-001", pending_table)
        self.assertIn("user-002", pending_table)

        all_rows = page_endpoint(
            self._request("/identity-governance/identity-matching"),
            queue="all",
        )
        all_body = self._text(all_rows)
        self.assertIn("user-001", self._table(all_body))
        self.assertIn("Temporarily deferred", all_body)
        self.assertIn(
            "Selected source identities were temporarily deferred",
            all_body,
        )

    def test_drawer_has_seven_scoped_evidence_stages_and_deep_links(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"
        self._seed_users(total=1, active_count=1)
        self.app.state.job_repo.create_job(
            "workbench-conflict-default",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="default",
        )
        self.app.state.conflict_repo.add_conflict(
            job_id="workbench-conflict-default",
            conflict_type="multiple_ad_candidates",
            source_id="user-001",
            message="default organization identity conflict",
        )
        self.app.state.audit_repo.add_log(
            org_id="default",
            actor_username="superadmin",
            action_type="identity.review",
            target_type="source_user",
            target_id="user-001",
            result="success",
            message="default organization identity audit",
        )
        self.app.state.job_repo.create_job(
            "workbench-conflict-other",
            trigger_type="unit_test",
            execution_mode="dry_run",
            status="COMPLETED",
            org_id="other-org",
        )
        self.app.state.conflict_repo.add_conflict(
            job_id="workbench-conflict-other",
            conflict_type="multiple_ad_candidates",
            source_id="user-001",
            message="other organization secret conflict",
        )
        self.app.state.audit_repo.add_log(
            org_id="other-org",
            actor_username="other-admin",
            action_type="identity.review",
            target_type="source_user",
            target_id="user-001",
            result="success",
            message="other organization secret audit",
        )
        path = "/identity-governance/identity-matching"

        response = self._route(path, "GET")(
            self._request(path, query={"queue": "conflict"}),
            queue="conflict",
        )

        body = self._text(response)
        table = self._table(body)
        self.assertEqual(table.count("<th>"), 8)
        self.assertEqual(table.count("data-identity-row="), 1)
        self.assertEqual(body.count("data-identity-timeline-step"), 7)
        for label in (
            "Source fields and Candidate calculation",
            "Before: Saved binding",
            "Latest verified AD status",
            "Planned: Latest Dry Run",
            "Applied: Latest Apply",
            "Current AD actual state",
            "Risks, conflicts, and audit records",
        ):
            self.assertIn(label, body)
        self.assertIn("default organization identity conflict", body)
        self.assertIn("default organization identity audit", body)
        self.assertNotIn("other organization secret conflict", body)
        self.assertNotIn("other organization secret audit", body)
        self.assertEqual(self._queue_count(body, "conflict"), 1)
        self.assertIn("Evidence fingerprint", body)
        self.assertNotIn("Evidence fingerprint", table)
        for href in (
            "/execution-center/jobs",
            "/identity-governance/manual-overrides",
            "/identity-governance/conflicts",
        ):
            self.assertIn(f'href="{href}', body)

        localized = self._route(path, "GET")(
            self._request(
                path,
                query={"queue": "conflict", "lang": "zh-CN"},
            ),
            queue="conflict",
        )
        localized_body = self._text(localized)
        for label in (
            "身份匹配工作台",
            "待处理",
            "可创建",
            "未绑定",
            "已绑定",
            "冲突",
            "全部",
            "身份关系存在冲突，需要复核",
            "来源字段和候选计算",
            "风险、冲突和审计记录",
        ):
            self.assertIn(label, localized_body)
