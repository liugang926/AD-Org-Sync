from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

from sync_app.web.security import hash_password
from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class WebAccountTakeoverTests(WebAuthzBaseTestCase):
    def _csrf(self, body: str) -> str:
        match = re.search(r'name="csrf_token" value="([^"]+)"', body)
        self.assertIsNotNone(match)
        return str(match.group(1))

    def test_takeover_web_flow_separates_submit_review_and_execute(self) -> None:
        self.app.state.user_repo.create_user(
            "reviewer1", hash_password("Admin123!"), role="mapping_reviewer"
        )
        self.app.state.user_repo.create_user(
            "executor1", hash_password("Admin123!"), role="sync_executor"
        )
        self.app.state.platform_account_repo.upsert_account(
            org_id="default",
            provider_id="dingtalk",
            connector_id="default",
            platform_account_id="ding-alice",
            display_name="Alice",
            employee_id="E001",
        )
        self.app.state.ad_account_repo.upsert_account(
            org_id="default",
            connector_id="default",
            object_guid="guid-alice",
            sam_account_name="alice",
            employee_id="E001",
        )

        self._login("superadmin")
        page_path = "/identity-governance/manual-overrides"
        page = self._route(page_path, "GET")(self._request(page_path))
        preview = self._route(
            "/identity-governance/manual-overrides/takeover/preview", "POST"
        )(
            self._request(
                "/identity-governance/manual-overrides/takeover/preview", "POST"
            ),
            csrf_token=self._csrf(self._text(page)),
            takeover_csv=(
                "provider_id,connector_id,platform_account_id,ad_account_key\n"
                "dingtalk,default,ding-alice,guid-alice\n"
            ),
            original_filename="reviewed.csv",
        )
        batch_id = parse_qs(urlparse(preview.headers["location"]).query)[
            "takeover_batch"
        ][0]
        batch = self.app.state.account_takeover_repo.get_batch(
            batch_id, org_id="default"
        )
        self.assertIsNotNone(batch)

        self._login("reviewer1")
        review_page = self._route(page_path, "GET")(
            self._request(page_path, query={"takeover_batch": batch_id})
        )
        approved = self._route(
            "/identity-governance/manual-overrides/takeover/{batch_id}/approve",
            "POST",
        )(
            self._request(
                f"/identity-governance/manual-overrides/takeover/{batch_id}/approve",
                "POST",
            ),
            batch_id=batch_id,
            csrf_token=self._csrf(self._text(review_page)),
            preview_fingerprint=batch["preview_fingerprint"],
        )
        self.assertEqual(approved.status_code, 303)

        self._login("executor1")
        apply_page = self._route(page_path, "GET")(
            self._request(page_path, query={"takeover_batch": batch_id})
        )
        applied = self._route(
            "/identity-governance/manual-overrides/takeover/{batch_id}/apply",
            "POST",
        )(
            self._request(
                f"/identity-governance/manual-overrides/takeover/{batch_id}/apply",
                "POST",
            ),
            batch_id=batch_id,
            csrf_token=self._csrf(self._text(apply_page)),
            preview_fingerprint=batch["preview_fingerprint"],
        )

        self.assertEqual(applied.status_code, 303)
        self.assertEqual(
            self.app.state.account_takeover_repo.get_batch(
                batch_id, org_id="default"
            )["status"],
            "applied",
        )
        self.assertEqual(
            len(self.app.state.enterprise_identity_repo.list_links(org_id="default")),
            2,
        )
        audit_actions = {
            item.action_type for item in self.app.state.audit_repo.list_recent_logs(20)
        }
        self.assertTrue(
            {
                "account_takeover.preview",
                "account_takeover.approve",
                "account_takeover.apply",
            }.issubset(audit_actions)
        )

    def test_rejected_takeover_approval_is_audited(self) -> None:
        self.app.state.platform_account_repo.upsert_account(
            org_id="default",
            provider_id="dingtalk",
            connector_id="default",
            platform_account_id="ding-alice",
            display_name="Alice",
            employee_id="E001",
        )
        self.app.state.ad_account_repo.upsert_account(
            org_id="default",
            connector_id="default",
            object_guid="guid-alice",
            sam_account_name="alice",
            employee_id="E001",
        )
        self._login("superadmin")
        page_path = "/identity-governance/manual-overrides"
        page = self._route(page_path, "GET")(self._request(page_path))
        preview = self._route(
            "/identity-governance/manual-overrides/takeover/preview", "POST"
        )(
            self._request(
                "/identity-governance/manual-overrides/takeover/preview", "POST"
            ),
            csrf_token=self._csrf(self._text(page)),
            takeover_csv=(
                "provider_id,connector_id,platform_account_id,ad_account_key\n"
                "dingtalk,default,ding-alice,guid-alice\n"
            ),
            original_filename="reviewed.csv",
        )
        batch_id = parse_qs(urlparse(preview.headers["location"]).query)[
            "takeover_batch"
        ][0]
        batch = self.app.state.account_takeover_repo.get_batch(
            batch_id, org_id="default"
        )
        review_page = self._route(page_path, "GET")(
            self._request(page_path, query={"takeover_batch": batch_id})
        )

        rejected = self._route(
            "/identity-governance/manual-overrides/takeover/{batch_id}/approve",
            "POST",
        )(
            self._request(
                f"/identity-governance/manual-overrides/takeover/{batch_id}/approve",
                "POST",
            ),
            batch_id=batch_id,
            csrf_token=self._csrf(self._text(review_page)),
            preview_fingerprint=batch["preview_fingerprint"],
        )

        self.assertEqual(rejected.status_code, 303)
        self.assertEqual(
            self.app.state.account_takeover_repo.get_batch(
                batch_id, org_id="default"
            )["status"],
            "ready",
        )
        audit = next(
            item
            for item in self.app.state.audit_repo.list_recent_logs(20)
            if item.action_type == "account_takeover.approve"
        )
        self.assertEqual(audit.result, "error")
        self.assertIn("different users", str(audit.payload.get("error") or ""))
