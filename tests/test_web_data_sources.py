from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class WebDataSourcesTests(WebAuthzBaseTestCase):
    def _save_base_connections(self, **overrides):
        payload = {
            "csrf_token": self.session.get("_csrf_token", ""),
            "source_provider": "wecom",
            "corpid": "corp-updated",
            "agentid": "10002",
            "corpsecret": "",
            "ldap_server": "dc02.example.local",
            "ldap_domain": "example.local",
            "ldap_username": "sync-admin",
            "ldap_password": "",
            "ldap_use_ssl": "true",
            "ldap_port": 636,
            "ldap_validate_cert": "true",
            "ldap_ca_cert_path": "",
        }
        payload.update(overrides)
        return self._route("/data-sources/connectors/base", "POST")(
            self._request("/data-sources/connectors/base", "POST"),
            **payload,
        )

    def _save_target_connection(self, **overrides):
        payload = {
            "csrf_token": self.session.get("_csrf_token", ""),
            "connector_id": "asia-ad",
            "name": "Asia AD",
            "config_path": "",
            "ldap_server": "asia-dc02.example.local",
            "ldap_domain": "asia.example.local",
            "ldap_username": "asia-sync",
            "ldap_password": "",
            "ldap_use_ssl": "true",
            "ldap_port": 636,
            "ldap_validate_cert": "true",
            "ldap_ca_cert_path": "",
            "is_enabled": "true",
        }
        payload.update(overrides)
        return self._route("/data-sources/connectors/targets", "POST")(
            self._request("/data-sources/connectors/targets", "POST"),
            **payload,
        )

    def test_connector_center_has_connection_only_responsibility_and_hides_secrets(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"

        response = self._route("/data-sources/connectors", "GET")(
            self._request("/data-sources/connectors")
        )

        self.assertEqual(response.status_code, 200)
        body = self._text(response)
        self.assertIn("Save Connection Settings", body)
        self.assertIn("Test source directory", body)
        self.assertIn("Test target AD", body)
        self.assertIn('name="connection_kind" value="source"', body)
        self.assertIn('name="connection_kind" value="ldap"', body)
        self.assertIn("Sync Policies", body)
        self.assertNotIn("Password123!", body)
        self.assertNotIn("secret-001", body)
        self.assertNotIn("Daily Schedule Time", body)
        self.assertNotIn("Brand Display Name", body)

    def test_base_connection_post_requires_csrf_preserves_blank_secrets_and_audits(self):
        self._login("superadmin")
        before = self.app.state.org_config_repo.get_raw_config(
            "default", config_path=str(self.config_path)
        )

        rejected = self._save_base_connections(csrf_token="invalid")
        self.assertEqual(rejected.status_code, 303)
        self.assertEqual(
            self.app.state.org_config_repo.get_raw_config(
                "default", config_path=str(self.config_path)
            )["ldap_server"],
            before["ldap_server"],
        )

        response = self._save_base_connections()

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/data-sources/connectors")
        saved = self.app.state.org_config_repo.get_raw_config(
            "default", config_path=str(self.config_path)
        )
        self.assertEqual(saved["corpid"], "corp-updated")
        self.assertEqual(saved["corpsecret"], "secret-001")
        self.assertEqual(saved["ldap_server"], "dc02.example.local")
        self.assertEqual(saved["ldap_password"], "Password123!")
        self.assertEqual(saved["schedule_time"], "03:00")
        log = next(
            item
            for item in self.app.state.audit_repo.list_recent_logs(20)
            if item.action_type == "data_source.base_connections_update"
        )
        self.assertEqual(log.org_id, "default")
        self.assertNotIn("Password123!", str(log.payload))
        self.assertNotIn("secret-001", str(log.payload))

    def test_target_connection_edit_preserves_policy_and_rejects_cross_org_id(self):
        self._login("superadmin")
        self.app.state.connector_repo.upsert_connector(
            connector_id="asia-ad",
            org_id="default",
            name="Asia AD",
            config_path="",
            ldap_server="asia-dc01.example.local",
            ldap_domain="asia.example.local",
            ldap_username="old-sync",
            ldap_password="ConnectorSecret!",
            ldap_use_ssl=True,
            ldap_port=636,
            ldap_validate_cert=True,
            ldap_ca_cert_path="",
            default_password="CreationSecret!",
            force_change_password=True,
            password_complexity="strong",
            root_department_ids=[10, 20],
            username_strategy="employee_id",
            username_collision_policy="append_hash",
            username_collision_template="-{hash}",
            username_template="{employee_id}",
            disabled_users_ou="Disabled/Asia",
            group_type="distribution",
            group_mail_domain="groups.example.local",
            custom_group_ou_path="Groups/Asia",
            managed_tag_ids=["tag-a"],
            managed_external_chat_ids=["chat-a"],
            is_enabled=True,
        )

        response = self._save_target_connection()

        self.assertEqual(response.status_code, 303)
        saved = self.app.state.connector_repo.get_connector_record(
            "asia-ad", org_id="default"
        )
        self.assertEqual(saved.ldap_server, "asia-dc02.example.local")
        self.assertEqual(saved.ldap_password, "ConnectorSecret!")
        self.assertEqual(saved.default_password, "CreationSecret!")
        self.assertEqual(saved.root_department_ids, [10, 20])
        self.assertEqual(saved.username_strategy, "employee_id")
        self.assertEqual(saved.username_collision_policy, "append_hash")
        self.assertEqual(saved.disabled_users_ou, "Disabled/Asia")
        self.assertEqual(saved.managed_tag_ids, ["tag-a"])

        self.app.state.organization_repo.upsert_organization(
            org_id="other",
            name="Other Organization",
            config_path=str(self.config_path),
            description="",
            is_enabled=True,
        )
        self.session["selected_org_id"] = "other"
        blocked = self._save_target_connection(ldap_server="other-dc.example.local")

        self.assertEqual(blocked.status_code, 303)
        self.assertEqual(
            self.app.state.connector_repo.get_connector_record(
                "asia-ad", org_id="default"
            ).ldap_server,
            "asia-dc02.example.local",
        )
        self.assertIsNone(
            self.app.state.connector_repo.get_connector_record(
                "asia-ad", org_id="other"
            )
        )
        self.assertIn("another organization", str(self.session["_flash"]))

    def test_operator_cannot_write_connectors_or_run_data_quality_scan(self):
        self._login("operator1")

        connector_response = self._save_base_connections()
        quality_response = self._route("/data-sources/data-quality/run", "POST")(
            self._request("/data-sources/data-quality/run", "POST"),
            csrf_token=self.session["_csrf_token"],
        )

        self.assertEqual(connector_response.status_code, 303)
        self.assertEqual(connector_response.headers["location"], "/dashboard")
        self.assertEqual(quality_response.status_code, 303)
        self.assertEqual(quality_response.headers["location"], "/dashboard")

    def test_snapshot_history_view_is_six_columns_and_organization_scoped(self):
        self._login("superadmin")
        first = self.app.state.source_directory_repo.start_refresh(
            org_id="default", provider_id="wecom", created_by="superadmin"
        )
        self.app.state.source_directory_repo.replace_snapshot(
            first,
            departments=[],
            users=[],
            fields=[],
            fingerprint="default-snapshot",
        )
        other = self.app.state.source_directory_repo.start_refresh(
            org_id="other", provider_id="wecom", created_by="other-admin"
        )
        self.app.state.source_directory_repo.fail_refresh(
            other, "private other-organization failure"
        )

        response = self._route("/data-sources/snapshots", "GET")(
            self._request("/data-sources/snapshots"),
            snapshot_id=other,
        )

        self.assertEqual(response.status_code, 200)
        body = self._text(response)
        self.assertIn("Snapshot History", body)
        self.assertIn(f"#{first}", body)
        self.assertNotIn(f"#{other}", body)
        self.assertNotIn("private other-organization failure", body)
        self.assertEqual(body.count("<th>"), 6)
        self.assertIn("Quality Change", body)
        self.assertIn("Refresh Duration", body)
        self.assertIn("Failure Reason", body)
        self.assertIn('aria-current="page">Snapshot History', body)


if __name__ == "__main__":
    import unittest

    unittest.main()
