import os
import unittest
from datetime import datetime, timezone
from pathlib import Path

from sync_app.core.models import AccountConfig, AppConfig, LDAPConfig, WeComConfig
from sync_app.storage.local_db import (
    DatabaseManager,
    OrganizationConfigRepository,
    SettingsRepository,
    SyncConnectorRepository,
    UserDepartmentOverrideRepository,
    UserIdentityBindingRepository,
    WebAdminUserRepository,
    WebAuditLogRepository,
)
from sync_app.services.config_store import save_editable_config
from sync_app.storage.secret_store import can_use_dpapi, is_encrypted_secret
from sync_app.web.security import hash_password, verify_password


class WebStorageTests(unittest.TestCase):
    def test_web_admin_password_min_length_defaults_to_eight_and_upgrades_legacy_default(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        db_path = test_root / "web_storage_settings.db"
        try:
            if db_path.exists():
                db_path.unlink()
            manager = DatabaseManager(db_path=str(db_path))
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)

            settings_repo = SettingsRepository(manager)
            self.assertEqual(settings_repo.get_int("web_admin_password_min_length", 0), 8)
            self.assertEqual(
                settings_repo.get_value("user_ou_placement_strategy", ""),
                "source_primary_department",
            )

            settings_repo.set_value("web_admin_password_min_length", "12", "int")
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)

            self.assertEqual(settings_repo.get_int("web_admin_password_min_length", 0), 8)
        finally:
            backup_dir = db_path.parent / "backups"
            for suffix in ("", "-wal", "-shm"):
                candidate = Path(str(db_path) + suffix)
                if candidate.exists():
                    try:
                        candidate.unlink()
                    except PermissionError:
                        pass
            if backup_dir.exists():
                for item in backup_dir.glob("*"):
                    try:
                        item.unlink()
                    except PermissionError:
                        pass

    def test_admin_user_and_audit_log_roundtrip(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        db_path = test_root / "web_storage_roundtrip.db"
        try:
            if db_path.exists():
                db_path.unlink()
            manager = DatabaseManager(db_path=str(db_path))
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)

            user_repo = WebAdminUserRepository(manager)
            audit_repo = WebAuditLogRepository(manager)
            binding_repo = UserIdentityBindingRepository(manager)
            override_repo = UserDepartmentOverrideRepository(manager)

            self.assertFalse(user_repo.has_any_user())
            user_id = user_repo.create_user("admin", hash_password("Admin123!"))
            self.assertGreater(user_id, 0)
            self.assertTrue(user_repo.has_any_user())

            user = user_repo.get_user_record_by_username("admin")
            self.assertIsNotNone(user)
            self.assertTrue(verify_password("Admin123!", user.password_hash))

            audit_repo.add_log(
                actor_username="admin",
                action_type="auth.login",
                target_type="web_admin_user",
                target_id="admin",
                result="success",
                message="登录成功",
                payload={"ip": "127.0.0.1"},
            )
            logs = audit_repo.list_recent_logs(limit=10)
            self.assertEqual(len(logs), 1)
            self.assertEqual(logs[0].action_type, "auth.login")
            self.assertEqual(logs[0].payload["ip"], "127.0.0.1")

            binding_repo.upsert_binding_for_source_user(
                "alice",
                "alice.ad",
                source="manual",
                notes="manual bind",
                preserve_manual=False,
            )
            binding = binding_repo.get_binding_record_by_source_user_id("alice")
            self.assertIsNotNone(binding)
            self.assertEqual(binding.ad_username, "alice.ad")
            self.assertEqual(binding.source, "manual")
            self.assertEqual(binding.source_user_id, "alice")
            self.assertEqual(binding_repo.get_binding_record_by_wecom_userid("alice").ad_username, "alice.ad")
            self.assertEqual(binding.to_dict()["source_user_id"], "alice")
            self.assertEqual(
                binding_repo.get_binding_record_by_source_user_id("alice").ad_username,
                "alice.ad",
            )
            binding_repo.set_enabled_for_source_user("alice", False)
            self.assertFalse(binding_repo.get_binding_record_by_source_user_id("alice").is_enabled)

            override_repo.upsert_override_for_source_user("alice", "2001", notes="main dept")
            override = override_repo.get_override_record_by_source_user_id("alice")
            self.assertIsNotNone(override)
            self.assertEqual(override.primary_department_id, "2001")
            self.assertEqual(override.source_user_id, "alice")
            self.assertEqual(
                override_repo.get_override_record_by_wecom_userid("alice").primary_department_id,
                "2001",
            )
            self.assertEqual(override.to_dict()["source_user_id"], "alice")
            self.assertEqual(
                override_repo.get_override_record_by_source_user_id("alice").primary_department_id,
                "2001",
            )
            override_repo.upsert_override_for_source_user("alice", "3001", notes="updated dept")
            self.assertEqual(
                override_repo.get_override_record_by_source_user_id("alice").primary_department_id,
                "3001",
            )
            override_repo.delete_override_for_source_user("alice")
            self.assertIsNone(override_repo.get_override_record_by_source_user_id("alice"))
        finally:
            backup_dir = db_path.parent / "backups"
            if db_path.exists():
                try:
                    db_path.unlink()
                except PermissionError:
                    pass
            if backup_dir.exists():
                for item in backup_dir.glob("*"):
                    try:
                        item.unlink()
                    except PermissionError:
                        pass

    def test_organization_config_secrets_are_protected_in_storage(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        db_path = test_root / "web_storage_org_secret.db"
        try:
            if db_path.exists():
                db_path.unlink()
            manager = DatabaseManager(db_path=str(db_path))
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)

            repo = OrganizationConfigRepository(manager)
            repo.save_config(
                "default",
                {
                    "source_provider": "wecom",
                    "corpid": "corp-id",
                    "agentid": "1001",
                    "corpsecret": "source-secret",
                    "webhook_url": "https://example.invalid/cgi-bin/webhook/send?key=test",
                    "ldap_server": "dc.example.com",
                    "ldap_domain": "example.com",
                    "ldap_username": "svc",
                    "ldap_password": "ldap-secret",
                    "default_password": "simple888",
                },
                config_path="config.ini",
            )

            with manager._connect() as conn:
                stored = conn.execute(
                    "SELECT value FROM app_settings WHERE key = ?",
                    ("orgcfg:default:corpsecret",),
                ).fetchone()
            self.assertIsNotNone(stored)
            stored_value = str(stored["value"] or "")
            self.assertNotEqual(stored_value, "source-secret")
            if can_use_dpapi():
                self.assertTrue(is_encrypted_secret(stored_value))

            config = repo.get_app_config("default", config_path="config.ini")
            self.assertEqual(config.source_connector.corpsecret, "source-secret")
            self.assertEqual(config.ldap.password, "ldap-secret")
            self.assertEqual(config.account.default_password, "simple888")
        finally:
            backup_dir = db_path.parent / "backups"
            for suffix in ("", "-wal", "-shm"):
                candidate = Path(str(db_path) + suffix)
                if candidate.exists():
                    try:
                        candidate.unlink()
                    except PermissionError:
                        pass
            if backup_dir.exists():
                for item in backup_dir.glob("*"):
                    try:
                        item.unlink()
                    except PermissionError:
                        pass

    def test_connector_override_secrets_are_protected_in_storage(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        db_path = test_root / "web_storage_connector_secret.db"
        try:
            if db_path.exists():
                db_path.unlink()
            manager = DatabaseManager(db_path=str(db_path))
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)

            repo = SyncConnectorRepository(manager)
            repo.upsert_connector(
                connector_id="hq",
                org_id="default",
                name="HQ",
                config_path="connector.ini",
                ldap_server="dc.example.com",
                ldap_domain="example.com",
                ldap_username="svc-hq",
                ldap_password="hq-secret",
                default_password="simple888",
            )

            with manager._connect() as conn:
                stored = conn.execute(
                    "SELECT ldap_password, default_password FROM sync_connectors WHERE connector_id = ?",
                    ("hq",),
                ).fetchone()
            self.assertIsNotNone(stored)
            self.assertNotEqual(str(stored["ldap_password"] or ""), "hq-secret")
            if can_use_dpapi():
                self.assertTrue(is_encrypted_secret(str(stored["ldap_password"] or "")))

            record = repo.get_connector_record("hq", org_id="default")
            self.assertIsNotNone(record)
            self.assertEqual(record.ldap_password, "hq-secret")
            self.assertEqual(record.default_password, "simple888")
        finally:
            backup_dir = db_path.parent / "backups"
            for suffix in ("", "-wal", "-shm"):
                candidate = Path(str(db_path) + suffix)
                if candidate.exists():
                    try:
                        candidate.unlink()
                    except PermissionError:
                        pass
            if backup_dir.exists():
                for item in backup_dir.glob("*"):
                    try:
                        item.unlink()
                    except PermissionError:
                        pass

    def test_cleanup_history_prunes_expired_jobs_and_events(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        db_path = test_root / "web_storage_cleanup.db"
        try:
            if db_path.exists():
                db_path.unlink()
            manager = DatabaseManager(db_path=str(db_path))
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)

            old_timestamp = "2000-01-01T00:00:00+00:00"
            current_timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
            with manager.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO sync_jobs (
                      job_id, trigger_type, execution_mode, status, started_at, ended_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("job-old", "unit_test", "dry_run", "COMPLETED", old_timestamp, old_timestamp),
                )
                conn.execute(
                    """
                    INSERT INTO sync_jobs (
                      job_id, trigger_type, execution_mode, status, started_at, ended_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("job-current", "unit_test", "dry_run", "COMPLETED", current_timestamp, current_timestamp),
                )
                conn.execute(
                    """
                    INSERT INTO sync_events (job_id, stage_name, level, event_type, message, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("job-old", "plan", "INFO", "job_event", "old job event", old_timestamp),
                )
                conn.execute(
                    """
                    INSERT INTO sync_events (job_id, stage_name, level, event_type, message, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("job-current", "plan", "INFO", "job_event", "stale event on current job", old_timestamp),
                )
                conn.execute(
                    """
                    INSERT INTO planned_operations (
                      job_id, object_type, operation_type, risk_level, status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("job-old", "user", "create_user", "normal", "planned", old_timestamp),
                )
                conn.execute(
                    """
                    INSERT INTO sync_operation_logs (
                      job_id, stage_name, object_type, operation_type, risk_level, status, message, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("job-old", "apply", "user", "create_user", "normal", "success", "created", old_timestamp),
                )
                conn.execute(
                    """
                    INSERT INTO sync_conflicts (
                      job_id, conflict_type, severity, status, source_id, target_key, message, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("job-old", "multiple_ad_candidates", "warning", "open", "alice", "identity_binding", "conflict", old_timestamp),
                )
                conn.execute(
                    """
                    INSERT INTO sync_plan_reviews (
                      job_id, plan_fingerprint, config_snapshot_hash, high_risk_operation_count, status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("job-old", "fingerprint", "config-hash", 1, "pending", old_timestamp),
                )
                conn.execute(
                    """
                    INSERT INTO web_audit_logs (
                      actor_username, action_type, target_type, target_id, result, message, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("admin", "job.run", "sync_job", "job-old", "success", "old audit log", old_timestamp),
                )
                conn.execute(
                    """
                    INSERT INTO web_audit_logs (
                      actor_username, action_type, target_type, target_id, result, message, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("admin", "job.run", "sync_job", "job-current", "success", "current audit log", current_timestamp),
                )

            result = manager.cleanup_history(
                job_retention_days=30,
                event_retention_days=30,
                audit_log_retention_days=30,
            )

            self.assertEqual(result["deleted_jobs"], 1)
            self.assertEqual(result["deleted_planned_operations"], 1)
            self.assertEqual(result["deleted_operation_logs"], 1)
            self.assertEqual(result["deleted_conflicts"], 1)
            self.assertEqual(result["deleted_review_requests"], 1)
            self.assertEqual(result["deleted_events"], 2)
            self.assertEqual(result["deleted_audit_logs"], 1)

            with manager.connection() as conn:
                remaining_jobs = conn.execute("SELECT job_id FROM sync_jobs ORDER BY job_id").fetchall()
                remaining_events = conn.execute("SELECT id FROM sync_events").fetchall()
                remaining_audit_logs = conn.execute("SELECT message FROM web_audit_logs").fetchall()
            self.assertEqual([row["job_id"] for row in remaining_jobs], ["job-current"])
            self.assertEqual(len(remaining_events), 0)
            self.assertEqual([row["message"] for row in remaining_audit_logs], ["current audit log"])
        finally:
            backup_dir = db_path.parent / "backups"
            if db_path.exists():
                try:
                    db_path.unlink()
                except PermissionError:
                    pass
            if backup_dir.exists():
                for item in backup_dir.glob("*"):
                    try:
                        item.unlink()
                    except PermissionError:
                        pass

    def test_audit_log_pagination_and_backup_cleanup(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        db_path = test_root / "web_storage_backups.db"
        try:
            if db_path.exists():
                db_path.unlink()
            manager = DatabaseManager(db_path=str(db_path))
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)

            audit_repo = WebAuditLogRepository(manager)
            for index in range(4):
                audit_repo.add_log(
                    actor_username="admin",
                    action_type="database.backup",
                    target_type="sqlite",
                    target_id="app.db",
                    result="success",
                    message=f"backup-message-{index}",
                )

            page_logs, total = audit_repo.list_recent_logs_page(limit=2, offset=2, query="backup-message")
            self.assertEqual(total, 4)
            self.assertEqual(len(page_logs), 2)
            self.assertEqual(page_logs[0].message, "backup-message-1")
            self.assertEqual(page_logs[1].message, "backup-message-0")

            backup_dir = Path(manager.backup_dir)
            backup_dir.mkdir(parents=True, exist_ok=True)
            now_ts = datetime.now(timezone.utc).timestamp()
            backup_specs = [
                ("backup-0.db", now_ts - 40 * 86400),
                ("backup-1.db", now_ts - 10 * 86400),
                ("backup-2.db", now_ts - 5 * 86400),
                ("backup-3.db", now_ts - 1 * 86400),
            ]
            for filename, modified_ts in backup_specs:
                path = backup_dir / filename
                path.write_text("test", encoding="utf-8")
                os.utime(path, (modified_ts, modified_ts))

            cleanup_result = manager.cleanup_backups(retention_days=30, max_files=2)
            self.assertEqual(cleanup_result["deleted_backups"], 2)
            self.assertEqual(cleanup_result["kept_backups"], 2)
            remaining_files = sorted(path.name for path in backup_dir.glob("*.db"))
            self.assertEqual(remaining_files, ["backup-2.db", "backup-3.db"])
        finally:
            backup_dir = db_path.parent / "backups"
            if db_path.exists():
                try:
                    db_path.unlink()
                except PermissionError:
                    pass
            if backup_dir.exists():
                for item in backup_dir.glob("*"):
                    try:
                        item.unlink()
                    except PermissionError:
                        pass

    def test_audit_log_org_filter_includes_global_entries(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        db_path = test_root / "web_storage_audit_org_scope.db"
        try:
            if db_path.exists():
                db_path.unlink()
            manager = DatabaseManager(db_path=str(db_path))
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)

            audit_repo = WebAuditLogRepository(manager)
            audit_repo.add_log(
                actor_username="admin",
                action_type="auth.login",
                target_type="web_admin_user",
                target_id="admin",
                result="success",
                message="global-entry",
            )
            audit_repo.add_log(
                org_id="asia",
                actor_username="admin",
                action_type="config.update",
                target_type="config_file",
                target_id="asia.ini",
                result="success",
                message="asia-entry",
            )
            audit_repo.add_log(
                org_id="europe",
                actor_username="admin",
                action_type="config.update",
                target_type="config_file",
                target_id="europe.ini",
                result="success",
                message="europe-entry",
            )

            asia_logs, asia_total = audit_repo.list_recent_logs_page(limit=10, offset=0, org_id="asia")
            self.assertEqual(asia_total, 2)
            self.assertEqual([record.message for record in asia_logs], ["asia-entry", "global-entry"])
            self.assertEqual([record.org_id for record in asia_logs], ["asia", ""])

            asia_only_logs, asia_only_total = audit_repo.list_recent_logs_page(
                limit=10,
                offset=0,
                org_id="asia",
                include_global=False,
            )
            self.assertEqual(asia_only_total, 1)
            self.assertEqual([record.message for record in asia_only_logs], ["asia-entry"])
        finally:
            backup_dir = db_path.parent / "backups"
            if db_path.exists():
                try:
                    db_path.unlink()
                except PermissionError:
                    pass
            if backup_dir.exists():
                for item in backup_dir.glob("*"):
                    try:
                        item.unlink()
                    except PermissionError:
                        pass

    def test_organization_config_imports_legacy_file_into_database(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        db_path = test_root / "web_storage_org_config.db"
        config_path = test_root / "web_storage_org_config.ini"
        try:
            for path in (db_path, config_path):
                if path.exists():
                    path.unlink()
            save_editable_config(
                {
                    "corpid": "corp-db-import",
                    "agentid": "20001",
                    "corpsecret": "secret-db-import",
                    "webhook_url": "https://example.invalid/webhook/db-import",
                    "ldap_server": "dc01.db-import.local",
                    "ldap_domain": "db-import.local",
                    "ldap_username": "db-import-admin",
                    "ldap_password": "Password123!",
                    "ldap_use_ssl": True,
                    "ldap_port": 636,
                    "ldap_validate_cert": True,
                    "ldap_ca_cert_path": "",
                    "default_password": "ChangeMe123!",
                    "force_change_password": True,
                    "password_complexity": "strong",
                    "schedule_time": "03:00",
                    "retry_interval": 60,
                    "max_retries": 3,
                },
                config_path=str(config_path),
            )

            manager = DatabaseManager(db_path=str(db_path))
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)

            org_config_repo = OrganizationConfigRepository(manager)
            self.assertFalse(org_config_repo.has_config("asia"))
            org_config_repo.ensure_loaded("asia", config_path=str(config_path))
            self.assertTrue(org_config_repo.has_config("asia"))

            config_path.unlink()

            editable = org_config_repo.get_editable_config("asia", config_path=str(config_path))
            self.assertEqual(editable["source_provider"], "wecom")
            self.assertEqual(editable["corpid"], "corp-db-import")
            self.assertTrue(editable["corpsecret_configured"])

            app_config = org_config_repo.get_app_config("asia", config_path=str(config_path))
            self.assertEqual(app_config.source_provider, "wecom")
            self.assertEqual(app_config.wecom.corpid, "corp-db-import")
            self.assertEqual(app_config.source_connector.corpid, "corp-db-import")
            self.assertEqual(app_config.ldap.server, "dc01.db-import.local")
            self.assertEqual(app_config.account.default_password, "ChangeMe123!")
            self.assertEqual(app_config.config_path, "db:org:asia")
        finally:
            backup_dir = db_path.parent / "backups"
            for path in (db_path, config_path):
                if path.exists():
                    try:
                        path.unlink()
                    except PermissionError:
                        pass
            if backup_dir.exists():
                for item in backup_dir.glob("*"):
                    try:
                        item.unlink()
                    except PermissionError:
                        pass

    def test_connector_config_imports_legacy_file_into_database(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        db_path = test_root / "web_storage_connector_config.db"
        config_path = test_root / "web_storage_connector_config.ini"
        try:
            for path in (db_path, config_path):
                if path.exists():
                    path.unlink()
            save_editable_config(
                {
                    "corpid": "corp-connector-import",
                    "agentid": "30001",
                    "corpsecret": "secret-connector-import",
                    "webhook_url": "https://example.invalid/webhook/connector-import",
                    "ldap_server": "dc01.connector-import.local",
                    "ldap_domain": "connector-import.local",
                    "ldap_username": "connector-admin",
                    "ldap_password": "Password123!",
                    "ldap_use_ssl": True,
                    "ldap_port": 636,
                    "ldap_validate_cert": True,
                    "ldap_ca_cert_path": "C:\\certs\\connector-ca.pem",
                    "default_password": "Connector123!",
                    "force_change_password": False,
                    "password_complexity": "medium",
                },
                config_path=str(config_path),
            )

            manager = DatabaseManager(db_path=str(db_path))
            manager.initialize(create_startup_snapshot=False, verify_integrity=True)

            connector_repo = SyncConnectorRepository(manager)
            connector_repo.upsert_connector(
                connector_id="asia",
                org_id="default",
                name="Asia Connector",
                config_path=str(config_path),
                root_department_ids=[2],
            )

            base_config = AppConfig(
                wecom=WeComConfig(corpid="corp-org", corpsecret="secret-org", agentid="1001"),
                ldap=LDAPConfig(
                    server="dc01.org.local",
                    domain="org.local",
                    username="org-admin",
                    password="OrgPassword123!",
                    use_ssl=True,
                    port=636,
                    validate_cert=True,
                    ca_cert_path="",
                ),
                domain="org.local",
                account=AccountConfig(default_password="OrgDefault123!", force_change_password=True, password_complexity="strong"),
                webhook_url="https://example.invalid/webhook/org",
                config_path="db:org:default",
            )

            app_config = connector_repo.get_connector_app_config("asia", base_config=base_config, org_id="default")
            self.assertIsNotNone(app_config)
            self.assertEqual(app_config.ldap.server, "dc01.connector-import.local")
            self.assertEqual(app_config.ldap.domain, "connector-import.local")
            self.assertEqual(app_config.account.default_password, "Connector123!")
            self.assertEqual(app_config.config_path, "db:connector:default:asia")

            config_path.unlink()

            connector_record = connector_repo.get_connector_record("asia", org_id="default")
            self.assertIsNotNone(connector_record)
            self.assertEqual(connector_record.ldap_server, "dc01.connector-import.local")
            self.assertEqual(connector_record.password_complexity, "medium")

            reloaded_config = connector_repo.get_connector_app_config("asia", base_config=base_config, org_id="default")
            self.assertIsNotNone(reloaded_config)
            self.assertEqual(reloaded_config.ldap.username, "connector-admin")
            self.assertFalse(reloaded_config.account.force_change_password)
        finally:
            backup_dir = db_path.parent / "backups"
            for path in (db_path, config_path):
                if path.exists():
                    try:
                        path.unlink()
                    except PermissionError:
                        pass
            if backup_dir.exists():
                for item in backup_dir.glob("*"):
                    try:
                        item.unlink()
                    except PermissionError:
                        pass


if __name__ == "__main__":
    unittest.main()
