import unittest
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from sync_app.services.external_integrations import emit_integration_event
from sync_app.storage.local_db import DatabaseManager, SyncJobRepository, WebAuditLogRepository
from sync_app.storage.repositories.system import IntegrationWebhookOutboxRepository, IntegrationWebhookSubscriptionRepository
from sync_app.web.runtime import (
    IntegrationOutboxWorker,
    LoginRateLimiter,
    WebSyncRunner,
    normalize_secure_cookie_mode,
    resolve_web_runtime_settings,
    web_runtime_requires_restart,
)


class FakeSettingsRepo:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get_value(self, key, default=""):
        return self.values.get(key, default)

    def get_int(self, key, default=0):
        return int(self.values.get(key, default))

    def get_bool(self, key, default=False):
        value = self.values.get(key, default)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}


class WebRuntimeTests(unittest.TestCase):
    def test_normalize_secure_cookie_mode_defaults_to_auto(self):
        self.assertEqual(normalize_secure_cookie_mode("always"), "always")
        self.assertEqual(normalize_secure_cookie_mode("AUTO"), "auto")
        self.assertEqual(normalize_secure_cookie_mode("invalid"), "auto")

    def test_resolve_web_runtime_settings_enables_secure_cookie_for_https_public_url(self):
        settings = FakeSettingsRepo(
            {
                "web_bind_host": "127.0.0.1",
                "web_bind_port": "8000",
                "web_public_base_url": "https://sync.example.com",
                "web_session_cookie_secure_mode": "auto",
            }
        )

        resolved = resolve_web_runtime_settings(settings)

        self.assertTrue(resolved["session_cookie_secure"])
        self.assertEqual(resolved["public_base_url"], "https://sync.example.com")
        self.assertEqual(resolved["warnings"], [])

    def test_resolve_web_runtime_settings_warns_on_wildcard_proxy_allowlist(self):
        settings = FakeSettingsRepo(
            {
                "web_bind_host": "0.0.0.0",
                "web_bind_port": "8443",
                "web_public_base_url": "http://sync.example.com",
                "web_session_cookie_secure_mode": "never",
                "web_trust_proxy_headers": "true",
                "web_forwarded_allow_ips": "*",
            }
        )

        resolved = resolve_web_runtime_settings(settings)

        self.assertFalse(resolved["session_cookie_secure"])
        self.assertIn("Secure session cookies are disabled.", resolved["warnings"])
        self.assertIn("Public base URL does not use HTTPS.", resolved["warnings"])
        self.assertIn("Forwarded proxy headers are trusted from every IP address.", resolved["warnings"])

    def test_web_runtime_requires_restart_detects_relevant_changes(self):
        current = {
            "bind_host": "127.0.0.1",
            "bind_port": 8000,
            "public_base_url": "",
            "session_cookie_secure_mode": "auto",
            "session_cookie_secure": False,
            "trust_proxy_headers": False,
            "forwarded_allow_ips": "127.0.0.1",
        }
        persisted = dict(current)

        self.assertFalse(web_runtime_requires_restart(current, persisted))
        self.assertTrue(
            web_runtime_requires_restart(
                {**current, "bind_port": 8443},
                persisted,
            )
        )

    def test_login_rate_limiter_locks_and_clears_failed_attempts(self):
        limiter = LoginRateLimiter(max_attempts=2, window_seconds=60, lockout_seconds=120)

        self.assertEqual(limiter.record_failure("admin", "127.0.0.1"), (False, 0))
        locked, retry_after = limiter.record_failure("admin", "127.0.0.1")
        self.assertTrue(locked)
        self.assertGreaterEqual(retry_after, 1)
        self.assertTrue(limiter.check("admin", "127.0.0.1")[0])

        limiter.clear("admin", "127.0.0.1")

        self.assertEqual(limiter.check("admin", "127.0.0.1"), (False, 0))

    def test_web_sync_runner_rejects_launch_when_database_has_active_job(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "web_runtime.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()
            audit_repo = WebAuditLogRepository(db_manager)
            SyncJobRepository(db_manager).create_job(
                job_id="job-running-001",
                trigger_type="web",
                execution_mode="apply",
                status="RUNNING",
                org_id="default",
            )

            runner = WebSyncRunner(db_path=str(db_path), audit_repo=audit_repo)
            ok, message = runner.launch(
                mode="apply",
                actor_username="admin",
                org_id="default",
                config_path="config.ini",
            )

        self.assertFalse(ok)
        self.assertIn("job-running-001", message)

    def test_integration_outbox_worker_flushes_pending_deliveries_on_interval(self):
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "web_outbox.db"
            db_manager = DatabaseManager(db_path=str(db_path))
            db_manager.initialize()
            subscription_repo = IntegrationWebhookSubscriptionRepository(db_manager)
            outbox_repo = IntegrationWebhookOutboxRepository(db_manager)
            subscription_repo.upsert_subscription(
                org_id="default",
                event_type="job.completed",
                target_url="https://example.invalid/hooks/worker",
                secret="worker-secret",
                is_enabled=True,
            )
            emit_integration_event(
                db_manager,
                org_id="default",
                event_type="job.completed",
                payload={"job": {"job_id": "job-worker-001"}},
                dispatch_inline=False,
                dispatch_async=False,
            )

            response = Mock()
            response.ok = True
            response.status_code = 200
            response.reason = "OK"
            response.text = ""
            worker = IntegrationOutboxWorker(
                db_path=str(db_path),
                poll_seconds=0.05,
                batch_limit=10,
                max_batches=2,
            )
            try:
                with patch("sync_app.services.external_integrations.requests.post", return_value=response):
                    worker.start()
                    deadline = time.monotonic() + 1.0
                    while time.monotonic() < deadline:
                        records = outbox_repo.list_delivery_records(org_id="default", limit=5)
                        if records and records[0].status == "delivered":
                            break
                        time.sleep(0.05)
            finally:
                worker.stop()

            records = outbox_repo.list_delivery_records(org_id="default", limit=5)
            self.assertTrue(records)
            self.assertEqual(records[0].status, "delivered")
            self.assertEqual(records[0].attempt_count, 1)
            self.assertEqual(worker.last_error, "")


if __name__ == "__main__":
    unittest.main()
