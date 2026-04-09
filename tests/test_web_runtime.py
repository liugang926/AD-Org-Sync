import unittest

from sync_app.web.runtime import (
    LoginRateLimiter,
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


if __name__ == "__main__":
    unittest.main()
