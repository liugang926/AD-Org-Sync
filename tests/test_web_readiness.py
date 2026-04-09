import json
import os
import unittest
from pathlib import Path

from starlette.requests import Request

from sync_app.web import create_app
from sync_app.web.security import hash_password


class WebReadinessTests(unittest.TestCase):
    def setUp(self):
        test_root = Path(os.getcwd()) / "test_artifacts"
        test_root.mkdir(exist_ok=True)
        self.db_path = test_root / "web_readiness.db"
        self.config_path = test_root / "web_readiness.ini"
        for candidate in (self.db_path, self.config_path):
            if candidate.exists():
                candidate.unlink()

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.db_path) + suffix)
            if candidate.exists():
                try:
                    candidate.unlink()
                except PermissionError:
                    pass
        if self.config_path.exists():
            self.config_path.unlink()

    def _route(self, app, path: str, method: str):
        method = method.upper()
        for route in app.router.routes:
            if getattr(route, "path", None) == path and method in getattr(route, "methods", set()):
                return route.endpoint
        raise AssertionError(f"route not found: {method} {path}")

    def _request(self, app, path: str, method: str = "GET") -> Request:
        scope = {
            "type": "http",
            "method": method.upper(),
            "path": path,
            "headers": [],
            "query_string": b"",
            "app": app,
            "session": {},
        }
        return Request(scope)

    def test_readyz_reports_setup_required_before_admin_bootstrap(self):
        app = create_app(db_path=str(self.db_path), config_path=str(self.config_path))

        response = self._route(app, "/readyz", "GET")(self._request(app, "/readyz"))

        self.assertEqual(response.status_code, 503)
        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["status"], "setup_required")
        self.assertFalse(payload["checks"]["admin_bootstrapped"])

    def test_readyz_reports_ready_after_admin_bootstrap(self):
        app = create_app(db_path=str(self.db_path), config_path=str(self.config_path))
        app.state.user_repo.create_user("admin", hash_password("simple88"), role="super_admin")

        response = self._route(app, "/readyz", "GET")(self._request(app, "/readyz"))

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["status"], "ready")
        self.assertTrue(payload["checks"]["admin_bootstrapped"])


if __name__ == "__main__":
    unittest.main()
