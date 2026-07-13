import unittest
from unittest.mock import patch

from sync_app.clients.feishu import FeishuAPI, FeishuAPIError
from sync_app.providers.source.feishu import FeishuSourceProvider


class _Response:
    def __init__(self, payload, status_code=200, headers=None):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")


class _Session:
    def __init__(self):
        self.calls = []
        self.responses = []

    def mount(self, *_args, **_kwargs):
        return None

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return self.responses.pop(0)

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)

    def close(self):
        return None


class FeishuClientTests(unittest.TestCase):
    def test_token_department_user_paging_and_normalization(self):
        session = _Session()
        session.responses = [
            _Response({"code": 0, "tenant_access_token": "t-test", "expire": 7200}),
            _Response({"code": 0, "data": {"items": [{"open_department_id": "od-a", "name": "R&D", "parent_department_id": "0"}], "has_more": False}}),
            _Response({"code": 0, "data": {"items": [], "has_more": False}}),
            _Response({"code": 0, "data": {"items": [{"user_id": "u1", "name": "Alice", "employee_no": "E001", "email": "alice@example.com", "mobile": "13800138000", "job_title": "Engineer", "department_ids": ["od-a"], "status": {"is_activated": True}}], "has_more": True, "page_token": "next"}}),
            _Response({"code": 0, "data": {"items": [{"user_id": "u2", "name": "Bob", "employee_no": "E002", "department_ids": ["od-a"], "status": {"is_resigned": True}}], "has_more": False}}),
            _Response({"code": 0, "data": {"user": {"user_id": "u1", "name": "Alice", "employee_no": "E001", "department_ids": ["od-a"], "status": {"is_activated": True}}}}),
        ]
        with patch("sync_app.clients.feishu.requests.Session", return_value=session):
            api = FeishuAPI("cli_app", "secret")
            departments = api.get_department_list()
            self.assertEqual(departments[0]["source_department_id"], "od-a")
            users = api.get_department_users(departments[0]["id"])
            self.assertEqual([item["userid"] for item in users], ["u1", "u2"])
            self.assertEqual(users[0]["employee_id"], "E001")
            self.assertFalse(users[1]["is_active"])
            detail = api.get_user_detail("u1")
            self.assertEqual(detail["employee_id"], "E001")
            self.assertNotIn("secret", str(session.calls[1:]))

    def test_permission_error_is_actionable_and_redacted(self):
        session = _Session()
        session.responses = [
            _Response({"code": 0, "tenant_access_token": "t-sensitive", "expire": 7200}),
            _Response({"code": 41050, "msg": "no user authority"}),
        ]
        with patch("sync_app.clients.feishu.requests.Session", return_value=session):
            api = FeishuAPI("cli_app", "app-secret")
            with self.assertRaisesRegex(FeishuAPIError, "permission or data scope") as raised:
                api.get_department_list()
        self.assertNotIn("app-secret", str(raised.exception))
        self.assertNotIn("t-sensitive", str(raised.exception))

    def test_provider_normalizes_employee_and_status(self):
        class FakeAPI:
            def __init__(self, *_args, **_kwargs):
                pass
            def get_department_list(self):
                return [{"id": 1, "name": "R&D", "parentid": 0}]
            def get_department_users(self, _department_id):
                return [{"userid": "u1", "name": "Alice", "employee_no": "E001", "department": [1], "status": {"is_activated": True}}]
            def get_user_detail(self, _user_id):
                return {"userid": "u1", "name": "Alice", "employee_no": "E001", "department": [1], "status": {"is_activated": True}}
            def close(self):
                return None
        provider = FeishuSourceProvider("app", "secret", api_factory=FakeAPI)
        user = provider.list_department_users(1)[0]
        self.assertEqual(user.employee_id, "E001")
        self.assertTrue(user.is_active)


if __name__ == "__main__":
    unittest.main()
