import unittest
from unittest.mock import patch

from sync_app.clients.dingtalk import DingTalkAPI, DingTalkAPIError
from sync_app.core.models import SourceDirectoryUser
from sync_app.core.value_coercion import coerce_int_list


class _Response:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class _Session:
    def __init__(self, response):
        self.response = response

    def mount(self, *_args, **_kwargs):
        return None

    def post(self, *_args, **_kwargs):
        return self.response

    def close(self):
        return None


class DingTalkClientBehaviorTests(unittest.TestCase):
    def _build_api(self) -> DingTalkAPI:
        api = DingTalkAPI.__new__(DingTalkAPI)
        api.app_secret = "app-secret"
        api.access_token = "app-access-token"
        api._ensure_token_valid = lambda: None
        return api

    def test_exchange_employee_auth_code_returns_trusted_user_identity(self):
        api = self._build_api()
        calls = []

        def request(method, url, **kwargs):
            calls.append((method, url, kwargs))
            return {
                "errcode": 0,
                "result": {"userid": "alice.dd", "name": "Alice", "sys_level": 1},
            }

        api._request = request

        identity = api.exchange_employee_auth_code("one-time-code")

        self.assertEqual(identity["userid"], "alice.dd")
        self.assertEqual(identity["name"], "Alice")
        self.assertEqual(calls[0][0], "POST")
        self.assertEqual(calls[0][2]["data"], {"code": "one-time-code"})
        self.assertNotIn("one-time-code", calls[0][1])

    def test_exchange_employee_auth_code_maps_invalid_and_expired_codes(self):
        for message, expected in (("invalid one-time-code", "invalid_auth_code"), ("code expired", "expired_auth_code")):
            api = self._build_api()
            api._request = lambda *_args, **_kwargs: {"errcode": 40078, "errmsg": message}

            with self.assertRaises(DingTalkAPIError) as raised:
                api.exchange_employee_auth_code("one-time-code")

            self.assertEqual(raised.exception.category, expected)
            self.assertNotIn("one-time-code", str(raised.exception))
            self.assertNotIn("one-time-code", raised.exception.detail)

    def test_exchange_employee_auth_code_rejects_missing_user_id(self):
        api = self._build_api()
        api._request = lambda *_args, **_kwargs: {"errcode": 0, "result": {"name": "Alice"}}

        with self.assertRaises(DingTalkAPIError) as raised:
            api.exchange_employee_auth_code("one-time-code")

        self.assertEqual(raised.exception.category, "invalid_response")

    def test_exchange_employee_auth_code_preserves_sanitized_network_category(self):
        api = self._build_api()

        def fail(*_args, **_kwargs):
            raise DingTalkAPIError("request unavailable", category="network_error")

        api._request = fail

        with self.assertRaises(DingTalkAPIError) as raised:
            api.exchange_employee_auth_code("one-time-code")

        self.assertEqual(raised.exception.category, "network_error")

    def test_authentication_error_preserves_provider_reason_and_redacts_secret(self):
        session = _Session(
            _Response(
                {
                    "code": "InvalidParameter",
                    "message": "Specified appKey or appSecret super-secret is invalid.",
                    "requestid": "ding-request-1",
                },
                status_code=400,
            )
        )

        with patch("sync_app.clients.dingtalk.requests.Session", return_value=session):
            with self.assertRaises(DingTalkAPIError) as raised:
                DingTalkAPI("ding-app-key", "super-secret")

        error = raised.exception
        self.assertEqual(error.category, "invalid_credentials")
        self.assertEqual(error.code, "InvalidParameter")
        self.assertEqual(error.status_code, 400)
        self.assertEqual(error.request_id, "ding-request-1")
        self.assertIn("appKey or appSecret", error.detail)
        self.assertNotIn("super-secret", str(error))
        self.assertNotIn("super-secret", error.detail)

    def test_get_department_list_includes_root_department(self):
        api = self._build_api()
        calls = []

        def fake_post(path: str, payload: dict):
            calls.append((path, dict(payload)))
            if path == "/topapi/v2/department/get":
                return {"dept_id": 1, "name": "Acme", "parent_id": 0}
            if path == "/topapi/v2/department/listsub" and int(payload["dept_id"]) == 1:
                return [{"dept_id": 10, "name": "研发部", "parent_id": 1}]
            if path == "/topapi/v2/department/listsub" and int(payload["dept_id"]) == 10:
                return []
            raise AssertionError(f"unexpected call: {path} {payload}")

        api._post_oapi = fake_post

        departments = api.get_department_list()

        self.assertEqual([item["id"] for item in departments], [1, 10])
        self.assertEqual(departments[0]["parentid"], 0)
        self.assertEqual(calls[0][0], "/topapi/v2/department/get")

    def test_get_department_users_parses_stringified_department_ids_and_false_string(self):
        api = self._build_api()
        calls = []

        def fake_post(path: str, payload: dict):
            calls.append((path, dict(payload)))
            if len(calls) > 1:
                raise AssertionError("unexpected extra page request")
            return {
                "list": {
                    "userid": "alice.dd",
                    "name": "Alice Ding",
                    "email": "alice@example.com",
                    "dept_id_list": "[2,3]",
                },
                "has_more": "false",
                "next_cursor": "10",
            }

        api._post_oapi = fake_post

        users = api.get_department_users(2)

        self.assertEqual(len(users), 1)
        self.assertEqual(users[0]["department"], [2, 3])
        self.assertEqual(users[0]["main_department"], 2)
        self.assertEqual(len(calls), 1)

    def test_get_user_detail_parses_single_department_id(self):
        api = self._build_api()
        api._post_oapi = lambda path, payload: {
            "userid": "zhangsan",
            "name": "张三",
            "dept_id": "2",
            "manager_userid": "manager240",
        }

        detail = api.get_user_detail("zhangsan")

        self.assertEqual(detail["department"], [2])
        self.assertEqual(detail["main_department"], 2)


class SourceDirectoryUserNormalizationTests(unittest.TestCase):
    def test_shared_integer_coercion_handles_nested_payloads_and_preserves_order(self):
        self.assertEqual(
            coerce_int_list(
                [
                    {"departmentId": "7"},
                    {"9": "ignored"},
                    {"nested": {"deptId": "11"}},
                    "7, -2, 9",
                ]
            ),
            [7, 9, 11, -2],
        )

    def test_from_source_payload_parses_stringified_department_list(self):
        user = SourceDirectoryUser.from_source_payload(
            {
                "userid": "alice.dd",
                "name": "Alice Ding",
                "dept_id_list": "[2,3,4]",
            }
        )

        self.assertEqual(user.departments, [2, 3, 4])

    def test_from_source_payload_parses_department_order_mapping_keys(self):
        user = SourceDirectoryUser.from_source_payload(
            {
                "userid": "zhangsan",
                "name": "张三",
                "dept_order_list": {"2": "10", "5": "20"},
            }
        )

        self.assertEqual(user.departments, [2, 5])


if __name__ == "__main__":
    unittest.main()
