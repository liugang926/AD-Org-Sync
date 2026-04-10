import unittest

from sync_app.clients.dingtalk import DingTalkAPI
from sync_app.core.models import SourceDirectoryUser


class DingTalkClientBehaviorTests(unittest.TestCase):
    def _build_api(self) -> DingTalkAPI:
        return DingTalkAPI.__new__(DingTalkAPI)

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
