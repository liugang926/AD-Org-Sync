"""Read-only verification of the proposed DingTalk collection guards."""
import os
from collections import deque
from contextlib import closing

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sync_app.settings")
import django

django.setup()

from sync_app.directory import DingTalk
from sync_app.models import Configuration


config = Configuration.objects.get(pk=1)
assert not config.schedule_enabled and not config.sspr_enabled
root_id = str(config.root_department)
assert root_id.isdigit() and int(root_id) > 0
queue = deque([(root_id, None)])
departments, people = set(), set()
pages = 0
with closing(DingTalk()) as source:
    while queue:
        dept_id, expected_parent = queue.popleft()
        assert dept_id not in departments, "department cycle or duplicate"
        detail = source.call("/topapi/v2/department/get", {"dept_id": int(dept_id)})
        assert isinstance(detail, dict) and str(detail.get("dept_id")) == dept_id and detail.get("name"), "invalid department detail"
        if expected_parent is not None:
            assert str(detail.get("parent_id")) == expected_parent, "parent mismatch"
        departments.add(dept_id)
        children = source.call("/topapi/v2/department/listsub", {"dept_id": int(dept_id)})
        assert isinstance(children, list), "invalid child list"
        for child in children:
            child_id = str(child.get("dept_id", "")) if isinstance(child, dict) else ""
            assert child_id.isdigit() and int(child_id) > 0, "invalid child ID"
            queue.append((child_id, dept_id))
        cursor, seen = 0, set()
        while True:
            assert cursor not in seen, "pagination cycle"
            seen.add(cursor)
            page = source.call("/topapi/v2/user/list", {"dept_id": int(dept_id), "cursor": cursor, "size": 100})
            assert isinstance(page, dict) and isinstance(page.get("list"), list) and isinstance(page.get("has_more"), bool), "invalid user page"
            pages += 1
            for item in page["list"]:
                assert isinstance(item, dict), "invalid user item"
                user_id = str(item.get("userid") or "")
                assert user_id, "missing user ID"
                people.add(user_id)
            if not page["has_more"]:
                break
            next_cursor = page.get("next_cursor")
            assert page["list"] and not isinstance(next_cursor, bool) and str(next_cursor).isdigit(), "invalid next cursor"
            cursor = int(next_cursor)

assert departments and people
print("source_guard_parent_links_valid=true")
print("source_guard_pagination_valid=true")
print("department_count=" + str(len(departments)))
print("unique_user_count=" + str(len(people)))
print("list_page_count=" + str(pages))
