"""Read-only, redacted diagnosis of DingTalk department authorization."""

import os
from contextlib import closing

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sync_app.settings")

import django

django.setup()

from sync_app.directory import DingTalk
from sync_app.models import Configuration, Snapshot


config = Configuration.objects.get(pk=1)
assert not config.schedule_enabled and not config.sspr_enabled
snapshot = Snapshot.objects.order_by("-pk").first()
assert snapshot is not None
candidates = [user for user in snapshot.users if user.get("employee_id") == "T0001919"]
assert len(candidates) == 1
source_id = candidates[0]["source_id"]
assert source_id


def safe_code(value):
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, str) and value.isdecimal() and len(value) <= 12:
        return value
    return "unavailable"


with closing(DingTalk()) as source:
    user = source.user(source_id)
    assert user["employee_id"] == "T0001919"
    departments = user["departments"]
    assert departments
    print("live_user_read=ok", flush=True)
    print("user_department_count=" + str(len(departments)), flush=True)

    targets = [("configured_root", str(config.root_department))]
    targets.extend(("user_department_" + str(index), dept) for index, dept in enumerate(departments, start=1))
    for label, dept_id in targets:
        assert dept_id.isdecimal() and int(dept_id) > 0
        response = source.http.post(
            "https://oapi.dingtalk.com/topapi/v2/department/get",
            params={"access_token": source.token},
            json={"dept_id": int(dept_id)},
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        assert isinstance(data, dict)
        print(label + "_errcode=" + safe_code(data.get("errcode")), flush=True)
        if data.get("errcode") != 0:
            print(label + "_sub_code=" + safe_code(data.get("sub_code")), flush=True)
            message = str(data.get("sub_msg") or data.get("errmsg") or "").casefold()
            keywords = {"permission": "权限", "visibility": "可见", "department": "部门", "token": "token", "ip": "ip", "throttle": "限流"}
            categories = [name for name, keyword in keywords.items() if keyword in message]
            print(label + "_categories=" + (",".join(categories) or "unclassified"), flush=True)

print("read_only_probe_complete=true", flush=True)
