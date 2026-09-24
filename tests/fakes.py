import copy
import uuid
from sync_app.domain import RuleError
from sync_app.directory import PasswordResetOutcome


def user(uid="u1", employee="1001"):
    return {"source_id": uid, "name": "测试员工", "employee_id": employee, "email": uid + "@example.com", "title": "工程师", "phone": "", "departments": ["1"], "primary_department": "1"}


def account(employee="1001", name="testuser", guid=None):
    return {"guid": guid or str(uuid.uuid4()), "dn": "CN=" + name + ",OU=People,DC=example,DC=com", "username": name, "employee_id": employee, "email": "u1@example.com", "enabled": True, "protected": False, "locked": False, "uac": 512, "ad_revision": "1", "attrs": {}}


class Source:
    def __init__(self, users=None):
        self.users = users if users is not None else [user()]

    def collect(self, root):
        return copy.deepcopy(self.users), [{"id": "1", "name": "公司", "parent": "0"}]

    def employee(self, code):
        if code != "valid":
            raise RuleError("授权码无效")
        return copy.deepcopy(self.users[0])

    def user(self, uid):
        return next(copy.deepcopy(u) for u in self.users if u["source_id"] == uid)

    def user_in_scope(self, employee, root_id):
        return str(root_id) in employee["departments"]

    def close(self):
        pass


class Directory:
    def __init__(self, accounts=None):
        self.items = accounts if accounts is not None else [account()]
        self.created = 0
        self.resets = 0
        self.disabled = []
        self.fail_update = set()
        self.ous = {"ou=people,dc=example,dc=com": str(uuid.uuid4())}

    def accounts(self):
        return copy.deepcopy(self.items)

    def match(self, field, value):
        key = {"source_id": "username"}.get(field, field)
        return [copy.deepcopy(a) for a in self.items if a.get(key, "").casefold() == value.casefold()]

    def by_guid(self, guid):
        rows = [copy.deepcopy(a) for a in self.items if a["guid"] == str(guid)]
        if not rows:
            raise RuleError("对象不存在")
        return rows[0]

    def check_account(self, guid):
        return self.by_guid(guid)

    def ensure_ou(self, dn, root):
        return self.ous.setdefault(dn.casefold(), str(uuid.uuid4()))

    def verify_ou(self, dn, guid=None):
        current = self.ous.get(dn.casefold())
        if not current or (guid and guid != current):
            raise RuleError("OU 不存在或已变化")
        return current

    def ou_identity(self, dn):
        return self.ous.get(dn.casefold())

    def create(self, user, username, ou, root, *, enabled=True, require_change=True):
        self.created += 1
        item = account(user["employee_id"], username)
        item["enabled"] = enabled
        item["require_change"] = require_change
        self.items.append(item)
        return copy.deepcopy(item)

    def update(self, guid, attrs, ou, root, *, allow_disabled=False):
        if str(guid) in self.fail_update:
            raise RuleError("属性更新失败")
        item = next(a for a in self.items if a["guid"] == str(guid))
        new_dn = item["dn"].split(",", 1)[0] + "," + ou
        changed = new_dn != item["dn"] or any(item["attrs"].get(key, "") != value for key, value in attrs.items())
        item["attrs"].update(attrs)
        item["dn"] = new_dn
        if changed:
            item["ad_revision"] = str(int(item["ad_revision"]) + 1)
        return self.by_guid(guid)

    def enable(self, guid, root):
        item = next(a for a in self.items if a["guid"] == str(guid))
        item["enabled"] = True
        item["ad_revision"] = str(int(item["ad_revision"]) + 1)
        return self.by_guid(guid)

    def disable(self, guid, root):
        self.disabled.append(guid)
        item = next(a for a in self.items if a["guid"] == guid)
        item["enabled"] = False
        item["ad_revision"] = str(int(item["ad_revision"]) + 1)

    def reset_password(self, guid, password, unlock=False):
        self.resets += 1
        item = next(a for a in self.items if a["guid"] == str(guid))
        item["ad_revision"] = str(int(item["ad_revision"]) + 1)
        return PasswordResetOutcome("密码已成功重置", True)

    def close(self):
        pass
