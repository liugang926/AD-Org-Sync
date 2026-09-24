"""Bounded DingTalk reads and encrypted LDAPS operations."""
import secrets
import ssl
import string
import uuid
from collections import deque
from dataclasses import dataclass

import requests
from django.conf import settings
from ldap3 import BASE, SUBTREE, Connection, MODIFY_REPLACE, Server, Tls
from ldap3.utils.conv import escape_filter_chars
from ldap3.utils.dn import escape_rdn, parse_dn

from .domain import RuleError, protected
from .models import Configuration


@dataclass(frozen=True)
class PasswordResetOutcome:
    message: str
    complete: bool


def under(dn, root):
    try:
        parts = [(a.casefold(), b.casefold()) for a, b, _ in parse_dn(dn)]
        suffix = [(a.casefold(), b.casefold()) for a, b, _ in parse_dn(root)]
        return bool(suffix) and len(parts) >= len(suffix) and parts[-len(suffix):] == suffix
    except Exception:
        return False


class DingTalk:
    def __init__(self):
        if not all((settings.DINGTALK_APP_KEY, settings.DINGTALK_APP_SECRET)):
            raise RuleError("请配置钉钉 AppKey 和 AppSecret")
        self.http = requests.Session()
        self.token = ""

    def close(self):
        self.http.close()

    def call(self, path, payload):
        try:
            if not self.token:
                response = self.http.post("https://api.dingtalk.com/v1.0/oauth2/accessToken", json={"appKey": settings.DINGTALK_APP_KEY, "appSecret": settings.DINGTALK_APP_SECRET}, timeout=20)
                response.raise_for_status()
                self.token = response.json().get("accessToken", "")
                if not self.token:
                    raise RuleError("钉钉应用认证失败，请检查凭据")
            response = self.http.post("https://oapi.dingtalk.com" + path, params={"access_token": self.token}, json=payload, timeout=20)
            response.raise_for_status()
            data = response.json()
            if data.get("errcode") != 0:
                code = data.get("errcode")
                safe_code = str(code) if isinstance(code, int) else "未知"
                sub_code = data.get("sub_code") or data.get("subCode")
                safe_sub_code = str(sub_code)
                suffix = (
                    f"，子错误码 {safe_sub_code}"
                    if safe_sub_code.isascii() and safe_sub_code.isdecimal() and len(safe_sub_code) <= 12
                    else ""
                )
                raise RuleError(f"钉钉请求失败（错误码 {safe_code}{suffix}），请检查应用权限、可见范围和服务器 IP 白名单")
            if "result" not in data:
                raise RuleError("钉钉返回数据不完整")
            return data["result"]
        except (requests.RequestException, ValueError, TypeError):
            raise RuleError("钉钉连接或数据读取失败，请稍后重试") from None

    def employee(self, code):
        result = self.call("/topapi/v2/user/getuserinfo", {"code": code})
        user_id = str(result.get("userid", "")).strip()
        if not user_id:
            raise RuleError("钉钉未返回可信员工身份")
        return self.user(user_id)

    def user(self, user_id):
        data = self.call("/topapi/v2/user/get", {"userid": user_id})
        if str(data.get("userid", "")) != str(user_id):
            raise RuleError("钉钉员工身份不一致")
        departments = [str(i) for i in data.get("dept_id_list", [])]
        return {
            "source_id": str(user_id), "name": str(data.get("name") or ""),
            "employee_id": str(data.get("job_number") or "").strip(),
            "email": str(data.get("org_email") or data.get("email") or "").strip(),
            "title": str(data.get("title") or ""),
            "phone": str(data.get("telephone") or ""),
            "departments": departments,
            "primary_department": str(data.get("main_department") or (departments[0] if len(departments) == 1 else "")),
        }

    def user_in_scope(self, user, root_id):
        root_id = str(root_id)
        if not root_id.isdigit() or int(root_id) <= 0:
            raise RuleError("钉钉根部门 ID 无效")
        departments = [str(value) for value in user.get("departments", [])]
        if root_id in departments:
            return True
        first_error = None
        for department_id in departments:
            current, seen = department_id, set()
            try:
                while current != "0":
                    if not current.isdigit() or int(current) <= 0 or current in seen or len(seen) >= 100:
                        raise RuleError("员工部门层级不完整或存在循环，不能确认同步范围")
                    seen.add(current)
                    detail = self.call("/topapi/v2/department/get", {"dept_id": int(current)})
                    if not isinstance(detail, dict) or str(detail.get("dept_id")) != current:
                        raise RuleError("员工部门详情不完整，不能确认同步范围")
                    current = str(detail.get("parent_id", ""))
                    if current == root_id:
                        return True
            except RuleError as exc:
                if first_error is None:
                    first_error = exc
        if first_error:
            raise first_error
        return False

    def collect(self, root_id):
        if not str(root_id).isdigit() or int(root_id) <= 0:
            raise RuleError("钉钉根部门 ID 无效")
        queue, departments, users = deque([(str(root_id), None)]), {}, {}
        while queue:
            dept_id, expected_parent = queue.popleft()
            if dept_id in departments:
                raise RuleError("部门结构重复或存在循环")
            detail = self.call("/topapi/v2/department/get", {"dept_id": int(dept_id)})
            if not isinstance(detail, dict) or str(detail.get("dept_id")) != dept_id or not detail.get("name"):
                raise RuleError("部门详情不完整")
            if expected_parent is not None and str(detail.get("parent_id")) != expected_parent:
                raise RuleError("部门父级与子部门列表不一致，禁止同步")
            departments[dept_id] = {"id": dept_id, "name": detail["name"], "parent": str(detail.get("parent_id", ""))}
            children = self.call("/topapi/v2/department/listsub", {"dept_id": int(dept_id)})
            if not isinstance(children, list):
                raise RuleError("部门列表不完整")
            for child in children:
                child_id = str(child.get("dept_id", "")) if isinstance(child, dict) else ""
                if not child_id.isdigit() or int(child_id) <= 0:
                    raise RuleError("子部门 ID 缺失或无效，禁止同步")
                queue.append((child_id, dept_id))
            cursor, seen = 0, set()
            while True:
                if cursor in seen:
                    raise RuleError("人员分页未前进，本次采集已停止")
                seen.add(cursor)
                page = self.call("/topapi/v2/user/list", {"dept_id": int(dept_id), "cursor": cursor, "size": 100})
                if not isinstance(page, dict) or not isinstance(page.get("list"), list) or not isinstance(page.get("has_more"), bool):
                    raise RuleError("人员分页格式不完整")
                for item in page["list"]:
                    if not isinstance(item, dict):
                        raise RuleError("人员分页条目不完整")
                    uid = str(item.get("userid") or "")
                    if not uid:
                        raise RuleError("人员缺少稳定 userId")
                    if uid not in users:
                        users[uid] = self.user(uid)
                if not page.get("has_more"):
                    break
                if not page.get("list") or page.get("next_cursor") is None:
                    raise RuleError("人员分页不完整")
                next_cursor = page["next_cursor"]
                if isinstance(next_cursor, bool) or not str(next_cursor).isdigit():
                    raise RuleError("人员分页游标无效，本次采集已停止")
                cursor = int(next_cursor)
        if not users:
            raise RuleError("通讯录为空，禁止执行同步；请检查应用可见范围")
        return sorted(users.values(), key=lambda u: u["source_id"]), sorted(departments.values(), key=lambda d: d["id"])


class ActiveDirectory:
    ATTRS = ["objectGUID", "sAMAccountName", "employeeID", "mail", "displayName", "title", "department", "telephoneNumber", "userAccountControl", "adminCount", "objectSid", "lockoutTime", "isCriticalSystemObject", "uSNChanged"]
    MATCH = {"employee_id": "employeeID", "email": "mail", "source_id": "sAMAccountName"}

    def __init__(self):
        self.extra_protected = {n.casefold() for n in Configuration.current().protected_usernames}
        if not all((settings.LDAP_HOST, settings.LDAP_BIND_DN, settings.LDAP_PASSWORD, settings.LDAP_BASE_DN)):
            raise RuleError("请配置 LDAPS 服务器、绑定账号和目录根 DN")
        try:
            tls = Tls(validate=ssl.CERT_REQUIRED if settings.LDAP_VERIFY_CERT else ssl.CERT_NONE, ca_certs_file=settings.LDAP_CA_FILE if settings.LDAP_VERIFY_CERT else None)
            server = Server(settings.LDAP_HOST, port=636, use_ssl=True, tls=tls, connect_timeout=10)
            self.conn = Connection(server, user=settings.LDAP_BIND_DN, password=settings.LDAP_PASSWORD, auto_bind=True, auto_referrals=False, receive_timeout=20)
        except Exception:
            raise RuleError("LDAPS 连接失败，请检查证书、网络和凭据") from None

    def close(self):
        self.conn.unbind()

    def search(self, query, base=None, attrs=None, scope=SUBTREE):
        results, cookie, seen = [], None, set()
        try:
            while True:
                self.conn.search(base or settings.LDAP_BASE_DN, query, scope, attributes=attrs or self.ATTRS, paged_size=500, paged_cookie=cookie)
                if self.conn.result.get("result") != 0:
                    raise RuleError("AD 查询未完整成功")
                if any(r["type"] == "searchResRef" for r in self.conn.response):
                    raise RuleError("AD 查询包含未处理的引用")
                results.extend(r for r in self.conn.response if r["type"] == "searchResEntry")
                cookie = self.conn.result.get("controls", {}).get("1.2.840.113556.1.4.319", {}).get("value", {}).get("cookie")
                if not cookie:
                    return results
                if cookie in seen:
                    raise RuleError("AD 分页未前进")
                seen.add(cookie)
        except RuleError:
            raise
        except Exception:
            raise RuleError("AD 查询失败，请检查目录连接") from None

    @staticmethod
    def account(entry, protected_names=()):
        attrs = entry["attributes"]
        def value(name, default=""):
            item = attrs.get(name, default)
            return item[0] if isinstance(item, list) and item else (default if isinstance(item, list) else item)
        uac = int(value("userAccountControl", 0))
        sid = str(value("objectSid"))
        critical = str(value("isCriticalSystemObject")).lower() == "true"
        explicit_protection = str(value("sAMAccountName")).casefold() in protected_names
        ad_revision = str(value("uSNChanged") or "").strip()
        return {"guid": str(uuid.UUID(str(value("objectGUID")).strip("{}"))), "dn": entry["dn"],
                "ad_revision": ad_revision,
                "username": str(value("sAMAccountName")).strip(), "employee_id": str(value("employeeID")).strip(),
                "email": str(value("mail")).strip(), "enabled": not bool(uac & 2), "uac": uac,
                "protected": critical or explicit_protection or int(value("adminCount", 0)) == 1 or sid.endswith(("-500", "-501", "-502")) or bool(uac & (2048 | 4096 | 8192)),
                "locked": bool(int(value("lockoutTime", 0))),
                "attrs": {k: str(value(k)) for k in ["displayName", "mail", "title", "department", "telephoneNumber"]}}

    def accounts(self):
        return [self.account(e, self.extra_protected) for e in self.search("(&(objectCategory=person)(objectClass=user))")]

    def match(self, field, value):
        if field not in self.MATCH or not value:
            raise RuleError("可信身份字段为空，不能匹配 AD 账号")
        query = f"(&(objectCategory=person)(objectClass=user)({self.MATCH[field]}={escape_filter_chars(value)}))"
        return [self.account(e, self.extra_protected) for e in self.search(query)]

    def by_guid(self, guid):
        escaped = "".join(f"\\{b:02x}" for b in uuid.UUID(str(guid)).bytes_le)
        entries = self.search(f"(&(objectCategory=person)(objectClass=user)(objectGUID={escaped}))")
        if len(entries) != 1:
            raise RuleError("AD 目标对象不存在或不唯一")
        return self.account(entries[0], self.extra_protected)

    def check_account(self, guid):
        account = self.by_guid(guid)
        if protected(account) or not account["enabled"]:
            raise RuleError("账号受保护或已禁用，不能执行此操作")
        return account

    def ensure_ou(self, dn, root):
        if not under(dn, root):
            raise RuleError("OU 超出管理范围")
        parts = parse_dn(dn)
        root_parts = parse_dn(root)
        for index in range(len(parts) - len(root_parts), -1, -1):
            current = "".join(a + "=" + b + sep for a, b, sep in parts[index:]).rstrip(",")
            try:
                found = self.search("(objectClass=organizationalUnit)", base=current, attrs=["objectGUID"], scope=BASE)
            except RuleError:
                if self.conn.result.get("result") != 32:
                    raise
                found = []
            if not found:
                if current.casefold() == root.casefold():
                    raise RuleError("配置的 AD 根 OU 不存在")
                if not self.conn.add(current, ["top", "organizationalUnit"]):
                    raise RuleError("创建 OU 失败")
        rows = self.search("(objectClass=organizationalUnit)", base=dn, attrs=["objectGUID"], scope=BASE)
        if len(rows) != 1:
            raise RuleError("OU 无法唯一确认")
        return str(uuid.UUID(str(rows[0]["attributes"]["objectGUID"]).strip("{}")))

    def verify_ou(self, dn, guid=None):
        rows = self.search("(objectClass=organizationalUnit)", base=dn, attrs=["objectGUID"], scope=BASE)
        if len(rows) != 1:
            raise RuleError("OU 不存在或不唯一")
        current = str(uuid.UUID(str(rows[0]["attributes"]["objectGUID"]).strip("{}")))
        if guid and current != guid:
            raise RuleError("OU 对象已被替换，请重新确认部门关联")
        return current

    def ou_identity(self, dn):
        try:
            return self.verify_ou(dn)
        except RuleError:
            if self.conn.result.get("result") == 32:
                return None
            raise

    def create(self, user, username, ou, root, *, enabled=True, require_change=True):
        if self.match("source_id", username):
            raise RuleError("AD 用户名已存在，需重新预览")
        if self.match("employee_id", user["employee_id"]):
            raise RuleError("工号已被 AD 账号使用，请重新预览")
        if not under(ou, root):
            raise RuleError("目标 OU 超出管理范围")
        dn = f"CN={escape_rdn(username)},{ou}"
        attrs = {"sAMAccountName": username, "displayName": user["name"], "employeeID": user["employee_id"], "userAccountControl": 514}
        if not self.conn.add(dn, ["top", "person", "organizationalPerson", "user"], attrs):
            result = self.conn.result or {}
            code = result.get("result")
            if code == 19:
                if "employeeid" in str(result.get("message", "")).casefold():
                    raise RuleError("AD 创建账号失败：工号不符合域控 employeeID 字段约束，请检查长度或格式")
                raise RuleError("AD 创建账号失败：目录字段约束不满足，请检查账号属性")
            safe_code = str(code) if isinstance(code, int) else "未知"
            raise RuleError(f"AD 创建账号失败（LDAP 错误码 {safe_code}），请检查目标 OU 权限和目录规则")
        matches = self.match("source_id", username)
        if len(matches) != 1:
            raise RuleError("AD 已创建但无法确认对象，需人工核验")
        account = matches[0]
        alphabet = string.ascii_letters + string.digits
        password = "Aa1!" + "".join(secrets.choice(alphabet) for _ in range(28))
        if not self.conn.extend.microsoft.modify_password(dn, password):
            raise RuleError("账号已创建但初始化密码失败，已保持禁用，请人工处理")
        if not self.conn.modify(dn, {"userAccountControl": [(MODIFY_REPLACE, [512 if enabled else 514])], "pwdLastSet": [(MODIFY_REPLACE, [0 if require_change else -1])]}):
            raise RuleError("账号初始化未完成，需人工处理")
        return self.by_guid(account["guid"])

    def update(self, guid, attrs, ou, root, *, allow_disabled=False):
        account = self.by_guid(guid)
        if protected(account) or (not account["enabled"] and not allow_disabled):
            raise RuleError("账号受保护或已禁用，不能更新")
        if not under(account["dn"], root) or not under(ou, root):
            raise RuleError("账号或目标 OU 超出管理范围")
        changes = {k: [(MODIFY_REPLACE, [v] if v else [])] for k, v in attrs.items() if account["attrs"].get(k, "") != v}
        if changes and not self.conn.modify(account["dn"], changes):
            raise RuleError("AD 属性更新失败")
        components = parse_dn(account["dn"])
        parent = "".join(a + "=" + b + sep for a, b, sep in components[1:]).rstrip(",")
        if parent.casefold() != ou.casefold():
            rdn = components[0][0] + "=" + components[0][1]
            if not self.conn.modify_dn(account["dn"], rdn, new_superior=ou):
                raise RuleError("AD 账号移动失败")
        return self.by_guid(guid)

    def disable(self, guid, root):
        account = self.check_account(guid)
        if not under(account["dn"], root):
            raise RuleError("账号超出管理范围")
        if not self.conn.modify(account["dn"], {"userAccountControl": [(MODIFY_REPLACE, [account["uac"] | 2])]}):
            raise RuleError("AD 禁用失败")

    def enable(self, guid, root):
        account = self.by_guid(guid)
        if protected(account) or not under(account["dn"], root):
            raise RuleError("目标受保护或不在管理范围内")
        if not self.conn.modify(account["dn"], {"userAccountControl": [(MODIFY_REPLACE, [account["uac"] & ~2])]}):
            raise RuleError("AD 账号恢复启用失败")
        return self.by_guid(guid)

    def reset_password(self, guid, password, unlock=False):
        account = self.check_account(guid)
        try:
            changed = self.conn.extend.microsoft.modify_password(account["dn"], password)
        except Exception:
            raise RuleError("目录响应中断，密码修改结果不明；请先验证或联系管理员") from None
        if not changed:
            raise RuleError("AD 拒绝密码，请检查复杂度和密码历史要求")
        if unlock:
            try:
                unlocked = self.conn.modify(account["dn"], {"lockoutTime": [(MODIFY_REPLACE, [0])]})
            except Exception:
                unlocked = False
            if not unlocked:
                return PasswordResetOutcome("密码已重置，但解锁失败，请联系管理员", False)
        return PasswordResetOutcome("密码已成功重置", True)
