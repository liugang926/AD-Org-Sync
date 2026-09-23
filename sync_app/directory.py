"""Bounded DingTalk reads and certificate-validated LDAPS operations."""
import secrets
import ssl
import string
import uuid
from collections import deque

import requests
from django.conf import settings
from ldap3 import BASE, SUBTREE, Connection, MODIFY_REPLACE, Server, Tls
from ldap3.utils.conv import escape_filter_chars
from ldap3.utils.dn import escape_rdn, parse_dn

from .domain import RuleError, protected


def under(dn, root):
    try:
        parts = [(a.casefold(), b.casefold()) for a, b, _ in parse_dn(dn)]
        suffix = [(a.casefold(), b.casefold()) for a, b, _ in parse_dn(root)]
        return bool(suffix) and len(parts) >= len(suffix) and parts[-len(suffix):] == suffix
    except Exception:
        return False


class DingTalk:
    def __init__(self):
        if not all((settings.DINGTALK_CORP_ID, settings.DINGTALK_APP_KEY, settings.DINGTALK_APP_SECRET)):
            raise RuleError("请配置钉钉企业 ID、AppKey 和 AppSecret")
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
                raise RuleError("钉钉请求失败，请检查权限、可见范围和服务器 IP 白名单")
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

    def collect(self, root_id):
        queue, departments, users = deque([str(root_id)]), {}, {}
        while queue:
            dept_id = queue.popleft()
            if dept_id in departments:
                raise RuleError("部门结构重复或存在循环")
            detail = self.call("/topapi/v2/department/get", {"dept_id": int(dept_id)})
            if str(detail.get("dept_id")) != dept_id or not detail.get("name"):
                raise RuleError("部门详情不完整")
            departments[dept_id] = {"id": dept_id, "name": detail["name"], "parent": str(detail.get("parent_id", ""))}
            children = self.call("/topapi/v2/department/listsub", {"dept_id": int(dept_id)})
            if not isinstance(children, list):
                raise RuleError("部门列表不完整")
            queue.extend(str(d["dept_id"]) for d in children)
            cursor, seen = 0, set()
            while True:
                if cursor in seen:
                    raise RuleError("人员分页未前进，本次采集已停止")
                seen.add(cursor)
                page = self.call("/topapi/v2/user/list", {"dept_id": int(dept_id), "cursor": cursor, "size": 100})
                if not isinstance(page, dict) or not isinstance(page.get("list"), list):
                    raise RuleError("人员分页格式不完整")
                for item in page["list"]:
                    uid = str(item.get("userid") or "")
                    if not uid:
                        raise RuleError("人员缺少稳定 userId")
                    if uid not in users:
                        users[uid] = self.user(uid)
                if not page.get("has_more"):
                    break
                if not page.get("list") or page.get("next_cursor") is None:
                    raise RuleError("人员分页不完整")
                cursor = int(page["next_cursor"])
        if not users:
            raise RuleError("通讯录为空，禁止执行同步；请检查应用可见范围")
        return sorted(users.values(), key=lambda u: u["source_id"]), sorted(departments.values(), key=lambda d: d["id"])


class ActiveDirectory:
    ATTRS = ["objectGUID", "sAMAccountName", "employeeID", "mail", "displayName", "title", "department", "telephoneNumber", "userAccountControl", "adminCount", "objectSid", "lockoutTime", "isCriticalSystemObject"]
    MATCH = {"employee_id": "employeeID", "email": "mail", "source_id": "sAMAccountName"}

    def __init__(self):
        if not all((settings.LDAP_HOST, settings.LDAP_BIND_DN, settings.LDAP_PASSWORD, settings.LDAP_BASE_DN)):
            raise RuleError("请配置 LDAPS 服务器、绑定账号和目录根 DN")
        try:
            server = Server(settings.LDAP_HOST, port=636, use_ssl=True, tls=Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=settings.LDAP_CA_FILE), connect_timeout=10)
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
    def account(entry):
        attrs = entry["attributes"]
        def value(name, default=""):
            item = attrs.get(name, default)
            return item[0] if isinstance(item, list) and item else (default if isinstance(item, list) else item)
        uac = int(value("userAccountControl", 0))
        sid = str(value("objectSid"))
        critical = str(value("isCriticalSystemObject")).lower() == "true"
        return {"guid": str(uuid.UUID(str(value("objectGUID")).strip("{}"))), "dn": entry["dn"],
                "username": str(value("sAMAccountName")), "employee_id": str(value("employeeID")),
                "email": str(value("mail")), "enabled": not bool(uac & 2), "uac": uac,
                "protected": critical or int(value("adminCount", 0)) == 1 or sid.endswith(("-500", "-501", "-502")) or bool(uac & (2048 | 4096 | 8192)),
                "locked": bool(int(value("lockoutTime", 0))),
                "attrs": {k: str(value(k)) for k in ["displayName", "mail", "title", "department", "telephoneNumber"]}}

    def accounts(self):
        return [self.account(e) for e in self.search("(&(objectCategory=person)(objectClass=user))")]

    def match(self, field, value):
        if field not in self.MATCH or not value:
            raise RuleError("可信身份字段为空，不能匹配 AD 账号")
        query = f"(&(objectCategory=person)(objectClass=user)({self.MATCH[field]}={escape_filter_chars(value)}))"
        return [self.account(e) for e in self.search(query)]

    def by_guid(self, guid):
        escaped = "".join(f"\\{b:02x}" for b in uuid.UUID(str(guid)).bytes_le)
        entries = self.search(f"(&(objectCategory=person)(objectClass=user)(objectGUID={escaped}))")
        if len(entries) != 1:
            raise RuleError("AD 目标对象不存在或不唯一")
        return self.account(entries[0])

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

    def create(self, user, username, ou, root):
        if self.match("source_id", username):
            raise RuleError("AD 用户名已存在，需重新预览")
        if not under(ou, root):
            raise RuleError("目标 OU 超出管理范围")
        dn = f"CN={escape_rdn(username)},{ou}"
        attrs = {"sAMAccountName": username, "displayName": user["name"], "employeeID": user["employee_id"], "userAccountControl": 514}
        if not self.conn.add(dn, ["top", "person", "organizationalPerson", "user"], attrs):
            raise RuleError("AD 创建账号失败")
        matches = self.match("source_id", username)
        if len(matches) != 1:
            raise RuleError("AD 已创建但无法确认对象，需人工核验")
        account = matches[0]
        alphabet = string.ascii_letters + string.digits
        password = "Aa1!" + "".join(secrets.choice(alphabet) for _ in range(28))
        if not self.conn.extend.microsoft.modify_password(dn, password):
            raise RuleError("账号已创建但初始化密码失败，已保持禁用，请人工处理")
        if not self.conn.modify(dn, {"userAccountControl": [(MODIFY_REPLACE, [512])], "pwdLastSet": [(MODIFY_REPLACE, [0])]}):
            raise RuleError("账号初始化未完成，需人工处理")
        return self.by_guid(account["guid"])

    def update(self, guid, attrs, ou, root):
        account = self.check_account(guid)
        if not under(account["dn"], root) or not under(ou, root):
            raise RuleError("账号或目标 OU 超出管理范围")
        changes = {k: [(MODIFY_REPLACE, [v])] for k, v in attrs.items() if v and account["attrs"].get(k) != v}
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

    def reset_password(self, guid, password, unlock=False):
        account = self.check_account(guid)
        try:
            if not self.conn.extend.microsoft.modify_password(account["dn"], password):
                raise RuleError("AD 拒绝密码，请检查复杂度和密码历史要求")
            if unlock and not self.conn.modify(account["dn"], {"lockoutTime": [(MODIFY_REPLACE, [0])]}):
                return "密码已重置，但解锁失败，请联系管理员"
            return "密码已成功重置"
        except RuleError:
            raise
        except Exception:
            raise RuleError("目录响应中断，密码修改结果不明；请先验证或联系管理员") from None
