"""Single worker orchestration with immutable plans and per-person outcomes."""
import uuid
from collections import Counter
from contextlib import closing

from django.conf import settings
from django.core import signing
from django.db import transaction
from django.utils import timezone
from ldap3.utils.dn import escape_rdn

from .directory import DingTalk, ActiveDirectory, under
from .domain import RuleError, candidate, fingerprint, protected, resolve
from .locking import lock
from .models import Configuration, Snapshot, Person, Binding, DepartmentBinding, Job, Operation, RuntimeState
from .security import audit


def configuration_signature(config):
    return fingerprint([dict(Configuration.objects.filter(pk=config.pk).values().get()), settings.DINGTALK_CORP_ID, settings.DINGTALK_APP_KEY, settings.LDAP_HOST, settings.LDAP_BASE_DN, settings.LDAP_VERIFY_CERT, settings.LDAP_CA_FILE])


def binding_signature():
    return fingerprint([list(Binding.objects.order_by("pk").values()), list(Person.objects.order_by("pk").values()), list(DepartmentBinding.objects.order_by("pk").values())])


def collect(source, config):
    started_at = timezone.now()
    anchor = fingerprint([settings.DINGTALK_CORP_ID, settings.DINGTALK_APP_KEY, settings.LDAP_HOST, settings.LDAP_BASE_DN])
    if config.identity_anchor and config.identity_anchor != anchor:
        raise RuleError("企业或 AD 目录已更换，禁止复用旧组织绑定；请使用新的数据库")
    users, departments = source.collect(config.root_department)
    if not users or len({u["source_id"] for u in users}) != len(users):
        raise RuleError("通讯录为空或来源身份重复，禁止同步")
    signature = fingerprint([users, departments, config.root_department])
    # Persist only complete successful snapshots.
    with transaction.atomic():
        if not config.identity_anchor:
            config.identity_anchor = anchor
            config.save(update_fields=["identity_anchor"])
        snap = Snapshot.objects.create(started_at=started_at, fingerprint=signature, root_department=config.root_department, users=users, departments=departments)
        for user in users:
            Person.objects.update_or_create(source_id=user["source_id"], defaults={"name": user["name"]})
    return snap


def department_dn(dept_id, departments, config, overrides):
    if dept_id in overrides and overrides[dept_id].manual:
        dn = overrides[dept_id].dn
        if not under(dn, config.root_ou):
            raise RuleError("部门 OU 超出管理范围")
        return dn
    if dept_id == config.root_department:
        return config.root_ou
    parts, seen = [], set()
    current = dept_id
    while current != config.root_department:
        if not current or current in seen or current not in departments:
            raise RuleError("主部门不在同步范围或部门结构不完整")
        seen.add(current)
        dept = departments[current]
        parts.append("OU=" + escape_rdn(dept["name"]))
        current = dept["parent"]
        if current in overrides and overrides[current].manual:
            return ",".join(parts + [overrides[current].dn])
    return ",".join(parts + [config.root_ou])


def selected_users(snap, scope, selected):
    users = snap.users
    if scope == "full":
        return users
    if not selected:
        raise RuleError("局部同步必须选择人员或部门")
    if scope == "users":
        if set(selected) - {u["source_id"] for u in users}:
            raise RuleError("选定人员不在当前完整通讯录中")
        return [u for u in users if u["source_id"] in selected]
    if scope == "department":
        if set(selected) - {d["id"] for d in snap.departments}:
            raise RuleError("选定部门不在当前通讯录中")
        included = set(selected)
        while True:
            expanded = included | {d["id"] for d in snap.departments if d["parent"] in included}
            if expanded == included:
                break
            included = expanded
        return [u for u in users if included.intersection(u["departments"])]
    raise RuleError("同步范围无效")


def department_plans(snap, scope, selected, users, config, overrides, ad):
    departments = {d["id"]: d for d in snap.departments}
    if scope == "full":
        included = set(departments)
    elif scope == "department":
        included = set(selected)
        while True:
            expanded = included | {d["id"] for d in departments.values() if d["parent"] in included}
            if expanded == included:
                break
            included = expanded
    else:
        people = {p.source_id: p for p in Person.objects.all()}
        included = {people[u["source_id"]].primary_department or u["primary_department"] for u in users}
        included.discard("")
    # Include ancestors even when they have no direct members.
    for dept in list(included):
        seen = set()
        while dept != config.root_department and dept in departments:
            if dept in seen:
                raise RuleError("部门层级存在循环")
            seen.add(dept)
            dept = departments[dept]["parent"]
            if dept in departments:
                included.add(dept)
    result, paths = [], {}
    for dept_id in sorted(included):
        entry = {"source_id": dept_id, "name": departments.get(dept_id, {}).get("name", dept_id), "action": "ensure_ou", "dn": "", "guid": None, "reason": "按来源部门建立关联"}
        try:
            dn = department_dn(dept_id, departments, config, overrides)
            entry["dn"] = dn
            binding = overrides.get(dept_id)
            if binding and not binding.manual and binding.dn.casefold() != dn.casefold():
                raise RuleError("部门改名或调整层级，请人工确认 OU")
            current = ad.ou_identity(dn)
            if binding and current != str(binding.object_guid):
                raise RuleError("关联 OU 不存在或对象已被替换")
            entry["guid"] = current
            entry["reason"] = "关联现有 OU" if current else "创建 OU"
            key = dn.casefold()
            if key in paths and not (binding and binding.manual and overrides.get(paths[key], None) and overrides[paths[key]].manual):
                raise RuleError("多个部门映射到同一 OU，请人工指定")
            paths[key] = dept_id
        except RuleError as exc:
            entry["action"], entry["reason"] = "conflict", str(exc)
        result.append(entry)
    return sorted(result, key=lambda item: len(item["dn"]))


def plan(job, source, ad):
    config = Configuration.current()
    if not config.root_ou or not under(config.root_ou, settings.LDAP_BASE_DN):
        raise RuleError("请设置 LDAP 目录范围内的同步根 OU")
    ad.verify_ou(config.root_ou)
    snap = collect(source, config)
    accounts = ad.accounts()
    bindings = {b.person.source_id: b for b in Binding.objects.select_related("person")}
    people = {p.source_id: p for p in Person.objects.all()}
    departments = {d["id"]: d for d in snap.departments}
    overrides = {d.source_id: d for d in DepartmentBinding.objects.all()}
    occupied = {str(b.object_guid) for b in bindings.values()}
    employee_counts = Counter(u.get(config.match_field, "").strip().casefold() for u in snap.users)
    actual_employee_counts = Counter(u.get("employee_id", "").strip().casefold() for u in snap.users)
    name_counts = Counter(candidate(u, config.naming).casefold() for u in snap.users)
    operations = []
    chosen_users = selected_users(snap, job.scope, job.selected)
    ou_plans = department_plans(snap, job.scope, job.selected, chosen_users, config, overrides, ad)
    for user in chosen_users:
        person, binding = people[user["source_id"]], bindings.get(user["source_id"])
        b = {"guid": str(binding.object_guid), "enabled": binding.enabled} if binding else None
        recovery = None
        if not binding:
            recovery = Operation.objects.filter(source_id=user["source_id"], action="create").order_by("-pk").first()
            if recovery and recovery.target_guid:
                # A prior AD write can outlive its local binding transaction.
                # Preserve its object identity even if source attributes changed.
                b = {"guid": str(recovery.target_guid), "enabled": True}
        action, target, reason = resolve(user, b, accounts, occupied, config.naming, employee_counts, name_counts, config.match_field, config.protected_usernames)
        if recovery and not recovery.target_guid:
            action, reason = "conflict", "此前建号结果缺少可靠对象证据，请人工核验并绑定，禁止自动重建"
        elif recovery and action == "update":
            reason = "核验此前已创建的 AD 对象，补全未完成的同步绑定"
            if target["guid"] in occupied:
                action, reason = "conflict", "此前创建的 AD 对象已绑定其他人员，请人工核验"
        if action == "create" and actual_employee_counts[user.get("employee_id", "").strip().casefold()] != 1:
            action, reason = "conflict", "工号重复，不能创建账号"
        if person.excluded:
            action, reason = "skip", "已排除同步"
        ou = ""
        try:
            if action not in {"skip", "conflict"}:
                dept = person.primary_department or user["primary_department"]
                if not dept or dept not in user["departments"]:
                    raise RuleError("主部门不明确，请在人员页面指定")
                ou = department_dn(dept, departments, config, overrides)
                if dept in overrides and not overrides[dept].manual and overrides[dept].dn.casefold() != ou.casefold():
                    raise RuleError("部门名称或层级发生变化，请人工确认 OU 对应关系")
                if dept in overrides:
                    ad.verify_ou(overrides[dept].dn, str(overrides[dept].object_guid))
                if target and not under(target["dn"], config.root_ou):
                    raise RuleError("已有账号不在受管 OU 内")
        except RuleError as exc:
            action, reason = "conflict", str(exc)
        values = {"displayName": user["name"], "mail": user["email"], "title": user["title"], "telephoneNumber": user["phone"], "department": departments.get(person.primary_department or user["primary_department"], {}).get("name", "")}
        attrs = {k: v for k, v in values.items() if k in config.attributes and (v or k in config.clear_attributes)}
        before = dict(target["attrs"]) if target else {}
        changes = [{"field": k, "before": before.get(k, ""), "after": v} for k, v in attrs.items() if before.get(k, "") != v]
        if target and action in {"update", "bind"} and parent_dn(target["dn"]).casefold() != ou.casefold():
            changes.append({"field": "OU", "before": parent_dn(target["dn"]), "after": ou})
            if action == "update":
                action = "move"
        operations.append({"source_id": user["source_id"], "user": user, "department_id": person.primary_department or user["primary_department"], "action": action, "target": target, "username": target["username"] if target else candidate(user, config.naming), "candidate": candidate(user, config.naming), "raw_naming_value": user.get(config.naming, ""), "binding": {"guid": str(binding.object_guid), "username": binding.username, "manual": binding.manual} if binding else None, "ou": ou, "attrs": attrs, "changes": changes, "reason": reason})
        if target and action == "bind":
            occupied.add(target["guid"])
    if job.scope == "full" and config.disable_missing:
        current_ids = {u["source_id"] for u in snap.users}
        for uid, binding in bindings.items():
            if uid in current_ids or not binding.enabled or binding.person.excluded:
                continue
            target = next((a for a in accounts if a["guid"] == str(binding.object_guid)), None)
            action = "skip"
            reason = "目标已禁用或不在管理范围"
            if target and target["enabled"] and not protected(target) and under(target["dn"], config.root_ou):
                action, reason = "disable", "完整全量中缺失的受管人员"
            operations.append({"source_id": uid, "action": action, "target": target, "username": binding.username, "reason": reason, "changes": [{"field": "enabled", "before": True, "after": False}] if action == "disable" else []})
    disables = sum(o["action"] == "disable" for o in operations)
    threshold = disables > config.disable_limit or disables * 100 > max(len(bindings), 1) * config.disable_percent
    return {"snapshot": snap.pk, "source": snap.fingerprint, "config": configuration_signature(config), "bindings": binding_signature(), "operations": operations, "departments": ou_plans, "high_risk": threshold, "scope": job.scope, "selected": job.selected}


def parent_dn(dn):
    from ldap3.utils.dn import parse_dn
    return "".join(a + "=" + b + sep for a, b, sep in parse_dn(dn)[1:]).rstrip(",")


def has_conflicts(plan_data):
    return any(o["action"] == "conflict" for o in plan_data.get("operations", []) + plan_data.get("departments", []))


def apply(job, source, ad):
    config = Configuration.current()
    saved = job.plan
    if not saved or saved.get("scope") != job.scope or saved.get("selected") != job.selected:
        raise RuleError("缺少有效预览")
    if saved["config"] != configuration_signature(config) or saved["bindings"] != binding_signature():
        raise RuleError("配置或绑定已变化，请重新预览")
    users, departments = source.collect(config.root_department)
    if fingerprint([users, departments, config.root_department]) != saved["source"]:
        raise RuleError("通讯录已变化，请重新预览")
    latest = Snapshot.objects.order_by("-pk").first()
    if not latest or latest.fingerprint != saved["source"]:
        raise RuleError("当前采集版本已变化，请重新预览")
    if has_conflicts(saved):
        raise RuleError("请先处理人员冲突，再重新预览")
    if saved["high_risk"] and not job.confirmed:
        raise RuleError("禁用数量超过阈值，需要管理员确认")
    # Preflight every existing target before the first write.
    for op in saved["operations"]:
        if op["action"] == "create":
            if ad.match("employee_id", op["user"]["employee_id"]) or ad.match("source_id", op["username"]):
                raise RuleError("预览后出现同工号或同名 AD 账号，请重新预览")
        if op["action"] not in {"skip", "conflict"} and op.get("target"):
            current = ad.by_guid(op["target"]["guid"])
            if fingerprint(current) != fingerprint(op["target"]):
                raise RuleError("AD 状态已变化，请重新预览")
    for dept in saved.get("departments", []):
        if ad.ou_identity(dept["dn"]) != dept["guid"]:
            raise RuleError("OU 状态已变化，请重新预览")
    for dept in saved.get("departments", []):
        record = Operation.objects.create(job=job, source_id="department:" + dept["source_id"], action="ensure_ou", evidence={"dn": dept["dn"], "name": dept["name"]})
        try:
            guid = ad.ensure_ou(dept["dn"], config.root_ou)
            DepartmentBinding.objects.get_or_create(source_id=dept["source_id"], defaults={"dn": dept["dn"], "object_guid": guid})
            record.target_guid, record.status = guid, "success"
            record.save()
        except Exception as exc:
            record.status = "failed"
            record.message = str(exc) if isinstance(exc, RuleError) else "OU 操作失败，请核验后重新预览"
            record.save()
            return "partial_failed"
    failed = False
    for op in saved["operations"]:
        record = Operation.objects.create(job=job, source_id=op["source_id"], action=op["action"], target_guid=op["target"]["guid"] if op.get("target") else None, evidence={"username": op.get("username", ""), "reason": op["reason"], "changes": op.get("changes", [])})
        if op["action"] == "skip":
            record.status = "skipped"
            record.save()
            continue
        try:
            key = op["target"]["guid"] if op.get("target") else "source-" + op["source_id"]
            with lock("account:" + key):
                if op.get("target") and fingerprint(ad.by_guid(key)) != fingerprint(op["target"]):
                    raise RuleError("执行前目标状态发生变化，请重新预览")
                if op["action"] == "disable":
                    ad.disable(key, config.root_ou)
                    record.target_guid = key
                else:
                    ou_binding = DepartmentBinding.objects.filter(source_id=op["department_id"]).first()
                    if ou_binding:
                        ad.verify_ou(ou_binding.dn, str(ou_binding.object_guid))
                    ou_guid = ad.ensure_ou(op["ou"], config.root_ou)
                    if not ou_binding:
                        DepartmentBinding.objects.create(source_id=op["department_id"], dn=op["ou"], object_guid=ou_guid)
                    account = op["target"]
                    if op["action"] == "create":
                        account = ad.create(op["user"], op["username"], op["ou"], config.root_ou, enabled=False, require_change=config.require_password_change)
                        # Write recovery evidence before any additional AD operation.
                        record.target_guid = account["guid"]
                        record.status = "created"
                        record.save()
                    account = ad.update(account["guid"], op["attrs"], op["ou"], config.root_ou, allow_disabled=op["action"] == "create")
                    if op["action"] == "create" and config.enable_new_accounts:
                        account = ad.enable(account["guid"], config.root_ou)
                    record.target_guid = account["guid"]
                    # Each successful user commits independently of later users.
                    with transaction.atomic():
                        person = Person.objects.get(source_id=op["source_id"])
                        existing = Binding.objects.filter(person=person).first()
                        if existing and str(existing.object_guid) != account["guid"]:
                            raise RuleError("绑定发生变化")
                        if not existing:
                            Binding.objects.create(person=person, object_guid=account["guid"], username=account["username"], enabled=op["action"] != "create" or config.enable_new_accounts)
                        else:
                            existing.username = account["username"]
                            existing.save(update_fields=["username", "updated_at"])
                        record.status = "success"
                        record.save()
                record.status = "success"
                record.save()
        except Exception as exc:
            failed = True
            record.status = "failed"
            record.message = str(exc) if isinstance(exc, RuleError) else "操作未完整完成，请核验 AD 后重新预览"
            record.save()
    return "partial_failed" if failed else "success"


def enqueue(kind="preview", scope="full", selected=None, actor="scheduler"):
    if kind not in {"preview", "scheduled", "connections", "refresh"} or scope not in {"full", "users", "department"}:
        raise RuleError("任务类型或范围无效")
    with lock("enqueue"), transaction.atomic():
        existing = Job.objects.filter(status__in=["queued", "running"]).first()
        if existing:
            raise RuleError("已有排队或执行中的任务")
        return Job.objects.create(kind=kind, scope=scope, selected=selected or [], actor=actor)


def queue_apply(job_id, actor, confirmed=False):
    with lock("enqueue"), transaction.atomic():
        job = Job.objects.get(pk=job_id)
        if job.status not in {"preview_ready", "needs_confirmation"} or job.kind != "preview":
            raise RuleError("此预览不可重复执行")
        if Job.objects.filter(status__in=["queued", "running"]).exists():
            raise RuleError("已有排队或执行中的任务")
        if job.plan.get("high_risk") and not confirmed:
            raise RuleError("请确认受影响的禁用账号清单")
        job.kind, job.status, job.actor, job.confirmed = "apply", "queued", actor, confirmed
        job.save()
        return job


def check_connections():
    checks = {}
    for name, factory in [("钉钉通讯录", DingTalk), ("LDAPS", ActiveDirectory)]:
        try:
            with closing(factory()) as client:
                if name == "钉钉通讯录":
                    users, departments = client.collect(Configuration.current().root_department)
                    if not users or not departments:
                        raise RuleError("通讯录为空，请检查权限和范围")
                else:
                    client.accounts()
            checks[name] = {"success": True, "message": "读取成功"}
        except Exception as exc:
            checks[name] = {"success": False, "message": str(exc) if isinstance(exc, RuleError) else "连接失败，请检查服务及凭据配置"}
    state = RuntimeState.current()
    state.connection_checks = checks
    state.connections_checked_at = timezone.now()
    state.save(update_fields=["connection_checks", "connections_checked_at"])
    return checks


def run_next():
    with lock("sync"):
        # Acquiring the OS lock proves there is no surviving worker executing an old job.
        Job.objects.filter(status="running").update(status="failed", message="进程中断，请核验逐项结果后重新预览", finished_at=timezone.now())
        job = Job.objects.filter(status="queued").order_by("created_at").first()
        if not job:
            return False
        job.status, job.started_at = "running", timezone.now()
        job.save(update_fields=["status", "started_at"])
        try:
            if job.kind == "connections":
                checks = check_connections()
                job.status = "success" if all(item["success"] for item in checks.values()) else "failed"
                job.message = "；".join(f"{name}：{item['message']}" for name, item in checks.items())
            elif job.kind == "refresh":
                with closing(DingTalk()) as source:
                    snap = collect(source, Configuration.current())
                    job.message = f"完整读取 {len(snap.users)} 名人员、{len(snap.departments)} 个部门"
                    job.status = "success"
            else:
                run_sync_job(job)
        except Exception as exc:
            job.status = "failed"
            job.message = str(exc) if isinstance(exc, RuleError) else "任务失败，请检查连接或联系管理员；不会自动重放写入"
        job.finished_at = timezone.now()
        job.save()
        if job.scope == "full" and job.status == "success" and job.kind in {"apply", "scheduled"}:
            state = RuntimeState.current()
            state.last_full_success = job.finished_at
            state.save(update_fields=["last_full_success"])
        audit(job.actor, job.kind, str(job.pk), job.status, success=job.status in {"success", "preview_ready"})
        return True


def run_sync_job(job):
    with closing(DingTalk()) as source, closing(ActiveDirectory()) as ad:
        if job.kind != "apply":
            job.plan = plan(job, source, ad)
            job.save(update_fields=["plan"])
            if has_conflicts(job.plan):
                job.status = "blocked"
            elif job.plan["high_risk"]:
                job.status = "needs_confirmation"
                job.kind = "preview"
            elif job.kind == "scheduled":
                job.status = apply(job, source, ad)
            else:
                job.status = "preview_ready"
        else:
            job.status = apply(job, source, ad)



def binding_review(person_id, username):
    with closing(DingTalk()) as source, closing(ActiveDirectory()) as ad:
        person = Person.objects.get(pk=person_id)
        config = Configuration.current()
        user = source.user(person.source_id)
        if not source.user_in_scope(user, config.root_department):
            raise RuleError("来源人员已不在当前同步范围，不能建立绑定")
        matches = ad.match("source_id", username.strip())
        if len(matches) != 1 or protected(matches[0]):
            raise RuleError("目标不存在、不唯一或受保护")
        target = matches[0]
        if not under(target["dn"], config.root_ou):
            raise RuleError("目标不在同步管理范围内")
        if Binding.objects.filter(object_guid=target["guid"]).exclude(person=person).exists():
            raise RuleError("目标已绑定其他人员")
        old = Binding.objects.filter(person=person).first()
        payload = {"person": person.pk, "username": target["username"], "guid": target["guid"], "revision": str(old.revision) if old else ""}
        return {"person": person, "old": old, "target": target, "confirmation": signing.dumps(payload, salt="binding-review")}


def bind_person(person_id, confirmation, actor, reason):
    if not reason.strip():
        raise RuleError("请填写绑定变更原因")
    try:
        reviewed = signing.loads(confirmation, salt="binding-review", max_age=300)
    except signing.BadSignature:
        raise RuleError("绑定确认已失效，请重新验证目标") from None
    if reviewed["person"] != person_id:
        raise RuleError("绑定确认对象不一致")
    with lock("sync"), closing(DingTalk()) as source, closing(ActiveDirectory()) as ad:
        person = Person.objects.get(pk=person_id)
        config = Configuration.current()
        user = source.user(person.source_id)
        if not source.user_in_scope(user, config.root_department):
            raise RuleError("来源人员已不在当前同步范围，不能建立绑定")
        matches = ad.match("source_id", reviewed["username"])
        if len(matches) != 1 or protected(matches[0]):
            raise RuleError("目标不存在、不唯一或受保护")
        account = matches[0]
        if account["guid"] != reviewed["guid"]:
            raise RuleError("AD 目标已变化，请重新验证")
        if not under(account["dn"], config.root_ou):
            raise RuleError("目标不在同步管理范围内")
        with lock("account:" + account["guid"]), transaction.atomic():
            old = Binding.objects.filter(person=person).first()
            if (str(old.revision) if old else "") != reviewed["revision"]:
                raise RuleError("当前绑定已变化，请重新确认")
            if Binding.objects.filter(object_guid=account["guid"]).exclude(person=person).exists():
                raise RuleError("目标已绑定其他人员")
            Binding.objects.update_or_create(person=person, defaults={"object_guid": account["guid"], "username": account["username"], "manual": True, "enabled": account["enabled"], "revision": uuid.uuid4()})
            audit(actor, "manual_bind", person.source_id, f"{str(old.object_guid) if old else '未绑定'} → {account['guid']}；{reason[:150]}")


def reactivate_person(person_id, actor, reason, confirmed):
    if not confirmed or not reason.strip():
        raise RuleError("恢复启用必须确认并填写原因")
    with lock("sync"), closing(DingTalk()) as source, closing(ActiveDirectory()) as ad:
        binding = Binding.objects.select_related("person").filter(person_id=person_id).first()
        if not binding or binding.person.excluded:
            raise RuleError("请先确认绑定且人员未排除同步")
        config = Configuration.current()
        user = source.user(binding.person.source_id)
        if not source.user_in_scope(user, config.root_department):
            raise RuleError("来源人员已不在当前同步范围，不能恢复 AD 账号")
        with lock("account:" + str(binding.object_guid)):
            target = ad.by_guid(binding.object_guid)
            if target["enabled"]:
                if binding.enabled:
                    raise RuleError("AD 账号与同步绑定均已启用")
                if protected(target) or not under(target["dn"], config.root_ou):
                    raise RuleError("目标受保护或不在管理范围内")
            else:
                ad.enable(binding.object_guid, config.root_ou)
            with transaction.atomic():
                binding.enabled = True
                binding.revision = uuid.uuid4()
                binding.save(update_fields=["enabled", "revision", "updated_at"])
                audit(actor, "reactivate", binding.person.source_id, f"恢复 {binding.object_guid}；{reason[:150]}")


def verify_binding(person_id):
    binding = Binding.objects.select_related("person").filter(person_id=person_id).first()
    if not binding:
        raise RuleError("该人员尚未绑定")
    with closing(ActiveDirectory()) as ad:
        account = ad.by_guid(binding.object_guid)
        return {"person": binding.person, "binding": binding, "account": account}


def change_person(person_id, actor, excluded, primary_department):
    with lock("sync"), transaction.atomic():
        person = Person.objects.get(pk=person_id)
        if primary_department and primary_department != person.primary_department:
            snapshot = Snapshot.objects.order_by("-pk").first()
            user = next((item for item in snapshot.users if item["source_id"] == person.source_id), None) if snapshot else None
            in_scope = {str(department["id"]) for department in snapshot.departments} if snapshot else set()
            memberships = {str(value) for value in user.get("departments", [])} if user else set()
            if primary_department not in memberships or primary_department not in in_scope:
                raise RuleError("指定主部门必须是该人员当前所属且在同步范围内的来源部门")
        person.excluded = excluded
        person.primary_department = primary_department
        person.save()
        audit(actor, "person_policy", person.source_id)


def unbind_person(person_id, actor):
    with lock("sync"), transaction.atomic():
        person = Person.objects.get(pk=person_id)
        Binding.objects.filter(person=person).delete()
        person.excluded = True
        person.save(update_fields=["excluded"])
        audit(actor, "unbind", person.source_id, "解除同步绑定并排除自动重新认领；不影响独立 LDAPS 密码重置")
