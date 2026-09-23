"""Single worker orchestration with immutable plans and per-person outcomes."""
import uuid
from collections import Counter
from contextlib import closing

from django.conf import settings
from django.db import transaction
from django.utils import timezone
from ldap3.utils.dn import escape_rdn

from .directory import DingTalk, ActiveDirectory, under
from .domain import RuleError, candidate, fingerprint, protected, resolve
from .locking import lock
from .models import Configuration, Snapshot, Person, Binding, DepartmentBinding, Job, Operation
from .security import audit


def configuration_signature(config):
    return fingerprint([dict(Configuration.objects.filter(pk=config.pk).values().get()), settings.DINGTALK_CORP_ID, settings.DINGTALK_APP_KEY, settings.LDAP_HOST, settings.LDAP_BASE_DN])


def binding_signature():
    return fingerprint([list(Binding.objects.order_by("pk").values()), list(Person.objects.order_by("pk").values()), list(DepartmentBinding.objects.order_by("pk").values())])


def collect(source, config):
    anchor = fingerprint([settings.DINGTALK_CORP_ID, settings.LDAP_HOST, settings.LDAP_BASE_DN])
    if config.identity_anchor and config.identity_anchor != anchor:
        raise RuleError("企业或 AD 目录已更换，禁止复用旧组织绑定；请使用新的数据库")
    if not config.identity_anchor:
        config.identity_anchor = anchor
        config.save(update_fields=["identity_anchor"])
    users, departments = source.collect(config.root_department)
    if not users or len({u["source_id"] for u in users}) != len(users):
        raise RuleError("通讯录为空或来源身份重复，禁止同步")
    signature = fingerprint([users, departments, config.root_department])
    # Persist only complete successful snapshots.
    with transaction.atomic():
        snap = Snapshot.objects.create(fingerprint=signature, root_department=config.root_department, users=users, departments=departments)
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
    employee_counts = Counter(u.get("employee_id", "").casefold() for u in snap.users)
    name_counts = Counter(candidate(u, config.naming).casefold() for u in snap.users)
    operations = []
    for user in selected_users(snap, job.scope, job.selected):
        person, binding = people[user["source_id"]], bindings.get(user["source_id"])
        b = {"guid": str(binding.object_guid), "enabled": binding.enabled} if binding else None
        action, target, reason = resolve(user, b, accounts, occupied, config.naming, employee_counts, name_counts)
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
        attrs = {k: v for k, v in values.items() if k in config.attributes and v}
        operations.append({"source_id": user["source_id"], "user": user, "department_id": person.primary_department or user["primary_department"], "action": action, "target": target, "username": target["username"] if target else candidate(user, config.naming), "ou": ou, "attrs": attrs, "reason": reason})
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
            operations.append({"source_id": uid, "action": action, "target": target, "reason": reason})
    disables = sum(o["action"] == "disable" for o in operations)
    threshold = disables > config.disable_limit or disables * 100 > max(len(bindings), 1) * config.disable_percent
    return {"snapshot": snap.pk, "source": snap.fingerprint, "config": configuration_signature(config), "bindings": binding_signature(), "operations": operations, "high_risk": threshold, "scope": job.scope, "selected": job.selected}


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
    if any(o["action"] == "conflict" for o in saved["operations"]):
        raise RuleError("请先处理人员冲突，再重新预览")
    if saved["high_risk"] and not job.confirmed:
        raise RuleError("禁用数量超过阈值，需要管理员确认")
    # Preflight every existing target before the first write.
    for op in saved["operations"]:
        if op["action"] not in {"skip", "conflict"} and op.get("target"):
            current = ad.by_guid(op["target"]["guid"])
            if fingerprint(current) != fingerprint(op["target"]):
                raise RuleError("AD 状态已变化，请重新预览")
    failed = False
    for op in saved["operations"]:
        record = Operation.objects.create(job=job, source_id=op["source_id"], action=op["action"], evidence={"username": op.get("username", ""), "reason": op["reason"]})
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
                        account = ad.create(op["user"], op["username"], op["ou"], config.root_ou)
                        # Write recovery evidence before any additional AD operation.
                        record.target_guid = account["guid"]
                        record.status = "created"
                        record.save()
                    account = ad.update(account["guid"], op["attrs"], op["ou"], config.root_ou)
                    record.target_guid = account["guid"]
                    # Each successful user commits independently of later users.
                    with transaction.atomic():
                        person = Person.objects.get(source_id=op["source_id"])
                        existing = Binding.objects.filter(person=person).first()
                        if existing and str(existing.object_guid) != account["guid"]:
                            raise RuleError("绑定发生变化")
                        if not existing:
                            Binding.objects.create(person=person, object_guid=account["guid"], username=account["username"])
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
    if kind not in {"preview", "scheduled"} or scope not in {"full", "users", "department"}:
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


def run_next():
    with lock("sync"):
        # Acquiring the OS lock proves there is no surviving worker executing an old job.
        Job.objects.filter(status="running").update(status="failed", message="进程中断，请核验逐项结果后重新预览", finished_at=timezone.now())
        job = Job.objects.filter(status="queued").order_by("created_at").first()
        if not job:
            return False
        job.status = "running"
        job.save(update_fields=["status"])
        try:
            with closing(DingTalk()) as source, closing(ActiveDirectory()) as ad:
                if job.kind != "apply":
                    job.plan = plan(job, source, ad)
                    job.save(update_fields=["plan"])
                    if any(o["action"] == "conflict" for o in job.plan["operations"]):
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
        except Exception as exc:
            job.status = "failed"
            job.message = str(exc) if isinstance(exc, RuleError) else "任务失败，请检查连接或联系管理员；不会自动重放写入"
        job.finished_at = timezone.now()
        job.save()
        audit(job.actor, "sync", str(job.pk), job.status)
        return True


def bind_person(person_id, username, actor, reason):
    if not reason.strip():
        raise RuleError("请填写绑定变更原因")
    with lock("sync"), closing(DingTalk()) as source, closing(ActiveDirectory()) as ad:
        person = Person.objects.get(pk=person_id)
        source.user(person.source_id)
        matches = ad.match("source_id", username)
        if len(matches) != 1 or protected(matches[0]):
            raise RuleError("目标不存在、不唯一或受保护")
        account = matches[0]
        if not under(account["dn"], Configuration.current().root_ou):
            raise RuleError("目标不在同步管理范围内")
        with lock("account:" + account["guid"]), transaction.atomic():
            if Binding.objects.filter(object_guid=account["guid"]).exclude(person=person).exists():
                raise RuleError("目标已绑定其他人员")
            Binding.objects.update_or_create(person=person, defaults={"object_guid": account["guid"], "username": account["username"], "manual": True, "enabled": True, "revision": uuid.uuid4()})
            audit(actor, "manual_bind", person.source_id, "人工绑定：" + reason[:150])


def change_person(person_id, actor, excluded, primary_department):
    with lock("sync"), transaction.atomic():
        person = Person.objects.get(pk=person_id)
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
