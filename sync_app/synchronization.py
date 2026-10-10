"""Single worker orchestration with immutable plans and per-person outcomes."""
import uuid
from collections import Counter
from contextlib import closing
from copy import copy

from django.conf import settings
from django.core import signing
from django.db import transaction
from django.utils import timezone
from ldap3.utils.dn import escape_rdn, parse_dn

from .directory import DingTalk, ActiveDirectory, under, validate_ou_dn
from .domain import RuleError, candidate, fingerprint, protected, resolve
from .locking import lock
from .models import Configuration, Snapshot, Person, Binding, DepartmentBinding, Job, Operation, RuntimeState
from .security import audit


def configuration_signature(config):
    return fingerprint([dict(Configuration.objects.filter(pk=config.pk).values().get()), settings.DINGTALK_CORP_ID, settings.DINGTALK_APP_KEY, settings.LDAP_HOST, settings.LDAP_BASE_DN, settings.LDAP_VERIFY_CERT, settings.LDAP_CA_FILE])


def binding_signature():
    return fingerprint([list(Binding.objects.order_by("pk").values()), list(Person.objects.order_by("pk").values()), list(DepartmentBinding.objects.order_by("pk").values())])


def valid_ad_revision(account):
    revision = str(account.get("ad_revision") or "")
    return revision.isdecimal() and int(revision) > 0


def directory_identity_anchor():
    return fingerprint([settings.DINGTALK_CORP_ID, settings.DINGTALK_APP_KEY, settings.LDAP_HOST, settings.LDAP_BASE_DN])


def validate_directory_identity(config):
    """Do not let a new directory claim previously collected identities."""
    anchor = directory_identity_anchor()
    if config.identity_anchor and config.identity_anchor != anchor:
        raise RuleError("企业或 AD 目录已更换，禁止复用旧组织绑定；请使用新的数据库")
    if not config.identity_anchor and (
        Snapshot.objects.exists() or Binding.objects.exists() or DepartmentBinding.objects.exists()
        or Operation.objects.filter(action="create").exists()
    ):
        raise RuleError("现有来源或绑定缺少目录身份锚点，禁止复用；请使用新的数据库")
    return anchor


def establish_directory_identity(config):
    """Call in a successful first-write transaction while owning the sync lock."""
    anchor = validate_directory_identity(config)
    if not config.identity_anchor:
        config.identity_anchor = anchor
        config.save(update_fields=["identity_anchor"])
    return anchor


def collect(source, config):
    started_at = timezone.now()
    validate_directory_identity(config)
    users, departments = source.collect(config.root_department)
    if not users or len({u["source_id"] for u in users}) != len(users):
        raise RuleError("通讯录为空或来源身份重复，禁止同步")
    signature = fingerprint([users, departments, config.root_department])
    # Persist only complete successful snapshots.
    with transaction.atomic():
        establish_directory_identity(config)
        snap = Snapshot.objects.create(started_at=started_at, fingerprint=signature, root_department=config.root_department, users=users, departments=departments)
        for user in users:
            Person.objects.update_or_create(source_id=user["source_id"], defaults={"name": user["name"]})
    return snap


def effective_configuration(config, departments):
    """Resolve an unset root from this source, without persisting configuration."""
    effective = copy(config)
    roots = [d for d in departments if d["id"] == config.root_department]
    if len(roots) != 1 or not isinstance(roots[0].get("name"), str) or not roots[0]["name"].strip():
        raise RuleError("钉钉根部门缺失或名称无效，无法计算根 OU")
    if not effective.root_ou:
        try:
            base = parse_dn(settings.LDAP_BASE_DN)
        except Exception:
            raise RuleError("请设置有效的 LDAP 目录范围") from None
        effective.root_ou = settings.LDAP_BASE_DN if base[0][0].casefold() == "ou" else "OU=" + escape_rdn(roots[0]["name"]) + "," + settings.LDAP_BASE_DN
    validate_ou_dn(effective.root_ou, settings.LDAP_BASE_DN)
    if not settings.LDAP_BASE_DN:
        raise RuleError("请设置有效的 LDAP 目录范围")
    return effective


def current_effective_configuration(config):
    if config.root_ou:
        return config
    snapshot = Snapshot.objects.order_by("-pk").first()
    if not snapshot or snapshot.root_department != config.root_department:
        raise RuleError("请先刷新通讯录以确定自动根 OU")
    return effective_configuration(config, snapshot.departments)


def department_dn(dept_id, departments, config, overrides):
    validate_ou_dn(config.root_ou)
    # Validate the entire source ancestry, including manually mapped branches.
    chain, seen = [], set()
    current = dept_id
    while True:
        if not current or current in seen or current not in departments:
            raise RuleError("主部门不在同步范围或部门结构不完整")
        seen.add(current)
        dept = departments[current]
        if not isinstance(dept.get("name"), str) or not dept["name"].strip() or "\x00" in dept["name"]:
            raise RuleError("部门名称为空或无效")
        chain.append(current)
        if current == config.root_department:
            break
        current = dept["parent"]
    parts = []
    for current in chain:
        binding = overrides.get(current)
        if binding and binding.manual:
            validate_ou_dn(binding.dn, config.root_ou)
            if current == config.root_department and binding.dn.casefold() != config.root_ou.casefold():
                raise RuleError("钉钉根部门必须对应同步根 OU，请核对根部门映射")
            return ",".join(parts + [binding.dn])
        if current == config.root_department:
            break
        parts.append("OU=" + escape_rdn(departments[current]["name"]))
    return ",".join(parts + [config.root_ou])


def selected_users(snap, scope, selected):
    users = snap.users
    if scope == "organization":
        if selected:
            raise RuleError("仅同步组织架构覆盖完整部门树，无需填写人员或部门 ID")
        return []
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
    if scope in {"full", "organization"}:
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
    return sorted(result, key=lambda item: len(parse_dn(item["dn"])) if item["action"] != "conflict" else 0)


def plan(job, source, ad):
    config = Configuration.current()
    snap = collect(source, config)
    config = effective_configuration(config, snap.departments)
    root_parent = None
    if ad.ou_identity(config.root_ou) is None:
        parent = parent_dn(config.root_ou)
        root_parent = {"dn": parent, "guid": ad.container_identity(parent)}
    accounts = [] if job.scope == "organization" else ad.accounts()
    bindings = {b.person.source_id: b for b in Binding.objects.select_related("person")}
    people = {p.source_id: p for p in Person.objects.all()}
    departments = {d["id"]: d for d in snap.departments}
    overrides = {d.source_id: d for d in DepartmentBinding.objects.all()}
    occupied = {str(b.object_guid) for b in bindings.values()}
    source_match_field = "employee_id" if config.match_field == "employee_username" else config.match_field
    employee_counts = Counter(u.get(source_match_field, "").strip().casefold() for u in snap.users)
    actual_employee_counts = Counter(u.get("employee_id", "").strip().casefold() for u in snap.users)
    name_counts = Counter(candidate(u, config.naming).casefold() for u in snap.users)
    operations = []
    chosen_users = selected_users(snap, job.scope, job.selected)
    ou_plans = department_plans(snap, job.scope, job.selected, chosen_users, config, overrides, ad)
    for user in chosen_users:
        person, binding = people[user["source_id"]], bindings.get(user["source_id"])
        b = {"guid": str(binding.object_guid), "enabled": binding.enabled if binding.sync_managed else True} if binding else None
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
            evidence = recovery.evidence if isinstance(recovery.evidence, dict) else {}
            employee_id = user.get("employee_id", "").strip().casefold()
            reason = "核验此前已创建的 AD 对象，补全未完成的同步绑定"
            if target["guid"] in occupied:
                action, reason = "conflict", "此前创建的 AD 对象已绑定其他人员，请人工核验"
            elif not (recovery.status == "failed" and valid_ad_revision(target)
                      and evidence.get("enabled_fingerprint")
                      and fingerprint(target) == evidence["enabled_fingerprint"]
                      and evidence.get("created_config") == configuration_signature(config)
                      and target["username"] == evidence.get("username")
                      and employee_id and actual_employee_counts[employee_id] == 1
                      and not protected(target) and under(target["dn"], config.root_ou)):
                action, reason = "conflict", "此前创建的 AD 对象状态或配置已变化，请人工核验后处理"
        elif recovery and action == "conflict" and target and not target["enabled"]:
            evidence = recovery.evidence if isinstance(recovery.evidence, dict) else {}
            employee_id = user.get("employee_id", "").strip().casefold()
            if target["guid"] in occupied:
                reason = "此前创建的 AD 对象已绑定其他人员，请人工核验"
            elif (recovery.status == "failed" and valid_ad_revision(target)
                  and fingerprint(target) in {evidence.get("created_fingerprint"), evidence.get("initialized_fingerprint")}
                  and evidence.get("created_config") == configuration_signature(config)
                  and target["username"] == evidence.get("username")
                  and employee_id and actual_employee_counts[employee_id] == 1
                  and not protected(target) and under(target["dn"], config.root_ou)):
                action, reason = "resume_create", "此前建号已保持禁用且状态未变化，继续初始化并补全绑定"
            else:
                reason = "此前建号对象已禁用或状态变化，请人工核验后处理"
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
                if action == "resume_create" and parent_dn(target["dn"]).casefold() != ou.casefold():
                    raise RuleError("此前未完成建号的目标 OU 与当前部门不一致，请人工核验")
        except RuleError as exc:
            action, reason = "conflict", str(exc)
        values = {"displayName": user["name"], "mail": user["email"], "title": user["title"], "telephoneNumber": user["phone"], "department": departments.get(person.primary_department or user["primary_department"], {}).get("name", "")}
        attrs = {k: v for k, v in values.items() if k in config.attributes and (v or k in config.clear_attributes)}
        before = dict(target["attrs"]) if target else {}
        changes = [{"field": k, "before": before.get(k, ""), "after": v} for k, v in attrs.items() if before.get(k, "") != v]
        if action == "resume_create" and config.enable_new_accounts:
            changes.append({"field": "enabled", "before": False, "after": True})
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
            if uid in current_ids or not binding.sync_managed or not binding.enabled or binding.person.excluded:
                continue
            target = next((a for a in accounts if a["guid"] == str(binding.object_guid)), None)
            action = "skip"
            reason = "目标已禁用或不在管理范围"
            if target and target["enabled"] and not protected(target) and under(target["dn"], config.root_ou):
                action, reason = "disable", "完整全量中缺失的受管人员"
            operations.append({"source_id": uid, "action": action, "target": target, "username": binding.username, "reason": reason, "changes": [{"field": "enabled", "before": True, "after": False}] if action == "disable" else []})
    disables = sum(o["action"] == "disable" for o in operations)
    managed_count = sum(binding.sync_managed for binding in bindings.values())
    threshold = disables > config.disable_limit or disables * 100 > max(managed_count, 1) * config.disable_percent
    return {"snapshot": snap.pk, "source": snap.fingerprint, "config": configuration_signature(config), "bindings": binding_signature(), "root_ou": config.root_ou, "root_parent": root_parent, "operations": operations, "departments": ou_plans, "high_risk": threshold, "scope": job.scope, "selected": job.selected}


def parent_dn(dn):
    from ldap3.utils.dn import parse_dn
    return "".join(a + "=" + b + sep for a, b, sep in parse_dn(dn)[1:]).rstrip(",")


def has_conflicts(plan_data):
    return any(o["action"] == "conflict" for o in plan_data.get("operations", []) + plan_data.get("departments", []))


def apply(job, source, ad):
    config = Configuration.current()
    validate_directory_identity(config)
    saved = job.plan
    if not saved or saved.get("scope") != job.scope or saved.get("selected") != job.selected:
        raise RuleError("缺少有效预览")
    if saved["config"] != configuration_signature(config) or saved["bindings"] != binding_signature():
        raise RuleError("配置或绑定已变化，请重新预览")
    users, departments = source.collect(config.root_department)
    if fingerprint([users, departments, config.root_department]) != saved["source"]:
        raise RuleError("通讯录已变化，请重新预览")
    config = effective_configuration(config, departments)
    if saved.get("root_ou") != config.root_ou:
        raise RuleError("根 OU 路径或计划版本已变化，请重新预览")
    if job.scope == "organization" and saved["operations"]:
        raise RuleError("组织架构计划不能包含人员操作，请重新预览")
    latest = Snapshot.objects.order_by("-pk").first()
    if not latest or latest.fingerprint != saved["source"]:
        raise RuleError("当前采集版本已变化，请重新预览")
    if has_conflicts(saved):
        raise RuleError("请先处理计划中的部门或人员冲突，再重新预览")
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
    root_parent = saved.get("root_parent")
    if root_parent and ad.container_identity(root_parent["dn"]) != root_parent["guid"]:
        raise RuleError("根 OU 的父容器已变化，请重新预览")
    for dept in saved.get("departments", []):
        record = Operation.objects.create(job=job, source_id="department:" + dept["source_id"], action="ensure_ou", evidence={"dn": dept["dn"], "name": dept["name"]})
        try:
            guid = ad.ensure_ou(dept["dn"], config.root_ou, allow_root_creation=dept["dn"].casefold() == config.root_ou.casefold() and root_parent is not None)
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
                        record.evidence = {**record.evidence, "created_fingerprint": fingerprint(account), "created_config": saved["config"]}
                        record.save(update_fields=["target_guid", "status", "evidence"])
                    creation_record = record if op["action"] == "create" else (
                        Operation.objects.filter(source_id=op["source_id"], action="create").order_by("-pk").first()
                        if op["action"] == "resume_create" else None
                    )
                    if op["action"] == "resume_create" and not creation_record:
                        raise RuleError("此前建号证据已缺失，请人工核验，不能继续初始化")
                    account = ad.update(account["guid"], op["attrs"], op["ou"], config.root_ou, allow_disabled=op["action"] in {"create", "resume_create"})
                    if creation_record:
                        creation_record.evidence = {**creation_record.evidence, "initialized_fingerprint": fingerprint(account)}
                        creation_record.save(update_fields=["evidence"])
                    if op["action"] in {"create", "resume_create"} and config.enable_new_accounts:
                        account = ad.enable(account["guid"], config.root_ou)
                        if creation_record:
                            creation_record.evidence = {**creation_record.evidence, "enabled_fingerprint": fingerprint(account)}
                            creation_record.save(update_fields=["evidence"])
                    record.target_guid = account["guid"]
                    # Each successful user commits independently of later users.
                    with transaction.atomic():
                        person = Person.objects.get(source_id=op["source_id"])
                        existing = Binding.objects.filter(person=person).first()
                        if existing and str(existing.object_guid) != account["guid"]:
                            raise RuleError("绑定发生变化")
                        if not existing:
                            Binding.objects.create(person=person, object_guid=account["guid"], username=account["username"], enabled=op["action"] not in {"create", "resume_create"} or config.enable_new_accounts)
                        else:
                            existing.username = account["username"]
                            existing.sync_managed = True
                            existing.enabled = account["enabled"]
                            existing.revision = uuid.uuid4()
                            existing.save(update_fields=["username", "sync_managed", "enabled", "revision", "updated_at"])
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


def enqueue(kind="preview", scope="full", selected=None, actor="scheduler", plan_seed=None):
    if kind not in {"preview", "scheduled", "connections", "refresh", "associate"} or scope not in {"full", "users", "department", "organization"}:
        raise RuleError("任务类型或范围无效")
    if scope == "organization" and (kind != "preview" or selected):
        raise RuleError("仅同步组织架构须先预览完整部门树")
    if kind == "associate" and (scope != "full" or selected):
        raise RuleError("账号关联核验必须读取完整来源范围")
    with lock("enqueue"), transaction.atomic():
        existing = Job.objects.filter(status__in=["queued", "running"]).first()
        if existing:
            raise RuleError("已有排队或执行中的任务")
        return Job.objects.create(kind=kind, scope=scope, selected=selected or [], actor=actor, plan=plan_seed or {})


def queue_apply(job_id, actor, confirmed=False):
    with lock("enqueue"), transaction.atomic():
        job = Job.objects.get(pk=job_id)
        if job.status not in {"preview_ready", "needs_confirmation"} or job.kind != "preview":
            raise RuleError("此预览不可重复执行")
        if has_conflicts(job.plan):
            raise RuleError("请先处理计划冲突，再重新预览")
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
            elif job.kind == "associate":
                from .account_associations import run_association_job
                run_association_job(job)
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
        ad.set_read_only(True)
        if job.kind != "apply":
            job.plan = plan(job, source, ad)
            job.save(update_fields=["plan"])
            person_conflicts = sum(op["action"] == "conflict" for op in job.plan["operations"])
            department_conflicts = sum(op["action"] == "conflict" for op in job.plan["departments"])
            if has_conflicts(job.plan):
                job.status = "blocked"
                job.message = f"预览发现 {person_conflicts} 项人员冲突、{department_conflicts} 项部门冲突；未执行 AD 写入"
            elif job.plan["high_risk"]:
                job.status = "needs_confirmation"
                job.kind = "preview"
                disables = sum(op["action"] == "disable" for op in job.plan["operations"])
                job.message = f"预览包含 {disables} 项离职禁用，超过保护阈值；请确认受影响清单"
            elif job.kind == "scheduled" and job.plan["root_parent"]:
                job.status, job.kind = "needs_confirmation", "preview"
                job.message = "同步根 OU 尚不存在；请核对根 OU 创建路径并确认执行"
            elif job.kind == "scheduled":
                ad.set_read_only(False)
                job.status = apply(job, source, ad)
            else:
                job.status = "preview_ready"
                job.message = f"预览完成：{len(job.plan['operations'])} 项人员计划、{len(job.plan['departments'])} 项部门计划；未执行 AD 写入"
        else:
            ad.set_read_only(False)
            job.status = apply(job, source, ad)
        if job.status in {"success", "partial_failed"}:
            outcomes = Counter(Operation.objects.filter(job=job).values_list("status", flat=True))
            if job.status == "success":
                job.message = f"同步执行完成：{outcomes['success']} 项成功、{outcomes['skipped']} 项跳过"
            else:
                job.message = f"同步执行部分失败：{outcomes['success']} 项成功、{outcomes['failed']} 项失败；请核验逐项结果"



def binding_review(person_id, username):
    config = Configuration.current()
    anchor = validate_directory_identity(config)
    with closing(DingTalk()) as source, closing(ActiveDirectory()) as ad:
        person = Person.objects.get(pk=person_id)
        user = source.user(person.source_id)
        if not source.user_in_scope(user, config.root_department):
            raise RuleError("来源人员已不在当前同步范围，不能建立绑定")
        matches = ad.match("source_id", username.strip())
        if len(matches) != 1:
            raise RuleError("目标不存在或不唯一")
        target = matches[0]
        if not under(target["dn"], settings.LDAP_BASE_DN):
            raise RuleError("目标超出当前 LDAP 目录范围")
        if Binding.objects.filter(object_guid=target["guid"]).exclude(person=person).exists():
            raise RuleError("目标已绑定其他人员")
        old = Binding.objects.filter(person=person).first()
        if validate_directory_identity(Configuration.current()) != anchor:
            raise RuleError("企业或 AD 目录已变化，请重新审核绑定")
        payload = {
            "person": person.pk, "username": target["username"], "guid": target["guid"],
            "employee_id": target["employee_id"], "dn": target["dn"], "enabled": target["enabled"],
            "protected": protected(target),
            "revision": str(old.revision) if old else "",
            "identity_anchor": anchor,
        }
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
        anchor = validate_directory_identity(config)
        if reviewed.get("identity_anchor") != anchor:
            raise RuleError("企业或 AD 目录身份确认已失效，请重新审核绑定")
        user = source.user(person.source_id)
        if not source.user_in_scope(user, config.root_department):
            raise RuleError("来源人员已不在当前同步范围，不能建立绑定")
        matches = ad.match("source_id", reviewed["username"])
        if len(matches) != 1:
            raise RuleError("目标不存在或不唯一")
        account = matches[0]
        if account["guid"] != reviewed["guid"]:
            raise RuleError("AD 目标已变化，请重新验证")
        with lock("account:" + account["guid"]):
            current = ad.by_guid(account["guid"])
            if (current["guid"] != reviewed["guid"]
                    or current["username"] != reviewed["username"]
                    or current["employee_id"] != reviewed.get("employee_id")
                    or current["dn"].casefold() != str(reviewed.get("dn", "")).casefold()
                    or current["enabled"] is not reviewed.get("enabled")
                    or protected(current) is not reviewed.get("protected")):
                raise RuleError("AD 目标状态已变化，请重新验证")
            if not under(current["dn"], settings.LDAP_BASE_DN):
                raise RuleError("目标超出当前 LDAP 目录范围")
            with transaction.atomic():
                current_config = Configuration.current()
                if validate_directory_identity(current_config) != reviewed["identity_anchor"]:
                    raise RuleError("企业或 AD 目录已变化，请重新审核绑定")
                old = Binding.objects.filter(person=person).first()
                if (str(old.revision) if old else "") != reviewed["revision"]:
                    raise RuleError("当前绑定已变化，请重新确认")
                if Binding.objects.filter(object_guid=current["guid"]).exclude(person=person).exists():
                    raise RuleError("目标已绑定其他人员")
                establish_directory_identity(current_config)
                managed = bool(old and old.sync_managed and str(old.object_guid) == str(current["guid"]))
                Binding.objects.update_or_create(person=person, defaults={"object_guid": current["guid"], "username": current["username"], "manual": True, "enabled": current["enabled"] if managed else False, "sync_managed": managed, "revision": uuid.uuid4()})
                audit(actor, "manual_bind", person.source_id, f"{str(old.object_guid) if old else '未绑定'} → {current['guid']}；{reason[:150]}")


def reactivate_person(person_id, actor, reason, confirmed):
    if not confirmed or not reason.strip():
        raise RuleError("恢复启用必须确认并填写原因")
    with lock("sync"), closing(DingTalk()) as source, closing(ActiveDirectory()) as ad:
        binding = Binding.objects.select_related("person").filter(person_id=person_id).first()
        if not binding or binding.person.excluded:
            raise RuleError("请先确认绑定且人员未排除同步")
        if not binding.sync_managed:
            raise RuleError("该账号仅维护身份关联，尚未纳入同步管理，不能从此处启用 AD 账号")
        config = current_effective_configuration(Configuration.current())
        validate_directory_identity(config)
        user = source.user(binding.person.source_id)
        if not source.user_in_scope(user, config.root_department):
            raise RuleError("来源人员已不在当前同步范围，不能恢复 AD 账号")
        with lock("account:" + str(binding.object_guid)):
            validate_directory_identity(Configuration.current())
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
    validate_directory_identity(Configuration.current())
    binding = Binding.objects.select_related("person").filter(person_id=person_id).first()
    if not binding:
        raise RuleError("该人员尚未绑定")
    with closing(ActiveDirectory()) as ad:
        account = ad.by_guid(binding.object_guid)
        return {"person": binding.person, "binding": binding, "account": account}


def change_person(person_id, actor, excluded, primary_department):
    with lock("sync"), transaction.atomic():
        validate_directory_identity(Configuration.current())
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
        validate_directory_identity(Configuration.current())
        person = Person.objects.get(pk=person_id)
        Binding.objects.filter(person=person).delete()
        person.excluded = True
        person.save(update_fields=["excluded"])
        audit(actor, "unbind", person.source_id, "解除同步绑定并排除自动重新认领；不影响独立 LDAPS 密码重置")
