"""Persist exact employee/account identities without granting AD write scope."""
import uuid
from collections import Counter
from contextlib import ExitStack, closing
from datetime import timedelta

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .directory import ActiveDirectory, DingTalk, under
from .domain import RuleError, fingerprint, protected
from .locking import lock
from .models import Binding, Configuration, Job, Operation, Person, Snapshot
from .security import audit
from .synchronization import collect, directory_identity_anchor, enqueue, validate_directory_identity


AUTOMATIC_ACTOR = "account-association"


def association_basis(config, snapshot):
    return fingerprint([
        directory_identity_anchor(), config.identity_anchor, config.root_department,
        config.match_field, config.auto_associate_accounts, snapshot.fingerprint,
        sorted(Person.objects.filter(excluded=True).values_list("source_id", flat=True)),
    ])


def enqueue_due_association():
    config = Configuration.current()
    if not config.auto_associate_accounts or config.match_field != "employee_username":
        return False
    snapshot = Snapshot.objects.order_by("-pk").only("fingerprint", "root_department").first()
    if not snapshot or snapshot.root_department != config.root_department:
        return False
    if Job.objects.filter(status__in=("queued", "running")).exists():
        return False
    basis = association_basis(config, snapshot)
    previous = Job.objects.filter(kind="associate").order_by("-created_at").first()
    if (previous and previous.plan.get("association_basis") == basis
            and (previous.finished_at or previous.created_at) > timezone.now() - timedelta(hours=1)):
        return False
    try:
        enqueue(kind="associate", actor=AUTOMATIC_ACTOR,
                plan_seed={"association_basis": basis, "source_fingerprint": snapshot.fingerprint, "operations": []})
    except RuleError:
        return False
    return True


def sync_note(account, config):
    if not account["enabled"]:
        return "AD 账号已禁用，仅维护账号关联"
    if protected(account):
        return "受保护账号，仅维护账号关联"
    if not config.root_ou or not under(account["dn"], config.root_ou):
        return "账号不在同步根 OU 内，仅维护账号关联"
    return "可生成同步预览，确认执行后才纳入同步管理"


def run_association_job(job, source=None, ad=None):
    """Called by the single worker while it owns the synchronization lock."""
    config = Configuration.current()
    validate_directory_identity(config)
    if config.match_field != "employee_username":
        raise RuleError("默认账号关联需要选择钉钉工号 → AD 账号名的匹配方式")
    if job.actor == AUTOMATIC_ACTOR and not config.auto_associate_accounts:
        raise RuleError("自动账号关联已关闭")
    results = []
    linked = 0
    current_result = None
    try:
        with ExitStack() as stack:
            source = stack.enter_context(closing(source if source is not None else DingTalk()))
            snapshot = collect(source, config)
            job.plan = {"association_basis": association_basis(config, snapshot), "source_fingerprint": snapshot.fingerprint,
                        "snapshot": snapshot.pk, "operations": results}
            job.save(update_fields=["plan"])
            ad = stack.enter_context(closing(ad if ad is not None else ActiveDirectory()))
            connection = getattr(ad, "conn", None)
            if connection is not None:
                connection.read_only = True
            accounts = ad.accounts()
            account_names = {}
            account_guids = {}
            for account in accounts:
                account_names.setdefault(account["username"].strip().casefold(), []).append(account)
                account_guids.setdefault(str(account["guid"]), []).append(account)
            counts = Counter(str(user.get("employee_id") or "").strip().casefold() for user in snapshot.users)
            bindings = {item.person.source_id: item for item in Binding.objects.select_related("person")}
            people = {item.source_id: item for item in Person.objects.all()}
            # Do not let association bypass the existing uncertain-create recovery path.
            recovery = Operation.objects.filter(action="create").exclude(source_id__in=bindings)
            reserved_people = set(recovery.values_list("source_id", flat=True))
            occupied = {str(item.object_guid) for item in bindings.values()}
            occupied.update(str(value) for value in recovery.exclude(target_guid=None).values_list("target_guid", flat=True))
            signature = association_basis(config, snapshot)
            for user in snapshot.users:
                person = people[user["source_id"]]
                existing = bindings.get(person.source_id)
                result = {"source_id": person.source_id, "user": {key: user.get(key, "") for key in ("source_id", "name", "employee_id")},
                          "username": "", "target": None, "action": "skip", "reason_code": "no_match", "reason": "未匹配到工号对应的 AD 账号", "sync_note": ""}
                current_result = result
                if person.excluded:
                    result.update(reason_code="excluded", reason="已排除自动关联，保留原设置")
                elif existing:
                    rows = account_guids.get(str(existing.object_guid), [])
                    result.update(username=existing.username, reason_code="retained", reason="保留人工关联" if existing.manual else "保留已有稳定账号关联")
                    if len(rows) != 1:
                        result.update(action="conflict", reason_code="missing_target", reason="已有关联的 AD 对象不存在或不能唯一确认，未重新认领其他账号")
                    else:
                        with lock("account:" + str(existing.object_guid)):
                            target = ad.by_guid(existing.object_guid)
                            if (str(target["guid"]) != str(existing.object_guid)
                                    or not under(target["dn"], settings.LDAP_BASE_DN)):
                                raise RuleError("已有账号关联的 AD 对象已变化，请重新核验")
                            with transaction.atomic():
                                fresh_config = Configuration.current()
                                validate_directory_identity(fresh_config)
                                if association_basis(fresh_config, snapshot) != signature:
                                    raise RuleError("账号关联配置或人员排除设置已变化，请重新核验")
                                fresh_binding = Binding.objects.get(pk=existing.pk)
                                if fresh_binding.object_guid != existing.object_guid or fresh_binding.revision != existing.revision:
                                    raise RuleError("已有账号关联已变化，请重新核验")
                                if fresh_binding.username != target["username"]:
                                    fresh_binding.username = target["username"]
                                    fresh_binding.revision = uuid.uuid4()
                                    fresh_binding.save(update_fields=["username", "revision", "updated_at"])
                                    audit(job.actor, "auto_associate", person.source_id, "同一 AD 对象的账号名已更新", target_username=target["username"])
                                    existing = fresh_binding
                        result["username"] = existing.username
                        result.update(target=target, sync_note=sync_note(target, config))
                        if existing.sync_managed and not existing.enabled:
                            result["reason"] = "关联已停用，保留原对象；不会自动恢复"
                elif person.source_id in reserved_people:
                    result.update(action="conflict", reason_code="recovery_required", reason="此前建号存在执行证据，请先核验恢复，不能自动认领")
                else:
                    identifier = str(user.get("employee_id") or "").strip().casefold()
                    matches = account_names.get(identifier, [])
                    if not identifier:
                        result.update(action="conflict", reason_code="missing_job", reason="钉钉工号缺失，不能默认关联")
                    elif counts[identifier] != 1:
                        result.update(action="conflict", reason_code="duplicate_job", reason="钉钉工号重复，不能默认关联")
                    elif len(matches) > 1:
                        result.update(action="conflict", reason_code="ambiguous_ad", reason="多个 AD 账号匹配工号，不能默认关联")
                    elif matches:
                        target = matches[0]
                        if str(target["guid"]) in occupied:
                            result.update(action="conflict", reason_code="occupied", reason="AD 对象已关联其他人员或保留于建号恢复证据")
                        else:
                            with lock("account:" + str(target["guid"])):
                                current_user = source.user(person.source_id)
                                if (current_user.get("source_id") != person.source_id
                                        or str(current_user.get("employee_id") or "").strip().casefold() != identifier
                                        or not source.user_in_scope(current_user, config.root_department)):
                                    result.update(action="conflict", reason_code="source_changed", reason="钉钉身份或工号已变化，请重新核验")
                                else:
                                    current = ad.by_guid(target["guid"])
                                    if (str(current["guid"]) != str(target["guid"])
                                            or current["username"].strip().casefold() != identifier
                                            or not under(current["dn"], settings.LDAP_BASE_DN)):
                                        result.update(action="conflict", reason_code="ad_changed", reason="AD 目标已变化或超出当前 LDAP 目录，请重新核验")
                                    else:
                                        with transaction.atomic():
                                            fresh_config = Configuration.current()
                                            validate_directory_identity(fresh_config)
                                            if association_basis(fresh_config, snapshot) != signature:
                                                raise RuleError("账号关联配置或人员排除设置已变化，请重新核验")
                                            if Binding.objects.filter(person=person).exists() or Binding.objects.filter(object_guid=current["guid"]).exists():
                                                raise RuleError("账号关联已变化，请重新核验")
                                            Binding.objects.create(person=person, object_guid=current["guid"], username=current["username"],
                                                                   manual=False, enabled=False, sync_managed=False)
                                            audit(job.actor, "auto_associate", person.source_id, "唯一工号与 AD 账号名一致，保存身份关联；尚未纳入同步管理",
                                                  actor_name=current_user.get("name", ""), employee_id=current_user["employee_id"], target_username=current["username"])
                                        occupied.add(str(current["guid"]))
                                        linked += 1
                                        result.update(action="associate", reason_code="linked", reason="唯一工号匹配，已默认关联", username=current["username"], target=current, sync_note=sync_note(current, config))
                # Store only identity/result fields, not complete LDAP attributes.
                if result.get("target"):
                    result["target"] = {key: result["target"].get(key) for key in ("guid", "username", "dn", "enabled", "protected")}
                results.append(result)
                current_result = None
            job.status = "success"
            job.message = f"账号关联核验完成：新增 {linked} 个默认关联；仅保存本地身份关系"
    except Exception as exc:
        if current_result is not None:
            current_result.update(action="conflict", reason_code="failed", reason="关联核验未完成，请检查任务原因后重试")
            current_result["target"] = None
            results.append(current_result)
        job.status = "partial_failed" if linked else "failed"
        job.message = str(exc) if isinstance(exc, RuleError) else "账号关联核验失败，请检查来源、AD 连接或审计；已保存的关联保留"
    job.plan = {**job.plan, "operations": results, "association_counts": dict(Counter(item["reason_code"] for item in results))}
    job.save(update_fields=["plan", "status", "message"])
