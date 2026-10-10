from functools import wraps
from datetime import timedelta
import time
from uuid import UUID

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.views import LoginView
from django.core.paginator import Paginator
from django.db import connection, DatabaseError
from django.db.models import Q
from django.http import JsonResponse, HttpResponseForbidden
from django.shortcuts import redirect, render, get_object_or_404
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.debug import sensitive_post_parameters
from django.views.decorators.http import require_GET, require_POST
from django.views.csrf import csrf_failure as default_csrf_failure
from django.utils.dateparse import parse_date
from django.utils import timezone

from . import sspr, synchronization
from .domain import ResetOutcomeUnknown, RuleError, candidate
from .models import Configuration, Person, Binding, DepartmentBinding, Job, Audit, Snapshot, RuntimeState, EmployeePageSettings, AuthPlatform
from .security import rate_limit, audit, client_address


JOB_STATUS_LABELS = {
    "queued": "排队中", "running": "执行中", "preview_ready": "待审阅",
    "needs_confirmation": "需确认", "blocked": "已阻断", "completed": "已完成",
    "success": "已完成", "failed": "失败", "partial": "部分完成", "partial_failed": "部分失败",
}
JOB_KIND_LABELS = {
    "preview": "同步预览", "refresh": "通讯录刷新", "connections": "连接检测",
    "apply": "执行同步", "scheduled": "定时同步", "associate": "账号关联核验",
}
ACTION_LABELS = {
    "create": "新建账号", "resume_create": "继续建号", "update": "更新属性",
    "move": "移动 OU", "bind": "关联账号", "onboard": "纳管并迁入 OU", "disable": "禁用账号",
    "skip": "跳过", "conflict": "冲突", "ensure_ou": "建立 OU", "associate": "保存账号关联",
}
AUDIT_LABELS = {
    "admin_login": "管理员登录", "settings": "配置变更",
    "employee_page_settings": "员工页面设置",
    "department_binding": "部门映射变更", "sspr_verified": "员工身份验证",
    "sspr_auth_failed": "员工验证失败", "sspr_reset": "员工密码重置",
    "manual_bind": "人工绑定", "reactivate": "恢复账号或绑定",
    "person_policy": "人员同步设置", "unbind": "解除绑定",
    "preview": "同步预览", "refresh": "通讯录刷新",
    "connections": "连接检测", "apply": "执行同步", "scheduled": "定时同步",
    "associate": "账号关联核验", "auto_associate": "自动账号关联",
}
OPERATION_STATUS_LABELS = {
    "success": "已完成", "created": "已创建", "failed": "失败",
    "skipped": "已跳过", "pending": "待执行",
}
ASSOCIATION_REASON_LABELS = {
    "no_match": "未匹配", "missing_job": "缺少工号", "duplicate_job": "工号重复",
    "ambiguous_ad": "多个 AD 候选", "occupied": "账号关联冲突", "recovery_required": "待恢复核验",
    "missing_target": "关联账号待核验", "source_changed": "来源已变化", "ad_changed": "AD 账号已变化",
    "failed": "核验失败", "excluded": "未执行关联",
}


def status_tone(status):
    if status in {"failed", "partial", "partial_failed", "blocked", "conflict", "disable"}:
        return "warning"
    if status in {
        "queued", "running", "preview_ready", "needs_confirmation",
        "create", "resume_create", "update", "move", "bind", "onboard", "ensure_ou", "associate",
    }:
        return "active"
    if status in {"success", "completed", "created"}:
        return "success"
    return "neutral"


def job_progress(job):
    active = job.status in {"queued", "running"}
    if job.status == "queued":
        message = "任务已排队，等待执行进程处理；无需重复提交。"
    elif job.status == "running":
        message = {
            "associate": "正在后台读取通讯录并核验 AD 账号关联；仅保存本地身份关系，不创建部门 OU。",
            "refresh": "正在读取钉钉人员和部门；耗时取决于部门数量及接口响应，不写入 AD。",
            "connections": "正在检查钉钉通讯录和 LDAPS 连接，请稍候。",
            "preview": "正在读取来源并生成变更预览；此阶段不写入 AD。",
            "apply": "正在执行已确认的同步计划，请等待逐项执行结果。",
            "scheduled": "正在处理定时同步任务，请等待最终结果。",
        }.get(job.kind, "任务正在后台处理，请等待结果。")
    else:
        message = job.message or "任务已结束，请查看详情。"
    started = job.started_at or job.created_at
    seconds = max(0, int(((job.finished_at or timezone.now()) - started).total_seconds()))
    elapsed = f"{seconds // 60} 分 {seconds % 60} 秒" if seconds >= 60 else f"{seconds} 秒"
    elapsed = ("已排队 " if job.status == "queued" else "已运行 " if active else "耗时 ") + elapsed
    return {"id": str(job.pk), "active": active, "status": job.status,
            "label": JOB_STATUS_LABELS.get(job.status, job.status), "tone": status_tone(job.status),
            "message": message, "elapsed": elapsed}


def department_results(job):
    # The immutable preview describes the directory before Apply. Overlay only
    # this job's OU evidence, not current bindings or an operation-results page.
    records = {
        record.source_id: record for record in job.operation_set.filter(action="ensure_ou")
        .only("source_id", "status", "target_guid", "evidence", "message").order_by("pk")
    }
    executed = job.kind == "apply" or bool(records) or (
        job.kind == "scheduled" and job.status in {"success", "partial_failed"}
    )
    rows = []
    counts = {"success": 0, "failed": 0, "unverified": 0, "waiting": 0}
    for item in job.plan.get("departments", []):
        row = {"item": item, "guid": None, "message": "", "tone": "active", "state": "planned"}
        record = records.get("department:" + item["source_id"])
        if record:
            evidence = record.evidence if isinstance(record.evidence, dict) else {}
            path_matches = bool(item.get("dn")) and str(evidence.get("dn", "")).casefold() == item["dn"].casefold()
            identity_matches = not item.get("guid") or str(record.target_guid) == str(item["guid"])
            if not path_matches or (record.status == "success" and (not record.target_guid or not identity_matches)):
                row.update(state="unverified", label="结果待核验", tone="warning", message="执行记录缺少匹配的 OU 路径或 GUID，请核验 AD 后重新预览。")
            elif record.status == "success":
                row.update(state="success", label="已关联" if item.get("guid") else "已创建", tone="success",
                           guid=str(record.target_guid), message="本次执行已确认 OU 并保存部门映射。")
            elif record.status == "failed":
                row.update(state="failed", label="执行失败", tone="warning", message=record.message or "操作未完整完成，请核验 AD 后重新预览。")
            elif record.status == "pending" and job.status == "running":
                row.update(state="waiting", label="执行中", message="等待此 OU 的执行结果。")
            else:
                row.update(state="unverified", label="结果待核验", tone="warning", message="已有执行记录但未确认完成，请核验 AD 后重新预览。")
        elif item.get("action") == "conflict":
            row.update(label="冲突", tone="warning", message=item.get("reason", ""))
        elif executed:
            if job.status in {"queued", "running"}:
                row.update(state="waiting", label="待执行", message="此部门尚未开始执行。")
            elif job.status == "success":
                row.update(state="unverified", label="结果待核验", tone="warning", message="未找到本次任务的 OU 执行记录，不能确认完成。")
            else:
                row.update(state="waiting", label="未执行", tone="neutral", message="本次任务结束前未执行到此部门。")
        else:
            row.update(label="已存在" if item.get("guid") else "待创建", tone="success" if item.get("guid") else "active")
        if row["state"] in counts:
            counts[row["state"]] += 1
        rows.append(row)
    return rows, counts, executed


def administrator(view):
    @wraps(view)
    def wrapped(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return redirect("login")
        if not request.user.is_active or not request.user.is_superuser:
            return HttpResponseForbidden("仅管理员可操作")
        try:
            return view(request, *args, **kwargs)
        except RuleError as exc:
            messages.error(request, str(exc))
            if request.resolver_match and request.resolver_match.url_name in {"person_action", "refresh_associations"}:
                return redirect("people")
            return redirect(request.path if request.method == "GET" else "/dashboard")
    return wrapped


class AdminLogin(LoginView):
    template_name = "registration/login.html"
    redirect_authenticated_user = True

    def form_valid(self, form):
        response = super().form_valid(form)
        audit(form.get_user().username, "admin_login", result="登录成功")
        return response

    def form_invalid(self, form):
        audit("anonymous", "admin_login", result="登录失败", success=False)
        return super().form_invalid(form)

    def post(self, request, *args, **kwargs):
        try:
            rate_limit("admin-login:" + client_address(request), 10)
        except RuleError as exc:
            return render(request, self.template_name, {"error": str(exc)}, status=429)
        return super().post(request, *args, **kwargs)


def health(request):
    return JsonResponse({"status": "ok"})


def ready(request):
    checks = {}
    try:
        with connection.cursor() as cursor:
            cursor.execute("PRAGMA quick_check")
            checks["database"] = cursor.fetchone()[0] == "ok"
        checks["schema"] = Configuration.objects.filter(pk=1).exists()
        heartbeat = settings.DATA_DIR / "worker-heartbeat"
        checks["worker"] = heartbeat.exists() and time.time() - heartbeat.stat().st_mtime < 90
    except Exception:
        checks["database"] = False
        checks["schema"] = False
    ok = all(checks.values())
    return JsonResponse({"status": "ready" if ok else "not_ready", "checks": checks}, status=200 if ok else 503)


@never_cache
@administrator
@require_GET
def job_status(request):
    ids = request.GET.getlist("id")
    if not ids or len(ids) > 12:
        return JsonResponse({"error": "请选择 1–12 个任务"}, status=400)
    try:
        ids = [UUID(value) for value in ids]
    except (ValueError, TypeError):
        return JsonResponse({"error": "任务 ID 无效"}, status=400)
    jobs = Job.objects.filter(pk__in=ids).only("id", "kind", "status", "message", "created_at", "started_at", "finished_at")
    return JsonResponse({"jobs": [job_progress(job) for job in jobs]})


@administrator
def dashboard(request):
    if request.method == "POST":
        kind = "refresh" if request.POST.get("action") == "refresh" else "preview"
        synchronization.enqueue(kind=kind, scope=request.POST.get("scope", "full"), selected=request.POST.get("selected", "").split(), actor=request.user.username)
        messages.success(request, "任务已排队，执行进程将生成预览")
        return redirect("dashboard")
    config = Configuration.current()
    latest = Job.objects.filter(actor="scheduler").order_by("-created_at").first()
    next_run = max(timezone.now(), latest.created_at + timedelta(minutes=config.interval_minutes)) if latest else timezone.now()
    jobs = list(Job.objects.order_by("-created_at")[:12])
    job_rows = [
        {"job": job, "kind": JOB_KIND_LABELS.get(job.kind, job.kind), "progress": job_progress(job)}
        for job in jobs
    ]
    snapshot = Snapshot.objects.order_by("-pk").first()
    latest_full_plan = Job.objects.filter(
        scope="full", kind__in=["preview", "apply", "scheduled"]
    ).exclude(plan={}).order_by("-created_at").first()
    latest_sync_job = Job.objects.filter(kind__in=["preview", "apply", "scheduled"]).order_by("-created_at").first()
    plan_data = latest_full_plan.plan if latest_full_plan and isinstance(latest_full_plan.plan, dict) else {}
    conflict_count = sum(
        item.get("action") == "conflict"
        for item in plan_data.get("operations", []) + plan_data.get("departments", [])
    )
    return render(request, "dashboard.html", {
        "jobs": jobs, "job_rows": job_rows, "config": config, "snapshot": snapshot,
        "runtime": RuntimeState.current(), "next_run": next_run if config.schedule_enabled else None,
        "person_count": Person.objects.count(), "binding_count": Binding.objects.count(),
        "managed_binding_count": Binding.objects.filter(sync_managed=True, person__excluded=False).count(),
        "identity_binding_count": Binding.objects.filter(sync_managed=False).count(),
        "department_binding_count": DepartmentBinding.objects.count(),
        "latest_full_plan": latest_full_plan, "conflict_count": conflict_count,
        "latest_sync_job": latest_sync_job,
        "has_active_jobs": any(row["progress"]["active"] for row in job_rows),
        "task_status_script_version": settings.TASK_STATUS_SCRIPT_VERSION,
    })


@administrator
def admin_home(request):
    return redirect("dashboard")


@administrator
def departments(request):
    snapshot = Snapshot.objects.order_by("-pk").first()
    source = {str(item["id"]): item for item in snapshot.departments} if snapshot else {}
    query = request.GET.get("q", "").strip()[:100]
    status_filter = request.GET.get("status", "")
    if status_filter not in {"", "mapped", "unmapped", "stale"}:
        status_filter = ""
    bindings = {item.source_id: item for item in DepartmentBinding.objects.all()}
    rows = []
    for source_id, department in sorted(source.items(), key=lambda item: str(item[1].get("name", ""))):
        binding = bindings.get(source_id)
        name = department.get("name") or "未命名部门"
        if status_filter == "stale" or (status_filter == "mapped" and binding is None) or (status_filter == "unmapped" and binding is not None):
            continue
        if query and not any(query.casefold() in str(value).casefold() for value in (source_id, name, binding.dn if binding else "")):
            continue
        rows.append({"source_id": source_id, "binding": binding, "name": name, "target": binding.dn.split(",", 1)[0] if binding else "", "status": "mapped" if binding else "unmapped"})
    for source_id, binding in sorted(bindings.items()):
        if source_id in source or status_filter in {"mapped", "unmapped"}:
            continue
        name = "来源部门未在最近采集中"
        if query and not any(query.casefold() in str(value).casefold() for value in (source_id, name, binding.dn)):
            continue
        rows.append({"source_id": source_id, "binding": binding, "name": name, "target": binding.dn.split(",", 1)[0], "status": "stale"})
    return render(request, "departments.html", {
        "page": Paginator(rows, 25).get_page(request.GET.get("page")), "query": query,
        "status_filter": status_filter, "snapshot": snapshot,
        "total_count": len(bindings), "source_count": len(source),
        "unmapped_count": sum(source_id not in bindings for source_id in source),
    })


@administrator
def job_detail(request, job_id):
    job = get_object_or_404(Job, pk=job_id)
    if request.method == "POST":
        if job.kind == "associate":
            messages.error(request, "账号关联任务仅保存本地关联，无需执行同步；请到人员页刷新账号关联")
            return redirect("job", job_id=job.pk)
        synchronization.queue_apply(job.pk, request.user.username, request.POST.get("confirmed") == "on")
        messages.success(request, "执行任务已排队")
        return redirect("job", job_id=job.pk)
    planned_operations = job.plan.get("operations", [])
    association_counts = {
        action: sum(item.get("action") == action for item in planned_operations)
        for action in ("associate", "skip", "conflict")
    } if job.kind == "associate" else {}
    conflict_count = sum(item.get("action") == "conflict" for item in planned_operations)
    department_conflict_count = sum(item.get("action") == "conflict" for item in job.plan.get("departments", []))
    disable_count = sum(item.get("action") == "disable" for item in planned_operations)
    plan_filter = request.GET.get("only", "")
    if plan_filter not in ({"conflicts"} if job.kind == "associate" else {"conflicts", "disables"}):
        plan_filter = ""
    if plan_filter:
        action = "conflict" if plan_filter == "conflicts" else "disable"
        planned_operations = [item for item in planned_operations if item.get("action") == action]
    planned_page = Paginator(planned_operations, 50).get_page(request.GET.get("page"))
    planned_rows = [
        {"item": item, "label": ACTION_LABELS.get(item.get("action"), item.get("action", "—")),
         "tone": status_tone(item.get("action")),
         "current_ou": synchronization.parent_dn(item["target"]["dn"]) if (item.get("target") or {}).get("dn") else ""}
        for item in planned_page
    ]
    if job.kind == "associate":
        for row in planned_rows:
            item = row["item"]
            reason_code = item.get("reason_code", "")
            row["label"] = {
                "linked": "已保存关联", "retained": "保留现有关联",
                **ASSOCIATION_REASON_LABELS,
            }.get(reason_code, "关联尚未完成")
            row["tone"] = "success" if reason_code == "linked" else (
                "neutral" if reason_code in {"retained", "no_match", "excluded"} else "warning"
            )
    operation_page = Paginator(job.operation_set.order_by("pk"), 50).get_page(request.GET.get("result_page"))
    operation_rows = [
        {"operation": operation, "label": ACTION_LABELS.get(operation.action, operation.action),
         "status": OPERATION_STATUS_LABELS.get(operation.status, operation.status),
         "tone": status_tone(operation.status)}
        for operation in operation_page
    ]
    department_rows, department_result_counts, execution_view = department_results(job)
    root_ou = job.plan.get("root_ou", "").casefold()
    root_ou_completed = any(row["state"] == "success" and row["item"].get("dn", "").casefold() == root_ou for row in department_rows)
    return render(request, "job.html", {
        "job": job,
        "planned_page": planned_page,
        "planned_rows": planned_rows,
        "operation_page": operation_page,
        "operation_rows": operation_rows,
        "job_status_label": JOB_STATUS_LABELS.get(job.status, job.status),
        "job_status_tone": status_tone(job.status),
        "job_kind_label": JOB_KIND_LABELS.get(job.kind, job.kind),
        "job_scope_label": "当前钉钉通讯录" if job.kind == "associate" else {"full": "完整管理范围", "organization": "仅同步组织架构（不修改人员）", "department": "指定部门及子部门", "users": "指定人员"}.get(job.scope, job.scope),
        "conflict_count": conflict_count,
        "department_conflict_count": department_conflict_count,
        "department_rows": department_rows,
        "department_result_counts": department_result_counts,
        "execution_view": execution_view,
        "root_ou_completed": root_ou_completed,
        "disable_count": disable_count,
        "plan_has_conflicts": synchronization.has_conflicts(job.plan),
        "plan_filter": plan_filter,
        "association_job": job.kind == "associate",
        "association_counts": association_counts,
        "progress": job_progress(job),
        "task_status_script_version": settings.TASK_STATUS_SCRIPT_VERSION,
    })


@administrator
def people(request):
    query = request.GET.get("q", "").strip()[:100]
    snapshot = Snapshot.objects.order_by("-pk").first()
    source = {u["source_id"]: u for u in snapshot.users} if snapshot else {}
    items = Person.objects.order_by("name")
    if query:
        from django.db.models import Q
        employee_ids = [
            source_id for source_id, user in source.items()
            if query.casefold() in str(user.get("employee_id") or "").casefold()
        ]
        items = items.filter(Q(name__icontains=query) | Q(source_id__icontains=query) | Q(source_id__in=employee_ids))
    page = Paginator(items, 30).get_page(request.GET.get("page"))
    bindings = {b.person_id: b for b in Binding.objects.filter(person__in=page.object_list)}
    department_names = {str(d["id"]): d["name"] for d in snapshot.departments} if snapshot else {}
    config = Configuration.current()
    naming = config.naming
    directory_identity_error = ""
    try:
        synchronization.validate_directory_identity(config)
    except RuleError as exc:
        directory_identity_error = str(exc)
    source_scope_changed = bool(snapshot and str(snapshot.root_department) != str(config.root_department))
    association_job = Job.objects.filter(kind="associate").order_by("-created_at").first()
    association_plan = association_job.plan if association_job and isinstance(association_job.plan, dict) else {}
    association_results_stale = (
        "source_fingerprint" in association_plan
        and (snapshot is None or association_plan["source_fingerprint"] != snapshot.fingerprint)
    )
    association_operations = {
        str(item.get("source_id")): item
        for item in association_plan.get("operations", []) if isinstance(item, dict)
    } if not (directory_identity_error or source_scope_changed or association_results_stale) else {}
    association_active = bool(association_job and association_job.status in {"queued", "running"})
    rows = []
    for person in page:
        user = source.get(person.source_id)
        department_ids = list(dict.fromkeys(
            str(value) for value in user.get("departments", []) if str(value) in department_names
        )) if user else []
        options = [{"id": department_id, "name": department_names.get(department_id, department_id)} for department_id in department_ids]
        if person.primary_department and person.primary_department not in department_ids:
            options.insert(0, {"id": person.primary_department, "name": "已保存（当前不在来源部门）"})
        binding = bindings.get(person.pk)
        operation = association_operations.get(str(person.source_id))
        binding_result_superseded = False
        if binding and operation:
            target = operation.get("target")
            target_guid = target.get("guid") if isinstance(target, dict) else None
            if target_guid:
                try:
                    binding_result_superseded = UUID(str(target_guid)) != binding.object_guid
                except (ValueError, TypeError, AttributeError):
                    binding_result_superseded = True
            if association_job.finished_at and binding.updated_at > association_job.finished_at:
                binding_result_superseded = True
            if binding_result_superseded:
                operation = None
        reason_code = operation.get("reason_code", "") if operation else ""
        association = {
            "label": "待核验", "tone": "neutral", "reason": "尚无账号关联核验结果",
            "sync_note": operation.get("sync_note", "") if operation else "",
        }
        if binding:
            if binding.sync_managed:
                association.update(label="已关联" if binding.enabled else "关联已停用", tone="success" if binding.enabled else "warning")
            else:
                association.update(label="账号已关联", tone="success")
            association["reason"] = operation.get("reason", "") if operation else ""
            if operation and (operation.get("action") == "conflict" or reason_code == "failed"):
                association.update(label="关联需核验", tone="warning")
            if directory_identity_error:
                association.update(label="历史关联，目录需核验", tone="warning", reason="此处为历史保存的关联，不代表当前企业或 AD 目录账号。", sync_note="")
            elif source_scope_changed or association_results_stale:
                association.update(label="已保存关联", reason="来源资料或范围已变化，等待完整通讯录刷新后重新核验。", sync_note="")
            elif binding_result_superseded:
                association["reason"] = "账号关联已更新，旧核验结果不再适用。"
        elif directory_identity_error:
            association.update(label="目录需核验", tone="warning", reason="目录身份校验未通过，请先核对企业与 AD 目录配置。")
        elif source_scope_changed:
            association.update(label="来源范围待刷新", tone="warning", reason="来源根部门已改变，请先刷新完整通讯录，再核验账号关联。")
        elif association_results_stale:
            association.update(label="待重新核验", reason="来源资料已更新，旧关联结果不再适用，等待重新核验。")
        elif association_active and not operation:
            association.update(label="正在核验", tone="active", reason="账号关联任务正在排队或执行，完成后刷新页面查看结果")
        elif operation:
            association["label"] = ASSOCIATION_REASON_LABELS.get(reason_code, "关联尚未完成")
            association["tone"] = "neutral" if reason_code in {"no_match", "excluded"} else "warning"
            association["reason"] = operation.get("reason") or "请查看最近任务核验结果"
        elif association_job and association_job.status in {"failed", "blocked", "partial", "partial_failed"}:
            association.update(label="核验未完成", tone="warning", reason=association_job.message or "最近账号关联任务未完成，请查看任务详情后重试")
        elif user and not str(user.get("employee_id") or "").strip():
            association.update(label="缺少工号", tone="warning", reason="钉钉来源未提供工号，无法按工号匹配 AD 登录名")
        elif not config.auto_associate_accounts or config.match_field != "employee_username":
            association["reason"] = "当前未开启按工号自动关联，可由管理员核验并人工关联已有 AD 账号"
        if user is None and not directory_identity_error and not source_scope_changed:
            association.update(label="保留历史关联" if binding else "不在当前通讯录", tone="neutral",
                               reason="最近完整通讯录中未出现该人员；保留已有记录，不自动建立新的账号关联。", sync_note="")
        rows.append((person, binding, user, candidate(user, naming) if user else "", options, association))
    return render(request, "people.html", {
        "page": page, "rows": rows, "query": query, "snapshot": snapshot, "config": config,
        "association_job": association_job, "association_active": association_active,
        "association_status_label": JOB_STATUS_LABELS.get(association_job.status, association_job.status) if association_job else "尚未核验",
        "association_status_tone": status_tone(association_job.status) if association_job else "neutral",
        "directory_identity_error": directory_identity_error, "source_scope_changed": source_scope_changed,
        "association_results_stale": association_results_stale,
    })


@administrator
@require_POST
def refresh_associations(request):
    synchronization.enqueue(kind="associate", scope="full", actor=request.user.username)
    messages.success(request, "账号关联核验已排队；唯一匹配的 AD 账号将保存为本地关联，不修改 AD")
    return redirect("people")


@administrator
@require_POST
def person_action(request, person_id):
    get_object_or_404(Person, pk=person_id)
    action = request.POST.get("action")
    if action == "bind":
        review = synchronization.binding_review(person_id, request.POST.get("username", ""))
        review["reason"] = request.POST.get("reason", "")
        return render(request, "binding_confirm.html", review)
    elif action == "confirm_bind":
        synchronization.bind_person(person_id, request.POST.get("confirmation", ""), request.user.username, request.POST.get("reason", ""))
    elif action == "verify":
        return render(request, "binding_status.html", synchronization.verify_binding(person_id))
    elif action == "reactivate":
        synchronization.reactivate_person(person_id, request.user.username, request.POST.get("reason", ""), request.POST.get("confirmed") == "on")
    elif action == "unbind":
        synchronization.unbind_person(person_id, request.user.username)
    elif action == "policy":
        synchronization.change_person(person_id, request.user.username, request.POST.get("excluded") == "on", request.POST.get("department", "").strip())
    else:
        raise RuleError("未知操作")
    messages.success(request, "人员设置已保存")
    return redirect("people")


@administrator
def logs(request):
    items = Audit.objects.select_related("password_notification").order_by("-pk")
    search = request.GET.get("q", "").strip()[:150]
    if search:
        items = items.filter(
            Q(actor_name__icontains=search) | Q(employee_id__icontains=search)
            | Q(actor__icontains=search) | Q(target_username__icontains=search)
            | Q(target__icontains=search)
        )
    action = request.GET.get("action", "")
    action_choices = list(AUDIT_LABELS.items())
    if action in AUDIT_LABELS:
        items = items.filter(action=action)
    else:
        action = ""
    for parameter, lookup in [("start", "created_at__date__gte"), ("end", "created_at__date__lte")]:
        value = request.GET.get(parameter, "")
        try:
            date = parse_date(value) if value else None
        except ValueError:
            date = None
        if value and date is None:
            messages.error(request, "日期格式无效，请选择有效日期")
            items = items.none()
        elif date:
            items = items.filter(**{lookup: date})
    result = request.GET.get("result", "")
    if result == "success":
        items = items.filter(Q(state="success") | Q(state="", success=True))
    elif result == "failed":
        items = items.filter(Q(state="failed") | Q(state="", success=False))
    elif result == "partial":
        items = items.filter(state="partial")
    elif result == "attention":
        items = items.filter(state__in=["pending", "unknown"])
    elif result in {"pending", "unknown"}:
        items = items.filter(state=result)
    else:
        result = ""
    query = request.GET.copy()
    query.pop("page", None)
    page = Paginator(items, 50).get_page(request.GET.get("page"))
    states = {
        "success": ("成功", "success"), "failed": ("失败", "warning"),
        "partial": ("部分完成", "warning"), "pending": ("处理中 / 未完成", "active"),
        "unknown": ("结果不明", "warning"),
    }
    audit_rows = []
    for item in page:
        state = item.state or ("success" if item.success else "failed")
        label, tone = states.get(state, ("未知状态", "warning"))
        employee_action = item.action.startswith("sspr_")
        actor_label = item.actor_name or ("—" if employee_action else item.actor)
        unverified_actor = employee_action and item.actor in {"anonymous", "employee", "未验证访客"} and not item.actor_name and not item.target
        if unverified_actor:
            actor_label = "未验证访客"
        state_note = ""
        if item.action == "sspr_reset":
            state_note = {
                "partial": "密码已修改，解锁未完成。",
                "unknown": "无法确认密码是否已写入，请核查 AD 或由本人确认登录结果。",
                "pending": "尚无完成结果，请先核查后再重试。",
            }.get(state, "")
        notification = getattr(item, "password_notification", None) if item.action == "sspr_reset" else None
        audit_rows.append({
            "item": item, "label": AUDIT_LABELS.get(item.action, item.action),
            "status": label, "tone": tone, "state_note": state_note,
            "employee_action": employee_action, "actor_label": actor_label,
            "actor_identifier": "" if unverified_actor else item.actor,
            "password_notification": notification,
        })
    filters = {"q": search, "action": action, "result": result, "start": request.GET.get("start", ""), "end": request.GET.get("end", "")}
    return render(request, "logs.html", {
        "page": page, "audit_rows": audit_rows, "filters": filters,
        "filter_query": query.urlencode(), "action_choices": action_choices,
    })


@administrator
@require_POST
def test_connections(request):
    synchronization.enqueue(kind="connections", actor=request.user.username)
    messages.success(request, "连接测试已排队，结果将显示在同步页面")
    return redirect("dashboard")


def employee_page_context(**context):
    try:
        context["page_settings"] = EmployeePageSettings.current()
        # Evaluate here so a presentation-store failure cannot hide a reset result.
        context["auth_platforms"] = (
            list(AuthPlatform.objects.filter(page_settings_id=1, enabled=True))
            if context.get("account") else []
        )
    except DatabaseError:
        context["page_settings"] = EmployeePageSettings()
        context["auth_platforms"] = []
    return context


@never_cache
@ensure_csrf_cookie
def employee(request):
    config = Configuration.current()
    account, error = None, ""
    token = request.COOKIES.get("employee_verification", "")
    if token:
        try:
            account = sspr.current_account(token)
        except RuleError as exc:
            error = str(exc)
        except Exception:
            error = "当前账号暂时无法核验，请稍后重新通过钉钉验证"
    return render(request, "sspr.html", employee_page_context(enabled=config.sspr_enabled, account=account, error=error, corp_id=settings.DINGTALK_CORP_ID, app_key=settings.DINGTALK_APP_KEY, minimum=config.minimum_password_length, script_version=settings.SSPR_SCRIPT_VERSION))


@never_cache
@require_POST
@sensitive_post_parameters()
def employee_auth(request):
    try:
        code = request.POST.get("code", "")
        token, _ = sspr.verify(code, client_address(request))
        response = JsonResponse({"next": "/sspr"})
        response.set_cookie("employee_verification", token, max_age=300, secure=True, httponly=True, samesite="Strict", path="/sspr")
        return response
    except RuleError as exc:
        return JsonResponse({"error": str(exc)}, status=400)


def csrf_failure(request, reason=""):
    match = getattr(request, "resolver_match", None)
    if request.method == "POST" and match and match.func is employee_reset:
        return employee_reset_csrf_rejected(request)
    return default_csrf_failure(request, reason=reason)


@never_cache
@sensitive_post_parameters()
def employee_reset_csrf_rejected(request):
    # CSRF rejection does not authenticate the cookie owner or any posted identity.
    message = "安全校验未通过，密码未提交；请重新打开密码服务并通过钉钉验证"
    try:
        audit(
            "未验证访客", "sspr_reset", result=message, success=False,
            client_ip=client_address(request),
        )
    except Exception:
        message = "安全校验未通过，密码未提交；审计暂时不可用，请稍后重新打开密码服务并通过钉钉验证"
    return render(request, "sspr.html", employee_page_context(
        error=message, security_rejected=True, enabled=False,
    ), status=403)


@never_cache
@require_POST
@sensitive_post_parameters()
def employee_reset(request):
    try:
        result = sspr.reset(request.COOKIES.get("employee_verification", ""), request.POST.get("password", ""), request.POST.get("confirmation", ""), client_address(request))
        response = render(request, "sspr.html", employee_page_context(result=result, enabled=True))
        response.delete_cookie("employee_verification", path="/sspr", samesite="Strict")
        return response
    except ResetOutcomeUnknown as exc:
        response = render(request, "sspr.html", employee_page_context(uncertain=str(exc), enabled=True))
        response.delete_cookie("employee_verification", path="/sspr", samesite="Strict")
        return response
    except RuleError as exc:
        config = Configuration.current()
        account = None
        token = request.COOKIES.get("employee_verification", "")
        if token:
            try:
                account = sspr.current_account(token)
            except Exception:
                # Only a still-valid, freshly matched session may reveal the account.
                pass
        return render(request, "sspr.html", employee_page_context(**{
            "error": str(exc), "enabled": config.sspr_enabled, "account": account,
            "minimum": config.minimum_password_length, "corp_id": settings.DINGTALK_CORP_ID,
            "app_key": settings.DINGTALK_APP_KEY, "script_version": settings.SSPR_SCRIPT_VERSION,
        }), status=400)
