from functools import wraps
from datetime import timedelta
import time
from contextlib import closing

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.views import LoginView
from django.core.paginator import Paginator
from django.db import connection
from django.http import JsonResponse, HttpResponseForbidden
from django.shortcuts import redirect, render, get_object_or_404
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.debug import sensitive_post_parameters
from django.views.decorators.http import require_POST
from django.utils.dateparse import parse_date
from django.utils import timezone

from . import sspr, synchronization
from .directory import ActiveDirectory, DingTalk
from .domain import RuleError, candidate
from .models import Configuration, Person, Binding, Job, Audit, Snapshot, RuntimeState
from .security import rate_limit, audit, client_address


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
            if request.resolver_match and request.resolver_match.url_name == "person_action":
                return redirect("people")
            return redirect(request.path if request.method == "GET" else "/dashboard")
    return wrapped


class AdminLogin(LoginView):
    template_name = "registration/login.html"

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
    return render(request, "dashboard.html", {"jobs": Job.objects.order_by("-created_at")[:30], "config": config, "snapshot": Snapshot.objects.order_by("-pk").first(), "runtime": RuntimeState.current(), "next_run": next_run if config.schedule_enabled else None})


@administrator
def job_detail(request, job_id):
    job = get_object_or_404(Job, pk=job_id)
    if request.method == "POST":
        synchronization.queue_apply(job.pk, request.user.username, request.POST.get("confirmed") == "on")
        messages.success(request, "执行任务已排队")
        return redirect("job", job_id=job.pk)
    planned_operations = job.plan.get("operations", [])
    conflict_count = sum(item.get("action") == "conflict" for item in planned_operations)
    show_conflicts_only = request.GET.get("only") == "conflicts"
    if show_conflicts_only:
        planned_operations = [item for item in planned_operations if item.get("action") == "conflict"]
    return render(request, "job.html", {
        "job": job,
        "operations": job.operation_set.all(),
        "planned_operations": planned_operations,
        "conflict_count": conflict_count,
        "show_conflicts_only": show_conflicts_only,
    })


@administrator
def people(request):
    query = request.GET.get("q", "")
    items = Person.objects.order_by("name")
    if query:
        from django.db.models import Q
        items = items.filter(Q(name__icontains=query) | Q(source_id__icontains=query))
    page = Paginator(items, 30).get_page(request.GET.get("page"))
    bindings = {b.person_id: b for b in Binding.objects.filter(person__in=page.object_list)}
    snapshot = Snapshot.objects.order_by("-pk").first()
    source = {u["source_id"]: u for u in snapshot.users} if snapshot else {}
    department_names = {str(d["id"]): d["name"] for d in snapshot.departments} if snapshot else {}
    naming = Configuration.current().naming
    rows = []
    for person in page:
        user = source.get(person.source_id)
        department_ids = list(dict.fromkeys(
            str(value) for value in user.get("departments", []) if str(value) in department_names
        )) if user else []
        options = [{"id": department_id, "name": department_names.get(department_id, department_id)} for department_id in department_ids]
        if person.primary_department and person.primary_department not in department_ids:
            options.insert(0, {"id": person.primary_department, "name": "已保存（当前不在来源部门）"})
        rows.append((person, bindings.get(person.pk), user, candidate(user, naming) if user else "", options))
    return render(request, "people.html", {"page": page, "rows": rows, "query": query, "snapshot": snapshot})


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
    items = Audit.objects.order_by("-pk")
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
    if result in {"success", "failed"}:
        items = items.filter(success=result == "success")
    query = request.GET.copy()
    query.pop("page", None)
    return render(request, "logs.html", {"page": Paginator(items, 50).get_page(request.GET.get("page")), "filters": request.GET, "filter_query": query.urlencode()})


@administrator
@require_POST
def test_connections(request):
    synchronization.enqueue(kind="connections", actor=request.user.username)
    messages.success(request, "连接测试已排队，结果将显示在同步页面")
    return redirect("dashboard")


@never_cache
@ensure_csrf_cookie
def employee(request):
    config = Configuration.current()
    account, error = None, ""
    token = request.COOKIES.get("employee_verification", "")
    if token:
        try:
            item, _ = sspr.session_for(token)
            with closing(ActiveDirectory()) as ad:
                target = ad.check_account(item.object_guid)
                account = {"username": target["username"][:2] + "***", "guid": str(item.object_guid), "name": item.display_name}
        except RuleError as exc:
            error = str(exc)
    return render(request, "sspr.html", {"enabled": config.sspr_enabled, "account": account, "error": error, "corp_id": settings.DINGTALK_CORP_ID, "app_key": settings.DINGTALK_APP_KEY, "minimum": config.minimum_password_length})


@never_cache
@require_POST
@sensitive_post_parameters()
def employee_auth(request):
    try:
        code = request.POST.get("code", "")
        if not code or len(code) > 4096:
            raise RuleError("缺少有效钉钉授权码")
        token, _ = sspr.verify(code, client_address(request))
        response = JsonResponse({"next": "/sspr"})
        response.set_cookie("employee_verification", token, max_age=300, secure=True, httponly=True, samesite="Strict", path="/sspr")
        return response
    except RuleError as exc:
        from .security import audit
        audit("employee", "sspr_auth_failed", result=str(exc), success=False)
        return JsonResponse({"error": str(exc)}, status=400)


@never_cache
@require_POST
@sensitive_post_parameters()
def employee_reset(request):
    try:
        result = sspr.reset(request.COOKIES.get("employee_verification", ""), request.POST.get("password", ""), request.POST.get("confirmation", ""), client_address(request))
        response = render(request, "sspr.html", {"result": result, "enabled": True})
        response.delete_cookie("employee_verification", path="/sspr", samesite="Strict")
        return response
    except RuleError as exc:
        return render(request, "sspr.html", {"error": str(exc), "enabled": True, "corp_id": settings.DINGTALK_CORP_ID, "app_key": settings.DINGTALK_APP_KEY}, status=400)
