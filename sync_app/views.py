from functools import wraps
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

from . import sspr, synchronization
from .directory import ActiveDirectory, DingTalk
from .domain import RuleError
from .models import Configuration, Person, Binding, Job, Audit, Snapshot
from .security import rate_limit


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
            return redirect(request.path if request.method == "GET" else "/dashboard")
    return wrapped


class AdminLogin(LoginView):
    template_name = "registration/login.html"

    def post(self, request, *args, **kwargs):
        try:
            rate_limit("admin-login:" + request.META.get("REMOTE_ADDR", ""), 10)
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
        synchronization.enqueue(scope=request.POST.get("scope", "full"), selected=request.POST.get("selected", "").split(), actor=request.user.username)
        messages.success(request, "任务已排队，执行进程将生成预览")
        return redirect("dashboard")
    return render(request, "dashboard.html", {"jobs": Job.objects.order_by("-created_at")[:30], "config": Configuration.current(), "snapshot": Snapshot.objects.order_by("-pk").first()})


@administrator
def job_detail(request, job_id):
    job = get_object_or_404(Job, pk=job_id)
    if request.method == "POST":
        synchronization.queue_apply(job.pk, request.user.username, request.POST.get("confirmed") == "on")
        messages.success(request, "执行任务已排队")
        return redirect("job", job_id=job.pk)
    return render(request, "job.html", {"job": job, "operations": job.operation_set.all()})


@administrator
def people(request):
    query = request.GET.get("q", "")
    items = Person.objects.order_by("name")
    if query:
        from django.db.models import Q
        items = items.filter(Q(name__icontains=query) | Q(source_id__icontains=query))
    page = Paginator(items, 30).get_page(request.GET.get("page"))
    bindings = {b.person_id: b for b in Binding.objects.filter(person__in=page.object_list)}
    return render(request, "people.html", {"page": page, "rows": [(p, bindings.get(p.pk)) for p in page], "query": query})


@administrator
@require_POST
def person_action(request, person_id):
    get_object_or_404(Person, pk=person_id)
    action = request.POST.get("action")
    if action == "bind":
        synchronization.bind_person(person_id, request.POST.get("username", ""), request.user.username, request.POST.get("reason", ""))
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
    return render(request, "logs.html", {"page": Paginator(Audit.objects.order_by("-pk"), 50).get_page(request.GET.get("page"))})


@administrator
@require_POST
def test_connections(request):
    with closing(DingTalk()) as source:
        source.collect(Configuration.current().root_department)
    with closing(ActiveDirectory()) as ad:
        ad.accounts()
    messages.success(request, "钉钉通讯录与 LDAPS 读取成功")
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
                account = {"username": target["username"][:2] + "***", "guid": str(item.object_guid)}
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
        token, _ = sspr.verify(code, request.META.get("REMOTE_ADDR", ""))
        response = JsonResponse({"next": "/sspr"})
        response.set_cookie("employee_verification", token, max_age=300, secure=True, httponly=True, samesite="Strict", path="/sspr")
        return response
    except RuleError as exc:
        from .security import audit
        audit("employee", "sspr_auth_failed", result=str(exc))
        return JsonResponse({"error": str(exc)}, status=400)


@never_cache
@require_POST
@sensitive_post_parameters()
def employee_reset(request):
    try:
        result = sspr.reset(request.COOKIES.get("employee_verification", ""), request.POST.get("password", ""), request.POST.get("confirmation", ""), request.META.get("REMOTE_ADDR", ""))
        response = render(request, "sspr.html", {"result": result, "enabled": True})
        response.delete_cookie("employee_verification", path="/sspr", samesite="Strict")
        return response
    except RuleError as exc:
        return render(request, "sspr.html", {"error": str(exc), "enabled": True, "corp_id": settings.DINGTALK_CORP_ID, "app_key": settings.DINGTALK_APP_KEY}, status=400)
