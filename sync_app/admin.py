from django.contrib import admin
from django.conf import settings
from django import forms
from django.shortcuts import redirect
from contextlib import closing
from zoneinfo import ZoneInfo
from django.db import transaction
from django.utils import timezone
from .directory import ActiveDirectory, under
from .domain import RuleError
from .models import Snapshot
from .models import Configuration, Audit, DepartmentBinding, Job, Operation, EmployeePageSettings, AuthPlatform
from .locking import lock
from .security import audit, client_address
from .synchronization import establish_directory_identity, validate_directory_identity


ATTRIBUTE_CHOICES = [
    ("displayName", "姓名"), ("mail", "邮箱"), ("title", "职位"),
    ("department", "部门"), ("telephoneNumber", "电话"),
]


class AuthPlatformInline(admin.StackedInline):
    model = AuthPlatform
    extra = 0
    fields = ("name", "authentication_note", "login_url", "password_note", "enabled", "position")


@admin.register(EmployeePageSettings)
class EmployeePageSettingsAdmin(admin.ModelAdmin):
    inlines = [AuthPlatformInline]
    fieldsets = (
        ("员工页面文案", {"fields": ("title", "description", "announcement", "help_text", "support_text"), "description": "以下内容以纯文本展示；请勿填写密码、密钥等敏感信息。"}),
    )

    def has_add_permission(self, request):
        return super().has_add_permission(request) and not EmployeePageSettings.objects.exists()

    def has_delete_permission(self, request, obj=None):
        return False

    def save_model(self, request, obj, form, change):
        obj.full_clean()
        super().save_model(request, obj, form, change)

    def save_related(self, request, form, formsets, change):
        # Django's changeform_view wraps the model, inlines, and audit in one
        # transaction. An audit failure must roll back every presentation edit.
        super().save_related(request, form, formsets, change)
        changed_fields = ", ".join(form.changed_data) or "无"
        added = sum(len(formset.new_objects) for formset in formsets)
        changed = sum(len(formset.changed_objects) for formset in formsets)
        deleted = sum(len(formset.deleted_objects) for formset in formsets)
        audit(
            request.user.get_username(), "employee_page_settings", str(form.instance.pk),
            f"页面字段：{changed_fields}；平台共 {form.instance.platforms.count()} 项（新增 {added}、修改 {changed}、删除 {deleted}）",
            client_ip=client_address(request),
        )


class ConfigurationForm(forms.ModelForm):
    attributes = forms.MultipleChoiceField(label="同步到 AD 的属性", choices=ATTRIBUTE_CHOICES, widget=forms.CheckboxSelectMultiple, required=False, help_text="仅更新勾选的属性；来源空值默认不覆盖 AD。")
    clear_attributes = forms.MultipleChoiceField(label="允许清空的属性", choices=ATTRIBUTE_CHOICES, widget=forms.CheckboxSelectMultiple, required=False, help_text="仅对已启用同步的属性生效。")
    protected_usernames = forms.CharField(label="额外保护账号", required=False, widget=forms.Textarea(attrs={"rows": 3, "placeholder": "每行一个 AD 账号名"}), help_text="每行一个 sAMAccountName；同步禁止操作。自助重置保护对实时核验为域管理员的本人账号不生效，其他受保护账号仍拒绝。")

    class Meta:
        model = Configuration
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance.pk:
            self.initial["protected_usernames"] = "\n".join(self.instance.protected_usernames or [])

    def clean_protected_usernames(self):
        values = [item.strip() for item in self.cleaned_data["protected_usernames"].splitlines()]
        return list(dict.fromkeys(item for item in values if item))


@admin.register(Configuration)
class ConfigurationAdmin(admin.ModelAdmin):
    form = ConfigurationForm
    readonly_fields = ("current_ldap_directory", "sspr_open_scope")
    fieldsets = (
        ("01 · 同步边界", {"fields": ("root_department", "root_ou"), "description": "只处理指定钉钉部门和 AD 根 OU 范围内的对象。"}),
        ("02 · 匹配与新建账号", {"fields": ("match_field", "naming", "enable_new_accounts", "require_password_change")}),
        ("03 · 属性与保护", {"fields": ("attributes", "clear_attributes", "protected_usernames")}),
        ("04 · 离职安全阈值", {"fields": ("disable_missing", "disable_limit", "disable_percent"), "description": "超过人数或比例阈值时，预览需人工确认。"}),
        ("05 · 员工自助重置", {"fields": ("current_ldap_directory", "sspr_open_scope", "sspr_enabled", "sspr_match", "unlock_after_reset", "minimum_password_length"), "description": "密码重置独立使用实时 LDAPS 唯一匹配，不依赖同步任务或本地绑定。已启用的域管理员也可重置本人密码；其他受保护账号仍拒绝。"}),
        ("06 · 定时任务", {"fields": ("schedule_enabled", "interval_minutes")}),
    )

    @admin.display(description="当前 LDAPS 目录")
    def current_ldap_directory(self, obj):
        return f"域控：{settings.LDAP_HOST or '未配置'}；目录：{settings.LDAP_BASE_DN or '未配置'}"

    @admin.display(description="当前开放范围")
    def sspr_open_scope(self, obj):
        count = len(settings.SSPR_ALLOWED_DINGTALK_USER_IDS)
        if count:
            return f"仅向 {count} 个已配置的钉钉账号开放；仍须通过 AD 唯一匹配与账号保护检查；已启用的域管理员同样可重置本人密码。"
        return "未限制钉钉 userId；仍须通过 AD 唯一匹配与账号保护检查；已启用的域管理员同样可重置本人密码。"

    def has_add_permission(self, request):
        return not Configuration.objects.exists()

    def has_delete_permission(self, request, obj=None):
        return False

    def save_model(self, request, obj, form, change):
        with lock("sync"):
            # A settings form may have been loaded before the first collection.
            # Preserve the independently established, non-editable directory identity.
            current = Configuration.objects.filter(pk=obj.pk).only("identity_anchor").first()
            if current:
                obj.identity_anchor = current.identity_anchor
            obj.full_clean()
            obj.save()
            audit(request.user.username, "settings")


class ReadOnlyAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(Audit)
class AuditAdmin(ReadOnlyAdmin):
    list_display = ("requested_time", "completed_time", "actor_name", "employee_id", "actor", "action", "target_username", "status_label", "result", "client_ip")
    list_filter = ("action", "state", "created_at")
    search_fields = ("actor_name", "employee_id", "actor", "target_username", "target")
    date_hierarchy = "created_at"
    fields = ("requested_time", "completed_time", "actor_name", "employee_id", "actor", "action", "target_username", "target", "status_label", "result", "client_ip")
    readonly_fields = fields

    @admin.display(description="请求 / 记录时间（北京时间）", ordering="created_at")
    def requested_time(self, obj):
        return timezone.localtime(obj.created_at, ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")

    @admin.display(description="完成时间（北京时间）", ordering="completed_at")
    def completed_time(self, obj):
        if not obj.completed_at:
            return "—"
        return timezone.localtime(obj.completed_at, ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")

    @admin.display(description="结果状态", ordering="state")
    def status_label(self, obj):
        state = obj.state or ("success" if obj.success else "failed")
        return {
            "success": "成功", "failed": "失败", "partial": "部分完成",
            "pending": "处理中 / 未完成", "unknown": "结果不明",
        }.get(state, "未知状态")


class DepartmentForm(forms.ModelForm):
    class Meta:
        model = DepartmentBinding
        fields = ["source_id", "dn"]
        labels = {"source_id": "钉钉部门 ID", "dn": "目标 AD 组织单位 DN"}
        help_texts = {"source_id": "必须是最近一次采集到的部门。", "dn": "必须位于配置的 AD 根 OU 内，保存前将实时核验目标 OU。"}

    def clean(self):
        data = super().clean()
        try:
            self.directory_identity_anchor = validate_directory_identity(Configuration.current())
        except RuleError as exc:
            raise forms.ValidationError(str(exc)) from None
        snap = Snapshot.objects.order_by("-pk").first()
        if not snap or data.get("source_id") not in {d["id"] for d in snap.departments}:
            raise forms.ValidationError("部门不在当前通讯录，请先采集")
        if not under(data.get("dn", ""), Configuration.current().root_ou):
            raise forms.ValidationError("OU 超出管理范围")
        try:
            with closing(ActiveDirectory()) as ad:
                self.instance.object_guid = ad.verify_ou(data["dn"])
        except RuleError as exc:
            raise forms.ValidationError(str(exc)) from None
        return data


@admin.register(DepartmentBinding)
class DepartmentAdmin(admin.ModelAdmin):
    form = DepartmentForm
    list_display = ("source_id", "dn", "manual")
    fieldsets = (("部门与目标 OU", {"fields": ("source_id", "dn"), "description": "修改映射前，请核对钉钉部门与 AD 组织单位的对应关系。"}),)

    def changelist_view(self, request, extra_context=None):
        return redirect("departments")

    def has_delete_permission(self, request, obj=None):
        return False

    def save_model(self, request, obj, form, change):
        with lock("sync"), transaction.atomic():
            config = Configuration.current()
            if validate_directory_identity(config) != getattr(form, "directory_identity_anchor", None):
                raise RuleError("企业或 AD 目录身份确认已失效，请重新审核部门映射")
            with closing(ActiveDirectory()) as ad:
                obj.object_guid = ad.verify_ou(obj.dn, str(obj.object_guid))
            establish_directory_identity(config)
            obj.manual = True
            obj.save()
            audit(request.user.username, "department_binding", obj.source_id)


admin.site.register([Job, Operation], ReadOnlyAdmin)
admin.site.site_header = "组织同步 · 系统管理"
admin.site.site_title = "组织同步控制台"
admin.site.index_title = "系统配置与维护"
admin.site.site_url = "/dashboard"
