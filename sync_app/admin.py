from django.contrib import admin
from django import forms
from django.shortcuts import redirect
from contextlib import closing
from .directory import ActiveDirectory, under
from .domain import RuleError
from .models import Snapshot
from .models import Configuration, Audit, DepartmentBinding, Job, Operation
from .locking import lock
from .security import audit


ATTRIBUTE_CHOICES = [
    ("displayName", "姓名"), ("mail", "邮箱"), ("title", "职位"),
    ("department", "部门"), ("telephoneNumber", "电话"),
]


class ConfigurationForm(forms.ModelForm):
    attributes = forms.MultipleChoiceField(label="同步到 AD 的属性", choices=ATTRIBUTE_CHOICES, widget=forms.CheckboxSelectMultiple, required=False, help_text="仅更新勾选的属性；来源空值默认不覆盖 AD。")
    clear_attributes = forms.MultipleChoiceField(label="允许清空的属性", choices=ATTRIBUTE_CHOICES, widget=forms.CheckboxSelectMultiple, required=False, help_text="仅对已启用同步的属性生效。")
    protected_usernames = forms.CharField(label="额外保护账号", required=False, widget=forms.Textarea(attrs={"rows": 3, "placeholder": "每行一个 AD 账号名"}), help_text="每行一个 sAMAccountName；同步与密码重置都禁止操作。")

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
    fieldsets = (
        ("01 · 同步边界", {"fields": ("root_department", "root_ou"), "description": "只处理指定钉钉部门和 AD 根 OU 范围内的对象。"}),
        ("02 · 匹配与新建账号", {"fields": ("match_field", "naming", "enable_new_accounts", "require_password_change")}),
        ("03 · 属性与保护", {"fields": ("attributes", "clear_attributes", "protected_usernames")}),
        ("04 · 离职安全阈值", {"fields": ("disable_missing", "disable_limit", "disable_percent"), "description": "超过人数或比例阈值时，预览需人工确认。"}),
        ("05 · 员工自助重置", {"fields": ("sspr_enabled", "sspr_match", "unlock_after_reset", "minimum_password_length"), "description": "密码重置独立使用实时 LDAPS 唯一匹配，不依赖同步任务或本地绑定。"}),
        ("06 · 定时任务", {"fields": ("schedule_enabled", "interval_minutes")}),
    )

    def has_add_permission(self, request):
        return not Configuration.objects.exists()

    def has_delete_permission(self, request, obj=None):
        return False

    def save_model(self, request, obj, form, change):
        with lock("sync"):
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
    list_display = ("created_at", "actor", "action", "target", "result")
    list_filter = ("action",)
    search_fields = ("actor", "target")


class DepartmentForm(forms.ModelForm):
    class Meta:
        model = DepartmentBinding
        fields = ["source_id", "dn"]
        labels = {"source_id": "钉钉部门 ID", "dn": "目标 AD 组织单位 DN"}
        help_texts = {"source_id": "必须是最近一次采集到的部门。", "dn": "必须位于配置的 AD 根 OU 内，保存前将实时核验目标 OU。"}

    def clean(self):
        data = super().clean()
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
        with lock("sync"), closing(ActiveDirectory()) as ad:
            obj.object_guid = ad.verify_ou(obj.dn, str(obj.object_guid))
            obj.manual = True
            obj.save()
            audit(request.user.username, "department_binding", obj.source_id)


admin.site.register([Job, Operation], ReadOnlyAdmin)
admin.site.site_header = "组织同步 · 系统管理"
admin.site.site_title = "组织同步控制台"
admin.site.index_title = "系统配置与维护"
admin.site.site_url = "/dashboard"
