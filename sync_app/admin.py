from django.contrib import admin
from django import forms
from contextlib import closing
from .directory import ActiveDirectory, under
from .domain import RuleError
from .models import Snapshot
from .models import Configuration, Audit, DepartmentBinding, Job, Operation
from .locking import lock
from .security import audit


@admin.register(Configuration)
class ConfigurationAdmin(admin.ModelAdmin):
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

    def has_delete_permission(self, request, obj=None):
        return False

    def save_model(self, request, obj, form, change):
        with lock("sync"), closing(ActiveDirectory()) as ad:
            obj.object_guid = ad.verify_ou(obj.dn, str(obj.object_guid))
            obj.manual = True
            obj.save()
            audit(request.user.username, "department_binding", obj.source_id)


admin.site.register([Job, Operation], ReadOnlyAdmin)
admin.site.site_header = "组织同步 · 设置"
admin.site.site_title = "组织同步"
