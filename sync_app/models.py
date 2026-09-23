import uuid
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models


class Configuration(models.Model):
    id = models.PositiveSmallIntegerField(primary_key=True, default=1, editable=False)
    root_department = models.CharField("钉钉根部门 ID", max_length=100, default="1")
    identity_anchor = models.CharField(max_length=64, blank=True, editable=False)
    root_ou = models.CharField("同步 AD 根 OU DN", max_length=500, blank=True)
    naming = models.CharField("新账号命名", max_length=30, choices=[("employee_id", "工号"), ("source_id", "钉钉 userId"), ("email", "邮箱前缀")], default="employee_id")
    attributes = models.JSONField("同步属性", default=list, blank=True, help_text="displayName、mail、title、department、telephoneNumber")
    disable_missing = models.BooleanField("全量同步禁用离职人员", default=False)
    disable_limit = models.PositiveIntegerField("禁用人数阈值", default=5, validators=[MinValueValidator(1)])
    disable_percent = models.PositiveIntegerField("禁用比例阈值 %", default=10, validators=[MinValueValidator(1), MaxValueValidator(100)])
    sspr_enabled = models.BooleanField("开启员工密码重置", default=False)
    sspr_match = models.CharField("密码重置匹配方式", max_length=30, choices=[("employee_id", "钉钉工号 → AD employeeID"), ("email", "钉钉邮箱 → AD mail"), ("source_id", "钉钉 userId → AD sAMAccountName")], default="employee_id")
    unlock_after_reset = models.BooleanField("重置后解锁", default=False)
    minimum_password_length = models.PositiveIntegerField("最短密码长度", default=12, validators=[MinValueValidator(12), MaxValueValidator(128)])
    schedule_enabled = models.BooleanField("开启定时同步", default=False)
    interval_minutes = models.PositiveIntegerField("同步间隔（分钟）", default=60, validators=[MinValueValidator(5)])
    updated_at = models.DateTimeField(auto_now=True)

    def clean(self):
        if self.pk != 1:
            raise ValidationError("只允许一份组织配置")
        allowed = {"displayName", "mail", "title", "department", "telephoneNumber"}
        if not isinstance(self.attributes, list) or any(x not in allowed for x in self.attributes):
            raise ValidationError("同步属性不合法")
        previous = Configuration.objects.filter(pk=self.pk).first()
        if previous and (Binding.objects.exists() or DepartmentBinding.objects.exists()) and (previous.root_department != self.root_department or previous.root_ou != self.root_ou):
            raise ValidationError("已有同步绑定时不能直接更换管理范围，请先审查并解除原绑定")

    @classmethod
    def current(cls):
        return cls.objects.get_or_create(pk=1)[0]

    def __str__(self):
        return "单组织设置"


class Snapshot(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    fingerprint = models.CharField(max_length=64)
    root_department = models.CharField(max_length=100)
    users = models.JSONField()
    departments = models.JSONField()


class Person(models.Model):
    source_id = models.CharField(max_length=100, unique=True)
    name = models.CharField(max_length=200)
    excluded = models.BooleanField(default=False)
    primary_department = models.CharField(max_length=100, blank=True)

    def __str__(self):
        return f"{self.name} ({self.source_id})"


class Binding(models.Model):
    person = models.OneToOneField(Person, on_delete=models.PROTECT)
    object_guid = models.UUIDField(unique=True)
    username = models.CharField(max_length=100)
    manual = models.BooleanField(default=False)
    enabled = models.BooleanField(default=True)
    revision = models.UUIDField(default=uuid.uuid4)
    updated_at = models.DateTimeField(auto_now=True)


class DepartmentBinding(models.Model):
    source_id = models.CharField(max_length=100, unique=True)
    dn = models.CharField(max_length=500)
    object_guid = models.UUIDField()
    manual = models.BooleanField(default=False)


class Job(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    kind = models.CharField(max_length=20, default="preview")
    status = models.CharField(max_length=30, default="queued")
    scope = models.CharField(max_length=20, default="full")
    selected = models.JSONField(default=list)
    actor = models.CharField(max_length=150, default="scheduler")
    plan = models.JSONField(default=dict)
    message = models.CharField(max_length=500, blank=True)
    confirmed = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True)


class Operation(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    source_id = models.CharField(max_length=100)
    action = models.CharField(max_length=30)
    status = models.CharField(max_length=30, default="pending")
    target_guid = models.UUIDField(null=True)
    evidence = models.JSONField(default=dict)
    message = models.CharField(max_length=300, blank=True)


class Audit(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    actor = models.CharField(max_length=150)
    action = models.CharField(max_length=40)
    target = models.CharField(max_length=150, blank=True)
    result = models.CharField(max_length=300)


class EmployeeSession(models.Model):
    digest = models.CharField(max_length=64, primary_key=True)
    source_id = models.CharField(max_length=100)
    object_guid = models.UUIDField()
    config_fingerprint = models.CharField(max_length=64)
    expires_at = models.DateTimeField()
    used = models.BooleanField(default=False)


class RateWindow(models.Model):
    key = models.CharField(max_length=64, primary_key=True)
    starts_at = models.DateTimeField()
    count = models.PositiveIntegerField(default=0)
