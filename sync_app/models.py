import uuid
from urllib.parse import urlsplit
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator, MaxValueValidator, MaxLengthValidator, URLValidator
from django.db import models
from django.utils import timezone


def validate_platform_login_url(value):
    if not value:
        return
    URLValidator(schemes=["http", "https"])(value)
    parsed = urlsplit(value)
    if parsed.username is not None or parsed.password is not None:
        raise ValidationError("登录地址不能包含用户名或密码。")


class EmployeePageSettings(models.Model):
    """Presentation settings are independent of AD identity and reset policy."""

    id = models.PositiveSmallIntegerField(primary_key=True, default=1, editable=False)
    title = models.CharField("页面标题", max_length=100, default="重置我的 AD 密码")
    description = models.TextField("页面说明", max_length=1000, validators=[MaxLengthValidator(1000)], default="通过钉钉验证身份，查询并重置本人 AD 账号密码。")
    announcement = models.TextField("公告", max_length=2000, validators=[MaxLengthValidator(2000)], blank=True)
    help_text = models.TextField("操作帮助", max_length=1000, validators=[MaxLengthValidator(1000)], blank=True)
    support_text = models.TextField("联系支持", max_length=500, validators=[MaxLengthValidator(500)], blank=True)
    updated_at = models.DateTimeField("更新时间", auto_now=True)

    class Meta:
        verbose_name = "员工页面设置"
        verbose_name_plural = "员工页面设置"
        constraints = [models.CheckConstraint(condition=models.Q(pk=1), name="employee_page_settings_singleton")]

    def clean(self):
        if self.pk != 1:
            raise ValidationError("只允许一份员工页面设置。")

    @classmethod
    def current(cls):
        return cls.objects.filter(pk=1).first() or cls()

    def __str__(self):
        return "员工密码服务页面"


class AuthPlatform(models.Model):
    page_settings = models.ForeignKey(EmployeePageSettings, on_delete=models.CASCADE, related_name="platforms")
    name = models.CharField("平台名称", max_length=100)
    authentication_note = models.CharField("认证说明", max_length=500, blank=True)
    login_url = models.URLField("登录地址", max_length=500, blank=True, validators=[validate_platform_login_url], help_text="仅填写 HTTP 或 HTTPS 地址，不得包含用户名或密码。")
    password_note = models.CharField("改密生效说明", max_length=500, blank=True)
    enabled = models.BooleanField("在员工页面显示", default=True)
    position = models.PositiveIntegerField("显示顺序", default=0, validators=[MaxValueValidator(9999)])

    class Meta:
        verbose_name = "AD 认证平台"
        verbose_name_plural = "AD 认证平台"
        ordering = ["position", "pk"]

    def __str__(self):
        return self.name


class Configuration(models.Model):
    class Meta:
        verbose_name = "组织配置"
        verbose_name_plural = "组织配置"

    id = models.PositiveSmallIntegerField(primary_key=True, default=1, editable=False)
    root_department = models.CharField("钉钉根部门 ID", max_length=100, default="1")
    identity_anchor = models.CharField(max_length=64, blank=True, editable=False)
    root_ou = models.CharField("同步 AD 根 OU DN", max_length=500, blank=True)
    naming = models.CharField("新账号命名", max_length=30, choices=[("employee_id", "工号"), ("source_id", "钉钉 userId"), ("email", "邮箱前缀")], default="employee_id")
    match_field = models.CharField(
        "同步匹配字段", max_length=30,
        choices=[
            ("employee_id", "钉钉工号 → AD employeeID（唯一精确匹配可自动绑定）"),
            ("employee_username", "钉钉工号 → AD sAMAccountName（唯一精确匹配可自动绑定）"),
            ("email", "邮箱（仅建议，需人工确认）"),
            ("source_id", "userId 与 AD 账号名（仅建议）"),
        ],
        default="employee_id",
        help_text="更改匹配方式后须重新生成预览；AD 根 OU 外的现有账号不会自动纳入同步。",
    )
    attributes = models.JSONField("同步属性", default=list, blank=True, help_text="displayName、mail、title、department、telephoneNumber")
    clear_attributes = models.JSONField("允许来源空值清除的属性", default=list, blank=True, help_text="必须属于已启用的同步属性；默认空值不覆盖 AD")
    enable_new_accounts = models.BooleanField("新建账号初始化成功后启用", default=True)
    require_password_change = models.BooleanField("新建账号首次登录必须改密", default=True)
    protected_usernames = models.JSONField("额外保护账号", default=list, blank=True, help_text="填写服务账号、共享账号等 sAMAccountName；同步和密码重置均禁止操作")
    disable_missing = models.BooleanField("全量同步禁用离职人员", default=False)
    disable_limit = models.PositiveIntegerField("禁用人数阈值", default=5, validators=[MinValueValidator(1)])
    disable_percent = models.PositiveIntegerField("禁用比例阈值 %", default=10, validators=[MinValueValidator(1), MaxValueValidator(100)])
    sspr_enabled = models.BooleanField("开启员工密码重置", default=False)
    sspr_match = models.CharField("密码重置匹配方式", max_length=30, choices=[("employee_id", "钉钉工号 → AD employeeID"), ("email", "钉钉邮箱 → AD mail"), ("source_id", "钉钉 userId → AD sAMAccountName"), ("employee_username", "钉钉工号 → AD sAMAccountName")], default="employee_id")
    unlock_after_reset = models.BooleanField("重置后解锁", default=False)
    minimum_password_length = models.PositiveIntegerField("最短密码长度", default=8, validators=[MinValueValidator(8), MaxValueValidator(128)], help_text="可设置为 8–128 位；企业 AD 密码策略仍会进行最终校验。")
    schedule_enabled = models.BooleanField("开启定时同步", default=False)
    interval_minutes = models.PositiveIntegerField("同步间隔（分钟）", default=60, validators=[MinValueValidator(5)])
    updated_at = models.DateTimeField(auto_now=True)

    def clean(self):
        if self.pk != 1:
            raise ValidationError("只允许一份组织配置")
        allowed = {"displayName", "mail", "title", "department", "telephoneNumber"}
        if not isinstance(self.attributes, list) or any(x not in allowed for x in self.attributes):
            raise ValidationError("同步属性不合法")
        if not isinstance(self.clear_attributes, list) or any(x not in self.attributes for x in self.clear_attributes):
            raise ValidationError("允许空值清除的属性必须属于已启用的同步属性")
        if not isinstance(self.protected_usernames, list) or any(not isinstance(x, str) or not x.strip() for x in self.protected_usernames):
            raise ValidationError("保护账号必须为非空账号名列表")
        previous = Configuration.objects.filter(pk=self.pk).first()
        if previous and (Binding.objects.exists() or DepartmentBinding.objects.exists()) and (previous.root_department != self.root_department or previous.root_ou != self.root_ou):
            raise ValidationError("已有同步绑定时不能直接更换管理范围，请先审查并解除原绑定")

    @classmethod
    def current(cls):
        return cls.objects.get_or_create(pk=1)[0]

    def __str__(self):
        return "单组织设置"


class Snapshot(models.Model):
    started_at = models.DateTimeField(default=timezone.now)
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
    class Meta:
        verbose_name = "部门映射"
        verbose_name_plural = "部门映射"

    source_id = models.CharField(max_length=100, unique=True)
    dn = models.CharField(max_length=500)
    object_guid = models.UUIDField()
    manual = models.BooleanField(default=False)


class Job(models.Model):
    class Meta:
        verbose_name = "同步任务"
        verbose_name_plural = "同步任务"

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
    started_at = models.DateTimeField(null=True)
    finished_at = models.DateTimeField(null=True)


class Operation(models.Model):
    class Meta:
        verbose_name = "执行记录"
        verbose_name_plural = "执行记录"

    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    source_id = models.CharField(max_length=100)
    action = models.CharField(max_length=30)
    status = models.CharField(max_length=30, default="pending")
    target_guid = models.UUIDField(null=True)
    evidence = models.JSONField(default=dict)
    message = models.CharField(max_length=300, blank=True)


class Audit(models.Model):
    class Meta:
        verbose_name = "审计日志"
        verbose_name_plural = "审计日志"

    created_at = models.DateTimeField("请求/记录时间", default=timezone.now)
    actor = models.CharField("操作者标识", max_length=150)
    actor_name = models.CharField("员工姓名", max_length=200, blank=True, db_default="")
    employee_id = models.CharField("工号", max_length=100, blank=True, db_default="")
    action = models.CharField("操作类型", max_length=40)
    target = models.CharField("对象标识", max_length=150, blank=True)
    target_username = models.CharField("AD 账号", max_length=100, blank=True, db_default="")
    client_ip = models.GenericIPAddressField("来源 IP", null=True, blank=True)
    completed_at = models.DateTimeField("结果记录时间", null=True, blank=True)
    result = models.CharField("结果说明/失败原因", max_length=300)
    success = models.BooleanField("是否全部完成", default=True)
    state = models.CharField("结果状态", max_length=16, blank=True, default="", choices=[
        ("success", "成功"), ("failed", "失败"), ("partial", "部分完成"),
        ("pending", "处理未完成"), ("unknown", "待确认"),
    ])


class EmployeeSession(models.Model):
    digest = models.CharField(max_length=64, primary_key=True)
    source_id = models.CharField(max_length=100)
    display_name = models.CharField(max_length=200, blank=True)
    employee_id = models.CharField("工号", max_length=100, blank=True, db_default="")
    target_username = models.CharField("AD 账号", max_length=100, blank=True, db_default="")
    object_guid = models.UUIDField()
    config_fingerprint = models.CharField(max_length=64)
    expires_at = models.DateTimeField()
    used = models.BooleanField(default=False)


class RateWindow(models.Model):
    key = models.CharField(max_length=64, primary_key=True)
    starts_at = models.DateTimeField()
    count = models.PositiveIntegerField(default=0)


class RuntimeState(models.Model):
    """Operational observations do not invalidate configuration fingerprints."""
    id = models.PositiveSmallIntegerField(primary_key=True, default=1)
    last_full_success = models.DateTimeField(null=True)
    last_cleanup_at = models.DateTimeField(null=True)
    connection_checks = models.JSONField(default=dict)
    connections_checked_at = models.DateTimeField(null=True)

    @classmethod
    def current(cls):
        return cls.objects.get_or_create(pk=1)[0]
