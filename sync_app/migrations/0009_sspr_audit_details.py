from django.db import migrations, models
from django.utils import timezone


class Migration(migrations.Migration):
    dependencies = [("sync_app", "0008_sspr_minimum_eight")]

    operations = [
        migrations.AddField(
            model_name="audit", name="actor_name",
            field=models.CharField("员工姓名", blank=True, max_length=200, db_default=""),
        ),
        migrations.AddField(
            model_name="audit", name="employee_id",
            field=models.CharField("工号", blank=True, max_length=100, db_default=""),
        ),
        migrations.AddField(
            model_name="audit", name="target_username",
            field=models.CharField("AD 账号", blank=True, max_length=100, db_default=""),
        ),
        migrations.AddField(
            model_name="audit", name="client_ip",
            field=models.GenericIPAddressField("来源 IP", blank=True, null=True),
        ),
        migrations.AddField(
            model_name="audit", name="completed_at",
            field=models.DateTimeField("结果记录时间", blank=True, null=True),
        ),
        migrations.AddField(
            model_name="employeesession", name="employee_id",
            field=models.CharField("工号", blank=True, max_length=100, db_default=""),
        ),
        migrations.AddField(
            model_name="employeesession", name="target_username",
            field=models.CharField("AD 账号", blank=True, max_length=100, db_default=""),
        ),
        migrations.AlterField(
            model_name="audit", name="created_at",
            field=models.DateTimeField("请求/记录时间", default=timezone.now),
        ),
        migrations.AlterField(
            model_name="audit", name="actor",
            field=models.CharField("操作者标识", max_length=150),
        ),
        migrations.AlterField(
            model_name="audit", name="action",
            field=models.CharField("操作类型", max_length=40),
        ),
        migrations.AlterField(
            model_name="audit", name="target",
            field=models.CharField("对象标识", blank=True, max_length=150),
        ),
        migrations.AlterField(
            model_name="audit", name="result",
            field=models.CharField("结果说明/失败原因", max_length=300),
        ),
        migrations.AlterField(
            model_name="audit", name="success",
            field=models.BooleanField("是否全部完成", default=True),
        ),
        migrations.AlterField(
            model_name="audit", name="state",
            field=models.CharField(
                "结果状态", blank=True, default="", max_length=16,
                choices=[("success", "成功"), ("failed", "失败"), ("partial", "部分完成"),
                         ("pending", "处理未完成"), ("unknown", "待确认")],
            ),
        ),
    ]
