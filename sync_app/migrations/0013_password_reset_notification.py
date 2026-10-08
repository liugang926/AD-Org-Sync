import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("sync_app", "0012_sync_employee_username")]

    operations = [
        migrations.CreateModel(
            name="PasswordResetNotification",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("state", models.CharField(choices=[("pending", "待发送"), ("sending", "发送中"), ("sent", "机器人已接受"), ("failed", "明确失败"), ("unknown", "结果不明，不自动重发")], default="pending", max_length=16, verbose_name="通知状态")),
                ("created_at", models.DateTimeField(auto_now_add=True, verbose_name="排队时间")),
                ("started_at", models.DateTimeField(blank=True, null=True, verbose_name="发送开始时间")),
                ("completed_at", models.DateTimeField(blank=True, null=True, verbose_name="通知结果时间")),
                ("message", models.CharField(blank=True, max_length=500, verbose_name="通知结果说明")),
                ("audit", models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name="password_notification", to="sync_app.audit", verbose_name="改密审计")),
            ],
            options={"verbose_name": "密码重置机器人通知", "verbose_name_plural": "密码重置机器人通知", "ordering": ["created_at", "pk"]},
        ),
    ]
