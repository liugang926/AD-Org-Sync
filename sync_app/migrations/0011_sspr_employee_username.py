from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("sync_app", "0010_employee_page_settings")]

    operations = [
        migrations.AlterField(
            model_name="configuration",
            name="sspr_match",
            field=models.CharField(
                "密码重置匹配方式",
                max_length=30,
                choices=[
                    ("employee_id", "钉钉工号 → AD employeeID"),
                    ("email", "钉钉邮箱 → AD mail"),
                    ("source_id", "钉钉 userId → AD sAMAccountName"),
                    ("employee_username", "钉钉工号 → AD sAMAccountName"),
                ],
                default="employee_id",
            ),
        ),
    ]
