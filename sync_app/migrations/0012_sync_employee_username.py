from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("sync_app", "0011_sspr_employee_username")]

    operations = [
        migrations.AlterField(
            model_name="configuration",
            name="match_field",
            field=models.CharField(
                "同步匹配字段", max_length=30,
                choices=[
                    ("employee_id", "钉钉工号 → AD employeeID（唯一精确匹配可自动绑定）"),
                    ("employee_username", "钉钉工号 → AD sAMAccountName（唯一精确匹配可自动绑定）"),
                    ("email", "邮箱（仅建议，需人工确认）"),
                    ("source_id", "userId 与 AD 账号名（仅建议）"),
                ],
                default="employee_id",
                help_text="更改匹配方式后须重新生成预览；AD 根 OU 外的现有账号不会自动纳入同步。",
            ),
        ),
    ]
