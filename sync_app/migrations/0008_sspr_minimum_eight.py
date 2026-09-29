from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import migrations, models
from django.utils import timezone


def update_existing_default(apps, schema_editor):
    configuration = apps.get_model("sync_app", "Configuration")
    configuration.objects.using(schema_editor.connection.alias).filter(
        minimum_password_length=12,
    ).update(minimum_password_length=8, updated_at=timezone.now())


def restore_previous_default(apps, schema_editor):
    configuration = apps.get_model("sync_app", "Configuration")
    configuration.objects.using(schema_editor.connection.alias).filter(
        minimum_password_length=8,
    ).update(minimum_password_length=12, updated_at=timezone.now())


class Migration(migrations.Migration):
    dependencies = [
        ("sync_app", "0007_audit_state"),
    ]

    operations = [
        migrations.AlterField(
            model_name="configuration",
            name="minimum_password_length",
            field=models.PositiveIntegerField(
                "最短密码长度",
                default=8,
                validators=[MinValueValidator(8), MaxValueValidator(128)],
                help_text="可设置为 8–128 位；企业 AD 密码策略仍会进行最终校验。",
            ),
        ),
        migrations.RunPython(update_existing_default, restore_previous_default),
    ]
