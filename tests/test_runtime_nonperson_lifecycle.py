from types import SimpleNamespace

from sync_app.services.runtime_user_phase import plan_disable_actions


class _Hooks:
    def __init__(self):
        self.planned = []

    def has_exception_rule(self, _rule_type, _value):
        return False

    def add_planned_operation(self, **payload):
        self.planned.append(payload)

    def record_event(self, *_args, **_kwargs):
        return None


def test_service_account_binding_is_excluded_from_source_absence_offboarding():
    binding = SimpleNamespace(
        connector_id="default",
        ad_username="svc_backup",
        source_user_id="service-backup",
        source_provider="wecom",
        target_object_dn="CN=svc_backup,OU=Service,DC=example,DC=local",
    )
    platform_account = SimpleNamespace(
        provider_id="wecom",
        connector_id="default",
        platform_account_id="service-backup",
        account_type="service",
        is_excluded=False,
    )
    hooks = _Hooks()
    ctx = SimpleNamespace(
        environment=SimpleNamespace(
            source_scope={"scope_type": "full"},
            bot=None,
            source_provider_name="wecom",
        ),
        config=SimpleNamespace(source_provider="wecom"),
        organization=SimpleNamespace(org_id="default"),
        repositories=SimpleNamespace(
            user_binding_repo=SimpleNamespace(
                list_enabled_binding_records=lambda **_kwargs: [binding]
            ),
            platform_account_repo=SimpleNamespace(
                list_accounts=lambda **_kwargs: [platform_account]
            ),
            offboarding_repo=SimpleNamespace(clear_pending=lambda **_kwargs: None),
        ),
        actions=SimpleNamespace(disable_actions=[]),
        working=SimpleNamespace(
            managed_ad_identities=set(),
            enabled_ad_users_by_connector={"default": ["svc_backup"]},
            current_source_ad_usernames_by_connector={"default": set()},
            source_user_ids=set(),
        ),
        identity=SimpleNamespace(binding_resolution_details={}),
        policy_settings=SimpleNamespace(
            offboarding_grace_days=0,
            offboarding_notify_managers=False,
        ),
        hooks=hooks,
        job_id="dry-run-service-account",
    )

    plan_disable_actions(
        ctx,
        is_protected_ad_account=lambda *_args: False,
        record_exception_skip=lambda **_kwargs: None,
        record_protected_account_skip=lambda **_kwargs: None,
    )

    assert ctx.actions.disable_actions == []
    assert ctx.working.managed_ad_identities == set()
    assert hooks.planned == [
        {
            "object_type": "user",
            "operation_type": "exclude_non_person_account",
            "source_id": "service-backup",
            "target_dn": "CN=svc_backup,OU=Service,DC=example,DC=local",
            "risk_level": "normal",
            "desired_state": {
                "connector_id": "default",
                "ad_username": "svc_backup",
                "account_type": "service",
                "reason": "non_person_accounts_are_never_offboarded_from_source_absence",
            },
        }
    ]
