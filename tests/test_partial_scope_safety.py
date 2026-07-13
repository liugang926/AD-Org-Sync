import unittest
from types import SimpleNamespace

from sync_app.services.runtime_apply_phase import apply_final_state_updates
from sync_app.services.runtime_user_phase import plan_disable_actions


class _Hooks:
    def __init__(self):
        self.events = []
    def record_event(self, *args, **kwargs):
        self.events.append((args, kwargs))


class _StateManager:
    def __init__(self):
        self.cleaned = False
        self.completed = False
    def cleanup_old_users(self, _ids):
        self.cleaned = True
    def set_sync_complete(self, _value):
        self.completed = True


class PartialScopeSafetyTests(unittest.TestCase):
    def _ctx(self, scope_type):
        hooks = _Hooks()
        manager = _StateManager()
        ctx = SimpleNamespace(
            environment=SimpleNamespace(source_scope={"scope_type": scope_type}),
            actions=SimpleNamespace(disable_actions=["would-disable"]),
            hooks=hooks,
            repositories=SimpleNamespace(state_manager=manager),
            working=SimpleNamespace(source_user_ids={"alice"}),
            sync_stats={"error_count": 0},
        )
        return ctx, hooks, manager

    def test_selected_users_suppresses_offboarding_planning(self):
        ctx, hooks, _manager = self._ctx("selected_users")
        plan_disable_actions(ctx, is_protected_ad_account=lambda *_: False, record_exception_skip=lambda **_: None, record_protected_account_skip=lambda **_: None)
        self.assertEqual(ctx.actions.disable_actions, [])
        self.assertIn("partial_scope_offboarding_suppressed", [args[1] for args, _kwargs in hooks.events])

    def test_department_scope_does_not_update_global_cleanup_marker(self):
        ctx, hooks, manager = self._ctx("department")
        apply_final_state_updates(ctx)
        self.assertFalse(manager.cleaned)
        self.assertFalse(manager.completed)
        self.assertIn("partial_scope_global_cleanup_suppressed", [args[1] for args, _kwargs in hooks.events])

    def test_full_scope_retains_offboarding_cleanup_semantics(self):
        ctx, _hooks, manager = self._ctx("full")
        apply_final_state_updates(ctx)
        self.assertTrue(manager.cleaned)
        self.assertTrue(manager.completed)


if __name__ == "__main__":
    unittest.main()
