import unittest
import random
from datetime import datetime, timezone

from sync_app.core.fingerprints import FINGERPRINT_VERSION, canonical_json, fingerprint_json
from sync_app.services.runtime_plan import compute_plan_fingerprint


class FingerprintTests(unittest.TestCase):
    def test_canonical_json_is_stable_for_mapping_and_set_order(self):
        left = {"values": {3, 1, 2}, "nested": {"b": 2, "a": 1}}
        right = {"nested": {"a": 1, "b": 2}, "values": {2, 3, 1}}

        self.assertEqual(canonical_json(left), canonical_json(right))
        self.assertEqual(
            fingerprint_json(left, namespace="test"),
            fingerprint_json(right, namespace="test"),
        )

    def test_fingerprint_is_versioned_and_namespace_scoped(self):
        payload = {"at": datetime(2026, 1, 2, tzinfo=timezone.utc)}

        config_fingerprint = fingerprint_json(payload, namespace="sync-config")
        plan_fingerprint = fingerprint_json(payload, namespace="sync-plan")

        self.assertTrue(config_fingerprint.startswith(f"{FINGERPRINT_VERSION}:"))
        self.assertNotEqual(config_fingerprint, plan_fingerprint)

    def test_plan_fingerprint_includes_desired_state(self):
        base_item = {
            "object_type": "user",
            "operation_type": "update_user",
            "source_id": "alice",
            "department_id": "10",
            "target_dn": "CN=alice,OU=Users,DC=example,DC=com",
            "risk_level": "normal",
        }

        first = compute_plan_fingerprint([{**base_item, "desired_state": {"title": "Engineer"}}])
        second = compute_plan_fingerprint([{**base_item, "desired_state": {"title": "Manager"}}])

        self.assertNotEqual(first, second)
        self.assertTrue(first.startswith(f"{FINGERPRINT_VERSION}:"))

    def test_plan_fingerprint_ignores_diagnostic_explanations(self):
        base_item = {
            "object_type": "user",
            "operation_type": "create_user",
            "source_id": "alice",
            "target_dn": "CN=alice,DC=example,DC=com",
        }
        first = compute_plan_fingerprint(
            [
                {
                    **base_item,
                    "desired_state": {
                        "ad_username": "alice",
                        "binding_resolution": {"source": "generated", "explanation": "Generated safely"},
                    },
                }
            ]
        )
        second = compute_plan_fingerprint(
            [
                {
                    **base_item,
                    "desired_state": {
                        "ad_username": "alice",
                        "binding_resolution": {"source": "existing", "explanation": "Persisted binding"},
                    },
                }
            ]
        )

        self.assertEqual(first, second)

    def test_plan_fingerprint_is_independent_of_operation_order(self):
        items = [
            {
                "object_type": "group",
                "operation_type": "create_group",
                "source_id": "20",
                "target_dn": "CN=G20,DC=example,DC=com",
                "desired_state": {"display_name": "Group 20"},
            },
            {
                "object_type": "user",
                "operation_type": "create_user",
                "source_id": "alice",
                "target_dn": "CN=alice,DC=example,DC=com",
                "desired_state": {"title": "Engineer"},
            },
        ]

        self.assertEqual(compute_plan_fingerprint(items), compute_plan_fingerprint(list(reversed(items))))

    def test_canonical_fingerprint_invariants_hold_across_randomized_key_orders(self):
        generator = random.Random(20260710)
        canonical_payload = {
            "tenant": "default",
            "settings": {f"key_{index}": index for index in range(20)},
            "members": {f"user_{index}" for index in range(12)},
        }
        expected = fingerprint_json(canonical_payload, namespace="property-test")

        for _ in range(100):
            settings_items = list(canonical_payload["settings"].items())
            generator.shuffle(settings_items)
            top_level_items = [
                ("members", set(canonical_payload["members"])),
                ("settings", dict(settings_items)),
                ("tenant", "default"),
            ]
            generator.shuffle(top_level_items)
            self.assertEqual(
                fingerprint_json(dict(top_level_items), namespace="property-test"),
                expected,
            )

    def test_semantic_mutations_change_fingerprint_across_randomized_payloads(self):
        generator = random.Random(42)
        for index in range(100):
            payload = {
                "index": index,
                "enabled": bool(generator.getrandbits(1)),
                "threshold": generator.randint(0, 100_000),
            }
            mutated = {**payload, "threshold": payload["threshold"] + 1}
            self.assertNotEqual(
                fingerprint_json(payload, namespace="mutation-test"),
                fingerprint_json(mutated, namespace="mutation-test"),
            )


if __name__ == "__main__":
    unittest.main()
