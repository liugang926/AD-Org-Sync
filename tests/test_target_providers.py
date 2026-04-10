import unittest

from sync_app.providers.target import ADLDAPSTargetProvider, build_target_provider, normalize_target_provider


class FakeADSyncClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def get_ou_dn(self, ou_path):
        return "/".join(ou_path)

    def get_users_batch(self, usernames):
        return {name: {"username": name} for name in usernames}

    def get_all_enabled_users(self):
        return ["alice"]

    def get_user_details(self, username):
        return {"username": username}

    def find_parent_groups_for_member(self, member_dn):
        return [member_dn]

    def inspect_department_group(self, **kwargs):
        return kwargs

    def ensure_ou(self, ou_name, parent_dn):
        return True, f"{parent_dn}/{ou_name}", True

    def ensure_department_group(self, **kwargs):
        return kwargs

    def ensure_custom_group(self, **kwargs):
        return kwargs

    def create_user(self, username, display_name, email, ou_dn):
        return True

    def update_user(self, username, display_name=None, email=None, target_ou=None):
        return True

    def disable_user(self, username):
        return True


class TargetProviderTests(unittest.TestCase):
    def test_normalize_target_provider_defaults_to_ad_ldaps(self):
        self.assertEqual(normalize_target_provider(None), "ad_ldaps")
        self.assertEqual(normalize_target_provider(""), "ad_ldaps")

    def test_ad_ldaps_target_provider_wraps_client(self):
        provider = ADLDAPSTargetProvider(FakeADSyncClient(server="ldap"))

        self.assertEqual(provider.get_ou_dn(["HQ", "IT"]), "HQ/IT")
        self.assertEqual(provider.get_all_enabled_users(), ["alice"])
        self.assertTrue(provider.disable_user("alice"))

    def test_build_target_provider_uses_ad_ldaps_adapter(self):
        provider = build_target_provider(
            client_factory=FakeADSyncClient,
            server="ldap.example.com",
            domain="example.com",
            username="svc_sync",
            password="Password123!",
        )

        self.assertIsInstance(provider, ADLDAPSTargetProvider)
        self.assertEqual(provider.client.kwargs["server"], "ldap.example.com")
