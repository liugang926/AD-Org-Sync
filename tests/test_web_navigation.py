import re

from sync_app.web.navigation import CANONICAL_ROUTE_PATHS, NAVIGATION_GROUPS
from tests.helpers.web_authz_case import WebAuthzBaseTestCase


class WebNavigationTests(WebAuthzBaseTestCase):
    @staticmethod
    def _navigation_markup(body: str) -> str:
        match = re.search(r'<nav data-sidebar-nav>(.*?)</nav>', body, re.S)
        if not match:
            raise AssertionError("sidebar navigation was not rendered")
        return match.group(1)

    def test_basic_mode_is_task_focused_and_keeps_one_current_page(self):
        self._login("superadmin")

        response = self._route("/dashboard", "GET")(self._request("/dashboard"))

        self.assertEqual(response.status_code, 200)
        navigation = self._navigation_markup(self._text(response))
        hrefs = re.findall(r'<a href="([^"]+)"', navigation)
        self.assertEqual(
            hrefs,
            [
                "/overview/control-tower",
                "/data-sources/source-directory",
                "/identity-governance/conflicts",
                "/execution-center/run-review",
            ],
        )
        self.assertEqual(navigation.count('aria-current="page"'), 1)
        self.assertIn("Data Sources", navigation)
        self.assertIn("Identity Governance", navigation)
        self.assertIn("Execution Center", navigation)
        self.assertNotIn("Policy Center", navigation)
        self.assertNotIn("System Management", navigation)

    def test_advanced_mode_exposes_target_sections_with_rbac_filtering(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"

        response = self._route("/dashboard", "GET")(self._request("/dashboard"))

        self.assertEqual(response.status_code, 200)
        navigation = self._navigation_markup(self._text(response))
        for label in (
            "Data Sources",
            "Identity Governance",
            "Sync Policies",
            "Execution Center",
            "Operations Center",
            "System Management",
            "Administrators &amp; Permissions",
        ):
            with self.subTest(label=label):
                self.assertIn(label, navigation)
        for href in CANONICAL_ROUTE_PATHS.values():
            with self.subTest(href=href):
                self.assertIn(f'href="{href}"', navigation)

        self._login("operator1")
        self.session["ui_mode"] = "advanced"
        operator_response = self._route("/dashboard", "GET")(
            self._request("/dashboard")
        )
        operator_navigation = self._navigation_markup(self._text(operator_response))
        self.assertNotIn(CANONICAL_ROUTE_PATHS["config"], operator_navigation)
        self.assertNotIn(CANONICAL_ROUTE_PATHS["organizations"], operator_navigation)
        self.assertNotIn(CANONICAL_ROUTE_PATHS["users"], operator_navigation)

    def test_legacy_get_routes_remain_registered_during_page_migration(self):
        legacy_paths = {
            "dashboard": "/dashboard",
            "config": "/config",
            "source-directory": "/source-directory",
            "data-quality": "/data-quality",
            "conflicts": "/conflicts",
            "mappings": "/mappings",
            "exceptions": "/exceptions",
            "advanced-sync": "/advanced-sync",
            "jobs": "/jobs",
            "lifecycle": "/lifecycle",
            "automation-center": "/automation-center",
            "integrations": "/integrations",
            "audit": "/audit",
            "organizations": "/organizations",
            "users": "/users",
            "database": "/database",
            "account": "/account",
        }

        for page, legacy_path in legacy_paths.items():
            canonical_path = CANONICAL_ROUTE_PATHS[page]
            with self.subTest(page=page, path=canonical_path):
                canonical_endpoint = self._route(canonical_path, "GET")
                legacy_endpoint = self._route(legacy_path, "GET")
                if page == "config":
                    self.assertIsNot(canonical_endpoint, legacy_endpoint)
                else:
                    self.assertIs(canonical_endpoint, legacy_endpoint)

        for page in ("snapshots", "binding-reconciliation", "sync-scope"):
            with self.subTest(page=page):
                self.assertTrue(callable(self._route(CANONICAL_ROUTE_PATHS[page], "GET")))

    def test_connector_center_is_dedicated_while_legacy_config_remains_available(self):
        self._login("superadmin")
        self.session["ui_mode"] = "advanced"

        connector_page = self._route(CANONICAL_ROUTE_PATHS["config"], "GET")(
            self._request(CANONICAL_ROUTE_PATHS["config"])
        )
        legacy_page = self._route("/config", "GET")(self._request("/config"))

        connector_body = self._text(connector_page)
        self.assertIn("Save Connection Settings", connector_body)
        self.assertIn("Test Saved Connections", connector_body)
        self.assertNotIn("Web Deployment", connector_body)
        self.assertIn("Web Deployment", self._text(legacy_page))

    def test_canonical_routes_keep_existing_permission_checks(self):
        self._login("operator1")

        response = self._route(CANONICAL_ROUTE_PATHS["config"], "GET")(
            self._request(CANONICAL_ROUTE_PATHS["config"])
        )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")

    def test_navigation_labels_are_present_in_both_catalogs(self):
        from sync_app.web.i18n import TRANSLATIONS

        labels = {
            group.label for group in NAVIGATION_GROUPS
        } | {
            item.label for group in NAVIGATION_GROUPS for item in group.items
        }
        for language in ("en", "zh-CN"):
            with self.subTest(language=language):
                self.assertEqual(labels - set(TRANSLATIONS[language]), set())
