import re
import unittest
from pathlib import Path


TEMPLATE_DIR = Path("sync_app/web/templates")
STATIC_DIR = Path("sync_app/web/static")
VENDOR_DIR = STATIC_DIR / "vendor"
ALLOWED_RAW_BUTTON_FILES = {
    TEMPLATE_DIR / "base.html",
    TEMPLATE_DIR / "components" / "ui.html",
}
INLINE_EVENT_PATTERN = re.compile(r"\b(?:onclick|onchange|onsubmit)\s*=")
INLINE_STYLE_PATTERN = re.compile(r"\bstyle\s*=")


class WebTemplateConventionTests(unittest.TestCase):
    def test_templates_do_not_use_inline_event_handlers(self):
        for path in TEMPLATE_DIR.rglob("*.html"):
            with self.subTest(path=path):
                text = path.read_text(encoding="utf-8")
                self.assertIsNone(INLINE_EVENT_PATTERN.search(text))

    def test_templates_do_not_use_inline_style_attributes(self):
        for path in TEMPLATE_DIR.rglob("*.html"):
            with self.subTest(path=path):
                text = path.read_text(encoding="utf-8")
                self.assertIsNone(INLINE_STYLE_PATTERN.search(text))

    def test_raw_button_markup_is_limited_to_base_and_ui_macro(self):
        for path in TEMPLATE_DIR.rglob("*.html"):
            if path in ALLOWED_RAW_BUTTON_FILES:
                continue
            with self.subTest(path=path):
                text = path.read_text(encoding="utf-8")
                self.assertNotIn("<button", text)
                self.assertNotIn('class="button', text)

    def test_base_template_loads_static_assets(self):
        base_template = (TEMPLATE_DIR / "base.html").read_text(encoding="utf-8")

        self.assertIn('/static/app.css', base_template)
        self.assertIn('/static/app.js', base_template)
        self.assertIn('/static/config-page.js', base_template)
        self.assertIn('/static/mappings-page.js', base_template)
        self.assertTrue((STATIC_DIR / "app.css").exists())
        self.assertTrue((STATIC_DIR / "app.js").exists())
        self.assertTrue((STATIC_DIR / "config-page.js").exists())
        self.assertTrue((STATIC_DIR / "mappings-page.js").exists())

    def test_base_template_uses_local_vendor_assets(self):
        base_template = (TEMPLATE_DIR / "base.html").read_text(encoding="utf-8")

        self.assertIn('/static/vendor/lucide.min.js', base_template)
        self.assertIn('/static/vendor/tom-select.complete.min.js', base_template)
        self.assertIn('/static/vendor/tom-select.default.min.css', base_template)
        self.assertNotIn("https://unpkg.com", base_template)
        self.assertNotIn("https://cdn.jsdelivr.net", base_template)
        self.assertNotIn("https://fonts.googleapis.com", base_template)
        self.assertTrue((VENDOR_DIR / "lucide.min.js").exists())
        self.assertTrue((VENDOR_DIR / "tom-select.complete.min.js").exists())
        self.assertTrue((VENDOR_DIR / "tom-select.default.min.css").exists())

    def test_shared_feedback_and_confirmation_markup_is_accessible(self):
        base_template = (TEMPLATE_DIR / "base.html").read_text(encoding="utf-8")
        forms_template = (TEMPLATE_DIR / "components" / "forms.html").read_text(encoding="utf-8")
        app_script = (STATIC_DIR / "app.js").read_text(encoding="utf-8")

        self.assertIn('aria-live="polite"', base_template)
        self.assertIn('aria-modal="true"', base_template)
        self.assertIn('data-confirm-input', base_template)
        self.assertIn('aria-describedby="{{ field_id }}-help"', forms_template)
        self.assertIn('restoreFocusTo.focus()', app_script)
        self.assertIn('event.key === "Tab"', app_script)

    def test_bulk_confirmation_reports_dynamic_scope_and_requires_selection(self):
        conflicts = (TEMPLATE_DIR / "conflicts.html").read_text(encoding="utf-8")
        app_script = (STATIC_DIR / "app.js").read_text(encoding="utf-8")

        self.assertIn('data-selection-requires-items', conflicts)
        self.assertIn('"{selected_count}"', conflicts)
        self.assertIn('"{selected_action}"', conflicts)
        self.assertIn('.replaceAll("{selected_count}"', app_script)
        self.assertIn('.replaceAll("{selected_action}"', app_script)

    def test_irreversible_deletes_require_typed_confirmation(self):
        organizations = (TEMPLATE_DIR / "organizations.html").read_text(encoding="utf-8")
        connectors = (TEMPLATE_DIR / "connectors.html").read_text(encoding="utf-8")

        self.assertIn('data-confirm-require="{{ organization.name }}"', organizations)
        self.assertIn('data-confirm-require="{{ connector.connector_id }}"', connectors)

    def test_scoped_deletes_describe_the_affected_record(self):
        templates = {
            "sync_policy_attribute_mappings.html": "Old value",
            "exceptions.html": "Match Value",
            "integration_center.html": "Target URL",
            "mappings.html": "Source User ID",
        }
        for name, expected_detail in templates.items():
            with self.subTest(template=name):
                text = (TEMPLATE_DIR / name).read_text(encoding="utf-8")
                self.assertIn("data-confirm-title", text)
                self.assertIn(expected_detail, text)

    def test_styles_use_tokens_outside_the_token_catalog(self):
        stylesheet = (STATIC_DIR / "app.css").read_text(encoding="utf-8")
        root_match = re.search(r":root\s*\{(.*?)\n\}", stylesheet, re.S)

        self.assertIsNotNone(root_match)
        stylesheet_without_catalog = stylesheet.replace(root_match.group(1), "", 1)
        raw_colors = re.findall(
            r"(?i)(?:#[0-9a-f]{3,8}\b|rgba?\([^)]*\))",
            stylesheet_without_catalog,
        )
        self.assertEqual(raw_colors, [])
        self.assertLessEqual(stylesheet.count("!important"), 6)

    def test_login_fields_declare_enterprise_friendly_autocomplete(self):
        login_template = (TEMPLATE_DIR / "login.html").read_text(encoding="utf-8")

        self.assertIn('autocomplete="username"', login_template)
        self.assertIn('autocomplete="current-password"', login_template)


if __name__ == "__main__":
    unittest.main()
