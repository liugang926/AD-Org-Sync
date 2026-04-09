import re
import unittest
from pathlib import Path


TEMPLATE_DIR = Path("sync_app/web/templates")
STATIC_DIR = Path("sync_app/web/static")
ALLOWED_RAW_BUTTON_FILES = {
    TEMPLATE_DIR / "base.html",
    TEMPLATE_DIR / "components" / "ui.html",
}
INLINE_EVENT_PATTERN = re.compile(r"\b(?:onclick|onchange|onsubmit)\s*=")


class WebTemplateConventionTests(unittest.TestCase):
    def test_templates_do_not_use_inline_event_handlers(self):
        for path in TEMPLATE_DIR.rglob("*.html"):
            with self.subTest(path=path):
                text = path.read_text(encoding="utf-8")
                self.assertIsNone(INLINE_EVENT_PATTERN.search(text))

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
        self.assertTrue((STATIC_DIR / "app.css").exists())
        self.assertTrue((STATIC_DIR / "app.js").exists())


if __name__ == "__main__":
    unittest.main()
