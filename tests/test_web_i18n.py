import unittest

from sync_app.web.i18n import (
    DEFAULT_UI_LANGUAGE,
    SUPPORTED_UI_LANGUAGES,
    TRANSLATIONS,
    detect_browser_ui_language,
    normalize_ui_language,
    translate,
)


class WebI18NTests(unittest.TestCase):
    def test_translation_catalogs_load_from_locale_files(self):
        self.assertIn("zh-CN", TRANSLATIONS)
        self.assertIn("Dashboard", TRANSLATIONS["zh-CN"])
        self.assertEqual(SUPPORTED_UI_LANGUAGES["en"], "English")

    def test_normalize_and_detect_language(self):
        self.assertEqual(normalize_ui_language("zh"), "zh-CN")
        self.assertEqual(normalize_ui_language("en-US"), "en")
        self.assertEqual(normalize_ui_language(None), DEFAULT_UI_LANGUAGE)
        self.assertEqual(detect_browser_ui_language("zh-CN,zh;q=0.9"), "zh-CN")
        self.assertEqual(detect_browser_ui_language("en-US,en;q=0.9"), "en")

    def test_translate_uses_catalog_and_falls_back_to_source_text(self):
        self.assertEqual(translate("zh-CN", "Dashboard"), TRANSLATIONS["zh-CN"]["Dashboard"])
        self.assertEqual(translate("en", "Dashboard"), "Dashboard")
        self.assertEqual(translate("zh-CN", "Unknown Text"), "Unknown Text")
