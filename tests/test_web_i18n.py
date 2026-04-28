import re
import unittest
from pathlib import Path

from sync_app.web.i18n import (
    DEFAULT_UI_LANGUAGE,
    SUPPORTED_UI_LANGUAGES,
    TRANSLATIONS,
    detect_browser_ui_language,
    normalize_ui_language,
    translate,
)
from sync_app.web.routes_advanced_sync import ADVANCED_SYNC_CLIENT_I18N_KEYS


WEB_DIR = Path("sync_app/web")


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

    def test_simplified_chinese_catalog_covers_literal_web_ui_keys(self):
        zh_catalog = TRANSLATIONS["zh-CN"]
        missing: dict[str, list[str]] = {}
        literal_t_pattern = re.compile(r"\bt\(\s*([\"'])(.*?)(?<!\\)\1", re.S)
        for path in [
            *WEB_DIR.joinpath("templates").rglob("*.html"),
            *WEB_DIR.rglob("*.py"),
            *WEB_DIR.joinpath("static").rglob("*.js"),
        ]:
            text = path.read_text(encoding="utf-8")
            for match in literal_t_pattern.finditer(text):
                key = match.group(2)
                if not re.search(r"[A-Za-z]", key):
                    continue
                if key not in zh_catalog:
                    missing.setdefault(key, []).append(f"{path}:{text[: match.start()].count(chr(10)) + 1}")

        literal_route_title_pattern = re.compile(r"title\s*=\s*([\"'])(.*?)(?<!\\)\1", re.S)
        for path in WEB_DIR.glob("routes_*.py"):
            text = path.read_text(encoding="utf-8")
            for match in literal_route_title_pattern.finditer(text):
                key = match.group(2)
                if not re.search(r"[A-Za-z]", key):
                    continue
                if key not in zh_catalog:
                    missing.setdefault(key, []).append(f"{path}:{text[: match.start()].count(chr(10)) + 1}")

        self.assertEqual(missing, {})

    def test_simplified_chinese_catalog_covers_advanced_sync_client_keys(self):
        zh_catalog = TRANSLATIONS["zh-CN"]
        missing = sorted(
            {key for key in ADVANCED_SYNC_CLIENT_I18N_KEYS if re.search(r"[A-Za-z]", key) and key not in zh_catalog}
        )

        self.assertEqual(missing, [])
