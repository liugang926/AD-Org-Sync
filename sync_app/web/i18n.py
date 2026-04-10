from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_UI_LANGUAGE = "en"
SUPPORTED_UI_LANGUAGES = {
    "en": "English",
    "zh-CN": "\u7b80\u4f53\u4e2d\u6587",
}
LOCALES_DIR = Path(__file__).with_name("locales")


@lru_cache(maxsize=1)
def load_translation_catalogs() -> dict[str, dict[str, str]]:
    catalogs: dict[str, dict[str, str]] = {}
    if not LOCALES_DIR.exists():
        return catalogs
    for locale_file in sorted(LOCALES_DIR.glob("*.json")):
        try:
            payload = json.loads(locale_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            catalogs[locale_file.stem] = {
                str(key): str(value)
                for key, value in payload.items()
            }
    return catalogs


TRANSLATIONS = load_translation_catalogs()


def detect_browser_ui_language(accept_language: str | None) -> str:
    first_token = str(accept_language or "").split(",", 1)[0].strip()
    if not first_token:
        return DEFAULT_UI_LANGUAGE
    language_tag = first_token.split(";", 1)[0].strip()
    return "zh-CN" if normalize_ui_language(language_tag) == "zh-CN" else DEFAULT_UI_LANGUAGE


def normalize_ui_language(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"zh", "zh-cn", "zh_hans", "zh-hans", "zh_cn"}:
        return "zh-CN"
    if normalized.startswith("en"):
        return "en"
    return DEFAULT_UI_LANGUAGE


def translate(language: str, text: str, **params: Any) -> str:
    raw_text = str(text or "")
    template = TRANSLATIONS.get(language, {}).get(raw_text, raw_text)
    if not params:
        return template
    try:
        return template.format(**params)
    except Exception:
        return template
