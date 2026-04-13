import unittest
from pathlib import Path
from types import SimpleNamespace


class WebMappingsPageTemplateTests(unittest.TestCase):
    def test_mappings_template_renders_pagination_and_department_name(self):
        try:
            from jinja2 import Environment, FileSystemLoader, select_autoescape
        except ImportError as exc:
            self.skipTest(f"jinja2 unavailable in current interpreter: {exc}")

        template_dir = Path("sync_app/web/templates")
        env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=select_autoescape(["html", "xml"]),
        )
        template = env.get_template("mappings.html")
        rendered = template.render(
            title="Mappings",
            page="mappings",
            current_user=SimpleNamespace(username="admin", role="super_admin"),
            app_version="test",
            flash=None,
            ui_language="en",
            language_options={"en": "English", "zh-CN": "简体中文"},
            language_urls={"en": "/mappings?lang=en", "zh-CN": "/mappings?lang=zh-CN"},
            current_path="/mappings",
            t=lambda text, **params: str(text).format(**params) if params else str(text),
            can=lambda capability: True,
            csrf_token="test-token",
            mapping_query="alice",
            mapping_status="all",
            bindings=[
                SimpleNamespace(
                    source_user_id="alice",
                    ad_username="alice.ad",
                    is_enabled=True,
                    source="manual",
                    notes="Headquarters",
                    updated_at="2026-03-27 10:00:00",
                )
            ],
            overrides=[
                SimpleNamespace(
                    source_user_id="alice",
                    primary_department_id="2001",
                    notes="Preferred placement",
                    updated_at="2026-03-27 10:00:00",
                )
            ],
            binding_page_data={
                "total_items": 1,
                "page": 1,
                "total_pages": 1,
                "has_previous": False,
                "has_next": False,
                "previous_page": 1,
                "next_page": 1,
            },
            override_page_data={
                "total_items": 1,
                "page": 1,
                "total_pages": 1,
                "has_previous": False,
                "has_next": False,
                "previous_page": 1,
                "next_page": 1,
            },
            department_name_map={"2001": "Technical Support"},
        )

        self.assertIn("Identity Binding List", rendered)
        self.assertIn("Source User ID", rendered)
        self.assertIn('id="binding_source_user_id"', rendered)
        self.assertIn('data-mapping-source-user-select', rendered)
        self.assertIn('data-mapping-target-user-select', rendered)
        self.assertIn('data-mapping-source-department-select', rendered)
        self.assertIn("page 1 / 1", rendered)
        self.assertIn("Technical Support", rendered)
        self.assertIn("alice.ad", rendered)


if __name__ == "__main__":
    unittest.main()
