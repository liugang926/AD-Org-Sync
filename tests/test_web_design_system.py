import re
import unittest
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader, select_autoescape

from sync_app.services.identity_relationships import BUSINESS_IDENTITY_STATUSES


TEMPLATE_DIR = Path("sync_app/web/templates")
STATIC_DIR = Path("sync_app/web/static")


class WebDesignSystemComponentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.environment = Environment(
            loader=FileSystemLoader(str(TEMPLATE_DIR)),
            autoescape=select_autoescape(("html",)),
        )

    def _render(self, source: str, **context) -> str:
        return self.environment.from_string(source).render(**context)

    def test_business_status_catalog_is_complete_and_semantic(self):
        self.assertEqual(
            set(BUSINESS_IDENTITY_STATUSES),
            {
                "pending",
                "bound",
                "unbound",
                "creatable",
                "conflict",
                "validation_unknown",
                "planned",
                "applied",
                "apply_failed",
            },
        )
        self.assertEqual(
            BUSINESS_IDENTITY_STATUSES["validation_unknown"]["level"],
            "unchecked",
        )
        self.assertEqual(
            BUSINESS_IDENTITY_STATUSES["planned"]["level"],
            "planned",
        )
        self.assertEqual(
            BUSINESS_IDENTITY_STATUSES["apply_failed"]["level"],
            "danger",
        )

    def test_status_badges_include_text_icon_and_canonical_palette(self):
        statuses = [
            ("pending", "Pending", "warning"),
            ("bound", "Bound", "success"),
            ("unbound", "Unbound", "info"),
            ("creatable", "Can create", "success"),
            ("conflict", "Conflict", "danger"),
            ("validation_unknown", "Validation unknown", "unchecked"),
            ("planned", "Planned", "planned"),
            ("applied", "Applied", "success"),
            ("apply_failed", "Apply failed", "danger"),
        ]
        markup = self._render(
            """
            {% import "components/ui.html" as ui %}
            {% for status, label, level in statuses %}
              {{ ui.business_status_badge(status, label) }}
            {% endfor %}
            """,
            statuses=statuses,
        )

        for status, label, level in statuses:
            with self.subTest(status=status):
                self.assertIn(f'data-business-status="{status}"', markup)
                self.assertIn(f"badge-{level}", markup)
                self.assertIn(label, markup)
        self.assertEqual(markup.count("badge__icon"), len(statuses))
        self.assertEqual(markup.count("badge__label"), len(statuses))

    def test_shared_page_filter_queue_state_and_drawer_components_render(self):
        markup = self._render(
            """
            {% import "components/ui.html" as ui %}
            {{ ui.page_header("People", "Review the queue") }}
            {{ ui.environment_badge("Production") }}
            {{ ui.snapshot_status("Current", "success", 42, "2026-07-19T10:00:00+00:00") }}
            {{ ui.stat("Pending", 4) }}
            {% call ui.filter_bar("/people", result_count=4, result_label="4 results") %}
              <label>Search<input name="q"></label>
              {{ ui.button("Apply filters", type="submit", variant="secondary") }}
            {% endcall %}
            {% call ui.work_queue_table("People queue", "4 results") %}
              <table><tbody><tr><td>Alice</td></tr></tbody></table>
            {% endcall %}
            {% call ui.sticky_batch_bar("test-batch", "data-test-batch") %}1 selected{% endcall %}
            {{ ui.empty_state("No results", "Change the filters") }}
            {% call ui.loading_state("Loading", "Please wait") %}{% endcall %}
            {% call ui.error_state("Could not load", "Try again") %}{% endcall %}
            {% call ui.detail_drawer("person-detail", "Alice", close_label="Close details") %}
              <p>Technical evidence</p>
            {% endcall %}
            """
        )

        for component in (
            "page-header",
            "snapshot-status",
            "stat-card",
            "filter-bar",
            "work-queue-table",
            "sticky-batch-bar",
            "empty-state",
            "loading-state",
            "error-state",
        ):
            with self.subTest(component=component):
                self.assertIn(f'data-component="{component}"', markup)
        self.assertIn('role="region" aria-label="People queue"', markup)
        self.assertIn('role="status" aria-live="polite"', markup)
        self.assertIn('role="alert" aria-live="assertive"', markup)
        self.assertIn('role="dialog" aria-modal="true"', markup)
        self.assertIn('aria-label="Close details"', markup)

    def test_time_component_preserves_raw_value_for_technical_details(self):
        markup = self._render(
            """
            {% import "components/ui.html" as ui %}
            {{ ui.local_time("2026-07-19T10:00:00+00:00", show_utc=True) }}
            """
        )

        self.assertIn('data-local-time', markup)
        self.assertIn('data-raw-utc', markup)
        self.assertEqual(
            markup.count("2026-07-19T10:00:00+00:00"),
            4,
        )

    def test_technical_identifiers_use_the_shared_copyable_component(self):
        value = "dry-run-" + ("identity-scope-" * 8) + "001"
        markup = self._render(
            """
            {% import "components/mode.html" as mode_ui %}
            {{ mode_ui.technical_identifier(
              presentation,
              value,
              "/execution-center/jobs/" ~ value,
              "View latest Dry Run",
              "",
              "Copy Dry Run ID",
              "Dry Run ID copied"
            ) }}
            """,
            presentation=SimpleNamespace(show_internal_identifiers=True),
            value=value,
        )

        self.assertIn('class="identifier"', markup)
        self.assertIn(f'title="{value}"', markup)
        self.assertIn(f'data-copy-value="{value}"', markup)
        self.assertIn('aria-label="Copy Dry Run ID"', markup)
        self.assertIn('data-copied-label="Dry Run ID copied"', markup)

    def test_table_script_implements_documented_keyboard_navigation(self):
        script = (STATIC_DIR / "app.js").read_text(encoding="utf-8")

        self.assertIn('".table-shell, .table-scroll, [data-table-region]"', script)
        for key in ("ArrowLeft", "ArrowRight", "ArrowDown", "ArrowUp", "Home", "End", "Enter"):
            with self.subTest(key=key):
                self.assertRegex(script, rf'event\.key\s*===\s*"{re.escape(key)}"')
        self.assertIn('row.setAttribute("tabindex", "-1")', script)
        self.assertIn('data-keyboard-row', script)

    def test_design_tokens_cover_all_six_status_levels(self):
        stylesheet = (STATIC_DIR / "app.css").read_text(encoding="utf-8")

        for level in ("success", "info", "warning", "danger", "unchecked", "planned"):
            with self.subTest(level=level):
                self.assertIn(f".badge-{level}", stylesheet)
        self.assertIn(":focus-visible", stylesheet)
        self.assertIn("--focus-ring:", stylesheet)
        self.assertRegex(
            stylesheet,
            r"\.identifier__link\s*\{[^}]*flex:\s*1\s+1\s+12ch;[^}]*width:\s*100%;"
            r"[^}]*min-width:\s*44px;",
        )

    def test_core_pages_keep_one_primary_action(self):
        dashboard = (TEMPLATE_DIR / "dashboard.html").read_text(encoding="utf-8")
        jobs = (TEMPLATE_DIR / "jobs.html").read_text(encoding="utf-8")
        config = (TEMPLATE_DIR / "config.html").read_text(encoding="utf-8")
        mappings = (TEMPLATE_DIR / "mappings.html").read_text(encoding="utf-8")

        self.assertEqual(
            len(re.findall(r"ui\.button\([^\n]+(?<!variant=\"secondary\")\)\s*}}", dashboard)),
            1,
        )
        self.assertEqual(
            jobs.count('href=job_center_summary.next_action_url'),
            1,
        )
        self.assertIn(
            'variant=("secondary" if apply_context.impact_count == 0 else "danger")',
            jobs,
        )
        self.assertIn(
            'variant=("secondary" if config_change_preview else "primary")',
            config,
        )
        self.assertIn(
            'ui.button(t("Save Override"), type="submit", variant="secondary")',
            mappings,
        )
        self.assertIn(
            'ui.button(t("Search"), type="submit", variant="secondary")',
            mappings,
        )

    def test_core_workflows_use_one_sticky_action_bar_and_four_to_six_summary_metrics(self):
        targets = (
            "getting_started.html",
            "execution_dry_run.html",
            "execution_plan_review.html",
            "execution_apply.html",
            "job_detail.html",
        )
        for name in targets:
            with self.subTest(template=name):
                text = (TEMPLATE_DIR / name).read_text(encoding="utf-8")
                self.assertEqual(text.count("ui.workflow_action_bar("), 1)

        getting_started = (TEMPLATE_DIR / "getting_started.html").read_text(encoding="utf-8")
        rollout_summary = getting_started.split('class="rollout-summary"', 1)[1].split(
            "</section>", 1
        )[0]
        self.assertEqual(rollout_summary.count("<div><span>"), 4)
        for name in (
            "execution_dry_run.html",
            "execution_plan_review.html",
            "execution_apply.html",
        ):
            with self.subTest(summary_template=name):
                text = (TEMPLATE_DIR / name).read_text(encoding="utf-8")
                self.assertEqual(text.count("ui.compact_stat("), 6)
        job_detail = (TEMPLATE_DIR / "job_detail.html").read_text(encoding="utf-8")
        self.assertEqual(job_detail.count('class="impact-card"'), 5)

        apply_template = (TEMPLATE_DIR / "execution_apply.html").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            'variant=("secondary" if context.impact_count == 0 else "danger")',
            apply_template,
        )


if __name__ == "__main__":
    unittest.main()
