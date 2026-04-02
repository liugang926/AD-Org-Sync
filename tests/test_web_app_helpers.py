import unittest

from sync_app.web.helpers import paginate_records, parse_bulk_bindings


class WebAppHelperTests(unittest.TestCase):
    def test_parse_bulk_bindings_accepts_csv_lines(self):
        rows, errors = parse_bulk_bindings("alice,alice.ad,Headquarters\nbob,bob.ad\n")

        self.assertEqual(errors, [])
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["source_user_id"], "alice")
        self.assertEqual(rows[0]["ad_username"], "alice.ad")
        self.assertEqual(rows[0]["notes"], "Headquarters")
        self.assertEqual(rows[1]["notes"], "")

    def test_parse_bulk_bindings_reports_invalid_rows(self):
        rows, errors = parse_bulk_bindings("alice\n,bob.ad\n")

        self.assertEqual(rows, [])
        self.assertEqual(len(errors), 2)
        self.assertIn("Line 1", errors[0])

    def test_paginate_records_slices_and_reports_navigation(self):
        page = paginate_records(list(range(1, 8)), page=2, page_size=3)

        self.assertEqual(page["items"], [4, 5, 6])
        self.assertEqual(page["page"], 2)
        self.assertEqual(page["total_pages"], 3)
        self.assertTrue(page["has_prev"])
        self.assertTrue(page["has_next"])


if __name__ == "__main__":
    unittest.main()
