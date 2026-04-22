import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from sync_app.services.data_quality_center import (
    build_data_quality_center_context,
    build_data_quality_export_rows,
    persist_data_quality_snapshot,
)
from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories.organizations import OrganizationRepository
from sync_app.storage.repositories.system import DataQualitySnapshotRepository


class DataQualityCenterTests(unittest.TestCase):
    def _build_db(self, temp_dir: str) -> DatabaseManager:
        db_path = Path(temp_dir) / "data_quality_center.db"
        config_path = str((Path(temp_dir) / "data_quality_center.ini").resolve())
        db_manager = DatabaseManager(db_path=str(db_path))
        db_manager.initialize()
        OrganizationRepository(db_manager).ensure_default(config_path=config_path)
        return db_manager

    @staticmethod
    def _snapshot_payload(
        *,
        generated_at: str,
        total_users: int,
        missing_email: int,
        missing_employee_id: int,
        department_anomaly_count: int,
        naming_risk_count: int,
        duplicate_email_count: int,
        duplicate_employee_id_count: int,
        error_issue_count: int,
        warning_issue_count: int,
        repair_items: list[dict[str, object]],
    ) -> dict[str, object]:
        return {
            "generated_at": generated_at,
            "analysis_notes": ["Counts reflect unique source users merged across departments."],
            "summary": {
                "total_users": total_users,
                "users_missing_email": missing_email,
                "users_missing_employee_id": missing_employee_id,
                "department_anomaly_count": department_anomaly_count,
                "naming_risk_count": naming_risk_count,
                "duplicate_email_count": duplicate_email_count,
                "duplicate_employee_id_count": duplicate_employee_id_count,
                "error_issue_count": error_issue_count,
                "warning_issue_count": warning_issue_count,
            },
            "connector_breakdown": [
                {
                    "connector_id": "default",
                    "name": "Default Connector",
                    "user_count": total_users,
                }
            ],
            "issues": [
                {
                    "key": "managed_username_collision",
                    "label": "Predicted managed username collisions",
                    "severity": "error",
                    "count": 1,
                    "description": "Two users would land on the same username.",
                    "action": "Adjust username policy.",
                    "samples": [{"title": "alice", "detail": "Would be generated for alice and bob."}],
                }
            ],
            "repair_items": repair_items,
            "high_risk_items": repair_items,
        }

    def test_persisted_snapshots_build_trend_context(self):
        with TemporaryDirectory() as temp_dir:
            db_manager = self._build_db(temp_dir)
            persist_data_quality_snapshot(
                db_manager,
                "default",
                created_by="tester",
                snapshot=self._snapshot_payload(
                    generated_at="2026-04-20T10:00:00+00:00",
                    total_users=100,
                    missing_email=8,
                    missing_employee_id=5,
                    department_anomaly_count=4,
                    naming_risk_count=3,
                    duplicate_email_count=1,
                    duplicate_employee_id_count=2,
                    error_issue_count=2,
                    warning_issue_count=4,
                    repair_items=[
                        {
                            "key": "managed_username_collision",
                            "label": "Predicted managed username collisions",
                            "severity": "error",
                            "title": "asmith [Default Connector]",
                            "source_user_id": "",
                            "source_user_ids": ["alice", "bob"],
                            "display_name": "",
                            "connector_id": "default",
                            "connector_name": "Default Connector",
                            "detail": "Would be generated for Alice and Bob.",
                            "action": "Adjust username policy.",
                        }
                    ],
                ),
            )
            persist_data_quality_snapshot(
                db_manager,
                "default",
                created_by="tester",
                snapshot=self._snapshot_payload(
                    generated_at="2026-04-21T10:00:00+00:00",
                    total_users=102,
                    missing_email=6,
                    missing_employee_id=4,
                    department_anomaly_count=3,
                    naming_risk_count=2,
                    duplicate_email_count=1,
                    duplicate_employee_id_count=1,
                    error_issue_count=1,
                    warning_issue_count=3,
                    repair_items=[
                        {
                            "key": "managed_username_collision",
                            "label": "Predicted managed username collisions",
                            "severity": "error",
                            "title": "asmith [Default Connector]",
                            "source_user_id": "",
                            "source_user_ids": ["alice", "bob"],
                            "display_name": "",
                            "connector_id": "default",
                            "connector_name": "Default Connector",
                            "detail": "Would be generated for Alice and Bob.",
                            "action": "Adjust username policy.",
                        },
                        {
                            "key": "missing_email",
                            "label": "Users missing work email",
                            "severity": "warning",
                            "title": "Bob [bob]",
                            "source_user_id": "bob",
                            "source_user_ids": [],
                            "display_name": "Bob",
                            "connector_id": "",
                            "connector_name": "",
                            "detail": "No work email was found on the source directory record.",
                            "action": "Backfill email.",
                        },
                    ],
                ),
            )

            context = build_data_quality_center_context(db_manager, "default")

        self.assertTrue(context["has_snapshots"])
        self.assertEqual(context["snapshot_count"], 2)
        self.assertEqual(context["selected_snapshot"].created_at, "2026-04-21T10:00:00+00:00")
        self.assertEqual(context["selected_summary"]["total_users"], 102)
        self.assertEqual(context["selected_summary"]["department_anomaly_count"], 3)
        self.assertEqual(len(context["snapshots"]), 2)
        self.assertEqual(context["snapshots"][0]["title"], "Snapshot #2")
        self.assertEqual(context["selected_delta"]["missing_email"], -2)
        self.assertEqual(len(context["high_risk_items"]), 1)
        self.assertEqual(context["high_risk_items"][0]["key"], "managed_username_collision")

    def test_export_rows_come_from_snapshot_repair_items(self):
        with TemporaryDirectory() as temp_dir:
            db_manager = self._build_db(temp_dir)
            persist_data_quality_snapshot(
                db_manager,
                "default",
                created_by="tester",
                snapshot=self._snapshot_payload(
                    generated_at="2026-04-21T10:00:00+00:00",
                    total_users=12,
                    missing_email=1,
                    missing_employee_id=0,
                    department_anomaly_count=1,
                    naming_risk_count=1,
                    duplicate_email_count=0,
                    duplicate_employee_id_count=0,
                    error_issue_count=1,
                    warning_issue_count=1,
                    repair_items=[
                        {
                            "key": "missing_email",
                            "label": "Users missing work email",
                            "severity": "warning",
                            "title": "Bob [bob]",
                            "source_user_id": "bob",
                            "source_user_ids": [],
                            "display_name": "Bob",
                            "connector_id": "",
                            "connector_name": "",
                            "detail": "No work email was found on the source directory record.",
                            "action": "Backfill email.",
                        }
                    ],
                ),
            )
            snapshot = DataQualitySnapshotRepository(db_manager).get_latest_snapshot_record(org_id="default")
            rows = build_data_quality_export_rows(snapshot)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "missing_email")
        self.assertEqual(rows[0][4], "bob")
        self.assertIn("No work email", rows[0][9])


if __name__ == "__main__":
    unittest.main()
