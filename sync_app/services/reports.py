import csv
import logging
import os
import time
from datetime import datetime
from typing import Iterable

from sync_app.core.common import format_time_duration
from sync_app.core.models import AppConfig, SyncRunStats


def generate_sync_operation_log(sync_stats: dict, start_time: float, config: AppConfig) -> str:
    try:
        stats = SyncRunStats.from_mapping(sync_stats)
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        report_filename = os.path.join(log_dir, f"sync_operations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

        with open(report_filename, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["sync_operation_log"])
            writer.writerow(["generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            writer.writerow(["duration", format_time_duration(time.time() - start_time)])
            writer.writerow(["domain", config.domain or "N/A"])
            writer.writerow(["ldap_server", config.ldap.server or "N/A"])
            writer.writerow([])

            writer.writerow(["operations"])
            writer.writerow(["operation_type", "count"])
            for key, value in stats.operations.items():
                writer.writerow([key, value])
            writer.writerow([])

            skipped_summary = stats.skipped_operations
            if skipped_summary.total:
                writer.writerow(["skipped_operations"])
                writer.writerow(["skip_type", "count"])
                for key, value in skipped_summary.by_action.items():
                    writer.writerow([key, value])
                writer.writerow([])

            writer.writerow(["errors"])
            writer.writerow(["error_type", "count"])
            for error_type, errors in stats.errors.items():
                if errors:
                    writer.writerow([error_type, len(errors)])
            writer.writerow([])

            for error_type, errors in stats.errors.items():
                if not errors:
                    continue
                writer.writerow([f"=== {error_type} details ==="])
                rows, headers = _normalize_error_rows(error_type, errors)
                writer.writerow(headers)
                for row in rows:
                    writer.writerow([row.get(header, "") for header in headers])
                writer.writerow([])

        logging.getLogger(__name__).info(f"sync operation log saved to: {report_filename}")
        return report_filename
    except Exception as exc:
        logging.getLogger(__name__).error(f"failed to generate sync operation log: {exc}")
        return ""


def generate_skip_detail_report(sync_stats: dict) -> str:
    try:
        stats = SyncRunStats.from_mapping(sync_stats)
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        report_filename = os.path.join(log_dir, f"sync_skip_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

        skip_details = stats.skipped_operations.details or []
        skip_samples = stats.skipped_operations.samples or []
        rows = skip_details or skip_samples

        with open(report_filename, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["job_id", stats.job_id])
            writer.writerow(["execution_mode", stats.execution_mode])
            writer.writerow(["generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            writer.writerow([])
            writer.writerow(["stage", "action_type", "group_sam", "group_dn", "reason", "matched_rules"])
            for row in rows:
                writer.writerow(
                    [
                        row.get("stage", ""),
                        row.get("action_type", ""),
                        row.get("group_sam", ""),
                        row.get("group_dn", ""),
                        row.get("reason", ""),
                        " | ".join(row.get("matched_rules") or []),
                    ]
                )

        logging.getLogger(__name__).info(f"skip detail report saved to: {report_filename}")
        return report_filename
    except Exception as exc:
        logging.getLogger(__name__).error(f"failed to generate skip detail report: {exc}")
        return ""


def generate_sync_validation_report(sync_stats: dict, source_users: set, ad_users: Iterable[str]) -> str:
    try:
        stats = SyncRunStats.from_mapping(sync_stats)
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        report_filename = os.path.join(log_dir, f"sync_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")

        ad_users_set = set(ad_users)
        should_be_in_ad = set(source_users)
        actually_in_ad = should_be_in_ad & ad_users_set
        missing_users = should_be_in_ad - actually_in_ad
        extra_users = ad_users_set - should_be_in_ad
        skipped_summary = stats.skipped_operations

        with open(report_filename, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("Source-AD Sync Validation Report\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            status = "SUCCESS" if stats.error_count == 0 else f"COMPLETED WITH {stats.error_count} ERRORS"
            f.write("[Status]\n")
            f.write(f"Result: {status}\n")
            f.write(f"Source users: {len(should_be_in_ad)}\n")
            f.write(f"AD enabled users: {len(ad_users_set)}\n\n")

            f.write("[Operations]\n")
            ops = stats.operations
            for key in [
                "departments_created",
                "departments_existed",
                "users_created",
                "users_updated",
                "users_disabled",
                "groups_assigned",
                "groups_nested",
                "group_relations_removed",
            ]:
                if key in ops:
                    f.write(f"{key}: {ops[key]}\n")
            if skipped_summary.total:
                f.write(f"skipped_operations: {skipped_summary.total}\n")
                for action_type, count in skipped_summary.by_action.items():
                    f.write(f"  - {action_type}: {count}\n")
            f.write("\n")

            f.write("[Consistency]\n")
            f.write(f"expected_in_ad: {len(should_be_in_ad)}\n")
            f.write(f"actually_in_ad: {len(actually_in_ad)}\n")
            f.write(f"missing_users: {len(missing_users)}\n")
            f.write(f"extra_users: {len(extra_users)}\n\n")

            if missing_users:
                f.write("[Missing Users]\n")
                for user in sorted(missing_users)[:50]:
                    f.write(f"- {user}\n")
                if len(missing_users) > 50:
                    f.write(f"... and {len(missing_users) - 50} more\n")
                f.write("\n")

            if stats.error_count > 0:
                f.write("[Error Summary]\n")
                for error_type, errors in stats.errors.items():
                    if errors:
                        f.write(f"{error_type}: {len(errors)}\n")
                f.write("\n")

            if skipped_summary.samples:
                f.write("[Skipped Samples]\n")
                for sample in skipped_summary.samples[:10]:
                    f.write(
                        f"- {sample.get('action_type', '')} | {sample.get('group_sam', '')} | {sample.get('reason', '')}\n"
                    )
                    matched_rules = ", ".join(sample.get("matched_rules") or [])
                    if matched_rules:
                        f.write(f"  rules: {matched_rules}\n")
                f.write("\n")
                if stats.skip_detail_report:
                    f.write(f"Detailed skip report: {stats.skip_detail_report}\n\n")

        logging.getLogger(__name__).info(f"sync validation report saved to: {report_filename}")
        return report_filename
    except Exception as exc:
        logging.getLogger(__name__).error(f"failed to generate validation report: {exc}")
        return ""


def _normalize_error_rows(error_type: str, errors: list[dict]) -> tuple[list[dict], list[str]]:
    if error_type == "department_errors":
        headers = ["department", "path", "error"]
    elif error_type in {"user_create_errors", "user_update_errors"}:
        headers = ["username", "display_name", "email", "department", "error"]
    elif error_type == "group_add_errors":
        headers = ["username", "groups", "group", "error"]
    elif error_type in {"group_hierarchy_errors", "group_relation_cleanup_errors"}:
        headers = ["child_group_sam", "parent_group_sam", "error"]
    elif error_type == "user_disable_errors":
        headers = ["username", "error"]
    else:
        headers = sorted({key for row in errors for key in row.keys()}) or ["error"]
    return errors, headers


# Backward-compatible names for old call sites.
_generate_sync_operation_log = generate_sync_operation_log
_generate_skip_detail_report = generate_skip_detail_report
_generate_sync_validation_report = generate_sync_validation_report
