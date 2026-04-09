import argparse

from sync_app.clients.wechat_bot import WeChatBot, mask_webhook_url
from sync_app.clients.wecom import WeComAPI
from sync_app.core import logging_utils as _logging
from sync_app.core.common import APP_VERSION, format_time_duration, generate_job_id, hash_department_state, hash_user_state
from sync_app.core.config import load_sync_config, test_ldap_connection, test_wecom_connection, validate_config
from sync_app.services.ad_sync import ADSync, ADSyncLDAPS, build_group_cn, build_group_display_name, build_group_sam
from sync_app.services.entry import main
from sync_app.services.reports import (
    _generate_skip_detail_report,
    _generate_sync_operation_log,
    _generate_sync_validation_report,
)
from sync_app.services.state import SyncStateManager


def setup_logging():
    return _logging.setup_logging()


def __getattr__(name):
    if name == "log_filename":
        return _logging.log_filename
    raise AttributeError(name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AD Org Sync runtime entry (LDAPS compatibility wrapper)")
    parser.add_argument("--execution-mode", choices=["apply", "dry_run"], default="apply")
    parser.add_argument("--trigger-type", default="cli")
    parser.add_argument("--db-path", default=None)
    args = parser.parse_args()
    main(execution_mode=args.execution_mode, trigger_type=args.trigger_type, db_path=args.db_path)
