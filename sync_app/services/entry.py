from typing import Optional


def main(
    stats_callback=None,
    cancel_flag=None,
    execution_mode: str = "apply",
    trigger_type: str = "manual",
    db_path: Optional[str] = None,
    config_path: str = "config.ini",
    org_id: str = "default",
    requested_by: str = "",
):
    from sync_app.services.sync_dispatch import run_sync_request

    return run_sync_request(
        stats_callback=stats_callback,
        cancel_flag=cancel_flag,
        execution_mode=execution_mode,
        trigger_type=trigger_type,
        db_path=db_path,
        config_path=config_path,
        org_id=org_id,
        requested_by=requested_by,
    )
