import logging
import os
from datetime import datetime

log_filename = ""


def setup_logging():
    """Configure runtime logging."""
    global log_filename

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, f"ad_org_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    detailed_log = logging.getLogger("detailed")
    detailed_log.setLevel(logging.DEBUG)
    detailed_log.handlers.clear()
    detailed_handler = logging.FileHandler(
        os.path.join(log_dir, f"ad_org_sync_detailed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        encoding="utf-8",
    )
    detailed_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s")
    )
    detailed_log.addHandler(detailed_handler)

    return logging.getLogger(__name__)
