from __future__ import annotations

import logging
import os
import time
import traceback
from datetime import datetime

from PyQt5.QtCore import QThread, QTimer, pyqtSignal

from sync_app.services.entry import main as sync_main


class SyncThread(QThread):
    """Execute sync work outside the desktop UI thread."""

    update_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(bool, str, dict)
    progress_signal = pyqtSignal(int)
    stats_signal = pyqtSignal(dict)

    def __init__(
        self,
        *,
        app_path: str,
        logs_dir: str,
        execution_mode: str = "apply",
        trigger_type: str = "manual",
    ):
        super().__init__()
        self.app_path = app_path
        self.logs_dir = logs_dir
        self.logger = logging.getLogger(__name__)
        self.is_cancelled = False
        self.execution_mode = execution_mode
        self.trigger_type = trigger_type

        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)

        class LogHandler(logging.Handler):
            def __init__(self, signal_fn):
                super().__init__()
                self.signal_fn = signal_fn

            def emit(self, record):
                self.signal_fn.emit(self.format(record))

        self.log_handler = LogHandler(self.update_signal)
        self.log_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        self.logger.addHandler(self.log_handler)
        logging.getLogger().addHandler(self.log_handler)

        for handler in list(logging.getLogger().handlers):
            if isinstance(handler, LogHandler) and handler != self.log_handler:
                logging.getLogger().removeHandler(handler)

    def run(self):
        try:
            run_label = "预演同步任务" if self.execution_mode == "dry_run" else "同步任务"
            self.update_signal.emit(f"开始执行{run_label}...")
            self.progress_signal.emit(5)

            progress_tracking = {
                "started": time.time(),
                "status": "running",
                "last_update": time.time(),
                "departments_done": False,
                "users_processed": 0,
                "total_users": 0,
            }

            def update_progress():
                if self.is_cancelled:
                    return

                elapsed = time.time() - progress_tracking["started"]
                if not progress_tracking["departments_done"]:
                    self.progress_signal.emit(min(30, int(10 + elapsed * 2)))
                elif progress_tracking["total_users"] > 0:
                    users_progress = progress_tracking["users_processed"] / progress_tracking["total_users"]
                    self.progress_signal.emit(min(90, int(30 + users_progress * 60)))

                if time.time() - progress_tracking["last_update"] > 30:
                    self.update_signal.emit(f"同步仍在进行中...(已运行 {int(elapsed)} 秒)")
                    progress_tracking["last_update"] = time.time()

                self.stats_signal.emit(progress_tracking)
                QTimer.singleShot(2000, update_progress)

            QTimer.singleShot(1000, update_progress)

            def sync_stats_callback(stage, data):
                if stage == "department_sync_done":
                    progress_tracking["departments_done"] = True
                    self.progress_signal.emit(30)
                    self.update_signal.emit("部门结构同步完成")
                elif stage == "total_users":
                    progress_tracking["total_users"] = data
                    self.update_signal.emit(f"开始同步用户，共计 {data} 个")
                elif stage == "user_processed":
                    progress_tracking["users_processed"] = data
                    total = progress_tracking["total_users"]
                    if total > 0:
                        user_progress = data / total
                        self.progress_signal.emit(int(30 + user_progress * 60))
                        should_log = (
                            (total < 100 and (data % 5 == 0 or data == 1))
                            or (100 <= total < 500 and (data % 10 == 0 or data == 1))
                            or (total >= 500 and (data % 20 == 0 or data == 1))
                        )
                        if should_log:
                            self.update_signal.emit(f"已处理用户 {data}/{total} ({int(user_progress * 100)}%)")
                elif stage == "disable_stage_start":
                    self.progress_signal.emit(90)
                    self.update_signal.emit("开始处理需要禁用的账号...")
                elif stage == "users_to_disable" and data > 0:
                    self.update_signal.emit(f"发现 {data} 个需要禁用的账号")
                elif stage == "user_disable_progress":
                    self.progress_signal.emit(int(90 + data * 10))

                progress_tracking["last_update"] = time.time()

            os.chdir(self.app_path)
            config_path = os.path.join(self.app_path, "config.ini")
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"找不到配置文件: {config_path}")

            self.update_signal.emit(f"开始使用配置文件: {config_path}")

            try:
                sync_stats = sync_main(
                    stats_callback=sync_stats_callback,
                    cancel_flag=self,
                    execution_mode=self.execution_mode,
                    trigger_type=self.trigger_type,
                    requested_by="desktop_ui",
                )
                if self.is_cancelled:
                    self.finished_signal.emit(False, "同步已取消", sync_stats)
                else:
                    self.progress_signal.emit(100)
                    self.finished_signal.emit(True, "同步任务完成", sync_stats)
            except Exception as exc:
                error_info = f"同步任务出错: {exc}\n{traceback.format_exc()}"
                self.logger.error(error_info)
                if not self.is_cancelled:
                    self.progress_signal.emit(0)
                self.finished_signal.emit(False, f"同步任务失败: {exc}", {})

        except Exception as exc:
            error_info = f"同步线程异常: {exc}\n{traceback.format_exc()}"
            self.logger.error(error_info)
            self.finished_signal.emit(False, f"同步线程异常: {exc}", {})

    def cancel(self):
        self.is_cancelled = True
        self.update_signal.emit("正在尝试取消同步任务...")


class ScheduleThread(QThread):
    """Run background schedule polling outside the desktop UI thread."""

    def __init__(self):
        super().__init__()
        self.running = True

    def run(self):
        import schedule

        while self.running:
            schedule.run_pending()
            time.sleep(1)

    def stop(self):
        self.running = False
