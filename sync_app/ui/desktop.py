import sys
import os
import time
import logging
import json
import configparser
import schedule
from datetime import datetime
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                            QPushButton, QLabel, QSystemTrayIcon, QMenu, QAction, 
                            QLineEdit, QFormLayout, QSpinBox, QTimeEdit, QTabWidget,
                            QTextEdit, QProgressBar, QFileDialog, QMessageBox, QGroupBox,
                            QHBoxLayout, QComboBox, QTableWidget, QTableWidgetItem,
                            QHeaderView, QAbstractItemView, QScrollArea,
                            QSizePolicy)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTime, QSettings, QTimer
from PyQt5.QtGui import QIcon, QPalette, QColor, QFont
import qtawesome as qta
from sync_app.core.models import SyncRunStats
from sync_app.storage.local_db import DatabaseManager, GroupExclusionRuleRepository, SettingsRepository

APP_TITLE = "企业微信-AD同步工具"

# 确定应用程序路径
def get_application_path():
    """获取应用程序的运行路径，处理PyInstaller打包和直接运行的情况"""
    if getattr(sys, 'frozen', False):
        # 如果是打包后的应用程序
        application_path = os.path.dirname(sys.executable)
    else:
        # 如果是直接运行源码，回退到项目根目录而不是 sync_app/ui
        module_dir = os.path.dirname(os.path.abspath(__file__))
        application_path = os.path.abspath(os.path.join(module_dir, "..", ".."))
    return application_path

# 设置工作目录为应用程序所在目录
APP_PATH = get_application_path()
os.chdir(APP_PATH)

# 确保logs目录存在
LOGS_DIR = os.path.join(APP_PATH, "logs")
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

# 导入主程序模块
try:
    from sync_app.clients.wechat_bot import WeChatBot
    from sync_app.clients.wecom import WeComAPI
    from sync_app.core.logging_utils import setup_logging
    from sync_app.services.ad_sync import ADSyncLDAPS
    from sync_app.services.entry import main as sync_main
    import wecom_sync_ad_ldaps
except ImportError as e:
    print(f"导入模块错误: {str(e)}")
    QMessageBox.critical(None, "导入错误", f"无法导入必要模块: {str(e)}\n请确保所有依赖已正确安装。")
    sys.exit(1)

class BlurWindow(QWidget):
    """实现毛玻璃效果的基础窗口类"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setWindowFlags(Qt.FramelessWindowHint)
        
        # 设置半透明背景
        self.setStyleSheet("""
            BlurWindow {
                background-color: rgba(255, 255, 255, 180);
                border-radius: 10px;
                border: 1px solid rgba(255, 255, 255, 200);
            }
        """)
        
        # 主布局
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(15, 15, 15, 15)
        
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.dragPos = event.globalPos() - self.frameGeometry().topLeft()
            event.accept()
            
    def mouseMoveEvent(self, event):
        if event.buttons() == Qt.LeftButton:
            self.move(event.globalPos() - self.dragPos)
            event.accept()

class SyncThread(QThread):
    """执行同步任务的线程"""
    update_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(bool, str, dict)  # 添加统计数据参数
    progress_signal = pyqtSignal(int)
    stats_signal = pyqtSignal(dict)  # 添加实时统计信号
    
    def __init__(self, execution_mode='apply', trigger_type='manual'):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.is_cancelled = False
        self.execution_mode = execution_mode
        self.trigger_type = trigger_type
        
        # 确保logs目录存在
        if not os.path.exists(LOGS_DIR):
            os.makedirs(LOGS_DIR)
            
        # 捕获日志输出
        class LogHandler(logging.Handler):
            def __init__(self, signal_fn):
                super().__init__()
                self.signal_fn = signal_fn
                
            def emit(self, record):
                msg = self.format(record)
                self.signal_fn.emit(msg)
                
        self.log_handler = LogHandler(self.update_signal)
        self.log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(self.log_handler)
        logging.getLogger().addHandler(self.log_handler)

        # 清理旧日志处理器，避免重复日志
        for handler in list(logging.getLogger().handlers):
            if isinstance(handler, LogHandler) and handler != self.log_handler:
                logging.getLogger().removeHandler(handler)
        
    def run(self):
        import traceback

        try:
            run_label = "预演同步任务" if self.execution_mode == 'dry_run' else "同步任务"
            self.update_signal.emit(f"开始执行{run_label}...")
            self.progress_signal.emit(5)

            progress_tracking = {
                'started': time.time(),
                'status': 'running',
                'last_update': time.time(),
                'departments_done': False,
                'users_processed': 0,
                'total_users': 0,
            }

            def update_progress():
                if self.is_cancelled:
                    return

                elapsed = time.time() - progress_tracking['started']
                if not progress_tracking['departments_done']:
                    self.progress_signal.emit(min(30, int(10 + elapsed * 2)))
                elif progress_tracking['total_users'] > 0:
                    users_progress = progress_tracking['users_processed'] / progress_tracking['total_users']
                    self.progress_signal.emit(min(90, int(30 + users_progress * 60)))

                if time.time() - progress_tracking['last_update'] > 30:
                    self.update_signal.emit(
                        f"同步仍在进行中...(已运行 {int(elapsed)} 秒)"
                    )
                    progress_tracking['last_update'] = time.time()

                self.stats_signal.emit(progress_tracking)
                QTimer.singleShot(2000, update_progress)

            QTimer.singleShot(1000, update_progress)

            def sync_stats_callback(stage, data):
                if stage == 'department_sync_done':
                    progress_tracking['departments_done'] = True
                    self.progress_signal.emit(30)
                    self.update_signal.emit("部门结构同步完成")
                elif stage == 'total_users':
                    progress_tracking['total_users'] = data
                    self.update_signal.emit(f"开始同步用户，共计 {data} 个")
                elif stage == 'user_processed':
                    progress_tracking['users_processed'] = data
                    total = progress_tracking['total_users']
                    if total > 0:
                        user_progress = data / total
                        self.progress_signal.emit(int(30 + user_progress * 60))
                        should_log = (
                            (total < 100 and (data % 5 == 0 or data == 1))
                            or (100 <= total < 500 and (data % 10 == 0 or data == 1))
                            or (total >= 500 and (data % 20 == 0 or data == 1))
                        )
                        if should_log:
                            self.update_signal.emit(
                                f"已处理用户 {data}/{total} ({int(user_progress * 100)}%)"
                            )
                elif stage == 'disable_stage_start':
                    self.progress_signal.emit(90)
                    self.update_signal.emit("开始处理需要禁用的账号...")
                elif stage == 'users_to_disable' and data > 0:
                    self.update_signal.emit(f"发现 {data} 个需要禁用的账号")
                elif stage == 'user_disable_progress':
                    self.progress_signal.emit(int(90 + data * 10))

                progress_tracking['last_update'] = time.time()

            os.chdir(APP_PATH)
            config_path = os.path.join(APP_PATH, "config.ini")
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"找不到配置文件: {config_path}")

            self.update_signal.emit(f"开始使用配置文件: {config_path}")

            try:
                sync_stats = sync_main(
                    stats_callback=sync_stats_callback,
                    cancel_flag=self,
                    execution_mode=self.execution_mode,
                    trigger_type=self.trigger_type,
                )
                if self.is_cancelled:
                    self.finished_signal.emit(False, "同步已取消", sync_stats)
                else:
                    self.progress_signal.emit(100)
                    self.finished_signal.emit(True, "同步任务完成", sync_stats)
            except Exception as e:
                error_info = f"同步任务出错: {str(e)}\n{traceback.format_exc()}"
                self.logger.error(error_info)
                if not self.is_cancelled:
                    self.progress_signal.emit(0)
                self.finished_signal.emit(False, f"同步任务失败: {str(e)}", {})

        except Exception as e:
            error_info = f"同步线程异常: {str(e)}\n{traceback.format_exc()}"
            self.logger.error(error_info)
            self.finished_signal.emit(False, f"同步线程异常: {str(e)}", {})

    def cancel(self):
        """请求取消正在执行的同步任务。"""
        self.is_cancelled = True
        self.update_signal.emit("正在尝试取消同步任务...")


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_TITLE)
        self.setWindowIcon(QIcon("sync.ico"))
        self.resize(1120, 760)
        self.setMinimumSize(900, 640)
        
        # 设置毛玻璃窗口为中央窗口
        self.central_widget = BlurWindow()
        self.setCentralWidget(self.central_widget)
        self.central_widget.layout.setSpacing(12)
        
        # 创建系统托盘图标
        self.create_tray_icon()
        
        # 创建UI组件
        self.setup_ui()
        self.init_local_storage()
        
        # 加载配置
        self.load_config()
        self.load_local_settings()
        
        # 初始化定时器
        self.setup_scheduler()
        
        # 应用深色主题
        self.apply_dark_theme()
        
    def apply_dark_theme(self):
        """应用深色主题样式"""
        dark_palette = QPalette()
        
        # 设置基本颜色
        dark_palette.setColor(QPalette.Window, QColor(53, 53, 53))
        dark_palette.setColor(QPalette.WindowText, Qt.white)
        dark_palette.setColor(QPalette.Base, QColor(35, 35, 35))
        dark_palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
        dark_palette.setColor(QPalette.ToolTipBase, QColor(25, 25, 25))
        dark_palette.setColor(QPalette.ToolTipText, Qt.white)
        dark_palette.setColor(QPalette.Text, Qt.white)
        dark_palette.setColor(QPalette.Button, QColor(53, 53, 53))
        dark_palette.setColor(QPalette.ButtonText, Qt.white)
        dark_palette.setColor(QPalette.BrightText, Qt.red)
        dark_palette.setColor(QPalette.Link, QColor(42, 130, 218))
        dark_palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
        dark_palette.setColor(QPalette.HighlightedText, Qt.black)
        
        # 应用调色板
        QApplication.instance().setPalette(dark_palette)
        
        # 设置全局样式表
        QApplication.instance().setStyleSheet("""
            QWidget {
                font-family: "Microsoft YaHei", "Segoe UI", Arial;
                font-size: 12px;
            }
            QTabWidget::pane {
                border: 1px solid rgba(255, 255, 255, 100);
                border-radius: 5px;
                background: rgba(35, 35, 35, 200);
            }
            QTabBar::tab {
                background: rgba(45, 45, 45, 200);
                color: white;
                padding: 8px 12px;
                margin-right: 2px;
                border-top-left-radius: 5px;
                border-top-right-radius: 5px;
            }
            QTabBar::tab:selected {
                background: rgba(65, 65, 65, 200);
            }
            QPushButton {
                background-color: rgba(65, 65, 65, 200);
                color: white;
                border: 1px solid rgba(100, 100, 100, 200);
                border-radius: 5px;
                padding: 8px;
            }
            QPushButton:hover {
                background-color: rgba(85, 85, 85, 200);
            }
            QPushButton:pressed {
                background-color: rgba(100, 100, 100, 200);
            }
            QLineEdit, QTimeEdit, QSpinBox, QComboBox, QTableWidget {
                background: rgba(45, 45, 45, 200);
                color: white;
                border: 1px solid rgba(100, 100, 100, 200);
                border-radius: 5px;
                padding: 5px;
            }
            QComboBox QAbstractItemView, QTableWidget {
                selection-background-color: rgba(42, 130, 218, 180);
                selection-color: white;
                gridline-color: rgba(100, 100, 100, 120);
            }
            QHeaderView::section {
                background: rgba(55, 55, 55, 220);
                color: white;
                border: none;
                border-bottom: 1px solid rgba(100, 100, 100, 120);
                padding: 6px;
            }
            QProgressBar {
                border: 1px solid rgba(100, 100, 100, 200);
                border-radius: 5px;
                text-align: center;
                background: rgba(35, 35, 35, 200);
            }
            QProgressBar::chunk {
                background-color: rgba(42, 130, 218, 200);
                border-radius: 5px;
            }
            QTextEdit {
                background: rgba(35, 35, 35, 200);
                color: white;
                border: 1px solid rgba(100, 100, 100, 200);
                border-radius: 5px;
            }
            QScrollArea {
                background: transparent;
                border: none;
            }
            QLabel {
                color: white;
            }
            QGroupBox {
                border: 1px solid rgba(100, 100, 100, 200);
                border-radius: 5px;
                margin-top: 10px;
                padding-top: 10px;
                color: white;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                subcontrol-position: top center;
                padding: 0 5px;
            }
        """)
        
    def save_local_settings(self):
        if self.local_db_error:
            raise RuntimeError(self.local_db_error)
        if not self.local_settings_repo or not self.local_rule_repo:
            return

        separator = self.group_separator_combo.currentData() or '-'
        recursive_enabled = bool(self.group_recursive_combo.currentData())
        cleanup_enabled = bool(self.group_cleanup_combo.currentData())
        execution_mode = self.execution_mode_combo.currentData() if hasattr(self, 'execution_mode_combo') else 'apply'

        self.local_settings_repo.set_value('group_display_separator', separator, 'string')
        self.local_settings_repo.set_value('group_recursive_enabled', str(recursive_enabled).lower(), 'bool')
        self.local_settings_repo.set_value('group_recursive_enabled_user_override', 'true', 'bool')
        self.local_settings_repo.set_value('managed_relation_cleanup_enabled', str(cleanup_enabled).lower(), 'bool')
        self.local_settings_repo.set_value('schedule_execution_mode', execution_mode, 'string')

        self.local_rule_repo.replace_soft_excluded_rules(self.collect_soft_excluded_rule_rows())
        self.refresh_local_strategy_summary()

    def add_soft_excluded_rule_row(self, group_name: str = '', enabled: bool = True, source: str = 'user_ui'):
        table = self.soft_excluded_groups_table
        row = table.rowCount()
        table.insertRow(row)

        enabled_item = QTableWidgetItem()
        enabled_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
        enabled_item.setCheckState(Qt.Checked if enabled else Qt.Unchecked)
        table.setItem(row, 0, enabled_item)

        group_item = QTableWidgetItem(group_name)
        table.setItem(row, 1, group_item)

        source_item = QTableWidgetItem(source)
        source_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
        table.setItem(row, 2, source_item)

    def load_soft_excluded_rule_table(self, rules):
        table = self.soft_excluded_groups_table
        table.setRowCount(0)
        for rule in rules:
            self.add_soft_excluded_rule_row(
                group_name=rule.get('match_value', ''),
                enabled=bool(rule.get('is_enabled')),
                source=rule.get('source', 'user_ui'),
            )

    def collect_soft_excluded_rule_rows(self):
        rows = []
        table = self.soft_excluded_groups_table
        for row in range(table.rowCount()):
            enabled_item = table.item(row, 0)
            group_item = table.item(row, 1)
            source_item = table.item(row, 2)
            group_name = group_item.text().strip() if group_item else ''
            if not group_name:
                continue
            rows.append(
                {
                    'match_value': group_name,
                    'display_name': group_name,
                    'is_enabled': bool(enabled_item and enabled_item.checkState() == Qt.Checked),
                    'source': source_item.text().strip() if source_item and source_item.text().strip() else 'user_ui',
                }
            )
        return rows

    def add_soft_excluded_rule(self):
        self.add_soft_excluded_rule_row()
        last_row = self.soft_excluded_groups_table.rowCount() - 1
        if last_row >= 0:
            self.soft_excluded_groups_table.setCurrentCell(last_row, 1)
            self.soft_excluded_groups_table.editItem(self.soft_excluded_groups_table.item(last_row, 1))

    def remove_selected_soft_excluded_rules(self):
        table = self.soft_excluded_groups_table
        selected_rows = sorted({index.row() for index in table.selectionModel().selectedRows()}, reverse=True)
        if not selected_rows and table.currentRow() >= 0:
            selected_rows = [table.currentRow()]
        for row in selected_rows:
            table.removeRow(row)

    def load_protected_rule_table(self, rules):
        if not hasattr(self, 'protected_groups_table'):
            return
        table = self.protected_groups_table
        table.setRowCount(0)
        for rule in rules:
            row = table.rowCount()
            table.insertRow(row)

            group_item = QTableWidgetItem(rule.get('match_value', ''))
            group_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            table.setItem(row, 0, group_item)

            match_type_item = QTableWidgetItem(rule.get('match_type', ''))
            match_type_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            table.setItem(row, 1, match_type_item)

            source_item = QTableWidgetItem(rule.get('source', ''))
            source_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            table.setItem(row, 2, source_item)

    def tray_icon_activated(self, reason):
        """托盘图标被激活时的响应"""
        if reason == QSystemTrayIcon.DoubleClick:
            self.show()
            self.activateWindow()
            
    def close_application(self):
        """关闭应用程序"""
        if hasattr(self, 'schedule_thread'):
            self.schedule_thread.stop()
        QApplication.quit()

    def start_sync(self, trigger_type='manual'):
        """启动同步任务。"""
        if hasattr(self, 'sync_thread') and self.sync_thread.isRunning():
            self.log_text.append("同步任务已在运行中，请等待完成。")
            return

        execution_mode = self.execution_mode_combo.currentData() if hasattr(self, 'execution_mode_combo') else 'apply'
        self.progress_bar.setValue(0)
        self.sync_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.status_label.setText(
            "正在预演..." if execution_mode == 'dry_run' else "正在同步..."
        )

        for label in self.stats_labels.values():
            label.setText("--")

        self.original_logs = []
        self.sync_thread = SyncThread(execution_mode=execution_mode, trigger_type=trigger_type)
        self.sync_thread.update_signal.connect(self.update_log)
        self.sync_thread.finished_signal.connect(self.sync_finished)
        self.sync_thread.progress_signal.connect(self.update_progress)
        self.sync_thread.stats_signal.connect(self.update_stats)
        self.sync_thread.start()

        self.sync_start_time = time.time()
        mode_label = "预演同步任务" if execution_mode == 'dry_run' else "同步任务"
        self.update_log(f"开始{mode_label} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self.update_elapsed_time)
        self.update_timer.start(1000)

    def stop_sync(self):
        """尝试停止同步任务。"""
        if hasattr(self, 'sync_thread') and self.sync_thread.isRunning():
            reply = QMessageBox.question(
                self,
                '确认停止',
                "当前同步任务将被中断，可能导致部分数据尚未完成处理。确定要停止吗？",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                self.sync_thread.cancel()
                self.update_log(
                    "用户已请求停止同步任务，正在等待当前操作完成..."
                )
                self.status_label.setText("正在停止...")

    def update_elapsed_time(self):
        """更新已运行时长。"""
        if hasattr(self, 'sync_start_time'):
            elapsed = time.time() - self.sync_start_time
            self.stats_labels["耗时"].setText(self.format_time(elapsed))

    def format_time(self, seconds):
        """格式化时长显示。"""
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        if hours > 0:
            return f"{hours}小时 {minutes}分钟 {seconds}秒"
        if minutes > 0:
            return f"{minutes}分钟 {seconds}秒"
        return f"{seconds}秒"

    def update_log(self, message):
        """更新日志显示。"""
        if not hasattr(self, 'original_logs'):
            self.original_logs = []
        self.original_logs.append(message)

        if " ERROR " in message:
            self.log_text.append(f'<span style="color:#FF5252;">{message}</span>')
        elif " WARNING " in message:
            self.log_text.append(f'<span style="color:#FFD740;">{message}</span>')
        else:
            self.log_text.append(message)

        scrollbar = self.log_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def update_stats(self, stats_data):
        """更新实时统计信息。"""
        stats_model = SyncRunStats.from_mapping(stats_data)
        if stats_model.total_users > 0:
            self.stats_labels["总用户数"].setText(str(stats_model.total_users))

        processed_users = stats_model.processed_users
        if processed_users <= 0 and isinstance(stats_data, dict):
            processed_users = int(stats_data.get('users_processed') or 0)
        if processed_users > 0:
            self.stats_labels["已同步"].setText(str(processed_users))

    def sync_finished(self, success, message, stats):
        """同步任务完成回调。"""
        stats_model = SyncRunStats.from_mapping(stats)
        self.sync_button.setEnabled(True)
        self.stop_button.setEnabled(False)

        if hasattr(self, 'update_timer') and self.update_timer.isActive():
            self.update_timer.stop()

        self.stats_labels["总用户数"].setText(str(stats_model.total_users))
        self.stats_labels["已同步"].setText(str(stats_model.processed_users))
        self.stats_labels["已禁用"].setText(str(len(stats_model.disabled_users)))
        self.stats_labels["错误数"].setText(str(stats_model.error_count))

        execution_mode = stats_model.execution_mode or 'apply'
        if success:
            done_text = "预演完成" if execution_mode == 'dry_run' else "同步完成"
            tray_text = (
                "预演任务已完成"
                if execution_mode == 'dry_run'
                else "同步任务已完成"
            )
            self.status_label.setText(done_text)
            self.tray_icon.showMessage(
                APP_TITLE,
                tray_text,
                QSystemTrayIcon.Information,
                3000,
            )
        else:
            self.status_label.setText("同步失败")
            self.tray_icon.showMessage(
                APP_TITLE,
                f"同步任务失败: {message}",
                QSystemTrayIcon.Critical,
                5000,
            )

        self.log_text.append(f"同步任务结束 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log_text.append(message)
        skipped_summary = stats_model.skipped_operations
        if skipped_summary.total:
            self.log_text.append(f"已跳过操作: {skipped_summary.total}")
            for action_type, count in skipped_summary.by_action.items():
                self.log_text.append(f"  - {action_type}: {count}")

        if success and stats_model.disabled_users:
            disabled_users = "\n".join(stats_model.disabled_users[:10])
            if len(stats_model.disabled_users) > 10:
                disabled_users += (
                    f"\n... 及其他 {len(stats_model.disabled_users) - 10} 个账号"
                )

            QMessageBox.warning(
                self,
                "账号禁用通知",
                (
                    f"同步任务已完成，但有 "
                    f"{len(stats_model.disabled_users)} 个账号被禁用:\n\n{disabled_users}"
                ),
            )

    def filter_logs(self):
        """根据过滤条件筛选日志。"""
        filter_text = self.filter_edit.text().lower()
        filter_level = self.log_level_combo.currentText()
        if not hasattr(self, 'original_logs'):
            return

        self.log_text.clear()
        for log in self.original_logs:
            if filter_level != "全部" and filter_level not in log:
                continue
            if filter_text and filter_text not in log.lower():
                continue
            if " ERROR " in log:
                self.log_text.append(f'<span style="color:#FF5252;">{log}</span>')
            elif " WARNING " in log:
                self.log_text.append(f'<span style="color:#FFD740;">{log}</span>')
            else:
                self.log_text.append(log)

    def clear_logs(self):
        """清空日志内容。"""
        self.log_text.clear()
        self.original_logs = []

    def copy_logs(self):
        """复制日志到剪贴板。"""
        clipboard = QApplication.clipboard()
        clipboard.setText(self.log_text.toPlainText())
        self.status_label.setText("日志已复制到剪贴板")

    def export_logs(self):
        """导出日志到文本文件。"""
        filename, _ = QFileDialog.getSaveFileName(
            self,
            "导出日志",
            f"同步日志_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            "文本文件 (*.txt);;所有文件 (*.*)",
        )

        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(self.log_text.toPlainText())
                self.status_label.setText(f"日志已导出到: {filename}")
            except Exception as e:
                QMessageBox.warning(
                    self,
                    "导出失败",
                    f"导出日志失败: {str(e)}",
                )

    def closeEvent(self, event):
        """关闭窗口时最小化到系统托盘。"""
        event.ignore()
        self.hide()
        self.tray_icon.showMessage(
            APP_TITLE,
            "程序仍在后台运行",
            QSystemTrayIcon.Information,
            2000,
        )

    def update_progress(self, value):
        """更新进度条显示。"""
        if 0 <= value <= 100:
            self.progress_bar.setValue(value)


    def init_local_storage(self):
        """初始化本地 SQLite 配置存储。"""
        self.local_db_manager = None
        self.local_settings_repo = None
        self.local_rule_repo = None
        self.local_db_error = None
        self.local_db_init_result = {}

        try:
            self.local_db_manager = DatabaseManager()
            self.local_db_init_result = self.local_db_manager.initialize() or {}
            self.local_settings_repo = SettingsRepository(self.local_db_manager)
            self.local_rule_repo = GroupExclusionRuleRepository(self.local_db_manager)
            migration_source = self.local_db_init_result.get("migration_source_path")
            if migration_source and hasattr(self, "log_text"):
                self.log_text.append(f"已迁移旧 SQLite 数据库到新位置: {migration_source}")
        except Exception as exc:
            self.local_db_error = str(exc)
            logging.getLogger(__name__).error(f"本地配置库初始化失败: {exc}")

    def refresh_local_strategy_summary(self):
        if not hasattr(self, 'local_strategy_summary_label'):
            return

        if self.local_db_error:
            self.local_strategy_summary_label.setText(f"本地策略存储不可用: {self.local_db_error}")
            return

        if not self.local_rule_repo or not self.local_db_manager:
            self.local_strategy_summary_label.setText("本地策略存储尚未初始化")
            return

        enabled_rules = self.local_rule_repo.list_enabled_rules()
        hard_protected = [
            row for row in self.local_rule_repo.list_rules(rule_type='protect', protection_level='hard')
            if row['is_enabled']
        ]
        soft_excluded = [
            row for row in self.local_rule_repo.list_rules(rule_type='exclude', protection_level='soft')
            if row['is_enabled']
        ]

        integrity_info = (self.local_db_init_result or {}).get("integrity_check") or {}
        integrity_text = ""
        if integrity_info:
            integrity_text = (
                f" | 完整性检查 {'通过' if integrity_info.get('ok') else integrity_info.get('result', '未知')}"
            )

        extra_notes = []
        if (self.local_db_init_result or {}).get("migration_source_path"):
            extra_notes.append("已迁移旧库")
        if (self.local_db_init_result or {}).get("startup_snapshot_path"):
            extra_notes.append("已创建启动快照")
        extra_text = f" | {' | '.join(extra_notes)}" if extra_notes else ""

        self.local_strategy_summary_label.setText(
            f"SQLite: {self.local_db_manager.db_path} | 备份目录 {self.local_db_manager.backup_dir} | "
            f"启用规则 {len(enabled_rules)} 条 | 硬保护组 {len(hard_protected)} | 软排除组 {len(soft_excluded)}"
            f"{integrity_text}{extra_text}"
        )

    def load_local_settings(self):
        if self.local_db_error:
            self.log_text.append(f"本地策略配置不可用: {self.local_db_error}")
            self.refresh_local_strategy_summary()
            return
        if not self.local_settings_repo or not self.local_rule_repo:
            return

        separator = self.local_settings_repo.get_value('group_display_separator', '-') or '-'
        separator_index = self.group_separator_combo.findData(separator)
        if separator_index >= 0:
            self.group_separator_combo.setCurrentIndex(separator_index)

        recursive_enabled = self.local_settings_repo.get_bool('group_recursive_enabled', True)
        recursive_index = self.group_recursive_combo.findData(recursive_enabled)
        if recursive_index >= 0:
            self.group_recursive_combo.setCurrentIndex(recursive_index)

        cleanup_enabled = self.local_settings_repo.get_bool('managed_relation_cleanup_enabled', False)
        cleanup_index = self.group_cleanup_combo.findData(cleanup_enabled)
        if cleanup_index >= 0:
            self.group_cleanup_combo.setCurrentIndex(cleanup_index)

        if hasattr(self, 'execution_mode_combo'):
            saved_mode = self.local_settings_repo.get_value('schedule_execution_mode', 'apply') or 'apply'
            mode_index = self.execution_mode_combo.findData(saved_mode)
            if mode_index >= 0:
                self.execution_mode_combo.setCurrentIndex(mode_index)

        protected_rules = [
            dict(row)
            for row in self.local_rule_repo.list_rules(rule_type='protect', protection_level='hard')
            if row['is_enabled']
        ]
        self.load_protected_rule_table(protected_rules)
        self.load_soft_excluded_rule_table(self.local_rule_repo.list_soft_excluded_rules())
        self.refresh_local_strategy_summary()

    def run_local_db_integrity_check(self):
        if not self.local_db_manager:
            QMessageBox.warning(self, "数据库不可用", "本地 SQLite 数据库尚未初始化。")
            return

        try:
            result = self.local_db_manager.run_integrity_check()
            self.local_db_init_result = self.local_db_init_result or {}
            self.local_db_init_result["integrity_check"] = result
            self.refresh_local_strategy_summary()
            message = f"SQLite 完整性检查结果: {result['result']}"
            self.log_text.append(message)
            self.status_label.setText("数据库检查通过" if result.get("ok") else "数据库检查失败")
            dialog = QMessageBox.information if result.get("ok") else QMessageBox.warning
            dialog(self, "完整性检查", message)
        except Exception as exc:
            self.log_text.append(f"SQLite 完整性检查失败: {exc}")
            self.status_label.setText("数据库检查失败")
            QMessageBox.critical(self, "完整性检查失败", str(exc))

    def create_local_db_backup(self):
        if not self.local_db_manager:
            QMessageBox.warning(self, "数据库不可用", "本地 SQLite 数据库尚未初始化。")
            return

        try:
            backup_path = self.local_db_manager.backup_database(label="manual_ui")
            self.log_text.append(f"SQLite 备份已创建: {backup_path}")
            self.status_label.setText("数据库备份已创建")
            self.refresh_local_strategy_summary()
            QMessageBox.information(self, "备份完成", f"备份文件已生成:\n{backup_path}")
        except Exception as exc:
            self.log_text.append(f"SQLite 备份失败: {exc}")
            self.status_label.setText("数据库备份失败")
            QMessageBox.critical(self, "备份失败", str(exc))

    def create_tray_icon(self):
        """创建系统托盘图标。"""
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(QIcon("sync.ico"))
        self.tray_icon.setToolTip(APP_TITLE)

        tray_menu = QMenu()

        show_action = QAction("显示主窗口", self)
        show_action.triggered.connect(self.show)
        tray_menu.addAction(show_action)

        sync_action = QAction("立即同步", self)
        sync_action.triggered.connect(self.start_sync)
        tray_menu.addAction(sync_action)

        tray_menu.addSeparator()

        exit_action = QAction("退出程序", self)
        exit_action.triggered.connect(self.close_application)
        tray_menu.addAction(exit_action)

        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.activated.connect(self.tray_icon_activated)
        self.tray_icon.show()

    def setup_ui(self):
        """构建主界面。"""

        def configure_form_layout(form_layout):
            form_layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
            form_layout.setRowWrapPolicy(QFormLayout.WrapLongRows)
            form_layout.setHorizontalSpacing(14)
            form_layout.setVerticalSpacing(10)

        self.setWindowTitle(APP_TITLE)

        main_layout = self.central_widget.layout
        main_layout.setSpacing(12)

        title_layout = QVBoxLayout()
        title_layout.setContentsMargins(0, 0, 0, 0)
        title_layout.setSpacing(4)

        title_label = QLabel(APP_TITLE)
        title_label.setFont(QFont("Microsoft YaHei", 16, QFont.Bold))
        title_label.setAlignment(Qt.AlignCenter)
        title_layout.addWidget(title_label)

        self.status_label = QLabel("就绪")
        self.status_label.setAlignment(Qt.AlignCenter)
        title_layout.addWidget(self.status_label)

        header_widget = QWidget()
        header_widget.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        header_widget.setLayout(title_layout)
        main_layout.addWidget(header_widget)

        tab_widget = QTabWidget()
        tab_widget.setDocumentMode(True)
        tab_widget.setUsesScrollButtons(True)

        status_tab = QWidget()
        status_layout = QVBoxLayout(status_tab)
        status_layout.setContentsMargins(0, 0, 0, 0)
        status_layout.setSpacing(12)

        top_panel = QHBoxLayout()
        top_panel.setSpacing(12)

        control_group = QGroupBox("操作控制")
        control_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        control_layout = QVBoxLayout(control_group)

        buttons_layout = QHBoxLayout()
        self.sync_button = QPushButton(qta.icon('fa5s.sync'), "立即同步")
        self.sync_button.clicked.connect(self.start_sync)
        buttons_layout.addWidget(self.sync_button)

        self.stop_button = QPushButton(qta.icon('fa5s.stop'), "停止同步")
        self.stop_button.clicked.connect(self.stop_sync)
        self.stop_button.setEnabled(False)
        buttons_layout.addWidget(self.stop_button)

        self.execution_mode_combo = QComboBox()
        self.execution_mode_combo.addItem("正式执行", "apply")
        self.execution_mode_combo.addItem("仅预演", "dry_run")
        buttons_layout.addWidget(self.execution_mode_combo)
        control_layout.addLayout(buttons_layout)

        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        control_layout.addWidget(self.progress_bar)

        stats_group = QGroupBox("同步统计")
        stats_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        stats_layout = QVBoxLayout(stats_group)

        self.stats_labels = {}
        for label_name in ["总用户数", "已同步", "已禁用", "错误数", "耗时"]:
            row_layout = QHBoxLayout()
            name_label = QLabel(f"{label_name}:")
            name_label.setMinimumWidth(80)
            value_label = QLabel("--")
            value_label.setStyleSheet("font-weight: bold;")
            row_layout.addWidget(name_label)
            row_layout.addWidget(value_label)
            stats_layout.addLayout(row_layout)
            self.stats_labels[label_name] = value_label

        top_panel.addWidget(control_group, 3)
        top_panel.addWidget(stats_group, 2)
        status_layout.addLayout(top_panel)

        log_group = QGroupBox("执行日志")
        log_layout = QVBoxLayout(log_group)

        filter_layout = QHBoxLayout()
        filter_label = QLabel("日志过滤:")
        self.filter_edit = QLineEdit()
        self.filter_edit.setPlaceholderText("输入关键字过滤日志...")
        self.filter_edit.textChanged.connect(self.filter_logs)

        level_label = QLabel("级别:")
        self.log_level_combo = QComboBox()
        self.log_level_combo.addItems(["全部", "INFO", "WARNING", "ERROR"])
        self.log_level_combo.currentTextChanged.connect(self.filter_logs)

        self.clear_log_button = QPushButton(qta.icon('fa5s.eraser'), "清空")
        self.clear_log_button.clicked.connect(self.clear_logs)

        filter_layout.addWidget(filter_label)
        filter_layout.addWidget(self.filter_edit, 1)
        filter_layout.addWidget(level_label)
        filter_layout.addWidget(self.log_level_combo)
        filter_layout.addWidget(self.clear_log_button)
        log_layout.addLayout(filter_layout)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setLineWrapMode(QTextEdit.WidgetWidth)
        self.log_text.setStyleSheet(
            """
            QTextEdit {
                font-family: "Consolas", "Courier New", monospace;
                font-size: 10pt;
            }
            """
        )
        log_layout.addWidget(self.log_text)

        export_layout = QHBoxLayout()
        export_layout.addStretch()

        self.copy_log_button = QPushButton(qta.icon('fa5s.copy'), "复制日志")
        self.copy_log_button.clicked.connect(self.copy_logs)
        export_layout.addWidget(self.copy_log_button)

        self.export_log_button = QPushButton(qta.icon('fa5s.file-export'), "导出日志")
        self.export_log_button.clicked.connect(self.export_logs)
        export_layout.addWidget(self.export_log_button)
        log_layout.addLayout(export_layout)

        status_layout.addWidget(log_group, 1)

        config_tab = QWidget()
        config_outer_layout = QVBoxLayout(config_tab)
        config_outer_layout.setContentsMargins(0, 0, 0, 0)

        config_scroll = QScrollArea()
        config_scroll.setWidgetResizable(True)
        config_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        config_scroll_content = QWidget()
        config_layout = QVBoxLayout(config_scroll_content)
        config_layout.setContentsMargins(4, 4, 12, 4)
        config_layout.setSpacing(12)
        config_scroll.setWidget(config_scroll_content)
        config_outer_layout.addWidget(config_scroll, 1)

        wecom_group = QGroupBox("企业微信配置")
        wecom_layout = QFormLayout(wecom_group)
        configure_form_layout(wecom_layout)

        self.corpid_edit = QLineEdit()
        wecom_layout.addRow("企业ID (CorpID):", self.corpid_edit)

        self.corpsecret_edit = QLineEdit()
        wecom_layout.addRow("应用 Secret:", self.corpsecret_edit)

        self.webhook_edit = QLineEdit()
        wecom_layout.addRow("机器人 Webhook:", self.webhook_edit)
        config_layout.addWidget(wecom_group)

        ldap_group = QGroupBox("LDAP/AD 域配置")
        ldap_layout = QFormLayout(ldap_group)
        configure_form_layout(ldap_layout)

        self.ldap_server_edit = QLineEdit()
        ldap_layout.addRow("LDAP 服务器:", self.ldap_server_edit)

        self.ldap_domain_edit = QLineEdit()
        ldap_layout.addRow("域名:", self.ldap_domain_edit)

        self.ldap_username_edit = QLineEdit()
        ldap_layout.addRow("管理员账号:", self.ldap_username_edit)

        self.ldap_password_edit = QLineEdit()
        self.ldap_password_edit.setEchoMode(QLineEdit.Password)
        ldap_layout.addRow("管理员密码:", self.ldap_password_edit)

        ssl_layout = QHBoxLayout()
        self.ldap_ssl_checkbox = QComboBox()
        self.ldap_ssl_checkbox.addItems(["启用SSL", "禁用SSL"])
        ssl_layout.addWidget(self.ldap_ssl_checkbox)

        self.ldap_port_spinbox = QSpinBox()
        self.ldap_port_spinbox.setMinimum(1)
        self.ldap_port_spinbox.setMaximum(65535)
        self.ldap_port_spinbox.setValue(636)
        ssl_layout.addWidget(QLabel("端口:"))
        ssl_layout.addWidget(self.ldap_port_spinbox)
        ldap_layout.addRow("SSL/端口:", ssl_layout)
        config_layout.addWidget(ldap_group)

        schedule_group = QGroupBox("计划任务配置")
        schedule_layout = QFormLayout(schedule_group)
        configure_form_layout(schedule_layout)

        self.schedule_time_edit = QTimeEdit()
        self.schedule_time_edit.setDisplayFormat("HH:mm")
        schedule_layout.addRow("每日执行时间:", self.schedule_time_edit)

        self.interval_spinbox = QSpinBox()
        self.interval_spinbox.setMinimum(5)
        self.interval_spinbox.setMaximum(1440)
        self.interval_spinbox.setValue(30)
        schedule_layout.addRow("执行间隔(分钟):", self.interval_spinbox)

        self.retry_spinbox = QSpinBox()
        self.retry_spinbox.setMinimum(0)
        self.retry_spinbox.setMaximum(10)
        self.retry_spinbox.setValue(3)
        schedule_layout.addRow("最大重试次数:", self.retry_spinbox)
        config_layout.addWidget(schedule_group)

        strategy_group = QGroupBox("本地同步策略")
        strategy_layout = QFormLayout(strategy_group)
        configure_form_layout(strategy_layout)

        self.group_separator_combo = QComboBox()
        self.group_separator_combo.addItem("连接符 -", "-")
        self.group_separator_combo.addItem("连接符 _", "_")
        self.group_separator_combo.addItem("空格", " ")
        strategy_layout.addRow("分组显示连接符:", self.group_separator_combo)

        self.group_recursive_combo = QComboBox()
        self.group_recursive_combo.addItem("启用", True)
        self.group_recursive_combo.addItem("禁用", False)
        strategy_layout.addRow("递归组层级:", self.group_recursive_combo)

        self.group_cleanup_combo = QComboBox()
        self.group_cleanup_combo.addItem("禁用", False)
        self.group_cleanup_combo.addItem("启用", True)
        strategy_layout.addRow("受管关系清理:", self.group_cleanup_combo)

        self.protected_groups_text = QTextEdit()
        self.protected_groups_text.setReadOnly(True)
        self.protected_groups_text.setMaximumHeight(120)
        self.protected_groups_text.hide()
        self.protected_groups_text.setPlaceholderText("系统硬保护组会以只读表格显示。")
        strategy_layout.addRow("硬保护组(只读):", self.protected_groups_text)

        self.protected_groups_table = QTableWidget(0, 3)
        self.protected_groups_table.setHorizontalHeaderLabels(["组名", "匹配类型", "来源"])
        self.protected_groups_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.protected_groups_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.protected_groups_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.protected_groups_table.verticalHeader().setVisible(False)
        self.protected_groups_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.protected_groups_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.protected_groups_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.protected_groups_table.setMinimumHeight(140)
        strategy_layout.addRow("硬保护组:", self.protected_groups_table)

        self.soft_excluded_groups_table = QTableWidget(0, 3)
        self.soft_excluded_groups_table.setHorizontalHeaderLabels(["启用", "组名", "来源"])
        self.soft_excluded_groups_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.soft_excluded_groups_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.soft_excluded_groups_table.setEditTriggers(
            QAbstractItemView.DoubleClicked
            | QAbstractItemView.EditKeyPressed
            | QAbstractItemView.SelectedClicked
        )
        self.soft_excluded_groups_table.verticalHeader().setVisible(False)
        self.soft_excluded_groups_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.soft_excluded_groups_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.soft_excluded_groups_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.soft_excluded_groups_table.setMinimumHeight(180)

        soft_rule_layout = QVBoxLayout()
        soft_rule_layout.addWidget(self.soft_excluded_groups_table)

        soft_rule_button_layout = QHBoxLayout()
        self.add_soft_rule_button = QPushButton("新增规则")
        self.add_soft_rule_button.clicked.connect(self.add_soft_excluded_rule)
        soft_rule_button_layout.addWidget(self.add_soft_rule_button)

        self.remove_soft_rule_button = QPushButton("移除选中")
        self.remove_soft_rule_button.clicked.connect(self.remove_selected_soft_excluded_rules)
        soft_rule_button_layout.addWidget(self.remove_soft_rule_button)
        soft_rule_button_layout.addStretch()
        soft_rule_layout.addLayout(soft_rule_button_layout)

        self.soft_rule_table_widget = QWidget()
        self.soft_rule_table_widget.setLayout(soft_rule_layout)
        strategy_layout.addRow("软排除组:", self.soft_rule_table_widget)

        self.local_strategy_summary_label = QLabel("本地策略尚未加载...")
        self.local_strategy_summary_label.setWordWrap(True)
        strategy_layout.addRow("摘要:", self.local_strategy_summary_label)

        db_action_layout = QHBoxLayout()
        self.db_integrity_check_button = QPushButton(qta.icon('fa5s.shield-alt'), "完整性检查")
        self.db_integrity_check_button.clicked.connect(self.run_local_db_integrity_check)
        db_action_layout.addWidget(self.db_integrity_check_button)

        self.db_backup_button = QPushButton(qta.icon('fa5s.database'), "创建备份")
        self.db_backup_button.clicked.connect(self.create_local_db_backup)
        db_action_layout.addWidget(self.db_backup_button)
        db_action_layout.addStretch()
        strategy_layout.addRow("数据库运维:", db_action_layout)

        config_layout.addWidget(strategy_group)
        config_layout.addStretch(1)

        self.save_config_button = QPushButton(qta.icon('fa5s.save'), "保存配置")
        self.save_config_button.clicked.connect(self.save_config)
        save_action_layout = QHBoxLayout()
        save_action_layout.addStretch()
        save_action_layout.addWidget(self.save_config_button)
        config_outer_layout.addLayout(save_action_layout)

        tab_widget.addTab(status_tab, qta.icon('fa5s.desktop'), "状态")
        tab_widget.addTab(config_tab, qta.icon('fa5s.cog'), "配置")
        main_layout.addWidget(tab_widget, 1)

        footer_label = QLabel("(c) 2026 Notting AD Sync")
        footer_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(footer_label)

    def load_config(self):
        """从配置文件加载界面配置。"""
        try:
            config_path = os.path.join(APP_PATH, "config.ini")
            self.log_text.append(f"正在加载配置文件: {config_path}")

            config = configparser.ConfigParser()
            config.read(config_path, encoding='utf-8')

            self.corpid_edit.setText(config.get('WeChat', 'CorpID', fallback=''))
            self.corpsecret_edit.setText(config.get('WeChat', 'CorpSecret', fallback=''))
            self.webhook_edit.setText(config.get('WeChatBot', 'WebhookUrl', fallback=''))

            self.ldap_server_edit.setText(config.get('LDAP', 'Server', fallback=''))
            self.ldap_domain_edit.setText(
                config.get('LDAP', 'Domain', fallback=config.get('Domain', 'Name', fallback=''))
            )
            self.ldap_username_edit.setText(config.get('LDAP', 'Username', fallback=''))
            self.ldap_password_edit.setText(config.get('LDAP', 'Password', fallback=''))

            use_ssl = config.getboolean('LDAP', 'UseSSL', fallback=True)
            self.ldap_ssl_checkbox.setCurrentText("启用SSL" if use_ssl else "禁用SSL")
            self.ldap_port_spinbox.setValue(config.getint('LDAP', 'Port', fallback=636 if use_ssl else 389))

            if 'Schedule' in config:
                if 'Time' in config['Schedule']:
                    try:
                        hour, minute = map(int, config.get('Schedule', 'Time').split(':'))
                        self.schedule_time_edit.setTime(QTime(hour, minute))
                    except (ValueError, IndexError):
                        self.schedule_time_edit.setTime(QTime(3, 0))

                if 'RetryInterval' in config['Schedule']:
                    try:
                        self.interval_spinbox.setValue(config.getint('Schedule', 'RetryInterval'))
                    except ValueError:
                        self.interval_spinbox.setValue(60)

                if 'MaxRetries' in config['Schedule']:
                    try:
                        self.retry_spinbox.setValue(config.getint('Schedule', 'MaxRetries'))
                    except ValueError:
                        self.retry_spinbox.setValue(3)

            self.log_text.append("配置加载成功")
            self.status_label.setText("配置已加载")
        except Exception as exc:
            self.log_text.append(f"加载配置失败: {exc}")
            self.status_label.setText("配置加载失败")

    def save_config(self):
        """保存当前界面配置到配置文件。"""
        try:
            config_path = os.path.join(APP_PATH, "config.ini")
            self.log_text.append(f"正在保存配置到: {config_path}")

            config = configparser.ConfigParser()
            if os.path.exists(config_path):
                config.read(config_path, encoding='utf-8')

            for section in [
                'WeChat', 'WeChatBot', 'Domain', 'LDAP', 'ExcludeUsers', 'ExcludeDepartments',
                'Sync', 'Account', 'Schedule', 'Logging'
            ]:
                if not config.has_section(section):
                    config.add_section(section)

            config.set('WeChat', 'CorpID', self.corpid_edit.text())
            config.set('WeChat', 'CorpSecret', self.corpsecret_edit.text())
            config.set('WeChatBot', 'WebhookUrl', self.webhook_edit.text())

            config.set('LDAP', 'Server', self.ldap_server_edit.text())
            config.set('LDAP', 'Domain', self.ldap_domain_edit.text())
            config.set('LDAP', 'Username', self.ldap_username_edit.text())
            config.set('LDAP', 'Password', self.ldap_password_edit.text())
            use_ssl = self.ldap_ssl_checkbox.currentText() == "启用SSL"
            config.set('LDAP', 'UseSSL', str(use_ssl).lower())
            config.set('LDAP', 'Port', str(self.ldap_port_spinbox.value()))

            config.set('Domain', 'Name', self.ldap_domain_edit.text())

            if 'SystemAccounts' not in config['ExcludeUsers']:
                config.set('ExcludeUsers', 'SystemAccounts', 'admin,administrator,guest,krbtgt')
            if 'CustomAccounts' not in config['ExcludeUsers']:
                config.set('ExcludeUsers', 'CustomAccounts', '')
            if 'Names' not in config['ExcludeDepartments']:
                config.set('ExcludeDepartments', 'Names', '')

            if 'ForceFullSync' not in config['Sync']:
                config.set('Sync', 'ForceFullSync', 'false')
            if 'SyncMode' not in config['Sync']:
                config.set('Sync', 'SyncMode', 'full')
            if 'KeepHistoryDays' not in config['Sync']:
                config.set('Sync', 'KeepHistoryDays', '30')

            if 'DefaultPassword' not in config['Account']:
                config.set('Account', 'DefaultPassword', '')
            if 'ForceChangePassword' not in config['Account']:
                config.set('Account', 'ForceChangePassword', 'true')
            if 'PasswordComplexity' not in config['Account']:
                config.set('Account', 'PasswordComplexity', 'strong')

            config.set('Schedule', 'Time', self.schedule_time_edit.time().toString("HH:mm"))
            config.set('Schedule', 'RetryInterval', str(self.interval_spinbox.value()))
            config.set('Schedule', 'MaxRetries', str(self.retry_spinbox.value()))

            if 'Level' not in config['Logging']:
                config.set('Logging', 'Level', 'INFO')
            if 'DetailedLogging' not in config['Logging']:
                config.set('Logging', 'DetailedLogging', 'true')
            if 'KeepLogsDays' not in config['Logging']:
                config.set('Logging', 'KeepLogsDays', '30')

            self.save_local_settings()
            with open(config_path, 'w', encoding='utf-8') as configfile:
                config.write(configfile)

            self.status_label.setText("配置已保存")
            self.log_text.append("配置保存成功")
            self.setup_scheduler()
        except Exception as exc:
            self.log_text.append(f"保存配置失败: {exc}")
            self.status_label.setText("配置保存失败")

    def setup_scheduler(self):
        """设置定时任务调度，并确保只存在一个调度线程。"""
        schedule.clear()

        time_str = self.schedule_time_edit.time().toString("HH:mm")
        schedule.every().day.at(time_str).do(self.start_sync, 'schedule')

        interval_minutes = self.interval_spinbox.value()
        schedule.every(interval_minutes).minutes.do(self.start_sync, 'schedule')

        if hasattr(self, 'schedule_thread') and self.schedule_thread.isRunning():
            self.schedule_thread.stop()
            self.schedule_thread.wait(2000)

        self.log_text.append(f"调度设置完成: 每日 {time_str} 和每 {interval_minutes} 分钟执行一次")
        self.schedule_thread = ScheduleThread()
        self.schedule_thread.start()

class ScheduleThread(QThread):
    """执行定时任务的线程"""
    def __init__(self):
        super().__init__()
        self.running = True
    
    def run(self):
        while self.running:
            schedule.run_pending()
            time.sleep(1)
    
    def stop(self):
        self.running = False

def main():
    # 确保只有一个实例运行
    app = QApplication(sys.argv)
    app.setApplicationName("Notting AD Sync")
    app.setApplicationVersion("1.0.0")
    
    # 设置应用程序工作目录
    os.chdir(APP_PATH)
    
    # 创建配置目录和日志目录
    try:
        if not os.path.exists(LOGS_DIR):
            os.makedirs(LOGS_DIR)
            
        # 如果配置文件不存在，创建默认配置
        config_path = os.path.join(APP_PATH, "config.ini")
        if not os.path.exists(config_path):
            default_config = """[WeChat]
CorpID = 
CorpSecret = 

[WeChatBot]
WebhookUrl = 

[Domain]
Name = 

[LDAP]
# LDAP服务器地址（域控制器地址）
Server = dc.example.com
# 域名
Domain = example.com
# 管理员用户名（格式：DOMAIN\\username 或 username@domain）
Username = DOMAIN\\administrator
# 管理员密码
Password = 
# 是否使用SSL/TLS加密连接
UseSSL = true
# LDAP端口（默认：636用于LDAPS，389用于LDAP）
Port = 636

[ExcludeUsers]
SystemAccounts = admin,administrator,guest,krbtgt
CustomAccounts = 

[ExcludeDepartments]
Names = 

[Sync]
ForceFullSync = false
SyncMode = full
KeepHistoryDays = 30

[Account]
DefaultPassword =
ForceChangePassword = true
PasswordComplexity = strong

[Schedule]
Time = 03:00
RetryInterval = 60
MaxRetries = 3

[Logging]
Level = INFO
DetailedLogging = true
KeepLogsDays = 30
"""
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(default_config)
            print(f"已创建默认配置文件: {config_path}")
    except Exception as e:
        print(f"初始化应用程序环境出错: {str(e)}")
    
    # 尝试加载应用程序图标
    icon_path = os.path.join(APP_PATH, "sync.ico")
    if os.path.exists(icon_path):
        app.setWindowIcon(QIcon(icon_path))
    
    # 创建主窗口
    window = MainWindow()
    window.show()
    
    # 启动应用程序
    sys.exit(app.exec_())

if __name__ == '__main__':
    main() 
