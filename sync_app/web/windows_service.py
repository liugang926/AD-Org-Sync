from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:  # pragma: no cover - exercised on Windows deployments
    import servicemanager
    import win32event
    import win32evtlogutil
    import win32service
    import win32serviceutil
except ImportError:  # pragma: no cover - keeps imports safe for non-service test runs
    servicemanager = None
    win32event = None
    win32evtlogutil = None
    win32service = None
    win32serviceutil = None

from sync_app.core.common import APP_VERSION
from sync_app.storage.local_db import DatabaseManager, OrganizationRepository, SettingsRepository
from sync_app.web.app import create_app
from sync_app.web.runtime import resolve_web_runtime_settings

SERVICE_NAME = "ADOrgSyncWeb"
SERVICE_DISPLAY_NAME = "AD Org Sync Web"
SERVICE_DESCRIPTION = "Hosts the AD Org Sync web control plane as a Windows service."
DEFAULT_SERVICE_LOG_PATH = str(
    (Path(os.environ.get("PROGRAMDATA") or Path.home()) / "ADOrgSync" / "logs" / "web-service.log").resolve()
)
SERVICE_STATE_LABELS = {
    1: "stopped",
    2: "start_pending",
    3: "stop_pending",
    4: "running",
    5: "continue_pending",
    6: "pause_pending",
    7: "paused",
}


class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": SERVICE_NAME,
            "app_version": APP_VERSION,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def _require_pywin32() -> None:
    if win32serviceutil is None or win32service is None or win32event is None:
        raise RuntimeError("pywin32 is required for Windows service operations")


def _to_bool_text(value: bool | None) -> str:
    if value is None:
        return ""
    return "true" if value else "false"


def _parse_bool_text(value: Any) -> bool | None:
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return None


def _resolve_service_class_string() -> str:
    return f"{ADOrgSyncWebService.__module__}.{ADOrgSyncWebService.__name__}"


@dataclass
class ServiceOptions:
    db_path: str = ""
    config_path: str = ""
    bind_host: str = ""
    bind_port: int | None = None
    public_base_url: str = ""
    secure_cookies: str = ""
    trust_proxy_headers: bool | None = None
    forwarded_allow_ips: str = ""
    log_path: str = DEFAULT_SERVICE_LOG_PATH

    def to_registry_values(self) -> dict[str, Any]:
        return {
            "db_path": self.db_path,
            "config_path": self.config_path,
            "bind_host": self.bind_host,
            "bind_port": int(self.bind_port or 0),
            "public_base_url": self.public_base_url,
            "secure_cookies": self.secure_cookies,
            "trust_proxy_headers": _to_bool_text(self.trust_proxy_headers),
            "forwarded_allow_ips": self.forwarded_allow_ips,
            "log_path": self.log_path,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "db_path": self.db_path,
            "config_path": self.config_path,
            "bind_host": self.bind_host,
            "bind_port": self.bind_port,
            "public_base_url": self.public_base_url,
            "secure_cookies": self.secure_cookies,
            "trust_proxy_headers": self.trust_proxy_headers,
            "forwarded_allow_ips": self.forwarded_allow_ips,
            "log_path": self.log_path,
        }


def _service_exists(service_name: str = SERVICE_NAME) -> bool:
    _require_pywin32()
    try:
        win32serviceutil.QueryServiceStatus(service_name)
        return True
    except Exception:
        return False


def load_service_options(service_name: str = SERVICE_NAME) -> ServiceOptions:
    _require_pywin32()
    return ServiceOptions(
        db_path=str(win32serviceutil.GetServiceCustomOption(service_name, "db_path", "") or ""),
        config_path=str(win32serviceutil.GetServiceCustomOption(service_name, "config_path", "") or ""),
        bind_host=str(win32serviceutil.GetServiceCustomOption(service_name, "bind_host", "") or ""),
        bind_port=(
            int(win32serviceutil.GetServiceCustomOption(service_name, "bind_port", 0) or 0) or None
        ),
        public_base_url=str(win32serviceutil.GetServiceCustomOption(service_name, "public_base_url", "") or ""),
        secure_cookies=str(win32serviceutil.GetServiceCustomOption(service_name, "secure_cookies", "") or ""),
        trust_proxy_headers=_parse_bool_text(
            win32serviceutil.GetServiceCustomOption(service_name, "trust_proxy_headers", "")
        ),
        forwarded_allow_ips=str(
            win32serviceutil.GetServiceCustomOption(service_name, "forwarded_allow_ips", "") or ""
        ),
        log_path=str(
            win32serviceutil.GetServiceCustomOption(service_name, "log_path", DEFAULT_SERVICE_LOG_PATH)
            or DEFAULT_SERVICE_LOG_PATH
        ),
    )


def _resolve_service_options(args: argparse.Namespace, *, existing: ServiceOptions | None = None) -> ServiceOptions:
    current = existing or ServiceOptions()
    return ServiceOptions(
        db_path=str(args.db_path if args.db_path is not None else current.db_path or ""),
        config_path=str(args.config if args.config is not None else current.config_path or ""),
        bind_host=str(args.host if args.host is not None else current.bind_host or ""),
        bind_port=(args.port if args.port is not None else current.bind_port),
        public_base_url=str(args.public_base_url if args.public_base_url is not None else current.public_base_url or ""),
        secure_cookies=str(args.secure_cookies if args.secure_cookies is not None else current.secure_cookies or ""),
        trust_proxy_headers=(
            True
            if getattr(args, "trust_proxy_headers", False)
            else False
            if getattr(args, "no_trust_proxy_headers", False)
            else current.trust_proxy_headers
        ),
        forwarded_allow_ips=str(
            args.forwarded_allow_ips if args.forwarded_allow_ips is not None else current.forwarded_allow_ips or ""
        ),
        log_path=str(args.log_path if args.log_path is not None else current.log_path or DEFAULT_SERVICE_LOG_PATH),
    )


def save_service_options(options: ServiceOptions, *, service_name: str = SERVICE_NAME) -> None:
    _require_pywin32()
    for key, value in options.to_registry_values().items():
        win32serviceutil.SetServiceCustomOption(service_name, key, value)


def _coerce_startup_mode(value: str) -> tuple[int, bool | None]:
    _require_pywin32()
    normalized = str(value or "auto").strip().lower()
    if normalized == "auto":
        return win32service.SERVICE_AUTO_START, False
    if normalized == "delayed":
        return win32service.SERVICE_AUTO_START, True
    if normalized == "manual":
        return win32service.SERVICE_DEMAND_START, None
    if normalized == "disabled":
        return win32service.SERVICE_DISABLED, None
    raise ValueError(f"unsupported startup mode: {value}")


def _query_service_status(service_name: str = SERVICE_NAME) -> dict[str, Any]:
    _require_pywin32()
    status = win32serviceutil.QueryServiceStatus(service_name)
    current_state = int(status[1])
    result = {
        "service_name": service_name,
        "display_name": SERVICE_DISPLAY_NAME,
        "installed": True,
        "state_code": current_state,
        "state": SERVICE_STATE_LABELS.get(current_state, f"unknown:{current_state}"),
        "controls_accepted": int(status[2]),
        "win32_exit_code": int(status[3]),
        "service_exit_code": int(status[4]),
        "check_point": int(status[5]),
        "wait_hint": int(status[6]),
    }
    result["options"] = load_service_options(service_name).to_dict()
    return result


def _configure_logging(log_path: str) -> None:
    resolved_log_path = Path(log_path).expanduser().resolve()
    resolved_log_path.parent.mkdir(parents=True, exist_ok=True)
    root_logger = logging.getLogger()
    file_handler = logging.FileHandler(resolved_log_path, encoding="utf-8")
    file_handler.setFormatter(JsonLineFormatter())
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler],
        force=True,
    )


def _emit_service_event(message: str, *, level: str = "info") -> None:
    normalized_level = str(level or "info").strip().lower()
    if servicemanager is not None:
        if normalized_level == "error":
            servicemanager.LogErrorMsg(message)
        elif normalized_level == "warning":
            servicemanager.LogWarningMsg(message)
        else:
            servicemanager.LogInfoMsg(message)
    if win32evtlogutil is None:
        return
    try:  # pragma: no cover - depends on host Event Log registration
        event_type = {
            "error": 1,
            "warning": 2,
        }.get(normalized_level, 4)
        win32evtlogutil.ReportEvent(
            SERVICE_NAME,
            eventID=0x1000,
            eventCategory=0,
            eventType=event_type,
            strings=[message],
        )
    except Exception:
        return


def _configure_service_recovery_policy(service_name: str = SERVICE_NAME) -> None:
    commands = [
        ["sc.exe", "failure", service_name, "reset=", "86400", "actions=", "restart/60000/restart/60000/restart/60000"],
        ["sc.exe", "failureflag", service_name, "1"],
    ]
    for command in commands:
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
        except Exception as exc:  # pragma: no cover - depends on host SCM permissions
            logging.getLogger(__name__).warning("failed to configure service recovery policy: %s", exc)


def _build_runtime(service_name: str = SERVICE_NAME) -> tuple[DatabaseManager, str, dict[str, Any]]:
    options = load_service_options(service_name)
    _configure_logging(options.log_path or DEFAULT_SERVICE_LOG_PATH)
    db_manager = DatabaseManager(db_path=options.db_path or None)
    db_manager.initialize()
    organization_repo = OrganizationRepository(db_manager)
    existing_default = organization_repo.get_organization_record("default")
    effective_config_path = (
        str(options.config_path or "").strip()
        or (existing_default.config_path if existing_default else "")
        or "config.ini"
    )
    settings_repo = SettingsRepository(db_manager)
    web_runtime_settings = resolve_web_runtime_settings(
        settings_repo,
        bind_host=options.bind_host or None,
        bind_port=options.bind_port,
        public_base_url=options.public_base_url or None,
        session_cookie_secure_mode=options.secure_cookies or None,
        trust_proxy_headers=options.trust_proxy_headers,
        forwarded_allow_ips=options.forwarded_allow_ips or None,
    )
    return db_manager, effective_config_path, web_runtime_settings


class ADOrgSyncWebService(win32serviceutil.ServiceFramework if win32serviceutil else object):
    _svc_name_ = SERVICE_NAME
    _svc_display_name_ = SERVICE_DISPLAY_NAME
    _svc_description_ = SERVICE_DESCRIPTION

    def __init__(self, args):
        _require_pywin32()
        super().__init__(args)
        self._stop_event = win32event.CreateEvent(None, 0, 0, None)
        self._server = None
        self._server_thread: threading.Thread | None = None
        self._logger = logging.getLogger("sync_app.web.windows_service")

    def SvcStop(self):  # pragma: no cover - exercised by Windows SCM
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        if self._server is not None:
            self._server.should_exit = True
        win32event.SetEvent(self._stop_event)

    def SvcDoRun(self):  # pragma: no cover - exercised by Windows SCM
        import uvicorn

        db_manager = None
        try:
            db_manager, effective_config_path, web_runtime_settings = _build_runtime(self._svc_name_)
            self._logger.info(
                "starting %s version=%s host=%s port=%s db_path=%s",
                SERVICE_NAME,
                APP_VERSION,
                web_runtime_settings["bind_host"],
                web_runtime_settings["bind_port"],
                db_manager.db_path,
            )
            if servicemanager is not None:
                _emit_service_event(
                    f"{SERVICE_NAME} starting on {web_runtime_settings['bind_host']}:{web_runtime_settings['bind_port']}",
                    level="info",
                )

            app = create_app(
                db_path=db_manager.db_path,
                config_path=effective_config_path,
                bind_host=web_runtime_settings["bind_host"],
                bind_port=web_runtime_settings["bind_port"],
                public_base_url=web_runtime_settings["public_base_url"],
                session_cookie_secure_mode=web_runtime_settings["session_cookie_secure_mode"],
                trust_proxy_headers=web_runtime_settings["trust_proxy_headers"],
                forwarded_allow_ips=web_runtime_settings["forwarded_allow_ips"],
            )
            config = uvicorn.Config(
                app,
                host=web_runtime_settings["bind_host"],
                port=web_runtime_settings["bind_port"],
                loop="sync_app.cli:windows_selector_loop_factory" if sys.platform.startswith("win") else "auto",
                proxy_headers=web_runtime_settings["trust_proxy_headers"],
                forwarded_allow_ips=(
                    web_runtime_settings["forwarded_allow_ips"]
                    if web_runtime_settings["trust_proxy_headers"]
                    else None
                ),
            )
            self._server = uvicorn.Server(config)
            self._server.install_signal_handlers = lambda: None
            self._server_thread = threading.Thread(target=self._server.run, name=SERVICE_NAME, daemon=True)
            self._server_thread.start()
            self.ReportServiceStatus(win32service.SERVICE_RUNNING)

            while self._server_thread.is_alive():
                wait_result = win32event.WaitForSingleObject(self._stop_event, 1000)
                if wait_result == win32event.WAIT_OBJECT_0:
                    break

            if self._server is not None:
                self._server.should_exit = True
            if self._server_thread is not None:
                self._server_thread.join(timeout=30)
            self._logger.info("%s stopped", SERVICE_NAME)
            _emit_service_event(f"{SERVICE_NAME} stopped", level="info")
        except Exception as exc:
            self._logger.exception("failed to run %s: %s", SERVICE_NAME, exc)
            _emit_service_event(f"{SERVICE_NAME} failed: {exc}", level="error")
            raise
        finally:
            self.ReportServiceStatus(win32service.SERVICE_STOPPED)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m sync_app.web.windows_service")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common_options(command_parser: argparse.ArgumentParser) -> None:
        command_parser.add_argument("--db-path", default=None, help="Override local SQLite database path")
        command_parser.add_argument("--config", default=None, help="Optional legacy config file path")
        command_parser.add_argument("--host", default=None, help="Bind host override")
        command_parser.add_argument("--port", type=int, default=None, help="Bind port override")
        command_parser.add_argument("--public-base-url", default=None, help="Public HTTPS base URL override")
        command_parser.add_argument(
            "--secure-cookies",
            choices=["auto", "always", "never"],
            default=None,
            help="Secure cookie policy override",
        )
        proxy_group = command_parser.add_mutually_exclusive_group()
        proxy_group.add_argument("--trust-proxy-headers", action="store_true", help="Trust forwarded headers")
        proxy_group.add_argument("--no-trust-proxy-headers", action="store_true", help="Disable forwarded headers")
        command_parser.add_argument("--forwarded-allow-ips", default=None, help="Forwarded header allowlist")
        command_parser.add_argument("--log-path", default=None, help="Service log file path")

    install_parser = subparsers.add_parser("install", help="Install the Windows service")
    add_common_options(install_parser)
    install_parser.add_argument(
        "--startup",
        choices=["auto", "delayed", "manual", "disabled"],
        default="auto",
        help="Windows service startup mode",
    )
    install_parser.add_argument("--service-user", default=None, help="Optional Windows service account")
    install_parser.add_argument("--service-password", default=None, help="Optional Windows service account password")

    update_parser = subparsers.add_parser("update", help="Update the Windows service configuration")
    add_common_options(update_parser)
    update_parser.add_argument(
        "--startup",
        choices=["auto", "delayed", "manual", "disabled"],
        default="auto",
        help="Windows service startup mode",
    )
    update_parser.add_argument("--service-user", default=None, help="Optional Windows service account")
    update_parser.add_argument("--service-password", default=None, help="Optional Windows service account password")

    start_parser = subparsers.add_parser("start", help="Start the Windows service")
    start_parser.add_argument("--wait", type=int, default=30, help="Seconds to wait for running state")

    stop_parser = subparsers.add_parser("stop", help="Stop the Windows service")
    stop_parser.add_argument("--wait", type=int, default=30, help="Seconds to wait for stopped state")

    restart_parser = subparsers.add_parser("restart", help="Restart the Windows service")
    restart_parser.add_argument("--wait", type=int, default=30, help="Seconds to wait for running state")

    remove_parser = subparsers.add_parser("remove", help="Remove the Windows service")
    remove_parser.add_argument("--wait", type=int, default=30, help="Seconds to wait for stopped state before removal")

    status_parser = subparsers.add_parser("status", help="Show Windows service state")
    status_parser.add_argument("--json", action="store_true", help="Print machine-readable output")
    return parser


def _handle_install(args: argparse.Namespace) -> int:
    _require_pywin32()
    if _service_exists():
        print(f"service already exists: {SERVICE_NAME}", file=sys.stderr)
        print("use `update` to modify the service configuration", file=sys.stderr)
        return 1
    service_options = _resolve_service_options(args)
    start_type, delayedstart = _coerce_startup_mode(args.startup)
    win32serviceutil.InstallService(
        _resolve_service_class_string(),
        SERVICE_NAME,
        SERVICE_DISPLAY_NAME,
        startType=start_type,
        serviceDeps=None,
        userName=args.service_user,
        password=args.service_password,
        exeName=sys.executable,
        exeArgs=None,
        description=SERVICE_DESCRIPTION,
        delayedstart=delayedstart,
    )
    save_service_options(service_options)
    _configure_service_recovery_policy(SERVICE_NAME)
    print(f"service installed: {SERVICE_NAME}")
    print(f"log_path: {service_options.log_path}")
    return 0


def _handle_update(args: argparse.Namespace) -> int:
    _require_pywin32()
    if not _service_exists():
        print(f"service not installed: {SERVICE_NAME}", file=sys.stderr)
        return 1
    existing = load_service_options()
    service_options = _resolve_service_options(args, existing=existing)
    start_type, delayedstart = _coerce_startup_mode(args.startup)
    win32serviceutil.ChangeServiceConfig(
        _resolve_service_class_string(),
        SERVICE_NAME,
        startType=start_type,
        userName=args.service_user,
        password=args.service_password,
        exeName=sys.executable,
        displayName=SERVICE_DISPLAY_NAME,
        exeArgs=None,
        description=SERVICE_DESCRIPTION,
        delayedstart=delayedstart,
    )
    save_service_options(service_options)
    _configure_service_recovery_policy(SERVICE_NAME)
    print(f"service updated: {SERVICE_NAME}")
    print(f"log_path: {service_options.log_path}")
    return 0


def _handle_start(args: argparse.Namespace) -> int:
    _require_pywin32()
    if not _service_exists():
        print(f"service not installed: {SERVICE_NAME}", file=sys.stderr)
        return 1
    win32serviceutil.StartService(SERVICE_NAME)
    if args.wait:
        win32serviceutil.WaitForServiceStatus(SERVICE_NAME, win32service.SERVICE_RUNNING, args.wait)
    print(f"service started: {SERVICE_NAME}")
    return 0


def _handle_stop(args: argparse.Namespace) -> int:
    _require_pywin32()
    if not _service_exists():
        print(f"service not installed: {SERVICE_NAME}", file=sys.stderr)
        return 1
    win32serviceutil.StopService(SERVICE_NAME)
    if args.wait:
        win32serviceutil.WaitForServiceStatus(SERVICE_NAME, win32service.SERVICE_STOPPED, args.wait)
    print(f"service stopped: {SERVICE_NAME}")
    return 0


def _handle_restart(args: argparse.Namespace) -> int:
    _require_pywin32()
    if not _service_exists():
        print(f"service not installed: {SERVICE_NAME}", file=sys.stderr)
        return 1
    win32serviceutil.RestartService(SERVICE_NAME, waitSeconds=args.wait)
    print(f"service restarted: {SERVICE_NAME}")
    return 0


def _handle_remove(args: argparse.Namespace) -> int:
    _require_pywin32()
    if not _service_exists():
        print(f"service not installed: {SERVICE_NAME}", file=sys.stderr)
        return 1
    try:
        win32serviceutil.StopService(SERVICE_NAME)
        if args.wait:
            win32serviceutil.WaitForServiceStatus(SERVICE_NAME, win32service.SERVICE_STOPPED, args.wait)
    except Exception:
        pass
    win32serviceutil.RemoveService(SERVICE_NAME)
    print(f"service removed: {SERVICE_NAME}")
    return 0


def _handle_status(args: argparse.Namespace) -> int:
    _require_pywin32()
    if not _service_exists():
        payload = {
            "service_name": SERVICE_NAME,
            "display_name": SERVICE_DISPLAY_NAME,
            "installed": False,
            "state": "missing",
        }
    else:
        payload = _query_service_status()
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"service_name: {payload['service_name']}")
        print(f"display_name: {payload['display_name']}")
        print(f"installed: {str(payload['installed']).lower()}")
        print(f"state: {payload['state']}")
        options = payload.get("options") or {}
        if options:
            for key in (
                "db_path",
                "config_path",
                "bind_host",
                "bind_port",
                "public_base_url",
                "secure_cookies",
                "trust_proxy_headers",
                "forwarded_allow_ips",
                "log_path",
            ):
                print(f"{key}: {options.get(key)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    command = str(args.command or "").strip().lower()
    if command == "install":
        return _handle_install(args)
    if command == "update":
        return _handle_update(args)
    if command == "start":
        return _handle_start(args)
    if command == "stop":
        return _handle_stop(args)
    if command == "restart":
        return _handle_restart(args)
    if command == "remove":
        return _handle_remove(args)
    if command == "status":
        return _handle_status(args)
    parser.error(f"unsupported command: {command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
