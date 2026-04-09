@echo off
setlocal
chcp 65001 >nul

set ROOT=%~dp0
cd /d "%ROOT%"

echo [WARN] Scheduled task deployment is deprecated.
echo [INFO] Removing the Windows service instead...
powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%uninstall_web_service.ps1" %*
exit /b %errorlevel%
