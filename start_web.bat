@echo off
setlocal
chcp 65001 >nul

set ROOT=%~dp0
cd /d "%ROOT%"

if not exist "venv\Scripts\python.exe" (
    echo [ERROR] venv\Scripts\python.exe not found. Run install_web_env.bat first.
    exit /b 1
)

if not exist "logs" mkdir logs

echo [INFO] Starting web control plane...
venv\Scripts\python.exe -m sync_app.cli web --config "%ROOT%config.ini" %*
