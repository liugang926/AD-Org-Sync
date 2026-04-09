@echo off
setlocal
chcp 65001 >nul

set ROOT=%~dp0
cd /d "%ROOT%"

set VENV_DIR=.venv
if exist "venv\Scripts\python.exe" if not exist ".venv\Scripts\python.exe" set VENV_DIR=venv

if not exist "%VENV_DIR%\Scripts\python.exe" (
    echo [ERROR] %VENV_DIR%\Scripts\python.exe not found. Run install_web_env.bat first.
    exit /b 1
)

if not exist "logs" mkdir logs

echo [INFO] Starting AD Org Sync web control plane...
"%VENV_DIR%\Scripts\python.exe" -m sync_app.cli web --config "%ROOT%config.ini" %*
