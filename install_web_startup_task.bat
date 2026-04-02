@echo off
setlocal
chcp 65001 >nul

set ROOT=%~dp0
cd /d "%ROOT%"

set TASK_NAME=WeComADSyncWeb
set TASK_CMD=%SystemRoot%\System32\cmd.exe /c ""%ROOT%run_web_background.bat"""

echo [INFO] Registering startup task %TASK_NAME% ...
schtasks /Create /F /SC ONSTART /RU SYSTEM /RL HIGHEST /TN "%TASK_NAME%" /TR "%TASK_CMD%"
if errorlevel 1 (
    echo [ERROR] Failed to register startup task.
    exit /b 1
)

echo [OK] Startup task created: %TASK_NAME%
echo [INFO] Use schtasks /Run /TN "%TASK_NAME%" to start it immediately.
