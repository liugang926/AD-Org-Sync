@echo off
setlocal
chcp 65001 >nul

set TASK_NAME=WeComADSyncWeb

echo [INFO] Removing startup task %TASK_NAME% ...
schtasks /Delete /F /TN "%TASK_NAME%"
if errorlevel 1 (
    echo [ERROR] Failed to remove startup task or task does not exist.
    exit /b 1
)

echo [OK] Startup task removed: %TASK_NAME%
