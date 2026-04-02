@echo off
setlocal
chcp 65001 >nul

set ROOT=%~dp0
cd /d "%ROOT%"

set DIST_DIR=dist\web-release

if exist "%DIST_DIR%" rmdir /s /q "%DIST_DIR%"
mkdir "%DIST_DIR%"
mkdir "%DIST_DIR%\sync_app"

xcopy /E /I /Y "sync_app" "%DIST_DIR%\sync_app" >nul
copy /Y "README.md" "%DIST_DIR%\README.md" >nul
copy /Y "LICENSE" "%DIST_DIR%\LICENSE" >nul
copy /Y "SECURITY.md" "%DIST_DIR%\SECURITY.md" >nul
copy /Y "CONTRIBUTING.md" "%DIST_DIR%\CONTRIBUTING.md" >nul
copy /Y "requirements.txt" "%DIST_DIR%\requirements.txt" >nul
copy /Y "pyproject.toml" "%DIST_DIR%\pyproject.toml" >nul
copy /Y "config.example.ini" "%DIST_DIR%\config.example.ini" >nul
copy /Y "start_web.bat" "%DIST_DIR%\start_web.bat" >nul
copy /Y "run_web_background.bat" "%DIST_DIR%\run_web_background.bat" >nul
copy /Y "install_web_env.bat" "%DIST_DIR%\install_web_env.bat" >nul
copy /Y "install_web_startup_task.bat" "%DIST_DIR%\install_web_startup_task.bat" >nul
copy /Y "remove_web_startup_task.bat" "%DIST_DIR%\remove_web_startup_task.bat" >nul

if not exist "%DIST_DIR%\logs" mkdir "%DIST_DIR%\logs"
if not exist "%DIST_DIR%\.appdata" mkdir "%DIST_DIR%\.appdata"

echo [INFO] Web release assembled at %DIST_DIR%
echo [INFO] Next steps:
echo         1. Copy config.example.ini to config.ini
echo         2. Run install_web_env.bat
echo         3. Run start_web.bat
