@echo off
setlocal
chcp 65001 >nul

set ROOT=%~dp0
cd /d "%ROOT%"

set VENV_PYTHON=%ROOT%.venv\Scripts\python.exe
if not exist "%VENV_PYTHON%" set VENV_PYTHON=%ROOT%venv\Scripts\python.exe
if not exist "%VENV_PYTHON%" (
    echo [ERROR] Python virtual environment not found. Run install_web_env.bat first.
    exit /b 1
)

set VERSION_FILE=%TEMP%\ad_org_sync_version.txt
"%VENV_PYTHON%" -c "import pathlib; import sync_app.core.common as c; pathlib.Path(r'%VERSION_FILE%').write_text(c.APP_VERSION, encoding='utf-8')"
if errorlevel 1 exit /b 1
set /p APP_VERSION=<"%VERSION_FILE%"
del /f /q "%VERSION_FILE%" >nul 2>&1
if "%APP_VERSION%"=="" (
    echo [ERROR] Failed to resolve APP_VERSION.
    exit /b 1
)
set DIST_ROOT=%ROOT%dist
set RELEASE_DIR=%DIST_ROOT%\ad-org-sync-web-%APP_VERSION%
set ZIP_PATH=%DIST_ROOT%\ad-org-sync-web-%APP_VERSION%.zip
set WHEEL_DIR=%DIST_ROOT%\packages

if exist "%RELEASE_DIR%" rmdir /s /q "%RELEASE_DIR%"
if exist "%ZIP_PATH%" del /f /q "%ZIP_PATH%"
if exist "%WHEEL_DIR%" rmdir /s /q "%WHEEL_DIR%"

echo [INFO] Installing build dependencies...
"%VENV_PYTHON%" -m pip install -r requirements-build.txt
if errorlevel 1 exit /b 1

echo [INFO] Building wheel...
"%VENV_PYTHON%" -m build --wheel --outdir "%WHEEL_DIR%"
if errorlevel 1 exit /b 1

mkdir "%RELEASE_DIR%"
mkdir "%RELEASE_DIR%\sync_app"
mkdir "%RELEASE_DIR%\packages"

xcopy /E /I /Y "sync_app" "%RELEASE_DIR%\sync_app" >nul
xcopy /E /I /Y "%WHEEL_DIR%" "%RELEASE_DIR%\packages" >nul
copy /Y "README.md" "%RELEASE_DIR%\README.md" >nul
copy /Y "LICENSE" "%RELEASE_DIR%\LICENSE" >nul
copy /Y "SECURITY.md" "%RELEASE_DIR%\SECURITY.md" >nul
copy /Y "CONTRIBUTING.md" "%RELEASE_DIR%\CONTRIBUTING.md" >nul
copy /Y "pyproject.toml" "%RELEASE_DIR%\pyproject.toml" >nul
copy /Y "config.example.ini" "%RELEASE_DIR%\config.example.ini" >nul
copy /Y "requirements.txt" "%RELEASE_DIR%\requirements.txt" >nul
copy /Y "requirements-web.txt" "%RELEASE_DIR%\requirements-web.txt" >nul
copy /Y "requirements-deploy.txt" "%RELEASE_DIR%\requirements-deploy.txt" >nul
copy /Y "requirements-desktop.txt" "%RELEASE_DIR%\requirements-desktop.txt" >nul
copy /Y "requirements-test.txt" "%RELEASE_DIR%\requirements-test.txt" >nul
copy /Y "requirements-build.txt" "%RELEASE_DIR%\requirements-build.txt" >nul
copy /Y "start_web.bat" "%RELEASE_DIR%\start_web.bat" >nul
copy /Y "run_web_background.bat" "%RELEASE_DIR%\run_web_background.bat" >nul
copy /Y "install_web_env.bat" "%RELEASE_DIR%\install_web_env.bat" >nul
copy /Y "install_web_service.ps1" "%RELEASE_DIR%\install_web_service.ps1" >nul
copy /Y "upgrade_web_service.ps1" "%RELEASE_DIR%\upgrade_web_service.ps1" >nul
copy /Y "manage_web_service.ps1" "%RELEASE_DIR%\manage_web_service.ps1" >nul
copy /Y "uninstall_web_service.ps1" "%RELEASE_DIR%\uninstall_web_service.ps1" >nul
copy /Y "install_web_startup_task.bat" "%RELEASE_DIR%\install_web_startup_task.bat" >nul
copy /Y "remove_web_startup_task.bat" "%RELEASE_DIR%\remove_web_startup_task.bat" >nul
if not exist "%RELEASE_DIR%\docs" mkdir "%RELEASE_DIR%\docs"
copy /Y "docs\deployment-windows-service.md" "%RELEASE_DIR%\docs\deployment-windows-service.md" >nul

if not exist "%RELEASE_DIR%\logs" mkdir "%RELEASE_DIR%\logs"
if not exist "%RELEASE_DIR%\.appdata" mkdir "%RELEASE_DIR%\.appdata"

echo [INFO] Creating release zip...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Compress-Archive -Path '%RELEASE_DIR%\\*' -DestinationPath '%ZIP_PATH%' -Force"
if errorlevel 1 exit /b 1

echo [OK] Web release assembled at %RELEASE_DIR%
echo [OK] Release zip created at %ZIP_PATH%
echo [INFO] Suggested next step:
echo         powershell -ExecutionPolicy Bypass -File .\install_web_service.ps1 -AdminUsername admin -AdminPassword "simple88"
