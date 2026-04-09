@echo off
setlocal
chcp 65001 >nul

set ROOT=%~dp0
cd /d "%ROOT%"

set VENV_DIR=.venv
if exist "venv\Scripts\python.exe" if not exist ".venv\Scripts\python.exe" set VENV_DIR=venv

if not exist "%VENV_DIR%\Scripts\python.exe" (
    echo [INFO] Creating virtual environment...
    py -3 -m venv "%VENV_DIR%" 2>nul
    if errorlevel 1 (
        python -m venv "%VENV_DIR%"
        if errorlevel 1 (
            echo [ERROR] Failed to create virtual environment.
            exit /b 1
        )
    )
)

echo [INFO] Upgrading pip...
"%VENV_DIR%\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 exit /b 1

echo [INFO] Installing web deployment dependencies...
"%VENV_DIR%\Scripts\python.exe" -m pip install -r requirements-deploy.txt
if errorlevel 1 exit /b 1

echo [OK] Web environment is ready.
