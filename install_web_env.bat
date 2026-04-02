@echo off
setlocal
chcp 65001 >nul

set ROOT=%~dp0
cd /d "%ROOT%"

if not exist "venv\Scripts\python.exe" (
    echo [INFO] Creating virtual environment...
    py -3 -m venv venv 2>nul
    if errorlevel 1 (
        python -m venv venv
        if errorlevel 1 (
            echo [ERROR] Failed to create virtual environment.
            exit /b 1
        )
    )
)

echo [INFO] Upgrading pip...
venv\Scripts\python.exe -m pip install --upgrade pip
if errorlevel 1 exit /b 1

echo [INFO] Installing project dependencies...
venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 exit /b 1

echo [OK] Web environment is ready.
