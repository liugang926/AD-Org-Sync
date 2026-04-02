@echo off
setlocal
chcp 65001 >nul

set ROOT=%~dp0
cd /d "%ROOT%"

if not exist "logs" mkdir logs

call "%ROOT%start_web.bat" %* >> "%ROOT%logs\web_console.log" 2>&1
