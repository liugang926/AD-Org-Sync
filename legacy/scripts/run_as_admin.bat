@echo off
:: 以管理员权限运行构建脚本

echo 正在请求管理员权限...

:: 使用PowerShell以管理员权限启动构建脚本
powershell -Command "Start-Process cmd -ArgumentList '/c cd /d \"%~dp0\" && call build_exe.bat %*' -Verb RunAs"

exit /b 0 