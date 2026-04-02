@echo off
chcp 65001 >nul
echo ========================================
echo   企业微信-AD同步工具 LDAPS依赖安装
echo ========================================
echo.

REM 检查是否在虚拟环境中
if defined VIRTUAL_ENV (
    echo ✅ 检测到虚拟环境: %VIRTUAL_ENV%
    echo.
) else (
    echo ⚠️  未检测到虚拟环境
    echo 正在激活虚拟环境...
    
    if exist "venv\Scripts\activate.bat" (
        call venv\Scripts\activate.bat
        echo ✅ 虚拟环境已激活
        echo.
    ) else (
        echo ❌ 找不到虚拟环境，请先创建：
        echo    python -m venv venv
        pause
        exit /b 1
    )
)

REM 升级pip
echo 📦 升级pip...
python -m pip install --upgrade pip
echo.

REM 安装ldap3
echo 📦 安装ldap3库...
pip install ldap3>=2.9.1
echo.

REM 安装其他依赖
echo 📦 检查并安装其他依赖...
pip install -r requirements.txt
echo.

echo ========================================
echo   ✅ 依赖安装完成！
echo ========================================
echo.
echo 下一步：
echo 1. 编辑 config.ini 文件，配置LDAP连接信息
echo 2. 确保域控已启用LDAPS（端口636）
echo 3. 运行测试脚本验证连接：python test_ldap.py
echo 4. 运行同步工具：python wecom_sync_ui.py
echo.

pause

