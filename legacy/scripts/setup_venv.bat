@echo off
chcp 65001 >nul
echo ========================================
echo 创建Python虚拟环境
echo ========================================
echo.

echo [1/4] 检查Python版本...
python --version
if %errorlevel% neq 0 (
    echo 错误：未找到Python，请先安装Python
    pause
    exit /b 1
)
echo.

echo [2/4] 创建虚拟环境...
if exist "venv" (
    echo 虚拟环境已存在，删除旧环境...
    rmdir /s /q venv
)
python -m venv venv
echo 虚拟环境创建成功 ✓
echo.

echo [3/4] 激活虚拟环境并安装依赖...
call venv\Scripts\activate.bat
echo.

echo 升级pip...
python -m pip install --upgrade pip
echo.

echo 安装必要的依赖包...
pip install PyQt5==5.15.9
pip install ldap3>=2.9.1
pip install requests>=2.28.0
pip install pyinstaller>=6.0.0
pip install Pillow>=10.0.0
echo.

echo [4/4] 验证安装...
pip list
echo.

echo ========================================
echo 虚拟环境设置完成！
echo ========================================
echo.
echo 虚拟环境位置: %CD%\venv
echo.
echo 下一步操作：
echo   1. 运行: build_in_venv.bat
echo   2. 这将在虚拟环境中打包程序
echo.
echo 提示：
echo   - 虚拟环境中只包含必要的依赖
echo   - 打包速度更快
echo   - exe文件更小
echo   - 避免依赖冲突
echo.
pause

