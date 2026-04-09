# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_data_files
from PyInstaller.utils.hooks import collect_submodules

datas = []
hiddenimports = ['ldap3.core.exceptions', 'ldap3.protocol.formatters.formatters', 'ldap3.strategy.sync', 'ldap3.strategy.asynchronous', 'ldap3.strategy.asyncStream', 'ldap3.utils.conv', 'PyQt5.QtCore', 'PyQt5.QtGui', 'PyQt5.QtWidgets', 'PyQt5.sip']
datas += collect_data_files('certifi')
datas += collect_data_files('qtawesome')
hiddenimports += collect_submodules('certifi')
hiddenimports += collect_submodules('charset_normalizer')
hiddenimports += collect_submodules('urllib3')
hiddenimports += collect_submodules('idna')
hiddenimports += collect_submodules('requests')
hiddenimports += collect_submodules('qtawesome')
hiddenimports += collect_submodules('sync_app')


a = Analysis(
    ['wecom_sync_ui.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['setuptools', 'pip', 'wheel', 'jaraco', 'pkg_resources', 'numpy', 'pandas', 'matplotlib'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='AD-Org-Sync',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['icon.ico'],
)
