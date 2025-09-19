# -*- mode: python ; coding: utf-8 -*-
import os
import sys

# Handle pythonnet DLLs for 32-bit compatibility
pythonnet_dlls = []
try:
    import pythonnet
    import clr
    # Try to get pythonnet DLLs if available
    pythonnet_path = os.path.dirname(pythonnet.__file__)
    for dll_name in ['Python.Runtime.dll', 'clr.dll']:
        dll_path = os.path.join(pythonnet_path, dll_name)
        if os.path.exists(dll_path):
            pythonnet_dlls.append((dll_path, '.'))
except ImportError:
    print("Warning: pythonnet not found, continuing without pythonnet DLLs")

a = Analysis(
    ['tests\\test_find_matches_simple.py'],
    pathex=['.'],
    binaries=pythonnet_dlls + [('Advantage.Data.Provider.dll', '.')],
    datas=[('mappings.json', '.'), ('Advantage.Data.Provider.dll', '.')],
    hiddenimports=['src', 'src.config', 'src.controllers', 'src.db', 'src.utils', 'src.dbf_enc_reader', 'clr', 'pythonnet', 'dotenv', 'System'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=['pyi_rth_pythonnet.py'],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='DBF_Recibos',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
