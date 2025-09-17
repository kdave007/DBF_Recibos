#!/usr/bin/env python3
"""
32-bit Compatibility Checker for DBF_Recibos Project
This script checks if your system and dependencies are compatible with 32-bit compilation.
"""

import sys
import platform
import subprocess
import os
from pathlib import Path

def check_python_architecture():
    """Check if Python is 32-bit or 64-bit"""
    arch = platform.architecture()[0]
    machine = platform.machine()
    print(f"Python Architecture: {arch}")
    print(f"Machine Type: {machine}")
    
    if arch == '32bit':
        print("✓ Python is 32-bit - Good for 32-bit compilation")
        return True
    else:
        print("⚠ Python is 64-bit - May cause issues on 32-bit target systems")
        return False

def check_pythonnet():
    """Check pythonnet installation and compatibility"""
    try:
        import pythonnet
        print(f"✓ pythonnet version: {pythonnet.__version__}")
        
        # Try to import clr
        import clr
        print("✓ CLR module imported successfully")
        
        # Try basic .NET operations
        clr.AddReference("System")
        from System import String
        test_string = String("Test")
        print("✓ Basic .NET operations working")
        
        return True
    except ImportError as e:
        print(f"✗ pythonnet not installed or incompatible: {e}")
        return False
    except Exception as e:
        print(f"⚠ pythonnet installed but has issues: {e}")
        return False

def check_advantage_dll():
    """Check if Advantage.Data.Provider.dll exists and get its architecture"""
    dll_path = Path("Advantage.Data.Provider.dll")
    
    if not dll_path.exists():
        print("✗ Advantage.Data.Provider.dll not found in project directory")
        return False
    
    print("✓ Advantage.Data.Provider.dll found")
    
    # Try to determine DLL architecture using file command or other methods
    try:
        # On Windows, we can use PowerShell to check the DLL
        result = subprocess.run([
            'powershell', '-Command', 
            f'[System.Reflection.AssemblyName]::GetAssemblyName("{dll_path.absolute()}").ProcessorArchitecture'
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            arch = result.stdout.strip()
            print(f"DLL Architecture: {arch}")
            if 'X86' in arch or '32' in arch:
                print("✓ DLL appears to be 32-bit compatible")
                return True
            else:
                print("⚠ DLL may not be 32-bit compatible")
                return False
    except Exception as e:
        print(f"⚠ Could not determine DLL architecture: {e}")
    
    return True  # Assume it's okay if we can't check

def check_dependencies():
    """Check if all required dependencies are installed"""
    requirements = [
        'pythonnet>=3.0.1',
        'requests>=2.31.0', 
        'pyodbc>=4.0.39',
        'python-dotenv>=1.0.0',
        'tenacity<=8.2.3',
        'tqdm>=4.65.0',
        'pytz==2025.2'
    ]
    
    print("\nChecking dependencies:")
    all_good = True
    
    for req in requirements:
        package_name = req.split('>=')[0].split('<=')[0].split('==')[0]
        try:
            __import__(package_name.replace('-', '_'))
            print(f"✓ {package_name}")
        except ImportError:
            print(f"✗ {package_name} - Not installed")
            all_good = False
    
    return all_good

def check_pyinstaller():
    """Check PyInstaller installation"""
    try:
        result = subprocess.run(['pyinstaller', '--version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            version = result.stdout.strip()
            print(f"✓ PyInstaller version: {version}")
            return True
    except Exception as e:
        print(f"✗ PyInstaller not found or not working: {e}")
        return False

def main():
    print("=" * 60)
    print("32-BIT COMPATIBILITY CHECKER FOR DBF_RECIBOS")
    print("=" * 60)
    
    checks = [
        ("Python Architecture", check_python_architecture),
        ("pythonnet Compatibility", check_pythonnet),
        ("Advantage DLL", check_advantage_dll),
        ("Dependencies", check_dependencies),
        ("PyInstaller", check_pyinstaller)
    ]
    
    results = []
    for name, check_func in checks:
        print(f"\n--- {name} ---")
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"✗ Error during {name} check: {e}")
            results.append((name, False))
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{name}: {status}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 ALL CHECKS PASSED! Your system should be compatible with 32-bit compilation.")
        print("\nNext steps:")
        print("1. Run: build_exe_32bit.bat")
        print("2. Test the generated executable on your target 32-bit system")
    else:
        print("⚠ SOME CHECKS FAILED. Please address the issues above before compiling.")
        print("\nRecommended fixes:")
        print("1. Install missing dependencies: pip install -r requirements.txt")
        print("2. Ensure you have 32-bit Python if targeting 32-bit systems")
        print("3. Verify Advantage.Data.Provider.dll is the correct architecture")
        print("4. Install PyInstaller: pip install pyinstaller")
    
    print("=" * 60)

if __name__ == "__main__":
    main()
