@echo off
echo Building DBF_Recibos executable for 32-bit systems...

rem Check if DLL exists
if not exist "Advantage.Data.Provider.dll" (
  echo ERROR: Advantage.Data.Provider.dll not found in the project directory.
  echo Please copy the 32-bit version of the DLL to the project root directory before building.
  echo Make sure you have the 32-bit version of Advantage.Data.Provider.dll
  exit /b 1
)

rem Check Python architecture
python -c "import platform; print('Python architecture:', platform.architecture()[0])"
if %ERRORLEVEL% NEQ 0 (
  echo ERROR: Could not determine Python architecture
  exit /b 1
)

rem Clean previous build artifacts
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist

rem Check if pythonnet is installed and compatible
python -c "import pythonnet; print('pythonnet version:', pythonnet.__version__)" 2>nul
if %ERRORLEVEL% NEQ 0 (
  echo WARNING: pythonnet not found or not compatible
  echo Installing compatible pythonnet version for 32-bit...
  pip install "pythonnet>=3.0.1,<4.0.0"
)

rem Use the existing spec file for better control
echo Running PyInstaller with 32-bit optimizations...
pyinstaller --noconfirm --clean DBF_Recibos.spec

echo.
if %ERRORLEVEL% EQU 0 (
  echo Creating logs directory in the distribution folder...
  mkdir dist\logs
  
  echo Creating .env.template file...
  (
    echo # DBF Configuration - 32-bit System
    echo # Use the path to the 32-bit DLL on the target machine
    echo DBF_DLL_PATH=Advantage.Data.Provider.dll
    echo DBF_ENCRYPTION_PASSWORD=X3WGTXG5QJZ6K9ZC4VO2
    echo # Set this to the path of your DBF files on the target machine
    echo DBF_SOURCE_DIR=C:\path\to\your\dbf\files
    echo # Set this to a writable location for logs
    echo LOG_PATH=logs
    echo.
    echo # PostgreSQL Configuration
    echo PG_DATABASE=py_process
    echo PG_USER=postgres
    echo PG_PASSWORD=comexcare
    echo PG_HOST=localhost
    echo PG_PORT=5432
    echo.
    echo # Application Settings
    echo ENCRYPTED=False
    echo STOP_SCRIPT=False
    echo DEBUG_MODE=True
    echo SQL_ENABLED=True
    echo INTERNET_CHECK=True
    echo.
    echo # API Configuration
    echo API_BASE_URL=https://c8.velneo.com:17262/api/vLatamERP_db_dat/v2/_process/PRO_CICLO_VTA_FAC
    echo API_GET_URL=https://c8.velneo.com:17262/api/vLatamERP_db_dat/v2/_process/PRO_VTA_FAC_JSON
    echo API_KEY=654321
    echo.
    echo # Date Settings
    echo SPECIFIC_DATE=True
    echo START=16/052025
    echo END=16/052025
    echo.
    echo # Store Information
    echo CLAVE_SUCURSAL=ARAUC
    echo CLAVE_PLAZA=XALAP
    echo CLIENT_ID=2
  ) > dist\.env.template
  
  echo Copying 32-bit DLL to distribution folder...
  copy Advantage.Data.Provider.dll dist\
  
  echo.
  echo ========================================
  echo 32-BIT BUILD COMPLETED SUCCESSFULLY!
  echo ========================================
  echo Executable is in the 'dist' folder.
  echo.
  echo IMPORTANT NOTES FOR 32-BIT SYSTEMS:
  echo 1. The executable requires a .env file in the SAME DIRECTORY
  echo 2. Rename .env.template to .env and update the paths for your machine
  echo 3. Especially update DBF_SOURCE_DIR to point to your DBF files location
  echo 4. Make sure Advantage.Data.Provider.dll is the 32-bit version
  echo 5. Target machine must have .NET Framework 4.0 or higher installed
  echo.
) else (
  echo Build failed with error code %ERRORLEVEL%
  echo.
  echo TROUBLESHOOTING FOR 32-BIT SYSTEMS:
  echo 1. Ensure you're using 32-bit Python
  echo 2. Verify Advantage.Data.Provider.dll is 32-bit version
  echo 3. Check that pythonnet is compatible with your Python version
  echo 4. Try: pip install --force-reinstall "pythonnet>=3.0.1,<4.0.0"
)
pause
