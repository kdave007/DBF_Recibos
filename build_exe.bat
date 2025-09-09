@echo off
echo Building DBF_Recibos executable...

rem Check if DLL exists
if not exist "Advantage.Data.Provider.dll" (
  echo ERROR: Advantage.Data.Provider.dll not found in the project directory.
  echo Please copy the DLL to the project root directory before building.
  exit /b 1
)

rem Clean previous build artifacts
rmdir /s /q build dist
del /q *.spec

rem Run PyInstaller with all required options
pyinstaller --noconfirm ^
  --onefile ^
  --name "DBF_Recibos" ^
  --add-data "mappings.json;." ^
  --add-data "tests\art_m.json;tests" ^
  --add-data "Advantage.Data.Provider.dll;." ^
  --hidden-import=src ^
  --hidden-import=src.config ^
  --hidden-import=src.controllers ^
  --hidden-import=src.db ^
  --hidden-import=src.utils ^
  --hidden-import=src.dbf_enc_reader ^
  --hidden-import=clr ^
  --hidden-import=pythonnet ^
  --hidden-import=dotenv ^
  --paths="." ^
  tests\test_find_matches_simple.py

echo.
if %ERRORLEVEL% EQU 0 (
  echo Creating logs directory in the distribution folder...
  mkdir dist\logs
  
  echo Creating .env.template file...
  (
    echo # DBF Configuration
    echo # Use the path to the DLL on the target machine
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
  
  echo Copying DLL to distribution folder...
  copy Advantage.Data.Provider.dll dist\
  
  echo Build completed successfully! Executable is in the 'dist' folder.
  echo.
  echo IMPORTANT: The executable requires a .env file in the SAME DIRECTORY.
  echo Rename .env.template to .env and update the paths for your machine.
  echo Especially update DBF_SOURCE_DIR to point to your DBF files location.
) else (
  echo Build failed with error code %ERRORLEVEL%
)
pause
