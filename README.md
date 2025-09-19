# DBF_Recibos

A Python application for processing DBF files and integrating with PostgreSQL databases and APIs.

## Compilation Instructions

This project supports compilation for both 32-bit and 64-bit systems using PyInstaller.

### For 32-bit Systems:

```bash
# 1. Check compatibility first
python check_32bit_compatibility.py

# 2. If all checks pass, compile
build_exe_32bit.bat
```

### For Windows 7 32-bit Systems:

```bash
# Use Windows 7 specific build (handles pythonnet compatibility issues)
build_exe_win7.bat
```

### For 64-bit Systems:

```bash
# Simply use the standard build script
build_exe.bat
```

## Architecture Requirements

- **Python Architecture Must Match Target System:**
  - For 32-bit executable: Use 32-bit Python 3.8.10 (recommended for 32-bit VMs)
  - For 64-bit executable: Use 64-bit Python
  - Check your Python: `python -c "import platform; print(platform.architecture())"`

- **DLL Architecture Must Match:**
  - Ensure `Advantage.Data.Provider.dll` matches your target architecture
  - 32-bit DLL for 32-bit systems, 64-bit DLL for 64-bit systems

## Important Notes

1. **Cross-compilation is NOT possible** - you must compile on the same architecture as your target system
2. **Dependencies must match** - all Python packages must be the correct architecture
3. **The compatibility checker** (`check_32bit_compatibility.py`) will identify any issues before compilation

## Dependencies

Install required packages:

```bash
pip install -r requirements.txt
```

Required packages:
- pythonnet>=3.0.1
- requests>=2.31.0
- pyodbc>=4.0.39
- python-dotenv>=1.0.0
- tenacity<=8.2.3
- tqdm>=4.65.0
- pytz==2025.2

## Configuration

1. Copy `.env.template` to `.env`
2. Update the configuration values for your environment:
   - `DBF_SOURCE_DIR`: Path to your DBF files
   - `PG_*`: PostgreSQL connection settings
   - `API_*`: API configuration
   - Other application settings as needed

## Usage

After compilation, the executable will be in the `dist` folder. Make sure to:

1. Place the `.env` file in the same directory as the executable
2. Ensure `Advantage.Data.Provider.dll` is in the same directory
3. Have .NET Framework 4.0 or higher installed on the target machine

## Troubleshooting

If you encounter compilation issues:

1. Run the compatibility checker: `python check_32bit_compatibility.py`
2. Install missing dependencies: `pip install python-dotenv`
3. For pythonnet issues on Python 3.8.10: `pip install "pythonnet>=3.0.1,<4.0.0"`
4. Ensure all dependencies are installed for the correct architecture
5. Verify the Advantage.Data.Provider.dll is the correct architecture (32-bit for 32-bit systems)
6. Check that pythonnet is compatible with your Python version

### Common 32-bit VM Issues:

- **Python 3.8.10 + pythonnet**: Use `pip install "pythonnet==3.0.3"` for better compatibility
- **Missing python-dotenv**: Install with `pip install python-dotenv>=1.0.0`
- **DLL Architecture**: Ensure you have the 32-bit version of Advantage.Data.Provider.dll

### Windows 7 Specific Issues:

- **pythonnet Runtime Error**: Use `build_exe_win7.bat` instead of regular build
- **Missing .NET Framework**: Install .NET Framework 4.0 or higher on target machine
- **Visual C++ Redistributable**: May need Visual C++ Redistributable 2015-2019
- **pythonnet Version**: For Windows 7, use `pip install "pythonnet==3.0.1"`
