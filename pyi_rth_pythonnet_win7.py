import os
import sys

# Windows 7 compatible pythonnet runtime hook
try:
    # Set environment variables for Windows 7 compatibility
    os.environ['PYTHONNET_PYDLL'] = sys.executable.replace('python.exe', 'python38.dll')
    
    # Initialize pythonnet with Windows 7 specific settings
    import clr
    
    # Use more conservative .NET Framework loading for Windows 7
    try:
        # Try .NET Framework 4.0 first (most compatible with Windows 7)
        clr.AddReference("System")
        clr.AddReference("System.Core")
        
        # Only add System.Data if available
        try:
            clr.AddReference("System.Data")
        except:
            pass
            
    except Exception as e:
        # Fallback for minimal .NET Framework support
        try:
            clr.AddReference("mscorlib")
        except:
            pass
    
    # Try to load Advantage DLL with better error handling
    try:
        advantage_dll_paths = [
            os.path.join(os.path.dirname(sys.executable), "Advantage.Data.Provider.dll"),
            os.path.join(os.path.dirname(__file__), "Advantage.Data.Provider.dll"),
            "Advantage.Data.Provider.dll"
        ]
        
        for dll_path in advantage_dll_paths:
            if os.path.exists(dll_path):
                try:
                    clr.AddReference(dll_path)
                    break
                except Exception:
                    continue
                    
    except Exception:
        # Don't fail if Advantage DLL can't be loaded at startup
        pass
        
except ImportError:
    # pythonnet not available, continue without it
    pass
except Exception as e:
    # Log the error but don't fail the application startup
    try:
        with open('pythonnet_error.log', 'w') as f:
            f.write(f"pythonnet initialization error: {str(e)}\n")
            f.write(f"Python version: {sys.version}\n")
            f.write(f"Platform: {sys.platform}\n")
    except:
        pass
