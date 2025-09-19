import os
import sys

# Enhanced pythonnet runtime hook with Windows 7 compatibility
try:
    import clr
    
    # Conservative .NET Framework loading for Windows 7 compatibility
    try:
        clr.AddReference("System")
        clr.AddReference("System.Core")
    except:
        # Fallback for Windows 7 - try minimal references
        try:
            clr.AddReference("mscorlib")
        except:
            pass
    
    # Optional references that may not exist on older systems
    try:
        clr.AddReference("System.Data")
    except:
        pass
    
    # Try to add the Advantage Data Provider if available
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
                except:
                    continue
    except:
        pass
        
except ImportError:
    # pythonnet not available, skip initialization
    pass
except Exception as e:
    # Log errors for debugging but don't crash
    try:
        with open('pythonnet_error.log', 'w') as f:
            f.write(f"pythonnet error: {str(e)}\n")
    except:
        pass
