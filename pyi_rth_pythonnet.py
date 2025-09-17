import os
import sys

# Enhanced pythonnet runtime hook for 32-bit compatibility
try:
    import clr
    
    # Add common .NET Framework references for 32-bit systems
    clr.AddReference("System")
    clr.AddReference("System.Core")
    clr.AddReference("System.Data")
    
    # Try to add the Advantage Data Provider if available
    try:
        # Look for the DLL in the current directory first
        advantage_dll = os.path.join(os.path.dirname(sys.executable), "Advantage.Data.Provider.dll")
        if not os.path.exists(advantage_dll):
            # Fallback to script directory
            advantage_dll = os.path.join(os.path.dirname(__file__), "Advantage.Data.Provider.dll")
        
        if os.path.exists(advantage_dll):
            clr.AddReference(advantage_dll)
    except Exception as e:
        # Don't fail if Advantage DLL can't be loaded at startup
        pass
        
except ImportError:
    # pythonnet not available, skip initialization
    pass
