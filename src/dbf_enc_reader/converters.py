from decimal import Decimal
from typing import Any

class DataConverter:
    def smart_trim(self, value: Any) -> Any:
        """
        Trim spaces intelligently based on value type.
        
        Args:
            value: Value to trim
            
        Returns:
            Trimmed value maintaining its type
        """
        if isinstance(value, str):
            return value.strip()  # Trim both leading and trailing spaces
        elif isinstance(value, (int, float, Decimal)):
            return value  # Don't trim numbers
        elif value is None:
            return None
        return value

    def convert_value(self, value: Any) -> Any:
        """
        Convert a value from DBF to Python native type.
        
        Args:
            value: Value to convert
            
        Returns:
            Converted value
        """
        if value is None:
            return None
            
        # Handle .NET DateTime objects specifically to ensure consistent format
        if hasattr(value, 'ToString') and 'DateTime' in str(type(value)):
            # Convert .NET DateTime to consistent DD/MM/YYYY HH:MM:SS format
            try:
                original_value = str(value)
                # Use ToString with specific format to ensure consistency across systems
                formatted_date = value.ToString("dd/MM/yyyy HH:mm:ss")
                # print(f"[DBF DATE CONVERSION] Before: '{original_value}' -> After: '{formatted_date}'")
                return formatted_date
            except:
                # Fallback to default string conversion if formatting fails
                fallback_value = str(value)
                # print(f"[DBF DATE CONVERSION] Failed formatting, using fallback: '{fallback_value}'")
                return fallback_value
        
        # Handle other .NET types that aren't JSON serializable
        elif hasattr(value, 'ToString'):
            value = str(value)
            
        # Apply smart trimming after conversion
        return self.smart_trim(value)
