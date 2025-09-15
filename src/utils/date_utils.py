from datetime import datetime, date
import logging
from typing import Optional, Union

logger = logging.getLogger(__name__)

def parse_fecha(fecha_str: Optional[str]) -> date:
    """
    Parse a date string that could be in multiple formats and return a date object.
    Handles both 24-hour and 12-hour formats, with or without AM/PM indicators.
    
    Args:
        fecha_str: The date string to parse (can be None)
        
    Returns:
        date: The parsed date in UTC, or current date if parsing fails
    """
    if not fecha_str:
        logger.warning("Empty date string provided, using current date")
        return date.today()
    
    # print(f"[DEBUG] Original date string: '{fecha_str}'")
    
    # Normalize Spanish AM/PM indicators to English
    normalized_str = (
        str(fecha_str)
        .replace(' a. m.', ' AM')
        .replace(' p. m.', ' PM')
        .strip()
    )
    
    # print(f"[DEBUG] Normalized date string: '{normalized_str}'")
    
    # List of possible date formats to try (European DD/MM format first since DBF uses DD-MM-YYYY)
    date_formats = [
        # 12-hour formats with AM/PM - European DD/MM format first
        '%d/%m/%Y %I:%M:%S %p',  # European format with 12-hour time (6/7/2025 12:00:00 AM = 6th July)
        '%m/%d/%Y %I:%M:%S %p',  # US format with 12-hour time (7/6/2025 12:00:00 AM = 6th July)
        # 24-hour formats - European DD/MM format first  
        '%d/%m/%Y %H:%M:%S',     # European 24-hour format (06/07/2025 14:30:00 = 6th July)
        '%m/%d/%Y %H:%M:%S',     # US 24-hour format (07/06/2025 14:30:00 = 6th July)
        # Date only formats - European DD/MM format first
        '%d/%m/%Y',              # European date only (06/07/2025 = 6th July)
        '%m/%d/%Y',              # US date only (07/06/2025 = 6th July)
        # ISO and hyphen formats
        '%d-%m-%Y',              # European with hyphens (21-08-2025)
        '%Y-%m-%d',              # ISO format (2025-07-06)
    ]
    
    for fmt in date_formats:
        try:
            # print(f"[DEBUG] Trying format: '{fmt}'")
            # Try parsing with the current format
            parsed_date = datetime.strptime(normalized_str, fmt).date()
            # print(f"[DEBUG] SUCCESS! Parsed '{fecha_str}' as {parsed_date} using format '{fmt}'")
            return parsed_date
        except ValueError as e:
            # print(f"[DEBUG] Format '{fmt}' failed: {e}")
            continue
    
    # If we get here, none of the formats worked
    logger.warning(f"Could not parse date: '{fecha_str}'. Using current date instead.")
    return date.today()

def format_fecha_iso(fecha: Union[str, date, datetime, None]) -> str:
    """
    Format a date object or string to ISO format (YYYY-MM-DD).
    
    Args:
        fecha: Date as string, date, or datetime object
        
    Returns:
        str: Date in ISO format (YYYY-MM-DD), or empty string if input is None/empty
    """
    if not fecha:
        return ""
        
    if isinstance(fecha, str):
        try:
            # Try to parse the string first
            fecha = parse_fecha(fecha)
        except (ValueError, TypeError):
            return ""
    
    if isinstance(fecha, datetime):
        return fecha.date().isoformat()
    elif isinstance(fecha, date):
        return fecha.isoformat()
    
    return ""
