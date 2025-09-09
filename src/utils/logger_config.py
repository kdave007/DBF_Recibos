import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from src.utils.get_enc import EncEnv

def setup_logging(log_level=logging.INFO):
    """
    Configure logging to output to both console and file.
    
    Args:
        log_level: The logging level (default: logging.INFO)
    """
    # Load environment variables if not already loaded
    from dotenv import load_dotenv
    load_dotenv()
    env = EncEnv()
    
    # Try multiple possible locations for logs
    logs_dir = None
    possible_log_dirs = []
    
    # First try from environment variable
    env_log_path = env.get('LOG_PATH')
    if env_log_path:
        possible_log_dirs.append(env_log_path)
    
    # If running as PyInstaller executable
    if getattr(sys, 'frozen', False):
        # Try next to the executable
        exe_dir = os.path.dirname(sys.executable)
        possible_log_dirs.append(os.path.join(exe_dir, 'logs'))
    
    # Try in current directory
    possible_log_dirs.append(os.path.join(os.getcwd(), 'logs'))
    
    # Try in user's documents folder
    possible_log_dirs.append(os.path.join(os.path.expanduser('~'), 'Documents', 'DBF_Recibos_Logs'))
    
    # Try in temp directory as last resort
    possible_log_dirs.append(os.path.join(os.environ.get('TEMP', os.path.join(os.path.expanduser('~'), 'temp')), 'DBF_Recibos_Logs'))
    
    # Try each location until one works
    for log_dir in possible_log_dirs:
        try:
            os.makedirs(log_dir, exist_ok=True)
            # If we get here, we successfully created or accessed the directory
            logs_dir = log_dir
            break
        except (PermissionError, OSError):
            continue
    
    # If all attempts failed, use a fallback approach with just console logging
    if not logs_dir:
        print("WARNING: Could not create log directory in any location. Using console logging only.")
        # Set a dummy path that won't be used (we'll skip file logging)
        logs_dir = os.getcwd()
    
    # Generate log filename with date only (one file per day)
    date_today = datetime.now().strftime('%Y%m%d')
    log_file = os.path.join(logs_dir, f'dbf_recibos_{date_today}.log')
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    
    # Create file handler (with error handling)
    try:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        
        # Add handlers to root logger
        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)
        
    except (PermissionError, OSError) as e:
        # If we can't create the file handler, just use console logging
        root_logger.addHandler(console_handler)
        logging.warning(f"Could not create log file at {log_file}: {str(e)}")
        logging.warning("Using console logging only.")
        log_file = None
    
    return log_file
