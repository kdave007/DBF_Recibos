from dataclasses import dataclass
from pathlib import Path
import os
import logging
from dotenv import load_dotenv
from src.utils.get_enc import EncEnv

class DBFConfig:
    """Configuration for DBF connection and reading."""
    
    def __init__(self, dll_path=None, encryption_password=None, source_directory=None, limit_rows=None):
        # Store only the override values provided directly
        self._dll_path_override = dll_path
        self._encryption_password_override = encryption_password
        self._source_directory_override = source_directory
        self.limit_rows = limit_rows
        
        # Load environment once to validate required fields exist
        load_dotenv()
        env = EncEnv()
        
        # Check if required fields exist in .env
        dll_path_value = dll_path or env.get('DBF_DLL_PATH')
        encryption_password_value = encryption_password or env.get('DBF_ENCRYPTION_PASSWORD')
        source_directory_value = source_directory or env.get('DBF_SOURCE_DIR')
        
        # Validate required fields
        if not dll_path_value:
            raise ValueError("dll_path is required. Set it directly or via DBF_DLL_PATH in .env")
        if not encryption_password_value:
            raise ValueError("encryption_password is required. Set it directly or via DBF_ENCRYPTION_PASSWORD in .env")
        if not source_directory_value:
            raise ValueError("source_directory is required. Set it directly or via DBF_SOURCE_DIR in .env")
            
        logging.info("DBFConfig initialized - will use values directly from .env file")
    
    @property
    def dll_path(self):
        """Get the DLL path directly from .env each time"""
        env = EncEnv()
        path = self._dll_path_override or env.get('DBF_DLL_PATH')
        print(f"[DBFConfig] Using DLL path: {path}")
        logging.info(f"[DBFConfig] Using DLL path: {path}")
        return path
        
    @property
    def encryption_password(self):
        """Get the encryption password directly from .env each time"""
        env = EncEnv()
        password = self._encryption_password_override or env.get('DBF_ENCRYPTION_PASSWORD')
        # Don't print the actual password for security reasons
        print(f"[DBFConfig] Using encryption password from .env")
        logging.info(f"[DBFConfig] Using encryption password from .env")
        return password
        
    @property
    def source_directory(self):
        """Get the source directory directly from .env each time"""
        env = EncEnv()
        path = self._source_directory_override or env.get('DBF_SOURCE_DIR')
        print(f"[DBFConfig] Using source directory: {path}")
        logging.info(f"[DBFConfig] Using source directory: {path}")
        return path
        
    def get_table_path(self, table_name: str) -> str:
        """Get the full path for a DBF table.
        
        Args:
            table_name: Name of the DBF table/file
            
        Returns:
            Full path to the DBF file
        """
        # Get the source directory (this will print it via the property)
        source_dir = self.source_directory
        
        # Join the paths without resolving
        full_path = os.path.join(source_dir, table_name)
        
        # Print the full path and check if it exists
        print(f"[DBFConfig] Table path for {table_name}: {full_path}")
        print(f"[DBFConfig] Table file exists: {os.path.exists(full_path)}")
        logging.info(f"[DBFConfig] Table path for {table_name}: {full_path}")
        logging.info(f"[DBFConfig] Table file exists: {os.path.exists(full_path)}")
        
        return full_path
