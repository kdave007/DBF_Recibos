import clr
import os
import sys
import logging
from pathlib import Path
from typing import Optional, List
from src.utils.get_enc import EncEnv

class DBFConnection:
    _dll_loaded = False

    @classmethod
    def set_dll_path(cls, path: str) -> None:
        """Set the path to Advantage Data Provider DLL.
        
        Args:
            path: Full path to Advantage.Data.Provider.dll
        """
        # Try multiple possible locations for the DLL
        dll_paths_to_try = []
        
        # First try the provided path
        dll_paths_to_try.append(path)
        
        # Get just the filename
        dll_filename = os.path.basename(path)
        
        # If running as PyInstaller executable
        if getattr(sys, 'frozen', False):
            # Try in the executable directory
            exe_dir = os.path.dirname(sys.executable)
            dll_paths_to_try.append(os.path.join(exe_dir, dll_filename))
            
            # Try in the PyInstaller temp directory if available
            if hasattr(sys, '_MEIPASS'):
                dll_paths_to_try.append(os.path.join(sys._MEIPASS, dll_filename))
        
        # Try in the current directory
        dll_paths_to_try.append(os.path.join(os.getcwd(), dll_filename))
        
        # Log all paths we're going to try
        # logging.info(f"Attempting to load DLL from multiple locations:")
        # for p in dll_paths_to_try:
        #     logging.info(f"  - {p} (exists: {os.path.exists(p)})")
        
        # Try each path until one works
        errors = []
        for dll_path in dll_paths_to_try:
            try:
                if os.path.exists(dll_path):
                    # logging.info(f"Trying to load DLL from: {dll_path}")
                    clr.AddReference(dll_path)
                    cls._dll_loaded = True
                    # logging.info(f"Successfully loaded DLL from: {dll_path}")
                    return
                else:
                    errors.append(f"Path does not exist: {dll_path}")
            except Exception as e:
                errors.append(f"Failed to load from {dll_path}: {str(e)}")
        
        # If we get here, all attempts failed
        error_msg = "\n".join(errors)
        logging.error(f"Failed to load Advantage DLL from any location:\n{error_msg}")
        raise RuntimeError(f"Failed to load Advantage DLL from any location:\n{error_msg}")

    @classmethod
    def _check_dll_loaded(cls) -> None:
        """Check if DLL is loaded before attempting connection."""
        if not cls._dll_loaded:
            raise RuntimeError(
                "Advantage DLL path not set. Call DBFConnection.set_dll_path() first with the path to Advantage.Data.Provider.dll"
            )

    def __init__(self, data_source: str, encryption_password: str):
        """
        Initialize DBF connection.
        
        Args:
            data_source: Path to the DBF file
            encryption_password: Password for encrypted DBF
        """
        # Use the data source path directly without resolving it
        self.data_source = data_source
        # logging.info(f"Using data source path: {self.data_source}")
        
        env = EncEnv()
        # Check if encryption is enabled via environment variable
        encrypted = env.get('ENCRYPTED', 'True').lower() == 'true'
        
        # Check if the data source path exists
        if not os.path.exists(self.data_source):
            logging.warning(f"Data source path does not exist: {self.data_source}")
            logging.warning("Trying to find an alternative path...")
            
            # Try to find an alternative path
            alt_paths = []
            
            # Try in the executable directory
            if getattr(sys, 'frozen', False):
                exe_dir = os.path.dirname(sys.executable)
                alt_paths.append(os.path.join(exe_dir, os.path.basename(self.data_source)))
                
                # Try in a 'data' subdirectory
                alt_paths.append(os.path.join(exe_dir, 'data'))
            
            # Try in the current directory
            alt_paths.append(os.path.join(os.getcwd(), os.path.basename(self.data_source)))
            
            # Log all alternative paths
            # logging.info(f"Checking alternative data source paths:")
            # for p in alt_paths:
            #     logging.info(f"  - {p} (exists: {os.path.exists(p)})")
            
            # Use the first path that exists
            for path in alt_paths:
                if os.path.exists(path):
                    self.data_source = path
                    # logging.info(f"Using alternative data source path: {self.data_source}")
                    break
        
        # Print the data source path before using it
        # print(f"\n[DBFConnection] ABOUT TO USE DATA SOURCE: {self.data_source}")
        # print(f"[DBFConnection] DATA SOURCE EXISTS: {os.path.exists(self.data_source)}")
        
        # If it's a directory, check if the DBF files exist
        if os.path.isdir(self.data_source):
            venta_path = os.path.join(self.data_source, "VENTA.DBF")
            partvta_path = os.path.join(self.data_source, "PARTVTA.DBF")
            # print(f"[DBFConnection] VENTA.DBF exists: {os.path.exists(venta_path)}")
            # print(f"[DBFConnection] PARTVTA.DBF exists: {os.path.exists(partvta_path)}")
        
        # Build connection string with or without encryption password based on ENCRYPTED flag
        connection_parts = [
            f"data source={self.data_source}; ",
            "ServerType=LOCAL; ",
            "TableType=CDX; ",
            "Shared=TRUE; "
        ]
        
        # Log the full connection string (without password)
        connection_string_safe = f"data source={self.data_source}; ServerType=LOCAL; TableType=CDX; Shared=TRUE;"
        # print(f"[DBFConnection] Connection string: {connection_string_safe}")
        # logging.info(f"[DBFConnection] Connection string: {connection_string_safe}")
        
        # Only add encryption password if ENCRYPTED flag is True
        if encrypted:
            connection_parts.append(f"EncryptionPassword={encryption_password};")
            
        self.connection_string = "".join(connection_parts)
        # print(f"Debug - Connection string: {self.connection_string}")
        self.conn: Optional[AdsConnection] = None
        self.reader = None



    def connect(self) -> None:
        """Establish connection to the DBF file."""
        self._check_dll_loaded()
        
        try:
            # Import here after DLL is loaded
            from Advantage.Data.Provider import AdsConnection
            from System import Exception as SystemException
            
            self.conn = AdsConnection(self.connection_string)
            self.conn.Open()
        except SystemException as e:
            raise ConnectionError(f"Failed to connect to DBF: {str(e)}")
        except ImportError as e:
            raise RuntimeError(f"Failed to import Advantage modules: {str(e)}. Make sure DLL is loaded correctly.")

    def get_reader(self, table_name: str, sql_query: str = None):
        """Get a reader for the specified table.
        
        Args:
            table_name: Name of the table to read
            sql_query: Optional SQL query to execute instead of reading whole table
            
        Returns:
            Data reader object
        """
        if not self.conn or not hasattr(self.conn, 'State') or self.conn.State != 'Open':
            self.connect()

        try:
            cmd = self.conn.CreateCommand()
            
            if sql_query:
                # Use SQL query
                print(f"\nExecuting SQL query: {sql_query}")
                cmd.CommandText = sql_query
            else:
                # Direct table access
                from System.Data import CommandType
                cmd.CommandText = table_name
                cmd.CommandType = CommandType.TableDirect
            
            self.reader = cmd.ExecuteReader()
            return self.reader
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {str(e)}")

    def close(self) -> None:
        """Close all connections and readers."""
        if self.reader:
            self.reader.Close()
        if self.conn and hasattr(self.conn, 'State'):
            try:
                from System.Data import ConnectionState
                if self.conn.State == ConnectionState.Open:
                    self.conn.Close()
            except ImportError:
                # Fallback if we can't import ConnectionState
                if self.conn.State == 'Open':
                    self.conn.Close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
