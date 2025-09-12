import sqlite3
import logging
import threading
from typing import Optional, Dict, Any, List

class DBConnectionPool:
    """
    A singleton connection pool for SQLite database connections.
    This helps reduce the overhead of creating and closing connections.
    """
    _instance = None
    _pool = None
    _local = threading.local()
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DBConnectionPool, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_config: Dict[str, Any], min_conn: int = 1, max_conn: int = 10):
        # If already initialized, don't reinitialize
        if hasattr(self, '_initialized') and self._initialized:
            return
            
        self.db_config = db_config
        self.min_conn = min_conn
        self.max_conn = max_conn
        
        # Initialize class variables if they haven't been set yet
        if DBConnectionPool._pool is None:
            DBConnectionPool._pool = []
        
        self._initialized = True
        
        try:
            # Make sure the database directory exists
            import os
            from pathlib import Path
            db_path = self.db_config['database']
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            
            # Test connection to make sure the database is accessible
            conn = sqlite3.connect(self.db_config['database'])
            
            # Enable dictionary cursor by default
            conn.row_factory = sqlite3.Row
            
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Close the test connection
            conn.close()
            
            logging.info(f"SQLite connection pool initialized with max {max_conn} connections")
        except Exception as e:
            logging.error(f"Error initializing SQLite connection pool: {e}")
            DBConnectionPool._pool = None
    
    def get_connection(self) -> Optional[sqlite3.Connection]:
        """Get a connection from the pool"""
        if DBConnectionPool._pool is None:
            # Try to initialize the pool if it's not initialized
            try:
                DBConnectionPool._pool = []
                logging.info("Initializing connection pool on demand")
            except Exception as e:
                logging.error(f"Failed to initialize connection pool: {e}")
                return None
            
        try:
            # Check if this thread already has a connection
            if not hasattr(DBConnectionPool._local, 'connection'):
                # Create a new connection for this thread
                conn = sqlite3.connect(self.db_config['database'])
                # Enable dictionary cursor by default
                conn.row_factory = sqlite3.Row
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                # Store in thread local storage
                DBConnectionPool._local.connection = conn
                
            return DBConnectionPool._local.connection
        except Exception as e:
            logging.error(f"Error getting SQLite connection: {e}")
            return None
    
    def release_connection(self, conn: sqlite3.Connection) -> None:
        """Return a connection to the pool
        
        For SQLite, we don't actually return connections to a pool,
        but we might need to perform cleanup operations.
        """
        # For SQLite, we keep the connection open for reuse in the same thread
        # No action needed here, but verify the connection is still valid
        try:
            # Test if the connection is still valid
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
        except sqlite3.Error:
            # If the connection is invalid, remove it from thread local storage
            if hasattr(DBConnectionPool._local, 'connection'):
                try:
                    DBConnectionPool._local.connection.close()
                except:
                    pass
                delattr(DBConnectionPool._local, 'connection')
    
    def close_all(self) -> None:
        """Close all connections in the pool"""
        if DBConnectionPool._pool is None:
            return
            
        try:
            if hasattr(DBConnectionPool._local, 'connection'):
                DBConnectionPool._local.connection.close()
                delattr(DBConnectionPool._local, 'connection')
            # Reset the pool
            DBConnectionPool._pool = []
            logging.info("All SQLite connections have been closed")
        except Exception as e:
            logging.error(f"Error closing SQLite connections: {e}")
