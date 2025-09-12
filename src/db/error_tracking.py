import sqlite3
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz
from src.db.db_connection_pool import DBConnectionPool

class ErrorTracking:
    """Sistema de seguimiento para detalles de facturas"""
    
    def __init__(self, db_config: dict):
        self.config = db_config
        # Initialize the connection pool
        self.pool = DBConnectionPool(db_config, min_conn=2, max_conn=10)

    
    def insert(self, desc: str, class_name: str) -> bool:
        """Insert a new error record into the error_tracking table
        
        Args:
            desc: Description of the error
            class_name: Name of the class where the error occurred
            
        Returns:
            bool: True if the operation was successful, False otherwise
        """
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return False
                
            cursor = conn.cursor()
            
            # Insert error record with SQLite placeholders
            query = """
                INSERT INTO errores (
                    fecha, descripcion, clase
                ) VALUES (
                    datetime('now'), ?, ?
                )
                RETURNING id
            """
            
            params = (desc, class_name)
            cursor.execute(query, params)
            result = cursor.fetchone()
            conn.commit()
            
            if result:
                error_id = result[0]
                print(f"Successfully logged error with ID {error_id}")
                return True
            else:
                print("Failed to log error")
                return False
                
        except sqlite3.Error as e:
            logging.error(f"SQLite error logging to error_tracking: {e}")
            return False
        except Exception as e:
            logging.error(f"Error logging to error_tracking: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.release_connection(conn)
