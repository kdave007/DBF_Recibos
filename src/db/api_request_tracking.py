import sqlite3
import json
import logging
from src.config.db_config import SQLiteConnection


class APIRequestTracking:
    """
    Simple class to track API requests and responses.
    """
    
    def __init__(self, db_config):
        """
        Initialize the API request tracking with database configuration.
        
        Args:
            db_config: Database configuration object
        """
        self.db_config = db_config
        self.table_name = "api_logs"
        self._create_table()
    
    def _create_table(self):
        """Create the API logs table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folio TEXT NOT NULL,
            json_request TEXT NOT NULL,
            post_response TEXT,
            get_response TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        try:
            with sqlite3.connect(self.db_config['database']) as conn:
                conn.execute(create_table_sql)
                conn.commit()
                logging.info(f"Table {self.table_name} created or verified successfully")
        except sqlite3.Error as e:
            logging.error(f"Error creating table {self.table_name}: {e}")
            raise
    
    def log_request(self, folio: str, request_data: dict):
        """
        Log an API request and return the record ID.
        
        Args:
            folio: Document folio number
            request_data: Request payload as dictionary
            
        Returns:
            int: The ID of the inserted record
        """
        insert_sql = f"""
        INSERT INTO {self.table_name} (folio, json_request)
        VALUES (?, ?)
        """
        
        try:
            with sqlite3.connect(self.db_config['database']) as conn:
                cursor = conn.cursor()
                cursor.execute(insert_sql, (
                    folio,
                    json.dumps(request_data, indent=2)
                ))
                conn.commit()
                record_id = cursor.lastrowid
                logging.info(f"API request logged for folio {folio} with ID {record_id}")
                return record_id
        except sqlite3.Error as e:
            logging.error(f"Error logging API request for folio {folio}: {e}")
            raise
    
    def update_post_response(self, record_id: int, post_response: str):
        """
        Update the record with POST response data (usually an ID).
        
        Args:
            record_id: The ID of the record to update
            post_response: POST response data as string (usually an ID)
        """
        update_sql = f"""
        UPDATE {self.table_name} 
        SET post_response = ?
        WHERE id = ?
        """
        
        try:
            with sqlite3.connect(self.db_config['database']) as conn:
                conn.execute(update_sql, (post_response, record_id))
                conn.commit()
                logging.info(f"API POST response updated for record ID {record_id}")
        except sqlite3.Error as e:
            logging.error(f"Error updating API POST response for record ID {record_id}: {e}")
            raise
    
    def update_get_response(self, record_id: int, get_response: str):
        """
        Update the record with GET response data (JSON with IDs).
        
        Args:
            record_id: The ID of the record to update
            get_response: GET response data as JSON string
        """
        update_sql = f"""
        UPDATE {self.table_name} 
        SET get_response = ?
        WHERE id = ?
        """
        
        try:
            with sqlite3.connect(self.db_config['database']) as conn:
                conn.execute(update_sql, (get_response, record_id))
                conn.commit()
                logging.info(f"API GET response updated for record ID {record_id}")
        except sqlite3.Error as e:
            logging.error(f"Error updating API GET response for record ID {record_id}: {e}")
            raise
    
    def update_get_response_by_folio(self, folio: str, post_response_id: str, get_response: str):
        """
        Update the GET response for a record by matching folio and POST response ID.
        
        Args:
            folio: Document folio number
            post_response_id: The ID returned by the POST request
            get_response: GET response data as JSON string
        """
        update_sql = f"""
        UPDATE {self.table_name} 
        SET get_response = ?
        WHERE folio = ? AND post_response = ?
        """
        
        try:
            with sqlite3.connect(self.db_config['database']) as conn:
                cursor = conn.cursor()
                cursor.execute(update_sql, (get_response, folio, post_response_id))
                rows_affected = cursor.rowcount
                conn.commit()
                if rows_affected > 0:
                    logging.info(f"API GET response updated for folio {folio} with POST ID {post_response_id}")
                else:
                    logging.warning(f"No matching record found for folio {folio} with POST ID {post_response_id}")
        except sqlite3.Error as e:
            logging.error(f"Error updating API GET response for folio {folio}: {e}")
            raise

    def get_logs_by_folio(self, folio: str):
        """
        Get all API logs for a specific folio.
        
        Args:
            folio: Document folio number
            
        Returns:
            list: List of log records for the folio
        """
        select_sql = f"""
        SELECT * FROM {self.table_name} 
        WHERE folio = ? 
        ORDER BY created_at DESC
        """
        
        try:
            with sqlite3.connect(self.db_config['database']) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(select_sql, (folio,))
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logging.error(f"Error retrieving logs for folio {folio}: {e}")
            return []
