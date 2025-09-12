from typing import Dict, Any, Optional, List, Iterator
from src.config.db_config import SQLiteConnection
from datetime import datetime
import sqlite3
import logging

class BaseModel:
    def __init__(self):
        self.db = SQLiteConnection()
        self.table_name = ""  # Will be set by child classes
        self.chunk_size = 100  # Default chunk size for processing

    class DatabaseError(Exception):
        """Custom error for database operations"""
        pass

    def _chunk_records(self, records: List[Dict[str, Any]]) -> Iterator[List[Dict[str, Any]]]:
        """Split records into chunks of specified size"""
        for i in range(0, len(records), self.chunk_size):
            yield records[i:i + self.chunk_size]

    def insert_records(self, records: List[Dict[str, Any]], connection: Any = None) -> None:
        """Insert multiple records into the database"""
        if not records:
            return

        # Get the column names from the first record
        columns = list(records[0].keys())
        
        # Build the INSERT query
        column_names = ', '.join(columns)
        
        # Convert records to list of tuples for executemany
        values = [[record[col] for col in columns] for record in records]
        
        # Always print query for tracking table
        if self.table_name == 'estado_factura_venta':
            print(f"\nDEBUG: Printing tracking query for {self.table_name}")
            # Get values from first record
            first_values = [str(records[0][col]) for col in columns]
            # Format values for SQL
            formatted_values = [f"'{val}'" for val in first_values]
            # Create the full INSERT statement
            example_query = f"INSERT INTO {self.table_name} ({column_names}) VALUES ({', '.join(formatted_values)})"
            print(f"\nTracking Query:\n{example_query}")
        
        # Print first query for any table
        elif not hasattr(self, '_printed_' + self.table_name):
            print(f"\nQuery for {self.table_name}:")
            print(f"INSERT INTO {self.table_name} ({column_names}) VALUES ({', '.join(['?']*len(columns))})")
            setattr(self, '_printed_' + self.table_name, True)
        
        # Build the parameterized query for actual execution
        placeholders = ', '.join(['?'] * len(columns))
        query = f"INSERT INTO {self.table_name} ({column_names}) VALUES ({placeholders})"
        print(query)
        
        # Execute the query
        try:
            if connection:
                cursor = connection.cursor()
            else:
                connection = self.db.get_connection()
                cursor = connection.cursor()
                
            cursor.executemany(query, values)
            if not connection:  # Only commit if we're not in a transaction
                connection.commit()
        except Exception as e:
            error_msg = f"Failed to insert records into {self.table_name}"
            if isinstance(e, sqlite3.Error):
                error_msg += f": {str(e)}"
            else:
                error_msg += f": {str(e)}"
            raise self.DatabaseError(error_msg) from e

    def update_batch_status(self, record_ids: List[str], status: str, 
                          api_response: Dict[str, Any], 
                          error_message: Optional[str] = None) -> List[Dict[str, Any]]:
        """Update status for multiple records in chunks"""
        if not record_ids:
            return []
            
        try:
            query = f"""
            UPDATE {self.table_name}
            SET 
                status = ?,
                error_message = ?,
                api_response = ?,
                updated_at = DATETIME('now'),
                retry_count = CASE 
                    WHEN status = 'failed' THEN retry_count + 1
                    ELSE retry_count
                END
            WHERE record_id IN (SELECT value FROM json_each(?))
            RETURNING record_id, status
            """
            
            results = []
            # Process record_ids in chunks
            for chunk_ids in self._chunk_records(record_ids):
                # Convert record_ids list to JSON string for SQLite json_each function
                import json
                params = (status, error_message, json.dumps(api_response), json.dumps(chunk_ids))
                
                chunk_result = self.db.execute_query(query, params)
                if not chunk_result:
                    logging.warning(f"No records updated in chunk for {self.table_name}")
                results.append(chunk_result)
            
            return results
            
        except Exception as e:
            error_msg = f"Failed to update batch status in {self.table_name}"
            if isinstance(e, sqlite3.Error):
                error_msg += f": {str(e)}"
            else:
                error_msg += f": {str(e)}"
            raise self.DatabaseError(error_msg) from e

    def get_pending_records(self, limit: int = 300) -> List[Dict[str, Any]]:
        """Get pending records up to the specified limit"""
        try:
            query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE status = 'pending'
            AND (retry_count < 3 OR retry_count IS NULL)
            ORDER BY created_at ASC
            LIMIT ?
            """
            
            result = self.db.execute_query(query, (limit,))
            if not result:
                logging.info(f"No pending records found in {self.table_name}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to get pending records from {self.table_name}"
            if isinstance(e, sqlite3.Error):
                error_msg += f": {str(e)}"
            else:
                error_msg += f": {str(e)}"
            raise self.DatabaseError(error_msg) from e

    def get_failed_records(self, min_retries: int = 3) -> List[Dict[str, Any]]:
        """Get records that have failed multiple times"""
        try:
            query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE status = 'failed'
            AND retry_count >= ?
            ORDER BY updated_at DESC
            """
            
            result = self.db.execute_query(query, (min_retries,))
            if not result:
                logging.info(f"No failed records found in {self.table_name} with {min_retries}+ retries")
            return result
            
        except Exception as e:
            error_msg = f"Failed to get failed records from {self.table_name}"
            if isinstance(e, sqlite3.Error):
                error_msg += f": {str(e)}"
            else:
                error_msg += f": {str(e)}"
            raise self.DatabaseError(error_msg) from e