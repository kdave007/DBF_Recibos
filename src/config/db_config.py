from typing import Optional, Dict, Any, List, Union, Tuple
import sqlite3
import os
from pathlib import Path
from dotenv import load_dotenv
from src.utils.get_enc import EncEnv

class SQLiteConnection:
    # Simple connection management
    _db_path = None
    
    @staticmethod
    def get_db_config() -> Dict[str, str]:
        """
        Returns the database configuration in the format needed by other classes.
        For SQLite, this is primarily the database path.
        
        Returns:
            Dict[str, str]: Database configuration dictionary
        """
        # Set up the database path if not already done
        if SQLiteConnection._db_path is None:
            SQLiteConnection._setup_db_path()
            
        # Return the configuration with the correct key names
        return {
            'database': SQLiteConnection._db_path
        }
    
    @staticmethod
    def _setup_db_path():
        """Set up the database path from environment variables"""
        env = EncEnv()
        
        # For SQLite, we need the database path and can use the DATABASE name from env
        db_path = env.get('SQLITE_DATABASE_PATH')
        db_name = env.get('DATABASE')
        
        if db_path:
            # If SQLITE_DATABASE_PATH is provided, check if it's a directory or file
            path_obj = Path(db_path)
            if path_obj.is_dir():
                # It's a directory, so append the database name
                if db_name:
                    db_path = str(path_obj / f"{db_name}.sqlite")
                else:
                    db_path = str(path_obj / "py_process.sqlite")
            # Otherwise assume it's already a full path to the database file
            print(f"Using specified SQLite database path: {db_path}")
        else:
            # Create a path in the project root
            if db_name:
                # Use the specified database name
                db_path = str(Path(__file__).parent.parent.parent / f"{db_name}.sqlite")
                print(f"Using database path with specified name: {db_path}")
            else:
                # Fall back to default name
                db_path = str(Path(__file__).parent.parent.parent / 'py_process.sqlite')
                print(f"Using default SQLite database path: {db_path}")
                
        # Create the database directory if it doesn't exist
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            
        SQLiteConnection._db_path = db_path

    def __init__(self):
        """Initialize SQLite connection with environment variables"""
        # Set up the database path if not already done
        if SQLiteConnection._db_path is None:
            SQLiteConnection._setup_db_path()
            
        self.db_config = {
            'database': SQLiteConnection._db_path
        }
        self.connection = None
        self.cursor = None
    
    def get_connection(self) -> sqlite3.Connection:
        """Get a connection to the SQLite database
        
        Returns:
            sqlite3.Connection: A SQLite connection object
        """
        try:
            # Create a new connection if one doesn't exist
            if self.connection is None:
                # Make sure the database directory exists
                db_dir = os.path.dirname(self.db_config['database'])
                if db_dir and not os.path.exists(db_dir):
                    os.makedirs(db_dir, exist_ok=True)
                    
                # Create the connection
                self.connection = sqlite3.connect(self.db_config['database'])
                
                # Enable dictionary cursor by default
                self.connection.row_factory = sqlite3.Row
                
                # Enable foreign keys
                self.connection.execute("PRAGMA foreign_keys = ON")
                
                # Set pragmas for better performance
                self.connection.execute("PRAGMA journal_mode = WAL")
                self.connection.execute("PRAGMA synchronous = NORMAL")
                
                # Test the connection with a simple query
                test_cursor = self.connection.cursor()
                test_cursor.execute("SELECT 1")
                test_cursor.fetchone()
                test_cursor.close()
                
                print("SQLite database connection successful")
                
            # Check if connection is still valid
            if self.connection:
                try:
                    # Test if connection is still working
                    test_cursor = self.connection.cursor()
                    test_cursor.execute("SELECT 1")
                    test_cursor.fetchone()
                    test_cursor.close()
                except sqlite3.Error:
                    # Connection is not valid, create a new one
                    self.close_pool()
                    return self.get_connection()
                    
            return self.connection
            
        except Exception as e:
            print(f"ERROR - Could not get database connection: {str(e)}")
            self.connection = None
            raise Exception(f"Could not connect to database: {str(e)}")
            
        return self.connection
    
    def release_connection(self, conn: sqlite3.Connection) -> None:
        """Release a connection (no-op for SQLite)
        
        Args:
            conn (sqlite3.Connection): The connection to release
        """
        # For SQLite, we keep the connection open
        pass
    
    def return_connection(self, conn: sqlite3.Connection) -> None:
        """Alias for release_connection
        
        Args:
            conn (sqlite3.Connection): The connection to return
        """
        self.release_connection(conn)
    
    def execute_query(self, query: str, params: Optional[Any] = None) -> Dict[str, Any]:
        """Execute a query and return results as dictionary
        
        Args:
            query: SQL query string
            params: Parameters for the query. Can be a dict, tuple, or list
            
        Returns:
            Dictionary with query results or error information
        """
        cursor = None
        connection = None
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                # Get a connection
                connection = self.get_connection()
                
                # Create a cursor
                cursor = connection.cursor()
                
                # Execute the query
                if params is not None:
                    # Convert dict params to tuple if needed for SQLite
                    if isinstance(params, dict):
                        # Replace named parameters with ? placeholders
                        if '?' not in query and ':' not in query and '%s' in query:
                            # Convert from PostgreSQL style to SQLite style
                            query = query.replace('%s', '?')
                            # Extract values in order
                            param_values = tuple(params.values())
                            cursor.execute(query, param_values)
                        else:
                            # Use named parameters directly
                            cursor.execute(query, params)
                    else:
                        # Already a tuple or list
                        cursor.execute(query, params)
                else:
                    cursor.execute(query)
                    
                # Fetch results if the query returns data
                if cursor.description:
                    results = cursor.fetchall()
                    if results and isinstance(results[0], sqlite3.Row):
                        # Already in dict-like format
                        result_dicts = [dict(row) for row in results]
                    elif results:
                        # Convert to dict
                        column_names = [desc[0] for desc in cursor.description]
                        result_dicts = [{column: row[i] for i, column in enumerate(column_names)} for row in results]
                    else:
                        result_dicts = []
                    connection.commit()
                    return {'data': result_dicts}
                else:
                    connection.commit()
                    return {'affected_rows': cursor.rowcount}
                
            except sqlite3.OperationalError as e:
                # Handle database locked errors by retrying
                error_msg = str(e).strip()
                if "database is locked" in error_msg.lower() and retry_count < max_retries - 1:
                    print(f"Database locked, retrying... (attempt {retry_count + 1})")
                    retry_count += 1
                    # Wait a bit before retrying
                    import time
                    time.sleep(0.5 * (retry_count + 1))
                    continue
                else:
                    # Other operational error or max retries reached
                    if self.connection:
                        self.connection.rollback()
                    print(f"SQL ERROR: {error_msg}")
                    return {'error': error_msg}
                    
            except sqlite3.Error as e:
                # Handle other SQLite errors
                if self.connection:
                    self.connection.rollback()
                error_msg = str(e).strip()
                print(f"SQL ERROR: {error_msg}")
                return {'error': error_msg}
                
            except Exception as e:
                # Handle any other unexpected errors
                if self.connection:
                    self.connection.rollback()
                error_msg = str(e).strip()
                print(f"UNEXPECTED ERROR: {error_msg}")
                return {'error': error_msg}
                
            finally:
                # Close cursor but keep connection open for reuse
                if cursor:
                    cursor.close()
                    
            # If we get here, we've exhausted our retries
            break
            
        # If we get here without returning, something went wrong
        return {'error': 'Failed to execute query after multiple attempts'}
    
    def begin_transaction(self) -> sqlite3.Connection:
        """Begin a new transaction and return the connection"""
        connection = self.get_connection()
        return connection
    
    def commit_transaction(self, connection: sqlite3.Connection) -> None:
        """Commit the transaction"""
        connection.commit()
    
    def rollback_transaction(self, connection: sqlite3.Connection) -> None:
        """Rollback the transaction"""
        connection.rollback()
    
    def execute_batch_update(self, query: str, params_list: List[Any], 
                           connection: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
        """Execute the same query with different parameters in batch
        
        Args:
            query: SQL query string
            params_list: List of parameter sets for the query
            connection: Optional existing connection to use
            
        Returns:
            Dictionary with affected rows count or error information
        """
        own_connection = connection is None
        cursor = None
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                if own_connection:
                    connection = self.get_connection()
                cursor = connection.cursor()
                
                # Convert PostgreSQL style to SQLite style if needed
                if '%s' in query and '?' not in query and ':' not in query:
                    query = query.replace('%s', '?')
                
                # SQLite doesn't have execute_batch, so we'll use executemany
                cursor.executemany(query, params_list)
                
                results = {"affected_rows": cursor.rowcount}
                
                if own_connection:
                    connection.commit()
                
                return results
            
            except sqlite3.OperationalError as e:
                # Handle database locked errors by retrying
                error_msg = str(e).strip()
                if "database is locked" in error_msg.lower() and retry_count < max_retries - 1:
                    print(f"Database locked during batch update, retrying... (attempt {retry_count + 1})")
                    retry_count += 1
                    # Wait a bit before retrying
                    import time
                    time.sleep(0.5 * (retry_count + 1))
                    continue
                else:
                    # Other operational error or max retries reached
                    if own_connection and connection:
                        connection.rollback()
                    print(f"DB BATCH ERROR: {error_msg}")
                    return {"error": error_msg}
            
            except sqlite3.Error as e:
                if own_connection and connection:
                    connection.rollback()
                error_msg = str(e).strip()
                print(f"DB BATCH ERROR: {error_msg}")
                return {"error": error_msg}
            
            except Exception as e:
                if own_connection and connection:
                    connection.rollback()
                error_msg = str(e).strip()
                print(f"UNEXPECTED BATCH ERROR: {error_msg}")
                return {"error": error_msg}
            
            finally:
                if cursor:
                    cursor.close()
            
            # If we get here, we've exhausted our retries
            break
        
        # If we get here without returning, something went wrong
        return {"error": "Failed to execute batch update after multiple attempts"}
    
    def close_pool(self) -> None:
        """Close all connections"""
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
            except Exception as e:
                print(f"Error closing SQLite connection: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close_pool()

# For backwards compatibility, create an alias
PostgresConnection = SQLiteConnection
