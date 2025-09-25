import sqlite3
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz
import json

class RetriesTracking:
    """Sistema de seguimiento para detalles de facturas"""
    
    def __init__(self, db_config: dict):
        self.config = db_config

    def insert_or_update_fac(self, folio: int, completado: bool = False, fecha_registro: date = None) -> bool:
        """Insert a new record or update an existing one in the retries_tracking table
        
        Args:
            folio: The invoice folio (primary key)
            intentos: Number of retry attempts
            completado: Whether the process was completed successfully
            
        Returns:
            bool: True if the operation was successful, False otherwise
        """
        try:
            # Connect to SQLite database
            with sqlite3.connect(self.config['database']) as conn:
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                
                # Create cursor
                cursor = conn.cursor()
                
                # Always start with intentos = 1 for new records
                # Use current date if fecha_registro is not provided
                if fecha_registro is None:
                    fecha_registro = date.today()
                
                # Check if record exists
                check_query = "SELECT folio, intentos FROM reintentos_fac_venta WHERE folio = ?"
                cursor.execute(check_query, (folio,))
                existing_record = cursor.fetchone()
                
                if existing_record:
                    logging.warning(f"updating retry record by folio {folio}")
                    # Update existing record
                    update_query = """
                        UPDATE reintentos_fac_venta
                        SET intentos = intentos + 1,
                            completado = ?,
                            fecha_del_registro = ?
                        WHERE folio = ?
                    """
                    cursor.execute(update_query, (completado, fecha_registro, folio))
                else:
                    # Insert new record
                    logging.warning(f"inserting new retry record by folio {folio}")
                    insert_query = """
                        INSERT INTO reintentos_fac_venta (
                            folio, intentos, completado, fecha_del_registro
                        ) VALUES (?, ?, ?, ?)
                    """
                    cursor.execute(insert_query, (folio, 1, completado, fecha_registro))
                
                # Commit changes
                conn.commit()
                
                # Check if operation was successful
                cursor.execute("SELECT folio FROM reintentos_fac_venta WHERE folio = ?", (folio,))
                result = cursor.fetchone()
                
                if result:
                    logging.info(f"Successfully inserted/updated retry tracking for folio {folio}")
                    return True
                else:
                    logging.info(f"Failed to insert/update retry tracking for folio {folio}")
                    return False
                        
        except sqlite3.Error as e:
            logging.error(f"SQLite error in retry tracking for folio {folio}: {e}")
            return False
        except Exception as e:
            logging.error(f"Error in retry tracking for folio {folio}: {e}")
            return False

    def get_ignore_list(self, start_date, end_date):
        """Get a list of folios to ignore based on date range and retry criteria
        
        Args:
            start_date: The start date for the range (inclusive)
            end_date: The end date for the range (inclusive)
            
        Returns:
            list: List of folios that meet the criteria
        """
        try:
            # Connect to SQLite database
            with sqlite3.connect(self.config['database']) as conn:
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                
                # Create cursor
                cursor = conn.cursor()
                
                # Select folios within date range
                query = """
                    SELECT folio 
                    FROM reintentos_fac_venta
                    WHERE fecha_del_registro BETWEEN ? AND ?
                    AND intentos >= 3
                    AND completado = 0
                    ORDER BY folio
                """
                
                params = (start_date, end_date)
                cursor.execute(query, params)
                
                # Fetch all results and extract folios
                results = cursor.fetchall()
                folios = [row[0] for row in results]
                
                print(f"Found {len(folios)} folios to ignore in date range {start_date} to {end_date}")
                return folios
                    
        except sqlite3.Error as e:
            logging.error(f"SQLite error getting ignore list: {e}")
            return []
        except Exception as e:
            logging.error(f"Error getting ignore list: {e}")
            return []

    def completed(self, folio):
        """Update a record to mark it as completed
        
        Args:
            folio: The folio of the record to update
            
        Returns:
            bool: True if the update was successful, False otherwise
        """
        try:
            # Connect to SQLite database
            with sqlite3.connect(self.config['database']) as conn:
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                
                # Create cursor
                cursor = conn.cursor()
                
                # Update completado to True (1 in SQLite) for the specified folio
                query = """
                    UPDATE reintentos_fac_venta
                    SET completado = 1
                    WHERE folio = ?
                """
                
                cursor.execute(query, (folio,))
                conn.commit()
                
                # Check if any rows were affected
                rows_affected = cursor.rowcount
                
                # Even if no record was found, we consider this a success
                # since we're calling this method after successful processing
                if rows_affected > 0:
                    print(f"Successfully marked folio {folio} as completed")
                else:
                    print(f"No retry record found for folio {folio} - this is normal if it was processed on first try")
                
                return True
                    
        except sqlite3.Error as e:
            logging.error(f"SQLite error marking folio {folio} as completed: {e}")
            return False
        except Exception as e:
            logging.error(f"Error marking folio {folio} as completed: {e}")
            return False