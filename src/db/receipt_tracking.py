
import sqlite3
import logging
import json
from typing import List, Dict
from datetime import date, datetime
from src.db.db_connection_pool import DBConnectionPool

class ReceiptTracking:
    
    def __init__(self, db_config: dict):
        self.config = db_config
        # Initialize the connection pool
        self.pool = DBConnectionPool(db_config, min_conn=2, max_conn=10)

    def insert_receipts_on_wait(self, receipts: List[Dict], action, estado) -> bool:
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return False
                
            cursor = conn.cursor()
            inserted_count = 0
            
            # Simple insert query with SQLite placeholders
            insert_query = """
                INSERT INTO recibo_venta (
                    folio, num_ref, hash, estado, fecha_emision, fecha_procesamiento, indice
                ) VALUES (?, ?, ?, ?, ?, datetime('now'), ?)
            """
            print(f'RECEIPTS TO PROCESS: {receipts}')

            # Process each receipt
            for receipt in receipts:
                # Get basic fields from the receipt
                num_ref = receipt.get('num_ref', '')
                folio = receipt.get('folio', '')
                indice = receipt.get('indice')
                
                # Get fecha_emision (default to today)
                fecha_emision = receipt.get('fecha_emision', date.today())
                if isinstance(fecha_emision, str):
                    try:
                        # Try to parse date in format '15/07/2025 12:00:00 a. m.'
                        fecha_emision = datetime.strptime(fecha_emision.split(' ')[0], '%d/%m/%Y').date()
                    except ValueError:
                        try:
                            # Try standard ISO format
                            fecha_emision = datetime.strptime(fecha_emision, '%Y-%m-%d').date()
                        except ValueError:
                            # Default to today if parsing fails
                            fecha_emision = date.today()
                elif isinstance(fecha_emision, datetime):
                    fecha_emision = fecha_emision.date()
                
                # Get hash and estado (default values if not provided)
                hash_value = receipt.get('hash', '')

                try:
                    # Insert the record
                    cursor.execute(insert_query, (
                        folio, num_ref, hash_value, estado, fecha_emision, indice
                    ))
                    conn.commit()  # Commit after each insert to ensure it's saved
                    inserted_count += 1
                    
                except sqlite3.Error as e:
                    print(f"RECEIPTS Error inserting record: {e}")
                    logging.error(f"RECEIPTS :: Error inserting record: {e}")
                    # Continue with next record

                logging.info(f"Inserting record: folio={folio}, num_ref={num_ref}")

            print(f"Batch replace completed: {inserted_count} records inserted")
            return inserted_count > 0
        
        except sqlite3.Error as e:
            logging.error(f"RECEIPTS :: SQLite error in insert_receipts_on_wait: {e}")
            print(f"Database error: {e}")
            return False
        except Exception as e:
            logging.error(f"RECEIPTS :: Error in insert_receipts_on_wait: {e}")
            print(f"Error: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.release_connection(conn)
        
    def batch_replace_by_id(self, receipts: List[Dict]) -> bool:
        """Procesa múltiples recibos en una sola transacción, utilizando el ID como referencia principal."""
        if not receipts:
            return True  # Nothing to process
            
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return False
                
            cursor = conn.cursor()
            inserted_count = 0
            
            # Simple insert query with SQLite placeholders
            insert_query = """
                INSERT INTO recibo_venta (
                    folio, num_ref, hash, estado, fecha_emision, fecha_procesamiento
                ) VALUES (?, ?, ?, ?, ?, datetime('now'))
            """
            print(f'RECEIPTS TO PROCESS: {receipts}')
            
            # Process each receipt
            for receipt in receipts:
                # Get basic fields from the receipt
                num_ref = receipt.get('num_ref', '')
                folio = receipt.get('folio', '')
                
                # Get fecha_emision (default to today)
                fecha_emision = receipt.get('fecha_emision', date.today())
                if isinstance(fecha_emision, str):
                    try:
                        # Try to parse date in format '15/07/2025 12:00:00 a. m.'
                        fecha_emision = datetime.strptime(fecha_emision.split(' ')[0], '%d/%m/%Y').date()
                    except ValueError:
                        try:
                            # Try standard ISO format
                            fecha_emision = datetime.strptime(fecha_emision, '%Y-%m-%d').date()
                        except ValueError:
                            # Default to today if parsing fails
                            fecha_emision = date.today()
                elif isinstance(fecha_emision, datetime):
                    fecha_emision = fecha_emision.date()
                
                # Get hash and estado (default values if not provided)
                hash_value = receipt.get('hash', '')
                estado = receipt.get('estado', 'pending')

                logging.info(f"Inserting record: folio={folio}, num_ref={num_ref}")
                
                try:
                    # Insert the record
                    cursor.execute(insert_query, (
                        folio, num_ref, hash_value, estado, fecha_emision
                    ))
                    conn.commit()  # Commit after each insert to ensure it's saved
                    inserted_count += 1
                    
                except sqlite3.Error as e:
                    print(f"RECEIPTS Error inserting record: {e}")
                    logging.error(f"RECEIPTS :: Error inserting record: {e}")
                    # Continue with next record
            
            print(f"Batch replace completed: {inserted_count} records inserted")
            return inserted_count > 0
        
        except sqlite3.Error as e:
            logging.error(f"RECEIPTS :: SQLite error in batch_replace_by_id: {e}")
            print(f"Database error: {e}")
            return False
        except Exception as e:
            logging.error(f"RECEIPTS :: Error in batch_replace_by_id: {e}")
            print(f"Error: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.release_connection(conn)