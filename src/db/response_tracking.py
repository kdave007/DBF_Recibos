import sqlite3
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz
from src.db.db_connection_pool import DBConnectionPool

class ResponseTracking:
    def __init__(self, db_config: dict):
        self.config = db_config
        # Initialize the connection pool
        self.pool = DBConnectionPool(db_config, min_conn=2, max_conn=10)

    def delete_by_id(self, id) -> bool:
        """Delete a record from estado_factura_venta by ID"""
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return False
                
            cursor = conn.cursor()
            
            # Delete record by ID
            query = """
                DELETE FROM estado_factura_venta
                WHERE id = ?
                RETURNING id
            """
            
            cursor.execute(query, (id,))
            deleted_id = cursor.fetchone()
            conn.commit()
            
            if deleted_id:
                print(f"Successfully deleted record with ID {id}")
                return True
            else:
                print(f"No record found with ID {id}")
                return False
                        
        except sqlite3.Error as e:
            logging.error(f"SQLite error deleting record with ID {id}: {e}")
            return False
        except Exception as e:
            logging.error(f"Error deleting record with ID {id}: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.release_connection(conn)

    def insert_fac(self, 
                        id,
                        folio: str, 
                        total_partidas: int,
                        hash: str,
                        estado: str,
                        accion: str,
                        fecha_emision: date,
                        total_recibos: int,
                        id_cola: int,
                        tipo_doc: str,
                        ) -> bool:
        """Inserta estado de factura"""
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return False
                
            cursor = conn.cursor()
            
            current_date = datetime.now().date()
            
            # Insert new record - direct insert without checking
            query = """
                INSERT INTO estado_factura_venta (
                    id, folio, total_partidas, hash,
                    fecha_procesamiento, estado, fecha_emision, accion, total_recibos, id_cola, tipo_doc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
            """
            # Debug print for query and parameters
            
            
            params = (
                id,
                folio, 
                total_partidas, 
                hash, 
                current_date, 
                estado, 
                fecha_emision, 
                accion,
                total_recibos,
                id_cola,
                tipo_doc
            )

            # print(f"[DEBUG] SQL Query: {query}")
            # print(f"[DEBUG] Params: {params}")
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            conn.commit()
            
            if result:
                print(f"Operation successful for folio {folio} - hash: {hash}")
                return True
            
            print(f"Operation failed for folio {folio}")
            return False
            
        except sqlite3.Error as e:
            logging.error(f"SQLite error inserting/updating record: {e}")
            return False
        except Exception as e:
            logging.error(f"Error inserting/updating record: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.release_connection(conn)
            
    def update_head_status(self, folio, new_id: int, estado: str, accion: str, tipo_doc: str) -> bool:
        """Update only the estado and accion fields for a record by its folio
        
        Args:
            folio: The folio of the record to update
            new_id: The new ID value
            estado: The new estado value
            accion: The new accion value
            tipo_doc: The document type
            
        Returns:
            bool: True if the update was successful, False otherwise
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
            
            # Update only estado, accion and id fields
            query = """
                UPDATE estado_factura_venta 
                SET estado = ?, 
                    accion = ?,
                    id = ?
                WHERE folio = ? AND tipo_doc = ?
                RETURNING id
            """
            
            # Execute the query
            cursor.execute(query, (estado, accion, new_id, folio, tipo_doc))
            updated_id = cursor.fetchone()
            conn.commit()
            
            if updated_id:
                logging.info(f"Successfully updated status for record with folio {folio}")
                return True
            else:
                logging.warning(f"No record found with folio {folio}")
                return False
                
        except sqlite3.Error as e:
            logging.error(f"SQLite error updating status for record with folio {folio}: {e}")
            return False
        except Exception as e:
            logging.error(f"Error updating status for record with folio {folio}: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.release_connection(conn)


    def update_detail_status(self, details) -> bool:
        """Update estado and id fields for multiple detail records
        
        Args:
            details: List of detail records to update
            
        Returns:
            bool: True if all updates were successful, False otherwise
        """
        if not details or len(details) == 0:
            logging.warning("No details provided to update")
            return False
            
        success_count = 0
        total_count = len(details)
        
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return False
                
            cursor = conn.cursor()
            
            # Process each detail record
            for record in details:
             
                estado = record.get('estado')
                folio = record.get('folio')
                new_id = record.get('id')
                indice = record.get('indice')
                
                if not all([estado, folio, new_id, indice]):
                    logging.warning(f"Missing required fields in detail record: {record}")
                    continue

                query = """
                    UPDATE detalle_estado
                    SET estado = ?, 
                        id = ?
                    WHERE folio = ? AND indice = ?
                    RETURNING id
                """
                
                cursor.execute(query, (estado, new_id, folio, indice))
                updated_id = cursor.fetchone()
                conn.commit()
                
                if updated_id:
                    logging.info(f"Successfully updated detail status for folio {folio}, indice {indice}")
                    success_count += 1
                else:
                    logging.warning(f"No detail record found for folio {folio}, indice {indice}")
            
            return success_count == total_count  # Return True only if all updates succeeded
                
        except sqlite3.Error as e:
            logging.error(f"SQLite error updating detail statuses: {e}")
            return False
        except Exception as e:
            logging.error(f"Error updating detail statuses: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.release_connection(conn)

    
    def update_receipt_status(self, receipts) -> bool:
        """Update estado and response fields for multiple receipt records
        
        Args:
            receipts: List of receipt records to update
            
        Returns:
            bool: True if all updates were successful, False otherwise
        """
        if not receipts or len(receipts) == 0:
            logging.warning("No receipts provided to update")
            return False
            
        success_count = 0
        total_count = len(receipts)
        
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return False
                
            cursor = conn.cursor()
           
            
            # Process each receipt record
            for record in receipts:

                # print(f"/*/*/*/*/*/*/*/*/*/*/*/*/*/ RECORD recibo update {record}")

                estado = record.get('estado')
                folio = record.get('folio')
                new_id = record.get('id')
                response_data = record.get('respuesta')
                        
                if not all([estado, folio, new_id, response_data]):
                    logging.warning(f"Missing required fields in receipt record: {record}")
                    continue

                query = """
                    UPDATE recibo_venta 
                    SET estado = ?, 
                        respuesta = ?
                    WHERE folio = ?
                    RETURNING id_sql
                """
                
                cursor.execute(query, (estado, response_data, folio))
                updated_id = cursor.fetchone()
                conn.commit()
                
                if updated_id:
                    logging.info(f"Successfully updated receipt status for folio {folio}")
                    success_count += 1
                else:
                    logging.warning(f"No receipt record found for folio {folio}")
            
            return success_count == total_count  # Return True only if all updates succeeded
                
        except sqlite3.Error as e:
            logging.error(f"SQLite error updating receipt statuses: {e}")
            return False
        except Exception as e:
            logging.error(f"Error updating receipt statuses: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.release_connection(conn)