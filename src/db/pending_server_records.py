import sqlite3
import logging
import json
from typing import List, Dict
from datetime import date, datetime
from src.db.db_connection_pool import DBConnectionPool

class PendingServerRecords:
    
    def __init__(self, db_config: dict):
        self.config = db_config
        # Initialize the connection pool
        self.pool = DBConnectionPool(db_config, min_conn=2, max_conn=10)
    
    def get_pending_records(self, tipo_doc, limit=100):
        """
        Get records with 'pendiente' status from estado_factura_venta table
        
        Args:
            limit (int): Maximum number of records to retrieve
            
        Returns:
            list: List of pending records or empty list if none found
        """
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return []
                
            cursor = conn.cursor()
            
            # SQL query with SQLite placeholders
            query = """
                SELECT id, folio, fecha_emision, hash, fecha_procesamiento, total_partidas, total_recibos, id_cola
                FROM estado_factura_venta
                WHERE estado = 'pendiente' AND accion = 'enviado' AND tipo_doc = ?
                ORDER BY fecha_procesamiento DESC
                LIMIT ?
            """
            
            # Execute the query with the limit parameter
            cursor.execute(query, (tipo_doc, limit))
            
            # Fetch all results
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            result = []
            for row in rows:
                result.append({
                    'id': row[0],
                    'folio': row[1],
                    'fecha_emision': row[2],
                    'hash': row[3],
                    'fecha_procesamiento': row[4],
                    'total_partidas': row[5],
                    'total_recibos': row[6],
                    'id_cola': row[7]
                })
            
            if not result:
                logging.info("No pending records found in estado_factura_venta")
            else:
                logging.info(f"Found {len(result)} pending records")
                
            return result
            
        except sqlite3.Error as e:
            logging.error(f"SQLite error in get_pending_records: {e}")
            print(f"Database error: {e}")
            return []
        except Exception as e:
            logging.error(f"Error in get_pending_records: {e}")
            print(f"Error: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.release_connection(conn)