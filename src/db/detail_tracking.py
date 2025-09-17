import sqlite3
from datetime import datetime, date
from typing import List, Dict, Optional
import logging
import pytz
import json

class DetailTracking:
    """Sistema de seguimiento para detalles de facturas"""
    
    def __init__(self, db_config: dict):
        self.config = db_config
        
    
    def insert_or_update_detail(self, 
                               id,
                               folio: str, 
                               hash_detalle: str,
                               fecha: date,
                               estado,
                               accion,
                               ref: str = '') -> bool:
        """
        Inserta un nuevo registro de detalle o actualiza uno existente
        
        Args:
            folio: Número de folio
            hash_detalle: Hash MD5 del detalle
            fecha: Fecha del detalle
            estado: Estado del detalle (pendiente, procesado, error)
            accion: Tipo de operación (create, update, delete)
            ref: Referencia del detalle
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        try:
            # Connect to SQLite database
            with sqlite3.connect(self.config['database']) as conn:
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                
                # Create cursor
                cursor = conn.cursor()
                
                # Check if record exists
                check_query = "SELECT id FROM detalle_estado WHERE id = ?"
                cursor.execute(check_query, (id,))
                existing_record = cursor.fetchone()
                
                if existing_record:
                    # Update existing record
                    update_query = """
                        UPDATE detalle_estado
                        SET estado = ?,
                            accion = ?,
                            hash_detalle = ?,
                            ref = ?
                        WHERE id = ?
                    """
                    cursor.execute(update_query, (estado, accion, hash_detalle, ref, id))
                else:
                    # Insert new record
                    insert_query = """
                        INSERT INTO detalle_estado (
                            id, folio, hash_detalle, fecha, estado, accion, ref
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """
                    cursor.execute(insert_query, (id, folio, hash_detalle, fecha, estado, accion, ref))
                
                # Commit changes
                conn.commit()
                
                # Check if operation was successful
                cursor.execute("SELECT id FROM detalle_estado WHERE id = ?", (id,))
                result = cursor.fetchone()
                
                return result != None
                    
        except sqlite3.Error as e:
            logging.error(f"SQLite error al insertar/actualizar detalle: {e}")
            return False
        except Exception as e:
            logging.error(f"Error al insertar/actualizar detalle: {e}")
            return False
    
    def get_details_by_folio(self, folio: str) -> List[Dict]:
        """
        Obtiene todos los detalles asociados a un folio específico
        
        Args:
            folio: Número de folio a consultar
            
        Returns:
            Lista de diccionarios con los detalles encontrados
        """
        try:
            # Connect to SQLite database
            with sqlite3.connect(self.config['database']) as conn:
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                
                # Create cursor and set row_factory to get dictionary-like results
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                query = """
                    SELECT id, folio, hash_detalle, fecha, estado, accion, ref
                    FROM detalle_estado
                    WHERE folio = ?
                    ORDER BY id ASC
                """
                
                cursor.execute(query, (folio,))
                
                # Convert to list of dictionaries
                results = [dict(row) for row in cursor.fetchall()]
                return results
                    
        except sqlite3.Error as e:
            logging.error(f"SQLite error al obtener detalles por folio: {e}")
            return []
        except Exception as e:
            logging.error(f"Error al obtener detalles por folio: {e}")
            return []
    
    def get_details_by_date_range(self, start_date: date, end_date: date) -> List[Dict]:
        """
        Obtiene todos los detalles en un rango de fechas
        
        Args:
            start_date: Fecha inicial del rango
            end_date: Fecha final del rango
            
        Returns:
            Lista de diccionarios con los detalles encontrados
        """
        try:
            # Connect to SQLite database
            with sqlite3.connect(self.config['database']) as conn:
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                
                # Create cursor and set row_factory to get dictionary-like results
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                query = """
                    SELECT id, folio, hash_detalle, fecha, estado, accion, ref
                    FROM detalle_estado
                    WHERE fecha BETWEEN ? AND ?
                    ORDER BY fecha DESC, folio ASC
                """
                
                cursor.execute(query, (start_date, end_date))
                
                # Convert to list of dictionaries
                results = [dict(row) for row in cursor.fetchall()]
                return results
                    
        except sqlite3.Error as e:
            logging.error(f"SQLite error al obtener detalles por rango de fechas: {e}")
            return []
        except Exception as e:
            logging.error(f"Error al obtener detalles por rango de fechas: {e}")
            return []

    def insert_details_on_wait(self, details: List[Dict], action, estado) -> bool:
        try:
            # Connect to SQLite database
            with sqlite3.connect(self.config['database']) as conn:
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                
                deleted_count = 0
                inserted_count = 0
               
                for detail in details:
                    detail_id = detail.get('id')
                    
                    try:
                        cursor = conn.cursor()
                        
                        # Check if record exists with the same folio and indice
                        check_query = "SELECT id FROM detalle_estado WHERE folio = ? AND indice = ?"
                        cursor.execute(check_query, (detail.get('folio'), detail.get('indice')))
                        existing_record = cursor.fetchone()
                        
                        if existing_record:
                            # Update existing record
                            update_query = """
                                UPDATE detalle_estado
                                SET id = ?,
                                    hash_detalle = ?,
                                    fecha = ?,
                                    estado = ?,
                                    accion = ?,
                                    ref = ?
                                WHERE folio = ? AND indice = ?
                            """
                            params = (
                                detail_id,
                                detail.get('detail_hash'),
                                detail.get('fecha'),
                                estado,
                                action,
                                detail.get('ref'),
                                detail.get('folio'),
                                detail.get('indice')
                            )
                            cursor.execute(update_query, params)
                        else:
                            # Insert new record
                            insert_query = """
                                INSERT INTO detalle_estado (
                                    id, folio, hash_detalle, fecha, estado, accion, ref, indice
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """
                            params = (
                                detail_id,
                                detail.get('folio'),
                                detail.get('detail_hash'),
                                detail.get('fecha'),
                                estado,
                                action,
                                detail.get('ref'),
                                detail.get('indice')
                            )
                            cursor.execute(insert_query, params)
                        
                        inserted_count += 1
                        
                        print(f'detail_tracking :: INSERT REPLACE: ID={detail_id}, FOLIO={detail.get("folio")}, HASH={detail.get("detail_hash")}, '
                              f'FECHA={detail.get("fecha")}, ESTADO={estado}, ACCION={action}, REF={detail.get("ref")}, INDICE={detail.get("indice")}')
                        
                        # Commit the transaction for this ID
                        conn.commit()
                        print(f"Successfully processed ID {detail_id}: deleted {deleted_count}, inserted {inserted_count}")
                        
                    except sqlite3.Error as e:
                        logging.error(f"SQLite error in insert details on wait: {e}")
                        return False
                    except Exception as e:
                        logging.error(f"DETAILS :: Error in insert details on wait: {e}")
                        return False
                
                return inserted_count > 0
                
        except sqlite3.Error as e:
            logging.error(f"SQLite error in insert details on wait: {e}")
            return False
        except Exception as e:
            logging.error(f"detail_tracking :: Error insert details on wait: {e}")
            return False
    
    def batch_replace_by_id(self, details: List[Dict], action, estado) -> bool:
        """
        Procesa múltiples detalles en una sola transacción, utilizando el ID como referencia
        principal en lugar del folio.
        
        Args:
            details: Lista de diccionarios con los detalles a insertar
                Cada diccionario debe contener: id, folio, hash_detalle, fecha, estado, accion
                
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        if not details:
            return True  # Nothing to process
        
        try:
            # Connect to SQLite database
            with sqlite3.connect(self.config['database']) as conn:
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                
                # Group details by ID
                print(f'DETAILS {details}')

                details_by_id = {}
                for detail in details:
                    print(f'batch_replace_by_id {detail}')
                    detail_id = detail.get('id') or detail.get('sql_id')  # here goes the id not parent id
                    if detail_id:
                        if detail_id not in details_by_id:
                            details_by_id[detail_id] = []
                        details_by_id[detail_id].append(detail)
                
                # Track successful operations
                deleted_count = 0
                inserted_count = 0
                
                # Process each ID in a separate transaction
                for detail_id, id_details in details_by_id.items():
                    try:
                        cursor = conn.cursor()
                        
                        # First delete all existing records for this ID
                        delete_query = "DELETE FROM detalle_estado WHERE id = ?"
                        cursor.execute(delete_query, (detail_id,))
                        deleted_count += cursor.rowcount
                        print(f"Deleted {cursor.rowcount} existing records for ID {detail_id}")
                        
                        # Then insert all new records for this ID
                        # Insert query
                        insert_query = """
                            INSERT INTO detalle_estado (
                                id, folio, hash_detalle, fecha, estado, accion, ref
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        # Insert each detail (should be just one per ID)
                        for detail in id_details:
                            # Get the folio from the detail
                            folio = detail.get('folio', '')
                            
                            # Get current date if fecha is not provided
                            fecha = detail.get('fecha')
                            if not fecha:
                                fecha = date.today()
                            
                            # Get the REF value
                            ref_value = ''
                            if 'REF' in detail:
                                ref_value = detail['REF']
                            elif 'ref' in detail:
                                ref_value = detail['ref']
                            
                            # Extract values
                            detail_hash = detail.get('hash_detail') or detail.get('hash_detalle') or detail.get('detail_hash')
                          
                            params = (
                                detail_id,  # Use the actual ID from the API
                                folio,
                                detail_hash,
                                fecha,
                                estado,
                                action,
                                ref_value
                            )
                            
                            logging.info(f'detail_tracking :: insert_details_on_wait: ID={detail_id}, FOLIO={folio}, HASH={detail_hash}, '
                                   f'FECHA={fecha}, ESTADO={estado}, ACCION={action}, REF={ref_value}')
                            
                            cursor.execute(insert_query, params)
                            inserted_count += 1
                    
                        # Commit the transaction for this ID
                        conn.commit()
                        print(f"Successfully processed ID {detail_id}: deleted {deleted_count}, inserted {inserted_count}")
                        
                    except sqlite3.Error as e:
                        # If anything goes wrong, rollback this ID's transaction
                        conn.rollback()
                        logging.error(f"SQLite error processing ID {detail_id}: {e}")
                        inserted_count = 0
                        # Continue with the next ID
                    except Exception as e:
                        # If anything goes wrong, rollback this ID's transaction
                        conn.rollback()
                        logging.error(f"Error processing ID {detail_id}: {e}")
                        inserted_count = 0
                        # Continue with the next ID
                        
                return inserted_count > 0
                
        except sqlite3.Error as e:
            logging.error(f"SQLite error in batch_replace_by_id: {e}")
            return False
        except Exception as e:
            logging.error(f"DETAILS :: Error in batch_replace_by_id: {e}")
            return False
    
    def batch_insert_details(self, details: List[Dict]) -> bool:
        """
        Inserta múltiples detalles en una sola transacción
        
        Args:
            details: Lista de diccionarios con los detalles a insertar
                Cada diccionario debe contener: folio, hash_detalle, fecha, estado, accion
                
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        
        if not details:
            return True  # Nothing to insert
            
        try:
            # Connect to SQLite database
            with sqlite3.connect(self.config['database']) as conn:
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                
                # First, get existing folios to determine starting counters
                folio_counters = {}
                
                try:
                    cursor = conn.cursor()
                    # Query to get max index for each folio - SQLite doesn't have SPLIT_PART
                    # We'll use a different approach to extract the index
                    count_query = """
                        SELECT folio, MAX(SUBSTR(id, INSTR(id, '-') + 1)) as max_index
                        FROM detalle_estado
                        GROUP BY folio
                    """
                    cursor.execute(count_query)
                    
                    # Initialize counters based on existing data
                    for row in cursor.fetchall():
                        folio, max_index = row
                        try:
                            folio_counters[folio] = int(max_index) if max_index else 0
                        except (ValueError, TypeError):
                            folio_counters[folio] = 0
                except sqlite3.Error as e:
                    logging.warning(f"SQLite error retrieving existing counters: {e}")
                except Exception as e:
                    logging.warning(f"Could not retrieve existing counters: {e}")
                
                # Continue with inserts
                cursor = conn.cursor()
                
                # Track successful inserts
                success_count = 0
                
                for detail in details:
                    print(f' $$$$ inserting record detail : {detail}')
                    folio = detail.get('folio')
                    print(f"checkpoint______________________________")
                    
                    # Initialize counter for this folio if not exists
                    if folio not in folio_counters:
                        folio_counters[folio] = 0
                    
                    # Increment counter for this folio
                    folio_counters[folio] += 1
                    
                    # Get ID from detail
                    detail_id = detail.get('id')
                    
                    # Get current date if fecha is not provided
                    fecha = detail.get('fecha')
                    if not fecha:
                        fecha = date.today()
                    
                    # Get the REF value - check both 'REF' and 'ref' keys to handle case sensitivity
                    ref_value = ''
                    if 'REF' in detail:
                        ref_value = detail['REF']
                    elif 'ref' in detail:
                        ref_value = detail['ref']
                    
                    # Extract values for better debugging
                    detail_hash = detail.get('detail_hash') or detail.get('hash_detalle')
                    estado = detail.get('estado', 'completado')
                    operation = detail.get('operation') or detail.get('accion', 'create')
                    
                    try:
                        # Check if record exists with the same folio and ref
                        check_query = "SELECT id FROM detalle_estado WHERE folio = ? AND ref = ?"
                        cursor.execute(check_query, (folio, ref_value))
                        existing_record = cursor.fetchone()
                        
                        if existing_record:
                            # Update existing record
                            update_query = """
                                UPDATE detalle_estado
                                SET estado = ?,
                                    accion = ?,
                                    hash_detalle = ?,
                                    ref = ?
                                WHERE folio = ? AND ref = ?
                            """
                            params = (
                                estado,
                                operation,
                                detail_hash,
                                ref_value,
                                folio,
                                ref_value
                            )
                            cursor.execute(update_query, params)
                        else:
                            # Insert new record
                            insert_query = """
                                INSERT INTO detalle_estado (
                                    id, folio, hash_detalle, fecha, estado, accion, ref
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """
                            params = (
                                detail_id,
                                folio,
                                detail_hash,
                                fecha,
                                estado,
                                operation,
                                ref_value
                            )
                            cursor.execute(insert_query, params)
                        
                        # Detailed debug print to identify values
                        print(f'DEBUG INSERT: ID={detail_id}, FOLIO={folio}, HASH={detail_hash}, '
                              f'FECHA={fecha}, ESTADO={estado}, ACCION={operation}, REF={ref_value}')
                        print(f'ORIGINAL DETAIL: {detail}')
                        
                        success_count += 1
                    except sqlite3.Error as e:
                        # Log the error but continue with other records
                        logging.error(f"SQLite error inserting record {detail_id}: {e}")
                        conn.rollback()
                        continue
                    except Exception as e:
                        # Log the error but continue with other records
                        logging.error(f"DETAILS :: Error inserting record {detail_id}: {e}")
                        conn.rollback()
                        continue
                
                conn.commit()
                return success_count > 0
                
        except sqlite3.Error as e:
            logging.error(f"SQLite error al insertar detalles en lote: {e}")
            return False
        except Exception as e:
            logging.error(f"DETAILS :: Error al insertar detalles en lote: {e}")
            return False


    def delete_by_folio(self, folio) -> bool:
        """
        Elimina todos los registros asociados a un folio específico
        
        Args:
            folio: Número de folio a eliminar
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        try:
            # Connect to SQLite database
            with sqlite3.connect(self.config['database']) as conn:
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                
                # Create cursor
                cursor = conn.cursor()
                
                query = """
                    DELETE FROM detalle_estado
                    WHERE folio = ?
                """
                
                cursor.execute(query, (folio,))
                
                # Get number of rows affected
                rows_deleted = cursor.rowcount
                conn.commit()
                
                return rows_deleted > 0
                
        except sqlite3.Error as e:
            logging.error(f"SQLite error al eliminar registros por folio: {e}")
            return False
        except Exception as e:
            logging.error(f"Error al eliminar registros por folio: {e}")
            return False

    def delete_by_id(self, id) -> bool:
        """
        Elimina un registro específico por su ID
        
        Args:
            id: Identificador único del registro a eliminar
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        try:
            # Connect to SQLite database
            with sqlite3.connect(self.config['database']) as conn:
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                
                # Create cursor
                cursor = conn.cursor()
                
                query = """
                    DELETE FROM detalle_estado
                    WHERE id = ?
                """
                
                cursor.execute(query, (id,))
                
                # Get number of rows affected
                rows_deleted = cursor.rowcount
                conn.commit()
                
                return rows_deleted > 0
                
        except sqlite3.Error as e:
            logging.error(f"SQLite error al eliminar registro por ID: {e}")
            return False
        except Exception as e:
            logging.error(f"Error al eliminar registro por ID: {e}")
            return False