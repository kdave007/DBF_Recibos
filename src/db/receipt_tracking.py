
import psycopg2
import logging
from typing import List, Dict
from datetime import date, datetime

class ReceiptTracking:
    
    def __init__(self, db_config: dict):
        self.config = db_config
        
    def batch_replace_by_id(self, receipts: List[Dict]) -> bool:
        """
        Procesa múltiples recibos en una sola transacción, utilizando el ID como referencia
        principal.
        
        Args:
            receipts: Lista de diccionarios con los recibos a insertar
                Cada diccionario debe contener: id, id_fac
                
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        if not receipts:
            return True  # Nothing to process
            
        try:
            # Connect with explicit parameters instead of using **
            with psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            ) as conn:
                # Group receipts by ID
                print(f'RECEIPTS {receipts}')

                receipts_by_id = {}
                for receipt in receipts:
                    print(f'batch_replace_by_id {receipt}')
                    receipt_id = receipt.get('id')  # ID from the API response
                    if receipt_id:
                        if receipt_id not in receipts_by_id:
                            receipts_by_id[receipt_id] = []
                        receipts_by_id[receipt_id].append(receipt)
                
                # Track successful operations
                deleted_count = 0
                inserted_count = 0
                
                # Process each ID in a separate transaction
                for receipt_id, id_receipts in receipts_by_id.items():
                    try:
                        # First delete all existing records for this ID
                        with conn.cursor() as cursor:
                            delete_query = "DELETE FROM recibo_venta WHERE id = %s"
                            cursor.execute(delete_query, (receipt_id,))
                            deleted_count += cursor.rowcount
                            print(f"Deleted {cursor.rowcount} existing records for receipt ID {receipt_id}")
                            
                        # Then insert all new records for this ID
                        with conn.cursor() as cursor:
                            # Insert query with updated fields
                            insert_query = """
                                INSERT INTO recibo_venta (
                                    id, folio, num_ref, dtl_doc_cob_t, cta_cor_t, rbo_cob_t, 
                                    hash, estado, fecha_emision, fecha_procesamiento
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                            """
                            
                            # Insert each receipt (should be just one per ID)
                            for receipt in id_receipts:
                                # Get fields from receipt
                                num_ref = receipt.get('num_ref', '')
                                folio = receipt.get('folio', '')
                                
                                # Get the new fields
                                dtl_doc_cob_t = receipt.get('id_dtl_doc_cob_t', None)
                                cta_cor_t = receipt.get('id_cta_cor_t', None)
                                rbo_cob_t = receipt.get('id_rbo_cob_t', None)
                                
                                # Get current date if fecha_emision is not provided
                                fecha_emision = receipt.get('fecha_emision')
                                if not fecha_emision:
                                    fecha_emision = date.today()
                                
                                # Ensure fecha_emision is date only (no time component)
                                if isinstance(fecha_emision, datetime):
                                    fecha_emision = fecha_emision.date()
                                elif isinstance(fecha_emision, str):
                                    # Try to parse the date string and extract only the date part
                                    try:
                                        # First try standard ISO format
                                        fecha_emision = datetime.strptime(fecha_emision, '%Y-%m-%d').date()
                                    except ValueError:
                                        try:
                                            # Then try dd/mm/yyyy format
                                            fecha_emision = datetime.strptime(fecha_emision, '%d/%m/%Y').date()
                                        except ValueError:
                                            try:
                                                # Then try with time component
                                                fecha_emision = datetime.strptime(fecha_emision.split()[0], '%d/%m/%Y').date()
                                            except ValueError:
                                                # If all parsing fails, use today's date
                                                fecha_emision = date.today()
                                
                                # Hash will be empty for now
                                hash_value = ''
                                
                                # Estado is always "completado"
                                estado = 'completado'
                                
                                params = (
                                    receipt_id,  # Use the actual ID from the API
                                    folio,
                                    num_ref,
                                    dtl_doc_cob_t,
                                    cta_cor_t,
                                    rbo_cob_t,
                                    hash_value,
                                    estado,
                                    fecha_emision
                                )
                                
                                # Debug print
                                print(f'REPLACE RECEIPT: ID={receipt_id}, NUM_REF={num_ref}, '
                                      f'DTL_DOC_COB_T={dtl_doc_cob_t}, CTA_COR_T={cta_cor_t}, RBO_COB_T={rbo_cob_t}, '
                                      f'HASH={hash_value}, FECHA_EMISION={fecha_emision}, ESTADO={estado}')
                                
                                cursor.execute(insert_query, params)
                                inserted_count += 1
                        
                        # Commit the transaction for this ID
                        conn.commit()
                        print(f"Successfully processed receipt ID {receipt_id}: deleted {deleted_count}, inserted {inserted_count}")
                        
                    except Exception as e:
                        # If anything goes wrong, rollback this ID's transaction
                        conn.rollback()
                        logging.error(f"Error processing receipt ID {receipt_id}: {e}")
                        # Continue with the next ID
                        
                return inserted_count > 0
                
        except Exception as e:
            logging.error(f"Error in batch_replace_by_id: {e}")
            return False
    