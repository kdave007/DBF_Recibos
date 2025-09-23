from ast import Try
import os
import sys
from turtle import st
from pathlib import Path
from src.config.db_config import SQLiteConnection
from src.db.response_tracking import ResponseTracking
from src.db.api_request_tracking import APIRequestTracking
from src.utils.response_simulator import ResponseSimulator
import requests
import json
import logging
from decimal import Decimal
from datetime import datetime, date
from dotenv import load_dotenv
from src.utils.get_enc import EncEnv
from src.controllers.verify_document import VerifyDocument

# Load environment variables
load_dotenv()

# Set debug flag from .env - set to True to use simulated responses instead of real API calls
env = EncEnv()
DEBUG_MODE = env.get('DEBUG_MODE', 'True').lower() == 'true'
CLIENT_ID = env.get('CLIENT_ID')

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

class SendRequest:

    def __init__(self):
        # Get database configuration as a dictionary
        self.db_config = SQLiteConnection.get_db_config()
        # Initialize ResponseTracking with the configuration dictionary
        self.response_tracking = ResponseTracking(self.db_config)
        # Initialize API request tracking
        self.api_request_tracking = APIRequestTracking(self.db_config)

    

    def waiting_line(self, record, single_payload, tipo_doc, base_url, api_key):
        """
        Process a single record and send it to the API
        
        Args:
            record: A single record dictionary containing 'folio' and 'dbf_record'
            
        Returns:
            Dictionary with 'success' and 'failed' lists containing the operation result
        """
        results = {
            'success': [],  # Will store folio -> result for successful operations
            'failed': []   # Will store folio -> result for failed operations
        }

        headers = {
            "Content-Type": "application/json",
         
             "accept": "*/*",
  
            "accept-encoding": "gzip, deflate, br",
        }
        
        folio = record.get('folio')
        dbf_record = record.get('dbf_record', {})

        
        # Add decorative logging for sending folio
        border = "=" * 80

        if len(dbf_record.get('detalles', [])) == 0 or (len(dbf_record.get('recibos', [])) == 0 and tipo_doc == "FA") :
            logging.warning(f"Declined send request for folio {folio} found with {len(dbf_record.get('detalles', []))} detalles and {len(dbf_record.get('recibos', []))} recibos")
            results['failed'].append({
                        'folio': folio,
                        'fecha_emision': dbf_record.get('fecha'),
                        'total_partidas': len(dbf_record.get('detalles', [])),
                        'hash': "",
                        'status': 500,
                        'error_msg': "Skipped due to empty recibos or detalles"
                    })
            
            return results
            
        
        logging.info(f"SENDING REQUEST FOR FOLIO: {folio}, det:{len(dbf_record.get('detalles', []))} , rec:{len(dbf_record.get('recibos', []))}")
       
        
        try:
            # Use the pre-formatted payload passed from PayloadFormatter
            # No need to create payload here since it's already formatted
            
            print(f' OG RECORD AS : {dbf_record}')
            # Send the record
            print(f"Sending record for folio {folio}")
            post_data = json.dumps(single_payload, cls=CustomJSONEncoder, indent=4)
            print(f"POST Request URL: {base_url}?api_key={api_key}")
            print(f"POST Request Data:\n{post_data}")
            
            # Log the API request and get record ID
            api_log_id = self.api_request_tracking.log_request(folio, single_payload)
            
            # Use simulated response if DEBUG_MODE is enabled
            if DEBUG_MODE:
                print(f"DEBUG MODE: Using simulated response for folio {folio}")
                # status_code, response_json = ResponseSimulator.simulate_response(dbf_record, folio)
                # response = ResponseSimulator.create_mock_response(status_code, response_json)
                response = ResponseSimulator.simulate_id_response()

            else:
                # Make actual API request
                try:
                    response = requests.post(
                        f"{base_url}?api_key={api_key}", 
                        headers=headers,
                        data=post_data,
                        timeout=60,  # Set timeout to 60 seconds
                        verify=False  # Disable SSL certificate verification
                    )
                    # Warning: verify=False disables SSL certificate verification, which is not recommended for production
                except requests.exceptions.Timeout:
                    error_msg = f"Request timed out after 60 seconds for folio {folio}"
                    logging.error(error_msg)
                    print(error_msg)
                    raise TimeoutError(error_msg)
                except requests.exceptions.ConnectionError as e:
                    error_msg = f"Connection error for folio {folio}: {str(e)}"
                    logging.error(error_msg)
                    print(error_msg)
                    raise ConnectionError(error_msg)
            
            print(f"Response Status Code for folio {folio}: {response.status_code}")
            print(f"Response Headers for folio {folio}: {response.headers}")

            logging.info(f"Response Status Code for folio {folio}: {response.status_code}")
            
            # Process the response
            """TODO: 
                -Create a loop to send all fac ?
                -Send first the json create post request, this should get an ID or a 0, if an ID is received, it means this json data its being processed by the server...
                -Save in a table the Folio (num_doc), year (date ) and serie, this table will contain the waiting list of fac in process
                -Later on make a GET request with the 3 params so the server will respond with the required ID's to save in the DB log.
            """ 
            response_value = response.text

            # Validate response: good status code and non-empty, non-zero ID
            is_valid_response = (
                response.status_code in [200, 201, 202, 204] and 
                response_value and 
                response_value.strip() and 
                response_value.strip() != "0"
            )
            
            # Log the POST response
            self.api_request_tracking.update_post_response(api_log_id, response_value)
            
            if is_valid_response:
                
                # formatted_json = json.dumps(response_json, indent=4, sort_keys=False)
                logging.info(f"Response waiting line ID for folio {folio}: {response_value}")

                success_entry = {
                        'folio': folio,
                        'id': response_value,
                        'fecha_emision': dbf_record.get('fecha'),
                        'total_partidas': len(dbf_record.get('detalles', [])),
                        'total_recibos': len(dbf_record.get('recibos', [])),
                        'hash': dbf_record.get('md5_hash', ''),
                        'status': response.status_code,
                        'accion':'enviado',
                        'estado':'pendiente',
                        'partidas': [],
                        'recibos': []
                }

                if len(dbf_record.get('detalles', [])) > 0 :
                    for index, detail in enumerate(dbf_record.get('detalles', []),1):
                        data = {
                            'id': 0,
                            'indice': index,
                            'folio': str(folio),
                            'ref': detail['REF'],
                            'fecha': single_payload.get('fch', ''),
                            'detail_hash':detail['detail_hash'],
                        }

                        success_entry['partidas'].append(data)
                                

                if len(single_payload.get('recibos')) > 0 :
                    for index, rec in enumerate(dbf_record.get('recibos', []),1):
                        data = {
                            'id_dtl_cob_apl_t': None,
                            'id_cta_cor_t': None,
                            'id_dtl_doc_cob_t': None,
                            'id_rbo_cob_t': None,
                            'id_fac': 0,
                            'indice':index,
                            'accion':'enviado',
                            'estado':'pendiente',
                            'num_ref':rec.get('ref_recibo'),
                            'folio': str(folio),  # From CA
                            'fecha': single_payload.get('fch')
                        }

                        success_entry['recibos'].append(data)
                
                results['success'].append(success_entry)
                print(f"Successfully processed response for folio {folio}")
                    
            else:
                results['failed'].append({
                    'folio': folio,
                    'fecha_emision': dbf_record.get('fecha'),
                    'total_partidas': len(dbf_record.get('detalles', [])),
                    'total_recibos': len(dbf_record.get('recibos', [])),
                    'hash': dbf_record.get('dbf_hash', ''),
                    'status': response.status_code,
                    'error_msg': "response by server : "+response_value
                })                    
                

        #     
        except Exception as e:
            logging.info(f"Exception during create operation: {str(e)}")
            error_message = f"Exception during create operation: {str(e)}"
            print(error_message)
            # Mark the record as failed
            results['failed'].append({
                'folio': folio, 
                'fecha_emision': dbf_record.get('fecha'),
                'hash': record.get('dbf_hash', ''),
                'status': None,
                'error_msg': error_message
            })
                  
        return results
