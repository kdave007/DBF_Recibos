import json
import random
from datetime import datetime

class ResponseSimulator:
    """
    Class to simulate API responses for debugging and testing purposes.
    Generates dynamic responses based on the input data structure.
    """
    
    @staticmethod
    def simulate_response(record, folio, status_code=200):
        """
        Simulate an API response based on the provided DBF record.
        
        Args:
            record (dict): The DBF record containing details and receipts
            folio (str): The folio number for the record
            status_code (int): HTTP status code to simulate
            
        Returns:
            tuple: (status_code, response_json)
        """
        # Generate random IDs for the response
        ca_id = random.randint(1, 100)
        id_cta_cor_t = random.randint(1, 100)
        id_dtl_doc_cob_t = random.randint(1, 100)
        id_rbo_cob_t = random.randint(1, 100)
        
        # Create the base response structure with new format
        response = {
            "CA": {
                "id": ca_id,
                "folio": folio
            },
            "MENSAJE": "",
            "ST": "OK",
            "PA": [],
            "CO": {
                "CTA_COR_T": [],
                "DTL_COB_APL_T": [],
                "DTL_DOC_COB_T": []
            }
        }
        
        # Generate PA (partidas) entries based on details in the record
        total_details = record.get('total_partidas', 0)
        for i in range(1, total_details + 1):
            pa_entry = {
                "id": random.randint(1, 1000),
                "_indice": i
            }
            response["PA"].append(pa_entry)
            # response["PA"].append(pa_entry)#
        
        # Generate entries for CO arrays - only DTL_COB_APL_T can have multiple records
        
        # Generate CTA_COR_T entries (always 1 record)
        cta_cor_entry = {
            "_indice": 1,
            "id": random.randint(1000, 9999)
        }
        response["CO"]["CTA_COR_T"].append(cta_cor_entry)
        
        # Generate DTL_COB_APL_T entries (1-3 records)
        dtl_cob_apl_count = random.randint(1, 3)
        for i in range(1, dtl_cob_apl_count + 1):
            dtl_cob_apl_entry = {
                "ID_DTL_COB_APL": i,
                "_indice": i
            }
            response["CO"]["DTL_COB_APL_T"].append(dtl_cob_apl_entry)
        
        # Generate DTL_DOC_COB_T entries (always 1 record)
        dtl_doc_cob_entry = {
            "ID_DTL_DOC_COB_T": 1,
            "ID_RBO_COB_T": random.randint(1000, 9999),
            "_indice": 1
        }
        response["CO"]["DTL_DOC_COB_T"].append(dtl_doc_cob_entry)
        
        return status_code, response
    
    @staticmethod
    def create_mock_response(status_code, json_data):
        """
        Create a mock response object that mimics requests.Response
        
        Args:
            status_code (int): HTTP status code
            json_data (dict): JSON data to return from .json() method
            
        Returns:
            MockResponse: A mock response object
        """
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code
                self.text = json.dumps(json_data)
                self.headers = {
                    'Content-Type': 'application/json',
                    'Date': datetime.now().strftime('%a, %d %b %Y %H:%M:%S GMT')
                }
            
            def json(self):
                return self.json_data
        
        return MockResponse(json_data, status_code)

        
    @staticmethod
    def simulate_id_response(min_value=1, max_value=1000):
        """
        Simulate a simple ID response with a plan number greater than 0.
        
        Args:
            min_value (int): Minimum value for the ID (default: 1)
            max_value (int): Maximum value for the ID (default: 1000)
            
        Returns:
            MockResponse: A mock response object containing the ID as plain text
        """
        id_value = random.randint(min_value, max_value)
        
        class MockResponse:
            def __init__(self, id_value):
                self.status_code = 200
                # Store the ID as the response text directly
                self.text = str(id_value)
                self.headers = {
                    'Content-Type': 'text/plain',
                    'Date': datetime.now().strftime('%a, %d %b %Y %H:%M:%S GMT')
                }
            
            def json(self):
                # This will raise an exception if called, as this is not JSON
                raise ValueError("Response is not JSON format")
        
        return MockResponse(id_value)
