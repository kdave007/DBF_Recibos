from src.utils.get_enc import EncEnv

class PayloadFormatter :
    def __init__(self) -> None:
        env = EncEnv()
        DEBUG_MODE = env.get('DEBUG_MODE', 'True').lower() == 'true'
        self.CLIENT_ID = env.get('CLIENT_ID')


    def get(self, record):
        try:
            folio = record.get('folio')
            dbf_record = record.get('dbf_record', {})

            formatted_details = self._format_details(dbf_record)
            formatted_receipts = self._format_receipts(dbf_record)
            # Prepare payload for a single record
            single_payload = {
                "emp": str(dbf_record.get('emp')),
                "emp_div": str(dbf_record.get('emp_div')),
                "num_doc": folio,
                #   "num_doc": folio,
                # "clt": dbf_record.get('clt'),
                "clt": int(self.CLIENT_ID),
                "fpg": dbf_record.get('fpg'),
                # "fpg": 20,
                "cmr": dbf_record.get('cmr'),
                "fch": self._format_date_to_iso(dbf_record.get("fecha")),
                # "tot_fac": dbf_record.get("total_bruto"),
                "ser": dbf_record.get('ser'),
                "hor": dbf_record.get('hor'),
                "pai": dbf_record.get('pai'),
                "ent_rel_tip": 1,
                "mon_c": 1,
                "cot": 1,
                "fch_vto": self._format_date_to_iso(dbf_record.get("fecha")),
                "pre_con_iva_inc": 0,
                "trm": 1,
                "dum": 1,
                "alm": str(dbf_record.get('alm')),
                "fac": "1",
                "off": 1,
                "detalles": formatted_details ,
                "recibos": formatted_receipts,
                "usr":1,
                "aut_usr":1,
                "por_dto":0,
                "num_det":len(formatted_details),
                "num_rec":len(formatted_receipts)
                }
        except Exception as e:
                print(f'Error preparing payload: {e}')
                raise

        return single_payload


    def _format_date_to_iso(self, date_str):
        """
        Convert date from format like "30/04/2025 12:00:00 a. m." to "2025-04-30"
        
        Args:
            date_str: Date string in DD/MM/YYYY format with possible time component
            
        Returns:
            Date string in YYYY-MM-DD format
        """
        if not date_str:
            return ""
            
        try:
            # Split by space to separate date and time
            parts = date_str.split(' ')
            date_part = parts[0]
            
            # Split the date part by /
            day, month, year = date_part.split('/')
            
            # Format to YYYY-MM-DD
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        except Exception as e:
            print(f"Error formatting date {date_str}: {e}")
            return date_str  # Return original if parsing fails
            
    def _format_hour_to_12h(self, hour_value):
        """
        Format hour value with minutes and seconds
        
        Args:
            hour_value: Integer representing hour in 24-hour format (0-23)
            
        Returns:
            String in format "hh:00:00"
        """
        if hour_value is None:
            return ""
            
        try:
            # Convert to integer if it's a string
            if isinstance(hour_value, str):
                hour_value = int(hour_value)
                
            # Format hour with minutes and seconds
            return f"{hour_value:02d}:00:00"
        except Exception as e:
            print(f"Error formatting hour: {e}")
            return f"{hour_value}:00:00"  # Return original if parsing fails
            
    def _format_details(self, parent_ref):
        records = parent_ref.get('detalles')
        array_payload = []
        for index, record in enumerate(records,1):
            single_payload = {
                        "_indice":index,
                        "alm":str(record.get('alm')),
                        "art": record.get('art'),
                        "und_med":1,
                        "can_und": record.get('cantidad'),
                        "can":record.get('cantidad'),
                        "emp_div": str(record.get('emp_div')),
                        "emp": str(record.get('emp')),
                        "fch": self._format_date_to_iso(parent_ref.get("fecha")),
                        "hor":record.get('hor'),
                        # "pre": float(record.get('precio', 0)) ,
                        "pre": float(record.get('precio', 0)) + float(record.get('n_descto_1', 0)) + float(record.get('n_descto_2', 0)) ,
                        # "pre": float(record.get('imp_part', 0)) + float(record.get('iva_part', 0)),
                        "por_dto": record.get('descuento'),
                        "reg_iva_vta":record.get('reg_iva_vta'),
                        # "vta_fac": parent_ref.get('parent_id'),
                        "clt":record.get('clt'),
                        "mov_tip":record.get('mov_tip'),
                        "cal_arr":1
                        
                    }
            array_payload.append(single_payload)
        return array_payload

    def _format_receipts(self, parent_ref):
        records = parent_ref.get('recibos')
        array_payload = []
     
        for index, record in enumerate(records,1):
            single_payload = {
                        "_indice":index,
                        "ser":3,
                        "fch": self._format_date_to_iso(parent_ref.get("fecha")),
                        "ref_recibo": record.get('ref_recibo'),
                        "importe": record.get('importe'),
                        "caja_bco": record.get('caja_bco'),
                        # "caja_bco": None,
                        "tienda": record.get('tienda'),
                        "ref_tipo": record.get('ref_tipo'),
                        "hora": record.get('hora'),
                        "num_doc": f"{record.get('plaza')}-{record.get('tienda')}-{record.get('ref_tipo')}-{record.get('ref_recibo')}",
                        "fpg": record.get('fpg')
                    }
            print(record)        
            array_payload.append(single_payload)
        return array_payload