

import logging

class VerifyDocument:

   def isReady(self, record):
      logging.info(f'Validating record: {record}')
      
      # Check header fields
      if not self.check_header(record):
         return False
      
      # Check details if they exist
      details = record.get('detalles', [])
      if details and not self.check_details(details):
         return False
      
      # Check receipts if they exist
      receipts = record.get('recibos', [])
      if receipts and not self.check_receipts(receipts):
         return False
      
      # All validations passed
      return True


   def check_header(self, record):
      # Check if emp exists and is not empty or just whitespace
      emp = record.get('emp')
      if emp is None or emp.strip() == "":
         logging.error(f"verify document :: 'emp' field is missing or empty in record {record.get('emp')}")
         return False
      
      # Check emp_div field
      emp_div = record.get('emp_div')
      if emp_div is None or emp_div.strip() == "":
         logging.error(f"verify document :: 'emp_div' field is missing or empty in record")
         return False
      
      # Check num_doc field
      num_doc = record.get('num_doc')
      if num_doc is None or str(num_doc).strip() == "":
         logging.error(f"verify document :: 'num_doc' field is missing or empty in record")
         return False
      
      # Check clt field - must exist and not be 0
      clt = record.get('clt')
      if clt is None or clt == 0:
         logging.error(f"verify document :: 'clt' field is missing or zero in record")
         return False
      
      # Check fpg field - must exist and not be 0
      fpg = record.get('fpg')
      if fpg is None or fpg == 0:
         logging.error(f"verify document :: 'fpg' field is missing or zero in record")
         return False
      
      # Check cmr field - must exist and not be 0
      cmr = record.get('cmr')
      if cmr is None or cmr == 0:
         logging.error(f"verify document :: 'cmr' field is missing or zero in record")
         return False
      
      # Check fch field (date) - must exist and not be empty
      fch = record.get('fch')
      if fch is None or str(fch).strip() == "":
         logging.error(f"verify document :: 'fch' field is missing or empty in record")
         return False
      
      # Check ser field - must exist and not be empty
      ser = record.get('ser')
      if ser is None or str(ser).strip() == "":
         logging.error(f"verify document :: 'ser' field is missing or empty in record")
         return False
      
      # Check hor field (time) - must exist and not be empty
      hor = record.get('hor')
      if hor is None or str(hor).strip() == "":
         logging.error(f"verify document :: 'hor' field is missing or empty in record")
         return False
      
      # Check pai field - must exist and not be 0
      pai = record.get('pai')
      if pai is None or pai == 0:
         logging.error(f"verify document :: 'pai' field is missing or zero in record")
         return False

      # Check fch_vto field (date) - must exist and not be empty
      fch_vto = record.get('fch_vto')
      if fch_vto is None or str(fch_vto).strip() == "":
         logging.error(f"verify document :: 'fch_vto' field is missing or empty in record")
         return False

      alm = record.get('alm')
      if alm is None or alm.strip() == "":
         logging.error(f"verify document :: 'alm' field is missing or empty in record")
         return False
    
      
      return True
 
   def check_details(self, details):
    for detail in details:
        # Check _indice field
        _indice = detail.get('_indice')
        if _indice is None or _indice == 0:
            logging.error(f"verify document :: '_indice' field is missing or zero in detail")
            return False
        
        # Check alm field
        alm = detail.get('alm')
        if alm is None or str(alm).strip() == "":
            logging.error(f"verify document :: 'alm' field is missing or empty in detail")
            return False
        
        # Check art field
        art = detail.get('art')
        if art is None or art == 0:
            logging.error(f"verify document :: 'art' field is missing or zero in detail")
            return False
        
        # Check und_med field
        und_med = detail.get('und_med')
        if und_med is None or und_med == 0:
            logging.error(f"verify document :: 'und_med' field is missing or zero in detail")
            return False
        
        # Check can_und field
        can_und = detail.get('can_und')
        if can_und is None:
            logging.error(f"verify document :: 'can_und' field is missing or zero in detail")
            return False
        
        # Check can field
        can = detail.get('can')
        if can is None:
            logging.error(f"verify document :: 'can' field is missing or zero in detail")
            return False
        
        # Check emp_div field
        emp_div = detail.get('emp_div')
        if emp_div is None or str(emp_div).strip() == "":
            logging.error(f"verify document :: 'emp_div' field is missing or empty in detail")
            return False
        
        # Check emp field
        emp = detail.get('emp')
        if emp is None or str(emp).strip() == "":
            logging.error(f"verify document :: 'emp' field is missing or empty in detail")
            return False
        
        # Check fch field
        fch = detail.get('fch')
        if fch is None or str(fch).strip() == "":
            logging.error(f"verify document :: 'fch' field is missing or empty in detail")
            return False
        
        # Check hor field
        hor = detail.get('hor')
        if hor is None or str(hor).strip() == "":
            logging.error(f"verify document :: 'hor' field is missing or empty in detail")
            return False
        
        # Check pre field
        pre = detail.get('pre')
        if pre is None:
            logging.error(f"verify document :: 'pre' field is missing or zero in detail")
            return False
        
        # Check por_dto field (can be 0)
        por_dto = detail.get('por_dto')
        if por_dto:
            logging.error(f"verify document :: 'por_dto' field is missing in detail")
            return False
        
        # Check reg_iva_vta field
        reg_iva_vta = detail.get('reg_iva_vta')
        if reg_iva_vta is None or str(reg_iva_vta).strip() == "":
            logging.error(f"verify document :: 'reg_iva_vta' field is missing or empty in detail")
            return False
        
        # Check clt field
        clt = detail.get('clt')
        if clt is None or clt == 0:
            logging.error(f"verify document :: 'clt' field is missing or zero in detail")
            return False
        
        # Check mov_tip field
        mov_tip = detail.get('mov_tip')
        if mov_tip is None or str(mov_tip).strip() == "":
            logging.error(f"verify document :: 'mov_tip' field is missing or empty in detail")
            return False
        
        # Check cal_arr field
        cal_arr = detail.get('cal_arr')
        if cal_arr is None:
            logging.error(f"verify document :: 'cal_arr' field is missing or zero in detail")
            return False
        hor = detail.get('hor')
        if hor is None or str(hor).strip() == "":
            logging.error(f"verify document :: 'hor' field is missing or empty in detail")
            return False
    
    # All details passed validation
    return True
    
   def check_receipts(self, receipts):
      for receipt in receipts:
         # Check _indice field
         _indice = receipt.get('_indice')
         if _indice is None or _indice == 0:
            logging.error(f"verify document :: '_indice' field is missing or zero in receipt")
            return False
         
         # Check ser field
         ser = receipt.get('ser')
         if ser is None or ser == 0:
            logging.error(f"verify document :: 'ser' field is missing or zero in receipt")
            return False
         
         # Check fch field
         fch = receipt.get('fch')
         if fch is None or str(fch).strip() == "":
            logging.error(f"verify document :: 'fch' field is missing or empty in receipt")
            return False
         
         # Check ref_recibo field
         ref_recibo = receipt.get('ref_recibo')
         if ref_recibo is None or ref_recibo == 0:
            logging.error(f"verify document :: 'ref_recibo' field is missing or zero in receipt")
            return False
         
         # Check importe field
         importe = receipt.get('importe')
         if importe is None :
            logging.error(f"verify document :: 'importe' field is missing receipt")
            return False
         
         # Check caja_bco field
         caja_bco = receipt.get('caja_bco')
         if caja_bco is None:
            logging.error(f"verify document :: 'caja_bco' field is missing or zero in receipt")
            return False
         
         # Check tienda field
         tienda = receipt.get('tienda')
         if tienda is None or str(tienda).strip() == "":
            logging.error(f"verify document :: 'tienda' field is missing or empty in receipt")
            return False
         
         # Check ref_tipo field
         ref_tipo = receipt.get('ref_tipo')
         if ref_tipo is None or str(ref_tipo).strip() == "":
            logging.error(f"verify document :: 'ref_tipo' field is missing or empty in receipt")
            return False
         
         # Check hora field
         hora = receipt.get('hora')
         if hora is None or str(hora).strip() == "":
            logging.error(f"verify document :: 'hora' field is missing or empty in receipt")
            return False
         
         # Check num_doc field
         num_doc = receipt.get('num_doc')
         if num_doc is None or str(num_doc).strip() == "":
            logging.error(f"verify document :: 'num_doc' field is missing or empty in receipt")
            return False
         
         # Check fpg field
         fpg = receipt.get('fpg')
         if fpg is None or str(fpg).strip() == "":
            logging.error(f"verify document :: 'fpg' field is missing or zero in receipt")
            return False
      
      # All receipts passed validation
      return True