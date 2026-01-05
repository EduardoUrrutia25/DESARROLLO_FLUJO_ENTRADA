import csv
from decimal        import Decimal, ConversionSyntax, InvalidOperation
from typing         import List, Optional

from repositories   import POHoldRepository
from dtos           import POHoldDTO
from .base_service  import FileProcessingService

from utils.parsers  import (
    to_bool,
    parse_datetime_safely,
    to_decimal_safely,
    to_int_safely,
    to_str_or_empty,
    to_str_or_none,
    to_str_safely,
    to_uuid_safely
)
    

class POFileService(FileProcessingService):
    """Servicio para procesar la modificación de orden de compra."""
    def __init__(self):
        self.repository = POHoldRepository()

    def process(self, file_path: str):

        PO_dtos: List[POHoldDTO] = []

        try:
            with open(file_path, mode='r', encoding='latin-1') as file:
                header = file.readline()
                file.seek(0)
                
                delimiter = ';' if ';' in header else ','
                
                reader = csv.DictReader(file, delimiter=delimiter)

                for i, row in enumerate(reader, start=2):

                    #print(f"DEBUG: Procesando fila {i}...")
                    try:
                        #print("DEBUG: Dentro del bloque try, a punto de crear el DTO...")
                        
                        dto = POHoldDTO(
                                    POId 	        = to_uuid_safely(row['POId']),
                                    StatusCode 	    = row['StatusCode'],
                                    PONumber 	    = row['PONumber'],
                                    PODate 	        = parse_datetime_safely(row['PODate']),
                                    CancelDate 	    = parse_datetime_safely(row['CancelDate']),
                                    ShipDate 	    = parse_datetime_safely(row['ShipDate']),
                                    StoreNo 	    = to_int_safely(row['StoreNo']),
                                    Comment1 	    = row['Comment1'],
                                    Comment2 	    = row['Comment2'],
                                    Comment3 	    = row['Comment3'],
                                    Notes 	        = row['Notes'],
                                    CreatedBy 	    = to_str_or_none(row['CreatedBy']),
                                    CreationDate 	= parse_datetime_safely(row['CreationDate']),
                                    ModifiedBy 	    = row['ModifiedBy'],
                                    ChangeDate 	    = parse_datetime_safely(row['ChangeDate']),
                                    PONo 	        = to_int_safely(row['PONo']),
                                    TrackNo 	    = row['TrackNo'],
                                    DocReference 	= row['DocReference']
                                )
                        PO_dtos.append(dto)
                    
                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
            
            if not PO_dtos:
                print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
                return

            print(f"--- Se han creado {len(PO_dtos)} DTOs. Enviando a la base de datos... ---")
            self.repository.save_batch(PO_dtos)

        except Exception as e:
            
            import traceback
            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
            traceback.print_exc()
