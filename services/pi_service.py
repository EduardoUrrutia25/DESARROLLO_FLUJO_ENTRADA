import csv
from decimal import Decimal, ConversionSyntax, InvalidOperation
from typing import List, Optional

from repositories import PIHoldRepository
from dtos import PIHoldDTO
from .base_service import FileProcessingService

from utils.parsers import (
    to_bool,
    parse_datetime_safely,
    to_decimal_safely,
    to_int_safely,
    to_str_or_empty,
    to_str_or_none,
    to_str_safely
)
    

class PIFileService(FileProcessingService):
    """Servicio para procesar la creación de inventarios."""
    def __init__(self):
        self.repository = PIHoldRepository()

    def process(self, file_path: str):

        PI_dtos: List[PIHoldDTO] = []

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
                        
                        dto = PIHoldDTO(
                                    PINumber	    = row['PINumber'],
                                    StoreNo	        = to_int_safely(row['StoreNo']),
                                    TypeCode	    = row['TypeCode'],
                                    StatusCode	    = row['StatusCode'],
                                    CreatedBy	    = row['CreatedBy'],
                                    CreationDate	= parse_datetime_safely(row['CreationDate']),
                                    ChangeDate	    = parse_datetime_safely(row['ChangeDate']),
                                    Notes	        = row['Notes'],
                                    SystemQty	    = to_decimal_safely(row['SystemQty']),
                                    PhysicalQty	    = to_decimal_safely(row['PhysicalQty']),
                                    StartDate	    = parse_datetime_safely(row['StartDate']),
                                    StartBy	        = row['StartBy'],
                                    EndDate	        = parse_datetime_safely(row['EndDate']),
                                    EndBy	        = row['EndBy'],
                                    Process1	    = to_int_safely(row['Process1']),
                                    Process2	    = to_int_safely(row['Process2']),
                                    Process3	    = to_int_safely(row['Process3'])
                                )
                        PI_dtos.append(dto)
                    
                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
            
            if not PI_dtos:
                print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
                return

            print(f"--- Se han creado {len(PI_dtos)} DTOs. Enviando a la base de datos... ---")
            self.repository.save_batch(PI_dtos)

        except Exception as e:
            
            import traceback
            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
            traceback.print_exc()
