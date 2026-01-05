import csv
from decimal import Decimal, ConversionSyntax, InvalidOperation
from typing import List, Optional

from repositories import SlipHoldRepository
from dtos import SlipHoldDTO
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
    

class SlipFileService(FileProcessingService):
    """Servicio para procesar el line de la creación de inventarios."""
    def __init__(self):
        self.repository = SlipHoldRepository()

    def process(self, file_path: str):

        Slip_dtos: List[SlipHoldDTO] = []

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
                        
                        dto = SlipHoldDTO(
                                    StoreNo	        = to_int_safely(row['StoreNo']),
                                    OutToStore	    = to_int_safely(row['OutToStore']),
                                    StatusCode	    = row['StatusCode'],
                                    DocNumber	    = row['DocNumber'],
                                    DocReference	= row['DocReference'],
                                    Notes	        = row['Notes'],
                                    Comment1	    = row['Comment1'],
                                    Comment2	    = row['Comment2'],
                                    Comment3	    = row['Comment3'],
                                    SlipDate	    = parse_datetime_safely(row['SlipDate']),
                                    ChangeDate	    = parse_datetime_safely(row['ChangeDate']),
                                    LineCount	    = to_int_safely(row['LineCount']),
                                    Process1	    = to_int_safely(row['Process1']),
                                    Process2	    = to_int_safely(row['Process2']),
                                    Process3	    = to_int_safely(row['Process3'])
                                )
                        Slip_dtos.append(dto)
                    
                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
            
            if not Slip_dtos:
                print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
                return

            print(f"--- Se han creado {len(Slip_dtos)} DTOs. Enviando a la base de datos... ---")
            self.repository.save_batch(Slip_dtos)

        except Exception as e:
            
            import traceback
            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
            traceback.print_exc()
