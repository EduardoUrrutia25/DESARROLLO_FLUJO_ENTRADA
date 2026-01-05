import csv
from decimal import Decimal, ConversionSyntax, InvalidOperation
from typing import List, Optional

from repositories import PILineHoldRepository
from dtos import PILineHoldDTO
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
    

class PILineFileService(FileProcessingService):
    """Servicio para procesar el line de la creación de inventarios."""
    def __init__(self):
        self.repository = PILineHoldRepository()

    def process(self, file_path: str):

        PILine_dtos: List[PILineHoldDTO] = []

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
                        
                        dto = PILineHoldDTO(
                                    PINumber        = row['PINumber'],
                                    StoreNo         = to_int_safely(row['StoreNo']),
                                    SKU             = to_int_safely(row['SKU']),
                                    SystemQty       = to_decimal_safely(row['SystemQty']),
                                    PhysicalQty     = to_int_safely(row['PhysicalQty']),
                                    DiffQty         = to_decimal_safely(row['DiffQty']),
                                    AvgCost         = to_decimal_safely(row['AvgCost'])
                                )
                        PILine_dtos.append(dto)
                    
                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
            
            if not PILine_dtos:
                print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
                return

            print(f"--- Se han creado {len(PILine_dtos)} DTOs. Enviando a la base de datos... ---")
            self.repository.save_batch(PILine_dtos)

        except Exception as e:
            
            import traceback
            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
            traceback.print_exc()
