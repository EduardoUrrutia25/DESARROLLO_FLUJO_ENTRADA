import csv
from decimal import Decimal, ConversionSyntax, InvalidOperation
from typing import List, Optional

from repositories import AVGCostHoldRepository
from dtos import AVGCostHoldDTO
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
    

class AVGCostFileService(FileProcessingService):
    """Servicio para procesar el costo promedio."""
    def __init__(self):
        self.repository = AVGCostHoldRepository()

    def process(self, file_path: str):

        AVGcost_dtos: List[AVGCostHoldDTO] = []

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
                        
                        dto = AVGCostHoldDTO(
                                    SKU             = to_int_safely(row['SKU']),
                                    LastCost        = to_decimal_safely(row['LastCost']),
                                    AvgCost         = to_decimal_safely(row['AvgCost']),
                                    CreationDate    = parse_datetime_safely(row['CreationDate']),
                                    CreatedBy       = row['CreatedBy'],
                                    Process1        = to_int_safely(row['Process1']),
                                    Process2        = to_int_safely(row['Process2']),
                                    Process3        = to_int_safely(row['Process3'])
                                )
                        AVGcost_dtos.append(dto)
                    
                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
            
            if not AVGcost_dtos:
                print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
                return

            print(f"--- Se han creado {len(AVGcost_dtos)} DTOs. Enviando a la base de datos... ---")
            self.repository.save_batch(AVGcost_dtos)

        except Exception as e:
            
            import traceback
            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
            traceback.print_exc()
