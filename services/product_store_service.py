import csv
from decimal import Decimal, ConversionSyntax, InvalidOperation
from typing import List, Optional

from repositories import ProductStoreHoldRepository
from dtos import ProductStoreHoldDTO
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
    

class ProductStoreFileService(FileProcessingService):
    """Servicio para procesar product store."""
    def __init__(self):
        self.repository = ProductStoreHoldRepository()

    def process(self, file_path: str):

        ProductStore_dtos: List[ProductStoreHoldDTO] = []

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
                        
                        dto = ProductStoreHoldDTO(
                                    StoreNo         = to_int_safely(row['StoreNo']),
                                    SKU             = to_int_safely(row['SKU']),
                                    OnHandQty       = to_decimal_safely(row['OnHandQty']),
                                    OnOrderedQty    = to_decimal_safely(row['OnOrderedQty']),
                                    CommittedQty    = to_decimal_safely(row['CommittedQty']),
                                    InTransitQty    = to_decimal_safely(row['InTransitQty']),
                                    ProcessedFlag   = row['ProcessedFlag'],
                                    ProcessedDate   = parse_datetime_safely(row['ProcessedDate'])
                                )
                        ProductStore_dtos.append(dto)
                    
                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
            
            if not ProductStore_dtos:
                print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
                return

            print(f"--- Se han creado {len(ProductStore_dtos)} DTOs. Enviando a la base de datos... ---")
            self.repository.save_batch(ProductStore_dtos)

        except Exception as e:
            
            import traceback
            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
            traceback.print_exc()