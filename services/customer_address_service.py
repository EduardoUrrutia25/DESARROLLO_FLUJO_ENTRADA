import csv
from decimal import Decimal, ConversionSyntax, InvalidOperation
from typing import List, Optional

from repositories import CustomerAddressHoldRepository
from dtos import CustomerAddressHoldDTO
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


class CustomerAddressFileService(FileProcessingService):
    """Servicio para procesar el archivo de tienda asociado a una promoción."""
    def __init__(self):
        self.repository = CustomerAddressHoldRepository()

    def process(self, file_path: str):

        customerAddress_dtos: List[CustomerAddressHoldDTO] = []

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
                        
                        dto = CustomerAddressHoldDTO(
                                    CustomerNo  = to_int_safely(row['CustomerNo']),
                                    Address1    = to_str_or_empty(row['Address1']),
                                    City        = to_str_or_empty(row['City']),
                                    State       = to_str_or_empty(row['State']),
                                    Abbrev      = row['Abbrev']
                                )
                        customerAddress_dtos.append(dto)
                    
                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
            
            if not customerAddress_dtos:
                print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
                return

            print(f"--- Se han creado {len(customerAddress_dtos)} DTOs. Enviando a la base de datos... ---")
            self.repository.save_batch(customerAddress_dtos)

        except Exception as e:
            
            import traceback
            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
            traceback.print_exc()
