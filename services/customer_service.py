import csv
from decimal import Decimal, ConversionSyntax, InvalidOperation
from typing import List, Optional

from repositories import CustomerHoldRepository
from dtos import CustomerHoldDTO
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
   

class CustomerFileService(FileProcessingService):
    """Servicio para procesar el archivo de Clientes."""
    def __init__(self):
        self.repository = CustomerHoldRepository()

    def process(self, file_path: str):

        customer_dtos: List[CustomerHoldDTO] = []
        try:
            with open(file_path, mode='r', encoding='latin-1') as file:
                header = file.readline()
                file.seek(0)
                
                delimiter = ';' if ';' in header else ','
                
                reader = csv.DictReader(file, delimiter=delimiter)

                for i, row in enumerate(reader, start=2):

                    try:
                        
                        dto = CustomerHoldDTO(
                                    CustomerNo      = to_int_safely(row['CustomerNo']),
                                    CustomerType    = row['CustomerType'],
                                    UDF5            = to_int_safely(row['UDF5']),
                                    UDF6            = to_int_safely(row['UDF6']),
                                    LicenseNumber   = row['LicenseNumber'],
                                    Info1           = to_str_or_empty(row['Info1']),
                                    StatusCode      = row['StatusCode'],
                                    FirstName       = row['FirstName'],
                                    LastName        = to_str_or_empty(row['LastName']),
                                    MaidenName      = to_str_or_empty(row['MaidenName']),
                                    CompanyName     = to_str_or_empty(row['CompanyName']),
                                    Gender          = row['Gender'],
                                    Email           = to_str_or_empty(row['Email']),
                                    CreationDate    = parse_datetime_safely(row['CreationDate']),
                                    CreatedBy       = row['CreatedBy'],
                                    Process1        = to_int_safely(row['Process1']),
                                    Process2        = to_int_safely(row['Process2']),
                                    Process3        = to_int_safely(row['Process3'])
                            )
                        
                        customer_dtos.append(dto)
                    
                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
            
            if not customer_dtos:
                #print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
                return

            print(f"--- Se han creado {len(customer_dtos)} DTOs. Enviando a la base de datos... ---")
            self.repository.save_batch(customer_dtos)

        except Exception as e:
            
            import traceback
            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
            traceback.print_exc()
