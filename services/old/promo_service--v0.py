import csv
from decimal    import Decimal, ConversionSyntax, InvalidOperation
from typing     import List, Optional

from repositories   import PromoHoldRepository
from dtos           import PromoHoldDTO
from .base_service  import FileProcessingService


from utils.parsers import (
    to_bool,
    parse_datetime_safely,
    to_decimal_safely,
    to_int_safely,
    to_str_or_empty,
    to_str_or_none,
    to_str_safely
)

NEGOCIO_TO_DB_MAP = {
    "TAMBO": "xx",
    "ARUMA": "yy"
}

class PromoFileService(FileProcessingService):
    """Servicio para procesar el archivo de Promociones."""
    #def __init__(self):
        #self.repository = PromoHoldRepository()
    def __init__(self, db_identifier: str):
        self.repository = PromoHoldRepository(db_identifier=db_identifier)

    def process(self, file_path: str):
        
        #print("DEBUG: Entrando al método process...")

        promo_dtos: List[PromoHoldDTO] = []
        try:
            with open(file_path, mode='r', encoding='latin-1') as file:
                header = file.readline()
                file.seek(0)
                
                delimiter = ';' if ';' in header else ','
                #print(f"--- Delimitador determinado manualmente: '{delimiter}' ---")
                
                reader = csv.DictReader(file, delimiter=delimiter)

                for i, row in enumerate(reader, start=2):

                    #print(f"DEBUG: Procesando fila {i}...")
                    try:
                        #print("DEBUG: Dentro del bloque try, a punto de crear el DTO...")
                        
                        dto = PromoHoldDTO(
                                    PromoId         = to_int_safely(row['PromoId']),
                                    Comment         = to_str_or_empty(row['Comment']),
                                    PromoName       = to_str_or_none(row['PromoName']),
                                    StartDate       = parse_datetime_safely(row['StartDate']),
                                    EndDate         = parse_datetime_safely(row['EndDate']),
                                    StatusCode      = row['StatusCode'],
                                    ReasonCode      = to_str_or_none(row['ReasonCode']),
                                    TypeCode        = row['TypeCode'],
                                    SubTypeCode     = to_str_or_none(row['SubTypeCode']),
                                    Qty1            = to_decimal_safely(row['Qty1']),
                                    GetPercentOff   = to_decimal_safely(row['GetPercentOff']),
                                    GetMoneyOff     = to_decimal_safely(row['GetMoneyOff']),
                                    SpendMoney      = to_decimal_safely(row['SpendMoney']),
                                    PromoTenderId   = to_int_safely(row['PromoTenderId']),
                                    ItemType        = row['ItemType'],
                                    SKU             = to_int_safely(row['SKU']),
                                    Price           = to_decimal_safely(row['Price']),
                                    GroupNo         = to_int_safely(row['GroupNo']),
                                    GroupQty        = to_decimal_safely(row['GroupQty']),
                                    Process1        = to_int_safely(row['Process1']),
                                    Process2        = to_int_safely(row['Process2']),
                                    Process3        = to_int_safely(row['Process3']),
                                    
                                    # Campos booleanos que no pueden ser nulos
                                    ApplyOverExistingPromo  = to_bool(row['ApplyOverExistingPromo']),
                                    ApplyOverOriginalPrice  = to_bool(row['ApplyOverOriginalPrice']),
                                    UseCouponsAsTender      = to_bool(row['UseCouponsAsTender']),
                                    Deleted                 = to_bool(row['Deleted'])
                            )
                        
                        promo_dtos.append(dto)
                    
                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
            
            if not promo_dtos:
                #print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
                return

            print(f"--- Se han creado {len(promo_dtos)} DTOs. Enviando a la base de datos... ---")
            self.repository.save_batch(promo_dtos)

        except Exception as e:
            
            import traceback
            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
            traceback.print_exc()
