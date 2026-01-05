import csv
import asyncio

from decimal import Decimal, ConversionSyntax, InvalidOperation
from typing import List, Optional

from repositories import PromoCouponHoldRepository
from dtos import PromoCouponHoldDTO
from .base_service import FileProcessingService

from utils.async_db import execute_query

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
    

class PromoCouponFileService(FileProcessingService):
    """Servicio para procesar el archivo de cupones asociados a una promoción."""
    #def __init__(self):
    #    self.repository = PromoCouponHoldRepository()

    def __init__(self):
        self.repository = None # Se inicializará durante el process

    def _get_db_id_from_file(self, file_path: str, delimiter: str) -> str:
        """Lee la primera fila del archivo para determinar la BD."""
        with open(file_path, mode='r', encoding='latin-1') as file:
            reader = csv.DictReader(file, delimiter=delimiter)

            # --- AQUÍ LA SOLUCIÓN ---
            if reader.fieldnames:
                # Limpia espacios y caracteres invisibles (como el BOM)
                reader.fieldnames = [h.strip().lstrip('\ufeff') for h in reader.fieldnames]
            # -------------------------

            first_row = next(reader, None)
            
            if first_row is None:
                raise ValueError("El archivo está vacío.")
            
            print(f"first_row: {first_row}")
            
            negocio = first_row.get("Organization")

            print(f"Organzation: {negocio}")

            if negocio is None:
                raise ValueError("El archivo no contiene la columna 'Organization'")
                
            db_id = NEGOCIO_TO_DB_MAP.get(negocio.upper())

            print(f"db_id: {db_id}")

            if db_id is None:
                raise ValueError(f"Organization '{negocio}' no reconocido.")
                
            return db_id

#    def process(self, file_path: str):
#
#        #promoCoupon_dtos: List[PromoCouponHoldDTO] = []
#
#        try:
#            with open(file_path, mode='r', encoding='latin-1') as file:
#                header = file.readline()
#                file.seek(0)
#                
#                delimiter = ';' if ';' in header else ','
#                
#                reader = csv.DictReader(file, delimiter=delimiter)
#
#                for i, row in enumerate(reader, start=2):
#
#                    #print(f"DEBUG: Procesando fila {i}...")
#                    try:
#                        #print("DEBUG: Dentro del bloque try, a punto de crear el DTO...")
#                        
#                        dto = PromoCouponHoldDTO(
#                                    PromoId     = to_int_safely(row['PromoId']),
#                                    Comment     = to_str_or_empty(row['Comment']),
#                                    CouponCode  = row['CouponCode'],
#                                    Amount      = to_decimal_safely(row['Amount']),
#                                    StatusCode  = row['StatusCode']
#                                )
#                        promoCoupon_dtos.append(dto)
#                    
#                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
#                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
#            
#            if not promoCoupon_dtos:
#                print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
#                return
#
#            print(f"--- Se han creado {len(promoCoupon_dtos)} DTOs. Enviando a la base de datos... ---")
#            self.repository.save_batch(promoCoupon_dtos)
#
#        except Exception as e:
#            
#            import traceback
#            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
#            traceback.print_exc()


    def _create_dto_from_row(self, row: dict) -> PromoCouponHoldDTO:
        """Función auxiliar para convertir una fila en DTO."""
        return PromoCouponHoldDTO(
                                PromoId         = to_int_safely(row['PromoId']),
                                Comment         = to_str_or_empty(row['Comment']),
                                CouponCode      = row['CouponCode'],
                                Amount          = to_decimal_safely(row['Amount']),
                                StatusCode      = row['StatusCode']
        )
    
    async def process(self, file_path: str):
        print(f"Iniciando procesamiento de PROMO COUPON: {file_path}")
        
        # --- (Detección de delimitador y BD) ---
        with open(file_path, 'r', encoding='latin-1') as f:
            header = f.readline()
        delimiter = ';' if ';' in header else ','
        
        db_identifier = self._get_db_id_from_file(file_path, delimiter)
        self.repository = PromoCouponHoldRepository(db_identifier=db_identifier)
        
        # --- Listas ---
        dtos_para_guardar = []
        error_rows_report = []
        
        # --- Mapa de Validación de Decimales ---
        COLUMN_PRECISION_MAP = {
            'Amount': (18, 5)
        }
        DECIMAL_COLUMNS = list(COLUMN_PRECISION_MAP.keys())
        print(f"--- Iniciando validación de Precisión y Escala... ---")

        # --- 1. ÚNICA PASADA: Validación y Creación de DTOs ---
        with open(file_path, mode='r', encoding='latin-1') as file:
            reader = csv.DictReader(file, delimiter=delimiter)
            
            if reader.fieldnames:
                reader.fieldnames = [h.strip().lstrip('\ufeff') for h in reader.fieldnames]

            for i, row in enumerate(reader, start=2):
                has_error = False
                
                # --- Validación de Precisión y Escala ---
                for col_name in DECIMAL_COLUMNS:
                    if col_name not in row: continue
                    value_str = row.get(col_name)
                    if not value_str or value_str.strip().lower() in ('', 'null'): continue

                    try:
                        value_decimal = Decimal(value_str.strip())
                        t = value_decimal.as_tuple()
                        if not isinstance(t.exponent, int):
                            raise InvalidOperation("No finito (NaN/Inf)")
                        
                        scale = abs(t.exponent)
                        integer_digits = len(t.digits) - scale
                        if integer_digits <= 0: integer_digits = 1
                        
                        allowed_precision, allowed_scale = COLUMN_PRECISION_MAP[col_name]
                        allowed_integer_digits = allowed_precision - allowed_scale
                        
                        if scale > allowed_scale:
                            error_rows_report.append(f"Fila {i}: Col '{col_name}' tiene {scale} decimales (Límite: {allowed_scale}). Valor: '{value_str}'")
                            has_error = True
                        elif integer_digits > allowed_integer_digits:
                            error_rows_report.append(f"Fila {i}: Col '{col_name}' tiene {integer_digits} dígitos enteros (Límite: {allowed_integer_digits}). Valor: '{value_str}'")
                            has_error = True
                    
                    except InvalidOperation:
                        error_rows_report.append(f"Fila {i}: Col '{col_name}' no es un decimal válido. Valor: '{value_str}'")
                        has_error = True
                
                # Si la fila tiene error decimal, se salta
                if has_error:
                    continue 

                # Si la fila es válida, se crea el DTO
                try:
                    dto = self._create_dto_from_row(row)
                    dtos_para_guardar.append(dto)
                except Exception as e:
                    print(f"!!! Error creando DTO en fila {i}: {e}. Fila omitida: {row}")

        # --- 2. Reporte de Errores ---
        if error_rows_report:
            print("\n" + "="*50)
            print(" ERRORES DE PRECISIÓN/ESCALA ENCONTRADOS ")
            print(f"Se encontraron {len(error_rows_report)} filas con errores decimales. Estas filas serán omitidas.")
            for error_line in error_rows_report[:50]: print(error_line)
            if len(error_rows_report) > 50: print(f"... y {len(error_rows_report) - 50} más.")
            print("="*50 + "\n")
        else:
            print("--- No se encontraron errores de precisión o escala. ---")

        # --- 3. Guardar Lote Final ---
        if not dtos_para_guardar:
            print("ADVERTENCIA: No se generaron DTOs válidos para procesar.")
            return

        print(f"--- Se han creado {len(dtos_para_guardar)} DTOs. Enviando a la base de datos... ---")
        try:
            await asyncio.to_thread(self.repository.save_batch, dtos_para_guardar)
            print("--- Lote guardado en la base de datos exitosamente. ---")

        except Exception as e:
            print(f"Error en la base de datos. Realizando rollback... {e}")
            raise