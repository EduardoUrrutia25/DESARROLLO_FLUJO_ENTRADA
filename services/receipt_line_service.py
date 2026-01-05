import csv
import asyncio

from decimal        import Decimal, ConversionSyntax, InvalidOperation
from typing         import List, Optional

from repositories   import ReceiptLineHoldRepository
from dtos           import ReceiptLineHoldDTO
from .base_service  import FileProcessingService

from utils.async_db import execute_query

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

NEGOCIO_TO_DB_MAP = {
    "TAMBO": "xx",
    "ARUMA": "yy"
}
    

class ReceiptLineFileService(FileProcessingService):
    """Servicio para procesar el detalle del envío de transacciones de venta."""
    #def __init__(self):
    #    self.repository = ReceiptLineHoldRepository()

    def __init__(self):
        self.repository = None # Se inicializará durante el process

    def _get_db_id_from_file(self, file_path: str, delimiter: str) -> str:
        """Lee la primera fila del archivo para determinar la BD."""
        with open(file_path, mode='r', encoding='latin-1') as file:
            reader = csv.DictReader(file, delimiter=delimiter)

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
#        ReceiptLine_dtos: List[ReceiptLineHoldDTO] = []
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
#                        dto = ReceiptLineHoldDTO(
#                                    StoreNo                 = to_int_safely(row['StoreNo']),
#                                    ReceiptId               = to_uuid_safely(row['ReceiptId']),
#                                    LineId                  = to_int_safely(row['LineId']),
#                                    SKU                     = to_int_safely(row['SKU']),
#                                    Qty                     = to_decimal_safely(row['Qty']),
#                                    OriginalPrice           = to_decimal_safely(row['OriginalPrice']),
#                                    RetailPrice             = to_decimal_safely(row['RetailPrice']),
#                                    DiscPercent             = to_decimal_safely(row['DiscPercent']),
#                                    LineNotes               = to_str_or_none(row['LineNotes']),
#                                    AlternativeLookUpCode   = to_str_or_none(row['AlternativeLookUpCode']),
#                                    Clerk                   = to_str_or_none(row['Clerk']),
#                                    TaxPercent              = to_decimal_safely(row['TaxPercent']),
#                                    SalesCode               = row['SalesCode']
#                                )
#                        ReceiptLine_dtos.append(dto)
#                    
#                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
#                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
#            
#            if not ReceiptLine_dtos:
#                print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
#                return
#
#            print(f"--- Se han creado {len(ReceiptLine_dtos)} DTOs. Enviando a la base de datos... ---")
#            self.repository.save_batch(ReceiptLine_dtos)
#
#        except Exception as e:
#            
#            import traceback
#            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
#            traceback.print_exc()

    def _create_dto_from_row(self, row: dict) -> ReceiptLineHoldDTO:
        """Función auxiliar para convertir una fila en DTO."""
        return ReceiptLineHoldDTO(
                                StoreNo                 = to_int_safely(row['StoreNo']),
                                LineId                  = to_int_safely(row['LineId']),
                                SKU                     = to_int_safely(row['SKU']),
                                Qty                     = to_decimal_safely(row['Qty']),
                                OriginalPrice           = to_decimal_safely(row['OriginalPrice']),
                                RetailPrice             = to_decimal_safely(row['RetailPrice']),
                                DiscPercent             = to_decimal_safely(row['DiscPercent']),
                                LineNotes               = to_str_or_none(row['LineNotes']),
                                AlternativeLookUpCode   = to_str_or_none(row['AlternativeLookUpCode']),
                                Clerk                   = to_str_or_none(row['Clerk']),
                                TaxPercent              = to_decimal_safely(row['TaxPercent']),
                                SalesCode               = row['SalesCode'],
                                ReceiptId               = row['ReceiptId']
        )
    
    async def get_store_no(self, org: str, ext_id: str) -> Optional[str]:
        query = "SELECT StoreNo FROM STORE WHERE StoreExternalId = ?"
        row = await execute_query(org, query, {"ext_id": ext_id})
        #return row[0] if row else None
        if row and row[0] is not None:
            return str(row[0])
        return None
    
    async def get_sku_from_upc(self, org: str, upc: str) -> Optional[str]:
        query = "SELECT SKU FROM PRODUCT WHERE UPC = ?"
        row = await execute_query(org, query, {"upc": upc})
        #return row[0] if row else None
        if row and row[0] is not None:
            return str(row[0])
        return None
    
    async def process(self, file_path: str):
        print(f"Iniciando procesamiento de LÍNEA DE RECIBO: {file_path}")
        
        # --- (Detección de delimitador y BD) ---
        with open(file_path, 'r', encoding='latin-1') as f:
            header = f.readline()
        delimiter = ';' if ';' in header else ','
        
        db_identifier = self._get_db_id_from_file(file_path, delimiter)
        # Asegúrate de que el nombre del Repositorio sea el correcto
        self.repository = ReceiptLineHoldRepository(db_identifier=db_identifier)
        
        # --- Listas de DTOs y Errores ---
        valid_rows_for_translation = []
        error_rows_report = []
        
        # --- Mapa de Validación de Decimales ---
        COLUMN_PRECISION_MAP = {
            'Qty': (18, 5),
            'OriginalPrice': (18, 5),
            'RetailPrice': (18, 5),
            'DiscPercent': (18, 5),
            'TaxPercent': (18, 5)
        }
        DECIMAL_COLUMNS = list(COLUMN_PRECISION_MAP.keys())
        print(f"--- Iniciando validación de Precisión y Escala... ---")
        
        # --- Sets para recolectar IDs a traducir ---
        stores_a_traducir = set()
        upcs_a_traducir = set()

        # --- 1. Primera Pasada: Validación de Decimales y Recolección ---
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
                
                if has_error:
                    continue 

                valid_rows_for_translation.append(row)
                
                # Recolecta IDs para traducir
                store_ext_id = row.get('StoreNo')
                if store_ext_id:
                    stores_a_traducir.add(store_ext_id)
                
                upc = row.get('SKU')
                if upc:
                    upcs_a_traducir.add(upc)

        # --- 2. Reporte de Errores de Validación ---
        if error_rows_report:
            print("\n" + "="*50)
            print(" ERRORES DE PRECISIÓN/ESCALA ENCONTRADOS ")
            print(f"Se encontraron {len(error_rows_report)} filas con errores decimales. Estas filas serán omitidas.")
            for error_line in error_rows_report[:50]: print(error_line)
            if len(error_rows_report) > 50: print(f"... y {len(error_rows_report) - 50} más.")
            print("="*50 + "\n")
        else:
            print("--- No se encontraron errores de precisión o escala. ---")

        # --- 3. Traducción Masiva en Paralelo ---
        print(f"Traduciendo {len(stores_a_traducir)} StoreIDs y {len(upcs_a_traducir)} UPCs únicos...")
        
        store_tasks = [self.get_store_no(db_identifier, ext_id) for ext_id in stores_a_traducir]
        sku_tasks = [self.get_sku_from_upc(db_identifier, upc) for upc in upcs_a_traducir]
        
        store_resultados, sku_resultados = await asyncio.gather(
            asyncio.gather(*store_tasks),
            asyncio.gather(*sku_tasks)
        )
        
        # --- 4. Crear Mapas de Traducción (Caché) ---
        store_cache = dict(zip(stores_a_traducir, store_resultados))
        sku_cache = dict(zip(upcs_a_traducir, sku_resultados))
        
        # --- 5. Segunda Pasada: Crear DTOs Finales ---
        print("Actualizando DTOs con valores traducidos...")
        dtos_para_guardar = []
        for row in valid_rows_for_translation:
            try:
                dto = self._create_dto_from_row(row)
                
                # Traduce StoreNo
                store_ext_id = row.get('StoreNo')
                translated_store = store_cache.get(store_ext_id)
                if translated_store:
                    dto.StoreNo = to_int_safely(translated_store)
                
                # Traduce SKU (que es un UPC en el 'row')
                upc = row.get('SKU')
                translated_sku = sku_cache.get(upc)
                if translated_sku:
                    dto.SKU = to_int_safely(translated_sku)

                dtos_para_guardar.append(dto)
            except Exception as e:
                print(f"!!! Error creando DTO final: {e}. Fila omitida: {row}")

        # --- 6. Guardar Lote Final ---
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