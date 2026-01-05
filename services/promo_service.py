import csv
import asyncio

from decimal    import Decimal, ConversionSyntax, InvalidOperation
from typing     import List, Optional

from repositories   import PromoHoldRepository
from dtos           import PromoHoldDTO
from .base_service  import FileProcessingService

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

class PromoFileService(FileProcessingService):
    """Servicio para procesar el archivo de Promociones."""
    #def __init__(self):
        #self.repository = PromoHoldRepository()

    #def __init__(self, db_identifier: str):
    #    self.repository = PromoHoldRepository(db_identifier=db_identifier)

    def __init__(self):
        self.repository = None # Se inicializará durante el process

    def _get_db_id_from_file(self, file_path: str, delimiter: str) -> str:
        """Lee la primera fila del archivo para determinar la BD."""
        with open(file_path, mode='r', encoding='latin-1') as file:
            reader = csv.DictReader(file, delimiter=delimiter)

            if reader.fieldnames:
                # Limpia espacios y caracteres invisibles (como el BOM)
                reader.fieldnames = [h.strip().lstrip('\ufeff') for h in reader.fieldnames]

            first_row = next(reader, None)
            
            if first_row is None:
                raise ValueError("El archivo está vacío.")
            
            print(f"first_row: {first_row}")
            
            negocio = first_row.get("Organization")

            print(f"Organization: {negocio}")

            if negocio is None:
                raise ValueError("El archivo no contiene la columna 'Organization'")
                
            db_id = NEGOCIO_TO_DB_MAP.get(negocio.upper())

            print(f"db_id: {db_id}")

            if db_id is None:
                raise ValueError(f"Organization '{negocio}' no reconocido.")
                
            return db_id
        
    def _create_dto_from_row(self, row: dict) -> PromoHoldDTO:
        """Función auxiliar para convertir una fila en DTO."""
        return PromoHoldDTO(
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
                                    
                                # Campos booleanos que no pueden ser nulos
                                ApplyOverExistingPromo  = to_bool(row['ApplyOverExistingPromo']),
                                ApplyOverOriginalPrice  = to_bool(row['ApplyOverOriginalPrice']),
                                UseCouponsAsTender      = to_bool(row['UseCouponsAsTender']),
                                Deleted                 = to_bool(row['Deleted'])
        )


    async def get_sku_from_upc(self, org: str, upc: str) -> Optional[str]:
        #query = "SELECT SKU FROM PRODUCT WHERE UPC = :upc"
        # NOTA: pyodbc usa '?' como placeholders. Ajustamos la consulta.
        query = "SELECT SKU FROM PRODUCT WHERE UPC = ?"
        row = await execute_query(org, query, {"upc": upc})
        #return row[0] if row else None
        if row and row[0] is not None:
            return str(row[0])
        return None


    async def process(self, file_path: str):
        print(f"Iniciando procesamiento de: {file_path}")
        
        # --- (Detección de delimitador y BD) ---
        with open(file_path, 'r', encoding='latin-1') as f:
            header = f.readline()

        delimiter = ';' if ';' in header else ','
        
        db_identifier = self._get_db_id_from_file(file_path, delimiter)
        self.repository = PromoHoldRepository(db_identifier=db_identifier)
        
        promo_dtos = []
        error_rows_report = []

        original_dtos = []
        upcs_a_traducir = set()
        
        # --- 1. ¡REVISA Y RELLENA ESTE MAPA! ---
        # (p = Precisión Total, s = N° de Decimales)
        # Esto es un EJEMPLO, debes poner los valores de tu tabla SQL
        COLUMN_PRECISION_MAP = {
            'Qty1': (18, 5),
            'GetPercentOff': (18, 5),
            'GetMoneyOff': (18, 5),
            'SpendMoney': (18, 5),
            'Price': (18, 5),
            'GroupQty': (18, 5)
        }

        DECIMAL_COLUMNS = list(COLUMN_PRECISION_MAP.keys())
        print(f"--- Iniciando validación de Precisión y Escala... ---")

        with open(file_path, mode='r', encoding='latin-1') as file:

            reader = csv.DictReader(file, delimiter=delimiter)
            
            if reader.fieldnames:
                reader.fieldnames = [h.strip().lstrip('\ufeff') for h in reader.fieldnames]

            for i, row in enumerate(reader, start=2):
                has_error = False
                
                # --- 2. VALIDACIÓN DE PRECISIÓN Y ESCALA ---
                for col_name in DECIMAL_COLUMNS:
                    if col_name not in row:
                        continue # La columna no está en el CSV, saltar

                    value_str = row.get(col_name)
                    
                    if not value_str or value_str.strip().lower() in ('', 'null'):
                        continue # Valor vacío o nulo, es válido

                    try:
                        value_decimal = Decimal(value_str.strip())
                        
                        t = value_decimal.as_tuple()

                        if not isinstance(t.exponent, int):
                            raise InvalidOperation("El valor no es un número finito (NaN o Infinito)")
                        
                        scale = abs(t.exponent)
                        total_digits = len(t.digits)
                        integer_digits = total_digits - scale
                        
                        if integer_digits <= 0:
                            integer_digits = 1 # Para números como 0.123

                        # --- INICIO DEL CÓDIGO FALTANTE ---
                        
                        # Obtener límites de la tabla
                        allowed_precision, allowed_scale = COLUMN_PRECISION_MAP[col_name]
                        allowed_integer_digits = allowed_precision - allowed_scale
                        
                        # 1. Comprobar Escala (decimales)
                        if scale > allowed_scale:
                            error_rows_report.append(
                                f"Fila {i}: Columna '{col_name}' tiene {scale} decimales (Límite: {allowed_scale}). Valor: '{value_str}'"
                            )
                            has_error = True
                        
                        # 2. Comprobar Precisión (dígitos enteros)
                        elif integer_digits > allowed_integer_digits:
                            error_rows_report.append(
                                f"Fila {i}: Columna '{col_name}' tiene {integer_digits} dígitos enteros (Límite: {allowed_integer_digits}). Valor: '{value_str}'"
                            )
                            has_error = True
                        
                        # --- FIN DEL CÓDIGO FALTANTE ---

                    except InvalidOperation:
                        # Este bloque captura "abc", "NaN", "Infinity", etc.
                        error_rows_report.append(f"Fila {i}: Columna '{col_name}' no es un decimal válido. Valor: '{value_str}'")
                        has_error = True
            
                if has_error:
                    continue 

                # --- 3. PROCESAMIENTO NORMAL DE FILAS VÁLIDAS ---
                try:
                    #dto = self._create_dto_from_row(row)
                    #promo_dtos.append(dto)

                    # Creamos el DTO, pero 'SKU' contendrá el UPC por ahora
                    dto = self._create_dto_from_row(row)
                    original_dtos.append(dto)

                    # Usamos 'SKU' (que es el UPC) para la traducción
                    upc = row.get('SKU') 
                    if upc:
                        upcs_a_traducir.add(upc)

                except Exception as e:
                    #print(f"!!! Error de DTO en fila {i}: {e}. Fila omitida: {row}")
                    print(f"!!! Error en fila: {e}. Fila omitida: {row}")

        # 4. Traducción masiva en paralelo
        print(f"Traduciendo {len(upcs_a_traducir)} UPCs únicos...")
        tasks = [self.get_sku_from_upc(db_identifier, upc) for upc in upcs_a_traducir]
        resultados = await asyncio.gather(*tasks)
        
        # 5. Crear el mapa de traducción (cache)
        translation_cache = dict(zip(upcs_a_traducir, resultados))
        
        # 6. Segunda pasada: Actualizar DTOs con el SKU traducido
        print("Actualizando DTOs con SKUs traducidos...")
        dtos_para_guardar = []
        for dto in original_dtos:
            upc_original = str(dto.SKU) # El DTO tiene el UPC como int
            translated_sku_str = translation_cache.get(upc_original)
            
            if translated_sku_str:
                # Si encontramos traducción, la usamos
                dto.SKU = to_int_safely(translated_sku_str)
            else:
                # Si no hay traducción, ¿qué hacemos? ¿Guardamos el UPC? ¿Omitimos?
                # Asumamos que guardamos el UPC original
                pass 
                
            dtos_para_guardar.append(dto)

        # 7. Guardar el lote final (usando asyncio.to_thread)
        if not dtos_para_guardar:
            print("ADVERTENCIA: No se generaron DTOs válidos para procesar.")
            return

        print(f"--- Se han creado {len(dtos_para_guardar)} DTOs. Enviando a la base de datos... ---")
        
        try:
            # self.repository.save_batch es SÍNCRONO
            await asyncio.to_thread(self.repository.save_batch, dtos_para_guardar)
            print("--- Lote guardado en la base de datos exitosamente. ---")

        except Exception as e:
            print(f"Error en la base de datos. Realizando rollback... {e}")
            raise


# --- 4. IMPRIMIR REPORTE DE ERRORES ---
#        if error_rows_report:
#            print("\n" + "="*50)
#            print(" DETECCIÓN DE ERRORES DE PRECISIÓN/ESCALA FINALIZADA ")
#            print("="*50)
#            print(f"Se encontraron {len(error_rows_report)} filas que exceden los límites de la base de datos.")
#            
#            for error_line in error_rows_report[:50]:
#                print(error_line)
#            if len(error_rows_report) > 50:
#                print(f"... y {len(error_rows_report) - 50} más.")
#            print("="*50 + "\n")
#        else:
#            print("--- No se encontraron errores de precisión o escala. ---")
#
#        
#        # --- 5. ENVÍO A LA BASE DE DATOS SÓLO DE FILAS VÁLIDAS ---
#        if not promo_dtos:
#            print("ADVERTENCIA: No se generaron DTOs válidos para procesar.")
#            return
#
#        print(f"--- Se han creado {len(promo_dtos)} DTOs (sin errores). Enviando a la base de datos... ---")
#
#        try:
#            self.repository.save_batch(promo_dtos)
#            print("--- Lote guardado en la base de datos exitosamente. ---")
#        except Exception as e:
#            print(f"Error en la base de datos. Realizando rollback... {e}")
#            raise