import csv
import asyncio

from decimal        import Decimal, ConversionSyntax, InvalidOperation
from typing         import List, Optional

from repositories   import ReceiptTenderHoldRepository
from dtos           import ReceiptTenderHoldDTO
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

TENDER_ID_MAP_TAMBO = {
    "1000": "1",    # CASH
    "1020": "111",  # REDONDEO
    "3001": "5",    # IZIPAY
    "4001": "98",   # COUPON
    "4040": "103",  # GIFTCARD TAMBO
    "4050": "102",  # VALE TAMBO
    "9003": "104",  # PAGO EN LINEA
    "9001": "109",  # RAPPI
    "9002": "106",  # PEDIDOS YA
}

TENDER_ID_MAP_ARUMA = {
    "1000": "1",    # CASH
    "1020": "111",  # REDONDEO
    "3001": "5",    # IZIPAY
    "4001": "98",   # CUPON
    "4040": "6",    # GIFTCARD
    "9001": "108",  # RAPPI
    "9003": "109",  # PAGO EN LINEA
    "4050": "8",    # CREDITO
}

class ReceiptTenderFileService(FileProcessingService):
    """Servicio para procesar el Tender de transacciones de venta."""
    #def __init__(self):
    #    self.repository = ReceiptTenderHoldRepository()

    def __init__(self):
        self.repository = None  # Se inicializará durante el process
        self.tender_map = None  # Se asignará según la organización
        self.organization = None  # Se guardará la organización actual
        

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
            
            self.organization = negocio.upper()

            # Asignar el diccionario correcto según la organización
            if self.organization == "TAMBO":
                self.tender_map = TENDER_ID_MAP_TAMBO
                print("Usando mapeo de TenderId para TAMBO")
            elif self.organization == "ARUMA":
                self.tender_map = TENDER_ID_MAP_ARUMA
                print("Usando mapeo de TenderId para ARUMA")
            else:
                raise ValueError(f"No hay mapeo de TenderId definido para '{self.organization}'")
                
            return db_id

#    def process(self, file_path: str):
#
#        ReceiptTender_dtos: List[ReceiptTenderHoldDTO] = []
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
#                        dto = ReceiptTenderHoldDTO(
#                                    StoreNo 	        = to_int_safely(row['StoreNo']),
#                                    LineId 	            = to_int_safely(row['LineId']),
#                                    TenderId 	        = row['TenderId'],
#                                    PaymentDate 	    = parse_datetime_safely(row['PaymentDate']),
#                                    CardName 	        = to_str_or_none(row['CardName']),
#                                    CardNumber 	        = to_str_or_none(row['CardNumber']),
#                                    CardExpDate 	    = to_str_or_none(row['CardExpDate']),
#                                    CardAuthorization 	= to_str_or_none(row['CardAuthorization']),
#                                    CardZipCode 	    = to_str_or_none(row['CardZipCode']),
#                                    CardSequenceNumber 	= to_str_or_none(row['CardSequenceNumber']),
#                                    EFT 	            = row['EFT'],
#                                    CheckNumber 	    = row['CheckNumber'],
#                                    GiftCertNumber 	    = row['GiftCertNumber'],
#                                    TakeAmount 	        = to_decimal_safely(row['TakeAmount']),
#                                    GiveAmount 	        = to_decimal_safely(row['GiveAmount']),
#                                    Notes 	            = to_str_or_none(row['Notes']),
#                                    DebitSale 	        = row['DebitSale'],
#                                    PaymentDay 	        = to_int_safely(row['PaymentDay']),
#                                    CurrencyId 	        = to_int_safely(row['CurrencyId']),
#                                    ExchangeRate 	    = to_decimal_safely(row['ExchangeRate']),
#                                    TakeBase 	        = to_decimal_safely(row['TakeBase']),
#                                    TakeExchange 	    = to_decimal_safely(row['TakeExchange']),
#                                    ReceiptId 	        = to_str_or_none(row['ReceiptId'])
#                                )
#                        ReceiptTender_dtos.append(dto)
#                    
#                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
#                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
#            
#            if not ReceiptTender_dtos:
#                print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
#                return
#
#            print(f"--- Se han creado {len(ReceiptTender_dtos)} DTOs. Enviando a la base de datos... ---")
#            self.repository.save_batch(ReceiptTender_dtos)
#
#        except Exception as e:
#            
#            import traceback
#            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
#            traceback.print_exc()


    def _create_dto_from_row(self, row: dict) -> ReceiptTenderHoldDTO:
        """Función auxiliar para convertir una fila en DTO."""
        
        # Helper para campos bit obligatorios con default
        def parse_bool_required(value, default=False):
            """Para campos bit obligatorios (NOT NULL)"""
            if not value or value.strip() == '':
                return default
            
            cleaned = value.strip()
            if cleaned == '1':
                return True
            elif cleaned == '0':
                return False
            else:
                return default  # Para valores inesperados
                
        # Helper para int obligatorios con default
        def parse_int_required(value, default=0):
            """Para campos int obligatorios (NOT NULL)"""
            result = to_int_safely(value)
            return result if result is not None else default
        
        # Helper para decimal obligatorios con default
        def parse_decimal_required(value, default=Decimal('0')):
            """Para campos decimal obligatorios (NOT NULL)"""
            result = to_decimal_safely(value)
            return result if result is not None else default
        
        return ReceiptTenderHoldDTO(
            # ===== OBLIGATORIOS (NOT NULL) =====
            StoreNo             = parse_int_required(row['StoreNo']),
            LineId              = parse_int_required(row['LineId']),
            ReceiptId           = to_str_or_empty(row['ReceiptId']),
            TenderId            = to_str_or_empty(row['TenderId']),
            PaymentDate         = to_str_or_empty(row['PaymentDate']),  # VARCHAR(19), no datetime
            EFT                 = parse_bool_required(row['EFT'], default=False),
            TakeAmount          = parse_decimal_required(row['TakeAmount']),
            GiveAmount          = parse_decimal_required(row['GiveAmount']),
            DebitSale           = parse_bool_required(row['DebitSale'], default=False),
            CurrencyId          = parse_int_required(row['CurrencyId']),
            
            # ===== OPCIONALES (NULL) =====
            CardName            = to_str_or_none(row['CardName']),
            CardNumber          = to_str_or_none(row['CardNumber']),
            CardExpDate         = to_str_or_none(row['CardExpDate']),
            CardAuthorization   = to_str_or_none(row['CardAuthorization']),
            CardZipCode         = to_str_or_none(row['CardZipCode']),
            CardSequenceNumber  = to_str_or_none(row['CardSequenceNumber']),
            CheckNumber         = to_str_or_none(row['CheckNumber']),
            GiftCertNumber      = to_str_or_none(row['GiftCertNumber']),
            Notes               = to_str_or_none(row['Notes']),
            PaymentDay          = to_int_safely(row['PaymentDay']),
            ExchangeRate        = to_decimal_safely(row['ExchangeRate']),
            TakeBase            = to_decimal_safely(row['TakeBase']),
            TakeExchange        = to_decimal_safely(row['TakeExchange'])
        )

    async def get_store_no(self, org: str, ext_id: str) -> Optional[str]:
        query = "SELECT StoreNo FROM STORE WHERE StoreExternalId = ?"
        row = await execute_query(org, query, {"ext_id": ext_id})
        #return row[0] if row else None
        if row and row[0] is not None:
            return str(row[0])
        return None
    
    def transform_tender_id(self, tender_oracle_id: str) -> Optional[str]:
        """Transforma TENDER_TYPE_ID_ORACLE a TenderId interno usando el diccionario de la organización."""
        if not tender_oracle_id:
            return None
        
        if self.tender_map is None:
            print(f"ERROR: No se ha establecido el mapeo de TenderId. Organización: {self.organization}")
            return None
        
        tender_id_clean = tender_oracle_id.strip()
        transformed = self.tender_map.get(tender_id_clean)
        
        if transformed is None:
            print(f"ADVERTENCIA: TenderId Oracle '{tender_id_clean}' no encontrado en el mapeo de {self.organization}.")
        
        return transformed
    
    
    async def process(self, file_path: str):
        print(f"Iniciando procesamiento de TENDER DE RECIBO: {file_path}")
        
        # --- (Detección de delimitador y BD) ---
        with open(file_path, 'r', encoding='latin-1') as f:
            header = f.readline()
        delimiter = ';' if ';' in header else ','
        
        db_identifier = self._get_db_id_from_file(file_path, delimiter)
        # Asegúrate de que el nombre del Repositorio sea el correcto
        self.repository = ReceiptTenderHoldRepository(db_identifier=db_identifier)
        
        # --- Listas de DTOs y Errores ---
        valid_rows_for_translation = []
        error_rows_report = []
        
        # --- Mapa de Validación de Decimales ---
        COLUMN_PRECISION_MAP = {
            'TakeAmount': (18, 5),
            'GiveAmount': (18, 5),
            'ExchangeRate': (18, 5),
            'TakeBase': (18, 5),
            'TakeExchange': (18, 5)
        }
        DECIMAL_COLUMNS = list(COLUMN_PRECISION_MAP.keys())
        print(f"--- Iniciando validación de Precisión y Escala... ---")
        
        # --- Sets para recolectar IDs a traducir ---
        stores_a_traducir = set()

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
        print(f"Traduciendo {len(stores_a_traducir)} StoreIDs únicos...")
        
        store_tasks = [self.get_store_no(db_identifier, ext_id) for ext_id in stores_a_traducir]
        
        store_resultados = await asyncio.gather(*store_tasks)
        
        # --- 4. Crear Mapas de Traducción (Caché) ---
        store_cache = dict(zip(stores_a_traducir, store_resultados))
        
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

                tender_oracle_id = row.get('TenderId')
                if tender_oracle_id:
                    transformed_tender = self.transform_tender_id(tender_oracle_id)
                    if transformed_tender:
                        dto.TenderId = transformed_tender
                    else:
                        print(f"ADVERTENCIA: No se pudo transformar TenderId '{tender_oracle_id}'. Se omite la fila.")
                        continue  # Omitir esta fila si no se puede transformar

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