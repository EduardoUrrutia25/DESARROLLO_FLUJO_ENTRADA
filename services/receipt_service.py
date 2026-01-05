import csv
import asyncio

from decimal        import Decimal, ConversionSyntax, InvalidOperation
from typing         import List, Optional

from repositories   import ReceiptHoldRepository
from dtos           import ReceiptHoldDTO
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

class ReceiptFileService(FileProcessingService):
    """Servicio para procesar el envío de transacciones de venta."""
    #def __init__(self):
    #    self.repository = ReceiptHoldRepository()

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
#        Receipt_dtos: List[ReceiptHoldDTO] = []
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
#                        dto = ReceiptHoldDTO(
#                                    StoreNo	            = to_int_safely(row['StoreNo']),
#                                    ReceiptId	        = to_uuid_safely(row['ReceiptId']),
#                                    ReceiptNo	        = to_int_safely(row['ReceiptNo']),
#                                    SalesCode	        = row['SalesCode'],
#                                    SalesDate	        = parse_datetime_safely(row['SalesDate']),
#                                    DocReference	    = row['DocReference'],
#                                    DocNumber	        = row['DocNumber'],
#                                    Cashier	            = row['Cashier'],
#                                    SubTotal	        = to_decimal_safely(row['SubTotal']),
#                                    TaxPercent	        = to_decimal_safely(row['TaxPercent']),
#                                    ShippingTotal	    = to_decimal_safely(row['ShippingTotal']),
#                                    ShipViaName	        = row['ShipViaName'],
#                                    FeeTotal	        = to_decimal_safely(row['FeeTotal']),
#                                    FeeName	            = row['FeeName'],
#                                    PayTotal	        = to_decimal_safely(row['PayTotal']),
#                                    DiscTotal	        = to_decimal_safely(row['DiscTotal']),
#                                    SpecialTaxAmount	= to_decimal_safely(row['SpecialTaxAmount']),
#                                    Notes	            = row['Notes'],
#                                    Notes2	            = row['Notes2'],
#                                    Notes3	            = row['Notes3'],
#                                    Flag1	            = to_int_safely(row['Flag1']),
#                                    Flag2	            = to_int_safely(row['Flag2']),
#                                    Flag3	            = to_int_safely(row['Flag3']),
#                                    ReferenceId 	    = row['ReferenceId'],
#                                    EReceipt	        = row['EReceipt'],
#                                    EReceiptInfo	    = row['EReceiptInfo'],
#                                    BillCustomer	    = to_str_or_none(row['BillCustomer']),
#                                    ShipCustomer	    = to_str_or_none(row['ShipCustomer'])
#                                )
#                        Receipt_dtos.append(dto)
#                    
#                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
#                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
#            
#            if not Receipt_dtos:
#                print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
#                return
#
#            print(f"--- Se han creado {len(Receipt_dtos)} DTOs. Enviando a la base de datos... ---")
#            self.repository.save_batch(Receipt_dtos)
#
#        except Exception as e:
#            
#            import traceback
#            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
#            traceback.print_exc()


#    def _create_dto_from_row(self, row: dict) -> ReceiptHoldDTO:
#        """Función auxiliar para convertir una fila en DTO."""
#        return ReceiptHoldDTO(
#                                StoreNo	            = to_int_safely(row['StoreNo']),
#                                #ReceiptId	        = to_uuid_safely(row['ReceiptId']),
#                                ReceiptId	        = row['ReceiptId'],
#                                ReceiptNo	        = to_int_safely(row['ReceiptNo']),
#                                SalesCode	        = row['SalesCode'],
#                                SalesDate	        = parse_datetime_safely(row['SalesDate']),
#                                DocReference	    = row['DocReference'],
#                                DocNumber	        = row['DocNumber'],
#                                Cashier	            = row['Cashier'],
#                                SubTotal	        = to_decimal_safely(row['SubTotal']),
#                                TaxPercent	        = to_decimal_safely(row['TaxPercent']),
#                                ShippingTotal	    = to_decimal_safely(row['ShippingTotal']),
#                                ShipViaName	        = row['ShipViaName'],
#                                FeeTotal	        = to_decimal_safely(row['FeeTotal']),
#                                FeeName	            = row['FeeName'],
#                                PayTotal	        = to_decimal_safely(row['PayTotal']),
#                                DiscTotal	        = to_decimal_safely(row['DiscTotal']),
#                                SpecialTaxAmount	= to_decimal_safely(row['SpecialTaxAmount']),
#                                Notes	            = row['Notes'],
#                                Notes2	            = row['Notes2'],
#                                Notes3	            = row['Notes3'],
#                                Flag1	            = to_int_safely(row['Flag1']),
#                                Flag2	            = to_int_safely(row['Flag2']),
#                                Flag3	            = to_int_safely(row['Flag3']),
#                                ReferenceId 	    = row['ReferenceId'],
#                                EReceipt	        = row['EReceipt'],
#                                EReceiptInfo	    = row['EReceiptInfo'],
#                                BillCustomer	    = to_str_or_none(row['BillCustomer']),
#                                ShipCustomer	    = to_str_or_none(row['ShipCustomer']),
#                                Process1            = to_bool(row['Process1']),
#                                Process2            = to_bool(row['Process2']),
#                                Process3            = to_bool(row['Process3']),
#                                Type_Document       = to_int_safely(row['Type_Document']),
#                                ID_Document         = to_str_or_none(row['ID_Document']),
#                                FullName            = to_str_or_none(row['FullName']),
#                                Address             = to_str_or_none(row['Address'])
#        )
    
    def _create_dto_from_row(self, row: dict) -> ReceiptHoldDTO:
        """
        Función auxiliar para convertir una fila en DTO (Corregida).
        """
        try:
            _StoreNo            = to_int_safely(row['StoreNo'])
            _ReceiptId	        = row['ReceiptId']
            _ReceiptNo	        = to_int_safely(row['ReceiptNo'])
            _SalesCode	        = row['SalesCode']
            _SalesDate	        = parse_datetime_safely(row['SalesDate'])
            _DocReference	    = to_str_or_none(row['DocReference'])
            _DocNumber	        = row['DocNumber']
            _Cashier            = row['Cashier']
            _SubTotal	        = to_decimal_safely(row['SubTotal'])
            _TaxPercent	        = to_decimal_safely(row['TaxPercent'])
            _ShippingTotal	    = to_decimal_safely(row['ShippingTotal'])
            _ShipViaName        = to_str_or_none(row['ShipViaName'])
            _FeeTotal	        = to_decimal_safely(row['FeeTotal'])
            _FeeName            = to_str_or_none(row['FeeName'])
            _PayTotal	        = to_decimal_safely(row['PayTotal'])
            _DiscTotal	        = to_decimal_safely(row['DiscTotal'])
            _SpecialTaxAmount	= to_decimal_safely(row['SpecialTaxAmount'])
            _Notes	            = to_str_or_none(row['Notes'])
            _Notes2	            = to_str_or_none(row['Notes2'])
            _Notes3	            = to_str_or_none(row['Notes3'])
            _Flag1	            = to_int_safely(row['Flag1'])
            _Flag2	            = to_int_safely(row['Flag2'])
            _Flag3	            = to_int_safely(row['Flag3'])
            _ReferenceId 	    = to_str_or_none(row['ReferenceId'])
            _EReceipt	        = to_str_or_none(row['EReceipt'])
            _EReceiptInfo	    = to_str_or_none(row['EReceiptInfo'])
            _BillCustomer	    = row['BillCustomer']
            _ShipCustomer	    = row['ShipCustomer']
            _Process1           = to_bool(row['Process1'])
            _Process2           = to_bool(row['Process2'])
            _Process3           = to_bool(row['Process3'])
            _Type_Document      = to_int_safely(row['Type_Document'])
            _ID_Document        = row['ID_Document']
            _FullName           = row['FullName']
            _Address            = row['Address']

            # --- Construcción del DTO ---
            return ReceiptHoldDTO(
                StoreNo             = _StoreNo,
                ReceiptId           = _ReceiptId,
                ReceiptNo           = _ReceiptNo,
                SalesCode           = _SalesCode,
                SalesDate           = _SalesDate,
                DocReference        = _DocReference,
                DocNumber           = _DocNumber,
                Cashier             = _Cashier,
                SubTotal            = _SubTotal,
                TaxPercent          = _TaxPercent,
                ShippingTotal       = _ShippingTotal,
                ShipViaName         = _ShipViaName,
                FeeTotal            = _FeeTotal,
                FeeName             = _FeeName,
                PayTotal            = _PayTotal,
                DiscTotal           = _DiscTotal,
                SpecialTaxAmount    = _SpecialTaxAmount,
                Notes               = _Notes,
                Notes2              = _Notes2,
                Notes3              = _Notes3,
                Flag1               = _Flag1,
                Flag2               = _Flag2,
                Flag3               = _Flag3,
                ReferenceId         = _ReferenceId,
                EReceipt            = _EReceipt,
                EReceiptInfo        = _EReceiptInfo,
                BillCustomer        = _BillCustomer,
                ShipCustomer        = _ShipCustomer,
                Process1            = _Process1,
                Process2            = _Process2,
                Process3            = _Process3,
                Type_Document       = _Type_Document,
                ID_Document         = _ID_Document,
                FullName            = _FullName,
                Address             = _Address
            )
        
        except (ValueError, TypeError) as e:
            print("\n" + "!"*60)
            print(f"!!! ERROR DE TIPO DE DATO DETECTADO: {e}")
            print(f"!!! Revisa funciones 'to_..._safely' para la fila:")
            print(f"{row}")
            print("!"*60 + "\n")
            raise
    
    async def get_store_no(self, org: str, ext_id: str) -> Optional[str]:
        query = "SELECT StoreNo FROM STORE WHERE StoreExternalId = ?"
        row = await execute_query(org, query, {"ext_id": ext_id})
        #return row[0] if row else None
        if row and row[0] is not None:
            return str(row[0])
        return None
    
    async def get_vendor_code(self, org: str, ext_id: str) -> Optional[str]:
        query   = "SELECT VendorCode FROM VENDOR WHERE ExternalId = ?"
        row     = await execute_query(org, query, {"ext_id": ext_id})
        #return  row[0] if row else None
        if row and row[0] is not None:
            return str(row[0])
        return None
    
    async def process(self, file_path: str):
        print(f"Iniciando procesamiento de RECIBO: {file_path}")
        
        with open(file_path, 'r', encoding='latin-1') as f:
            header = f.readline()
        delimiter = ';' if ';' in header else ','
        
        db_identifier = self._get_db_id_from_file(file_path, delimiter)
        self.repository = ReceiptHoldRepository(db_identifier=db_identifier)
        
        valid_rows_for_translation = []
        error_rows_report = []
        
        # --- Mapa de Validación de Decimales ---
        COLUMN_PRECISION_MAP = {
            'SubTotal': (18, 5),
            'TaxPercent': (18, 5),
            'ShippingTotal': (18, 5),
            'FeeTotal': (18, 5),
            'PayTotal': (18, 5),
            'DiscTotal': (18, 5),
            "SpecialTaxAmount": (18, 5)
        }
        DECIMAL_COLUMNS = list(COLUMN_PRECISION_MAP.keys())
        print(f"--- Iniciando validación de Precisión y Escala... ---")
        
        stores_a_traducir = set()
        # cashiers_a_traducir ha sido eliminado

        # --- 1. Primera Pasada: Validación de Decimales y Recolección ---
        with open(file_path, mode='r', encoding='latin-1') as file:
            reader = csv.DictReader(file, delimiter=delimiter)
            
            if reader.fieldnames:
                reader.fieldnames = [h.strip().lstrip('\ufeff') for h in reader.fieldnames]

            for i, row in enumerate(reader, start=2):
                has_error = False
                
                # --- Validación de Precisión y Escala (sin cambios) ---
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
                
                # Recolecta solo StoreNo
                store_ext_id = row.get('StoreNo')
                if store_ext_id:
                    stores_a_traducir.add(store_ext_id)
                
                # Lógica de recolección de Cashier eliminada

        # --- 2. Reporte de Errores de Validación (sin cambios) ---
        if error_rows_report:
            print("\n" + "="*50)
            print(" ERRORES DE PRECISIÓN/ESCALA ENCONTRADOS ")
            print(f"Se encontraron {len(error_rows_report)} filas con errores decimales. Estas filas serán omitidas.")
            for error_line in error_rows_report[:50]: print(error_line)
            if len(error_rows_report) > 50: print(f"... y {len(error_rows_report) - 50} más.")
            print("="*50 + "\n")
        else:
            print("--- No se encontraron errores de precisión o escala. ---")

        # --- 3. Traducción Masiva en Paralelo (Solo StoreNo) ---
        print(f"Traduciendo {len(stores_a_traducir)} StoreIDs únicos...")
        
        store_tasks = [self.get_store_no(db_identifier, ext_id) for ext_id in stores_a_traducir]
        
        # Se eliminó la tarea de Cashier
        store_resultados = await asyncio.gather(*store_tasks)
        
        # --- 4. Crear Mapas de Traducción (Caché) ---
        store_cache = dict(zip(stores_a_traducir, store_resultados))
        # Se eliminó cashier_cache
        
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
                
                # Lógica de traducción de Cashier eliminada
                # dto.Cashier se queda con el valor original del archivo

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