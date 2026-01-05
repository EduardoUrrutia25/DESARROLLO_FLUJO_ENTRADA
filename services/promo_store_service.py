import csv
import asyncio

from decimal import Decimal, ConversionSyntax, InvalidOperation
from typing import List, Optional

from repositories import PromoStoreHoldRepository
from dtos import PromoStoreHoldDTO
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
    

class PromoStoreFileService(FileProcessingService):
    """Servicio para procesar el archivo de tienda asociado a una promoción."""
    #def __init__(self):
    #    self.repository = PromoStoreHoldRepository()

    def __init__(self):
        self.repository = None # Se inicializará durante el process

    def _get_db_id_from_file(self, file_path: str, delimiter: str) -> str:
        """Lee la primera fila del archivo para determinar la BD."""
        with open(file_path, mode='r', encoding='latin-1') as file:
            reader = csv.DictReader(file, delimiter=delimiter)

            if reader.fieldnames:
                reader.fieldnames = [h.strip().lstrip('\ufeff') for h in reader.fieldnames]

            first_row = next(reader, None)
            
            if first_row is None:
                raise ValueError("El archivo está vacío.")
            
            negocio = first_row.get("Organization")

            if negocio is None:
                raise ValueError("El archivo no contiene la columna 'Organization'")
                
            db_id = NEGOCIO_TO_DB_MAP.get(negocio.upper())

            if db_id is None:
                raise ValueError(f"Organization '{negocio}' no reconocido.")
                
            return db_id

#    def process(self, file_path: str):
#
#        promoStore_dtos: List[PromoStoreHoldDTO] = []
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
#                        dto = PromoStoreHoldDTO(
#                                    PromoId = to_int_safely(row['PromoId']),
#                                    Comment = to_str_or_empty(row['Comment']),
#                                    StoreNo = to_int_safely(row['StoreNo']),
#                                    Deleted = to_int_safely(row['Deleted'])
#                                )
#                        promoStore_dtos.append(dto)
#                    
#                    except (ValueError, TypeError, KeyError, ConversionSyntax) as e:
#                        print(f"!!! Error CAPTURADO en la fila {i}. Causa: {e} | Datos: {row}")
#            
#            if not promoStore_dtos:
#                print("--- ADVERTENCIA: No se generaron DTOs válidos para procesar. ---")
#                return
#
#            print(f"--- Se han creado {len(promoStore_dtos)} DTOs. Enviando a la base de datos... ---")
#            self.repository.save_batch(promoStore_dtos)
#
#        except Exception as e:
#            
#            import traceback
#            print(f"!!! ERROR FATAL DETALLADO en el servicio: {e}")
#            traceback.print_exc()


    def _create_dto_from_row(self, row: dict) -> PromoStoreHoldDTO:
        """Función auxiliar para convertir una fila en DTO."""
        return PromoStoreHoldDTO(
            PromoId = to_int_safely(row['PromoId']),
            Comment = to_str_or_empty(row['Comment']),
            StoreNo = to_int_safely(row['StoreNo']), # 'StoreNo' contiene el ext_id por ahora
            Deleted = to_int_safely(row['Deleted'])
        )
    
    async def get_store_no(self, org: str, ext_id: str) -> Optional[str]:
        """Función de traducción para obtener el StoreNo interno."""
        query = "SELECT StoreNo FROM STORE WHERE StoreExternalId = ?"
        row = await execute_query(org, query, {"ext_id": ext_id})
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
        self.repository = PromoStoreHoldRepository(db_identifier=db_identifier)
        
        original_dtos = []
        externalids_a_traducir = set()
        
        # --- 1. LECTURA Y CREACIÓN DE DTOS ---
        with open(file_path, mode='r', encoding='latin-1') as file:
            reader = csv.DictReader(file, delimiter=delimiter)
            
            if reader.fieldnames:
                reader.fieldnames = [h.strip().lstrip('\ufeff') for h in reader.fieldnames]

            for i, row in enumerate(reader, start=2):
                try:
                    # Creamos el DTO, pero 'StoreNo' contendrá el StoreExternalId por ahora
                    dto = self._create_dto_from_row(row)
                    original_dtos.append(dto)

                    # Usamos 'StoreNo' (que es el StoreExternalId) para la traducción
                    ext_id = row.get('StoreNo') 
                    if ext_id:
                        externalids_a_traducir.add(ext_id)

                except Exception as e:
                    print(f"!!! Error en fila {i}: {e}. Fila omitida: {row}")

        # 2. Traducción masiva en paralelo
        print(f"Traduciendo {len(externalids_a_traducir)} ExternalIDs únicos...")
        tasks = [self.get_store_no(db_identifier, ext_id) for ext_id in externalids_a_traducir]
        resultados = await asyncio.gather(*tasks)
        
        # 3. Crear el mapa de traducción (cache)
        translation_cache = dict(zip(externalids_a_traducir, resultados))
        
        # 4. Segunda pasada: Actualizar DTOs con el StoreNo traducido
        print("Actualizando DTOs con StoreNo traducidos...")
        dtos_para_guardar = []
        for dto in original_dtos:
            # El DTO tiene el StoreExternalId (como int o None)
            external_id_original = str(dto.StoreNo) 
            translated_StoreNo_str = translation_cache.get(external_id_original)
            
            if translated_StoreNo_str:
                # Si encontramos traducción, la usamos
                dto.StoreNo = to_int_safely(translated_StoreNo_str)
            else:
                # Si no hay traducción, guardamos el StoreExternalId original
                pass 
                
            dtos_para_guardar.append(dto)

        # 5. Guardar el lote final
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