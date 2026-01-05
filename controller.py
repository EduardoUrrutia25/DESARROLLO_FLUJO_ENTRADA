import os
import glob
import shutil
import asyncio

from services   import FileOrchestrator
from typing     import Optional

def setup_directories():
    base_path = './data/'
    for folder in ['Download', 'Progress', 'Finished', 'Error']:
        os.makedirs(os.path.join(base_path, folder), exist_ok=True)

def parse_filename(file_name: str) -> Optional[str]:
    """Parsea el nombre del archivo 'IN-FLOWNAME-YYYYMMDDHHMMSS.csv'."""
    try:
        parts = file_name.replace('.csv', '').split('-', 2)
        if len(parts) == 3 and parts[0] == 'IN':
            return parts[1] # flow_type
        return None
    except Exception as e:
        print(f"Error parseando nombre de archivo {file_name}: {e}")
        return None

async def process_downloaded_files() -> dict:
    """
    Procesa todos los archivos encontrados en la carpeta Download.
    """
    setup_directories()
    
    download_path   = './data/Download'
    progress_path   = './data/Progress'
    finished_path   = './data/Finished'
    error_path      = './data/Error'
    
    orchestrator = FileOrchestrator()
    
    # 1. Busca archivos con el prefijo 'IN-'
    files_in_download = glob.glob(os.path.join(download_path, "IN-*.csv"))
    
    if not files_in_download:
        return {"status": "No-op", "message": "No se encontraron archivos en Download."}

    processed_files, failed_files = [], []
    
    for source_path in files_in_download:
        file_name = os.path.basename(source_path)
        
# 2. Extrae el flow_type del nombre del archivo
#        flow_type = parse_filename(file_name)
#        
#        if flow_type is None:
#            print(f"Archivo '{file_name}' no cumple el formato 'IN-FLOWNAME-...' y será ignorado.")
#            continue
            
        progress_file_path = os.path.join(progress_path, file_name)

        try:
            shutil.move(source_path, progress_file_path)

            flow_type = parse_filename(file_name)
            if flow_type is None:
                raise ValueError("Nombre de archivo no cumple el formato.")

            print(f"Procesando: {file_name} (Flujo: {flow_type})")
            
            # 3. Llamada al orquestador ahora es asíncrona
            await orchestrator.process_file(progress_file_path, flow_type)
            
            # 3. Llama al orquestador con el flow_type extraído
            #print(f"Procesando: {file_name} (Flujo: {flow_type})")
            #orchestrator.process_file(progress_file_path, flow_type)
            
            finished_file_path = os.path.join(finished_path, file_name)
            shutil.move(progress_file_path, finished_file_path)
            processed_files.append(file_name)

        except Exception as e:
            print(f"!!! ERROR fatal al procesar '{file_name}': {e}")
            error_file_path = os.path.join(error_path, file_name)
            shutil.move(progress_file_path, error_file_path)
            failed_files.append({"file": file_name, "error": str(e)})
            
    message = f"Procesamiento completado. Exitosos: {len(processed_files)}, Fallidos: {len(failed_files)}."
    return {
        "status": "Completed", 
        "message": message, 
        "files_finished": processed_files,
        "files_in_error": failed_files
    }