import os
import asyncio
from flask import Flask, jsonify
from utils.blob_downloader import BlobService # <-- Importamos la clase que te di antes
from controller import process_downloaded_files

app = Flask(__name__)

# Definición de rutas locales (Deben coincidir con las que usa tu controller.py)
LOCAL_DOWNLOAD_DIR = './data/Download'
LOCAL_FINISHED_DIR = './data/Finished'
LOCAL_ERROR_DIR    = './data/Error'

# Instancia del servicio de Azure
blob_service = BlobService()

@app.route('/process-inbox', methods=['POST'])
async def trigger_inbox_processing():
    """
    Orquestador principal:
    1. Mueve Azure Inbox -> Azure Progress -> Descarga Local.
    2. Ejecuta procesamiento local (que mueve archivos localmente).
    3. Sincroniza el estado final en Azure basado en el resultado local.
    """
    print("--- Petición recibida en /process-inbox ---")
    
    # Asegurar que existan los directorios locales de entrada
    os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)

    # ---------------------------------------------------------
    # PASO 1: Preparación (Azure -> Local)
    # ---------------------------------------------------------
    files_in_batch = [] # Lista para rastrear qué archivos estamos procesando
    
    # Listar archivos en Azure Inbox
    azure_inbox_files = blob_service.list_inbox_files()
    
    if not azure_inbox_files:
        return jsonify({"status": "No-op", "message": "No se encontraron archivos en Azure Inbox."})

    print(f"Archivos detectados en Inbox: {len(azure_inbox_files)}")

    for blob_name in azure_inbox_files:
        try:
            # A. Mover de 'Inbox' a 'Progress' en Azure (para bloquear el archivo)
            blob_name_progress = blob_service.move_to_progress(blob_name)
            
            # B. Descargar el archivo desde 'Progress' a local 'Download'
            filename = os.path.basename(blob_name_progress)
            local_path = os.path.join(LOCAL_DOWNLOAD_DIR, filename)
            
            blob_service.download_blob(blob_name_progress, local_path)
            
            # Guardamos el nombre y su ruta en Azure Progress para rastrearlo luego
            files_in_batch.append({
                "filename": filename,
                "azure_blob_progress": blob_name_progress
            })
            
        except Exception as e:
            print(f"Error preparando archivo {blob_name}: {e}")
            # Si falla la preparación, intentamos moverlo a Error en Azure inmediatamente
            try:
                blob_service.move_to_error(blob_name) 
            except: 
                pass

    if not files_in_batch:
        return jsonify({"status": "Error", "message": "Hubo archivos en Inbox pero fallaron al descargarse."})

    # ---------------------------------------------------------
    # PASO 2: Procesamiento Local (Tu Controller)
    # ---------------------------------------------------------
    print("--- Iniciando procesamiento local ---")
    
    # Esto procesará los archivos en ./data/Download y los moverá 
    # localmente a ./data/Finished o ./data/Error
    result = await process_downloaded_files()

    # ---------------------------------------------------------
    # PASO 3: Sincronización (Local -> Azure)
    # ---------------------------------------------------------
    print("--- Sincronizando estado con Azure ---")
    
    moves_summary = {"finished": [], "error": [], "unknown": []}

    for item in files_in_batch:
        filename = item["filename"]
        azure_blob_path = item["azure_blob_progress"] # Actualmente en carpeta Progress/
        
        # Verificamos dónde terminó el archivo localmente
        path_in_finished = os.path.join(LOCAL_FINISHED_DIR, filename)
        path_in_error = os.path.join(LOCAL_ERROR_DIR, filename)
        
        try:
            if os.path.exists(path_in_finished):
                # CASO ÉXITO: El controller lo movió a Finished localmente
                blob_service.move_to_finished(azure_blob_path)
                moves_summary["finished"].append(filename)
                
            elif os.path.exists(path_in_error):
                # CASO ERROR: El controller lo movió a Error localmente
                blob_service.move_to_error(azure_blob_path)
                moves_summary["error"].append(filename)
                
            else:
                # CASO EXTRAÑO: El archivo no está ni en Finished ni en Error.
                # ¿Quizás sigue en Download? ¿O el controller le cambió el nombre?
                # Por seguridad, lo movemos a Error en Azure para revisarlo.
                print(f"ADVERTENCIA: El archivo {filename} desapareció del flujo local.")
                blob_service.move_to_error(azure_blob_path)
                moves_summary["unknown"].append(filename)
                
        except Exception as e:
            print(f"Error sincronizando archivo {filename} en Azure: {e}")

    # ---------------------------------------------------------
    # Respuesta Final
    # ---------------------------------------------------------
    final_response = {
        "local_processing_result": result,
        "azure_sync_summary": moves_summary
    }
    
    return jsonify(final_response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5300, debug=True)