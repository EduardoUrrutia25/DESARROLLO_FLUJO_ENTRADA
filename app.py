from flask import Flask, jsonify
from utils.blob_downloader import download_files_from_inbox
from controller import process_downloaded_files
import os

app = Flask(__name__)

@app.route('/process-inbox', methods=['POST'])
async def trigger_inbox_processing():
    """
    Endpoint de la API para iniciar el procesamiento completo del Inbox.
    No requiere payload.
    """
    print("Petición recibida en /process-inbox")
    
    # # 1. Descarga archivos de Azure
    download_folder = './data/Download'
    os.makedirs(download_folder, exist_ok=True)
    downloaded_files = download_files_from_inbox(download_folder)
    
    if not downloaded_files:
        return jsonify({"status": "No-op", "message": "No se encontraron archivos en Azure Inbox."})

    # 2. Procesa los archivos descargados localmente
    #result = process_downloaded_files()
    result = await process_downloaded_files()
    
    return jsonify(result)

if __name__ == '__main__':  
    app.run(host='0.0.0.0', port=5300, debug=True)