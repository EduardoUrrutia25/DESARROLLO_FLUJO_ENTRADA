import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from config import AZURE_BLOB_CONNECTION_STRING

def download_files_from_inbox(download_folder: str) -> list[str]:
    """
    Se conecta a Azure 'inbox', descarga todos los archivos a 'download_folder'
    y los elimina del contenedor. Devuelve la lista de archivos descargados.
    """
    print("Conectando a Azure Blob Storage...")
    try:
        # AZURE_BLOB_CONNECTION_STRING es la URL SAS al contenedor
        print(f"URL cargada: {AZURE_BLOB_CONNECTION_STRING[:80]}...") # Imprime solo el inicio para no mostrar el token completo
        container_client = ContainerClient.from_container_url(AZURE_BLOB_CONNECTION_STRING)

        prefix = "Inbox/"
        blobs = container_client.list_blobs(name_starts_with=prefix)
        downloaded_files = []
        
        for blob in blobs:
            blob_name = blob.name # ej: "Inbox/IN-PROMO-20251031.csv"
            
            # 1. Ignora la carpeta "Inbox/" si aparece en la lista
            if blob_name == prefix:
                continue

            # 2. Extrae solo el nombre del archivo (ej: "IN-PROMO-20251031.csv")
            file_only_name = os.path.basename(blob_name)
            
            print(f"Descargando archivo: {blob_name} como {file_only_name}")
            
            # 3. Define la ruta de descarga local correcta
            local_file_path = os.path.join(download_folder, file_only_name)
            
            # Descarga el archivo
            blob_client = container_client.get_blob_client(blob_name)

            with open(local_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            
            # Elimina el blob después de la descarga exitosa
            #print(f"Eliminando blob: {blob_name}")
            #blob_client.delete_blob()
            
            downloaded_files.append(local_file_path)
        
        # 4. Mueve el 'return' fuera del bucle 'for'
        return downloaded_files

    except Exception as e:
        print(f"!!! ERROR al conectar o descargar de Azure: {e}")
        return []