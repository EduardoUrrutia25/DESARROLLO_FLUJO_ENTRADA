import os
import time
#from azure.storage.blob import BlobServiceClient
#from config import CONNECTION_STRING, CONTAINER_NAME # Asegúrate de importar tu config
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from config import AZURE_BLOB_CONNECTION_STRING

class BlobService:
    """
    Servicio para gestionar el ciclo de vida de los archivos en Azure Blob Storage.
    Flujo: Inbox -> Progress -> Finished (o Error)
    """

    def __init__(self):
        #self.blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        #self.container_client = self.blob_service_client.get_container_client(CONTAINER_NAME)
        self.container_client = ContainerClient.from_container_url(AZURE_BLOB_CONNECTION_STRING)
        
        # Definición de carpetas (prefijos)
        self.FOLDER_INBOX = "Inbox/"
        self.FOLDER_PROGRESS = "Progress/"
        self.FOLDER_FINISHED = "Finished/"
        self.FOLDER_ERROR = "Error/"

    def list_inbox_files(self):
        """Lista solo los archivos que están en la carpeta Inbox."""
        print(f"--- Buscando archivos en: {self.FOLDER_INBOX} ---")
        blob_list = self.container_client.list_blobs(name_starts_with=self.FOLDER_INBOX)
        
        # Filtramos para no traer el nombre de la carpeta en sí, si existe
        files = [blob.name for blob in blob_list if blob.name != self.FOLDER_INBOX]
        return files

    def download_blob(self, blob_name: str, local_path: str):
        """Descarga un blob a una ruta local."""
        print(f"Descargando: {blob_name} -> {local_path}")
        blob_client = self.container_client.get_blob_client(blob_name)
        
        with open(local_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        
        return local_path

    def move_blob(self, blob_name: str, target_folder: str) -> str:
        """
        'Mueve' un archivo copiándolo al destino y borrando el original.
        Devuelve el nuevo nombre del blob.
        """
        # 1. Determinar el nombre del nuevo blob
        # Ejemplo: Reemplaza "Inbox/archivo.csv" por "Progress/archivo.csv"
        # Detectamos en qué carpeta está actualmente basándonos en el nombre
        if blob_name.startswith(self.FOLDER_INBOX):
            current_folder = self.FOLDER_INBOX
        elif blob_name.startswith(self.FOLDER_PROGRESS):
            current_folder = self.FOLDER_PROGRESS
        else:
            # Si no tiene carpeta conocida, asumimos raíz o no hacemos nada
            current_folder = ""

        if current_folder:
            new_blob_name = blob_name.replace(current_folder, target_folder, 1)
        else:
            # Si el archivo no tenía prefijo, se lo agregamos
            new_blob_name = target_folder + blob_name

        print(f"Moviendo blob en Azure: {blob_name} -> {new_blob_name}")

        source_blob = self.container_client.get_blob_client(blob_name)
        dest_blob = self.container_client.get_blob_client(new_blob_name)

        # 2. Iniciar la copia (Start Copy)
        dest_blob.start_copy_from_url(source_blob.url)

        # 3. Esperar a que la copia termine (importante para archivos grandes)
        props = dest_blob.get_blob_properties()
        while props.copy.status == 'pending':
            time.sleep(0.1) # Pequeña espera
            props = dest_blob.get_blob_properties()

        # 4. Verificar éxito y borrar original
        if props.copy.status == 'success':
            source_blob.delete_blob()
            print(f"Movimiento exitoso. Original eliminado.")
            return new_blob_name
        else:
            raise Exception(f"Fallo al copiar el blob: {props.copy.status}")

    # --- Helpers rápidos para el flujo de trabajo ---

    def move_to_progress(self, blob_name: str) -> str:
        return self.move_blob(blob_name, self.FOLDER_PROGRESS)

    def move_to_finished(self, blob_name: str) -> str:
        return self.move_blob(blob_name, self.FOLDER_FINISHED)

    def move_to_error(self, blob_name: str) -> str:
        return self.move_blob(blob_name, self.FOLDER_ERROR)