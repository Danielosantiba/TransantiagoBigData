import os
from datetime import date
from google.cloud import storage

def list_and_download_files(bucket_name):
    # Crea un cliente de almacenamiento
    client = storage.Client.from_service_account_json('key.json')
    
    # Obtiene el bucket
    bucket = client.get_bucket(bucket_name)
    
    # Crea una carpeta con el nombre de la fecha de hoy
    today = date.today()
    directory = today.strftime("%d-%m-%Y")
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Enumera todos los archivos en el bucket
    blobs = bucket.list_blobs()
    
    for blob in blobs:
        # Descarga el archivo en la carpeta recién creada
        blob.download_to_filename(f"{directory}/{blob.name}")
    
    print("Carpeta creada y archivos trasladados.")

# Reemplaza 'informacion-gob' con el nombre de tu bucket
list_and_download_files('informacion-gob')