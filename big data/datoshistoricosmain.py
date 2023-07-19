import requests
import zipfile
import os
import csv
from google.cloud import storage
from datetime import datetime

# Realiza una solicitud GET al enlace proporcionado
response = requests.get('https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et')
data = response.json()

# Extrae el primer enlace en "url" del JSON
url = data['result']['resources'][0]['url']

# Extrae la fecha de "last_modified" del JSON
last_modified = data['result']['resources'][0]['last_modified']
date_string = datetime.strptime(last_modified, "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y%m%d")

# Descarga el archivo ZIP
r = requests.get(url)
with open('file.zip', 'wb') as f:
    f.write(r.content)

# Crea un directorio con la fecha extraída
os.makedirs(date_string, exist_ok=True)

# Extrae el archivo ZIP al directorio creado
with zipfile.ZipFile('file.zip', 'r') as zip_ref:
    zip_ref.extractall(date_string)

# Convierte todos los archivos .txt extraídos a .csv
for filename in os.listdir(date_string):
    if filename.endswith('.txt'):
        with open(f'{date_string}/{filename}', 'r') as in_file:
            stripped = (line.strip() for line in in_file)
            lines = (line.split(",") for line in stripped if line)
            with open(f'{date_string}/{filename[:-4]}.csv', 'w') as out_file:
                writer = csv.writer(out_file)
                writer.writerows(lines)
        os.remove(f'{date_string}/{filename}')  # Remove the .txt file after conversion

# Sube todos los archivos .csv a un bucket de Google Cloud Storage
storage_client = storage.Client()
bucket = storage_client.bucket('informacion-gob')

for filename in os.listdir(date_string):
    if filename.endswith('.csv'):
        blob = bucket.blob(f'{date_string}/{filename}')
        blob.upload_from_filename(f'{date_string}/{filename}')