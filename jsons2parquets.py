import pandas as pd
import os
import json
from deltalake.writer import write_deltalake

# Ruta del directorio de los archivos JSON
JSONS_RASTREOS_DIR = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\9.jsons_tracking_Import"
PARQUETS_DIR = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\10.parquet_Export"

def convertir(JSONS_RASTREOS_DIR,PARQUETS_DIR):
# Lista para almacenar los registros de JSON
    records = []
    # Iterar sobre los archivos JSON en el directorio
    for json_file in os.listdir(JSONS_RASTREOS_DIR):
        if json_file.endswith('.json'):
            json_path = os.path.join(JSONS_RASTREOS_DIR, json_file)
            # Cargar el archivo JSON
            with open(json_path, 'r') as f:
                data = json.load(f)
            # Agregar los registros del JSON a la lista
            records.extend(data)
    # Crear un DataFrame a partir de la lista de registros
    df = pd.DataFrame(records)
    # Exporta el DataFrame de Pandas en un DeltaLake
    write_deltalake(PARQUETS_DIR, df)

