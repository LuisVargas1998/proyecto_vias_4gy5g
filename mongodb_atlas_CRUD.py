
import os
import pandas as pd
from pymongo import MongoClient
import geopandas as gpd
import json
from bson.objectid import ObjectId


import pandas as pd
from pymongo import MongoClient

def conectar_mongo(CREDENCIALES_MONGO):
    """
    Conecta a MongoDB Atlas y devuelve el cliente.
    """
    client = MongoClient(CREDENCIALES_MONGO)
    return client

def subir_documentos_parquet(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_NAME, RUTA_PARQUET):
    """
    Sube documentos a una colección en MongoDB desde un archivo Parquet.
    """
    client = conectar_mongo(CREDENCIALES_MONGO)
    db = client[BASE_DE_DATOS_NAME]
    collection = db[COLECCION_NAME]
    df = pd.read_parquet(RUTA_PARQUET)
    data = df.to_dict('records')
    collection.insert_many(data)
    print(f"Datos subidos a la colección {COLECCION_NAME}")

def subir_documentos_json(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_NAME, RUTA_JSON):
    """
    Sube documentos a una colección en MongoDB desde un archivo JSON.
    """
    client = conectar_mongo(CREDENCIALES_MONGO)
    db = client[BASE_DE_DATOS_NAME]
    collection = db[COLECCION_NAME]
    df = pd.read_json(RUTA_JSON)
    data = df.to_dict('records')
    collection.insert_many(data)
    print(f"Datos subidos a la colección {COLECCION_NAME}")

def subir_documento_geojson(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_NAME, RUTA_GEOJSON):
    """
    Sube documento geojson a una colección en MongoDB
    """
    # Configuración de conexión a MongoDB
    client = MongoClient(CREDENCIALES_MONGO)  # Cambia la URL según tu configuración
    db = client[BASE_DE_DATOS_NAME]  # Nombre de tu base de datos
    collection = db[COLECCION_NAME]  # Nombre de tu colección

    # Cargar el archivo GeoJSON
    ruta_geojson = RUTA_GEOJSON  # Reemplaza con la ruta a tu archivo GeoJSON

    with open(ruta_geojson, "r", encoding="utf-8") as archivo:
        geojson_data = json.load(archivo)

    # Insertar el archivo GeoJSON completo como un solo documento
    collection.insert_one(geojson_data)
    #print("GeoJSON cargado con éxito en MongoDB.")

    # Crear un índice geoespacial en caso de que sea necesario
    collection.create_index([("geometry", "2dsphere")])


def leer_documentos_de_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_NAME):
    """
    Lee todos los documentos de una colección en MongoDB y los devuelve como un DataFrame de pandas.
    """
    client = conectar_mongo(CREDENCIALES_MONGO)
    db = client[BASE_DE_DATOS_NAME]
    collection = db[COLECCION_NAME]
    data = list(collection.find())
    df = pd.DataFrame(data)
    print(f"Datos leídos de la colección {COLECCION_NAME}")
    return df

def leer_documento_de_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_NAME, DOCUMENT_ID):
    """
    Lee un documento específico de una colección en MongoDB.
    """
    client = conectar_mongo(CREDENCIALES_MONGO)
    db = client[BASE_DE_DATOS_NAME]
    collection = db[COLECCION_NAME]
    document = collection.find_one({"_id": DOCUMENT_ID})
    df = pd.DataFrame(document)
    print(f"Documento leído de la colección {COLECCION_NAME}")
    return df

def editar_documento_de_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_NAME, DOCUMENT_ID, new_values):
    """
    Edita un documento específico en una colección en MongoDB.
    """
    client = conectar_mongo(CREDENCIALES_MONGO)
    db = client[BASE_DE_DATOS_NAME]
    collection = db[COLECCION_NAME]
    collection.update_one({"_id": DOCUMENT_ID}, {'$set': new_values})
    print(f"Documento actualizado en la colección {COLECCION_NAME}")

def eliminar_documento_de_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_NAME, DOCUMENT_ID):
    """
    Elimina un documento específico de una colección en MongoDB.
    """
    client = conectar_mongo(CREDENCIALES_MONGO)
    db = client[BASE_DE_DATOS_NAME]
    collection = db[COLECCION_NAME]
    collection.delete_one({"_id": DOCUMENT_ID})
    print(f"Documento eliminado de la colección {COLECCION_NAME}")

def eliminar_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_NAME):
    """
    Elimina una colección completa de una base de datos en MongoDB.
    """
    client = conectar_mongo(CREDENCIALES_MONGO)
    db = client[BASE_DE_DATOS_NAME]
    collection = db[COLECCION_NAME]
    collection.drop()
    print(f"Colección {COLECCION_NAME} eliminada")

def crear_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_NAME):
    """
    Crea una colección en una base de datos en MongoDB.
    """
    client = conectar_mongo(CREDENCIALES_MONGO)
    db = client[BASE_DE_DATOS_NAME]
    db.create_collection(COLECCION_NAME)
    print(f"Colección {COLECCION_NAME} creada en la base de datos {BASE_DE_DATOS_NAME}")

def contar_documentos(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_NAME):
    """
    Cuenta el número de documentos en una colección en MongoDB.
    """
    client = conectar_mongo(CREDENCIALES_MONGO)
    db = client[BASE_DE_DATOS_NAME]
    collection = db[COLECCION_NAME]
    count = collection.count_documents({})
    print(f"Total de documentos en la colección {COLECCION_NAME}: {count}")
    return count

def contar_colecciones(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME):
    """
    Cuenta el número de colecciones en una base de datos en MongoDB.
    """
    client = conectar_mongo(CREDENCIALES_MONGO)
    db = client[BASE_DE_DATOS_NAME]
    collections = db.list_collection_names()
    count = len(collections)
    print(f"Total de colecciones en la base de datos {BASE_DE_DATOS_NAME}: {count}")
    return count

def listar_colecciones(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME):
    """
    Lista todas las colecciones en una base de datos en MongoDB.
    """
    client = conectar_mongo(CREDENCIALES_MONGO)
    db = client[BASE_DE_DATOS_NAME]
    collections = db.list_collection_names()
    print(f"Colecciones en la base de datos {BASE_DE_DATOS_NAME}:")
    for collection in collections:
        print(collection)
    return collections

def listar_documentos(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_NAME):
    """
    Lista todos los documentos de una colección en MongoDB.
    """
    client = conectar_mongo(CREDENCIALES_MONGO)
    db = client[BASE_DE_DATOS_NAME]
    collection = db[COLECCION_NAME]
    documents = list(collection.find())
    print(f"Documentos en la colección {COLECCION_NAME}:")
    for document in documents:
        print(document)
    return documents

