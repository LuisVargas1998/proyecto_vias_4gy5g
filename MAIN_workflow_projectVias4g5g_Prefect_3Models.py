import httpx
from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from typing import Optional, Dict, Any
import os


#Puertos de interés
# prefect: http://127.0.0.1:4200
#  dask: http://localhost:8787/status

# Encencender Prefect
# prefect server start


# ENTRADAS NECESARIAS A DEFINIR
# Google Drive
#service_drive = r'C:\Users\elaut\OneDrive\Escritorio\Pasantia\codigo_app\credential-vargasluis.igg1@gmail.com.json'
CREDENCIALES_DRIVE = r'C:\Users\elaut\OneDrive\Escritorio\Proyectos\codigo_base\googleDrive\credential-vargasluis.igg@gmail.com.json'
USER_MAIL = "levc.proyectovias4gy5g@gmail.com"
DRIVE_FOLDER_ID_VIDEOS_COMPLETOS = "1B-gPwm73w89TLogeLfkC3OvGZBvpeG3E"

# Variables generales
CREDENCIALES_MONGO = "mongodb+srv://proyectovias4gy5g:iPUrjIMWii2tfNJJ@cluster0.1npjd.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
BASE_DE_DATOS_NAME = "proyecto-vias-4gy5g"
COLECCION_REGISTRO_NAME = "registro_videos"
BASE_DE_DATOS_GPS = "gps_routes"



@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def busqueda_de_archivos_a_procesar(service_drive):
    import google_drive_request
    import mongodb_atlas_CRUD
    
    """
    Revisa los documentos de una colección y procesa el primero que no ha sido procesado.
    """
    df = mongodb_atlas_CRUD.leer_documentos_de_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_REGISTRO_NAME)
    
    for index, row in df.iterrows():
        if row['procesado'] == False:
            ruta_name = row['ruta']
            print(f"Se encontró video {ruta_name} como no procesado, por lo tanto se procederá a procesarlo.")
            # Buscar el archivo en Google Drive
            extension_video = ["mov", "mp4"]
            video_id = google_drive_request.get_file_id(service_drive, DRIVE_FOLDER_ID_VIDEOS_COMPLETOS, ruta_name, extension_video)
            # Obtener el ID del archivo
            extension_archivo = ["json", "geojson", "gpx"]
            archivo_id = google_drive_request.get_file_id(service_drive, DRIVE_FOLDER_ID_VIDEOS_COMPLETOS, ruta_name, extension_archivo)
            if video_id:
                # Actualizar la base de datos con el ID del video y el archivo en Drive
                mongodb_atlas_CRUD.editar_documento_de_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_REGISTRO_NAME, row['_id'], {'id_almacenamiento_video': video_id})
                mongodb_atlas_CRUD.editar_documento_de_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_REGISTRO_NAME, row['_id'], {'id_almacenamiento_archivo': archivo_id})
                #print(f"ID de almacenamiento actualizado en la base de datos: {video_id}")

                return ruta_name
            else:
                print(f"No se encontró el video {ruta_name} en Google Drive.")
    return None



@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def descarga_archivos(service_drive,ruta_name:str):

    """
    Busca archivos de video y GPS en una carpeta de Google Drive y los descarga.
    """

    import google_drive_request

    # DEFINICIONES
    output_video_path = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\1.1.video_completo_Import"
    output_gps_path = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\1.2.archivo_video_completo_Import"


    # Listar archivos en la carpeta
    items = google_drive_request.list_files_in_folder(service_drive, DRIVE_FOLDER_ID_VIDEOS_COMPLETOS)

    # Inicializar variables para los nombres de los archivos
    video_name = None
    gps_name = None

    # Extensiones posibles para video y GPS
    video_extensions = ["mov", "mp4"]
    gps_extensions = ["gpx", "geojson", "json"]

    # Buscar archivos con las extensiones especificadas
    for item in items:
        for ext in video_extensions:
            if item['name'] == f"{ruta_name}.{ext}":
                video_name = item['name']
                video_id = item['id']
                break
        for ext in gps_extensions:
            if item['name'] == f"{ruta_name}.{ext}":
                gps_name = item['name']
                gps_id = item['id']
                break

    # Descargar archivos si se encontraron
    if video_name:
        google_drive_request.download_file(service_drive, video_name, DRIVE_FOLDER_ID_VIDEOS_COMPLETOS, output_video_path)
    else:
        print(f"No se encontró el archivo de video para {ruta_name} con las extensiones {video_extensions}")
    
    if gps_name:
        google_drive_request.download_file(service_drive, gps_name, DRIVE_FOLDER_ID_VIDEOS_COMPLETOS, output_gps_path)
    else:
        print(f"No se encontró el archivo de GPS para {ruta_name} con las extensiones {gps_extensions}")

    return video_name, gps_name

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def carga_archivo_gps_a_mongo(gps_name):
    
    import mongodb_atlas_CRUD

    # DEFINICIONES
    input_dir = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\1.2.archivo_video_completo_Import"


    coleccion_gps = os.path.splitext(gps_name)[0]
    gps_file_path = os.path.join(input_dir, gps_name)

    mongodb_atlas_CRUD.crear_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_GPS, coleccion_gps)
    mongodb_atlas_CRUD.subir_documento_geojson(CREDENCIALES_MONGO, BASE_DE_DATOS_GPS, coleccion_gps, gps_file_path)

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def video_completo_a_trozos():

    import fragmentacion_de_video
    import cantidad_de_frames_de_video

    # DEFINICIONES
    input_dir = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\1.1.video_completo_Import"
    output_dir = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\3.video_trozos_Export"
    trozos = 10 #cantidad de trozos a cortar el video

    # Listar todos los archivos de video en el directorio de entrada
    video_files = [f for f in os.listdir(input_dir) if f.endswith(('.mp4', '.mov'))]

    # Calcular la cantidad de frames del video completo
    video_completo_frames = cantidad_de_frames_de_video.calcular_frames(input_dir)

    # Procesar cada archivo de video
    for video_file in video_files:
        video_path = os.path.join(input_dir, video_file)
        fragmentacion_de_video.fragmentar_video(video_path, output_dir, trozos)
        # Eliminar el archivo de video original
        os.remove(video_path)

    return video_completo_frames   


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def carga_de_trozos_a_Drive(service_drive,video_name:str):

    import google_drive_request

    # DEFINICIONES
    input_dir = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\3.video_trozos_Export"
    parent_folder_id = '1sZzR0kGqGbJxf38-3scKhd8tTzz2YVJd' #Carpeta videos_cortados en Google Drive


    # Obtener el nombre de la carpeta quitando la extensión del archivo
    folder_name = os.path.splitext(video_name)[0]
    # Crear una carpeta en Google Drive
    folder_id = google_drive_request.create_folder(service_drive, folder_name, parent_folder_id)

    # Listar todos los archivos de video en el directorio de entrada
    video_files = [f for f in os.listdir(input_dir) if f.endswith(('.mp4', '.mov'))]

    # Carga cada archivo de video en Google Drive 
    for video_file in video_files:
        file_path = os.path.join(input_dir, video_file)
        # Subir el archivo a la carpeta creada
        file_id = google_drive_request.upload_file(service_drive, file_path, folder_id)  # Captura el ID del archivo subido
        # Compartir el archivo
        google_drive_request.share_file(service_drive, file_id, USER_MAIL)
        # Eliminar el archivo de video ya subido
        os.remove(file_path)

    aviso = f"{id}. Voy en la carga de cada video en Google Drive."
    return aviso



@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def descarga_trozos(service_drive,video_name:str):

    # DEFINICIONES

    parent_folder_id = '1sZzR0kGqGbJxf38-3scKhd8tTzz2YVJd' #Carpeta videos_cortados en Google Drive'
    output_dir = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\4.video_trozos_Import"

    import google_drive_request 

    # Obtener el nombre de la carpeta quitando la extensión del archivo
    folder_name = os.path.splitext(video_name)[0]

    folder_id = google_drive_request.get_folder_id(service_drive, parent_folder_id, folder_name)

    files = google_drive_request.list_files_in_folder(service_drive, folder_id)

    # Descargar cada archivo a la ruta de salida especificada
    for video_trozo in files:
        print(f"Descargando {video_trozo}...")
        google_drive_request.download_file(service_drive, video_trozo["name"], folder_id, output_dir)




@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def descarga_de_pesos(service_drive):

    import google_drive_request

    
    #DEFINICION DE VARIABLES
    folder_id = '17C5bw_mxXLPJvFDZRa7z54q_uvBhZ72V' # Carpeta con los pesos de los modelos
    file_names = ["sh_weigths.pth", "sv_weigths.pth", "dv_weigths.pth"]
    output_path = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\5.pesos"


    for file_name in file_names:
        file_path = os.path.join(output_path, file_name)
        if not os.path.exists(file_path):
            resultado = google_drive_request.download_file(service_drive, file_name, folder_id, output_path)
            print(f"El resultado de la operación para {file_name} es: {resultado}")
        else:
            print(f"El archivo {file_name} ya existe en {output_path}, no se descargará nuevamente.")


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def proc_de_videos_con_yolonas_3modelos(video_completo_frames:int):

    # PROCESA UN VIDEO CON LOS TRES MODELOS
    #import procesamiento_con_yolonas_dask
    import procesamiento_con_yolonas
    import funciones_para_eliminar

    # Definición de datos del YOLO NAS
    TROZOS_VIDEOS_PATH = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\4.video_trozos_Import"  # Guarda en TROZOS_VIDEO_PATH la ruta de los trozos del video
    GUARDADO_JSONS = True  # Cambia esto según tus necesidades
    GUARDADO_JSONS_DIR = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\6.jsons_detections_Export"
    MODEL = "yolo_nas_s"  # Se define el nombre del modelo
    SV_CHECKPOINT_DIR = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\5.pesos\sv_weigths.pth"
    SH_CHECKPOINT_DIR = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\5.pesos\sh_weigths.pth"
    DV_CHECKPOINT_DIR = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\5.pesos\dv_weigths.pth"
    WORKERS_DASK = 1  # Número de trabajadores Dask



    # Procesar secuencialmente
    procesamiento_con_yolonas.process(video_completo_frames,TROZOS_VIDEOS_PATH,
                                                            GUARDADO_JSONS,GUARDADO_JSONS_DIR, MODEL,
                                SV_CHECKPOINT_DIR,SH_CHECKPOINT_DIR,DV_CHECKPOINT_DIR,WORKERS_DASK)
    
    # Descomentar si se quiere proecesar con DASK en paralelo
    #procesamiento_con_yolonas_dask.process(video_completo_frames,TROZOS_VIDEOS_PATH,
    #                                                        GUARDADO_JSONS,GUARDADO_JSONS_DIR, MODEL,
    #                           SV_CHECKPOINT_DIR,SH_CHECKPOINT_DIR,DV_CHECKPOINT_DIR,WORKERS_DASK)


    # Eliminar los archivos los trozos de video
    funciones_para_eliminar.eliminar_todo_dentro(TROZOS_VIDEOS_PATH)

    # Obtener solo la carpeta sin los archivos
    carpeta_pesos = os.path.dirname(SV_CHECKPOINT_DIR)
    #Elimina los pesos
    funciones_para_eliminar.eliminar_todo_dentro(carpeta_pesos)



@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def rastreo_jsons_con_bytracker():
    import rastreo_bytetracker
    import funciones_para_eliminar


    # DEFINICIÓN
    GUARDADO_JSONS = True # Cambia esto según tus necesidades respondiendo a la pregunta, ¿Desea guardas archivos Json? "True"=SI, "False"=NO
    JSONS_DETECTIONS_DIR= r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\6.jsons_detections_Export"
    JSONS_RASTREOS_DIR= r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\7.jsons_tracking_Export"
    
    rastreo_bytetracker.rastrear_jsons_detections(GUARDADO_JSONS,JSONS_DETECTIONS_DIR,JSONS_RASTREOS_DIR)

    # Llamar a la función para eliminar los archivos .json después de que el proceso haya terminado
    funciones_para_eliminar.eliminar_todo_dentro(JSONS_DETECTIONS_DIR)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def carga_jsons_a_Drive(service_drive, video_name:str):

    import google_drive_request

    # DEFINICIÓN
    input_dir = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\7.jsons_tracking_Export"
    parent_folder_id = '1yO9n_WDAJoTj22w762VQRqfv1UfT9xE0' # Carpeta jsons_tracking en Google Drive

    # Obtener el nombre de la carpeta quitando la extensión del archivo
    folder_name = os.path.splitext(video_name)[0]
    # Crear una carpeta en Google Drive
    folder_id = google_drive_request.create_folder(service_drive, folder_name, parent_folder_id)

    # Listar todos los archivos JSON en el directorio de entrada
    json_files = [f for f in os.listdir(input_dir) if f.endswith(('.json'))]

    # Carga cada archivo de video en Google Drive
    for json_file in json_files:
        file_path = os.path.join(input_dir, json_file)
        # Subir el archivo a la carpeta creada
        file_id = google_drive_request.upload_file(service_drive, file_path, folder_id)  # Captura el ID del archivo subido
        # Compartir el archivo
        google_drive_request.share_file(service_drive, file_id, USER_MAIL)
        # Eliminar el archivo de video ya subido
        os.remove(file_path)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def descarga_jsons_de_Drive(service_drive, video_name:str):

    import google_drive_request 

    #DEFINICION
    parent_folder_id = '1yO9n_WDAJoTj22w762VQRqfv1UfT9xE0'
    output_dir = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\9.jsons_tracking_Import"


    # Obtener el nombre de la carpeta quitando la extensión del archivo
    folder_name = os.path.splitext(video_name)[0]

    folder_id = google_drive_request.get_folder_id(service_drive, parent_folder_id, folder_name)

    files = google_drive_request.list_files_in_folder(service_drive, folder_id)

    # Descargar cada archivo a la ruta de salida especificada
    for file in files:
        google_drive_request.download_file(service_drive, file['name'], folder_id, output_dir)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def conv_jsons_a_parquet():

    import jsons2parquets
    import funciones_para_eliminar

    # DEFINICIONES
    JSONS_RASTREOS_DIR = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\9.jsons_tracking_Import"
    PARQUETS_DIR = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\11.parquet_Export"


    # Convertir los archivos JSON a archivos PARQUET
    jsons2parquets.convertir(JSONS_RASTREOS_DIR, PARQUETS_DIR)

    # Elimina los JSON después de convertirlos a PARQUET
    funciones_para_eliminar.eliminar_todo_dentro(JSONS_RASTREOS_DIR)

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def carga_parquet_a_Drive(service_drive, video_name:str):

    import google_drive_request

    # DEFINICIONES
    input_dir = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\11.parquet_Export"
    parent_folder_id = '1O9veyw1zS09kHjinfeuOECMwu1T1Eb1I'


    # Obtener el nombre de la carpeta quitando la extensión del archivo de video
    folder_name = os.path.splitext(video_name)[0]
    # Crear una carpeta en Google Drive
    folder_id = google_drive_request.create_folder(service_drive, folder_name, parent_folder_id)
    print(f"Carpeta principal creada: {folder_name} con ID {folder_id}")

    # Función para subir archivos y carpetas recursivamente
    def upload_files_and_folders(service, local_dir, parent_id):
        for item in os.listdir(local_dir):
            item_path = os.path.join(local_dir, item)
            if os.path.isdir(item_path):
                # Crear una carpeta en Google Drive
                subfolder_id = google_drive_request.create_folder(service, item, parent_id)
                print(f"Carpeta creada: {item} con ID {subfolder_id}")
                # Subir el contenido de la carpeta recursivamente
                upload_files_and_folders(service, item_path, subfolder_id)
                # Eliminar la carpeta vacía
                try:
                    os.rmdir(item_path)
                    print(f"Carpeta {item_path} eliminada.")
                except OSError as e:
                    print(f"No se pudo eliminar la carpeta {item_path}: {e}")
            else:
                # Subir el archivo a la carpeta creada
                file_id = google_drive_request.upload_file(service, item_path, parent_id)  # Captura el ID del archivo subido
                # Compartir el archivo
                google_drive_request.share_file(service, file_id, USER_MAIL)
                # Eliminar el archivo ya subido
                os.remove(item_path)
                print(f"Archivo {item} subido y eliminado localmente.")

    # Llamar a la función para subir archivos y carpetas
    upload_files_and_folders(service_drive, input_dir, folder_id)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def descarga_parquets(service_drive, video_name:str):

    import google_drive_request 

    # DEFINICIONES
    parent_folder_id = '1O9veyw1zS09kHjinfeuOECMwu1T1Eb1I'
    output_dir = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\12.parquet_Import"


    # Obtener el nombre de la carpeta quitando la extensión del archivo
    folder_name = os.path.splitext(video_name)[0]

    folder_id = google_drive_request.get_folder_id(service_drive, parent_folder_id, folder_name)

    google_drive_request.download_all_of_a_folder(service_drive, folder_id, output_dir)  


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def parquet_a_mongodb(video_name):
    import mongodb_atlas_CRUD

    # DEFINICIONES
    ruta_parquet = r"C:\Users\elaut\OneDrive\Escritorio\Pasantia\arenaPrefect\12.parquet_Import"
    BASE_DE_DATOS_DETECCIONES = "data_detections"


    # Obtener el nombre de la carpeta quitando la extensión del archivo
    coleccion_name = os.path.splitext(video_name)[0]

    # Crear la colección en MongoDB
    mongodb_atlas_CRUD.crear_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_DETECCIONES, coleccion_name)

    # Subir los archivos PARQUET a MongoDB
    mongodb_atlas_CRUD.subir_documentos_parquet(CREDENCIALES_MONGO,BASE_DE_DATOS_DETECCIONES, coleccion_name, ruta_parquet)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def actualiza_estado_procesamiento_mongo():
    import mongodb_atlas_CRUD

    """
    Revisa los documentos de una colección y actualiza el ya procesado.
    """
    df = mongodb_atlas_CRUD.leer_documentos_de_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_REGISTRO_NAME)

    # Crear un iterador explícito sobre el DataFrame
    iterador = df.iterrows()

    while True:
        try:
            index, row = next(iterador)
            if row['procesado'] == False:
                # Actualizar la base de datos con el ID del video y el archivo en Drive
                mongodb_atlas_CRUD.editar_documento_de_coleccion(CREDENCIALES_MONGO, BASE_DE_DATOS_NAME, COLECCION_REGISTRO_NAME, row['_id'], {'procesado': True})
                
        except StopIteration:
            # Si se llega al final del iterador, salir del bucle
            break



@flow(retries=3, retry_delay_seconds=5, log_prints=True)
def prefect_proyecto_vias_4gy5g_flujo(repo_name: str = "PrefectHQ/prefect",):

    import google_drive_request
    service_drive = google_drive_request.authenticate_drive(CREDENCIALES_DRIVE)
    
    ruta_name = busqueda_de_archivos_a_procesar(service_drive)

    video_name, gps_name = descarga_archivos(service_drive,ruta_name)

    carga_archivo_gps_a_mongo(gps_name)

    video_completo_frames = video_completo_a_trozos()

    carga_de_trozos_a_Drive(service_drive, video_name)

    descarga_trozos(service_drive,video_name)

    descarga_de_pesos(service_drive)

    proc_de_videos_con_yolonas_3modelos(video_completo_frames)

    rastreo_jsons_con_bytracker()

    carga_jsons_a_Drive(service_drive,video_name)

    descarga_jsons_de_Drive(service_drive,video_name)

    conv_jsons_a_parquet()

    carga_parquet_a_Drive(service_drive,video_name)

    descarga_parquets(service_drive,video_name)

    parquet_a_mongodb(video_name)

    actualiza_estado_procesamiento_mongo()


    print(f"terminé el proceso 🤓:")



if __name__ == "__main__":
    prefect_proyecto_vias_4gy5g_flujo()