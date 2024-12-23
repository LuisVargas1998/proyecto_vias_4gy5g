from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
import os


# Función para autenticar y crear el cliente de Google Drive usando una cuenta de servicio
def authenticate_drive(credenciales_drive):
    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = credenciales_drive
    
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    
    return build('drive', 'v3', credentials=credentials)

# Función para listar archivos en una carpeta de Google Drive
def list_files_in_folder(service, folder_id):
    query = f"'{folder_id}' in parents"
    results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print(f"{item['name']} (ID: {item['id']})")
    return items


# Función para descargar un archivo por su nombre de Google Drive
def download_file(service, file_name, folder_id, output_path):
    files = list_files_in_folder(service, folder_id)
    file_id = None
    for file in files:
        if file['name'] == file_name:
            file_id = file['id']
            break
    if not file_id:
        #print(f"File '{file_name}' not found in folder.")
        return
    request = service.files().get_media(fileId=file_id)
    full_output_path = os.path.join(output_path, file_name)
    with open(full_output_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            #print("Download %d%%." % int(status.progress() * 100))

# Función para subir un archivo a Google Drive
def upload_file(service, file_path, folder_id=None):
    file_metadata = {'name': os.path.basename(file_path)}
    if folder_id:
        file_metadata['parents'] = [folder_id]
    media = MediaFileUpload(file_path, resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    #print("File ID: %s" % file.get('id'))
    return file.get('id')

# Función para compartir un archivo en Google Drive
def share_file(service, file_id, user_email):
    user_permission = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': user_email
    }
    service.permissions().create(fileId=file_id, body=user_permission, fields='id').execute()
    #print("File shared with %s" % user_email)

# Función para crear una carpeta en Google Drive
def create_folder(service, folder_name, parent_folder_id):
    """
    Crea una carpeta en Google Drive.

    :param credenciales_drive: Ruta al archivo de credenciales de Google Drive.
    :param folder_name: Nombre de la nueva carpeta.
    :param parent_folder_id: ID de la carpeta padre (opcional).
    :return: ID de la nueva carpeta creada.
    """
    
    # Crear los metadatos de la carpeta
    folder_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    
    # Si se proporciona un ID de carpeta padre, agregarlo a los metadatos
    if parent_folder_id:
        if not isinstance(parent_folder_id, str):
            raise ValueError("El ID de la carpeta padre no es válido.")
        folder_metadata['parents'] = [parent_folder_id]
    
    # Crear la carpeta en Google Drive
    folder = service.files().create(body=folder_metadata, fields='id').execute()
    
    # Retornar el ID de la nueva carpeta creada
    return folder.get('id')

# Encontrar el ID de la carpeta con el nombre extraído dentro de una carpeta principal
def get_folder_id(service, parent_folder_id, folder_name):
    query = f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and name = '{folder_name}'"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    items = results.get('files', [])
    if not items:
        raise FileNotFoundError(f"No se encontró la carpeta: {folder_name}")
    return items[0]['id']

# Función para listar archivos y carpetas en una carpeta de Google Drive
def list_files_and_folders(service, folder_id):
    files = list_files_in_folder(service, folder_id)
    for file in files:
        file_id = file['id']
        file_name = file['name']
        #print(f"{file_name} (ID: {file_id})")
    return files

# Función para descargar todo el contenido de una carpeta de Google Drive
def download_all_of_a_folder(service, folder_id, output_dir):
    
    files = list_files_and_folders(service, folder_id)

    for file in files:
        file_id = file['id']
        file_name = file['name']
        file_mime_type = file.get('mimeType', '')

        # Eliminar el contenido entre paréntesis y el paréntesis
        clean_name = file_name.split(' (ID:')[0]

        if '.' not in clean_name:
            # Es una carpeta, recrearla en el directorio de salida
            new_folder_path = os.path.join(output_dir, clean_name)
            os.makedirs(new_folder_path, exist_ok=True)
            # Llamar recursivamente para descargar el contenido de la carpeta
            download_all_of_a_folder(service, file_id, new_folder_path)
        else:
            # Es un archivo, descargarlo
            file_path = os.path.join(output_dir, clean_name)
            
            if file_mime_type.startswith('application/vnd.google-apps'):
                # Es un archivo de Google Docs, Sheets, Slides, etc.
                if file_mime_type == 'application/vnd.google-apps.document':
                    request = service.files().export_media(fileId=file_id, mimeType='application/pdf')
                    file_path = os.path.join(output_dir, clean_name + '.pdf')
                elif file_mime_type == 'application/vnd.google-apps.spreadsheet':
                    request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                    file_path = os.path.join(output_dir, clean_name + '.xlsx')
                elif file_mime_type == 'application/vnd.google-apps.presentation':
                    request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.presentationml.presentation')
                    file_path = os.path.join(output_dir, clean_name + '.pptx')
                else:
                    #print(f"No se puede descargar el archivo {clean_name} de tipo {file_mime_type}")
                    continue
            else:
                # Es un archivo binario
                request = service.files().get_media(fileId=file_id)
            
            with open(file_path, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    try:
                        status, done = downloader.next_chunk()
                        #print(f"Descargando {clean_name}: {int(status.progress() * 100)}%")
                    except HttpError as error:
                        #print(f"Error al descargar {clean_name}: {error}")
                        break

# Obtiene el ID de un archivo por su nombre y extensiones en una carpeta específica
def get_file_id(service, folder_id, file_name, extensions):
    """
    Busca un archivo en Google Drive por nombre y extensiones en una carpeta específica.
    """
    query = f"'{folder_id}' in parents and name contains '{file_name}'"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    
    for file in items:
        for ext in extensions:
            if file['name'] == f"{file_name}.{ext}":
                return file['id']
    return None