import os
import shutil

def eliminar_todo_dentro(ruta_carpeta):
    """
    Elimina todos los archivos y carpetas dentro de la carpeta especificada.
    
    :param ruta_carpeta: Ruta de la carpeta cuyo contenido se desea eliminar.
    """
    for elemento in os.listdir(ruta_carpeta):
        elemento_path = os.path.join(ruta_carpeta, elemento)
        if os.path.isfile(elemento_path) or os.path.islink(elemento_path):
            os.remove(elemento_path)
        elif os.path.isdir(elemento_path):
            shutil.rmtree(elemento_path)

def eliminar_archivo(ruta_carpeta, nombre_archivo):
    """
    Elimina un archivo específico dentro de una carpeta.
    
    :param ruta_carpeta: Ruta de la carpeta donde se encuentra el archivo.
    :param nombre_archivo: Nombre del archivo a eliminar.
    """
    archivo_path = os.path.join(ruta_carpeta, nombre_archivo)
    if os.path.exists(archivo_path) and os.path.isfile(archivo_path):
        os.remove(archivo_path)

def eliminar_carpeta(ruta_carpeta):
    """
    Elimina una carpeta y todo su contenido.
    
    :param ruta_carpeta: Ruta de la carpeta a eliminar.
    """
    if os.path.exists(ruta_carpeta) and os.path.isdir(ruta_carpeta):
        shutil.rmtree(ruta_carpeta)