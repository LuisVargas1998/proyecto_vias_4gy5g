import os
import cv2

# Función que calcula la cantidad de frames de un video, pide sólo la ruta del video, no el nombre del video
def calcular_frames(VIDEO_COMPLETO_PATH):
    nombre_video_completo = next(archivo for archivo in os.listdir(VIDEO_COMPLETO_PATH) if archivo.endswith(('.mp4', '.mov')))
    ruta_video_completo = os.path.join(VIDEO_COMPLETO_PATH, nombre_video_completo)
    video_completo = cv2.VideoCapture(ruta_video_completo)
    cantidad_frames_video_completo = int(video_completo.get(cv2.CAP_PROP_FRAME_COUNT))
    video_completo.release()
    return cantidad_frames_video_completo