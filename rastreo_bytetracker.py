import os
import yolox
import supervision  # Importa el módulo supervision
import numpy as np
import json
from yolox.tracker.byte_tracker import BYTETracker, STrack  # Importa las clases BYTETracker y STrack del módulo yolox.tracker.byte_tracker
from onemetric.cv.utils.iou import box_iou_batch  # Importa la función box_iou_batch del módulo onemetric.cv.utils.iou
from dataclasses import dataclass  # Importa la clase dataclass del módulo dataclasses
from supervision.tools.detections import Detections # Importa las clases Detections y BoxAnnotator del módulo supervision.tools.detections
from typing import List


@dataclass(frozen=True)
class BYTETrackerArgs:
    track_thresh: float = 0.5  # Ajusta este valor según la confianza de tus detecciones
    track_buffer: int = 50  # Aumenta el tamaño del búfer para mantener rastreos durante más tiempo
    match_thresh: float = 0.9  # Aumenta el umbral de similitud para asociaciones más estrictas
    aspect_ratio_thresh: float = 2.0  # Ajusta según las características de tus objetos
    min_box_area: float = 10.0  # Aumenta el área mínima para eliminar detecciones de objetos muy pequeños
    mot20: bool = False  # Activa esta opción si trabajas con datos similares a MOT20
#... Utilidades de Seguimiento
# Convierte las detecciones en un formato que puede ser consumido por la función match_detections_with_tracks
def detections2boxes(detections: Detections) -> np.ndarray:
    # Concatena las coordenadas xyxy de las detecciones con la columna de confianza
    return np.hstack((
        detections.xyxy,  # Coordenadas xyxy de las detecciones
        detections.confidence[:, np.newaxis]  # Columna de confianza de las detecciones
    ))
# Convierte List[STrack] en un formato que puede ser consumido por la función match_detections_with_tracks
def tracks2boxes(tracks: List[STrack]) -> np.ndarray:
    # Crea una matriz de las regiones de interés (tlbr) de los rastreos
    return np.array([
        track.tlbr  # Región de interés (tlbr) del rastreo
        for track in tracks
    ], dtype=float)
# Hace coincidir nuestros cuadros delimitadores con predicciones
def match_detections_with_tracks(detections: Detections, tracks: List[STrack]) -> Detections:
    # Verifica si no hay detecciones o no hay rastreos
    if not np.any(detections.xyxy) or len(tracks) == 0:
        return np.empty((0,))
    # Convierte los rastreos en un formato compatible para comparación con detecciones
    tracks_boxes = tracks2boxes(tracks=tracks)
    # Calcula el IoU (intersección sobre unión) entre los cuadros delimitadores de los rastreos y las detecciones
    iou = box_iou_batch(tracks_boxes, detections.xyxy)
    # Determina la asociación de rastreo a detección basándose en el IoU máximo
    track2detection = np.argmax(iou, axis=1)
    # Genera una lista de IDs de rastreo correspondientes a las detecciones
    tracker_ids = [None] * len(detections)
    for tracker_index, detection_index in enumerate(track2detection):
        # Verifica si hay una asociación válida basada en el IoU máximo
        if iou[tracker_index, detection_index] != 0:
            tracker_ids[detection_index] = tracks[tracker_index].track_id
    return tracker_ids

def rastrear_jsons_detections(GUARDADO_JSONS,JSONS_DETECTIONS_DIR,JSONS_RASTREOS_DIR):

    # Crea una instancia de BYTETracker con los argumentos especificados en BYTETrackerArgs
    byte_tracker = BYTETracker(BYTETrackerArgs())
    # Obtener todos los archivos JSON y ordenarlos numéricamente
    json_files = sorted(os.listdir(JSONS_DETECTIONS_DIR), key=lambda x: int(x.split('.')[0]) if x.split('.')[0].isdigit() else float('inf'))
    # print(json_files)
    # Iterar sobre los archivos JSON
    for filename in json_files:
        if filename.endswith('.json'):
            #print(f"Procesando archivo: {filename}")
            # Cargar los datos del archivo JSON
            with open(os.path.join(JSONS_DETECTIONS_DIR, filename), 'r') as f:
                data = json.load(f)
            # Extraer los valores de archivo json y convertirlos en ndarrays
            class_id_s = np.array([d['class_id'] for d in data])
            confidence_s = np.array([d['confidence'] for d in data])
            xyxy_s = np.array([[float(d['detect_box']['x1']), float(d['detect_box']['y1']), float(d['detect_box']['x2']), float(d['detect_box']['y2'])] for d in data])
            model_s = np.array([d['model'] for d in data])
            class_name_s = np.array([d['class_name'] for d in data])
            frame_s = np.array([d['frame'] for d in data])
            img_info_s = np.array([d['img_info'] for d in data])[0]
            # Ahora puedes usar estos ndarrays en tu código
            detections = Detections(
                xyxy=xyxy_s,
                confidence=confidence_s,
                class_id=class_id_s,
            )
            # Actualiza los rastreos con las detecciones
            tracks = byte_tracker.update(
                output_results=detections2boxes(detections=detections),
                img_info=img_info_s,
                img_size=img_info_s
            )
            tracker_id = match_detections_with_tracks(detections=detections, tracks=tracks)
            detections.tracker_id = np.array(tracker_id)
            # Filtra las detecciones que no tienen rastreador asignado
            mask = np.array([tracker_id is not None for tracker_id in detections.tracker_id], dtype=bool)
            detections.filter(mask=mask, inplace=True)
            # Creamos listas para almacenar las coordenadas y variables de cada elemento en tracks
            center_x_list = []
            center_y_list = []
            aspect_ratio_list = []
            height_list = []
            # Iteramos sobre cada elemento en tracks
            for track in tracks:
                # Obtenemos las coordenadas (x, y) del centro del bounding box
                center_x, center_y, aspect_ratio, height = track.tlwh_to_xyah(track.tlwh)
                center_x_list.append(center_x)
                center_y_list.append(center_y)
                aspect_ratio_list.append(aspect_ratio)
                height_list.append(height)
            # Crear un diccionario para almacenar las detecciones
            detections_dict = []
            # Verificar si hay detecciones para este fotograma
            if len(detections) > 0:
                a = 0
                for detect in range(len(detections)):
                    # Obtener los valores de las detecciones y seguimiento
                    model = model_s[detect]
                    class_name = class_name_s[detect]
                    class_id = int(class_id_s[detect])
                    confidence = float(confidence_s[detect])
                    x1, y1, x2, y2 = xyxy_s[detect]
                    track_id = tracker_id[detect]
                    frame = frame_s[detect]
                    # Construir el diccionario de track_box
                    if track_id is not None:
                        track_box = {
                            "centerX": center_x_list[a],
                            "centerY": center_y_list[a],
                            "ratio": aspect_ratio_list[a],
                            "height": height_list[a]
                        }
                        a=a+1
                    else:
                        track_box = {
                            "centerX": None,
                            "centerY": None,
                            "ratio": None,
                            "height": None
                        }
                    # Construir el diccionario de detección
                    detection_dict = {
                        "frame": frame,
                        "model": model,
                        "class_name": class_name,
                        "class_id": class_id,
                        "confidence": confidence,
                        "detect_box": {
                            "x1": x1,
                            "y1": y1,
                            "x2": x2,
                            "y2": y2
                        },
                        "track_id": track_id,
                        "track_box": track_box,
                        "img_info": img_info_s
                    }
                    detections_dict.append(detection_dict) # Agregar la detección al diccionario de detecciones
            # Crear el nombre del archivo JSON basado en el número de fotograma
            if len(detections_dict) > 0:
                if GUARDADO_JSONS:
                    json_filename = f"{frame}.json"
                    # Guardar el diccionario de detecciones como archivo JSON
                    with open(f"{JSONS_RASTREOS_DIR}/{json_filename}", "w") as json_file:
                        json.dump(detections_dict, json_file,default=lambda x:str(x),indent=4)
