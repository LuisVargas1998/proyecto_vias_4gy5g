import os
import re
import cv2
from dask.distributed import Client
from dask import delayed
import dask
import numpy as np
import json
from supervision.draw.color import ColorPalette
from supervision.geometry.dataclasses import Point
from supervision.video.dataclasses import VideoInfo
from supervision.video.source import get_video_frames_generator
from supervision.tools.detections import Detections, BoxAnnotator
from supervision.tools.line_counter import LineCounter, LineCounterAnnotator
from super_gradients.training import models


def frame_processing(frame, i,frames_por_trozo,numero_trozo,
                    sv_model,SV_CLASSES_ID,SV_CLASSES_NAMES_DICT,
                    sh_model,SH_CLASSES_ID,SH_CLASSES_NAMES_DICT,
                    dv_model,DV_CLASSES_ID,DV_CLASSES_NAMES_DICT,
                    GUARDADO_JSONS,GUARDADO_JSONS_DIR):
    
    detections_dict = []

    # Predicción del modelo de Señales Verticales (SV)
    results_sv = list(sv_model.predict(frame, conf=0.75))[0]
    detections_sv = Detections(
        xyxy=results_sv.prediction.bboxes_xyxy,
        confidence=results_sv.prediction.confidence,
        class_id=results_sv.prediction.labels.astype(int),
    )
    mask_sv = np.array([class_id in SV_CLASSES_ID for class_id in detections_sv.class_id], dtype=bool)
    detections_sv.filter(mask=mask_sv, inplace=True)

    for detect in range(len(detections_sv)):
        class_name = SV_CLASSES_NAMES_DICT[detections_sv.class_id[detect]]
        class_id = int(detections_sv.class_id[detect])
        confidence = float(detections_sv.confidence[detect])
        x1, y1, x2, y2 = detections_sv.xyxy[detect]
        frame_real = (frames_por_trozo * (numero_trozo - 1)) + (i+1)
        detection_dict = {
            "frame": frame_real,
            "model": "SV",
            "class_name": class_name,
            "class_id": class_id,
            "confidence": confidence,
            "detect_box": {"x1": x1, "y1": y1, "x2": x2, "y2": y2},
            "img_info": frame.shape
        }
        detections_dict.append(detection_dict)


    # Predicción del modelo de Señales Horizontales (SH)
    results_sh = list(sh_model.predict(frame, conf=0.75))[0]
    detections_sh = Detections(
        xyxy=results_sh.prediction.bboxes_xyxy,
        confidence=results_sh.prediction.confidence,
        class_id=results_sh.prediction.labels.astype(int),
    )
    mask_sh = np.array([class_id in SH_CLASSES_ID for class_id in detections_sh.class_id], dtype=bool)
    detections_sh.filter(mask=mask_sh, inplace=True)
    for detect in range(len(detections_sh)):
        class_name = SH_CLASSES_NAMES_DICT[detections_sh.class_id[detect]]
        class_id = int(detections_sh.class_id[detect])
        confidence = float(detections_sh.confidence[detect])
        x1, y1, x2, y2 = detections_sh.xyxy[detect]
        frame_real = (frames_por_trozo * (numero_trozo - 1)) + (i+1)
        detection_dict = {
            "frame": frame_real,
            "model": "SH",
            "class_name": class_name,
            "class_id": class_id,
            "confidence": confidence,
            "detect_box": {"x1": x1, "y1": y1, "x2": x2, "y2": y2},
            "img_info": frame.shape
        }
        detections_dict.append(detection_dict)


    # Predicción del modelo de Deterioro Vial (DV)
    results_dv = list(dv_model.predict(frame, conf=0.75))[0]
    detections_dv = Detections(
        xyxy=results_dv.prediction.bboxes_xyxy,
        confidence=results_dv.prediction.confidence,
        class_id=results_dv.prediction.labels.astype(int),
    )
    mask_dv = np.array([class_id in DV_CLASSES_ID for class_id in detections_dv.class_id], dtype=bool)
    detections_dv.filter(mask=mask_dv, inplace=True)
    for detect in range(len(detections_dv)):
        class_name = DV_CLASSES_NAMES_DICT[detections_dv.class_id[detect]]
        class_id = int(detections_dv.class_id[detect])
        confidence = float(detections_dv.confidence[detect])
        x1, y1, x2, y2 = detections_dv.xyxy[detect]
        frame_real = (frames_por_trozo * (numero_trozo - 1)) + (i+1)
        detection_dict = {
            "frame": frame_real,
            "model": "DV",
            "class_name": class_name,
            "class_id": class_id,
            "confidence": confidence,
            "detect_box": {"x1": x1, "y1": y1, "x2": x2, "y2": y2},
            "img_info": frame.shape
            
        }
        detections_dict.append(detection_dict)


    # Guardar el diccionario de detecciones como archivo JSON
    if len(detections_dict) > 0 and GUARDADO_JSONS:
        json_filename = f"{frame_real}.json"
        with open(os.path.join(GUARDADO_JSONS_DIR, json_filename), "w") as json_file:
            json.dump(detections_dict, json_file, default=lambda x: str(x), indent=4)
    return


def video_processing(video,cantidad_frames_video_completo,cantidad_video_trozos,
                     sv_model,SV_CLASSES_ID,SV_CLASSES_NAMES_DICT,
                     sh_model,SH_CLASSES_ID,SH_CLASSES_NAMES_DICT,
                     dv_model,DV_CLASSES_ID,DV_CLASSES_NAMES_DICT,
                     GUARDADO_JSONS,GUARDADO_JSONS_DIR):
    # Extraer del nombre del video el número de trozo
    nombre_video = os.path.basename(video)# CAMBIAR POR LA VARIABLE VIDEO
    nombre_sin_extension = os.path.splitext(nombre_video)[0]
    numero_trozo = int(re.search(r'trozo(\d+)', nombre_sin_extension).group(1))
    # Calculo de la cantidad de frames por trozo
    frames_por_trozo = abs(cantidad_frames_video_completo // cantidad_video_trozos)
    # Configuración de video y anotaciones
    LINE_START = Point(50, 1500)
    LINE_END = Point(3840-50, 1500)
    video_info = VideoInfo.from_video_path(video)
    generator = get_video_frames_generator(video)
    line_counter = LineCounter(start=LINE_START, end=LINE_END)
    box_annotator = BoxAnnotator(color=ColorPalette(), thickness=4, text_thickness=4, text_scale=2)
    line_annotator = LineCounterAnnotator(thickness=4, text_thickness=4, text_scale=2)

    # Uso de Dask para procesar en paralelo
    # Crear tareas Dask para cada frame directamente desde el generador
    delayed_frames = []
    for i, frame in enumerate(generator):
        task = delayed(frame_processing)(frame, i, frames_por_trozo, numero_trozo,
                                         sv_model, SV_CLASSES_ID, SV_CLASSES_NAMES_DICT,
                                         sh_model, SH_CLASSES_ID, SH_CLASSES_NAMES_DICT,
                                         dv_model, DV_CLASSES_ID, DV_CLASSES_NAMES_DICT,
                                         GUARDADO_JSONS, GUARDADO_JSONS_DIR)
        delayed_frames.append(task)
    
    dask.compute(*delayed_frames)
    


def trozos_videos(ruta):
    archivos = os.listdir(ruta)
    videos = [archivo for archivo in archivos if archivo.endswith(('.mp4', '.mov'))]
    for video in videos:
        yield os.path.join(ruta, video)


def process(video_completo_frames,TROZOS_VIDEOS_PATH,GUARDADO_JSONS,GUARDADO_JSONS_DIR,
                                MODEL,SV_CHECKPOINT_DIR,SH_CHECKPOINT_DIR,DV_CHECKPOINT_DIR,WORKERS_DASK):

    # Crea un cliente Dask con 2 trabajadores
    client_Dask_proc_frames = Client(n_workers=WORKERS_DASK)

    # Diccionarios de clases
    SV_CLASSES_NAMES_DICT = [
        'Pare', 'Ceda_el_Paso', 'Siga_de_Frente', 'No_Pase', 'Giro_Solamente_a_la_Izquierda', 'Prohibido_Girar_a_la_Izquierda',
        'Giro_Solamente_a_la_Derecha', 'Prohibido_Girar_a_la_Derecha','Prohibido_Girar_en_U', 'Doble_Via', 'Tres_Carriles(uno_en_contraflujo)',
        'Tres_Carriles(dos_en_contraflujo)', 'Prohibido_el_Cambio_de_Calzada','Prohibido_Circulacion_de_Vehiculos_Automotores', 'Vehiculos_Pesados_a_la_Derecha',
        'Prohibido_Circulacion_Vehiculos_de_Carga', 'Peatones_a_la_Izquierda','Prohibido_Circulacion_de_Peatones', 'Prohibido_Circulacion_de_Cabalgaduras',
        'Prohibido_Circulacion_de_Bicicletas', 'Prohibido_Circulacion_de_Motocicletas', 'Prohibido_Circulacion_de_Maquinaria_Agricola',
        'Prohibido_Circulacion_de_Vehiculos_de_Traccion_Animal','Prohibido_Adelantar', 'Prohibido_Parquear', 'Prohibido_Parquear_o_Detenerse',
        'Prohibido_Pitar', 'Velocidad_Maxima', 'Peso_Maximo_Total_Permitido', 'Altura_Maxima_Permitida', 'Ancho_Maximo_Permitido',
        'Zona_de_Estacionamiento_de_Taxis', 'Circulacion_con_Luces_Bajas', 'Reten', 'Cicloruta', 'Sentido_Unico_de_Circulacion',
        'Sentido_de_Circulacion_Doble', 'Paradero', 'Prohibido_Dejar_o_Recoger_Pasajeros','Zona_de_Cargue_y_Descargue', 'Prohibido_Cargue_y_Descargue',
        'Espaciamiento','Indicacion_de_Separador_de_Transito_a_la_Izquierda', 'Indicacion_de_Separador_de_Transito_a_la_Derecha','Delineador_de_Curva',
        'Curva_Peligrosa_a_la_Izquierda', 'Curva_Peligrosa_a_la_Derecha','Curva_Pronunciada_a_la_Izquierda', 'Curva_Pronunciada_a_la_Derecha',
        'Curva_y_Contracurva_Cerrada_(primera_es_hacia_la_izquierda)','Curva_y_Contracurva_Cerrada_(primera_es_hacia_la_derecha)',
        'Curvas_Sucesivas_(primera_es_hacia_la_derecha)','Curvas_Sucesivas_(primera_es_hacia_la_izquierda)','Curva_y_Contracurva_Pronunciada_(primera_es_hacia_la_izquierda)',
        'Curva_y_Contracurva_Pronunciada_(primera_es_hacia_la_Derecha)','Interseccion_de_Vias', 'Via_Lateral_Izquierda', 'Via_Lateral_Derecha',
        'Bifurcacion_en_T', 'Bifurcacion_en_Y', 'Bifurcacion_Izquierda','Bifurcacion_Derecha', 'Bifurcacion_Escalonada_Izquierda-Derecha',
        'Bifurcacion_Escalonada_Derecha-Izquierda', 'Glorieta','Incorporacion_de_Transito_(desde_la_Izquierda)', 'Incorporacion_de_Transito_(desde_la_Derecha)',
        'Semaforo', 'Superficie_Rizada', 'Resalto', 'Depresion','Descenso_Peligroso', 'Reduccion_Simetrica_de_la_Calzada', 'Prevencion_de_Pare',
        'Reduccion_Asimetrica_de_la_Calzada_(hacia_la_Derecha)','Reduccion_Asimetrica_de_la_Calzada_(hacia_la_Izquierda)',
        'Ensanche_Simetrico_de_la_Calzada', 'Prevencion_de_Ceda_el_Paso',
        'Ensanche_Asimetrico_de_la_Calzada_(hacia la Izquierda)','Ensanche_Asimetrico_de_la_Calzada_(hacia la Derecha)', 'Puente_Angosto', 'Tunel',
        'Peso_Maximo_Permitido', 'Circulacion_en_Dos_Sentidos', 'Flecha_Direccional','Tres_Carriles_(uno_en_contraflujo)', 'Zona_de_Derrumbe',
        'Tres_Carriles_(dos_en_contraflujo)','Superficie_Deslizante', 'Maquinaria_Agricola_en_la_Via', 'Peatones_en_la_Via','Zona_Escolar',
        'Zona_Deportiva', 'Animales_en_la via', 'Altura_Libre', 'Ancho_Libre','Cruce_a_Nivel_con_el_Ferrocarril', 'Barrera', 'Paso_a_Nivel',
        'Iniciacion_de_Via_con_Separador(Dos_Sentidos_un_sentido)','Terminacion_de_Via_con_Separador_(Dos_Sentidos_un_sentido)',
        'Final_del_Pavimento', 'Ciclistas_en_la_Via', 'Riesgo_de_Accidente','Ruta_Nacional', 'Poste_de_Referencia', 'Informacion_Previa_de_Destino', 'Croquis',
        'Confirmativa_de_Destino', 'Nomenclatura_Urbana', 'Geografica', 'Senal_Transitoria','Senal_Informativa'
    ]
    SV_CLASSES_ID = [i for i in range(len(SV_CLASSES_NAMES_DICT))]
    SV_NUM_CLASSES = len(SV_CLASSES_NAMES_DICT)  # Número de clases del modelo de Señales Verticales
    SH_CLASSES_NAMES_DICT = ['Senal_Horizontal', 'Amarilla_a_trazos', 'Amarilla_continua', 'Blanca_a_trazos', 'Blanca_continua']
    SH_CLASSES_ID = [i for i in range(len(SH_CLASSES_NAMES_DICT))]
    SH_NUM_CLASSES = len(SH_CLASSES_NAMES_DICT)  # Número de clases del modelo de Señales Horizontales
    DV_CLASSES_NAMES_DICT = ['Bache', 'Grieta', 'Hundimiento']
    DV_CLASSES_ID = [i for i in range(len(DV_CLASSES_NAMES_DICT))]
    DV_NUM_CLASSES = len(DV_CLASSES_NAMES_DICT)  # Número de clases del modelo de Deterioro de la Superficie Vial

    # Carga los modelos YOLO-NAS entrenados
    sv_model = models.get(MODEL, num_classes=SV_NUM_CLASSES, checkpoint_path=SV_CHECKPOINT_DIR)
    sh_model = models.get(MODEL, num_classes=SH_NUM_CLASSES, checkpoint_path=SH_CHECKPOINT_DIR)
    dv_model = models.get(MODEL, num_classes=DV_NUM_CLASSES, checkpoint_path=DV_CHECKPOINT_DIR) 

    # Predecir y anotar todo el video

    cantidad_frames_video_completo = video_completo_frames

    videos_list = list(trozos_videos(TROZOS_VIDEOS_PATH))
    cantidad_video_trozos = len(videos_list)

    # Usar Dask para procesar en paralelo
    delayed_videos = [delayed(video_processing)(video,cantidad_frames_video_completo,cantidad_video_trozos,
                                                sv_model,SV_CLASSES_ID,SV_CLASSES_NAMES_DICT,
                                                sh_model,SH_CLASSES_ID,SH_CLASSES_NAMES_DICT,
                                                dv_model,DV_CLASSES_ID,DV_CLASSES_NAMES_DICT,
                                                GUARDADO_JSONS,GUARDADO_JSONS_DIR) for video in videos_list]
    dask.compute(*delayed_videos)
    # Cierra el cliente Dask
    client_Dask_proc_frames.close()
