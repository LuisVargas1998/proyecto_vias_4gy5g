import cv2
import os



def fragmentar_video(video_path,output_dir,trozos):
    
    def write_video_chunk(start_frame, end_frame, output_path):
        cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(output_path, fourcc, fps, size)  
        for frame_num in range(start_frame, end_frame):
            ret, frame = cap.read()
            if not ret:
                break
            out.write(frame)
        out.release()
    
    # Crear el directorio de salida si no existe
    os.makedirs(output_dir, exist_ok=True)
    # Obtener el nombre del archivo sin la extensión
    video_name = os.path.splitext(os.path.basename(video_path))[0]
    # Leer el video
    cap = cv2.VideoCapture(video_path)
    # Obtener el número total de frames y la tasa de frames por segundo (fps)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    # Calcular el número de frames por trozo
    frames_per_part = total_frames // trozos
    # Obtener el tamaño del video (ancho y alto)
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    size = (frame_width, frame_height)

    # Procesar y guardar los trozos de video
    for i in range(trozos):
        start_frame = i * frames_per_part
        end_frame = (i + 1) * frames_per_part if i < (trozos-1) else total_frames
        output_path = os.path.join(output_dir, f'{video_name}_trozo{i+1}.mp4')
        write_video_chunk(start_frame, end_frame, output_path)
    # Liberar el objeto de captura de video
    cap.release()
    print("Video partido en trozos.")