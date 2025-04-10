import os
import random
import subprocess
import logging
import time
import requests
import hashlib
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from flask import Flask
from waitress import serve
import threading

app = Flask(__name__)

# Configuración logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Configuración
MEDIOS_URL = "https://raw.githubusercontent.com/n14-py/relaxstationmedios/master/mediosmusic.json"
YOUTUBE_CREDS = {
    'client_id': os.getenv("YOUTUBE_CLIENT_ID"),
    'client_secret': os.getenv("YOUTUBE_CLIENT_SECRET"),
    'refresh_token': os.getenv("YOUTUBE_REFRESH_TOKEN")
}

class GestorContenido:
    def __init__(self):
        self.media_cache_dir = os.path.abspath("./media_cache")
        os.makedirs(self.media_cache_dir, exist_ok=True)
        self.medios = self.cargar_medios()
    
    def procesar_imagen(self, url):
        try:
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}.jpg")
            
            if os.path.exists(ruta_local):
                return ruta_local

            logging.info(f"[DESCARGA] Imagen: {url}")
            temp_path = os.path.join(self.media_cache_dir, f"temp_{nombre_hash}")
            
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(temp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            subprocess.run([
                "ffmpeg", "-y", "-i", temp_path,
                "-vf", "scale=1280:720",
                "-q:v", "2",
                ruta_local
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            os.remove(temp_path)
            return ruta_local
        except Exception as e:
            logging.error(f"Error procesando imagen: {str(e)}")
            return None

    def descargar_musica(self, url):
        try:
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}.mp3")
            
            if os.path.exists(ruta_local):
                return ruta_local

            logging.info(f"[DESCARGA] Música: {url}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(ruta_local, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return ruta_local
        except Exception as e:
            logging.error(f"Error descargando música: {str(e)}")
            return None

    def cargar_medios(self):
        try:
            respuesta = requests.get(MEDIOS_URL, timeout=20)
            respuesta.raise_for_status()
            return respuesta.json()
        except Exception as e:
            logging.error(f"Error cargando medios: {str(e)}")
            return {"imagenes": [], "musica": []}

class YouTubeManager:
    def __init__(self):
        self.youtube = self.autenticar()
    
    def autenticar(self):
        try:
            creds = Credentials(
                token=None,
                refresh_token=YOUTUBE_CREDS['refresh_token'],
                client_id=YOUTUBE_CREDS['client_id'],
                client_secret=YOUTUBE_CREDS['client_secret'],
                token_uri="https://oauth2.googleapis.com/token",
                scopes=['https://www.googleapis.com/auth/youtube']
            )
            creds.refresh(Request())
            logging.info("[AUTENTICACIÓN] Conexión exitosa con YouTube")
            return build('youtube', 'v3', credentials=creds)
        except Exception as e:
            logging.error(f"[ERROR] Fallo autenticación: {str(e)}")
            return None
    
    def crear_transmision(self, titulo, imagen_path):
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=3)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "🎵 Música Chill 24/7 • Ambiente Relajante\n🔔 Activa las notificaciones\n👍 Déjanos tu like",
                        "scheduledStartTime": scheduled_start.isoformat() + "Z"
                    },
                    "status": {
                        "privacyStatus": "public",
                        "selfDeclaredMadeForKids": False,
                        "enableAutoStart": True,
                        "enableAutoStop": True,
                        "lifeCycleStatus": "created"
                    }
                }
            ).execute()

            stream = self.youtube.liveStreams().insert(
                part="snippet,cdn",
                body={
                    "snippet": {"title": "Stream Automático"},
                    "cdn": {
                        "format": "1080p",
                        "ingestionType": "rtmp",
                        "resolution": "1080p",
                        "frameRate": "30fps"
                    }
                }
            ).execute()

            self.youtube.liveBroadcasts().bind(
                part="id,contentDetails",
                id=broadcast['id'],
                streamId=stream['id']
            ).execute()

            self.youtube.thumbnails().set(
                videoId=broadcast['id'],
                media_body=imagen_path,
                media_mime_type='image/jpeg'
            ).execute()

            logging.info(f"[PROGRAMACIÓN] Inicio: {scheduled_start}")
            return {
                "rtmp": f"{stream['cdn']['ingestionInfo']['ingestionAddress']}/{stream['cdn']['ingestionInfo']['streamName']}",
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id'],
                "start_time": scheduled_start
            }
        except Exception as e:
            logging.error(f"[ERROR] Creando transmisión: {str(e)}")
            return None
    
    def verificar_stream(self, stream_id):
        try:
            response = self.youtube.liveStreams().list(
                part="status",
                id=stream_id
            ).execute()
            return response.get('items', [{}])[0].get('status', {}).get('streamStatus')
        except Exception as e:
            logging.error(f"[ERROR] Verificando stream: {str(e)}")
            return None
    
    def cambiar_estado(self, broadcast_id, estado):
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus=estado,
                id=broadcast_id,
                part="id,status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"[ERROR] Transición a {estado}: {str(e)}")
            return False

def generar_titulo(imagen):
    temas = ["Noche Estrellada", "Atardecer Urbano", "Bosque Mágico", "Océano Infinito"]
    return f"{random.choice(temas)} • {imagen['name']} • Música Chill 24/7"

def manejar_transmision(gestor, youtube, stream_info, imagen):
    fifo_path = os.path.join(gestor.media_cache_dir, "audio_fifo")
    proceso = None
    
    try:
        # Iniciar FFmpeg 90 segundos antes
        tiempo_espera = (stream_info['start_time'] - timedelta(seconds=90) - datetime.utcnow()).total_seconds()
        if tiempo_espera > 0:
            logging.info(f"[ESPERA] Inicio FFmpeg en {tiempo_espera:.0f}s")
            time.sleep(tiempo_espera)
        
        # Configurar FIFO
        if os.path.exists(fifo_path):
            os.remove(fifo_path)
        os.mkfifo(fifo_path)

        # Comando FFmpeg mejorado
        ffmpeg_cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-re",
            "-loop", "1",
            "-i", imagen['local_path'],
            "-f", "mp3",
            "-i", fifo_path,
            "-vf", "format=yuv420p,scale=1280:720",
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "stillimage",
            "-b:v", "3000k",
            "-maxrate", "3000k",
            "-bufsize", "6000k",
            "-g", "60",
            "-c:a", "aac",
            "-b:a", "192k",
            "-f", "flv",
            stream_info['rtmp']
        ]

        proceso = subprocess.Popen(ffmpeg_cmd)
        logging.info("[FFMPEG] Transmisión RTMP iniciada")

        # Esperar activación del stream
        for i in range(1, 16):
            estado = youtube.verificar_stream(stream_info['stream_id'])
            if estado == 'active':
                logging.info("[ESTADO] Stream activo confirmado")
                break
            logging.info(f"[ESPERA] Verificando stream ({i}/15)...")
            time.sleep(5)
        else:
            logging.error("[ERROR] Stream no se activó")
            return False

        # Transición a testing
        if youtube.cambiar_estado(stream_info['broadcast_id'], 'testing'):
            logging.info("[ESTADO] Vista previa activada")
        else:
            return False

        # Esperar inicio programado
        tiempo_restante = (stream_info['start_time'] - datetime.utcnow()).total_seconds()
        if tiempo_restante > 0:
            logging.info(f"[ESPERA] Inicio LIVE en {tiempo_restante:.0f}s")
            time.sleep(tiempo_restante)
        
        # Transición a live
        if youtube.cambiar_estado(stream_info['broadcast_id'], 'live'):
            logging.info("[ESTADO] Transmisión LIVE activa")
        else:
            return False

        # Reproducción continua
        inicio = time.time()
        while (time.time() - inicio) < 28800:  # 8 horas
            musica = random.choice(gestor.medios['musica'])
            logging.info(f"[MÚSICA] {musica['name']}")
            
            try:
                with open(musica['local_path'], 'rb') as f:
                    with open(fifo_path, 'wb') as fifo:
                        fifo.write(f.read())
            except Exception as e:
                logging.error(f"[ERROR] Reproducción: {str(e)}")

        logging.info("[FINAL] Transmisión completada")
        return True

    except Exception as e:
        logging.error(f"[ERROR CRÍTICO] {str(e)}")
        return False
    finally:
        if proceso:
            proceso.terminate()
            proceso.wait()
        if youtube.youtube:
            try:
                youtube.cambiar_estado(stream_info['broadcast_id'], 'complete')
            except Exception as e:
                logging.error(f"[ERROR] Finalizando: {str(e)}")

def ciclo_principal():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    if not youtube.youtube:
        return

    while True:
        try:
            # Selección de contenido
            if not gestor.medios['imagenes']:
                logging.error("[ERROR] No hay imágenes disponibles")
                time.sleep(60)
                continue
                
            imagen = random.choice(gestor.medios['imagenes'])
            logging.info(f"[CONTENIDO] Imagen: {imagen['name']}")
            
            # Crear transmisión
            stream_info = youtube.crear_transmision(
                generar_titulo(imagen),
                imagen['local_path']
            )
            if not stream_info:
                raise Exception("Error creación transmisión")

            # Manejar transmisión
            if manejar_transmision(gestor, youtube, stream_info, imagen):
                logging.info("[CICLO] Nueva transmisión en 5 minutos...")
                time.sleep(300)
            else:
                time.sleep(60)

        except Exception as e:
            logging.error(f"[FALLO] {str(e)}")
            time.sleep(60)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    logging.info("[INICIO] Sistema de transmisión iniciado")
    threading.Thread(target=ciclo_principal, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
