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
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('streamer.log')
    ]
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

            logging.info(f"⬇️ Descargando imagen: {url}")
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

            logging.info(f"⬇️ Descargando música: {url}")
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
            datos = respuesta.json()
            
            for img in datos['imagenes']:
                img['local_path'] = self.procesar_imagen(img['url'])
            
            for musica in datos['musica']:
                musica['local_path'] = self.descargar_musica(musica['url'])
            
            return datos
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
            logging.info("🔑 Autenticación con YouTube exitosa")
            return build('youtube', 'v3', credentials=creds)
        
        except Exception as e:
            logging.error(f"🚨 Error de autenticación: {str(e)}")
            logging.error("⚠️ Solución: Genera un nuevo refresh token con el script de autenticación")
            return None
    
    def crear_transmision(self, titulo, imagen_path):
        if not self.youtube:
            return None
            
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=5)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "🎵 Música Chill Continua 24/7 • Mezcla profesional\n🔔 Activa las notificaciones\n👍 Déjanos tu like",
                        "scheduledStartTime": scheduled_start.isoformat() + "Z"
                    },
                    "status": {
                        "privacyStatus": "public",
                        "selfDeclaredMadeForKids": False,
                        "enableAutoStart": True,
                        "enableAutoStop": True,
                        "enableArchive": True,
                        "lifeCycleStatus": "created"
                    }
                }
            ).execute()

            stream = self.youtube.liveStreams().insert(
                part="snippet,cdn",
                body={
                    "snippet": {
                        "title": "Stream de música chill"
                    },
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
                media_body=imagen_path
            ).execute()

            logging.info(f"📡 Transmisión programada: {scheduled_start}")
            return {
                "rtmp": f"{stream['cdn']['ingestionInfo']['ingestionAddress']}/{stream['cdn']['ingestionInfo']['streamName']}",
                "scheduled_start": scheduled_start,
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id']
            }
        except Exception as e:
            logging.error(f"Error creando transmisión: {str(e)}")
            return None
    
    def obtener_estado_stream(self, stream_id):
        try:
            response = self.youtube.liveStreams().list(
                part="status",
                id=stream_id
            ).execute()
            return response.get('items', [{}])[0].get('status', {}).get('streamStatus')
        except Exception as e:
            logging.error(f"Error obteniendo estado del stream: {str(e)}")
            return None
    
    def transicionar_estado(self, broadcast_id, estado):
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus=estado,
                id=broadcast_id,
                part="id,status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Error transicionando a {estado}: {str(e)}")
            return False

def generar_titulo(imagen):
    temas = {
        'naturaleza': ['Bosque', 'Montaña', 'Playa', 'Selva'],
        'ciudad': ['Ciudad Nocturna', 'Metrópolis', 'Skyline', 'Urbano'],
        'abstracto': ['Arte Digital', 'Geometría', 'Fluido', 'Psicodélico']
    }
    categoria = random.choice(list(temas.keys()))
    return f"{random.choice(temas[categoria])} Vibes • {imagen['name']} • 24/7"

def manejar_transmision(stream_data, gestor, youtube):
    if not youtube.youtube:
        return False
        
    ffmpeg_process = None
    try:
        # Configurar tiempo de inicio
        tiempo_inicio_ffmpeg = stream_data['scheduled_start'] - timedelta(minutes=1)
        espera_ffmpeg = max(0, (tiempo_inicio_ffmpeg - datetime.utcnow()).total_seconds())
        
        logging.info(f"⏳ Esperando {espera_ffmpeg:.0f}s para iniciar FFmpeg...")
        time.sleep(espera_ffmpeg)

        # Configurar FIFO
        fifo_path = os.path.join(gestor.media_cache_dir, "audio_fifo")
        if os.path.exists(fifo_path):
            os.remove(fifo_path)
        os.mkfifo(fifo_path)

        # Iniciar FFmpeg
        ffmpeg_cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-re",
            "-loop", "1",
            "-i", stream_data['imagen']['local_path'],
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
            stream_data['rtmp']
        ]

        ffmpeg_process = subprocess.Popen(ffmpeg_cmd)
        logging.info("🟢 FFmpeg iniciado - Estableciendo conexión RTMP...")

        # Verificar estado del stream
        for _ in range(10):
            estado = youtube.obtener_estado_stream(stream_data['stream_id'])
            if estado == 'active':
                logging.info("✅ Stream activo")
                break
            time.sleep(5)
        else:
            logging.error("❌ Stream no se activó")
            return False

        # Transición a testing
        if not youtube.transicionar_estado(stream_data['broadcast_id'], 'testing'):
            return False
        logging.info("🎬 Modo vista previa activado")

        # Esperar inicio programado
        tiempo_restante = max(0, (stream_data['scheduled_start'] - datetime.utcnow()).total_seconds())
        logging.info(f"⏳ Esperando {tiempo_restante:.0f}s para LIVE...")
        time.sleep(tiempo_restante)

        # Transición a live
        if not youtube.transicionar_estado(stream_data['broadcast_id'], 'live'):
            return False
        logging.info("🎥 Transmisión LIVE iniciada")

        # Reproducción continua
        start_time = datetime.utcnow()
        while (datetime.utcnow() - start_time) < timedelta(hours=8):
            musica = random.choice([m for m in gestor.medios['musica'] if m['local_path']])
            logging.info(f"🎵 Reproduciendo: {musica['name']}")
            
            try:
                with open(musica['local_path'], 'rb') as audio_file:
                    with open(fifo_path, 'wb') as fifo:
                        fifo.write(audio_file.read())
            except Exception as e:
                logging.error(f"Error reproduciendo música: {str(e)}")

        logging.info("🕒 Transmisión completada (8 horas)")
        return True

    except Exception as e:
        logging.error(f"Error en transmisión: {str(e)}")
        return False
    finally:
        if ffmpeg_process:
            ffmpeg_process.terminate()
        if youtube.youtube:
            youtube.transicionar_estado(stream_data['broadcast_id'], 'complete')

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    if not youtube.youtube:
        logging.error("🚨 No se puede continuar sin autenticación válida")
        return

    current_stream = None
    
    while True:
        try:
            if not current_stream:
                imagen = random.choice([i for i in gestor.medios['imagenes'] if i['local_path']])
                logging.info(f"🖼️ Imagen seleccionada: {imagen['name']}")

                stream_info = youtube.crear_transmision(
                    generar_titulo(imagen), 
                    imagen['local_path']
                )
                
                if not stream_info:
                    raise Exception("Error creando transmisión")

                current_stream = {
                    **stream_info,
                    "imagen": imagen,
                    "end_time": stream_info['scheduled_start'] + timedelta(hours=8)
                }

                threading.Thread(
                    target=manejar_transmision,
                    args=(current_stream, gestor, youtube),
                    daemon=True
                ).start()

            else:
                if datetime.utcnow() >= current_stream['end_time'] + timedelta(minutes=5):
                    current_stream = None
                    logging.info("🔄 Preparando nueva transmisión...")
                
                time.sleep(15)

        except Exception as e:
            logging.error(f"🔥 Error crítico: {str(e)}")
            current_stream = None
            time.sleep(60)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    logging.info("🎧 Iniciando transmisor de música chill...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
