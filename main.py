import os
import random
import subprocess
import logging
import time
import requests
import hashlib
import mimetypes
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from flask import Flask
from waitress import serve
from urllib.parse import urlparse, parse_qs
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

PLANTILLAS_TITULOS = [
    "🎧 {nombre} • Música Continua 24/7",
    "🌌 Ambiente {nombre} • Mix Relajante",
    "🔥 Lo mejor de {nombre} • Stream Infinito",
    "🧘♂️ {nombre} • Música para Meditar",
    "🌿 Sonidos de {nombre} • Naturaleza Viva",
    "🚀 Energía {nombre} • Mix Motivacional"
]

class GestorContenido:
    def __init__(self):
        self.media_cache_dir = os.path.abspath("./media_cache")
        os.makedirs(self.media_cache_dir, exist_ok=True)
        self.medios = self.cargar_medios()
    
    def descargar_archivo(self, url, es_imagen=False):
        try:
            # Manejar Google Drive
            if "drive.google.com" in url:
                file_id = parse_qs(urlparse(url).query).get('id', [None])[0]
                if file_id:
                    url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            
            extension = os.path.splitext(urlparse(url).path)[1].lower() if es_imagen else '.mp3'
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}{extension}")
            
            if os.path.exists(ruta_local):
                return ruta_local

            logging.info(f"⬇️ Descargando {'imagen' if es_imagen else 'música'}: {url}")
            
            session = requests.Session()
            response = session.get(url, stream=True, timeout=30)
            response.raise_for_status()

            # Manejar descargas grandes de Google Drive
            if "drive.google.com" in url and "confirm=t" in url:
                for key, value in response.cookies.items():
                    if key.startswith("download_warning"):
                        params = {'id': file_id, 'confirm': value}
                        response = session.get("https://drive.google.com/uc", params=params, stream=True)
                        break

            with open(ruta_local, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            return ruta_local
        except Exception as e:
            logging.error(f"Error descargando archivo: {str(e)}")
            return None

    def procesar_imagen(self, url):
        try:
            ruta_original = self.descargar_archivo(url, es_imagen=True)
            if not ruta_original:
                return None

            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_procesada = os.path.join(self.media_cache_dir, f"{nombre_hash}.jpg")

            # Convertir a JPEG con ffmpeg
            subprocess.run([
                "ffmpeg", "-y",
                "-i", ruta_original,
                "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:-1:-1",
                "-q:v", "2",
                "-pix_fmt", "yuv420p",
                ruta_procesada
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            return ruta_procesada
        except Exception as e:
            logging.error(f"Error procesando imagen: {str(e)}")
            return None

    def cargar_medios(self):
        try:
            respuesta = requests.get(MEDIOS_URL, timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            # Procesar imágenes
            for img in datos['imagenes']:
                img['local_path'] = self.procesar_imagen(img['url'])
            
            # Procesar música
            for musica in datos['musica']:
                musica['local_path'] = self.descargar_archivo(musica['url'])
            
            # Filtrar elementos inválidos
            datos['imagenes'] = [i for i in datos['imagenes'] if i['local_path']]
            datos['musica'] = [m for m in datos['musica'] if m['local_path']]
            
            if not datos['imagenes'] or not datos['musica']:
                raise ValueError("No hay contenido válido para transmitir")
            
            logging.info("✅ Medios verificados y listos")
            return datos
        except Exception as e:
            logging.error(f"Error cargando medios: {str(e)}")
            return {"imagenes": [], "musica": []}

class YouTubeManager:
    def __init__(self):
        self.youtube = self.autenticar()
    
    def autenticar(self):
        creds = Credentials(
            token="",
            refresh_token=YOUTUBE_CREDS['refresh_token'],
            client_id=YOUTUBE_CREDS['client_id'],
            client_secret=YOUTUBE_CREDS['client_secret'],
            token_uri="https://oauth2.googleapis.com/token",
            scopes=['https://www.googleapis.com/auth/youtube']
        )
        creds.refresh(Request())
        return build('youtube', 'v3', credentials=creds)
    
    def crear_transmision(self, titulo, imagen_path):
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=5)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "Disfruta de nuestra selección musical continua con ambientación visual profesional.\n\n🔔 Activa las notificaciones\n👍 Déjanos tu like\n💬 Comenta tus sugerencias",
                        "scheduledStartTime": scheduled_start.isoformat() + "Z"
                    },
                    "status": {
                        "privacyStatus": "public",
                        "selfDeclaredMadeForKids": False,
                        "enableAutoStart": True,
                        "enableAutoStop": True,
                        "enableArchive": True
                    }
                }
            ).execute()

            stream = self.youtube.liveStreams().insert(
                part="snippet,cdn",
                body={
                    "snippet": {
                        "title": "Stream Continuo"
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

            # Subir miniatura
            self.youtube.thumbnails().set(
                videoId=broadcast['id'],
                media_body=imagen_path,
                media_mime_type='image/jpeg'
            ).execute()

            return {
                "rtmp": f"{stream['cdn']['ingestionInfo']['ingestionAddress']}/{stream['cdn']['ingestionInfo']['streamName']}",
                "scheduled_start": scheduled_start,
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id']
            }
        except Exception as e:
            logging.error(f"Error creando transmisión: {str(e)}")
            return None
    
    def control_estado(self, broadcast_id, estado):
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus=estado,
                id=broadcast_id,
                part="id,status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Error cambiando a estado {estado}: {str(e)}")
            return False

def generar_titulo(nombre_imagen):
    nombre = nombre_imagen.split('-')[0].strip().title()
    return random.choice(PLANTILLAS_TITULOS).format(nombre=nombre)

def manejar_transmision(stream_data, youtube):
    proceso = None
    try:
        # Esperar inicio programado
        tiempo_espera = (stream_data['scheduled_start'] - datetime.utcnow()).total_seconds()
        if tiempo_espera > 0:
            logging.info(f"⏳ Esperando {tiempo_espera:.0f}s para preparar transmisión...")
            time.sleep(tiempo_espera)
        
        # Configurar FIFO
        fifo_path = os.path.join(stream_data['gestor'].media_cache_dir, "audio_fifo")
        if os.path.exists(fifo_path):
            os.remove(fifo_path)
        os.mkfifo(fifo_path)

        # Comando FFmpeg optimizado
        cmd = [
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

        proceso = subprocess.Popen(cmd)
        logging.info("🟢 FFmpeg iniciado - Transmisión activa")

        # Control de estados
        youtube.control_estado(stream_data['broadcast_id'], 'testing')
        logging.info("🎬 Transmisión en vista previa")
        
        tiempo_restante = (stream_data['scheduled_start'] - datetime.utcnow()).total_seconds()
        if tiempo_restante > 0:
            time.sleep(tiempo_restante)
        
        youtube.control_estado(stream_data['broadcast_id'], 'live')
        logging.info("🎥 Transmisión LIVE iniciada")

        # Reproducción continua
        tiempo_inicio = datetime.utcnow()
        while (datetime.utcnow() - tiempo_inicio) < timedelta(hours=8):
            musica = random.choice(stream_data['gestor'].medios['musica'])
            logging.info(f"🎵 Reproduciendo: {musica['name']}")
            
            try:
                with open(musica['local_path'], 'rb') as audio_file:
                    with open(fifo_path, 'wb') as fifo:
                        fifo.write(audio_file.read())
            except Exception as e:
                logging.error(f"Error enviando audio: {str(e)}")

        proceso.terminate()
        youtube.control_estado(stream_data['broadcast_id'], 'complete')
        logging.info("🛑 Transmisión finalizada correctamente")

    except Exception as e:
        logging.error(f"ERROR: {str(e)}")
        if proceso:
            proceso.kill()
        youtube.control_estado(stream_data['broadcast_id'], 'complete')

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    while True:
        try:
            if not gestor.medios['imagenes'] or not gestor.medios['musica']:
                logging.error("No hay medios válidos, reintentando...")
                time.sleep(60)
                gestor = GestorContenido()
                continue
            
            imagen = random.choice(gestor.medios['imagenes'])
            logging.info(f"🖼️ Imagen seleccionada: {imagen['name']}")
            
            titulo = generar_titulo(imagen['name'])
            logging.info(f"📝 Título generado: {titulo}")
            
            stream_info = youtube.crear_transmision(titulo, imagen['local_path'])
            if not stream_info:
                raise Exception("Error al crear transmisión")
            
            stream_data = {
                "rtmp": stream_info['rtmp'],
                "scheduled_start": stream_info['scheduled_start'],
                "imagen": imagen,
                "broadcast_id": stream_info['broadcast_id'],
                "gestor": gestor
            }

            hilo = threading.Thread(
                target=manejar_transmision,
                args=(stream_data, youtube),
                daemon=True
            )
            hilo.start()
            
            # Esperar 8 horas + margen
            time.sleep(28800 + 300)
            
        except Exception as e:
            logging.error(f"ERROR GRAVE: {str(e)}")
            time.sleep(60)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    logging.info("🎧 Iniciando transmisor de música continua...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
