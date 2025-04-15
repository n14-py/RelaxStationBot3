import os
import random
import subprocess
import logging
import time
import requests
import hashlib
import signal
import sys
from datetime import datetime, timedelta
from flask import Flask
from waitress import serve
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from urllib.parse import urlparse
import threading

app = Flask(__name__)

# Configuración logging IDÉNTICA a tu versión
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Configuración mejorada
CONFIG = {
    "MEDIOS_URL": "https://raw.githubusercontent.com/n14-py/relaxstationmedios/master/mediosmusic.json",
    "CACHE_DIR": os.path.abspath("./media_cache"),
    "STREAM_DURATION": 8 * 3600,  # 8 horas
    "RETRY_DELAY": 300,  # 5 minutos entre intentos fallidos
    "YOUTUBE_CREDS": {
        'client_id': os.getenv("YOUTUBE_CLIENT_ID"),
        'client_secret': os.getenv("YOUTUBE_CLIENT_SECRET"),
        'refresh_token': os.getenv("YOUTUBE_REFRESH_TOKEN")
    },
    "FFMPEG_PARAMS": {
        "video_codec": "libx264",
        "audio_codec": "aac",
        "video_bitrate": "3000k",
        "audio_bitrate": "192k",
        "resolution": "1280x720",
        "fps": "30",
        "preset": "ultrafast",
        "tune": "zerolatency",
        "pixel_format": "yuv420p"
    }
}

class GestorContenido:
    def __init__(self):
        os.makedirs(CONFIG['CACHE_DIR'], exist_ok=True)
        self.medios = self.cargar_medios()
    
    def procesar_url(self, url):
        if "drive.google.com" in url:
            file_id = url.split('id=')[-1].split('&')[0]
            return f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
        return url
    
    def procesar_imagen(self, url):
        try:
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_local = os.path.join(CONFIG['CACHE_DIR'], f"{nombre_hash}.jpg")
            
            if os.path.exists(ruta_local):
                logging.info(f"♻️ Usando imagen en caché: {nombre_hash}")
                return ruta_local

            logging.info(f"⬇️ Descargando imagen: {url}")
            url = self.procesar_url(url)
            
            # Descargar imagen temporal
            temp_path = os.path.join(CONFIG['CACHE_DIR'], f"temp_{nombre_hash}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(temp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            # Optimizar imagen
            subprocess.run([
                "ffmpeg", "-y", "-i", temp_path,
                "-vf", "scale=1280:720:force_original_aspect_ratio=increase",
                "-q:v", "2", ruta_local
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            os.remove(temp_path)
            logging.info(f"✅ Imagen optimizada: {ruta_local}")
            return ruta_local
        except Exception as e:
            logging.error(f"Error procesando imagen: {str(e)}")
            return None

    def descargar_musica(self, url):
        try:
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_local = os.path.join(CONFIG['CACHE_DIR'], f"{nombre_hash}.mp3")
            
            if os.path.exists(ruta_local):
                logging.info(f"♻️ Usando música en caché: {nombre_hash}")
                return ruta_local

            logging.info(f"⬇️ Descargando música: {url}")
            url = self.procesar_url(url)
            
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
            logging.info("📡 Obteniendo medios remotos...")
            respuesta = requests.get(CONFIG["MEDIOS_URL"], timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            # Procesar imágenes
            for img in datos['imagenes']:
                img['local_path'] = self.procesar_imagen(img['url'])
            
            # Procesar música
            for musica in datos['musica']:
                musica['local_path'] = self.descargar_musica(musica['url'])
            
            # Filtrar elementos válidos
            datos['imagenes'] = [img for img in datos['imagenes'] if img['local_path']]
            datos['musica'] = [m for m in datos['musica'] if m['local_path']]
            
            logging.info(f"✅ Medios cargados: {len(datos['imagenes'])} imágenes, {len(datos['musica'])} canciones")
            return datos
        except Exception as e:
            logging.error(f"Error cargando medios: {str(e)}")
            return {"imagenes": [], "musica": []}

class YouTubeManager:
    def __init__(self):
        self.youtube = self.autenticar()
    
    def autenticar(self):
        try:
            logging.info("🔑 Autenticando con YouTube...")
            creds = Credentials(
                token="",
                refresh_token=CONFIG['YOUTUBE_CREDS']['refresh_token'],
                client_id=CONFIG['YOUTUBE_CREDS']['client_id'],
                client_secret=CONFIG['YOUTUBE_CREDS']['client_secret'],
                token_uri="https://oauth2.googleapis.com/token",
                scopes=['https://www.googleapis.com/auth/youtube']
            )
            creds.refresh(Request())
            return build('youtube', 'v3', credentials=creds)
        except Exception as e:
            logging.error(f"Error autenticación YouTube: {str(e)}")
            return None
    
    def crear_transmision(self, titulo, imagen_path):
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=5)
            logging.info(f"📡 Programando transmisión para {scheduled_start}")
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "🎵 Música Continua 24/7 • Transmisión Automática\n👉 https://github.com/n14-py/RelaxStation",
                        "scheduledStartTime": scheduled_start.isoformat() + "Z"
                    },
                    "status": {
                        "privacyStatus": "public",
                        "selfDeclaredMadeForKids": False,
                        "enableAutoStart": True,
                        "enableAutoStop": True
                    }
                }
            ).execute()

            stream = self.youtube.liveStreams().insert(
                part="snippet,cdn",
                body={
                    "snippet": {"title": "Stream Automático"},
                    "cdn": {
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

            if imagen_path and os.path.exists(imagen_path):
                self.youtube.thumbnails().set(
                    videoId=broadcast['id'],
                    media_body=imagen_path
                ).execute()

            return {
                "rtmp": f"{stream['cdn']['ingestionInfo']['ingestionAddress']}/{stream['cdn']['ingestionInfo']['streamName']}",
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id'],
                "scheduled_start": scheduled_start
            }
        except Exception as e:
            logging.error(f"Error creando transmisión: {str(e)}")
            return None
    
    def transicionar_estado(self, broadcast_id, estado):
        try:
            logging.info(f"🔄 Transicionando a {estado}...")
            self.youtube.liveBroadcasts().transition(
                broadcastStatus=estado,
                id=broadcast_id,
                part="id,status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Error transicionando a {estado}: {str(e)}")
            return False
    
    def finalizar_transmision(self, broadcast_id):
        try:
            self.transicionar_estado(broadcast_id, "complete")
            return True
        except Exception as e:
            logging.error(f"Error finalizando transmisión: {str(e)}")
            return False

def generar_titulo(imagen):
    return f"🎧 {imagen['name']} • Música Continua • {datetime.utcnow().strftime('%H:%M UTC')}"

def manejar_transmision(stream_data, youtube, imagen, canciones):
    proceso = None
    fifo_path = os.path.join(CONFIG['CACHE_DIR'], "audio_fifo")
    
    try:
        if os.path.exists(fifo_path):
            os.remove(fifo_path)
        os.mkfifo(fifo_path)

        # Comando FFmpeg mejorado
        cmd = [
            "ffmpeg",
            "-loglevel", "verbose",
            "-re",
            "-loop", "1",
            "-i", os.path.abspath(imagen['local_path']),
            "-f", "concat",
            "-safe", "0",
            "-protocol_whitelist", "file,pipe",
            "-i", fifo_path,
            "-vf", f"fps={CONFIG['FFMPEG_PARAMS']['fps']},format={CONFIG['FFMPEG_PARAMS']['pixel_format']},scale=1280:720",
            "-c:v", CONFIG['FFMPEG_PARAMS']['video_codec'],
            "-preset", CONFIG['FFMPEG_PARAMS']['preset'],
            "-tune", CONFIG['FFMPEG_PARAMS']['tune'],
            "-b:v", CONFIG['FFMPEG_PARAMS']['video_bitrate'],
            "-maxrate", CONFIG['FFMPEG_PARAMS']['video_bitrate'],
            "-bufsize", "6000k",
            "-g", "60",
            "-c:a", CONFIG['FFMPEG_PARAMS']['audio_codec'],
            "-b:a", CONFIG['FFMPEG_PARAMS']['audio_bitrate'],
            "-f", "flv",
            stream_data['rtmp']
        ]

        proceso = subprocess.Popen(cmd, stderr=subprocess.PIPE)
        
        def log_ffmpeg():
            while proceso.poll() is None:
                output = proceso.stderr.readline().decode().strip()
                if output: logging.info(f"FFMPEG: {output}")

        threading.Thread(target=log_ffmpeg, daemon=True).start()

        # Esperar y activar stream
        for _ in range(12):  # 2 minutos máximo
            estado = youtube.obtener_estado(stream_data['stream_id'])
            if estado == 'active' and youtube.transicionar_estado(stream_data['broadcast_id'], 'live'):
                break
            time.sleep(10)
        
        # Reproducción aleatoria continua
        tiempo_inicio = datetime.utcnow()
        while (datetime.utcnow() - tiempo_inicio) < timedelta(hours=8):
            cancion = random.choice(canciones)
            logging.info(f"🎵 Reproduciendo: {cancion['name']}")
            
            try:
                with open(cancion['local_path'], 'rb') as audio_file:
                    with open(fifo_path, 'wb') as fifo:
                        fifo.write(audio_file.read())
                time.sleep(1)  # Pausa entre canciones
            except Exception as e:
                logging.error(f"Error reproduciendo canción: {str(e)}")
        
        proceso.terminate()
        return True

    except Exception as e:
        logging.error(f"🚨 Error en transmisión: {str(e)}")
        return False
    finally:
        if os.path.exists(fifo_path):
            os.remove(fifo_path)
        if proceso: proceso.kill()
        youtube.finalizar_transmision(stream_data['broadcast_id'])

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    while True:
        try:
            if not gestor.medios['imagenes'] or not gestor.medios['musica']:
                logging.warning("🚨 Contenido insuficiente, reintentando...")
                time.sleep(CONFIG['RETRY_DELAY'])
                continue
            
            imagen = random.choice(gestor.medios['imagenes'])
            canciones = gestor.medios['musica'].copy()
            random.shuffle(canciones)
            
            logging.info(f"\n{'='*50}\n🖼️ Iniciando transmisión con: {imagen['name']}\n{'='*50}")
            
            stream_info = youtube.crear_transmision(generar_titulo(imagen), imagen['local_path'])
            if not stream_info:
                time.sleep(CONFIG['RETRY_DELAY'])
                continue
            
            manejar_transmision(stream_info, youtube, imagen, canciones)
            logging.info(f"\n{'='*50}\n🔄 Ciclo de 8 horas completado\n{'='*50}")
            
        except Exception as e:
            logging.error(f"🔥 Error crítico: {str(e)}")
            time.sleep(CONFIG['RETRY_DELAY'])

@app.route('/health')
def health_check():
    return "OK", 200

def signal_handler(sig, frame):
    logging.info("\n🛑 Deteniendo servicio...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    logging.info("\n" + "="*50 + "\n🎧 Iniciando Transmisor Automático 24/7\n" + "="*50)
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
