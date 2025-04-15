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
    
    def procesar_media(self, url, es_imagen=False):
        try:
            # Manejar enlaces de Google Drive
            if "drive.google.com" in url:
                file_id = url.split('id=')[-1].split('&')[0]
                url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            extension = ".jpg" if es_imagen else ".mp3"
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}{extension}")
            
            if os.path.exists(ruta_local):
                return ruta_local

            logging.info(f"⬇️ Descargando {'imagen' if es_imagen else 'música'}: {url}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(ruta_local, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            if es_imagen:
                return self.optimizar_imagen(ruta_local)
            return ruta_local
        except Exception as e:
            logging.error(f"Error procesando archivo: {str(e)}")
            return None

    def optimizar_imagen(self, ruta_original):
        try:
            ruta_optimizada = ruta_original.replace(".jpg", "_opt.jpg")
            subprocess.run([
                "ffmpeg", "-y",
                "-i", ruta_original,
                "-vf", "scale=1280:720:force_original_aspect_ratio=increase",
                "-q:v", "2",
                "-compression_level", "6",
                ruta_optimizada
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return ruta_optimizada if os.path.exists(ruta_optimizada) else None
        except Exception as e:
            logging.error(f"Error optimizando imagen: {str(e)}")
            return ruta_original

    def cargar_medios(self):
        try:
            respuesta = requests.get(MEDIOS_URL, timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            # Procesar imágenes
            datos['imagenes'] = [{
                "name": img['name'],
                "local_path": self.procesar_media(img['url'], es_imagen=True)
            } for img in datos.get('imagenes', []) if img.get('url')]
            
            # Procesar música
            datos['musica'] = [{
                "name": m['name'],
                "local_path": self.procesar_media(m['url'])
            } for m in datos.get('musica', []) if m.get('url')]
            
            # Filtrar elementos fallidos
            datos['imagenes'] = [img for img in datos['imagenes'] if img['local_path']]
            datos['musica'] = [m for m in datos['musica'] if m['local_path']]
            
            logging.info(f"✅ Medios listos: {len(datos['musica'])} canciones, {len(datos['imagenes'])} imágenes")
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
                token="",
                refresh_token=YOUTUBE_CREDS['refresh_token'],
                client_id=YOUTUBE_CREDS['client_id'],
                client_secret=YOUTUBE_CREDS['client_secret'],
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
            scheduled_start = datetime.utcnow() + timedelta(seconds=30)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "🎵 Música continua 24/7 • Cambio automático cada 8 horas\n🔔 Activa las notificaciones",
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
                    "snippet": {"title": "Stream principal"},
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

            # Subir miniatura
            if imagen_path and os.path.exists(imagen_path):
                self.youtube.thumbnails().set(
                    videoId=broadcast['id'],
                    media_body=imagen_path,
                    media_mime_type='image/jpeg'
                ).execute()

            return {
                "rtmp": f"{stream['cdn']['ingestionInfo']['ingestionAddress']}/{stream['cdn']['ingestionInfo']['streamName']}",
                "broadcast_id": broadcast['id'],
                "start_time": scheduled_start
            }
        except Exception as e:
            logging.error(f"Error creando transmisión: {str(e)}")
            return None
    
    def iniciar_transmision(self, broadcast_id):
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus="live",
                id=broadcast_id,
                part="id,status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Error iniciando transmisión: {str(e)}")
            return False

def generar_playlist(canciones):
    playlist_path = os.path.join("./media_cache", "playlist.txt")
    with open(playlist_path, "w") as f:
        for cancion in canciones:
            f.write(f"file '{cancion['local_path']}'\n")
    return playlist_path

def manejar_transmision(gestor, youtube):
    try:
        if not gestor.medios['imagenes'] or not gestor.medios['musica']:
            logging.error("❌ No hay suficiente contenido para transmitir")
            return False
        
        # Seleccionar contenido
        imagen = random.choice(gestor.medios['imagenes'])
        canciones = random.sample(gestor.medios['musica'], min(50, len(gestor.medios['musica'])))
        playlist = generar_playlist(canciones)
        
        # Crear transmisión
        stream_info = youtube.crear_transmision(
            f"🎧 {imagen['name']} • Música Continua", 
            imagen['local_path']
        )
        
        if not stream_info:
            return False

        # Configurar FFmpeg
        cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-re",
            "-loop", "1",
            "-i", imagen['local_path'],
            "-f", "concat",
            "-safe", "0",
            "-i", playlist,
            "-vf", "format=yuv420p,scale=1280:720",
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "stillimage",
            "-b:v", "3000k",
            "-g", "60",
            "-c:a", "aac",
            "-b:a", "192k",
            "-f", "flv",
            stream_info['rtmp']
        ]

        proceso = subprocess.Popen(cmd)
        logging.info("🟢 Iniciando transmisión...")
        
        # Esperar 8 horas
        start_time = time.time()
        while time.time() - start_time < 28800:
            if proceso.poll() is not None:
                logging.warning("⚡ Reconectando FFmpeg...")
                proceso.kill()
                proceso = subprocess.Popen(cmd)
            time.sleep(30)
        
        proceso.kill()
        logging.info("🛑 Transmisión finalizada")
        return True
        
    except Exception as e:
        logging.error(f"Error en transmisión: {str(e)}")
        return False

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    while True:
        try:
            if not gestor.medios['imagenes']:
                logging.error("🚨 No hay imágenes disponibles - Reintentando en 5 minutos...")
                time.sleep(300)
                continue
                
            if manejar_transmision(gestor, youtube):
                logging.info("⏳ Esperando 5 minutos para nueva transmisión...")
                time.sleep(300)
                
        except Exception as e:
            logging.error(f"Error crítico: {str(e)}")
            time.sleep(60)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    logging.info("🎧 Iniciando sistema de radio 24/7...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
