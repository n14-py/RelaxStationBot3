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
from urllib.parse import urlparse
import threading

app = Flask(__name__)

# Configuración logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Configuración
MEDIOS_URL = "https://raw.githubusercontent.com/n14-py/RelaxStationmedios/master/mediosmusic.json"
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
        self.verificar_contenido()
    
    def verificar_contenido(self):
        if not self.medios['imagenes']:
            logging.error("❌ No se encontraron imágenes válidas en el JSON")
        if not self.medios['musica']:
            logging.error("❌ No se encontraron canciones válidas en el JSON")
    
    def procesar_google_drive_url(self, url):
        try:
            parsed = urlparse(url)
            query = parsed.query
            params = dict(x.split('=') for x in query.split('&'))
            return f"https://drive.google.com/uc?export=download&id={params['id']}&confirm=t"
        except:
            return url
    
    def optimizar_imagen(self, ruta_original):
        try:
            ruta_optimizada = f"{ruta_original}_opt.jpg"
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
            return None
    
    def descargar_archivo(self, url, es_imagen=False):
        try:
            url = self.procesar_google_drive_url(url)
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
            
            return self.optimizar_imagen(ruta_local) if es_imagen else ruta_local
        except Exception as e:
            logging.error(f"Error descargando archivo: {str(e)}")
            return None
    
    def cargar_medios(self):
        try:
            respuesta = requests.get(MEDIOS_URL, timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            # Validar estructura del JSON
            if not all(key in datos for key in ["imagenes", "musica"]):
                raise ValueError("Estructura JSON inválida")
            
            # Procesar imágenes
            imagenes_procesadas = []
            for img in datos['imagenes']:
                if 'url' in img and 'name' in img:
                    local_path = self.descargar_archivo(img['url'], es_imagen=True)
                    if local_path:
                        imagenes_procesadas.append({
                            "name": img['name'],
                            "local_path": local_path
                        })
                        logging.info(f"✅ Imagen procesada: {img['name']}")
                    else:
                        logging.warning(f"⚠️ Imagen fallida: {img['name']}")
            
            # Procesar música
            musica_procesada = []
            for m in datos['musica']:
                if 'url' in m and 'name' in m:
                    local_path = self.descargar_archivo(m['url'])
                    if local_path:
                        musica_procesada.append({
                            "name": m['name'],
                            "local_path": local_path
                        })
                        logging.info(f"✅ Música procesada: {m['name']}")
                    else:
                        logging.warning(f"⚠️ Música fallida: {m['name']}")
            
            return {
                "imagenes": imagenes_procesadas,
                "musica": musica_procesada
            }
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
            scheduled_start = datetime.utcnow() + timedelta(minutes=2)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "🎵 Relax Station Radio • Música Continua 24/7\n\nDisfruta de nuestra programación musical las 24 horas\n\n🔔 Activa las notificaciones\n\n#MusicaContinua #RadioOnline",
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
                    "snippet": {"title": "Stream Principal"},
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
    return f"🎧 {imagen['name']} • Música Continua • {datetime.utcnow().strftime('%H:%M UTC')}"

def generar_playlist(canciones, cache_dir):
    playlist_path = os.path.join(cache_dir, "playlist.txt")
    with open(playlist_path, "w") as f:
        f.write("\n".join([f"file '{m['local_path']}'" for m in canciones]))
    return playlist_path

def manejar_transmision(stream_data, youtube, imagen, playlist_path):
    try:
        # Configurar FFmpeg
        cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-re",
            "-loop", "1",
            "-i", imagen['local_path'],
            "-f", "concat",
            "-safe", "0",
            "-i", playlist_path,
            "-vf", "scale=1280:720,setsar=1",
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "stillimage",
            "-b:v", "3000k",
            "-g", "60",
            "-c:a", "aac",
            "-b:a", "192k",
            "-f", "flv",
            stream_data['rtmp']
        ]

        proceso = subprocess.Popen(cmd)
        logging.info(f"🟢 Iniciando transmisión: {imagen['name']}")
        
        # Transición a LIVE después de 1 minuto
        time.sleep(60)
        if not youtube.transicionar_estado(stream_data['broadcast_id'], 'live'):
            raise Exception("No se pudo iniciar transmisión LIVE")
        
        logging.info("🎥 Transmisión LIVE activada")
        
        # Mantener por 8 horas
        tiempo_inicio = time.time()
        while time.time() - tiempo_inicio < 28800:  # 8 horas
            if proceso.poll() is not None:
                logging.warning("⚡ Reconectando FFmpeg...")
                proceso.kill()
                proceso = subprocess.Popen(cmd)
            time.sleep(30)
        
        proceso.kill()
        logging.info("🛑 Transmisión finalizada correctamente")
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
                
            if not gestor.medios['musica']:
                logging.error("🚨 No hay música disponible - Reintentando en 5 minutos...")
                time.sleep(300)
                continue

            # Seleccionar contenido
            imagen = random.choice(gestor.medios['imagenes'])
            canciones = random.sample(gestor.medios['musica'], len(gestor.medios['musica']))
            
            # Generar playlist
            playlist_path = generar_playlist(canciones, gestor.media_cache_dir)
            
            # Crear transmisión
            stream_info = youtube.crear_transmision(generar_titulo(imagen), imagen['local_path'])
            if not stream_info:
                raise Exception("Error al crear stream")
            
            # Manejar transmisión
            if manejar_transmision(stream_info, youtube, imagen, playlist_path):
                logging.info("⏳ Esperando 5 minutos para nueva transmisión...")
                time.sleep(300)
                
        except Exception as e:
            logging.error(f"Error crítico: {str(e)}")
            time.sleep(60)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    logging.info("📻 Iniciando Radio 24/7...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
