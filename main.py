import os
import random
import subprocess
import logging
import time
import requests
import hashlib
import json
import threading
import signal
import sys
from datetime import datetime, timedelta
from urllib.parse import urlparse
from flask import Flask
from waitress import serve
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

app = Flask(__name__)

# Configuración logging mejorada
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Configuración global actualizada
CONFIG = {
    "MEDIOS_URL": "https://raw.githubusercontent.com/n14-py/RelaxStationmedios/master/mediosmusic.json",
    "CACHE_DIR": os.path.abspath("./radio_cache"),
    "STREAM_DURATION": 8 * 3600,
    "RETRY_DELAY": 300,
    "STREAM_ACTIVATION_TIMEOUT": 180,
    "STREAM_CHECK_INTERVAL": 15,
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
        "g": "60",
        "buffersize": "6000k"
    },
    "MAX_THUMBNAIL_SIZE": 2 * 1024 * 1024
}

class GestorContenido:
    def __init__(self):
        os.makedirs(CONFIG['CACHE_DIR'], exist_ok=True)
        self.medios = self.cargar_medios()
        self.verificar_contenido()
    
    def verificar_contenido(self):
        if not self.medios['imagenes']:
            logging.error("❌ No hay imágenes disponibles")
        if not self.medios['musica']:
            logging.error("❌ No hay música disponible")
    
    def procesar_url_google_drive(self, url):
        try:
            if "drive.google.com" in url:
                file_id = url.split('d/')[1].split('/')[0] if 'd/' in url else url.split('id=')[1]
                return f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            return url
        except:
            return url
    
    def optimizar_imagen(self, ruta_original):
        try:
            ruta_optimizada = f"{ruta_original}_opt.jpg"
            subprocess.run([
                "ffmpeg", "-y", "-i", ruta_original,
                "-vf", "scale=1280:720:force_original_aspect_ratio=increase",
                "-q:v", "2", ruta_optimizada
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            calidad = 85
            while os.path.getsize(ruta_optimizada) > CONFIG['MAX_THUMBNAIL_SIZE'] and calidad > 30:
                subprocess.run([
                    "ffmpeg", "-y", "-i", ruta_optimizada,
                    "-q:v", str(calidad), ruta_optimizada
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                calidad -= 5
            return ruta_optimizada
        except Exception as e:
            logging.error(f"Error optimizando imagen: {str(e)}")
            return ruta_original
    
    def descargar_archivo(self, url, es_imagen=False):
        try:
            url = self.procesar_url_google_drive(url)
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            extension = ".jpg" if es_imagen else ".mp3"
            ruta_local = os.path.join(CONFIG['CACHE_DIR'], f"{nombre_hash}{extension}")
            
            if os.path.exists(ruta_local):
                return ruta_local
                
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(ruta_local, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk: f.write(chunk)
            
            return self.optimizar_imagen(ruta_local) if es_imagen else ruta_local
        except Exception as e:
            logging.error(f"Error descargando archivo: {str(e)}")
            return None
    
    def cargar_medios(self):
        try:
            respuesta = requests.get(CONFIG["MEDIOS_URL"], timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            return {
                "imagenes": [{"name": img['name'], "local_path": self.descargar_archivo(img['url'], True)} for img in datos['imagenes'] if self.descargar_archivo(img['url'], True)],
                "musica": [{"name": m['name'], "local_path": self.descargar_archivo(m['url'])} for m in datos['musica'] if self.descargar_archivo(m['url'])]
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
                refresh_token=CONFIG['YOUTUBE_CREDS']['refresh_token'],
                client_id=CONFIG['YOUTUBE_CREDS']['client_id'],
                client_secret=CONFIG['YOUTUBE_CREDS']['client_secret'],
                token_uri="https://oauth2.googleapis.com/token",
                scopes=['https://www.googleapis.com/auth/youtube']
            )
            creds.refresh(Request())
            return build('youtube', 'v3', credentials=creds)
        except Exception as e:
            logging.error(f"Error autenticación: {str(e)}")
            return None
    
    def crear_transmision(self, titulo, imagen_path):
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=2)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "🎵 24/7 Música Relajante • Relax Station Radio",
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
            
            if imagen_path:
                self.youtube.thumbnails().set(
                    videoId=broadcast['id'],
                    media_body=imagen_path
                ).execute()
            
            return {
                "rtmp": f"{stream['cdn']['ingestionInfo']['ingestionAddress']}/{stream['cdn']['ingestionInfo']['streamName']}",
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id'],
                "start_time": scheduled_start
            }
        except Exception as e:
            logging.error(f"Error creando stream: {str(e)}")
            return None
    
    def obtener_estado_stream(self, stream_id):
        try:
            response = self.youtube.liveStreams().list(
                part="status",
                id=stream_id
            ).execute()
            return response.get('items', [{}])[0].get('status', {}).get('streamStatus')
        except IndexError:
            return None
        except Exception as e:
            logging.error(f"Error estado stream: {str(e)}")
            return None
    
    def transicionar_estado(self, broadcast_id, estado):
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus=estado,
                id=broadcast_id,
                part="status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Error transición {estado}: {str(e)}")
            return False
    
    def finalizar_transmision(self, broadcast_id):
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus="complete",
                id=broadcast_id,
                part="status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Error finalizando: {str(e)}")
            return False

def generar_playlist(canciones, cache_dir):
    try:
        playlist_path = os.path.join(cache_dir, "playlist.m3u")
        with open(playlist_path, "w") as f:
            f.write("#EXTM3U\n")
            for cancion in canciones:
                f.write(f"#EXTINF:-1,{cancion['name']}\nfile:'{cancion['local_path']}'\n")
        return playlist_path
    except Exception as e:
        logging.error(f"Error playlist: {str(e)}")
        return None

def generar_titulo(imagen):
    return f"🎧 {imagen['name']} • Música Continua • {datetime.utcnow().strftime('%H:%M UTC')}"

def manejar_transmision(stream_data, youtube, imagen, playlist_path):
    proceso = None
    try:
        cmd = [
            "ffmpeg",
            "-loglevel", "debug",
            "-re",
            "-loop", "1",
            "-i", imagen['local_path'],
            "-f", "concat",
            "-safe", "0",
            "-i", playlist_path,
            "-vf", f"scale={CONFIG['FFMPEG_PARAMS']['resolution']},setsar=1",
            "-c:v", CONFIG['FFMPEG_PARAMS']['video_codec'],
            "-preset", CONFIG['FFMPEG_PARAMS']['preset'],
            "-tune", CONFIG['FFMPEG_PARAMS']['tune'],
            "-b:v", CONFIG['FFMPEG_PARAMS']['video_bitrate'],
            "-maxrate", CONFIG['FFMPEG_PARAMS']['video_bitrate'],
            "-bufsize", CONFIG['FFMPEG_PARAMS']['buffersize'],
            "-r", CONFIG['FFMPEG_PARAMS']['fps'],
            "-g", CONFIG['FFMPEG_PARAMS']['g'],
            "-c:a", CONFIG['FFMPEG_PARAMS']['audio_codec'],
            "-b:a", CONFIG['FFMPEG_PARAMS']['audio_bitrate'],
            "-f", "flv",
            stream_data['rtmp']
        ]
        
        proceso = subprocess.Popen(cmd, stderr=subprocess.PIPE, universal_newlines=True)
        
        def log_ffmpeg():
            while True:
                line = processo.stderr.readline()
                if not line: break
                logging.info(f"FFMPEG: {line.strip()}")
        
        threading.Thread(target=log_ffmpeg, daemon=True).start()
        
        for i in range(CONFIG['STREAM_ACTIVATION_TIMEOUT'] // CONFIG['STREAM_CHECK_INTERVAL']):
            estado = youtube.obtener_estado_stream(stream_data['stream_id'])
            if estado == "active":
                youtube.transicionar_estado(stream_data['broadcast_id'], "testing")
                break
            time.sleep(CONFIG['STREAM_CHECK_INTERVAL'])
        else:
            raise Exception("Timeout activación stream")
        
        tiempo_restante = (stream_data['start_time'] - datetime.utcnow()).total_seconds()
        if tiempo_restante > 0: time.sleep(tiempo_restante)
        
        youtube.transicionar_estado(stream_data['broadcast_id'], "live")
        
        start_time = time.time()
        while (time.time() - start_time) < CONFIG['STREAM_DURATION']:
            if proceso.poll() is not None:
                proceso = subprocess.Popen(cmd, stderr=subprocess.PIPE)
            time.sleep(15)
        
        proceso.kill()
        youtube.finalizar_transmision(stream_data['broadcast_id'])
        return True
    
    except Exception as e:
        logging.error(f"Error transmisión: {str(e)}")
        if proceso: processo.kill()
        try: youtube.finalizar_transmision(stream_data['broadcast_id'])
        except: pass
        return False

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    while True:
        try:
            if not gestor.medios['imagenes'] or not gestor.medios['musica']:
                time.sleep(CONFIG['RETRY_DELAY'])
                continue
            
            imagen = random.choice(gestor.medios['imagenes'])
            canciones = random.sample(gestor.medios['musica'], min(50, len(gestor.medios['musica'])))
            playlist_path = generar_playlist(canciones, CONFIG['CACHE_DIR'])
            
            if not playlist_path: continue
            
            stream_info = youtube.crear_transmision(generar_titulo(imagen), imagen['local_path'])
            if not stream_info: continue
            
            threading.Thread(
                target=manejar_transmision,
                args=(stream_info, youtube, imagen, playlist_path),
                daemon=True
            ).start()
            
            time.sleep(CONFIG['STREAM_DURATION'] + 300)
        
        except Exception as e:
            logging.error(f"Error ciclo: {str(e)}")
            time.sleep(CONFIG['RETRY_DELAY'])

@app.route('/health')
def health_check():
    return "OK", 200

def signal_handler(sig, frame):
    logging.info("\n🛑 Apagado seguro...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logging.info("\n🔥 Iniciando Radio 24/7...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
