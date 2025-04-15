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

# Configuración logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Configuración global
CONFIG = {
    "MEDIOS_URL": "https://raw.githubusercontent.com/n14-py/RelaxStationmedios/master/mediosmusic.json",
    "CACHE_DIR": os.path.abspath("./radio_cache"),
    "STREAM_DURATION": 8 * 3600,  # 8 horas
    "RETRY_DELAY": 300,  # 5 minutos
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
        "tune": "stillimage",
        "thumbnail_quality": 85
    },
    "MAX_THUMBNAIL_SIZE": 2 * 1024 * 1024  # 2MB
}

class GestorContenido:
    def __init__(self):
        os.makedirs(CONFIG['CACHE_DIR'], exist_ok=True)
        self.medios = self.cargar_medios()
        self.verificar_contenido()
    
    def verificar_contenido(self):
        if not self.medios['imagenes']:
            logging.error("❌ No hay imágenes disponibles en el JSON")
        if not self.medios['musica']:
            logging.error("❌ No hay música disponible en el JSON")
    
    def procesar_url_google_drive(self, url):
        try:
            if "drive.google.com" in url:
                file_id = url.split('id=')[-1].split('&')[0]
                return f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            return url
        except:
            return url
    
    def optimizar_imagen(self, ruta_original):
        try:
            ruta_optimizada = f"{ruta_original}_opt.jpg"
            calidad = CONFIG['FFMPEG_PARAMS']['thumbnail_quality']
            
            # Primera pasada: Redimensionar
            subprocess.run([
                "ffmpeg", "-y", "-i", ruta_original,
                "-vf", "scale=1280:720:force_original_aspect_ratio=increase",
                "-q:v", "2", ruta_optimizada
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            # Segunda pasada: Optimizar tamaño
            while os.path.getsize(ruta_optimizada) > CONFIG['MAX_THUMBNAIL_SIZE'] and calidad > 30:
                calidad -= 5
                subprocess.run([
                    "ffmpeg", "-y", "-i", ruta_optimizada,
                    "-q:v", str(calidad), "-compression_level", "6",
                    ruta_optimizada
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            return ruta_optimizada if os.path.exists(ruta_optimizada) else None
        except Exception as e:
            logging.error(f"Error optimizando imagen: {str(e)}")
            return None
    
    def descargar_archivo(self, url, es_imagen=False):
        try:
            url = self.procesar_url_google_drive(url)
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            extension = ".jpg" if es_imagen else ".mp3"
            ruta_local = os.path.join(CONFIG['CACHE_DIR'], f"{nombre_hash}{extension}")
            
            if os.path.exists(ruta_local):
                return ruta_local

            logging.info(f"⬇️ Descargando {'imagen' if es_imagen else 'música'}: {url}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(ruta_local, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            if es_imagen:
                return self.optimizar_imagen(ruta_local)
            return ruta_local
        except Exception as e:
            logging.error(f"Error descargando archivo: {str(e)}")
            return None
    
    def cargar_medios(self):
        try:
            respuesta = requests.get(CONFIG["MEDIOS_URL"], timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            # Procesar imágenes
            imagenes_procesadas = []
            for img in datos['imagenes']:
                local_path = self.descargar_archivo(img['url'], es_imagen=True)
                if local_path:
                    imagenes_procesadas.append({
                        "name": img['name'],
                        "local_path": local_path
                    })
            
            # Procesar música
            musica_procesada = []
            for m in datos['musica']:
                local_path = self.descargar_archivo(m['url'])
                if local_path:
                    musica_procesada.append({
                        "name": m['name'],
                        "local_path": local_path
                    })
            
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
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "🎵 Relax Station Radio • Música Continua 24/7\n\nDisfruta de nuestra programación musical las 24 horas\n\n🔔 Activa las notificaciones\n\n#MusicaContinua #RadioOnline #Relax",
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
                    "snippet": {"title": "Stream Principal Radio"},
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

            # Subir miniatura optimizada
            if imagen_path and os.path.exists(imagen_path):
                self.youtube.thumbnails().set(
                    videoId=broadcast['id'],
                    media_body=imagen_path,
                    media_mime_type='image/jpeg'  # Forzar tipo MIME
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
    
    def finalizar_transmision(self, broadcast_id):
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus="complete",
                id=broadcast_id,
                part="id,status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Error finalizando transmisión: {str(e)}")
            return False

def generar_playlist(canciones, cache_dir):
    playlist_path = os.path.join(cache_dir, "playlist.m3u")
    with open(playlist_path, "w") as f:
        f.write("#EXTM3U\n")
        for cancion in canciones:
            f.write(f"#EXTINF:-1,{cancion['name']}\n{cancion['local_path']}\n")
    return playlist_path

def generar_titulo(imagen):
    return f"🎧 {imagen['name']} • Música Continua • {datetime.utcnow().strftime('%H:%M UTC')}"

def manejar_transmision(stream_data, youtube, imagen, playlist_path):
    proceso = None
    try:
        # Configurar FFmpeg
        cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-re",
            "-stream_loop", "-1",
            "-i", imagen['local_path'],
            "-f", "concat",
            "-safe", "0",
            "-stream_loop", "-1",
            "-i", playlist_path,
            "-vf", f"scale={CONFIG['FFMPEG_PARAMS']['resolution']},setsar=1",
            "-c:v", CONFIG['FFMPEG_PARAMS']['video_codec'],
            "-preset", CONFIG['FFMPEG_PARAMS']['preset'],
            "-tune", CONFIG['FFMPEG_PARAMS']['tune'],
            "-b:v", CONFIG['FFMPEG_PARAMS']['video_bitrate'],
            "-r", CONFIG['FFMPEG_PARAMS']['fps'],
            "-g", "60",
            "-c:a", CONFIG['FFMPEG_PARAMS']['audio_codec'],
            "-b:a", CONFIG['FFMPEG_PARAMS']['audio_bitrate'],
            "-f", "flv",
            stream_data['rtmp']
        ]

        proceso = subprocess.Popen(cmd)
        logging.info("🟢 FFmpeg iniciado - Estableciendo conexión RTMP...")

        # Esperar activación del stream
        max_checks = 10
        for _ in range(max_checks):
            estado = youtube.obtener_estado_stream(stream_data['stream_id'])
            if estado == 'active':
                logging.info("✅ Stream activo - Transicionando a testing")
                if youtube.transicionar_estado(stream_data['broadcast_id'], 'testing'):
                    logging.info("🎬 Transmisión en VISTA PREVIA")
                break
            time.sleep(5)
        else:
            raise Exception("Stream no se activó a tiempo")

        # Transición a LIVE en el tiempo programado
        tiempo_restante = (stream_data['scheduled_start'] - datetime.utcnow()).total_seconds()
        if tiempo_restante > 0:
            logging.info(f"⏳ Esperando {tiempo_restante:.0f}s para LIVE...")
            time.sleep(tiempo_restante)
        
        if youtube.transicionar_estado(stream_data['broadcast_id'], 'live'):
            logging.info("🎥 Transmisión LIVE iniciada")
        else:
            raise Exception("No se pudo iniciar la transmisión LIVE")

        # Mantener transmisión por 8 horas
        tiempo_inicio = datetime.utcnow()
        while (datetime.utcnow() - tiempo_inicio) < timedelta(hours=8):
            if proceso.poll() is not None:
                logging.warning("⚡ Reconectando FFmpeg...")
                proceso.kill()
                proceso = subprocess.Popen(cmd)
            time.sleep(15)
        
        proceso.kill()
        youtube.finalizar_transmision(stream_data['broadcast_id'])
        logging.info("🛑 Transmisión finalizada correctamente")
        return True

    except Exception as e:
        logging.error(f"Error en transmisión: {str(e)}")
        if proceso:
            proceso.kill()
        youtube.finalizar_transmision(stream_data['broadcast_id'])
        return False

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    current_stream = None
    
    while True:
        try:
            if not current_stream:
                if not gestor.medios['imagenes'] or not gestor.medios['musica']:
                    logging.error("🚨 Contenido insuficiente - Reintentando en 5 minutos...")
                    time.sleep(CONFIG['RETRY_DELAY'])
                    continue
                
                imagen = random.choice(gestor.medios['imagenes'])
                canciones = random.sample(gestor.medios['musica'], len(gestor.medios['musica']))
                playlist_path = generar_playlist(canciones, CONFIG['CACHE_DIR'])
                
                stream_info = youtube.crear_transmision(
                    generar_titulo(imagen),
                    imagen['local_path']
                )
                
                if not stream_info:
                    raise Exception("Error creando transmisión")
                
                current_stream = {
                    "data": stream_info,
                    "imagen": imagen,
                    "playlist": playlist_path,
                    "start_time": datetime.utcnow()
                }

                threading.Thread(
                    target=manejar_transmision,
                    args=(stream_info, youtube, imagen, playlist_path),
                    daemon=True
                ).start()

            else:
                tiempo_transcurrido = (datetime.utcnow() - current_stream['start_time']).total_seconds()
                if tiempo_transcurrido > CONFIG['STREAM_DURATION']:
                    logging.info("🔄 Tiempo de transmisión completado - Preparando nueva...")
                    current_stream = None
                time.sleep(15)

        except Exception as e:
            logging.error(f"🔥 Error crítico: {str(e)}")
            current_stream = None
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
    
    logging.info("🎶 Iniciando Radio 24/7...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
