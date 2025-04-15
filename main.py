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

# Configuración logging detallada
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Configuración global mejorada
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
        "thumbnail_quality": 85,
        "buffersize": "6000k",
        "pixel_format": "yuv420p",
        "audio_filter": "aresample=async=1:first_pts=0,highpass=f=100,lowpass=f=14000"
    },
    "MAX_THUMBNAIL_SIZE": 2 * 1024 * 1024,
    "SONG_REPEAT_FACTOR": 15
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
                file_id = url.split('id=')[-1].split('&')[0]
                return f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            return url
        except:
            return url
    
    def optimizar_imagen(self, ruta_original):
        try:
            ruta_optimizada = f"{ruta_original}_opt.jpg"
            calidad = CONFIG['FFMPEG_PARAMS']['thumbnail_quality']
            subprocess.run([
                "ffmpeg", "-y", "-i", ruta_original,
                "-vf", "scale=1280:720:force_original_aspect_ratio=increase",
                "-q:v", "2", ruta_optimizada
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            while os.path.getsize(ruta_optimizada) > CONFIG['MAX_THUMBNAIL_SIZE'] and calidad > 30:
                calidad -= 5
                subprocess.run([
                    "ffmpeg", "-y", "-i", ruta_optimizada,
                    "-q:v", str(calidad), "-compression_level", "6",
                    ruta_optimizada
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return ruta_optimizada
        except Exception as e:
            logging.error(f"Error optimizando imagen: {str(e)}")
            return None
    
    def descargar_archivo(self, url, es_imagen=False):
        try:
            url = self.procesar_url_google_drive(url)
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            extension = ".jpg" if es_imagen else ".mp3"
            ruta_local = os.path.abspath(os.path.join(CONFIG['CACHE_DIR'], f"{nombre_hash}{extension}"))
            
            if os.path.exists(ruta_local):
                return ruta_local

            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(ruta_local, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            return self.optimizar_imagen(ruta_local) if es_imagen else ruta_local
        except Exception as e:
            logging.error(f"Error descargando archivo: {str(e)}")
            return None
    
    def cargar_medios(self):
        try:
            respuesta = requests.get(CONFIG["MEDIOS_URL"], timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            imagenes_procesadas = []
            for img in datos['imagenes']:
                local_path = self.descargar_archivo(img['url'], es_imagen=True)
                if local_path:
                    imagenes_procesadas.append({"name": img['name'], "local_path": local_path})
            
            musica_procesada = []
            for m in datos['musica']:
                local_path = self.descargar_archivo(m['url'])
                if local_path:
                    musica_procesada.append({"name": m['name'], "local_path": local_path})
            
            return {"imagenes": imagenes_procesadas, "musica": musica_procesada}
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
                        "description": "🎵 Relax Station Radio • Música Continua 24/7",
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
                    "cdn": {"ingestionType": "rtmp", "resolution": "1080p", "frameRate": "30fps"}
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
    
    def obtener_estado_stream(self, stream_id):
        try:
            response = self.youtube.liveStreams().list(part="status", id=stream_id).execute()
            return response.get('items', [{}])[0].get('status', {}).get('streamStatus')
        except Exception as e:
            logging.error(f"Error obteniendo estado: {str(e)}")
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

def generar_titulo(imagen):
    return f"🎧 {imagen['name']} • Música Continua • {datetime.utcnow().strftime('%H:%M UTC')}"

def manejar_transmision(stream_data, youtube, imagen, canciones):
    proceso = None
    try:
        canciones_duplicadas = canciones * CONFIG['SONG_REPEAT_FACTOR']
        random.shuffle(canciones_duplicadas)
        
        concat_entries = [f"file '{os.path.abspath(c['local_path'])}'\n" for c in canciones_duplicadas]
        concat_input = "".join(concat_entries)

        cmd = [
            "ffmpeg",
            "-loglevel", "verbose",
            "-re",
            "-stream_loop", "-1",
            "-i", os.path.abspath(imagen['local_path']),
            "-f", "concat",
            "-safe", "0",
            "-protocol_whitelist", "file,pipe",
            "-i", "pipe:0",
            "-vf", f"fps=30,format={CONFIG['FFMPEG_PARAMS']['pixel_format']},scale=1280:720:force_original_aspect_ratio=increase",
            "-c:v", CONFIG['FFMPEG_PARAMS']['video_codec'],
            "-preset", CONFIG['FFMPEG_PARAMS']['preset'],
            "-tune", CONFIG['FFMPEG_PARAMS']['tune'],
            "-b:v", CONFIG['FFMPEG_PARAMS']['video_bitrate'],
            "-maxrate", CONFIG['FFMPEG_PARAMS']['video_bitrate'],
            "-bufsize", CONFIG['FFMPEG_PARAMS']['buffersize'],
            "-g", "60",
            "-keyint_min", "60",
            "-x264-params", "nal-hrd=cbr",
            "-c:a", CONFIG['FFMPEG_PARAMS']['audio_codec'],
            "-b:a", CONFIG['FFMPEG_PARAMS']['audio_bitrate'],
            "-ar", "44100",
            "-af", CONFIG['FFMPEG_PARAMS']['audio_filter'],
            "-f", "flv",
            "-flvflags", "no_duration_filesize",
            stream_data['rtmp']
        ]

        proceso = subprocess.Popen(
            cmd, 
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        proceso.stdin.write(concat_input)
        proceso.stdin.close()

        def log_ffmpeg_output():
            while proceso.poll() is None:
                output = proceso.stderr.readline()
                if output: logging.info(f"FFMPEG: {output.strip()}")

        threading.Thread(target=log_ffmpeg_output, daemon=True).start()

        # Esperar activación mejorada
        for i in range(15):
            estado = youtube.obtener_estado_stream(stream_data['stream_id'])
            if estado == 'active':
                for intento in range(3):
                    if youtube.transicionar_estado(stream_data['broadcast_id'], 'testing'):
                        break
                    time.sleep(5)
                break
            time.sleep(10)

        tiempo_restante = (stream_data['scheduled_start'] - datetime.utcnow()).total_seconds()
        if tiempo_restante > 0: time.sleep(tiempo_restante)
        
        if not youtube.transicionar_estado(stream_data['broadcast_id'], 'live'):
            raise Exception("Error iniciando LIVE")

        tiempo_inicio = datetime.utcnow()
        while (datetime.utcnow() - tiempo_inicio) < timedelta(hours=8):
            if proceso.poll() is not None:
                random.shuffle(canciones_duplicadas)
                concat_input = "".join([f"file '{os.path.abspath(c['local_path'])}'\n" for c in canciones_duplicadas])
                proceso = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                proceso.stdin.write(concat_input)
                proceso.stdin.close()
            time.sleep(15)

        proceso.kill()
        youtube.finalizar_transmision(stream_data['broadcast_id'])
        return True

    except Exception as e:
        logging.error(f"Error en transmisión: {str(e)}")
        if proceso: proceso.kill()
        try: youtube.finalizar_transmision(stream_data['broadcast_id'])
        except: pass
        return False

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    current_stream = None
    
    while True:
        try:
            if not current_stream:
                if not gestor.medios['imagenes'] or not gestor.medios['musica']:
                    time.sleep(CONFIG['RETRY_DELAY'])
                    continue
                
                imagen = random.choice(gestor.medios['imagenes'])
                canciones = gestor.medios['musica'].copy()
                random.shuffle(canciones)
                
                stream_info = youtube.crear_transmision(generar_titulo(imagen), imagen['local_path'])
                if not stream_info: continue
                
                current_stream = {
                    "data": stream_info,
                    "imagen": imagen,
                    "canciones": canciones,
                    "start_time": datetime.utcnow()
                }

                threading.Thread(
                    target=manejar_transmision,
                    args=(stream_info, youtube, imagen, canciones),
                    daemon=True
                ).start()
            else:
                if (datetime.utcnow() - current_stream['start_time']).total_seconds() > CONFIG['STREAM_DURATION']:
                    current_stream = None
                time.sleep(15)

        except Exception as e:
            logging.error(f"Error crítico: {str(e)}")
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
    logging.info("\n" + "="*50 + "\n🎶 Iniciando Radio 24/7...\n" + "="*50)
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
